package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

var errStreamLimit = errors.New("log stream limit reached")

// streamResponse belongs to one enclosing ServeHTTP call. Its cleanup is set
// only after admission; no watchdog or worker is created for rejected streams.
type streamResponse struct {
	controller *http.ResponseController
	done       chan struct{}
	cleanup    func()
}

func (r *streamResponse) finish() {
	if r.cleanup != nil {
		r.cleanup()
	}
	close(r.done)
}

// watchStream revalidates without waiting for backend I/O. It is created only
// after acquiring a stream permit. The enclosing HTTP handler stops and joins it.
func watchStream(parent context.Context, engine *auth.Engine, actor auth.Actor) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			changed := engine.Changed()
			fresh, err := engine.Check(actor, auth.Read)
			if err != nil {
				cancel(err)
				return
			}
			if fresh.Role() != actor.Role() {
				cancel(auth.ErrPermissionDenied)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			case <-tick.C:
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled); <-done }
}

type logSelection struct {
	id       string
	offset   int64
	follow   bool
	attempt  uint32
	cursor   string
	retained bool
}

func (h *handler) StreamJobLogs(parent context.Context, request *connect.Request[v1.StreamJobLogsRequest], stream *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	if request.Msg == nil {
		return rpcError(fmt.Errorf("%w: log request required", control.ErrInvalid))
	}
	message := request.Msg
	return h.streamLogs(parent, logSelection{id: message.Id, offset: message.Offset, follow: message.Follow, attempt: message.Attempt}, func(chunk backend.LogChunk, attemptID, _ string) error {
		return stream.Send(&v1.StreamJobLogsResponse{Data: chunk.Data, NextOffset: chunk.NextOffset, Complete: chunk.Complete, Truncated: chunk.Truncated, AttemptId: attemptID})
	})
}

func (h *handler) StreamRetainedJobLogs(parent context.Context, request *connect.Request[v1.StreamRetainedJobLogsRequest], stream *connect.ServerStream[v1.StreamRetainedJobLogsResponse]) error {
	if request.Msg == nil {
		return rpcError(fmt.Errorf("%w: log request required", control.ErrInvalid))
	}
	message := request.Msg
	return h.streamLogs(parent, logSelection{id: message.Id, offset: message.Offset, follow: message.Follow, attempt: message.Attempt, cursor: message.Cursor, retained: true}, func(chunk backend.LogChunk, attemptID, cursor string) error {
		return stream.Send(&v1.StreamRetainedJobLogsResponse{Data: chunk.Data, NextOffset: chunk.NextOffset, Complete: chunk.Complete, Truncated: chunk.Truncated, AttemptId: attemptID, Cursor: cursor})
	})
}

func (h *handler) streamLogs(parent context.Context, selection logSelection, send func(backend.LogChunk, string, string) error) error {
	if !hexID(selection.id) || selection.offset < 0 || selection.offset > backend.MaxLogBytes || selection.attempt > 10 || len(selection.cursor) > 683 || !utf8.ValidString(selection.cursor) || (selection.cursor != "" && selection.offset != 0) {
		return rpcError(fmt.Errorf("%w: invalid log selector", control.ErrInvalid))
	}
	actor := requestActor(parent)
	job, err := h.control.Get(actor, selection.id)
	if err != nil {
		h.record(parent, "logs", "", err)
		return rpcError(err)
	}
	select {
	case h.streams <- struct{}{}:
	default:
		return connect.NewError(connect.CodeResourceExhausted, errStreamLimit)
	}
	lease, err := h.control.OpenLogs(actor, selection.id, selection.attempt, selection.cursor, selection.offset)
	if err != nil {
		<-h.streams
		h.record(parent, "logs", "", err)
		return rpcError(err)
	}
	response, ok := parent.Value(responseKey{}).(*streamResponse)
	if !ok {
		lease.Close()
		<-h.streams
		return connect.NewError(connect.CodeInternal, errors.New("log transport unavailable"))
	}
	wireAttemptID := ""
	if selection.retained || len(job.Attempts) != 0 || selection.attempt != 0 {
		wireAttemptID = lease.AttemptID()
	}
	limited, stop := context.WithTimeout(parent, 5*time.Minute)
	ctx, cancel := watchStream(limited, h.engine, actor)
	aborted := make(chan struct{})
	abort := context.AfterFunc(ctx, func() {
		defer close(aborted)
		// Retain the abort deadline through Connect's final trailers and flush.
		_ = response.controller.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	})
	response.cleanup = func() {
		if !abort() {
			<-aborted
			_ = response.controller.SetWriteDeadline(time.Time{})
		}
		cancel()
		stop()
		_ = h.control.RecordAccess(actor, "logs", job.ID, "ended")
	}
	_ = h.control.RecordAccess(actor, "logs", job.ID, "started")
	type result struct {
		chunk backend.LogChunk
		err   error
	}
	results := make(chan result, 1)
	go func() {
		// Both real backend completion and real HTTP completion own this lease.
		// A canceled but stuck backend keeps its lease and permit; no replacement.
		defer func() { <-response.done; lease.Close(); <-h.streams }()
		offset := lease.Offset()
		for {
			chunk, err := lease.Read(ctx, actor, offset)
			select {
			case results <- result{chunk, err}:
			case <-ctx.Done():
				return
			}
			if err != nil || chunk.Complete || (!selection.follow && len(chunk.Data) == 0) {
				return
			}
			offset = chunk.NextOffset
			if len(chunk.Data) == 0 {
				timer := time.NewTimer(250 * time.Millisecond)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return rpcError(context.Cause(ctx))
		case next := <-results:
			if ctx.Err() != nil {
				return rpcError(context.Cause(ctx))
			}
			if next.err != nil {
				return rpcError(next.err)
			}
			if err := lease.Check(actor); err != nil {
				return rpcError(err)
			}
			fresh, err := h.engine.Check(actor, auth.Read)
			if err != nil {
				return rpcError(err)
			}
			if fresh.Role() != actor.Role() {
				return rpcError(auth.ErrPermissionDenied)
			}
			cursor := ""
			if selection.retained {
				cursor, err = lease.Cursor(next.chunk.NextOffset)
				if err != nil {
					return rpcError(err)
				}
			}
			if ctx.Err() != nil {
				return rpcError(context.Cause(ctx))
			}
			if err := send(next.chunk, wireAttemptID, cursor); err != nil {
				return err
			}
			if next.chunk.Complete || (!selection.follow && len(next.chunk.Data) == 0) {
				return nil
			}
		}
	}
}
