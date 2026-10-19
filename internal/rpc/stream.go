package rpc

import (
	"context"
	"errors"
	"net/http"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
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

func (h *handler) StreamJobLogs(parent context.Context, r *connect.Request[v1.StreamJobLogsRequest], stream *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	actor := requestActor(parent)
	job, err := h.control.Get(actor, r.Msg.Id)
	if err != nil {
		h.record(parent, "logs", "", err)
		return rpcError(err)
	}
	select {
	case h.streams <- struct{}{}:
	default:
		return connect.NewError(connect.CodeResourceExhausted, errStreamLimit)
	}
	response := parent.Value(responseKey{}).(*streamResponse)
	limited, stop := context.WithTimeout(parent, 5*time.Minute)
	ctx, cancel := watchStream(limited, h.engine, actor)
	aborted := make(chan struct{})
	abort := context.AfterFunc(ctx, func() {
		defer close(aborted)
		// Keep this deadline through Connect's final trailer/flush. The short
		// allowance lets idle streams deliver a typed error; blocked writes
		// must still finish within the two-second revocation envelope.
		_ = response.controller.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	})
	response.cleanup = func() {
		// Normal completion stops the abort before normal cancellation. If it
		// has started, join it before resetting the connection's deadline.
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
		// A context-ignoring backend keeps its permit until it really returns.
		// A completed worker also waits for Connect trailers and outer cleanup,
		// bounding blocked writers and watchdogs, not only backend calls.
		defer func() { <-response.done; <-h.streams }()
		offset := r.Msg.Offset
		for {
			chunk, err := h.control.Logs(ctx, actor, r.Msg.Id, offset)
			select {
			case results <- result{chunk, err}:
			case <-ctx.Done():
				return
			}
			if err != nil || chunk.Complete || (!r.Msg.Follow && len(chunk.Data) == 0) {
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
			// Check the current scope after Logs returns, before publishing bytes.
			// A demoted admin must not keep reading another principal's job.
			if _, err := h.control.Get(actor, job.ID); err != nil {
				return rpcError(err)
			}
			fresh, err := h.engine.Check(actor, auth.Read)
			if err != nil {
				return rpcError(err)
			}
			if fresh.Role() != actor.Role() {
				return rpcError(auth.ErrPermissionDenied)
			}
			chunk := next.chunk
			if ctx.Err() != nil {
				return rpcError(context.Cause(ctx))
			}
			if err := stream.Send(&v1.StreamJobLogsResponse{Data: chunk.Data, NextOffset: chunk.NextOffset, Complete: chunk.Complete, Truncated: chunk.Truncated}); err != nil {
				return err
			}
			if chunk.Complete || (!r.Msg.Follow && len(chunk.Data) == 0) {
				return nil
			}
		}
	}
}
