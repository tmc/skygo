package rpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

// workerService serves WorkerGroupService over one control.Workers. It shares
// its handler's authentication, audit and stream permits, so it must be
// mounted on the handler's own mux. Cancellation is the existing CancelJob;
// there is no fallback to SubmitJob. Mounting it advertises nothing.
type workerService struct {
	h       *handler
	workers *control.Workers
	// watch is watchStream outside tests.
	watch func(context.Context, *auth.Engine, auth.Actor) (context.Context, context.CancelFunc)
}

// newWorkerService returns the mount path and handler of WorkerGroupService.
func newWorkerService(h *handler, w *control.Workers) (string, http.Handler) {
	return skyv1connect.NewWorkerGroupServiceHandler(&workerService{h: h, workers: w, watch: watchStream}, connect.WithReadMaxBytes(512<<10))
}

var _ skyv1connect.WorkerGroupServiceHandler = (*workerService)(nil)

// workerSpec converts a wire spec, checking the count before narrowing it.
func workerSpec(w *v1.WorkerSpec) (backend.WorkerSpec, error) {
	if w == nil {
		return backend.WorkerSpec{}, fmt.Errorf("%w: worker spec required", control.ErrInvalid)
	}
	if w.Count > backend.MaxWorkers {
		return backend.WorkerSpec{}, fmt.Errorf("%w: worker count must be between 1 and 16", control.ErrInvalid)
	}
	var mode backend.WorkerMode
	switch w.Mode {
	case v1.WorkerMode_WORKER_MODE_INDEPENDENT:
		mode = backend.WorkerIndependent
	case v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS:
		mode = backend.WorkerTCP
	default:
		return backend.WorkerSpec{}, fmt.Errorf("%w: invalid worker mode", control.ErrInvalid)
	}
	return backend.WorkerSpec{Count: uint8(w.Count), Mode: mode}, nil
}

func wireWorkerSpec(w backend.WorkerSpec) *v1.WorkerSpec {
	mode := v1.WorkerMode_WORKER_MODE_INDEPENDENT
	if w.Mode == backend.WorkerTCP {
		mode = v1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS
	}
	return &v1.WorkerSpec{Count: uint32(w.Count), Mode: mode}
}

var workerRankStates = map[control.WorkerRankState]v1.WorkerRankState{
	control.WorkerWaiting:   v1.WorkerRankState_WORKER_RANK_STATE_WAITING,
	control.WorkerStarting:  v1.WorkerRankState_WORKER_RANK_STATE_STARTING,
	control.WorkerRunning:   v1.WorkerRankState_WORKER_RANK_STATE_RUNNING,
	control.WorkerSucceeded: v1.WorkerRankState_WORKER_RANK_STATE_SUCCEEDED,
	control.WorkerFailed:    v1.WorkerRankState_WORKER_RANK_STATE_FAILED,
	control.WorkerCanceled:  v1.WorkerRankState_WORKER_RANK_STATE_CANCELED,
	control.WorkerUnknown:   v1.WorkerRankState_WORKER_RANK_STATE_UNKNOWN,
}

// wireWorkerRank projects one rank. A state with no wire value is Unknown.
func wireWorkerRank(r control.WorkerRank) *v1.WorkerRank {
	state, ok := workerRankStates[r.State]
	if !ok {
		state = v1.WorkerRankState_WORKER_RANK_STATE_UNKNOWN
	}
	return &v1.WorkerRank{Rank: uint32(r.Rank), State: state, Reason: r.Reason, ExitCode: exitCode(r.ExitCode)}
}

func (s *workerService) SubmitWorkerJob(ctx context.Context, request *connect.Request[v1.SubmitWorkerJobRequest]) (*connect.Response[v1.SubmitWorkerJobResponse], error) {
	actor, err := s.h.engine.Check(requestActor(ctx), auth.Mutate)
	if err != nil {
		return nil, rpcError(err)
	}
	m := request.Msg
	if m == nil || !validTaskUTF8(m.Task) || !retentionRequestID(m.RequestId) {
		return nil, rpcError(fmt.Errorf("%w: task and valid request ID required", control.ErrInvalid))
	}
	if hasWorkspaceTask(m.Task) {
		return nil, rpcError(fmt.Errorf("%w: worker tasks do not support workspaces", control.ErrInvalid))
	}
	spec, err := workerSpec(m.Workers)
	if err != nil {
		return nil, rpcError(err)
	}
	domain, err := requestDomain(m.Domain)
	if err != nil {
		return nil, rpcError(err)
	}
	accepted, normalized, err := s.workers.Submit(actor, domain, m.RequestId, task(m.Task), spec)
	s.h.record(ctx, "submit", accepted.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := retainedJob(accepted)
	if err != nil {
		return nil, rpcError(err)
	}
	if normalized != nil {
		out.Workers = wireWorkerSpec(*normalized)
	}
	return connect.NewResponse(&v1.SubmitWorkerJobResponse{Job: out}), nil
}

func (s *workerService) GetWorkerRanks(ctx context.Context, request *connect.Request[v1.GetWorkerRanksRequest]) (*connect.Response[v1.GetWorkerRanksResponse], error) {
	m := request.Msg
	if m == nil || !hexID(m.Id) || !hexID(m.AttemptId) {
		return nil, rpcError(fmt.Errorf("%w: job and attempt IDs required", control.ErrInvalid))
	}
	g, ranks, err := s.workers.Ranks(requestActor(ctx), m.Id, m.AttemptId)
	s.h.record(ctx, "ranks", m.Id, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &v1.GetWorkerRanksResponse{Id: g.Job.ID, AttemptId: g.Group.AttemptID, Workers: wireWorkerSpec(g.Group.Spec)}
	for _, r := range ranks {
		out.Ranks = append(out.Ranks, wireWorkerRank(r))
	}
	return connect.NewResponse(out), nil
}

// StreamWorkerLogs streams one rank's own log. Every response names the
// request's attempt and rank; offsets are that rank's alone. The selector is
// checked before any backend read. The stream permit, like a job log
// stream's, belongs to the enclosing HTTP response: its cleanup releases it
// once, after Connect has written and flushed the final trailers.
func (s *workerService) StreamWorkerLogs(parent context.Context, request *connect.Request[v1.StreamWorkerLogsRequest], stream *connect.ServerStream[v1.StreamWorkerLogsResponse]) error {
	m := request.Msg
	if m == nil || !hexID(m.Id) || !hexID(m.AttemptId) || m.Offset < 0 || m.Offset > backend.MaxLogBytes || m.Limit < 1 || m.Limit > backend.MaxChunkBytes || m.Rank >= backend.MaxWorkers {
		return rpcError(fmt.Errorf("%w: invalid worker log selector", control.ErrInvalid))
	}
	actor := requestActor(parent)
	if _, ranks, err := s.workers.Ranks(actor, m.Id, m.AttemptId); err != nil || int(m.Rank) >= len(ranks) {
		if err == nil {
			err = fmt.Errorf("%w: worker rank out of range", control.ErrInvalid)
		}
		s.h.record(parent, "logs", "", err)
		return rpcError(err)
	}
	response, ok := parent.Value(responseKey{}).(*streamResponse)
	if !ok {
		return connect.NewError(connect.CodeInternal, errors.New("log transport unavailable"))
	}
	select {
	case s.h.streams <- struct{}{}:
	default:
		return connect.NewError(connect.CodeResourceExhausted, errStreamLimit)
	}
	release, err := s.workers.BeginLogs(actor, m.Id, m.AttemptId, m.Rank)
	if err != nil {
		<-s.h.streams
		return rpcError(err)
	}
	limited, stop := context.WithTimeout(parent, 5*time.Minute)
	ctx, cancel := s.watch(limited, s.h.engine, actor)
	aborted := make(chan struct{})
	abort := context.AfterFunc(ctx, func() {
		defer close(aborted)
		// Retain the abort deadline through Connect's final trailers and flush.
		_ = response.controller.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	})
	response.cleanup = func() {
		defer release()
		if !abort() {
			<-aborted
			_ = response.controller.SetWriteDeadline(time.Time{})
		}
		cancel()
		stop()
		_ = s.h.control.RecordAccess(actor, "logs", m.Id, "ended")
		<-s.h.streams
	}
	_ = s.h.control.RecordAccess(actor, "logs", m.Id, "started")
	offset := m.Offset
	for {
		chunk, err := s.workers.Logs(ctx, actor, m.Id, m.AttemptId, m.Rank, offset, int(m.Limit))
		if ctx.Err() != nil {
			return rpcError(context.Cause(ctx))
		}
		if err != nil {
			return rpcError(err)
		}
		if err := stream.Send(&v1.StreamWorkerLogsResponse{Data: chunk.Data, NextOffset: chunk.NextOffset, Complete: chunk.Complete, Truncated: chunk.Truncated, AttemptId: m.AttemptId, Rank: m.Rank}); err != nil {
			return err
		}
		if chunk.Complete || chunk.NextOffset == backend.MaxLogBytes || !m.Follow && len(chunk.Data) == 0 {
			return nil
		}
		offset = chunk.NextOffset
		if len(chunk.Data) == 0 {
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return rpcError(context.Cause(ctx))
			case <-timer.C:
			}
		}
	}
}
