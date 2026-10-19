package rpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"io/fs"
	"net/http"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type handler struct {
	control *control.Control
	streams chan struct{}
}

// NewHandler serves the control RPCs. A nonempty token authenticates every
// RPC; transport encryption is the caller's responsibility.
func NewHandler(c *control.Control, token string) http.Handler {
	h := &handler{control: c, streams: make(chan struct{}, 32)}
	path, service := skyv1connect.NewControlServiceHandler(h, connect.WithReadMaxBytes(512<<10))
	mux := http.NewServeMux()
	mux.Handle(path, service)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func task(t *v1.Task) backend.Task {
	if t == nil {
		return backend.Task{}
	}
	return backend.Task{Argv: append([]string(nil), t.Argv...), Env: t.Env, Directory: t.Directory, Image: t.Image, CPUMillis: t.CpuMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds}
}
func wireTask(t backend.Task) *v1.Task {
	return &v1.Task{Argv: t.Argv, Env: t.Env, Directory: t.Directory, Image: t.Image, CpuMillis: t.CPUMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds}
}

var states = map[backend.State]v1.JobState{backend.Queued: v1.JobState_JOB_STATE_QUEUED, backend.Starting: v1.JobState_JOB_STATE_STARTING, backend.Running: v1.JobState_JOB_STATE_RUNNING, backend.Succeeded: v1.JobState_JOB_STATE_SUCCEEDED, backend.Failed: v1.JobState_JOB_STATE_FAILED, backend.Canceled: v1.JobState_JOB_STATE_CANCELED, backend.Unknown: v1.JobState_JOB_STATE_UNKNOWN}

func job(j backend.Job) *v1.Job {
	return &v1.Job{Id: j.ID, Task: wireTask(j.Task), Target: j.Target, State: states[j.Status.State], CancelRequested: j.CancelRequested, Reason: j.Status.Reason, ExitCode: j.Status.ExitCode, CreatedAt: timestamppb.New(j.CreatedAt), UpdatedAt: timestamppb.New(j.UpdatedAt)}
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	code := connect.CodeInternal
	switch {
	case errors.Is(err, context.Canceled):
		code = connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = connect.CodeDeadlineExceeded
	case errors.Is(err, control.ErrInvalid), errors.Is(err, fs.ErrInvalid):
		code = connect.CodeInvalidArgument
	case errors.Is(err, control.ErrUnsupported), errors.Is(err, control.ErrLogsUnavailable):
		code = connect.CodeFailedPrecondition
	case errors.Is(err, control.ErrUnavailable):
		code = connect.CodeUnavailable
	case errors.Is(err, store.ErrNotFound):
		code = connect.CodeNotFound
	case errors.Is(err, store.ErrConflict):
		code = connect.CodeAlreadyExists
	case errors.Is(err, store.ErrFull):
		code = connect.CodeResourceExhausted
	}
	if code == connect.CodeInternal {
		return connect.NewError(code, errors.New("internal operation failed"))
	}
	return connect.NewError(code, err)
}
func (h *handler) PlanTask(ctx context.Context, r *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	name, rejections, err := h.control.Plan(task(r.Msg.Task))
	if err != nil {
		return nil, rpcError(err)
	}
	out := &v1.PlanTaskResponse{Target: name}
	for _, r := range rejections {
		out.Rejections = append(out.Rejections, &v1.Rejection{Target: r.Target, Reason: r.Reason})
	}
	return connect.NewResponse(out), nil
}
func (h *handler) SubmitJob(ctx context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	j, err := h.control.Submit(r.Msg.RequestId, task(r.Msg.Task))
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.SubmitJobResponse{Job: job(j)}), nil
}
func (h *handler) GetJob(ctx context.Context, r *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	j, err := h.control.Get(r.Msg.Id)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.GetJobResponse{Job: job(j)}), nil
}
func (h *handler) ListJobs(ctx context.Context, r *connect.Request[v1.ListJobsRequest]) (*connect.Response[v1.ListJobsResponse], error) {
	jobs, next, err := h.control.List(int(r.Msg.PageSize), r.Msg.PageToken)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &v1.ListJobsResponse{NextPageToken: next}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, job(j))
	}
	return connect.NewResponse(out), nil
}
func (h *handler) CancelJob(ctx context.Context, r *connect.Request[v1.CancelJobRequest]) (*connect.Response[v1.CancelJobResponse], error) {
	j, err := h.control.Cancel(r.Msg.Id)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.CancelJobResponse{Job: job(j)}), nil
}
func (h *handler) ListTargets(ctx context.Context, r *connect.Request[v1.ListTargetsRequest]) (*connect.Response[v1.ListTargetsResponse], error) {
	out := &v1.ListTargetsResponse{}
	for _, t := range h.control.Targets() {
		out.Targets = append(out.Targets, wireTarget(t))
	}
	return connect.NewResponse(out), nil
}

// All current backends implement finite jobs. New target-specific semantics must
// be advertised and checked by that integration when they are introduced.
func wireTarget(t backend.Target) *v1.Target {
	return &v1.Target{Name: t.Name, Kind: t.Kind, Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}}
}

func (h *handler) GetServerInfo(ctx context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	out := &v1.GetServerInfoResponse{}
	for _, t := range h.control.Targets() {
		out.Targets = append(out.Targets, wireTarget(t))
	}
	return connect.NewResponse(out), nil
}

func (h *handler) StreamJobLogs(ctx context.Context, r *connect.Request[v1.StreamJobLogsRequest], stream *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	select {
	case h.streams <- struct{}{}:
		defer func() { <-h.streams }()
	default:
		return connect.NewError(connect.CodeResourceExhausted, errors.New("log stream limit reached"))
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	offset := r.Msg.Offset
	for {
		chunk, err := h.control.Logs(ctx, r.Msg.Id, offset)
		if err != nil {
			return rpcError(err)
		}
		if err = stream.Send(&v1.StreamJobLogsResponse{Data: chunk.Data, NextOffset: chunk.NextOffset, Complete: chunk.Complete, Truncated: chunk.Truncated}); err != nil {
			return err
		}
		offset = chunk.NextOffset
		if chunk.Complete || (!r.Msg.Follow && len(chunk.Data) == 0) {
			return nil
		}
		if len(chunk.Data) > 0 {
			continue
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return rpcError(ctx.Err())
		case <-timer.C:
		}
	}
}
