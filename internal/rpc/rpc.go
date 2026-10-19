package rpc

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type handler struct {
	control    *control.Control
	engine     *auth.Engine
	streams    chan struct{}
	denialMu   sync.Mutex
	lastDenial time.Time
}

type actorKey struct{}
type responseKey struct{}

// NewHandler authenticates and serves the control RPCs. Transport encryption is
// the caller's responsibility. The control and handler must share one Engine.
func NewHandler(c *control.Control, engine *auth.Engine) http.Handler {
	h := &handler{control: c, engine: engine, streams: make(chan struct{}, 32)}
	path, service := skyv1connect.NewControlServiceHandler(h, connect.WithReadMaxBytes(512<<10))
	mux := http.NewServeMux()
	mux.Handle(path, service)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actor, err := engine.Authenticate(r.Header.Get("Authorization"))
		if err != nil {
			h.denialMu.Lock()
			record := time.Since(h.lastDenial) >= time.Second
			if record {
				h.lastDenial = time.Now()
			}
			h.denialMu.Unlock()
			if record {
				_ = c.RecordAccess(auth.Actor{}, "authenticate", "", "denied")
			}
			code, message := http.StatusUnauthorized, "unauthenticated"
			if errors.Is(err, auth.ErrUnavailable) {
				code, message = http.StatusServiceUnavailable, "authentication unavailable"
			}
			http.Error(w, message, code)
			return
		}
		ctx := context.WithValue(r.Context(), actorKey{}, actor)
		response := &streamResponse{controller: http.NewResponseController(w), done: make(chan struct{})}
		ctx = context.WithValue(ctx, responseKey{}, response)
		// Connect writes and flushes final trailers after the RPC method returns.
		// Stream cleanup belongs to this enclosing HTTP lifetime.
		defer response.finish()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func requestActor(ctx context.Context) auth.Actor {
	actor, _ := ctx.Value(actorKey{}).(auth.Actor)
	return actor
}

func (h *handler) record(ctx context.Context, operation, resource string, err error) {
	result := "allowed"
	if err != nil {
		resource = ""
		result = "error"
		if errors.Is(err, auth.ErrUnauthenticated) || errors.Is(err, auth.ErrPermissionDenied) || errors.Is(err, store.ErrNotFound) {
			result = "denied"
		}
	}
	_ = h.control.RecordAccess(requestActor(ctx), operation, resource, result)
}

func task(t *v1.Task) backend.Task {
	if t == nil {
		return backend.Task{}
	}
	return backend.Task{Argv: slices.Clone(t.Argv), Env: maps.Clone(t.Env), Directory: t.Directory, Image: t.Image, CPUMillis: t.CpuMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds, CPULimitMillis: t.CpuLimitMillis, MemoryLimitBytes: t.MemoryLimitBytes, TargetLabels: maps.Clone(t.TargetLabels), NodeSelector: maps.Clone(t.NodeSelector), Retry: retry(t.Retry)}
}
func wireTask(t backend.Task) *v1.Task {
	return &v1.Task{Argv: slices.Clone(t.Argv), Env: maps.Clone(t.Env), Directory: t.Directory, Image: t.Image, CpuMillis: t.CPUMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds, CpuLimitMillis: t.CPULimitMillis, MemoryLimitBytes: t.MemoryLimitBytes, TargetLabels: maps.Clone(t.TargetLabels), NodeSelector: maps.Clone(t.NodeSelector), Retry: wireRetry(t.Retry)}
}

var states = map[backend.State]v1.JobState{backend.Queued: v1.JobState_JOB_STATE_QUEUED, backend.Starting: v1.JobState_JOB_STATE_STARTING, backend.Running: v1.JobState_JOB_STATE_RUNNING, backend.Succeeded: v1.JobState_JOB_STATE_SUCCEEDED, backend.Failed: v1.JobState_JOB_STATE_FAILED, backend.Canceled: v1.JobState_JOB_STATE_CANCELED, backend.Unknown: v1.JobState_JOB_STATE_UNKNOWN, backend.Retrying: v1.JobState_JOB_STATE_RETRYING}

func job(j backend.Job) *v1.Job {
	out := &v1.Job{Id: j.ID, Task: wireTask(j.Task), Target: j.Target, State: states[j.Status.State], CancelRequested: j.CancelRequested, Reason: j.Status.Reason, ExitCode: exitCode(j.Status.ExitCode), CreatedAt: timestamppb.New(j.CreatedAt), UpdatedAt: timestamppb.New(j.UpdatedAt)}
	for i, a := range j.Attempts {
		out.Attempts = append(out.Attempts, &v1.Attempt{Id: a.ID, Ordinal: uint32(i + 1), State: states[a.Status.State], Reason: a.Status.Reason, ExitCode: exitCode(a.Status.ExitCode), CreatedAt: timestamppb.New(a.CreatedAt), StartingAt: timestamp(a.StartingAt), FinishedAt: timestamp(a.FinishedAt)})
	}
	out.NextAttemptAt = timestamp(j.NextAttemptAt)
	return out
}
func rpcError(err error) error {
	if err == nil {
		return nil
	}
	code := connect.CodeInternal
	switch {
	case errors.Is(err, auth.ErrUnauthenticated):
		code = connect.CodeUnauthenticated
	case errors.Is(err, auth.ErrPermissionDenied):
		code = connect.CodePermissionDenied
	case errors.Is(err, auth.ErrUnavailable):
		code = connect.CodeUnavailable
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
	name, rejections, err := h.control.Plan(requestActor(ctx), task(r.Msg.Task))
	h.record(ctx, "plan", "", err)
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
	return h.submit(ctx, r)
}
func (h *handler) SubmitRetryJob(ctx context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	return h.submit(ctx, r)
}
func (h *handler) submit(ctx context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	j, err := h.control.Submit(requestActor(ctx), r.Msg.RequestId, task(r.Msg.Task))
	h.record(ctx, "submit", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.SubmitJobResponse{Job: job(j)}), nil
}
func (h *handler) GetJob(ctx context.Context, r *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	j, err := h.control.Get(requestActor(ctx), r.Msg.Id)
	h.record(ctx, "get", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.GetJobResponse{Job: job(j)}), nil
}
func (h *handler) ListJobs(ctx context.Context, r *connect.Request[v1.ListJobsRequest]) (*connect.Response[v1.ListJobsResponse], error) {
	jobs, next, err := h.control.List(requestActor(ctx), int(r.Msg.PageSize), r.Msg.PageToken)
	h.record(ctx, "list", "", err)
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
	j, err := h.control.Cancel(requestActor(ctx), r.Msg.Id)
	h.record(ctx, "cancel", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	return connect.NewResponse(&v1.CancelJobResponse{Job: job(j)}), nil
}
func (h *handler) ListTargets(ctx context.Context, r *connect.Request[v1.ListTargetsRequest]) (*connect.Response[v1.ListTargetsResponse], error) {
	out := &v1.ListTargetsResponse{}
	targets, err := h.control.Targets(requestActor(ctx))
	h.record(ctx, "targets", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	for _, t := range targets {
		out.Targets = append(out.Targets, wireTarget(t))
	}
	return connect.NewResponse(out), nil
}

// wireTarget reports configured support, not available capacity.
func wireTarget(t backend.Target) *v1.Target {
	capabilities := []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1}
	if t.SupportsResourceLimits {
		capabilities = append(capabilities, v1.Capability_CAPABILITY_RESOURCE_LIMITS_V1)
	}
	if t.SupportsNodeSelector {
		capabilities = append(capabilities, v1.Capability_CAPABILITY_NODE_SELECTOR_V1)
	}
	capabilities = append(capabilities, v1.Capability_CAPABILITY_TARGET_LABELS_V1)
	if t.SupportsRetry {
		capabilities = append(capabilities, v1.Capability_CAPABILITY_FINITE_JOB_RETRY_V1)
	}
	return &v1.Target{Name: t.Name, Kind: t.Kind, Capabilities: capabilities, Labels: maps.Clone(t.Labels), AcceleratorNames: slices.Clone(t.AcceleratorNames)}
}

func (h *handler) GetServerInfo(ctx context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	out := &v1.GetServerInfoResponse{}
	targets, err := h.control.Targets(requestActor(ctx))
	h.record(ctx, "info", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	for _, t := range targets {
		out.Targets = append(out.Targets, wireTarget(t))
	}
	return connect.NewResponse(out), nil
}
