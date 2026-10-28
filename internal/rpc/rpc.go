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
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type handler struct {
	control *control.Control
	engine  *auth.Engine
	// environments is the Controller this handler serves, or nil. Only a
	// non-nil one advertises any environment capability.
	environments *environmentcontrol.Controller
	streams      chan struct{}
	denialMu     sync.Mutex
	lastDenial   time.Time
}

type actorKey struct{}
type responseKey struct{}

// NewHandler authenticates and serves the control and workflow RPCs. Transport
// encryption is the caller's responsibility. The control and handler must share
// one Engine.
func NewHandler(c *control.Control, engine *auth.Engine) *HTTPHandler {
	return newHandler(c, engine, nil)
}

// NewEnvironmentHandler is NewHandler that also serves EnvironmentService
// over environments, which must share c's Store and Engine. Its calls share
// the handler's authentication. It also serves the authenticated HTTP route
// /environments/<id>/forward/<application-path>. A nil environments is
// NewHandler.
//
// With both mounted, GetServerInfo reports CAPABILITY_ENVIRONMENTS_V1 to a
// scoped credential, which environment creation requires, and each target
// reporting backend.Target.SupportsPersistentEnvironments, and with
// Control's artifact service SupportsEnvironmentArtifacts, reports the
// matching target capability; the artifact capability also requires
// environments.ArtifactsEnabled. Capabilities are configured support, not
// capacity, readiness or native qualification. NewHandler reports none of
// them, whatever its targets claim.
func NewEnvironmentHandler(c *control.Control, engine *auth.Engine, environments *environmentcontrol.Controller) *HTTPHandler {
	return newHandler(c, engine, environments)
}

func newHandler(c *control.Control, engine *auth.Engine, environments *environmentcontrol.Controller) *HTTPHandler {
	h := &handler{control: c, engine: engine, environments: environments, streams: make(chan struct{}, 32)}
	path, service := skyv1connect.NewControlServiceHandler(h, connect.WithReadMaxBytes(512<<10), connect.WithConditionalHandlerOptions(func(info connect.Spec) []connect.HandlerOption {
		if artifactProcedure(info.Procedure) && info.Procedure != skyv1connect.ControlServiceSubmitWorkspaceJobProcedure {
			limit := 16 << 10
			if info.Procedure == skyv1connect.ControlServiceWriteArtifactChunkProcedure {
				limit = 96 << 10
			}
			return []connect.HandlerOption{connect.WithReadMaxBytes(limit)}
		}
		if info.Procedure != skyv1connect.ControlServiceQuoteTaskProcedure {
			return nil
		}
		return []connect.HandlerOption{connect.WithCodec(quoteJSONCodec{name: "json"}), connect.WithCodec(quoteJSONCodec{name: "json; charset=utf-8"})}
	}))
	remotePath, remoteService := skyv1connect.NewRemoteDataServiceHandler(h,
		connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10),
		connect.WithConditionalHandlerOptions(func(info connect.Spec) []connect.HandlerOption {
			if info.Procedure == skyv1connect.RemoteDataServiceWriteOutputProcedure {
				return nil
			}
			return []connect.HandlerOption{connect.WithReadMaxBytes(16 << 10)}
		}))
	workflowPath, workflowService := skyv1connect.NewWorkflowServiceHandler(h, connect.WithReadMaxBytes(512<<10))
	mux := http.NewServeMux()
	mux.Handle(path, service)
	mux.Handle(workflowPath, workflowService)
	if environments != nil {
		mux.Handle(newEnvironmentService(environments))
	}
	lifetime := &HTTPHandler{owners: make(map[*artifactResponse]struct{}), terminal: make(chan struct{})}
	lifetime.serve = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.serveRemoteHTTP(lifetime, remotePath, remoteService, w, r) {
			return
		}
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
		// The forward route is served here, outside mux, which would clean
		// and redirect its path.
		if environments != nil && forwardRoute(r) {
			serveEnvironmentForward(environments, w, r.WithContext(ctx))
			return
		}
		if artifactProcedure(r.URL.Path) {
			action := artifactAction(r.URL.Path)
			if _, err := h.artifactActor(ctx, action); err != nil {
				code := http.StatusForbidden
				if errors.Is(err, auth.ErrUnauthenticated) {
					code = http.StatusUnauthorized
				}
				if errors.Is(err, auth.ErrUnavailable) {
					code = http.StatusServiceUnavailable
				}
				http.Error(w, "artifact authorization unavailable", code)
				return
			}
			w = &artifactWriter{ResponseWriter: w, ctx: ctx, handler: h, action: action}
			owner, err := lifetime.admit(c, r.Body, w)
			if err != nil {
				code := http.StatusServiceUnavailable
				if errors.Is(err, control.ErrArtifactBusy) {
					code = http.StatusTooManyRequests
				}
				http.Error(w, "artifact admission unavailable", code)
				return
			}
			// Connect and our finisher share this cached admitted-body result.
			// net/http already saved its own transport body before handler
			// entry; replacing r.Body does not intercept that separate Close.
			r.Body = owner.body
			ctx = context.WithValue(ctx, artifactResponseKey{}, owner)
			defer owner.finish()
		}
		response := &streamResponse{controller: http.NewResponseController(w), done: make(chan struct{})}
		ctx = context.WithValue(ctx, responseKey{}, response)
		// Connect writes and flushes final trailers after the RPC method returns.
		// Stream cleanup belongs to this enclosing HTTP lifetime.
		defer response.finish()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	return lifetime
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
	return backend.Task{Argv: slices.Clone(t.Argv), Env: maps.Clone(t.Env), Directory: t.Directory, Image: t.Image, CPUMillis: t.CpuMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds, CPULimitMillis: t.CpuLimitMillis, MemoryLimitBytes: t.MemoryLimitBytes, TargetLabels: maps.Clone(t.TargetLabels), NodeSelector: maps.Clone(t.NodeSelector), Retry: retry(t.Retry), Workdir: taskWorkdir(t.Workdir), Setup: slices.Clone(t.Setup), Outputs: slices.Clone(t.Outputs), DataMounts: taskDataMounts(t.DataMounts)}
}
func wireTask(t backend.Task) *v1.Task {
	return &v1.Task{Argv: slices.Clone(t.Argv), Env: maps.Clone(t.Env), Directory: t.Directory, Image: t.Image, CpuMillis: t.CPUMillis, MemoryBytes: t.MemoryBytes, Accelerator: t.Accelerator, AcceleratorCount: t.AcceleratorCount, Target: t.Target, TimeoutSeconds: t.TimeoutSeconds, CpuLimitMillis: t.CPULimitMillis, MemoryLimitBytes: t.MemoryLimitBytes, TargetLabels: maps.Clone(t.TargetLabels), NodeSelector: maps.Clone(t.NodeSelector), Retry: wireRetry(t.Retry), Workdir: wireTaskWorkdir(t.Workdir), Setup: slices.Clone(t.Setup), Outputs: slices.Clone(t.Outputs), DataMounts: wireDataMounts(t.DataMounts)}
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
	if retained := retentionError(err); retained != nil {
		return retained
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
	case errors.Is(err, control.ErrQuoteMetadata), errors.Is(err, control.ErrUnsupported), errors.Is(err, control.ErrLogsUnavailable), errors.Is(err, store.ErrRetentionDisabled):
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
	if r == nil || r.Msg == nil || !validDataTaskUTF8(r.Msg.Task) {
		if _, err := h.engine.Check(requestActor(ctx), auth.Read); err != nil {
			return nil, rpcError(err)
		}
		return nil, rpcError(control.ErrInvalid)
	}
	if hasWorkspaceTask(r.Msg.Task) && !h.remotePlanTask(ctx, r.Msg.Task) {
		if _, err := h.engine.Check(requestActor(ctx), auth.Read); err != nil {
			return nil, rpcError(err)
		}
		return nil, rpcError(control.ErrUnsupported)
	}
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
	if hasWorkspaceTask(r.Msg.Task) {
		if _, err := h.engine.Check(requestActor(ctx), auth.Mutate); err != nil {
			return nil, rpcError(err)
		}
		return nil, rpcError(control.ErrUnsupported)
	}
	j, err := h.control.Submit(requestActor(ctx), r.Msg.RequestId, task(r.Msg.Task))
	h.record(ctx, "submit", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := h.wireJob(ctx, j)
	if err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.SubmitJobResponse{Job: out}), nil
}
func (h *handler) GetJob(ctx context.Context, r *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	j, err := h.control.Get(requestActor(ctx), r.Msg.Id)
	h.record(ctx, "get", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := h.wireJob(ctx, j)
	if err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.GetJobResponse{Job: out}), nil
}
func (h *handler) ListJobs(ctx context.Context, r *connect.Request[v1.ListJobsRequest]) (*connect.Response[v1.ListJobsResponse], error) {
	jobs, next, err := h.control.List(requestActor(ctx), int(r.Msg.PageSize), r.Msg.PageToken)
	h.record(ctx, "list", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	out := &v1.ListJobsResponse{NextPageToken: next}
	for _, j := range jobs {
		projected, err := h.wireJob(ctx, j)
		if err != nil {
			return nil, artifactError(err)
		}
		out.Jobs = append(out.Jobs, projected)
	}
	return connect.NewResponse(out), nil
}
func (h *handler) CancelJob(ctx context.Context, r *connect.Request[v1.CancelJobRequest]) (*connect.Response[v1.CancelJobResponse], error) {
	j, err := h.control.Cancel(requestActor(ctx), r.Msg.Id)
	h.record(ctx, "cancel", j.ID, err)
	if err != nil {
		return nil, rpcError(err)
	}
	out, err := h.wireJob(ctx, j)
	if err != nil {
		return nil, artifactError(err)
	}
	return connect.NewResponse(&v1.CancelJobResponse{Job: out}), nil
}
func (h *handler) ListTargets(ctx context.Context, r *connect.Request[v1.ListTargetsRequest]) (*connect.Response[v1.ListTargetsResponse], error) {
	out := &v1.ListTargetsResponse{}
	targets, err := h.control.Targets(requestActor(ctx))
	h.record(ctx, "targets", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	for _, t := range targets {
		out.Targets = append(out.Targets, h.target(ctx, t))
	}
	return connect.NewResponse(out), nil
}

// environmentsServed reports whether h serves EnvironmentService and its
// forward route to the caller of ctx, a scoped credential.
func (h *handler) environmentsServed(ctx context.Context) bool {
	return h.environments != nil && requestActor(ctx).TokenID() != ""
}

// target projects t for the caller of ctx, with environment support only
// while h serves environments.
func (h *handler) target(ctx context.Context, t backend.Target) *v1.Target {
	out := h.wireTarget(t)
	if !h.environmentsServed(ctx) || !t.SupportsPersistentEnvironments {
		return out
	}
	out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_PERSISTENT_ENVIRONMENTS_V1)
	if t.SupportsEnvironmentArtifacts && h.control.ArtifactsEnabled() && h.environments.ArtifactsEnabled() {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_ENVIRONMENT_ARTIFACTS_V1)
	}
	return out
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
	out := &v1.Target{Name: t.Name, Kind: t.Kind, Capabilities: capabilities, Labels: maps.Clone(t.Labels), AcceleratorNames: slices.Clone(t.AcceleratorNames)}
	addRemoteTarget(t, out)
	return out
}

func (h *handler) GetServerInfo(ctx context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	out := &v1.GetServerInfoResponse{}
	if h.control.ArtifactsEnabled() {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1)
	}
	targets, err := h.control.Targets(requestActor(ctx))
	h.record(ctx, "info", "", err)
	if err != nil {
		return nil, rpcError(err)
	}
	if h.control.ArtifactsEnabled() && requestActor(ctx).TokenID() != "" && remoteRouteAvailable(targets) {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_REMOTE_DATA_TRANSFER_V1)
	}
	// NewHandler always mounts the four workflow methods on Control. Accepting a
	// workflow captures file authority, which a legacy credential cannot supply.
	if requestActor(ctx).TokenID() != "" {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_STATIC_WORKFLOWS_V1)
	}
	if h.environmentsServed(ctx) {
		out.Capabilities = append(out.Capabilities, v1.Capability_CAPABILITY_ENVIRONMENTS_V1)
	}
	for _, t := range targets {
		out.Targets = append(out.Targets, h.target(ctx, t))
	}
	return connect.NewResponse(out), nil
}

// Task conversion preserves every workspace value. Validation and the explicit
// workspace submit path are separate; old submit/plan methods never drop them.
func taskWorkdir(ref *v1.ArtifactRef) *artifact.Ref {
	if ref == nil {
		return nil
	}
	return &artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}
}

func wireTaskWorkdir(ref *artifact.Ref) *v1.ArtifactRef {
	if ref == nil {
		return nil
	}
	return &v1.ArtifactRef{Id: ref.ID, Sha256: ref.SHA256, Size: ref.Size}
}

func hasWorkspaceTask(value *v1.Task) bool {
	return value != nil && (value.Workdir != nil || len(value.Setup) != 0 || len(value.Outputs) != 0 || len(value.DataMounts) != 0)
}
