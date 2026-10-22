package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// dataProtocolPeer is a generated-handler wire oracle. QUEUED replies and its
// in-memory replay map prove no Store, grant, backend qualification or execution.
type dataProtocolPeer struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu       sync.Mutex
	mode     string
	calls    map[string]int
	requests []*v1.SubmitDataJobRequest
	info     *v1.GetServerInfoResponse
	accepted map[string]*v1.Job
}

func newDataProtocolPeer(t *testing.T) (*dataProtocolPeer, *httptest.Server) {
	t.Helper()
	p := &dataProtocolPeer{mode: "qualified", calls: make(map[string]int), accepted: make(map[string]*v1.Job)}
	auth := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, r connect.AnyRequest) (connect.AnyResponse, error) {
			if r.Header().Get("Authorization") != "Bearer data-token" {
				return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("missing data token"))
			}
			return next(ctx, r)
		}
	})
	_, handler := skyv1connect.NewControlServiceHandler(p, connect.WithInterceptors(auth))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.calls[filepath.Base(r.URL.Path)]++
		p.mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return p, server
}

func dataProtocolMode(mode string) bool {
	switch mode {
	case "qualified", "missing-endpoint", "missing-target", "mounts-off", "retry-off", "missing-policy", "invalid-policy", "duplicate-target", "unimplemented", "submit-unimplemented", "unauthenticated", "unavailable", "drift":
		return true
	}
	return false
}

func (p *dataProtocolPeer) setMode(mode string) { p.mu.Lock(); p.mode = mode; p.mu.Unlock() }
func (p *dataProtocolPeer) count(method string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if method != "ALL" {
		return p.calls[method]
	}
	var total int
	for _, n := range p.calls {
		total += n
	}
	return total
}
func (p *dataProtocolPeer) snapshotRequests() []*v1.SubmitDataJobRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*v1.SubmitDataJobRequest, len(p.requests))
	for i, request := range p.requests {
		out[i] = proto.Clone(request).(*v1.SubmitDataJobRequest)
	}
	return out
}

func dataProtocolInfo() *v1.GetServerInfoResponse {
	return &v1.GetServerInfoResponse{
		Capabilities: []v1.Capability{v1.Capability_CAPABILITY_REMOTE_DATA_TRANSFER_V1, v1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1},
		Targets: []*v1.Target{
			{Name: "data", Kind: "protocol-only", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1, v1.Capability_CAPABILITY_REMOTE_DATA_V1, v1.Capability_CAPABILITY_DATA_MOUNTS_V1, v1.Capability_CAPABILITY_FINITE_JOB_RETRY_V1, v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1}, DataPolicy: &v1.DataPolicy{StageSeconds: 600, CollectionSeconds: 600, DefaultTaskSeconds: 86400, MaxTaskSeconds: 604800, ShutdownSeconds: 120}},
			{Name: "local", Kind: "protocol-only", Capabilities: []v1.Capability{v1.Capability_CAPABILITY_FINITE_JOBS_V1, v1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1, v1.Capability_CAPABILITY_FINITE_JOB_RETRY_V1}},
		},
	}
}

func (p *dataProtocolPeer) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !dataProtocolMode(p.mode) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown protocol-only scenario"))
	}
	switch p.mode {
	case "unimplemented":
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("old discovery"))
	case "unauthenticated":
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("revoked credential"))
	case "unavailable":
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("discovery transport unavailable"))
	}
	info := dataProtocolInfo()
	if p.info != nil {
		info = proto.Clone(p.info).(*v1.GetServerInfoResponse)
	}
	switch p.mode {
	case "missing-endpoint":
		info.Capabilities = nil
	case "missing-target", "drift":
		info.Targets = nil
	case "mounts-off", "retry-off":
		remove := v1.Capability_CAPABILITY_DATA_MOUNTS_V1
		if p.mode == "retry-off" {
			remove = v1.Capability_CAPABILITY_REMOTE_DATA_RETRY_V1
		}
		var caps []v1.Capability
		for _, c := range info.Targets[0].Capabilities {
			if c != remove {
				caps = append(caps, c)
			}
		}
		info.Targets[0].Capabilities = caps
	case "missing-policy":
		info.Targets[0].DataPolicy = nil
	case "invalid-policy":
		info.Targets[0].DataPolicy.StageSeconds = 0
	case "duplicate-target":
		info.Targets = append(info.Targets, proto.Clone(info.Targets[0]).(*v1.Target))
	}
	return connect.NewResponse(info), nil
}

func (*dataProtocolPeer) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	return connect.NewResponse(&v1.GetRetentionResponse{Enabled: false}), nil
}
func (*dataProtocolPeer) PlanTask(_ context.Context, r *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	return connect.NewResponse(&v1.PlanTaskResponse{Target: r.Msg.Task.Target}), nil
}
func dataProtocolJob(method string, task *v1.Task) *v1.Job {
	return &v1.Job{Id: method + "-job", Target: task.Target, Task: proto.Clone(task).(*v1.Task), State: v1.JobState_JOB_STATE_QUEUED}
}
func (p *dataProtocolPeer) SubmitDataJob(_ context.Context, r *connect.Request[v1.SubmitDataJobRequest]) (*connect.Response[v1.SubmitDataJobResponse], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = append(p.requests, proto.Clone(r.Msg).(*v1.SubmitDataJobRequest))
	if !dataProtocolMode(p.mode) {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("unknown protocol-only scenario"))
	}
	if p.mode == "unimplemented" || p.mode == "submit-unimplemented" {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("old data endpoint"))
	}
	if r.Msg.Task == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("missing task"))
	}
	key := fmt.Sprintf("%s:%d/%s", r.Msg.Domain.GetLineage(), r.Msg.Domain.GetEpoch(), r.Msg.RequestId)
	if job := p.accepted[key]; job != nil {
		if !proto.Equal(job.Task, r.Msg.Task) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("request task conflict"))
		}
		return connect.NewResponse(&v1.SubmitDataJobResponse{Job: proto.Clone(job).(*v1.Job)}), nil
	}
	if p.mode == "drift" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("current target unavailable"))
	}
	job := dataProtocolJob("data", r.Msg.Task)
	p.accepted[key] = job
	return connect.NewResponse(&v1.SubmitDataJobResponse{Job: proto.Clone(job).(*v1.Job)}), nil
}
func (*dataProtocolPeer) SubmitJob(_ context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	return connect.NewResponse(&v1.SubmitJobResponse{Job: dataProtocolJob("ordinary", r.Msg.Task)}), nil
}
func (*dataProtocolPeer) SubmitRetryJob(_ context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	return connect.NewResponse(&v1.SubmitJobResponse{Job: dataProtocolJob("retry", r.Msg.Task)}), nil
}
func (*dataProtocolPeer) SubmitRetainedJob(_ context.Context, r *connect.Request[v1.SubmitRetainedJobRequest]) (*connect.Response[v1.SubmitRetainedJobResponse], error) {
	return connect.NewResponse(&v1.SubmitRetainedJobResponse{Job: dataProtocolJob("retained", r.Msg.Task)}), nil
}
func (*dataProtocolPeer) SubmitWorkspaceJob(_ context.Context, r *connect.Request[v1.SubmitWorkspaceJobRequest]) (*connect.Response[v1.SubmitWorkspaceJobResponse], error) {
	return connect.NewResponse(&v1.SubmitWorkspaceJobResponse{Job: dataProtocolJob("workspace", r.Msg.Task)}), nil
}

func TestDataScripts(t *testing.T) {
	for _, name := range []string{"validation", "preflight", "route", "replay", "old_server", "local_regression"} {
		t.Run(name, func(t *testing.T) {
			peer, server := newDataProtocolPeer(t)
			engine := scriptEngine(t)
			engine.Cmds["data-peer"] = script.Command(script.CmdUsage{Summary: "select protocol-only discovery behavior", Args: "MODE"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if len(args) != 1 || !dataProtocolMode(args[0]) {
					return nil, errors.New("data-peer requires a known protocol-only MODE")
				}
				peer.setMode(args[0])
				return nil, nil
			})
			engine.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check actual received HTTP method count", Args: "METHOD COUNT"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if len(args) != 2 {
					return nil, errors.New("calls requires METHOD COUNT")
				}
				want, err := strconv.Atoi(args[1])
				if err != nil {
					return nil, err
				}
				if got := peer.count(args[0]); got != want {
					return nil, fmt.Errorf("%s calls=%d, want %d", args[0], got, want)
				}
				return nil, nil
			})
			engine.Cmds["data-wire"] = script.Command(script.CmdUsage{Summary: "check independently specified ordered mount request", Args: "INDEX ID DOMAIN"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
				if len(args) != 3 {
					return nil, errors.New("data-wire requires INDEX ID DOMAIN")
				}
				i, err := strconv.Atoi(args[0])
				if err != nil {
					return nil, err
				}
				requests := peer.snapshotRequests()
				if i < 0 || i >= len(requests) {
					return nil, errors.New("missing actual data request")
				}
				r := requests[i]
				want := &v1.Task{Argv: []string{"echo", "hello"}, Target: "data", Directory: "/task", DataMounts: []*v1.DataMount{{Reference: "dataset-z", Destination: "/mnt/skygo/z", ReadOnly: false}, {Reference: "dataset-a", Destination: "/mnt/skygo/a", ReadOnly: true}}}
				if !proto.Equal(r.Task, want) || r.RequestId != args[1] {
					return nil, fmt.Errorf("actual data request differs: %v", r)
				}
				domain := fmt.Sprintf("%s:%d", r.Domain.GetLineage(), r.Domain.GetEpoch())
				if args[2] == "none" {
					if r.Domain != nil {
						return nil, errors.New("unexpected domain")
					}
				} else if domain != args[2] {
					return nil, fmt.Errorf("domain=%s, want %s", domain, args[2])
				}
				return nil, nil
			})
			scripttest.Test(t, context.Background(), engine, []string{"SKYGO_ENDPOINT=" + server.URL, "SKYGO_TOKEN=data-token"}, "testdata/data/"+name+".txt")
		})
	}
}
