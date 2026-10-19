package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
	"rsc.io/script/scripttest"
)

// extensionService deliberately accepts any task, like an old server ignoring
// unknown fields. The client must refuse submission when discovery is insufficient.
type extensionService struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu          sync.Mutex
	target      string
	targets     []*v1.Target
	infoErr     error
	calls       []string
	plans       []*v1.Task
	submissions []*v1.SubmitJobRequest
}

func (s *extensionService) PlanTask(_ context.Context, r *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header().Get("Authorization") != "Bearer extension-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	s.calls = append(s.calls, "plan")
	s.plans = append(s.plans, proto.Clone(r.Msg.Task).(*v1.Task))
	return connect.NewResponse(&v1.PlanTaskResponse{Target: s.target}), nil
}
func (s *extensionService) GetServerInfo(_ context.Context, r *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header().Get("Authorization") != "Bearer extension-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	s.calls = append(s.calls, "info")
	if s.infoErr != nil {
		return nil, s.infoErr
	}
	return connect.NewResponse(&v1.GetServerInfoResponse{Targets: s.targets}), nil
}
func (s *extensionService) SubmitJob(_ context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header().Get("Authorization") != "Bearer extension-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	s.calls = append(s.calls, "submit")
	s.submissions = append(s.submissions, proto.Clone(r.Msg).(*v1.SubmitJobRequest))
	return connect.NewResponse(&v1.SubmitJobResponse{Job: &v1.Job{Id: "job", Target: s.target, Task: r.Msg.Task, State: v1.JobState_JOB_STATE_SUCCEEDED}}), nil
}
func serveExtensions(t *testing.T, s *extensionService) *httptest.Server {
	t.Helper()
	path, h := skyv1connect.NewControlServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

const extensionYAML = `argv: [program, 雪]
env: {A: B}
cpu_millis: 100
memory_bytes: 1024
cpu_limit_millis: 200
memory_limit_bytes: 2048
target_labels: {pool: batch}
node_selector: {zone: west}
`

func TestTaskPreflight(t *testing.T) {
	all := []v1.Capability{1, 2, 3, 4}
	for _, tc := range []struct {
		name    string
		targets []*v1.Target
		infoErr error
		target  string
		code    connect.Code
	}{
		{name: "supported", target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: all}}},
		{name: "legacy", target: "chosen", infoErr: connect.NewError(connect.CodeUnimplemented, errors.New("old server")), code: connect.CodeUnimplemented},
		{name: "unavailable", target: "chosen", infoErr: connect.NewError(connect.CodeUnavailable, errors.New("unavailable")), code: connect.CodeUnavailable},
		{name: "missing limits", target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: []v1.Capability{1, 3, 4}}}, code: connect.CodeFailedPrecondition},
		{name: "missing selector", target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: []v1.Capability{1, 2, 4}}}, code: connect.CodeFailedPrecondition},
		{name: "missing labels", target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: []v1.Capability{1, 2, 3}}}, code: connect.CodeFailedPrecondition},
		{name: "another target", target: "chosen", targets: []*v1.Target{{Name: "other", Capabilities: all}, {Name: "chosen", Capabilities: []v1.Capability{99}}}, code: connect.CodeFailedPrecondition},
		{name: "absent", target: "chosen", code: connect.CodeNotFound},
		{name: "duplicate", target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: all}, {Name: "chosen", Capabilities: all}}, code: connect.CodeFailedPrecondition},
		{name: "empty planned target", code: connect.CodeFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, command := range []string{"plan", "run"} {
				t.Run(command, func(t *testing.T) {
					s := &extensionService{target: tc.target, targets: tc.targets, infoErr: tc.infoErr}
					srv := serveExtensions(t, s)
					file := filepath.Join(t.TempDir(), "task.yaml")
					if err := os.WriteFile(file, []byte(extensionYAML), 0600); err != nil {
						t.Fatal(err)
					}
					args := []string{"--endpoint", srv.URL, "--token", "extension-token", "--json", command, "-f", file}
					if command == "run" {
						args = append(args, "--request-id", "stable")
					}
					stdout, _, err := execute(context.Background(), args...)
					if tc.code != 0 {
						if connect.CodeOf(err) != tc.code {
							t.Fatalf("error = %v; want %v", err, tc.code)
						}
						if stdout != "" {
							t.Fatalf("failed preflight wrote stdout: %q", stdout)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					s.mu.Lock()
					defer s.mu.Unlock()
					wantSubmits := 0
					if tc.code == 0 && command == "run" {
						wantSubmits = 1
					}
					if len(s.submissions) != wantSubmits {
						t.Fatalf("submissions = %d; want %d", len(s.submissions), wantSubmits)
					}
					if wantSubmits == 1 {
						if !proto.Equal(s.plans[0], s.submissions[0].Task) || s.submissions[0].Task.Target != "" || s.submissions[0].RequestId != "stable" {
							t.Fatalf("submission mutated: %v", s.submissions[0])
						}
						if len(s.calls) != 3 || s.calls[0] != "plan" || s.calls[1] != "info" || s.calls[2] != "submit" {
							t.Fatalf("RPC order = %v", s.calls)
						}
					}
				})
			}
		})
	}
}

func TestDefaultTaskLegacyServer(t *testing.T) {
	s := &extensionService{target: "chosen", infoErr: connect.NewError(connect.CodeUnimplemented, errors.New("old server"))}
	srv := serveExtensions(t, s)
	file := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(file, []byte("argv: [program]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", "extension-token", "run", "-f", file, "--request-id", "old"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) != 1 || s.calls[0] != "submit" {
		t.Fatalf("default task required preflight: %v", s.calls)
	}
}

func TestExtensionScripts(t *testing.T) {
	s := &extensionService{target: "chosen", targets: []*v1.Target{{Name: "chosen", Capabilities: []v1.Capability{1, 2, 3, 4}}}}
	srv := serveExtensions(t, s)
	legacy := &extensionService{target: "chosen", infoErr: connect.NewError(connect.CodeUnimplemented, errors.New("old server"))}
	oldServer := serveExtensions(t, legacy)
	// scripttest runs parallel children; wait for their group before inspecting calls.
	t.Run("scripts", func(t *testing.T) {
		scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=extension-token", "OLD_SERVER=" + oldServer.URL}, "testdata/extensions/*.txt")
	})
	legacy.mu.Lock()
	defer legacy.mu.Unlock()
	if len(legacy.submissions) != 0 || len(legacy.plans) != 2 {
		t.Fatalf("old-server plans/submissions = %d/%d", len(legacy.plans), len(legacy.submissions))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.submissions) != 2 {
		t.Fatalf("submissions = %d", len(s.submissions))
	}
	if !proto.Equal(s.submissions[0], s.submissions[1]) {
		t.Fatal("retry changed original task or request ID")
	}
	want := &v1.Task{Argv: []string{"program", "雪"}, Env: map[string]string{"A": "B"}, Target: "chosen", CpuMillis: 100, MemoryBytes: 1024, CpuLimitMillis: 200, MemoryLimitBytes: 2048, TargetLabels: map[string]string{"pool": "batch"}, NodeSelector: map[string]string{"zone": "west"}}
	if !proto.Equal(s.submissions[0].Task, want) {
		t.Fatalf("task = %v; want %v", s.submissions[0].Task, want)
	}
}
