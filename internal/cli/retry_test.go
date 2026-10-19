package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

// retryService rejects planning and legacy submission so the real HTTP client's
// method choice cannot be hidden by a permissive server implementation.
type retryService struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu        sync.Mutex
	calls     []string
	requests  []*v1.SubmitJobRequest
	submitErr error
	polls     int
	logs      int
	history   bool
	attempts  []uint32
}

func (s *retryService) PlanTask(context.Context, *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "plan")
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("route removed"))
}
func (s *retryService) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "info")
	return nil, connect.NewError(connect.CodeUnavailable, errors.New("route removed"))
}
func (s *retryService) SubmitJob(context.Context, *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "legacy")
	return nil, errors.New("legacy submission forbidden")
}
func (s *retryService) SubmitRetryJob(_ context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "retry")
	if r.Header().Get("Authorization") != "Bearer retry-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
	}
	s.requests = append(s.requests, proto.Clone(r.Msg).(*v1.SubmitJobRequest))
	if s.submitErr != nil {
		return nil, s.submitErr
	}
	return connect.NewResponse(&v1.SubmitJobResponse{Job: &v1.Job{Id: "job", Task: r.Msg.Task, Target: "bound", State: v1.JobState_JOB_STATE_RETRYING}}), nil
}
func (s *retryService) GetJob(context.Context, *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, "get")
	s.polls++
	j := &v1.Job{Id: "job", Target: "bound", State: v1.JobState_JOB_STATE_RETRYING}
	if s.polls > 1 {
		j.State = v1.JobState_JOB_STATE_SUCCEEDED
	}
	if s.history {
		j.Attempts = []*v1.Attempt{{Id: "first", Ordinal: 1}, {Id: "second", Ordinal: 2}}
	}
	return connect.NewResponse(&v1.GetJobResponse{Job: j}), nil
}
func (s *retryService) StreamJobLogs(_ context.Context, r *connect.Request[v1.StreamJobLogsRequest], out *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	s.mu.Lock()
	s.logs++
	s.attempts = append(s.attempts, r.Msg.Attempt)
	s.calls = append(s.calls, "logs")
	s.mu.Unlock()
	return out.Send(&v1.StreamJobLogsResponse{Data: []byte("second\n"), Complete: true, AttemptId: "second"})
}
func serveRetry(t *testing.T, s *retryService) *httptest.Server {
	t.Helper()
	path, h := skyv1connect.NewControlServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}
func retryFile(t *testing.T, text string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(p, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

const retryYAML = "argv: [program]\ncpu_millis: 100\ncpu_limit_millis: 200\nretry:\n  max_attempts: 3\n  exit_codes: [9, 7, 9]\n"

func TestRetrySubmissionPreservesOriginal(t *testing.T) {
	s := &retryService{submitErr: connect.NewError(connect.CodeUnavailable, errors.New("lost reply"))}
	srv := serveRetry(t, s)
	file := retryFile(t, retryYAML)
	args := []string{"--endpoint", srv.URL, "--token", "retry-token", "--json", "run", "-f", file, "--request-id", "stable"}
	out, stderr, err := execute(context.Background(), args...)
	if connect.CodeOf(err) != connect.CodeUnavailable || out != "" || !strings.Contains(stderr, "request-id: stable") {
		t.Fatalf("first = %q,%q,%v", out, stderr, err)
	}
	s.mu.Lock()
	s.submitErr = nil
	s.mu.Unlock()
	if _, _, err := execute(context.Background(), args...); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !reflect.DeepEqual(s.calls, []string{"retry", "retry"}) || len(s.requests) != 2 || !proto.Equal(s.requests[0], s.requests[1]) {
		t.Fatalf("calls=%v requests=%v", s.calls, s.requests)
	}
	got := s.requests[0]
	want := &v1.Task{Argv: []string{"program"}, CpuMillis: 100, CpuLimitMillis: 200, Retry: &v1.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{9, 7, 9}}}
	if got.RequestId != "stable" || !proto.Equal(got.Task, want) {
		t.Fatalf("original request changed: %v", got)
	}
}
func TestRetryNeverFallsBack(t *testing.T) {
	for _, c := range []connect.Code{connect.CodeUnimplemented, connect.CodeUnavailable, connect.CodePermissionDenied} {
		t.Run(c.String(), func(t *testing.T) {
			s := &retryService{submitErr: connect.NewError(c, errors.New("retry unavailable"))}
			srv := serveRetry(t, s)
			_, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", "retry-token", "run", "-f", retryFile(t, retryYAML))
			if connect.CodeOf(err) != c {
				t.Fatalf("error=%v", err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if !reflect.DeepEqual(s.calls, []string{"retry"}) {
				t.Fatalf("calls=%v", s.calls)
			}
		})
	}
}
func TestRetryWait(t *testing.T) {
	s := &retryService{}
	srv := serveRetry(t, s)
	out, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", "retry-token", "run", "-f", retryFile(t, retryYAML), "--wait")
	if err != nil || !strings.Contains(out, "succeeded") {
		t.Fatalf("wait=%q,%v", out, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.polls != 2 {
		t.Fatalf("polls=%d; retrying was treated as terminal", s.polls)
	}
}
func TestRetryYAML(t *testing.T) {
	for _, policy := range []string{
		"{max_attempts: 2, exit_codes: [" + strings.Repeat("7,", 32) + "7]}",
		"{max_attempts: -1, exit_codes: [7]}", "{max_attempts: 4294967296, exit_codes: [7]}", "{max_attempts: 11, exit_codes: [7]}", "{max_attempts: 2, delay_seconds: -1, exit_codes: [7]}", "{max_attempts: 2, delay_seconds: 3601, exit_codes: [7]}", "{max_attempts: 2}", "{max_attempts: 2, exit_codes: [0]}", "{max_attempts: 2, exit_codes: [256]}", "{max_attempts: 2, exit_codes: [7], unknown: true}", "{max_attempts: 1, exit_codes: [7]}",
	} {
		t.Run(policy, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "unexpected request", http.StatusInternalServerError)
			}))
			defer srv.Close()
			_, _, err := execute(context.Background(), "--endpoint", srv.URL, "run", "-f", retryFile(t, "argv: [program]\nretry: "+policy+"\n"))
			if err == nil || (!strings.HasPrefix(err.Error(), "decode task:") && !strings.HasPrefix(err.Error(), "retry:")) {
				t.Fatalf("local retry validation error = %v", err)
			}
			if n := requests.Load(); n != 0 {
				t.Fatalf("invalid retry made %d requests", n)
			}
		})
	}
	for _, policy := range []string{"{}", "{max_attempts: 1}"} {
		t.Run(policy, func(t *testing.T) {
			s := &extensionService{target: "chosen"}
			srv := serveExtensions(t, s)
			_, _, err := execute(context.Background(), "--endpoint", srv.URL, "--token", "extension-token", "run", "-f", retryFile(t, "argv: [program]\nretry: "+policy+"\n"))
			if err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if !reflect.DeepEqual(s.calls, []string{"submit"}) {
				t.Fatalf("implicit retry calls=%v", s.calls)
			}
		})
	}
}
func TestAttemptLogSelection(t *testing.T) {
	for _, tc := range []struct {
		name, flag       string
		history, wantLog bool
	}{{"current", "", false, true}, {"first", "1", false, true}, {"second", "2", true, true}, {"old server", "2", false, false}, {"zero", "0", true, false}, {"negative", "-1", true, false}, {"overflow", "4294967296", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &retryService{history: tc.history}
			srv := serveRetry(t, s)
			args := []string{"--endpoint", srv.URL, "jobs", "logs", "job"}
			if tc.flag != "" {
				args = append(args, "--attempt", tc.flag)
			}
			_, _, err := execute(context.Background(), args...)
			if (err == nil) != tc.wantLog {
				t.Fatalf("error=%v", err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if (s.logs == 1) != tc.wantLog {
				t.Fatalf("stream opens=%d", s.logs)
			}
			if tc.flag == "2" && s.polls != 1 {
				t.Fatalf("history checks=%d", s.polls)
			}
			if tc.wantLog {
				var want uint32
				if tc.flag != "" {
					n, _ := strconv.ParseUint(tc.flag, 10, 32)
					want = uint32(n)
				}
				if !reflect.DeepEqual(s.attempts, []uint32{want}) {
					t.Fatalf("requested attempts=%v; want %d", s.attempts, want)
				}
			}
		})
	}
}
