package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This service qualifies CLI dispatch/receipt behavior only. Real handler,
// durable transitions and genuine S05 compatibility have separate script gates.
type retentionService struct {
	legacyRequestID string
	mu              sync.Mutex
	skyv1connect.UnimplementedControlServiceHandler
	old, enabled                         bool
	discoveryError, submitError          error
	discoveries, legacy, retained, plans int
	request                              *v1.SubmitRetainedJobRequest
}

func (s *retentionService) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discoveries++
	if s.discoveryError != nil {
		return nil, s.discoveryError
	}
	if s.old {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("old server"))
	}
	return connect.NewResponse(&v1.GetRetentionResponse{Enabled: s.enabled, Domain: &v1.RequestDomain{Lineage: strings.Repeat("a", 32), Epoch: 7}, AcceptUntil: timestamppb.New(time.Now().Add(time.Hour))}), nil
}
func (s *retentionService) SubmitRetainedJob(_ context.Context, request *connect.Request[v1.SubmitRetainedJobRequest]) (*connect.Response[v1.SubmitRetainedJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retained++
	s.request = proto.Clone(request.Msg).(*v1.SubmitRetainedJobRequest)
	if s.old {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("old server"))
	}
	if s.submitError != nil {
		return nil, s.submitError
	}
	return connect.NewResponse(&v1.SubmitRetainedJobResponse{Job: &v1.Job{Id: strings.Repeat("b", 32), Task: request.Msg.Task, State: v1.JobState_JOB_STATE_QUEUED}}), nil
}
func (s *retentionService) SubmitJob(_ context.Context, request *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacy++
	s.legacyRequestID = request.Msg.RequestId
	return connect.NewResponse(&v1.SubmitJobResponse{Job: &v1.Job{Id: strings.Repeat("b", 32), Task: request.Msg.Task, State: v1.JobState_JOB_STATE_QUEUED}}), nil
}
func (s *retentionService) SubmitRetryJob(ctx context.Context, request *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	return s.SubmitJob(ctx, request)
}
func (s *retentionService) PlanTask(context.Context, *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans++
	return nil, connect.NewError(connect.CodeInternal, errors.New("unexpected plan"))
}

func TestRetentionSubmissionDispatch(t *testing.T) {
	domain := strings.Repeat("a", 32) + ":7"
	for _, tc := range []struct {
		name                          string
		old, enabled                  bool
		args                          []string
		discoveryError                error
		wantCode                      connect.Code
		discoveries, legacy, retained int
	}{
		{"old_default", true, false, nil, nil, 0, 1, 1, 0},
		{"disabled_default", false, false, nil, nil, 0, 1, 1, 0},
		{"enabled_fresh", false, true, nil, nil, 0, 1, 0, 1},
		{"explicit_domain", false, true, []string{"--request-domain", domain, "--request-id", "stable"}, nil, 0, 0, 0, 1},
		{"explicit_old_refusal", true, false, []string{"--request-domain", domain, "--request-id", "stable"}, nil, connect.CodeUnimplemented, 0, 0, 1},
		{"supplied_id_no_epoch_adoption", false, true, []string{"--request-id", "legacy-id"}, nil, 0, 1, 1, 0},
		{"discovery_failure_no_fallback", false, false, nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable")), connect.CodeUnavailable, 1, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SKYGO_TOKEN", "")
			service := &retentionService{old: tc.old, enabled: tc.enabled, discoveryError: tc.discoveryError}
			path, handler := skyv1connect.NewControlServiceHandler(service)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			server := httptest.NewServer(mux)
			defer server.Close()
			file := filepath.Join(t.TempDir(), "task.yaml")
			if err := os.WriteFile(file, []byte("argv: [trainer]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			args := append([]string{"--endpoint", server.URL, "--json", "run", "-f", file}, tc.args...)
			out, diagnostic, err := execute(context.Background(), args...)
			if tc.wantCode == 0 && err != nil {
				t.Fatal(err)
			}
			if tc.wantCode != 0 && connect.CodeOf(err) != tc.wantCode {
				t.Fatalf("error=%v want=%v", err, tc.wantCode)
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if service.discoveries != tc.discoveries || service.legacy != tc.legacy || service.retained != tc.retained || service.plans != 0 {
				t.Fatalf("calls discovery=%d legacy=%d retained=%d plan=%d", service.discoveries, service.legacy, service.retained, service.plans)
			}
			if err != nil && out != "" {
				t.Fatalf("failed submission wrote stdout: %q", out)
			}
			if tc.legacy != 0 && len(tc.args) == 0 && !validHexID(service.legacyRequestID) {
				t.Fatal("omitted request ID did not generate legacy identity")
			}
			if service.request != nil {
				if len(tc.args) == 0 && !validHexID(service.request.RequestId) {
					t.Fatal("omitted request ID did not generate retained identity")
				}
				if service.request.Domain.Lineage != strings.Repeat("a", 32) || service.request.Domain.Epoch != 7 || service.request.Task.Target != "" || !proto.Equal(service.request.Task, &v1.Task{Argv: []string{"trainer"}}) {
					t.Fatal("request/domain/original task changed")
				}
				if !strings.Contains(diagnostic, "request-domain: "+domain) || !strings.Contains(diagnostic, "request-id: "+service.request.RequestId) || !strings.Contains(diagnostic, "task-file:") {
					t.Fatalf("incomplete retry hint: %q", diagnostic)
				}
			} else if strings.Contains(diagnostic, "request-domain:") {
				t.Fatalf("legacy request adopted a domain: %q", diagnostic)
			}
		})
	}
}

func TestRequestDomainRejectsAlternateIdentity(t *testing.T) {
	for _, value := range []string{"", strings.Repeat("a", 32) + ":0", strings.Repeat("a", 32) + ":01", strings.Repeat("a", 32) + ":+1", strings.Repeat("A", 32) + ":1", strings.Repeat("a", 32) + ":18446744073709551616", string([]byte{0xff})} {
		if _, err := parseRequestDomain(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
	value := strings.Repeat("a", 32) + ":18446744073709551615"
	parsed, err := parseRequestDomain(value)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := domainText(parsed); err != nil || got != value {
		t.Fatalf("domain roundtrip=%s,%v", got, err)
	}
}

func TestRetainedExpiryKeepsReceiptAndDoesNotFallback(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	expired := connect.NewError(connect.CodeFailedPrecondition, errors.New("retention result or request expired"))
	detail, err := connect.NewErrorDetail(&v1.RetentionExpiry{Reason: v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_REQUEST_EPOCH_EXPIRED})
	if err != nil {
		t.Fatal(err)
	}
	expired.AddDetail(detail)
	service := &retentionService{enabled: true, submitError: expired}
	path, handler := skyv1connect.NewControlServiceHandler(service)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	file := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(file, []byte("argv: [trainer]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	domain := strings.Repeat("a", 32) + ":7"
	out, diagnostic, err := execute(context.Background(), "--endpoint", server.URL, "run", "-f", file, "--request-domain", domain, "--request-id", "original")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "request epoch expired") {
		t.Fatalf("error=%v", err)
	}
	if out != "" || !strings.Contains(diagnostic, "request-domain: "+domain) || !strings.Contains(diagnostic, "request-id: original") {
		t.Fatalf("receipt changed: stdout=%q stderr=%q", out, diagnostic)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.discoveries != 0 || service.legacy != 0 || service.retained != 1 || service.plans != 0 || service.request.RequestId != "original" || service.request.Domain.Epoch != 7 {
		t.Fatal("expired receipt triggered discovery/fallback/new identity")
	}
}

func TestExplicitEmptyRequestIDDoesNotGenerateNewWork(t *testing.T) {
	for _, tc := range []struct {
		name         string
		old, enabled bool
	}{
		{"old", true, false}, {"disabled", false, false}, {"enabled", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SKYGO_TOKEN", "")
			service := &retentionService{old: tc.old, enabled: tc.enabled}
			path, handler := skyv1connect.NewControlServiceHandler(service)
			mux := http.NewServeMux()
			mux.Handle(path, handler)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); mux.ServeHTTP(w, r) }))
			defer server.Close()
			file := filepath.Join(t.TempDir(), "task.yaml")
			if err := os.WriteFile(file, []byte("argv: [trainer]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			out, diagnostic, err := execute(context.Background(), "--endpoint", server.URL, "run", "-f", file, "--request-id", "")
			if err == nil || !strings.Contains(err.Error(), "request ID") {
				t.Fatalf("empty explicit identity error=%v", err)
			}
			if out != "" || strings.Contains(diagnostic, "request-id:") || strings.Contains(diagnostic, "request-domain:") {
				t.Fatalf("invalid identity generated receipt: stdout=%q stderr=%q", out, diagnostic)
			}
			if calls.Load() != 0 {
				t.Fatalf("invalid identity made %d HTTP requests", calls.Load())
			}
			service.mu.Lock()
			defer service.mu.Unlock()
			if service.discoveries != 0 || service.legacy != 0 || service.retained != 0 || service.plans != 0 {
				t.Fatalf("invalid identity reached RPC: discovery=%d legacy=%d retained=%d plan=%d", service.discoveries, service.legacy, service.retained, service.plans)
			}
		})
	}
}
