package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// retryQuotaBackend checks admission but must never execute a workload.
type retryQuotaBackend struct {
	checks atomic.Int32
	calls  atomic.Int32
}

func (b *retryQuotaBackend) Check(backend.Task) error { b.checks.Add(1); return nil }
func (b *retryQuotaBackend) unexpected() error {
	b.calls.Add(1)
	return errors.New("unexpected execution callback")
}
func (b *retryQuotaBackend) Start(context.Context, string, backend.Task) error {
	return b.unexpected()
}
func (b *retryQuotaBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, b.unexpected()
}
func (b *retryQuotaBackend) Cancel(context.Context, backend.Reference) error {
	return b.unexpected()
}
func (b *retryQuotaBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, b.unexpected()
}

func TestRetainedRetryQuotaDefaultBoundaryHTTP(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("real retention filesystem sampling requires Linux or Darwin")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	config, initial := filepath.Join(dir, "auth.json"), filepath.Join(dir, "admin.token")
	if _, err := auth.Initialize(config, initial, s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(config, "operator", auth.Operator); err != nil {
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "operator.token")
	if _, err := auth.Issue(config, "operator", tokenPath, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	policy := store.DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	info, err := store.EnableRetention(state, policy)
	if err != nil {
		t.Fatal(err)
	}
	s, err = store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	authCtx, cancelAuth := context.WithCancel(context.Background())
	authDone := make(chan struct{})
	go func() { defer close(authDone); engine.Run(authCtx) }()
	t.Cleanup(func() {
		cancelAuth()
		select {
		case <-authDone:
		case <-time.After(15 * time.Second):
			t.Error("authentication refresh did not stop")
		}
	})
	readToken := func(path string) string {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSuffix(string(raw), "\n")
	}
	token := readToken(tokenPath)
	actor, err := engine.Authenticate("Bearer " + token)
	if err != nil || actor.Role() != auth.Operator {
		t.Fatalf("operator authentication: role=%v error=%v", actor.Role(), err)
	}
	admin, err := engine.Authenticate("Bearer " + readToken(initial))
	if err != nil {
		t.Fatal(err)
	}
	task := func(n uint32) backend.Task {
		return backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: n, ExitCodes: []int32{7}}}
	}
	// Real admissions reserve (204*10+8)*8192 = the default 16 MiB.
	// Settle cancellation through BeginAttempt before starting any control loop.
	var before []backend.Job
	for i := 0; i < 205; i++ {
		n := uint32(10)
		if i == 204 {
			n = 8
		}
		j, err := s.SubmitRetained(actor, info.Domain, fmt.Sprintf("job-%d", i), task(n), "remote", "test-binding", backend.RemoteOutput, 0)
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		if _, err := s.Update(j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
			t.Fatal(err)
		}
		j, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, time.Now())
		if err != nil || start || j.Status.State != backend.Canceled || len(j.Attempts) != 1 || j.Attempts[0].StartingAt != nil {
			t.Fatalf("settle seed %d: job=%+v start=%t error=%v", i, j, start, err)
		}
		// Snapshot the persisted representation, including decoded timestamps.
		j, err = s.GetFor(actor, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		before = append(before, j)
	}
	assertUsage := func() {
		t.Helper()
		current, err := s.RetentionInfo(admin)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range current.Usage {
			if u.Domain == store.RetryBytesQuota {
				if u.Used != 16<<20 || u.Limit != policy.RetryBytes || u.Limit != 16<<20 {
					t.Fatalf("retry usage=%+v, want default 16 MiB used and limit", u)
				}
				return
			}
		}
		t.Fatal("retry usage absent")
	}
	assertUsage()
	b := new(retryQuotaBackend)
	c, err := control.New(s, []backend.Target{{
		Name: "remote", Kind: "test", Binding: "test-binding", Backend: b,
		SupportsRetry: true, OutputStorage: backend.RemoteOutput,
		Retention: &backend.Retention{RemoveExecution: func(context.Context, backend.Reference) error { return b.unexpected() }},
	}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var stopOnce sync.Once
	stopRun := func() {
		stopOnce.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			case <-time.After(15 * time.Second):
				t.Error("control did not stop")
			}
		})
	}
	t.Cleanup(stopRun)
	// Admission must use Run's actual database/statfs sample, not injected metrics.
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !c.StorageReady() {
		select {
		case <-deadline.C:
			t.Fatal("real storage sample did not permit admission")
		case <-tick.C:
		}
	}
	var retained, other atomic.Int32
	handler := rpc.NewHandler(c, engine)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == skyv1connect.ControlServiceSubmitRetainedJobProcedure {
			retained.Add(1)
		} else {
			other.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	request := func(id string, n uint32) *connect.Request[v1.SubmitRetainedJobRequest] {
		return authorized(token, &v1.SubmitRetainedJobRequest{
			RequestId: id, Domain: &v1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch},
			Task: &v1.Task{Argv: []string{"work"}, Retry: &v1.RetryPolicy{MaxAttempts: n, ExitCodes: []int32{7}}},
		})
	}
	httpCtx, stopHTTP := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopHTTP()
	if !engine.Ready() {
		t.Fatal("authentication refresh unavailable before quota HTTP admission")
	}
	_, err = client.SubmitRetainedJob(httpCtx, request("refused", 2))
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("HTTP admission error=%v, want resource_exhausted", err)
	}
	var wireErr *connect.Error
	if !errors.As(err, &wireErr) {
		t.Fatalf("HTTP error type=%T", err)
	}
	if len(wireErr.Details()) != 1 {
		t.Fatalf("HTTP details=%d, want one typed capacity detail", len(wireErr.Details()))
	}
	value, err := wireErr.Details()[0].Value()
	if err != nil {
		t.Fatal(err)
	}
	capacity, ok := value.(*v1.RetentionCapacity)
	if !ok || capacity.Domain != v1.RetentionQuotaDomain_RETENTION_QUOTA_DOMAIN_RETRY_BYTES || capacity.Used != 16<<20 || capacity.Limit != policy.RetryBytes {
		t.Fatalf("HTTP capacity detail=%v (%T), want RETRY_BYTES used=limit=16 MiB", value, value)
	}
	replay, err := client.SubmitRetainedJob(httpCtx, request("job-0", 10))
	if err != nil || replay.Msg.Job.Id != before[0].ID || replay.Msg.Job.State != v1.JobState_JOB_STATE_CANCELED {
		t.Fatalf("HTTP replay=%v error=%v", replay, err)
	}
	stopRun()
	for _, original := range before {
		got, err := s.GetFor(actor, original.ID)
		if err != nil || !reflect.DeepEqual(got, original) {
			t.Fatalf("seed %s changed: error=%v before=%+v after=%+v", original.ID, err, original, got)
		}
	}
	if n, err := s.LastSequenceFor(actor); err != nil || n != 205 {
		t.Fatalf("sequence=%d error=%v, want 205", n, err)
	}
	if _, found, err := s.LookupRetained(actor, info.Domain, "refused", task(2)); err != nil || found {
		t.Fatalf("refused request persisted: found=%t error=%v", found, err)
	}
	assertUsage()
	if b.checks.Load() != 1 || b.calls.Load() != 0 {
		t.Fatalf("backend checks=%d execution/cleanup calls=%d, want 1/0", b.checks.Load(), b.calls.Load())
	}
	if retained.Load() != 2 || other.Load() != 0 {
		t.Fatalf("HTTP retained=%d other=%d, want 2/0 without fallback", retained.Load(), other.Load())
	}
}
