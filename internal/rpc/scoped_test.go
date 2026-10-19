package rpc_test

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
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// logBackend supplies only the fault under test. Jobs are accepted through the
// real control service, then marked running without starting a workload.
type logBackend struct {
	read func(context.Context, int64, int) (backend.LogChunk, error)
}

func (*logBackend) Check(backend.Task) error { return nil }
func (*logBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected start")
}
func (*logBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*logBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (b *logBackend) Logs(ctx context.Context, _ backend.Reference, offset int64, size int) (backend.LogChunk, error) {
	return b.read(ctx, offset, size)
}

type scopedFixture struct {
	path    string
	tokens  map[string]string
	ids     map[string]string
	store   *store.Store
	engine  *auth.Engine
	control *control.Control
	handler http.Handler
	client  skyv1connect.ControlServiceClient
}

func scoped(t *testing.T, b *logBackend) *scopedFixture {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	f := &scopedFixture{path: filepath.Join(dir, "auth.json"), tokens: make(map[string]string), ids: make(map[string]string), store: s}
	initial := filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(f.path, initial, s.Owner()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(initial)
	if err != nil {
		t.Fatal(err)
	}
	f.tokens["legacy"] = strings.TrimSuffix(string(data), "\n")
	for _, p := range []struct {
		name string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Operator}, {"reader", auth.Reader}, {"admin", auth.Admin}} {
		if err := auth.AddPrincipal(f.path, p.name, p.role); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, p.name+".token")
		info, err := auth.Issue(f.path, p.name, path, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f.tokens[p.name], f.ids[p.name] = strings.TrimSuffix(string(data), "\n"), info.ID
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	f.engine, err = auth.NewFile(f.path, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	f.control, err = control.New(s, []backend.Target{{Name: "test", Kind: "test", Backend: b}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	f.handler = rpc.NewHandler(f.control, f.engine)
	server := httptest.NewServer(f.handler)
	t.Cleanup(server.Close)
	f.client = skyv1connect.NewControlServiceClient(server.Client(), server.URL)
	return f
}
func authorized[T any](token string, msg *T) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+token)
	return r
}
func (f *scopedFixture) submit(t *testing.T, principal, request string) string {
	t.Helper()
	r, err := f.client.SubmitJob(context.Background(), authorized(f.tokens[principal], &v1.SubmitJobRequest{RequestId: request, Task: &v1.Task{Argv: []string{"unused", principal}}}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Msg.Job.Id
}
func (f *scopedFixture) running(t *testing.T, principal string) string {
	t.Helper()
	id := f.submit(t, principal, "stream")
	if _, err := f.store.Update(id, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
		t.Fatal(err)
	}
	return id
}
func (f *scopedFixture) receive(ctx context.Context, principal, id string) error {
	stream, err := f.client.StreamJobLogs(ctx, authorized(f.tokens[principal], &v1.StreamJobLogsRequest{Id: id, Follow: true}))
	if err != nil {
		return err
	}
	defer stream.Close()
	for stream.Receive() {
		if len(stream.Msg().Data) > 0 {
			return errors.New("unexpected log data")
		}
	}
	return stream.Err()
}
func waitEvent(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("event did not occur")
	}
}

func TestScopedHTTP(t *testing.T) {
	var reads atomic.Int32
	f := scoped(t, &logBackend{read: func(context.Context, int64, int) (backend.LogChunk, error) {
		reads.Add(1)
		return backend.LogChunk{Complete: true}, nil
	}})
	a, b := f.submit(t, "alice", "same"), f.submit(t, "bob", "same")
	if a == b {
		t.Fatal("principals shared request identity")
	}
	if got := f.submit(t, "alice", "same"); got != a {
		t.Fatal("retry identity changed")
	}
	for _, id := range []string{b, strings.Repeat("f", 32)} {
		_, err := f.client.GetJob(context.Background(), authorized(f.tokens["alice"], &v1.GetJobRequest{Id: id}))
		code(t, err, connect.CodeNotFound)
		_, err = f.client.CancelJob(context.Background(), authorized(f.tokens["alice"], &v1.CancelJobRequest{Id: id}))
		code(t, err, connect.CodeNotFound)
		code(t, f.receive(context.Background(), "alice", id), connect.CodeNotFound)
	}
	if reads.Load() != 0 {
		t.Fatal("foreign logs reached backend")
	}
	for _, principal := range []string{"alice", "bob", "admin"} {
		r, err := f.client.ListJobs(context.Background(), authorized(f.tokens[principal], &v1.ListJobsRequest{}))
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if principal == "admin" {
			want = 2
		}
		if len(r.Msg.Jobs) != want {
			t.Fatalf("%s listed %d jobs", principal, len(r.Msg.Jobs))
		}
	}
	_, err := f.client.SubmitJob(context.Background(), authorized(f.tokens["reader"], &v1.SubmitJobRequest{}))
	code(t, err, connect.CodePermissionDenied)
	_, err = f.client.CancelJob(context.Background(), authorized(f.tokens["reader"], &v1.CancelJobRequest{Id: "invalid"}))
	code(t, err, connect.CodePermissionDenied)
	_, err = f.client.GetJob(context.Background(), authorized(f.tokens["admin"], &v1.GetJobRequest{Id: b}))
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamRevocationCancelsBackend(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	f := scoped(t, &logBackend{read: func(ctx context.Context, _ int64, _ int) (backend.LogChunk, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		return backend.LogChunk{}, ctx.Err()
	}})
	id := f.running(t, "alice")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.receive(ctx, "alice", id) }()
	waitEvent(t, entered)
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-done:
		code(t, err, connect.CodeUnauthenticated)
	case <-deadline.C:
		t.Fatal("revocation exceeded two seconds")
	}
	waitEvent(t, canceled)
	j, err := f.store.Get(id)
	if err != nil || j.CancelRequested {
		t.Fatal("revocation changed accepted job", err)
	}
}

func TestIgnoredLogsRetainStreamPermits(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	entered := make(chan struct{}, 32)
	var calls atomic.Int32
	f := scoped(t, &logBackend{read: func(context.Context, int64, int) (backend.LogChunk, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return backend.LogChunk{Complete: true}, nil
	}})
	// Register after the fixture cleanup: release ignored calls before closing HTTP.
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	id := f.running(t, "alice")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 32)
	for range 32 {
		go func() { done <- f.receive(ctx, "alice", id) }()
	}
	for range 32 {
		waitEvent(t, entered)
	}
	if err := auth.Revoke(f.path, f.ids["alice"]); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for range 32 {
		select {
		case err := <-done:
			code(t, err, connect.CodeUnauthenticated)
		case <-deadline.C:
			t.Fatal("ignored backend prevented revocation")
		}
	}
	for range 40 {
		code(t, f.receive(ctx, "admin", id), connect.CodeResourceExhausted)
	}
	if calls.Load() != 32 {
		t.Fatalf("backend calls = %d, want 32", calls.Load())
	}
	once.Do(func() { close(release) })
	limit := time.Now().Add(2 * time.Second)
	for {
		err := f.receive(ctx, "admin", id)
		if err == nil {
			break
		}
		if connect.CodeOf(err) != connect.CodeResourceExhausted || time.Now().After(limit) {
			t.Fatal("permits did not recover", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 33 {
		t.Fatalf("recovered backend calls = %d, want 33", calls.Load())
	}
}

func TestRoleChangeStopsOwnAndForeignStreams(t *testing.T) {
	for _, principal := range []string{"alice", "admin"} {
		t.Run(principal, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			f := scoped(t, &logBackend{read: func(context.Context, int64, int) (backend.LogChunk, error) {
				close(entered)
				<-release
				return backend.LogChunk{Data: []byte("must not publish"), NextOffset: 16, Complete: true}, nil
			}})
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			id := f.running(t, "alice")
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- f.receive(ctx, principal, id) }()
			waitEvent(t, entered)
			if err := auth.SetRole(f.path, principal, auth.Reader); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				code(t, err, connect.CodePermissionDenied)
			case <-time.After(2 * time.Second):
				t.Fatal("role change did not stop stream")
			}
			once.Do(func() { close(release) })
			_, err := f.client.GetJob(ctx, authorized(f.tokens[principal], &v1.GetJobRequest{Id: id}))
			if principal == "alice" {
				if err != nil {
					t.Fatal("reader lost own job", err)
				}
			} else {
				code(t, err, connect.CodeNotFound)
			}
		})
	}
}
