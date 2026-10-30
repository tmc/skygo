package servingcontrol_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/store"
)

// These tests drive real Store transactions and real file-authenticated
// actors and delegations. The runtime is a model whose results each test
// sets; no provider is involved.

const testImage = "registry.example.com/team/web@sha256:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testSpec() serving.Spec {
	return serving.Spec{
		Image:     testImage,
		Argv:      []string{"/bin/web"},
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   serving.Scaling{Replicas: 1},
	}
}

var testLimits = store.ServiceLimits{CPUMillis: 4000, MemoryBytes: 8 << 30}

type fixture struct {
	store   *store.Store
	engine  *auth.Engine
	config  string // the credential file
	actors  map[string]auth.Actor
	domain  store.RequestDomain
	runtime *fakeRuntime
	c       *servingcontrol.Controller
	// held, if set, makes cleanup require that Close still reports a held
	// result instead of success.
	held bool
}

// newFixture builds a store with retention, file auth with operators alice
// and bob, reader, and the admins legacy and carol, and a Controller on a
// model runtime. Cleanup closes the Controller, joining its calls, before
// the store.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "store")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	config := filepath.Join(dir, "auth.json")
	f := &fixture{store: s, domain: info.Domain, actors: map[string]auth.Actor{}, config: config}
	tokens := map[string]string{"legacy": filepath.Join(dir, "legacy.token")}
	if _, err := auth.Initialize(config, tokens["legacy"], s.Owner()); err != nil {
		t.Fatal(err)
	}
	for id, role := range map[string]auth.Role{"alice": auth.Operator, "bob": auth.Operator, "reader": auth.Reader, "carol": auth.Admin} {
		if err := auth.AddPrincipal(config, id, role); err != nil {
			t.Fatal(err)
		}
		tokens[id] = filepath.Join(dir, id+".token")
		if _, err := auth.Issue(config, id, tokens[id], time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if f.engine, err = auth.NewFile(config, s.Owner()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	for id, path := range tokens {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if f.actors[id], err = f.engine.Authenticate("Bearer " + string(bytes.TrimSpace(raw))); err != nil {
			t.Fatal(err)
		}
	}
	f.runtime = &fakeRuntime{forwardStarted: make(chan struct{}, 1)}
	f.c, err = servingcontrol.New(servingcontrol.Config{
		Store: s, Auth: f.engine, Target: "kube", Binding: "kube/default", Runtime: f.runtime, Limits: testLimits,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := f.c.Close(ctx)
		switch {
		case f.held && (err == nil || errors.Is(err, context.DeadlineExceeded)):
			t.Errorf("close controller with a result held = %v, want the hold reported", err)
		case !f.held && err != nil:
			t.Errorf("close controller: %v", err)
		}
	})
	return f
}

func (f *fixture) create(t *testing.T, who, request, name string) store.Service {
	t.Helper()
	s, err := f.c.Create(f.actors[who], f.domain, request, name, "kube", testSpec())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f *fixture) reconcile(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return f.c.Reconcile(ctx)
}

func (f *fixture) read(t *testing.T, id string) store.Service {
	t.Helper()
	s, err := f.store.Service(id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// fakeRuntime is a model serving.Runtime. Apply returns applyNative and
// applyErr; Observe and Delete return their observations.
type fakeRuntime struct {
	mu          sync.Mutex
	applies     []serving.Ref
	observes    int
	deletes     []serving.Ref
	applyNative []serving.Native // returned in order, then the ref's own
	applyErr    error
	observation serving.Observation
	observeErr  error
	deleted     serving.Observation
	deleteErr   error
	// onDelete, if set, runs once inside the next Delete, after it is
	// recorded and before it returns.
	onDelete func(serving.Ref)
	// onApply, if set, runs once inside the next Apply, after it is recorded
	// and before it returns.
	onApply func(serving.Ref)

	forwards       int
	forwardBlock   bool
	forwardStarted chan struct{}
	// forwardHold, if not nil, keeps a blocked forward from returning after
	// its context ends until it is closed: it models an exchange whose
	// original response-body Close has not returned.
	forwardHold chan struct{}
}

func (r *fakeRuntime) Check(serving.Spec) error { return nil }

func (r *fakeRuntime) Apply(ctx context.Context, ref serving.Ref, rev serving.Revision, generation uint64) (serving.Native, error) {
	r.mu.Lock()
	r.applies = append(r.applies, ref)
	hook := r.onApply
	r.onApply = nil
	n, err := ref.Native, r.applyErr
	if len(r.applyNative) > 0 {
		n = r.applyNative[0]
		r.applyNative = r.applyNative[1:]
	}
	r.mu.Unlock()
	if hook != nil {
		hook(ref)
	}
	return n, err
}

func (r *fakeRuntime) Observe(ctx context.Context, ref serving.Ref) (serving.Observation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observes++
	return r.observation, r.observeErr
}

func (r *fakeRuntime) Delete(ctx context.Context, ref serving.Ref) (serving.Observation, error) {
	r.mu.Lock()
	r.deletes = append(r.deletes, ref)
	hook := r.onDelete
	r.onDelete = nil
	o, err := r.deleted, r.deleteErr
	r.mu.Unlock()
	if hook != nil {
		hook(ref)
	}
	return o, err
}

func (r *fakeRuntime) Forward(ctx context.Context, ref serving.Ref, w http.ResponseWriter, req *http.Request) error {
	r.mu.Lock()
	r.forwards++
	block, hold := r.forwardBlock, r.forwardHold
	r.mu.Unlock()
	if block {
		r.forwardStarted <- struct{}{}
		<-ctx.Done()
		if hold != nil {
			<-hold
		}
		return ctx.Err()
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (r *fakeRuntime) set(f func(*fakeRuntime)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(r)
}

func (r *fakeRuntime) calls() (applies, deletes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.applies), len(r.deletes)
}

var ready = serving.Observation{ObservedGeneration: 1, ReadyGeneration: 1, DesiredReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, ReadyEndpoints: 1}

func TestCreateCommitsBeforeRuntime(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	if a, _ := f.runtime.calls(); a != 0 {
		t.Fatalf("Create made %d runtime calls", a)
	}
	if s.Operation == nil || s.Operation.Kind != store.ServiceApply || s.Operation.Generation != 1 {
		t.Fatalf("accepted operation = %+v", s.Operation)
	}
	again := f.create(t, "alice", "req-1", "web")
	if again.ID != s.ID {
		t.Fatalf("replay returned %s, want %s", again.ID, s.ID)
	}
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:declared", "v1:created"}
		r.observation = ready
	})
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:created" || got.Operation != nil || got.Phase() != serving.Ready {
		t.Fatalf("after pass: native %q op %+v phase %v", got.Native, got.Operation, got.Phase())
	}
	// Each Apply saw the Native the previous one returned, committed.
	f.runtime.mu.Lock()
	refs := append([]serving.Ref(nil), f.runtime.applies...)
	f.runtime.mu.Unlock()
	want := []serving.Native{"", "v1:declared", "v1:created"}
	if len(refs) != len(want) {
		t.Fatalf("applies = %d, want %d", len(refs), len(want))
	}
	for i, r := range refs {
		if r.Native != want[i] {
			t.Errorf("apply %d saw native %q, want %q", i, r.Native, want[i])
		}
	}
}

func TestNativePersistedWithError(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	boom := errors.New("target refused")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:declared"}
		r.applyErr = boom
	})
	if err := f.reconcile(t); !errors.Is(err, boom) {
		t.Fatalf("reconcile = %v, want %v", err, boom)
	}
	got := f.read(t, s.ID)
	if got.Native != "v1:declared" {
		t.Fatalf("native %q not persisted with the error", got.Native)
	}
	if got.Operation == nil || *got.Operation != *s.Operation {
		t.Fatalf("operation %+v, want original %+v", got.Operation, s.Operation)
	}
}

func TestRefusesUnsupported(t *testing.T) {
	f := newFixture(t)
	tests := []struct {
		name   string
		mutate func(*serving.Spec)
	}{
		{"replicas", func(s *serving.Spec) { s.Scaling.Replicas = 9 }},
		{"mounts", func(s *serving.Spec) {
			s.Mounts = []serving.Mount{{Reference: "data", Destination: "/mnt/skygo/data"}}
		}},
		{"setup without workdir", func(s *serving.Spec) { s.Setup = []string{"make"} }},
	}
	for _, tt := range tests {
		spec := testSpec()
		tt.mutate(&spec)
		if _, err := f.c.Create(f.actors["alice"], f.domain, "req-"+tt.name, "web", "kube", spec); err == nil {
			t.Errorf("%s: accepted", tt.name)
		}
	}
	if _, err := f.c.Create(f.actors["alice"], f.domain, "req-target", "web", "other", testSpec()); !errors.Is(err, servingcontrol.ErrUnknownTarget) {
		t.Errorf("unknown target: %v", err)
	}
	if _, err := f.c.Create(f.actors["reader"], f.domain, "req-reader", "web", "kube", testSpec()); err == nil {
		t.Error("reader created a service")
	}
	p, err := f.store.Services(0, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Services) != 0 {
		t.Fatalf("refusals wrote %d services", len(p.Services))
	}
}

// TestHeldNativeBlocksRuntime checks that a Native whose commit failed is
// committed before any other runtime call or forward for its service.
func TestHeldNativeBlocksRuntime(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.applyNative = []serving.Native{"v1:declared"} })
	servingcontrol.SetCommit(f.c, func(string, uint64, store.ServiceCommit) (store.Service, error) {
		return store.Service{}, errors.New("disk full")
	})
	if err := f.reconcile(t); err == nil {
		t.Fatal("reconcile hid a failed native commit")
	}
	applies, _ := f.runtime.calls()
	for range 3 {
		if err := f.reconcile(t); err == nil {
			t.Fatal("reconcile passed with a held native result")
		}
	}
	if n, _ := f.runtime.calls(); n != applies {
		t.Fatalf("runtime called %d more times with a result held", n-applies)
	}
	if err := forwardStatus(f, "alice", s.ID); !errors.Is(err, servingcontrol.ErrNotReady) {
		t.Fatalf("forward with held result: %v", err)
	}
	servingcontrol.SetCommit(f.c, f.store.CommitService)
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if got := f.read(t, s.ID); got.Native != "v1:declared" {
		t.Fatalf("held native %q not committed first", got.Native)
	}
}

func TestDeleteUnderOriginalOperation(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) {
		r.applyNative = []serving.Native{"v1:declared"}
		r.applyErr = errors.New("lost response")
	})
	_ = f.reconcile(t)
	cur := f.read(t, s.ID)
	if _, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision); err != nil {
		t.Fatal(err)
	}
	// A stale absence: Delete cannot yet show every owned object gone.
	f.runtime.set(func(r *fakeRuntime) { r.deleted = serving.Observation{ObservedGeneration: 1} })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	got := f.read(t, s.ID)
	if got.Operation == nil || *got.Operation != *s.Operation {
		t.Fatalf("operation %+v, want original %+v kept", got.Operation, s.Operation)
	}
	if got.Reserved == (store.ServiceReservation{}) || got.Phase() == serving.Deleted {
		t.Fatalf("released without observed absence: %+v", got)
	}
	f.runtime.mu.Lock()
	lastDelete := f.runtime.deletes[len(f.runtime.deletes)-1]
	f.runtime.mu.Unlock()
	if lastDelete.Native != "v1:declared" {
		t.Fatalf("delete saw native %q, want the committed declaration", lastDelete.Native)
	}
	f.runtime.set(func(r *fakeRuntime) { r.deleted = serving.Observation{Absent: true} })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Service(s.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("service not retired: %v", err)
	}
	// An exact replay of the delete returns the retired view.
	v, err := f.c.Delete(f.actors["alice"], f.domain, "del-1", s.ID, cur.Revision)
	if err != nil || !v.Retired {
		t.Fatalf("replayed delete = %+v, %v", v, err)
	}
	// The name is reusable after retirement.
	f.create(t, "alice", "req-2", "web")
}

func forwardStatus(f *fixture, who, id string) error {
	req := httptest.NewRequest("GET", "/services/"+id+"/forward/", nil)
	return f.c.Forward(f.actors[who], id, httptest.NewRecorder(), req)
}

func TestForwardOwnPrincipalOnly(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	if err := forwardStatus(f, "alice", s.ID); !errors.Is(err, servingcontrol.ErrNotReady) {
		t.Fatalf("forward before ready: %v", err)
	}
	f.runtime.set(func(r *fakeRuntime) { r.observation = ready })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	if err := forwardStatus(f, "alice", s.ID); err != nil {
		t.Fatalf("owner forward: %v", err)
	}
	for who, want := range map[string]error{"bob": store.ErrNotFound, "legacy": store.ErrNotFound} {
		if err := forwardStatus(f, who, s.ID); !errors.Is(err, want) {
			t.Errorf("%s forward = %v, want %v", who, err, want)
		}
	}
	if err := forwardStatus(f, "reader", s.ID); err == nil {
		t.Error("reader forwarded")
	}
	// The admin may read it.
	if _, err := f.c.Get(f.actors["legacy"], s.ID); err != nil {
		t.Errorf("admin get: %v", err)
	}
	if _, err := f.c.Get(f.actors["bob"], s.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("bob get: %v", err)
	}
	f.runtime.mu.Lock()
	n := f.runtime.forwards
	f.runtime.mu.Unlock()
	if n != 1 {
		t.Fatalf("runtime forwards = %d, want only the owner's", n)
	}
}

func TestCloseJoinsForward(t *testing.T) {
	f := newFixture(t)
	s := f.create(t, "alice", "req-1", "web")
	f.runtime.set(func(r *fakeRuntime) { r.observation = ready })
	if err := f.reconcile(t); err != nil {
		t.Fatal(err)
	}
	f.runtime.set(func(r *fakeRuntime) { r.forwardBlock = true })
	done := make(chan error, 1)
	go func() { done <- forwardStatus(f, "alice", s.ID) }()
	select {
	case <-f.runtime.forwardStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("forward did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("forward = %v, want canceled", err)
		}
	default:
		t.Fatal("Close returned before the forward")
	}
	if err := forwardStatus(f, "alice", s.ID); !errors.Is(err, servingcontrol.ErrClosed) {
		t.Fatalf("forward after close: %v", err)
	}
}
