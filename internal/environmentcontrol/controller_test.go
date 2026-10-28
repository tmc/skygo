package environmentcontrol_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

// These tests drive real Store transactions and real file-authenticated
// actors and delegations. The runtime is a model: its frames and
// observations are what each test sets, and no provider is involved.

type fixture struct {
	dir     string
	store   *store.Store
	engine  *auth.Engine
	config  string
	tokens  map[string]string // principal: token file
	actors  map[string]auth.Actor
	runtime *fakeRuntime
	clock   *clock
	c       *environmentcontrol.Controller
	stop    func()
	// domain is the store's request domain when retention was enabled.
	domain store.RequestDomain
}

// setup builds a fixture in dir, which must be private. Principals alice and
// bob are operators, reader is a reader and legacy is the admin. The store
// has bounded retention enabled with the default policy.
func setup(dir string) (*fixture, error) {
	state := filepath.Join(dir, "store")
	info, err := store.EnableRetention(state, store.DefaultRetentionPolicy())
	if err != nil {
		return nil, err
	}
	s, err := store.Open(state)
	if err != nil {
		return nil, err
	}
	f := &fixture{dir: dir, store: s, domain: info.Domain, tokens: map[string]string{}, actors: map[string]auth.Actor{}}
	f.config = filepath.Join(dir, "auth.json")
	f.tokens["legacy"] = filepath.Join(dir, "legacy.token")
	if _, err := auth.Initialize(f.config, f.tokens["legacy"], s.Owner()); err != nil {
		return nil, err
	}
	for id, role := range map[string]auth.Role{"alice": auth.Operator, "bob": auth.Operator, "reader": auth.Reader} {
		if err := auth.AddPrincipal(f.config, id, role); err != nil {
			return nil, err
		}
		f.tokens[id] = filepath.Join(dir, id+".token")
		if _, err := auth.Issue(f.config, id, f.tokens[id], time.Hour); err != nil {
			return nil, err
		}
	}
	if err := s.ActivateScoped(); err != nil {
		return nil, err
	}
	if f.engine, err = auth.NewFile(f.config, s.Owner()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); f.engine.Run(ctx) }()
	f.stop = func() { cancel(); <-done; s.Close() }
	for id, path := range f.tokens {
		raw, err := os.ReadFile(path)
		if err != nil {
			f.stop()
			return nil, err
		}
		if f.actors[id], err = f.engine.Authenticate("Bearer " + string(bytes.TrimSpace(raw))); err != nil {
			f.stop()
			return nil, err
		}
	}
	f.runtime = newFakeRuntime()
	f.clock = &clock{t: time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)}
	if f.c, err = f.controller(); err != nil {
		f.stop()
		return nil, err
	}
	return f, nil
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	// The credential parent must be private; TempDir follows the umask.
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := setup(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		f.runtime.finishAll()
		f.stop()
	})
	return f
}

// controller returns a new Controller on the fixture's store, as a new
// server process would.
func (f *fixture) controller() (*environmentcontrol.Controller, error) {
	return environmentcontrol.New(environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: f.runtime, Now: f.clock.now})
}

func (f *fixture) create(t *testing.T, who, request string, spec environment.Spec) environmentcontrol.View {
	t.Helper()
	v, err := f.c.Create(f.actors[who], f.domain, request, spec)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// ready creates an environment for who and reconciles it to Ready.
func (f *fixture) ready(t *testing.T, who string, spec environment.Spec) environmentcontrol.View {
	t.Helper()
	v := f.create(t, who, "create-"+spec.Name, spec)
	if err := f.c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	return f.get(t, who, v.ID, environment.Ready)
}

// get reads id as who and checks its phase.
func (f *fixture) get(t *testing.T, who, id string, phase environment.Phase) environmentcontrol.View {
	t.Helper()
	v, err := f.c.Get(f.actors[who], id)
	if err != nil {
		t.Fatal(err)
	}
	if v.State.Phase != phase {
		t.Fatalf("phase = %v, want %v", v.State.Phase, phase)
	}
	return v
}

func (f *fixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testSpec(name string) environment.Spec {
	return environment.Spec{Name: name, Target: "kube", Image: "task:v1", CPUMillis: 100, MemoryBytes: 1 << 20, Port: environment.Port}
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeRuntime models a provider. By default Plan succeeds, Ensure reports
// Ready with native state "uid-<id>", Delete reports Deleted, an exec exits
// at once with its argv after the first as output, and a forward answers
// 200 with the relayed path.
type fakeRuntime struct {
	mu      sync.Mutex
	plans   int
	ensures []environmentcontrol.Ref
	deletes []environmentcontrol.Ref
	execRef []environmentcontrol.Ref
	fwdRef  []environmentcontrol.Ref
	frames  []*fakeFrame

	planErr  error
	ensure   func(environmentcontrol.Ref) (environmentcontrol.Observation, error)
	remove   func(environmentcontrol.Ref) (environmentcontrol.Observation, error)
	execErr  error
	hold     bool // exec frames wait for the test
	started  chan *fakeFrame
	onExec   func(environmentcontrol.Ref)
	onFwd    func(environmentcontrol.Ref)
	forward  func(http.ResponseWriter, *http.Request) environmentcontrol.ForwardResult
	forwards int
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{started: make(chan *fakeFrame, 64)}
}

func (r *fakeRuntime) Kind() string { return "fake-pod" }

func (r *fakeRuntime) Plan(principal string, spec environment.Spec, digest [32]byte) (string, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.plans++
	if r.planErr != nil {
		return "", nil, r.planErr
	}
	return "fake-binding", []byte("planned"), nil
}

func (r *fakeRuntime) Ensure(ctx context.Context, ref environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	r.mu.Lock()
	r.ensures = append(r.ensures, ref)
	ensure := r.ensure
	r.mu.Unlock()
	if ensure != nil {
		return ensure(ref)
	}
	return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte("uid-" + ref.Identity.ID)}, nil
}

func (r *fakeRuntime) Delete(ctx context.Context, ref environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	r.mu.Lock()
	r.deletes = append(r.deletes, ref)
	remove := r.remove
	r.mu.Unlock()
	if remove != nil {
		return remove(ref)
	}
	return environmentcontrol.Observation{Phase: environment.Deleted}, nil
}

func (r *fakeRuntime) Exec(ref environmentcontrol.Ref, req environmentcontrol.ExecRequest) (environmentcontrol.Exec, error) {
	r.mu.Lock()
	if r.execErr != nil {
		r.mu.Unlock()
		return nil, r.execErr
	}
	onExec := r.onExec
	x := newFakeFrame()
	r.execRef = append(r.execRef, ref)
	r.frames = append(r.frames, x)
	hold := r.hold
	r.mu.Unlock()
	if onExec != nil {
		onExec(ref)
	}
	if !hold {
		x.finish(environmentcontrol.ExecResult{Exited: true, Stdout: []byte(strings.Join(req.Argv[1:], " "))})
	}
	select {
	case r.started <- x:
	default:
	}
	return x, nil
}

func (r *fakeRuntime) Forward(w http.ResponseWriter, req *http.Request, ref environmentcontrol.Ref) environmentcontrol.Forward {
	r.mu.Lock()
	r.forwards++
	r.fwdRef = append(r.fwdRef, ref)
	forward, onFwd := r.forward, r.onFwd
	r.mu.Unlock()
	if onFwd != nil {
		onFwd(ref)
	}
	x := &fakeForward{done: make(chan struct{})}
	if forward != nil {
		x.res = forward(w, req)
	} else {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "port %d path %s", ref.Spec.Port, req.URL.Path)
		x.res.Status = http.StatusOK
	}
	close(x.done)
	return x
}

func (r *fakeRuntime) counts() (plans, ensures, deletes, execs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.plans, len(r.ensures), len(r.deletes), len(r.execRef)
}

func (r *fakeRuntime) lastEnsure() environmentcontrol.Ref {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensures[len(r.ensures)-1]
}

func (r *fakeRuntime) finishAll() {
	r.mu.Lock()
	frames := append([]*fakeFrame(nil), r.frames...)
	r.mu.Unlock()
	for _, x := range frames {
		x.finish(environmentcontrol.ExecResult{Exited: true})
	}
}

// fakeFrame is an exec frame that settles only when the test finishes it.
// Cancel records cancellation and settles nothing.
type fakeFrame struct {
	done     chan struct{}
	canceled chan struct{}
	settle   sync.Once
	cancel   sync.Once
	mu       sync.Mutex
	res      environmentcontrol.ExecResult
}

func newFakeFrame() *fakeFrame {
	return &fakeFrame{done: make(chan struct{}), canceled: make(chan struct{})}
}

func (x *fakeFrame) Done() <-chan struct{} { return x.done }
func (x *fakeFrame) Cancel()               { x.cancel.Do(func() { close(x.canceled) }) }

func (x *fakeFrame) Result() environmentcontrol.ExecResult {
	x.mu.Lock()
	defer x.mu.Unlock()
	r := x.res
	select {
	case <-x.done:
		r.Settled = true
	default:
	}
	return r
}

func (x *fakeFrame) finish(r environmentcontrol.ExecResult) {
	x.settle.Do(func() {
		x.mu.Lock()
		x.res = r
		x.mu.Unlock()
		close(x.done)
	})
}

func (x *fakeFrame) wasCanceled() bool {
	select {
	case <-x.canceled:
		return true
	default:
		return false
	}
}

type fakeForward struct {
	done chan struct{}
	res  environmentcontrol.ForwardResult
}

func (x *fakeForward) Done() <-chan struct{} { return x.done }
func (x *fakeForward) Cancel()               {}
func (x *fakeForward) Result() environmentcontrol.ForwardResult {
	r := x.res
	select {
	case <-x.done:
		r.Settled = true
	default:
	}
	return r
}

// eventually waits up to five seconds for cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewRefusesIncompleteConfig(t *testing.T) {
	f := newFixture(t)
	for _, test := range []struct {
		name string
		c    environmentcontrol.Config
	}{
		{"no store", environmentcontrol.Config{Auth: f.engine, Runtime: f.runtime}},
		{"no auth", environmentcontrol.Config{Store: f.store, Runtime: f.runtime}},
		{"no runtime", environmentcontrol.Config{Store: f.store, Auth: f.engine}},
		{"bad kind", environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: badKind{f.runtime}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := environmentcontrol.New(test.c); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

type badKind struct{ *fakeRuntime }

func (badKind) Kind() string { return "Not A Kind" }

func TestGetAndListScope(t *testing.T) {
	f := newFixture(t)
	a := f.create(t, "alice", "a", testSpec("alpha"))
	f.create(t, "bob", "b", testSpec("beta"))
	if _, err := f.c.Get(f.actors["bob"], a.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign get: %v", err)
	}
	if _, err := f.c.Get(f.actors["alice"], "not-an-id"); !errors.Is(err, environmentcontrol.ErrInvalid) {
		t.Fatalf("invalid id: %v", err)
	}
	if _, err := f.c.Get(auth.Actor{}, a.ID); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("zero actor: %v", err)
	}
	for _, test := range []struct {
		who  string
		want int
	}{{"alice", 1}, {"bob", 1}, {"reader", 0}, {"legacy", 2}} {
		p, err := f.c.List(f.actors[test.who], 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Environments) != test.want {
			t.Errorf("%s lists %d, want %d", test.who, len(p.Environments), test.want)
		}
	}
	if _, err := f.c.List(f.actors["alice"], 0, 201); !errors.Is(err, environmentcontrol.ErrInvalid) {
		t.Fatalf("large limit: %v", err)
	}
	v, err := f.c.Get(f.actors["legacy"], a.ID)
	if err != nil || v.Principal != "alice" || v.Spec.Name != "alpha" {
		t.Fatalf("admin get = %+v, %v", v, err)
	}
}
