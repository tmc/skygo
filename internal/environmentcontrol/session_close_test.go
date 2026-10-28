package environmentcontrol_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

// Source-only; NOT_RUN. These tests drive the actual Controller.Exec and
// Controller.Forward session paths, their background settlements and
// Integration.Close, against the real Store and file authentication of the
// fixture. The runtime is the fixture's model; the Store's commits pass
// through commitGate, which can hold one commit before it reaches the Store.

// commitGate passes environment commits to the Store. Armed, it holds the
// next commit, before the Store sees it, until release is closed.
type commitGate struct {
	s       *store.Store
	commits atomic.Int32

	mu      sync.Mutex
	armed   bool
	held    chan struct{} // closed when the armed commit is held
	release chan struct{}
}

func (g *commitGate) commit(id string, revision uint64, next store.EnvironmentCommit) (store.Environment, error) {
	g.commits.Add(1)
	g.mu.Lock()
	armed, held, release := g.armed, g.held, g.release
	g.armed = false
	g.mu.Unlock()
	if armed {
		close(held)
		<-release
	}
	return g.s.CommitEnvironment(id, revision, next)
}

// arm holds the next commit and returns its held and release channels.
func (g *commitGate) arm() (held <-chan struct{}, release chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed, g.held, g.release = true, make(chan struct{}), make(chan struct{})
	return g.held, g.release
}

// gatedController returns a new Controller of rt on the fixture's Store and
// authentication whose environment commits pass through a commitGate.
func gatedController(t *testing.T, f *fixture, rt environmentcontrol.Runtime) (*environmentcontrol.Controller, *commitGate) {
	t.Helper()
	c, err := environmentcontrol.New(environmentcontrol.Config{Store: f.store, Auth: f.engine, Runtime: rt, Now: f.clock.now})
	if err != nil {
		t.Fatal(err)
	}
	g := &commitGate{s: f.store}
	environmentcontrol.SetStoreFaults(c, f.store.Environment, g.commit)
	return c, g
}

// readyOn creates an environment for alice on c and reconciles it to Ready.
func readyOn(t *testing.T, f *fixture, c *environmentcontrol.Controller, name string) string {
	t.Helper()
	info, err := f.store.RetentionInfo(f.actors["alice"])
	if err != nil || !info.Enabled || info.Domain.Epoch == 0 {
		t.Fatalf("fixture has no real request domain: %+v, %v", info.Domain, err)
	}
	v, err := c.Create(f.actors["alice"], info.Domain, "create-"+name, testSpec(name))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(f.actors["alice"], v.ID)
	if err != nil || got.State.Phase != environment.Ready {
		t.Fatalf("environment = %+v, %v; want Ready", got.State, err)
	}
	return v.ID
}

func within[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not happen", what)
	}
	panic("unreachable")
}

func closeWithin(in environmentcontrol.Integration, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return in.Close(ctx)
}

func sessions(t *testing.T, f *fixture, c *environmentcontrol.Controller, id string) (open, unknown int) {
	t.Helper()
	v, err := c.Get(f.actors["alice"], id)
	if err != nil {
		t.Fatal(err)
	}
	return v.Sessions, v.Unknown
}

type execOutcome struct {
	res environmentcontrol.ExecResult
	err error
}

// TestIntegrationCloseJoinsExecSettlement: an exec whose caller left
// returns with its frame unsettled; its settlement untracks the frame and
// then commits the session's release. With that final commit held, the
// frame is no longer tracked, yet Integration.Close stays unsettled; once
// the commit returns, Close joins.
func TestIntegrationCloseJoinsExecSettlement(t *testing.T) {
	f := newFixture(t)
	f.runtime.mu.Lock()
	f.runtime.hold = true
	f.runtime.mu.Unlock()
	c, g := gatedController(t, f, f.runtime)
	id := readyOn(t, f, c, "dev")

	ctx, leave := context.WithCancel(context.Background())
	done := make(chan execOutcome, 1)
	go func() {
		res, err := c.Exec(ctx, f.actors["alice"], id, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
		done <- execOutcome{res, err}
	}()
	x := within(t, "exec frame", f.runtime.started)
	leave()
	out := within(t, "Exec return", done)
	if out.err != nil || out.res.Settled {
		t.Fatalf("Exec = %+v, %v; want an unsettled frame", out.res, out.err)
	}
	if n := environmentcontrol.SessionOwners(c); n != 1 {
		t.Fatalf("%d session owners after Exec returned, want its settlement", n)
	}

	held, release := g.arm()
	x.finish(environmentcontrol.ExecResult{Exited: true})
	within(t, "settlement's final commit", held)
	// The frame is untracked and its native Done is closed: neither is the
	// session's join.
	if environmentcontrol.TracksSession(c, id) {
		t.Fatal("settled frame still tracked")
	}
	in := environmentcontrol.Integration{Controller: c}
	if err := closeWithin(in, 50*time.Millisecond); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close before the final commit = %v, want unsettled", err)
	}
	close(release)
	if err := closeWithin(in, 5*time.Second); err != nil {
		t.Fatalf("Close after the final commit = %v", err)
	}
	if n := environmentcontrol.SessionOwners(c); n != 0 {
		t.Fatalf("%d session owners after Close", n)
	}
	if open, unknown := sessions(t, f, c, id); open != 0 || unknown != 0 {
		t.Fatalf("sessions = %d open, %d unknown; want the exit's release committed", open, unknown)
	}
}

// heldForwardRuntime is the fixture's model with forward frames that settle
// only when the test closes them.
type heldForwardRuntime struct {
	*fakeRuntime
	frames chan *heldForward
}

type heldForward struct {
	done     chan struct{}
	canceled atomic.Bool
}

func (x *heldForward) Done() <-chan struct{} { return x.done }
func (x *heldForward) Cancel()               { x.canceled.Store(true) }
func (x *heldForward) Result() environmentcontrol.ForwardResult {
	r := environmentcontrol.ForwardResult{Status: http.StatusOK}
	select {
	case <-x.done:
		r.Settled = true
	default:
	}
	return r
}

func (r heldForwardRuntime) Forward(w http.ResponseWriter, req *http.Request, ref environmentcontrol.Ref) environmentcontrol.Forward {
	w.WriteHeader(http.StatusOK)
	x := &heldForward{done: make(chan struct{})}
	r.frames <- x
	return x
}

// TestIntegrationCloseJoinsForwardSettlement is the exec test's forward
// counterpart: Forward returns with its exchange unsettled, and its
// settlement's release commit, after the frame's Done and untrack, holds
// Integration.Close.
func TestIntegrationCloseJoinsForwardSettlement(t *testing.T) {
	f := newFixture(t)
	rt := heldForwardRuntime{fakeRuntime: f.runtime, frames: make(chan *heldForward, 1)}
	c, g := gatedController(t, f, rt)
	id := readyOn(t, f, c, "web")

	res, err := c.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), f.actors["alice"], id)
	if err != nil || res.Settled {
		t.Fatalf("Forward = %+v, %v; want an unsettled exchange", res, err)
	}
	x := within(t, "forward frame", rt.frames)
	if n := environmentcontrol.SessionOwners(c); n != 1 {
		t.Fatalf("%d session owners after Forward returned", n)
	}
	held, release := g.arm()
	close(x.done)
	within(t, "settlement's release commit", held)
	if environmentcontrol.TracksSession(c, id) {
		t.Fatal("settled exchange still tracked")
	}
	in := environmentcontrol.Integration{Controller: c}
	if err := closeWithin(in, 50*time.Millisecond); err == nil {
		t.Fatal("Close returned before the release commit")
	}
	close(release)
	if err := closeWithin(in, 5*time.Second); err != nil {
		t.Fatalf("Close after the release commit = %v", err)
	}
	if open, unknown := sessions(t, f, c, id); open != 0 || unknown != 0 {
		t.Fatalf("sessions = %d open, %d unknown", open, unknown)
	}
}

// TestSessionsStoppedRefuseAtEntry: after Integration.Close, Exec and
// Forward refuse before authentication, any Store commit or any runtime
// call, and write nothing to the client.
func TestSessionsStoppedRefuseAtEntry(t *testing.T) {
	f := newFixture(t)
	c, g := gatedController(t, f, f.runtime)
	id := readyOn(t, f, c, "dev")
	if err := closeWithin(environmentcontrol.Integration{Controller: c}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	commits := g.commits.Load()
	_, _, _, execs := f.runtime.counts()
	f.runtime.mu.Lock()
	forwards := f.runtime.forwards
	f.runtime.mu.Unlock()

	req := environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}
	// A zero actor would be refused as unauthenticated by an admission
	// that reached authentication.
	for _, actor := range []auth.Actor{{}, f.actors["alice"]} {
		if _, err := c.Exec(context.Background(), actor, id, req); !errors.Is(err, environmentcontrol.ErrSessionsStopped) {
			t.Fatalf("Exec after Close = %v", err)
		}
		w := httptest.NewRecorder()
		if _, err := c.Forward(w, httptest.NewRequest(http.MethodGet, "/", nil), actor, id); !errors.Is(err, environmentcontrol.ErrSessionsStopped) {
			t.Fatalf("Forward after Close = %v", err)
		}
		if w.Body.Len() != 0 || len(w.Header()) != 0 {
			t.Fatal("refused Forward wrote to the client")
		}
	}
	_, _, _, execsAfter := f.runtime.counts()
	f.runtime.mu.Lock()
	forwardsAfter := f.runtime.forwards
	f.runtime.mu.Unlock()
	if g.commits.Load() != commits || execsAfter != execs || forwardsAfter != forwards {
		t.Fatal("a refused session reached the Store or the runtime")
	}
	if n := environmentcontrol.SessionOwners(c); n != 0 {
		t.Fatalf("%d session owners", n)
	}
}

// TestSessionAdmissionRacesStop: an admission counted before the stop and
// waiting for the lifecycle gate keeps its owner, so the join holds; once
// it holds the gate it sees the stop and commits no session.
func TestSessionAdmissionRacesStop(t *testing.T) {
	f := newFixture(t)
	c, g := gatedController(t, f, f.runtime)
	id := readyOn(t, f, c, "dev")
	entered, proceed := make(chan struct{}), make(chan struct{})
	environmentcontrol.SetBeforeGate(c, func() {
		close(entered)
		<-proceed
	})
	commits := g.commits.Load()
	done := make(chan execOutcome, 1)
	go func() {
		res, err := c.Exec(context.Background(), f.actors["alice"], id, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}})
		done <- execOutcome{res, err}
	}()
	within(t, "admission", entered)
	environmentcontrol.StopSessions(c)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := environmentcontrol.JoinSessions(c, ctx)
	cancel()
	if err == nil {
		t.Fatal("join returned while an admission was in flight")
	}
	close(proceed)
	if out := within(t, "Exec return", done); !errors.Is(out.err, environmentcontrol.ErrSessionsStopped) {
		t.Fatalf("Exec admitted across the stop = %+v, %v", out.res, out.err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := environmentcontrol.JoinSessions(c, ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, _, execs := f.runtime.counts(); execs != 0 {
		t.Fatal("the runtime was called")
	}
	if g.commits.Load() != commits {
		t.Fatal("a session was committed after the stop")
	}
	if open, unknown := sessions(t, f, c, id); open != 0 || unknown != 0 {
		t.Fatalf("sessions = %d open, %d unknown", open, unknown)
	}
}

// TestIntegrationCloseStopsRuntimeFirst: Close cancels a running exec's
// tracked frame. The model's frame ignores Cancel and settles only when the
// runtime is stopped, so Close joins only if it calls StopRuntime before it
// waits; without StopRuntime it stays unsettled until the frame settles.
func TestIntegrationCloseStopsRuntimeFirst(t *testing.T) {
	for _, stopRuntime := range []bool{true, false} {
		name := "without StopRuntime"
		if stopRuntime {
			name = "with StopRuntime"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.runtime.mu.Lock()
			f.runtime.hold = true
			f.runtime.mu.Unlock()
			c, _ := gatedController(t, f, f.runtime)
			id := readyOn(t, f, c, "dev")
			done := make(chan execOutcome, 1)
			go func() {
				res, err := c.Exec(context.Background(), f.actors["alice"], id, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
				done <- execOutcome{res, err}
			}()
			x := within(t, "exec frame", f.runtime.started)
			settle := func() { x.finish(environmentcontrol.ExecResult{Err: errors.New("runtime stopped")}) }
			in := environmentcontrol.Integration{Controller: c}
			if stopRuntime {
				in.StopRuntime = settle
				if err := closeWithin(in, 5*time.Second); err != nil {
					t.Fatalf("Close = %v", err)
				}
			} else {
				if err := closeWithin(in, 50*time.Millisecond); err == nil {
					t.Fatal("Close joined a frame that never settled")
				}
				settle()
				if err := closeWithin(in, 5*time.Second); err != nil {
					t.Fatalf("Close after the frame settled = %v", err)
				}
			}
			if !x.wasCanceled() {
				t.Fatal("Close did not cancel the tracked frame")
			}
			if out := within(t, "Exec return", done); out.err != nil || out.res.Exited {
				t.Fatalf("Exec = %+v, %v; want a started command without exit", out.res, out.err)
			}
			// No exit was reported: the session is recorded unknown, not
			// released, and nothing was deleted.
			if open, unknown := sessions(t, f, c, id); open != 0 || unknown != 1 {
				t.Fatalf("sessions = %d open, %d unknown", open, unknown)
			}
			if _, _, deletes, _ := f.runtime.counts(); deletes != 0 {
				t.Fatal("Close deleted the environment")
			}
		})
	}
}
