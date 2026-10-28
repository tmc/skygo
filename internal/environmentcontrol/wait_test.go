package environmentcontrol_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
)

// holdPass starts a Reconcile pass whose Ensure calls report Ready with
// native and wait until finish is called. It returns once the first Ensure
// has started, so the pass holds the lifecycle gate.
func (f *fixture) holdPass(t *testing.T, native string) (finish func(), passed <-chan error) {
	t.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	finish = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(finish)
	first := sync.OnceFunc(func() { close(entered) })
	f.runtime.mu.Lock()
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		first()
		<-gate
		return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte(native)}, nil
	}
	f.runtime.mu.Unlock()
	errc := make(chan error, 1)
	go func() { errc <- f.c.Reconcile(context.Background()) }()
	<-entered
	return finish, errc
}

// waiting returns a channel closed when the next session admission is about
// to wait for the lifecycle gate, after its first authority check.
func (f *fixture) waiting() <-chan struct{} {
	ch := make(chan struct{})
	environmentcontrol.SetBeforeGate(f.c, sync.OnceFunc(func() { close(ch) }))
	return ch
}

// start runs one session of who in environment id under ctx, a forward if
// forward is set and an exec otherwise. The channel yields its error.
func (f *fixture) start(ctx context.Context, who, id string, forward bool) <-chan error {
	errc := make(chan error, 1)
	go func() {
		var err error
		if forward {
			req := httptest.NewRequest("GET", "/", nil).WithContext(ctx)
			_, err = f.c.Forward(httptest.NewRecorder(), req, f.actors[who], id)
		} else {
			_, err = f.c.Exec(ctx, f.actors[who], id, environmentcontrol.ExecRequest{Argv: []string{"true"}})
		}
		errc <- err
	}()
	return errc
}

// engineState waits until the Engine's check of the admin is want, or
// succeeds for a nil want.
func (f *fixture) engineState(t *testing.T, want error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := f.engine.Check(f.actors["legacy"], auth.Read)
		if want == nil && err == nil || want != nil && errors.Is(err, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("engine check = %v, want %v", err, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var sessionKinds = []struct {
	name    string
	forward bool
}{{"exec", false}, {"forward", true}}

func TestAdmissionStopsWaitingWhenCanceled(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			const held = "uid-held-0006"
			finish, passed := f.holdPass(t, held)
			waiting := f.waiting()
			ctx, cancel := context.WithCancel(context.Background())
			done := f.start(ctx, "alice", v.ID, k.forward)
			<-waiting
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("session = %v, want canceled", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a canceled admission kept waiting for the held pass")
			}
			if !environmentcontrol.LifecycleBusy(f.c) {
				t.Error("the held pass lost the lifecycle gate to a canceled admission")
			}
			finish()
			if err := <-passed; err != nil {
				t.Fatalf("held pass: %v", err)
			}
			native, sessions, err := f.durable(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if native != held || sessions != 0 {
				t.Errorf("native %q and %d sessions, want the held pass's %q and none", native, sessions, held)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions for a canceled caller", len(refs))
			}
		})
	}
}

// With the gate free, both cases of the gate's select are ready for a caller
// whose ctx has ended, and select chooses either. Each test repeats the call
// so that both choices are likely taken; nothing here proves that both were.
const readyChoices = 64

func TestAdmissionRefusesCanceledCaller(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			before, err := f.store.Environment(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			for range readyChoices {
				if err := <-f.start(ctx, "alice", v.ID, k.forward); !errors.Is(err, context.Canceled) {
					t.Fatalf("session = %v, want canceled", err)
				}
			}
			after, err := f.store.Environment(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Revision != before.Revision {
				t.Errorf("revision %d, want %d: admission committed a session for a canceled caller", after.Revision, before.Revision)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions for a canceled caller", len(refs))
			}
		})
	}
}

func TestReconcileRefusesCanceledCaller(t *testing.T) {
	f := newFixture(t)
	f.ready(t, "alice", testSpec("analysis"))
	_, ensures, _, _ := f.runtime.counts()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range readyChoices {
		if err := f.c.Reconcile(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcile = %v, want canceled", err)
		}
	}
	if _, n, _, _ := f.runtime.counts(); n != ensures {
		t.Errorf("%d Ensure calls for a canceled pass", n-ensures)
	}
}

func TestCancellationAfterAdmissionStartsNothing(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			faults := f.faults("")
			ctx, cancel := context.WithCancel(context.Background())
			// Nothing is pending, so the first commit is the session's:
			// it lands, and the caller has left by the time it returns.
			faults.mu.Lock()
			faults.onCommit = sync.OnceFunc(cancel)
			faults.mu.Unlock()
			if err := <-f.start(ctx, "alice", v.ID, k.forward); !errors.Is(err, context.Canceled) {
				t.Errorf("session = %v, want canceled", err)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions for a caller that left after admission", len(refs))
			}
			if _, sessions, err := f.durable(v.ID); err != nil || sessions != 0 {
				t.Errorf("%d sessions recorded (%v), want the unstarted session ended", sessions, err)
			}
		})
	}
}

func TestAdmissionCanceledDuringFlushAdmitsNothing(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			const uid = "uid-pending-0007"
			f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
				return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte(uid)}, nil
			}
			faults := f.faults(uid)
			faults.set(true, false, 0)
			f.reconcileFails(t, 1)
			faults.set(false, false, 0)
			ctx, cancel := context.WithCancel(context.Background())
			commits := 0
			faults.mu.Lock()
			faults.onCommit = func() {
				commits++
				cancel()
			}
			faults.mu.Unlock()
			if err := <-f.start(ctx, "alice", v.ID, k.forward); !errors.Is(err, context.Canceled) {
				t.Errorf("session = %v, want canceled", err)
			}
			native, sessions, err := f.durable(v.ID)
			if err != nil {
				t.Fatal(err)
			}
			if native != uid || sessions != 0 {
				t.Errorf("native %q and %d sessions, want the flushed %q and none", native, sessions, uid)
			}
			if commits != 1 {
				t.Errorf("%d commits, want only the flush: a session was committed for a caller canceled during admission", commits)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions for a canceled caller", len(refs))
			}
		})
	}
}

// Each authority change below changes alice's authority in the fixture's
// real Engine, waits until the Engine reports the change, and returns the
// error a Mutate check now gives and a function restoring what can be
// restored.

func changeRole(t *testing.T, f *fixture) (error, func()) {
	if err := auth.SetRole(f.config, "alice", auth.Reader); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		a, err := f.engine.Check(f.actors["alice"], auth.Read)
		if err == nil && a.Role() == auth.Reader {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice is not a reader: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return auth.ErrPermissionDenied, func() {}
}

func revokeCredential(t *testing.T, f *fixture) (error, func()) {
	tokens, err := auth.Tokens(f.config)
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range tokens {
		if tok.Principal == "alice" {
			if err := auth.Revoke(f.config, tok.ID); err != nil {
				t.Fatal(err)
			}
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := f.engine.Check(f.actors["alice"], auth.Read)
		if errors.Is(err, auth.ErrUnauthenticated) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice's credential still accepted: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return auth.ErrUnauthenticated, func() {}
}

func makeUnavailable(t *testing.T, f *fixture) (error, func()) {
	// A credential file others may read is refused, and the Engine
	// becomes unavailable until it is private again.
	if err := os.Chmod(f.config, 0o644); err != nil {
		t.Fatal(err)
	}
	f.engineState(t, auth.ErrUnavailable)
	return auth.ErrUnavailable, func() {
		if err := os.Chmod(f.config, 0o600); err != nil {
			t.Fatal(err)
		}
		f.engineState(t, nil)
	}
}

// authorityChanges are the changes of authority a session must observe, for
// both kinds of session. recovers reports whether alice may mutate again
// after the restore.
var authorityChanges = []struct {
	name     string
	forward  bool
	change   func(*testing.T, *fixture) (error, func())
	recovers bool
}{
	{"exec_role", false, changeRole, false},
	{"forward_role", true, changeRole, false},
	{"exec_credential", false, revokeCredential, false},
	{"forward_credential", true, revokeCredential, false},
	{"exec_unavailable", false, makeUnavailable, true},
	{"forward_unavailable", true, makeUnavailable, true},
}

func TestAdmissionRechecksAuthorityAfterWait(t *testing.T) {
	for _, tc := range authorityChanges {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			b := f.ready(t, "bob", testSpec("beta"))
			faults := f.faults("")
			finish, passed := f.holdPass(t, "uid-held-0008")
			waiting := f.waiting()
			done := f.start(context.Background(), "alice", v.ID, tc.forward)
			<-waiting
			want, restore := tc.change(t, f)
			finish()
			<-passed // the pass's own authority checks may fail meanwhile
			if err := <-done; !errors.Is(err, want) {
				t.Errorf("session = %v, want %v: a session was admitted under authority that changed while it waited", err, want)
			}
			if n := faults.sessions(); n != 0 {
				t.Errorf("%d session commits, want none: a session was committed under authority that changed while it waited", n)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions under changed authority", len(refs))
			}
			if _, sessions, err := f.durable(v.ID); err != nil || sessions != 0 {
				t.Errorf("%d sessions recorded (%v), want none", sessions, err)
			}
			restore()
			// Another principal is unaffected, and nothing was revoked.
			if err := <-f.start(context.Background(), "bob", b.ID, false); err != nil {
				t.Errorf("bystander session: %v", err)
			}
			if g := f.get(t, "legacy", v.ID, environment.Ready); g.State.Desired != environment.Present {
				t.Errorf("state %+v, want the environment kept Present", g.State)
			}
		})
	}
}

// atSessionCommit pauses the next commit through faults, which must be a
// session's, until proceed is called; reached is closed when it pauses. If
// next is set, it runs before the commit after that one.
func atSessionCommit(t *testing.T, faults *storeFaults, next func()) (reached <-chan struct{}, proceed func()) {
	ch, gate := make(chan struct{}), make(chan struct{})
	proceed = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(proceed)
	n := 0
	faults.mu.Lock()
	faults.onCommit = func() {
		n++
		switch {
		case n == 1:
			close(ch)
			<-gate
		case n == 2 && next != nil:
			next()
		}
	}
	faults.mu.Unlock()
	return ch, proceed
}

// awaitCommit waits until reached is closed. It fails if done yields first
// or ten seconds pass.
func awaitCommit(t *testing.T, reached <-chan struct{}, done <-chan error) {
	t.Helper()
	select {
	case <-reached:
	case err := <-done:
		t.Fatalf("session = %v before its commit", err)
	case <-time.After(10 * time.Second):
		t.Fatal("no session commit")
	}
}

// TestDispatchRechecksAuthorityAfterCommit changes alice's authority in the
// real Engine during her session's commit, after admission's own checks,
// and lets the commit land in the real Store. The runtime is not called, the
// session ends unstarted, and the caller gets the Engine's error.
func TestDispatchRechecksAuthorityAfterCommit(t *testing.T) {
	for _, tc := range authorityChanges {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			b := f.ready(t, "bob", testSpec("beta"))
			faults := f.faults("")
			reached, proceed := atSessionCommit(t, faults, nil)
			done := f.start(context.Background(), "alice", v.ID, tc.forward)
			awaitCommit(t, reached, done)
			want, restore := tc.change(t, f)
			proceed()
			if err := <-done; !errors.Is(err, want) {
				t.Errorf("session = %v, want %v: a session started under authority that changed during its commit", err, want)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions under authority that changed during the session's commit", len(refs))
			}
			if _, sessions, err := f.durable(v.ID); err != nil || sessions != 0 {
				t.Errorf("%d sessions recorded (%v), want the unstarted session ended", sessions, err)
			}
			restore()
			// Another principal is unaffected, and nothing was revoked.
			if err := <-f.start(context.Background(), "bob", b.ID, false); err != nil {
				t.Errorf("bystander session: %v", err)
			}
			if g := f.get(t, "legacy", v.ID, environment.Ready); g.State.Desired != environment.Present {
				t.Errorf("state %+v, want the environment kept Present", g.State)
			}
			if tc.recovers {
				if err := <-f.start(context.Background(), "alice", v.ID, tc.forward); err != nil {
					t.Errorf("session after the Engine recovered: %v", err)
				}
			}
		})
	}
}

// TestDispatchAfterCommitUnchangedAuthority is the control: a session whose
// commit was paused in the same way, under unchanged authority, starts.
func TestDispatchAfterCommitUnchangedAuthority(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			faults := f.faults("")
			reached, proceed := atSessionCommit(t, faults, nil)
			done := f.start(context.Background(), "alice", v.ID, k.forward)
			awaitCommit(t, reached, done)
			proceed()
			if err := <-done; err != nil {
				t.Errorf("session = %v, want it started", err)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 1 {
				t.Errorf("%d runtime sessions, want 1", len(refs))
			}
		})
	}
}

// TestDispatchRefusalCleanupFailure refuses the session's release in the
// Store after a refusal at dispatch: the caller gets both errors, and the
// session stays recorded rather than being reported ended.
func TestDispatchRefusalCleanupFailure(t *testing.T) {
	for _, k := range sessionKinds {
		t.Run(k.name, func(t *testing.T) {
			f := newFixture(t)
			v := f.ready(t, "alice", testSpec("analysis"))
			b := f.ready(t, "bob", testSpec("beta"))
			faults := f.faults("")
			reached, proceed := atSessionCommit(t, faults, func() { faults.set(true, false, 0) })
			done := f.start(context.Background(), "alice", v.ID, k.forward)
			awaitCommit(t, reached, done)
			want, _ := changeRole(t, f)
			proceed()
			err := <-done
			if !errors.Is(err, want) || !errors.Is(err, errFault) {
				t.Errorf("session = %v, want %v joined with the release's %v", err, want, errFault)
			}
			if refs := f.runtime.sessionRefs(); len(refs) != 0 {
				t.Errorf("%d runtime sessions under refused authority", len(refs))
			}
			if _, sessions, err := f.durable(v.ID); err != nil || sessions != 1 {
				t.Errorf("%d sessions recorded (%v), want the session kept after its release failed", sessions, err)
			}
			faults.set(false, false, 0)
			if err := <-f.start(context.Background(), "bob", b.ID, false); err != nil {
				t.Errorf("bystander session: %v", err)
			}
		})
	}
}
