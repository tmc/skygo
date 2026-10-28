package environmentcontrol_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

func TestCreateReplaysBeforePlan(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	v := f.create(t, "alice", "create-1", spec)
	if v.State != (environment.State{Desired: environment.Present, Phase: environment.Creating}) || v.Principal != "alice" {
		t.Fatalf("created %+v", v)
	}
	same := f.create(t, "alice", "create-1", spec)
	if same.ID != v.ID {
		t.Fatalf("replay id %s, want %s", same.ID, v.ID)
	}
	spec.Image = "task:v2"
	if _, err := f.c.Create(f.actors["alice"], f.domain, "create-1", spec); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	// Bob's identical request is his own.
	if b := f.create(t, "bob", "create-1", testSpec("analysis")); b.ID == v.ID {
		t.Fatal("bob replayed alice's environment")
	}
	plans, ensures, _, _ := f.runtime.counts()
	if plans != 2 || ensures != 0 {
		t.Fatalf("plans %d ensures %d, want 2 and 0", plans, ensures)
	}
}

func TestCreateRefusals(t *testing.T) {
	f := newFixture(t)
	invalid := testSpec("analysis")
	invalid.Port = 80
	for _, test := range []struct {
		name  string
		actor auth.Actor
		spec  environment.Spec
		plan  error
		want  error
	}{
		{"zero actor", auth.Actor{}, testSpec("a"), nil, auth.ErrUnauthenticated},
		{"reader", f.actors["reader"], testSpec("a"), nil, auth.ErrPermissionDenied},
		{"invalid spec", f.actors["alice"], invalid, nil, environmentcontrol.ErrInvalid},
		{"unsupported", f.actors["alice"], testSpec("a"), environmentcontrol.ErrUnsupported, environmentcontrol.ErrUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.runtime.mu.Lock()
			f.runtime.planErr = test.plan
			f.runtime.mu.Unlock()
			if _, err := f.c.Create(test.actor, f.domain, "refused", test.spec); !errors.Is(err, test.want) {
				t.Fatalf("Create: %v, want %v", err, test.want)
			}
		})
	}
	p, err := f.store.Environments(0, 200)
	if err != nil || len(p.Environments) != 0 {
		t.Fatalf("refusals accepted %d environments: %v", len(p.Environments), err)
	}
}

func TestReconcileCommitsNativeBeforeEffects(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	first := f.runtime.lastEnsure()
	if !environment.ValidID(first.Nonce) || string(first.Native) != "planned" || first.Binding != "fake-binding" || first.Identity.ID != v.ID {
		t.Fatalf("first ensure %+v", first)
	}
	// The Ready observation's native state is committed before exec.
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	f.runtime.mu.Lock()
	x := f.runtime.execRef[0]
	f.runtime.mu.Unlock()
	if string(x.Native) != "uid-"+v.ID || x.Nonce != first.Nonce {
		t.Fatalf("exec ref native %q nonce %s", x.Native, x.Nonce)
	}
	// A new process keeps the identity and nonce.
	c, err := f.controller()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := f.runtime.lastEnsure(); r.Nonce != first.Nonce || r.Identity != first.Identity || string(r.Native) != "uid-"+v.ID {
		t.Fatalf("restarted ensure %+v", r)
	}
}

func TestEnsureErrorKeepsNative(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	lost := errors.New("create result lost")
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: environment.Unknown, Native: []byte("create sent")}, lost
	}
	if err := f.c.Reconcile(context.Background()); !errors.Is(err, lost) {
		t.Fatalf("Reconcile: %v", err)
	}
	f.get(t, "alice", v.ID, environment.Unknown)
	f.runtime.ensure = nil
	f.reconcile(t)
	if r := f.runtime.lastEnsure(); string(r.Native) != "create sent" {
		t.Fatalf("second ensure saw native %q", r.Native)
	}
	f.get(t, "alice", v.ID, environment.Ready)
}

func TestEnsureRefusedPhaseKeepsNative(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: environment.Deleted, Native: []byte("pod")}, nil
	}
	if err := f.c.Reconcile(context.Background()); err == nil {
		t.Fatal("Ensure reporting Deleted was accepted")
	}
	f.get(t, "alice", v.ID, environment.Creating)
	e, err := f.store.Environment(v.ID)
	if err != nil || !bytes.Contains(e.Checkpoint.Data, []byte(`"native":"cG9k"`)) {
		t.Fatalf("native not committed: %s %v", e.Checkpoint.Data, err)
	}
}

func TestRevokedAuthorityStopsEnsureNotDelete(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	e, err := f.store.Environment(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Revoke(f.config, f.actors["alice"].TokenID()); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revocation", func() bool { return f.engine.CheckDelegation(e.Authority) != nil })
	if err := f.c.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile ensured a revoked environment")
	}
	if _, ensures, _, _ := f.runtime.counts(); ensures != 0 {
		t.Fatalf("%d ensures under revoked authority", ensures)
	}
	if _, err := f.c.Delete(f.actors["legacy"], v.ID); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if _, _, deletes, _ := f.runtime.counts(); deletes != 1 {
		t.Fatalf("%d deletes, want 1", deletes)
	}
	f.get(t, "legacy", v.ID, environment.Deleted)
}

func TestDeleteScope(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	for _, test := range []struct {
		who  string
		want error
	}{{"bob", store.ErrNotFound}, {"reader", auth.ErrPermissionDenied}} {
		if _, err := f.c.Delete(f.actors[test.who], v.ID); !errors.Is(err, test.want) {
			t.Errorf("%s delete: %v, want %v", test.who, err, test.want)
		}
	}
	f.get(t, "alice", v.ID, environment.Ready)
	got, err := f.c.Delete(f.actors["legacy"], v.ID)
	if err != nil || got.State != (environment.State{Desired: environment.Absent, Phase: environment.Deleting}) {
		t.Fatalf("admin delete %+v, %v", got.State, err)
	}
}

func TestDeleteLifecycle(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	d, err := f.c.Delete(f.actors["alice"], v.ID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.c.Delete(f.actors["alice"], v.ID)
	if err != nil || again.State != d.State || again.UpdatedAt != d.UpdatedAt {
		t.Fatalf("second delete %+v, %v", again, err)
	}
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}}); !errors.Is(err, environmentcontrol.ErrClosed) {
		t.Fatalf("exec after delete: %v", err)
	}
	// Unknown, then Deleted: Deleted follows only Deleting.
	results := []environment.Phase{environment.Unknown, environment.Deleted, environment.Deleted}
	f.runtime.remove = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		if string(r.Native) != "uid-"+v.ID {
			t.Errorf("delete native %q", r.Native)
		}
		p := results[0]
		results = results[1:]
		return environmentcontrol.Observation{Phase: p}, nil
	}
	for _, want := range []environment.Phase{environment.Unknown, environment.Deleting, environment.Deleted} {
		f.reconcile(t)
		f.get(t, "alice", v.ID, want)
	}
	_, ensures, deletes, _ := f.runtime.counts()
	f.reconcile(t)
	if _, e2, d2, _ := f.runtime.counts(); e2 != ensures || d2 != deletes {
		t.Fatal("runtime called for a deleted environment")
	}
}

func TestDeleteWithheldWhileFrameUnsettled(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	done := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"sleep", "1000"}})
		done <- res
	}()
	x := <-f.runtime.started
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, "frame cancel", x.wasCanceled)
	res := <-done
	if res.Settled || res.Exited {
		t.Fatalf("canceled exec %+v", res)
	}
	f.reconcile(t)
	if g := f.get(t, "alice", v.ID, environment.Deleting); g.Unknown != 1 {
		t.Fatalf("unknown sessions %d, want 1", g.Unknown)
	}
	x.finish(environmentcontrol.ExecResult{})
	eventually(t, "frame settle", func() bool {
		f.reconcile(t)
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.State.Phase == environment.Deleted
	})
	if g := f.get(t, "alice", v.ID, environment.Deleted); g.Unknown != 0 || g.Sessions != 0 {
		t.Fatalf("deleted environment keeps sessions %+v", g)
	}
}

func TestIdleExpiry(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	spec.IdleTimeoutSeconds = environment.MinIdleTimeoutSeconds
	v := f.ready(t, "alice", spec)
	f.clock.advance(time.Duration(spec.IdleTimeoutSeconds)*time.Second - time.Second)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Ready)
	f.clock.advance(time.Second)
	f.reconcile(t)
	g, err := f.c.Get(f.actors["alice"], v.ID)
	if err != nil || !g.Expired || g.State != (environment.State{Desired: environment.Absent, Phase: environment.Deleted}) {
		t.Fatalf("expired %+v, %v", g, err)
	}
	if _, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}}); !errors.Is(err, environmentcontrol.ErrClosed) {
		t.Fatalf("exec after expiry: %v", err)
	}
}

func TestExpiryWaitsForSessions(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	spec.IdleTimeoutSeconds = environment.MinIdleTimeoutSeconds
	timeout := time.Duration(spec.IdleTimeoutSeconds) * time.Second
	v := f.ready(t, "alice", spec)
	f.runtime.hold = true
	done := make(chan error, 1)
	go func() {
		_, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
		done <- err
	}()
	x := <-f.runtime.started
	f.clock.advance(10 * timeout)
	f.reconcile(t)
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Expired || g.Sessions != 1 {
		t.Fatalf("expired with an open session: %+v", g)
	}
	x.finish(environmentcontrol.ExecResult{Exited: true})
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// The idle period restarts when the session ends.
	f.clock.advance(timeout - time.Second)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Ready)
	f.clock.advance(time.Second)
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleted)
}

func TestUnknownSessionBlocksExpiry(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	spec.IdleTimeoutSeconds = environment.MinIdleTimeoutSeconds
	v := f.ready(t, "alice", spec)
	f.runtime.hold = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(ctx, f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
		done <- res
	}()
	x := <-f.runtime.started
	cancel()
	<-done
	// The canceled frame settles without an exit status.
	x.finish(environmentcontrol.ExecResult{Err: errors.New("canceled")})
	eventually(t, "frame untracked", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Unknown == 1 && g.Sessions == 0
	})
	f.clock.advance(1000 * time.Hour)
	f.reconcile(t)
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Expired {
		t.Fatal("expired with an unknown session")
	}
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	// Deleted waits for the settled frame to be untracked.
	eventually(t, "deleted", func() bool {
		f.reconcile(t)
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.State.Phase == environment.Deleted
	})
	if g := f.get(t, "alice", v.ID, environment.Deleted); g.Unknown != 0 {
		t.Fatalf("deleted environment keeps %d unknown sessions", g.Unknown)
	}
}

func TestRestartRecordsExecUnknown(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	go f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
	<-f.runtime.started
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	f.runtime.forward = func(w http.ResponseWriter, r *http.Request) environmentcontrol.ForwardResult {
		<-block
		return environmentcontrol.ForwardResult{}
	}
	go f.c.Forward(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), f.actors["alice"], v.ID)
	eventually(t, "two sessions", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Sessions == 2
	})
	// A new process cannot know whether the command stopped.
	c, err := f.controller()
	if err != nil {
		t.Fatal(err)
	}
	g, err := c.Get(f.actors["alice"], v.ID)
	if err != nil || g.Unknown != 1 || g.Sessions != 0 {
		t.Fatalf("after restart %+v, %v", g, err)
	}
}

func TestAdmissionOrderedAgainstExpiry(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	spec.IdleTimeoutSeconds = environment.MinIdleTimeoutSeconds
	v := f.ready(t, "alice", spec)
	var violations []string
	f.runtime.onExec = func(r environmentcontrol.Ref) {
		e, err := f.store.Environment(r.Identity.ID)
		if err != nil || e.State.Desired != environment.Present {
			violations = append(violations, "exec started in a deleting environment")
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 20 {
			f.clock.advance(time.Duration(spec.IdleTimeoutSeconds) * time.Second)
			f.c.Reconcile(context.Background())
		}
	}()
	admitted, closed := 0, 0
	for range 40 {
		_, err := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"true"}})
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, environmentcontrol.ErrClosed):
			closed++
		default:
			t.Fatal(err)
		}
	}
	<-done
	if len(violations) != 0 {
		t.Fatal(violations)
	}
	if admitted+closed != 40 {
		t.Fatalf("admitted %d closed %d", admitted, closed)
	}
}

// The tests below model the declare-then-dispatch protocol of
// ErrWritePending. They show the Controller's ordering only; they are not
// evidence about any native API.

func TestDeclaredWriteDispatchedOnlyAfterCommit(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	var steps []string
	f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		if string(r.Native) != "declared" {
			steps = append(steps, "declare")
			return environmentcontrol.Observation{Phase: environment.Creating, Native: []byte("declared")}, environmentcontrol.ErrWritePending
		}
		// The declaration must be durable before the send.
		e, err := f.store.Environment(r.Identity.ID)
		if err != nil || !bytes.Contains(e.Checkpoint.Data, []byte(`"native":"ZGVjbGFyZWQ="`)) {
			t.Errorf("send before the declaration committed: %s %v", e.Checkpoint.Data, err)
		}
		steps = append(steps, "send")
		return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte("uid")}, nil
	}
	f.reconcile(t)
	if fmt.Sprint(steps) != "[declare send]" {
		t.Fatalf("steps %v", steps)
	}
	f.get(t, "alice", v.ID, environment.Ready)
}

// An oversized Native is a runtime fault, not a Store fault: it can never be
// committed, so the environment gets no runtime call again in this process.
func TestOversizedNativeNeverDispatched(t *testing.T) {
	f := newFixture(t)
	f.create(t, "alice", "create-1", testSpec("analysis"))
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Native: make([]byte, environmentcontrol.MaxNativeBytes+1)}, environmentcontrol.ErrWritePending
	}
	for range 3 {
		if err := f.c.Reconcile(context.Background()); err == nil {
			t.Fatal("Reconcile accepted an uncommittable declaration")
		}
	}
	if _, ensures, _, _ := f.runtime.counts(); ensures != 1 {
		t.Fatalf("%d ensures, want 1: a write was dispatched without its declaration", ensures)
	}
}

func TestPendingWritesBoundedPerPass(t *testing.T) {
	f := newFixture(t)
	f.create(t, "alice", "create-1", testSpec("analysis"))
	n := 0
	f.runtime.ensure = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		n++
		return environmentcontrol.Observation{Native: []byte(fmt.Sprint("declared-", n))}, environmentcontrol.ErrWritePending
	}
	if err := f.c.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("%d runtime calls in one pass, want 4", n)
	}
}

func TestDeleteDuringEnsureKeepsLateIdentity(t *testing.T) {
	f := newFixture(t)
	v := f.create(t, "alice", "create-1", testSpec("analysis"))
	f.runtime.ensure = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		// Absent is committed while the original creation is in flight.
		if _, err := f.c.Delete(f.actors["alice"], r.Identity.ID); err != nil {
			t.Error(err)
		}
		return environmentcontrol.Observation{Phase: environment.Ready, Native: []byte("uid-late")}, nil
	}
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleting)
	f.runtime.remove = func(r environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		if string(r.Native) != "uid-late" {
			t.Errorf("delete without the late identity: %q", r.Native)
		}
		return environmentcontrol.Observation{Phase: environment.Deleted}, nil
	}
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleted)
}

func TestRestartWithUnresolvedCreateStaysDeleting(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.c.Exec(ctx, f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"work"}})
	}()
	<-f.runtime.started
	cancel()
	<-done
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	// The runtime finds nothing, but an original creation is unresolved.
	f.runtime.remove = func(environmentcontrol.Ref) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: environment.Deleting}, nil
	}
	c, err := f.controller()
	if err != nil {
		t.Fatal(err)
	}
	f.c = c
	for range 3 {
		f.reconcile(t)
	}
	if g := f.get(t, "alice", v.ID, environment.Deleting); g.Unknown != 1 {
		t.Fatalf("unknown sessions %d, want 1", g.Unknown)
	}
}

func TestExpiresAt(t *testing.T) {
	f := newFixture(t)
	spec := testSpec("analysis")
	spec.IdleTimeoutSeconds = environment.MinIdleTimeoutSeconds
	v := f.ready(t, "alice", spec)
	at, ok := v.ExpiresAt()
	if want := f.clock.now().Add(time.Duration(spec.IdleTimeoutSeconds) * time.Second); !ok || !at.Equal(want) {
		t.Fatalf("ExpiresAt = %v, %v; want %v", at, ok, want)
	}
	v.Unknown = 1
	if _, ok := v.ExpiresAt(); ok {
		t.Fatal("expiry reported with an unknown session")
	}
	never := f.ready(t, "alice", testSpec("forever"))
	if _, ok := never.ExpiresAt(); ok {
		t.Fatal("expiry reported without a timeout")
	}
}
