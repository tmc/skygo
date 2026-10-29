package environmentcontrol_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
)

func TestForwardRelaysDeclaredPort(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	w := httptest.NewRecorder()
	res, err := f.c.Forward(w, httptest.NewRequest("GET", "/events?x=1", nil), f.actors["alice"], v.ID)
	if err != nil || !res.Settled || res.Status != http.StatusOK {
		t.Fatalf("Forward = %+v, %v", res, err)
	}
	if got := w.Body.String(); got != "port 8080 path /events" {
		t.Fatalf("body %q", got)
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 0 {
		t.Fatalf("%d sessions after forward", g.Sessions)
	}
}

func TestForwardRefusalsWriteNothing(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	closed := f.ready(t, "alice", testSpec("closed"))
	if _, err := f.c.Delete(f.actors["alice"], closed.ID); err != nil {
		t.Fatal(err)
	}
	noPort := testSpec("quiet")
	noPort.Port = 0
	quiet := f.ready(t, "alice", noPort)
	for _, test := range []struct {
		name string
		who  string
		id   string
		want error
	}{
		{"no port", "alice", quiet.ID, environmentcontrol.ErrNoPort},
		{"foreign", "bob", v.ID, store.ErrNotFound},
		{"closed", "alice", closed.ID, environmentcontrol.ErrClosed},
		{"invalid id", "alice", "../other", environmentcontrol.ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			if _, err := f.c.Forward(w, httptest.NewRequest("GET", "/", nil), f.actors[test.who], test.id); !errors.Is(err, test.want) {
				t.Fatalf("Forward: %v, want %v", err, test.want)
			}
			if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Flushed {
				t.Fatal("refused forward wrote a response")
			}
		})
	}
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	if f.runtime.forwards != 0 {
		t.Fatalf("%d refused forwards reached the runtime", f.runtime.forwards)
	}
}

// blockingForward waits for the request's context, as the gateway does.
func blockingForward(w http.ResponseWriter, r *http.Request) environmentcontrol.ForwardResult {
	<-r.Context().Done()
	return environmentcontrol.ForwardResult{Err: r.Context().Err()}
}

func TestForwardDisconnectIsolated(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.hold = true
	exec := make(chan struct{})
	go func() {
		defer close(exec)
		_, _ = f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"serve"}})
	}()
	x := <-f.runtime.started
	t.Cleanup(func() {
		x.finish(environmentcontrol.ExecResult{Exited: true})
		<-exec
	})
	entered := make(chan struct{})
	f.runtime.forward = func(w http.ResponseWriter, r *http.Request) environmentcontrol.ForwardResult {
		close(entered)
		return blockingForward(w, r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() { cancel(); <-joined })
	go func() {
		defer close(joined)
		req := httptest.NewRequest("GET", "/stream", nil).WithContext(ctx)
		_, err := f.c.Forward(httptest.NewRecorder(), req, f.actors["alice"], v.ID)
		done <- err
	}()
	// Admission precedes the final context check. Disconnect only after
	// the original runtime call has started, not just its durable session.
	within(t, "forward runtime entry", entered)
	eventually(t, "forward session", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Sessions == 2
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-joined
	if x.wasCanceled() {
		t.Fatal("client disconnect canceled an unrelated exec")
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 1 {
		t.Fatalf("%d sessions after disconnect, want 1", g.Sessions)
	}
	x.finish(environmentcontrol.ExecResult{Exited: true})
	<-exec
}

// A caller that already left must not reach the runtime or retain a session.
func TestForwardPreCanceledDoesNotStart(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/stream", nil).WithContext(ctx)
	if _, err := f.c.Forward(w, req, f.actors["alice"], v.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Forward = %v, want context canceled", err)
	}
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Flushed {
		t.Fatal("pre-canceled forward wrote a response")
	}
	f.runtime.mu.Lock()
	calls := f.runtime.forwards
	f.runtime.mu.Unlock()
	if calls != 0 {
		t.Fatalf("%d pre-canceled forwards reached the runtime", calls)
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 0 {
		t.Fatalf("%d sessions after pre-canceled forward, want 0", g.Sessions)
	}
}

func TestDeleteClosesForward(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	entered := make(chan struct{})
	f.runtime.forward = func(w http.ResponseWriter, r *http.Request) environmentcontrol.ForwardResult {
		close(entered)
		return blockingForward(w, r)
	}
	done := make(chan environmentcontrol.ForwardResult, 1)
	joined := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); <-joined })
	go func() {
		defer close(joined)
		res, _ := f.c.Forward(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil).WithContext(ctx), f.actors["alice"], v.ID)
		done <- res
	}()
	within(t, "forward runtime entry", entered)
	eventually(t, "forward session", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Sessions == 1
	})
	if _, err := f.c.Delete(f.actors["alice"], v.ID); err != nil {
		t.Fatal(err)
	}
	if res := <-done; !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("forward after delete %+v", res)
	}
	<-joined
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleted)
}
