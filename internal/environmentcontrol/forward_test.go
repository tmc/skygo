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
	exec := make(chan environmentcontrol.ExecResult, 1)
	go func() {
		res, _ := f.c.Exec(context.Background(), f.actors["alice"], v.ID, environmentcontrol.ExecRequest{Argv: []string{"serve"}})
		exec <- res
	}()
	x := <-f.runtime.started
	f.runtime.forward = blockingForward
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		req := httptest.NewRequest("GET", "/stream", nil).WithContext(ctx)
		_, err := f.c.Forward(httptest.NewRecorder(), req, f.actors["alice"], v.ID)
		done <- err
	}()
	eventually(t, "forward session", func() bool {
		g, err := f.c.Get(f.actors["alice"], v.ID)
		return err == nil && g.Sessions == 2
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if x.wasCanceled() {
		t.Fatal("client disconnect canceled an unrelated exec")
	}
	if g := f.get(t, "alice", v.ID, environment.Ready); g.Sessions != 1 {
		t.Fatalf("%d sessions after disconnect, want 1", g.Sessions)
	}
	x.finish(environmentcontrol.ExecResult{Exited: true})
	<-exec
}

func TestDeleteClosesForward(t *testing.T) {
	f := newFixture(t)
	v := f.ready(t, "alice", testSpec("analysis"))
	f.runtime.forward = blockingForward
	done := make(chan environmentcontrol.ForwardResult, 1)
	go func() {
		res, _ := f.c.Forward(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil), f.actors["alice"], v.ID)
		done <- res
	}()
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
	f.reconcile(t)
	f.get(t, "alice", v.ID, environment.Deleted)
}
