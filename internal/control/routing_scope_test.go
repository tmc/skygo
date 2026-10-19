package control_test

import (
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
)

type scopedRouteSpy struct{ logs int }

func (*scopedRouteSpy) Check(backend.Task) error                          { return nil }
func (*scopedRouteSpy) Start(context.Context, string, backend.Task) error { return nil }
func (*scopedRouteSpy) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{State: backend.Running}, nil
}
func (*scopedRouteSpy) Cancel(context.Context, backend.Reference) error { return nil }
func (b *scopedRouteSpy) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	b.logs++
	return backend.LogChunk{Data: []byte("owned")}, nil
}

func TestScopedRoutingDoesNotExposeForeignJob(t *testing.T) {
	f := newScopedControl(t)
	b := &scopedRouteSpy{}
	c, err := control.New(f.s, []backend.Target{{Name: "local", Binding: "test-binding", Backend: b}}, f.e)
	if err != nil {
		t.Fatal(err)
	}
	job, err := c.Submit(f.actors["bob"], "one", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Update(job.ID, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Logs(context.Background(), f.actors["alice"], job.ID, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := c.Cancel(f.actors["alice"], job.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if b.logs != 0 {
		t.Fatal("foreign job reached backend")
	}
	after, err := c.Get(f.actors["bob"], job.ID)
	if err != nil || after.CancelRequested {
		t.Fatalf("foreign cancel changed intent: %+v %v", after, err)
	}
	if _, err := c.Logs(context.Background(), f.actors["bob"], job.ID, 0); err != nil || b.logs != 1 {
		t.Fatalf("owner logs calls=%d err=%v", b.logs, err)
	}
}
