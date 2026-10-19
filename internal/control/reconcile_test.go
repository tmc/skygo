package control

import (
	"context"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type configuredBackend struct {
	starts int
}

func (*configuredBackend) Check(backend.Task) error { return nil }
func (b *configuredBackend) Start(context.Context, string, backend.Task) error {
	b.starts++
	return nil
}
func (*configuredBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{State: backend.Running}, nil
}
func (*configuredBackend) Cancel(context.Context, backend.Reference) error { return nil }
func (*configuredBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

func TestTargetRemovedAndRestored(t *testing.T) {
	for _, state := range []backend.State{backend.Queued, backend.Starting, backend.Running} {
		t.Run(string(state), func(t *testing.T) {
			s, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			b := &configuredBackend{}
			c, err := New(s, []backend.Target{{Name: "local", Binding: "test-binding", Backend: b}}, testEngine)
			if err != nil {
				t.Fatal(err)
			}
			j, err := c.Submit(testActor(), "one", backend.Task{Argv: []string{"work"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Update(j.ID, func(j *backend.Job) error {
				j.Status.State = state
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			// Configuration is fixed for each daemon. A replacement configuration
			// may omit the original target without changing durable execution state.
			missing, err := New(s, []backend.Target{{Name: "replacement", Binding: "test-binding", Backend: b}}, testEngine)
			if err != nil {
				t.Fatal(err)
			}
			if err := reconcileStep(t, missing); err != nil {
				t.Fatal(err)
			}
			got, err := c.Get(testActor(), j.ID)
			if err != nil || got.Status.State != state || got.Status.Reason != "target is not configured" {
				t.Fatalf("missing target: %+v, %v", got, err)
			}
			if err := reconcileStep(t, missing); err != nil {
				t.Fatal(err)
			}
			again, err := c.Get(testActor(), j.ID)
			if err != nil || !again.UpdatedAt.Equal(got.UpdatedAt) {
				t.Fatalf("unchanged diagnostic rewrote job: %+v, %v", again, err)
			}
			if b.starts != 0 {
				t.Fatalf("missing target started %d jobs", b.starts)
			}
			if err := reconcileStep(t, c); err != nil {
				t.Fatal(err)
			}
			got, err = c.Get(testActor(), j.ID)
			if err != nil || got.Status.State != backend.Running || got.Status.Reason != "" {
				t.Fatalf("restored target: %+v, %v", got, err)
			}
			if err := reconcileStep(t, c); err != nil {
				t.Fatal(err)
			}
			wantStarts := 0
			if state == backend.Queued {
				wantStarts = 1
			}
			if b.starts != wantStarts {
				t.Fatalf("starts = %d, want %d", b.starts, wantStarts)
			}
		})
	}
}

func TestCancelQueuedWithMissingTarget(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	b := &configuredBackend{}
	c, err := New(s, []backend.Target{{Name: "replacement", Binding: "test-binding", Backend: b}}, testEngine)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.Submit(testActor(), "one", backend.Task{Argv: []string{"work"}}, "removed", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	if err := reconcileStep(t, c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	if err := reconcileStep(t, c); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(testActor(), j.ID)
	if err != nil || got.Status.State != backend.Canceled || got.Status.Reason != "canceled before execution" || b.starts != 0 {
		t.Fatalf("cancel with missing target: %+v, starts=%d, %v", got, b.starts, err)
	}
}
