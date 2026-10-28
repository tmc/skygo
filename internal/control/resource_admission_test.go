package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
)

func resourceAdmissionFixture(t *testing.T) (*Control, backend.Job, string) {
	t.Helper()
	s := routeStore(t)
	b, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(s, []backend.Target{{Name: "local", Binding: binding, Backend: b}}, testEngine, Config{LocalBackend: b})
	if err != nil {
		t.Fatal(err)
	}
	count := filepath.Join(t.TempDir(), "launches")
	j, err := c.Submit(testActor(), "resource-wait", backend.Task{Argv: []string{"/bin/sh", "-c", `printf x >> "$1"; printf completed`, "resource-job", count}})
	if err != nil {
		t.Fatal(err)
	}
	j, started, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, time.Now())
	if err != nil || !started {
		t.Fatalf("BeginAttempt = %v, %v", started, err)
	}
	return c, j, count
}

// The refused Start result is MODELED; the Store transition and the subsequent
// same-ID local execution are real. No timing error or ambiguous Start error
// supplies the pre-execution proof.
func TestResourceStartDeferred(t *testing.T) {
	c, j, count := resourceAdmissionFixture(t)
	op := operation{kind: startOperation, job: j}
	s := &loopState{workers: []targetWorker{{target: c.targets[0], pending: &op}}}
	if err := c.integrate(s, operationResult{operation: op, err: fmt.Errorf("admit: %w", backend.ErrResourcesUnavailable)}); err != nil {
		t.Fatal(err)
	}
	got, err := c.store.Get(j.ID)
	if err != nil || got.Status.State != backend.Queued || got.Status.Reason != "waiting for local resources" {
		t.Fatalf("deferred = %+v, %v", got, err)
	}
	if got.CurrentAttemptID() != j.CurrentAttemptID() || !reflect.DeepEqual(got.Task, j.Task) || got.TargetBinding != j.TargetBinding {
		t.Fatal("deferral changed accepted identity")
	}
	if _, err := os.Stat(count); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workload ran before admission: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for got.Status.State != backend.Succeeded {
		if err := reconcileStep(t, c); err != nil {
			t.Fatal(err)
		}
		got, err = c.store.Get(j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status.State.Terminal() && got.Status.State != backend.Succeeded {
			t.Fatalf("unexpected final status: %+v", got.Status)
		}
		if time.Now().After(deadline) {
			t.Fatalf("job stayed %s", got.Status.State)
		}
		time.Sleep(time.Millisecond)
	}
	data, err := os.ReadFile(count)
	if err != nil || string(data) != "x" {
		t.Fatalf("launches = %q, %v; want one", data, err)
	}
}

func TestResourceStartDeferralScope(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Control, *backend.Job, *operationResult)
	}{
		{"ordinary error", func(_ *Control, _ *backend.Job, r *operationResult) { r.err = errors.New("start reply lost") }},
		{"deadline", func(_ *Control, _ *backend.Job, r *operationResult) { r.err = context.DeadlineExceeded }},
		{"binding failure", func(_ *Control, _ *backend.Job, r *operationResult) { r.notStarted = true }},
		{"retention", func(c *Control, _ *backend.Job, _ *operationResult) { c.retention.enabled = true }},
		{"retry", func(_ *Control, j *backend.Job, _ *operationResult) {
			j.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2}
		}},
		{"workspace", func(_ *Control, j *backend.Job, _ *operationResult) { j.Task.Workdir = &artifact.Ref{} }},
		{"running", func(_ *Control, j *backend.Job, _ *operationResult) { j.Status.State = backend.Running }},
		{"instance", func(_ *Control, j *backend.Job, _ *operationResult) { j.Status.Instance = "original" }},
		{"foreign target", func(_ *Control, j *backend.Job, _ *operationResult) { j.Target = "other" }},
		{"borrowed backend", func(c *Control, _ *backend.Job, _ *operationResult) { c.localBackend = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, j, count := resourceAdmissionFixture(t)
			before, err := c.store.Get(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			r := operationResult{operation: operation{kind: startOperation, job: j}, err: backend.ErrResourcesUnavailable}
			tt.change(c, &j, &r)
			if changed, err := c.deferResourceStart(j, r); changed || err != nil {
				t.Fatalf("out-of-scope deferral = %v, %v", changed, err)
			}
			after, err := c.store.Get(j.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("refused deferral changed Store: %+v, %v", after, err)
			}
			if _, err := os.Stat(count); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("workload ran: %v", err)
			}
		})
	}
}

func TestResourceStartDeferralKeepsCancellation(t *testing.T) {
	c, j, count := resourceAdmissionFixture(t)
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.deferResourceStart(j, operationResult{err: backend.ErrResourcesUnavailable}); !changed || err != nil {
		t.Fatalf("defer canceled request = %v, %v", changed, err)
	}
	if err := reconcileStep(t, c); err != nil {
		t.Fatal(err)
	}
	got, err := c.store.Get(j.ID)
	if err != nil || got.Status.State != backend.Canceled || !got.CancelRequested {
		t.Fatalf("canceled queued request = %+v, %v", got, err)
	}
	if _, err := os.Stat(count); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled job ran: %v", err)
	}
}
