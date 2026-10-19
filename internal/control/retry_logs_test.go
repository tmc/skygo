package control

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestRetryLogsRemainBoundToSelectedAttempt(t *testing.T) {
	s := routeStore(t)
	b := newRetryBackend()
	c := routeControl(t, s, retryTarget(b))
	j := acceptRetry(t, s, "logs", 1)
	first := beginAttempt(t, s, j, time.Now().UTC())
	selected, err := c.LogAttempt(testActor(), j.ID, 0)
	if err != nil || selected != first.ID {
		t.Fatalf("select: %q %v", selected, err)
	}
	waiting := finishAttempt(t, s, first, failedStatus())
	second := beginAttempt(t, s, waiting, *waiting.NextAttemptAt)
	current, err := c.LogAttempt(testActor(), j.ID, 0)
	if err != nil || current != second.CurrentAttemptID() || current == selected {
		t.Fatalf("new selection: %q %v", current, err)
	}
	for ordinal, want := range map[uint32]string{1: selected, 2: current} {
		got, err := c.LogAttempt(testActor(), j.ID, ordinal)
		if err != nil || got != want {
			t.Fatalf("ordinal %d: %q %v", ordinal, got, err)
		}
	}
	chunk, err := c.Logs(context.Background(), testActor(), j.ID, selected, 0)
	if err != nil || string(chunk.Data) != selected || !chunk.Complete {
		t.Fatalf("fixed stream switched attempts: %+v %v", chunk, err)
	}
	if _, err := c.LogAttempt(testActor(), j.ID, 3); !errors.Is(err, ErrInvalid) {
		t.Fatal("future ordinal accepted", err)
	}
	if _, err := c.Logs(context.Background(), testActor(), j.ID, "ffffffffffffffffffffffffffffffff", 0); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign execution accepted", err)
	}
}

func TestRetrySnapshotsOwnAllMutableFields(t *testing.T) {
	code := int32(7)
	now := time.Now().UTC()
	j := backend.Job{Task: retryTask(1), Status: backend.Status{State: backend.Retrying, ExitCode: &code}, NextAttemptAt: &now, Attempts: []backend.Attempt{{ID: "first", Status: backend.Status{State: backend.Failed, ExitCode: &code}, StartingAt: &now, FinishedAt: &now}}}
	original := cloneJob(j)
	copy := cloneJob(j)
	copy.Task.Retry.ExitCodes[0] = 9
	copy.Task.Retry.MaxAttempts = 9
	*copy.Status.ExitCode = 9
	*copy.NextAttemptAt = now.Add(time.Hour)
	*copy.Attempts[0].Status.ExitCode = 8
	*copy.Attempts[0].StartingAt = now.Add(time.Minute)
	*copy.Attempts[0].FinishedAt = now.Add(time.Minute)
	if !reflect.DeepEqual(original, j) {
		t.Fatal("operation snapshot aliases accepted mutable state")
	}
}

func TestPinnedInstanceResultFence(t *testing.T) {
	s := routeStore(t)
	j := beginAttempt(t, s, acceptRetry(t, s, "one", 1), time.Now().UTC())
	j = finishAttempt(t, s, j, backend.Status{State: backend.Running, Instance: "original"})
	b := newRetryBackend()
	target := retryTarget(b)
	c := routeControl(t, s, target)
	pending := &operation{kind: observeOperation, job: cloneJob(j)}
	loop := &loopState{workers: []targetWorker{{target: target, pending: pending}}, due: map[string]time.Time{j.CurrentAttemptID(): time.Now().Add(time.Hour)}}
	stale := cloneJob(j)
	stale.Status.Instance = "replacement"
	stale.Attempts[len(stale.Attempts)-1].Status.Instance = "replacement"
	if err := c.integrate(loop, operationResult{operation: operation{kind: observeOperation, job: stale}, status: failedStatus()}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(j.ID)
	if loop.workers[0].pending != pending || !reflect.DeepEqual(j, got) {
		t.Fatal("mismatched pinned identity cleared pending or wrote evidence")
	}
	if err := c.integrate(loop, operationResult{operation: *pending, err: backend.ErrIdentityMismatch}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(j.ID)
	if got.Status.State != backend.Unknown || got.Status.Instance != "original" || got.Status.Termination != backend.UnknownTermination || got.NextAttemptAt != nil {
		t.Fatalf("identity mismatch lost pin or permitted retry: %+v", got)
	}
}

func TestOlderStartingObservationDoesNotRegressRunning(t *testing.T) {
	s := routeStore(t)
	j := beginAttempt(t, s, acceptRetry(t, s, "one", 1), time.Now().UTC())
	j = finishAttempt(t, s, j, backend.Status{State: backend.Running, Instance: "original"})
	b := newRetryBackend()
	target := retryTarget(b)
	c := routeControl(t, s, target)
	op := operation{kind: observeOperation, job: cloneJob(j)}
	loop := &loopState{workers: []targetWorker{{target: target, pending: &op}}}
	if err := c.integrate(loop, operationResult{operation: op, status: backend.Status{State: backend.Starting, Instance: "original"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(j.ID)
	if !reflect.DeepEqual(j, got) || !c.Healthy() {
		t.Fatal("stale startup regressed state or failed control")
	}
}
