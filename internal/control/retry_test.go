package control

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func retryTask(delay uint32) backend.Task {
	return backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: delay, ExitCodes: []int32{7}}}
}
func retryTarget(b backend.Backend) backend.Target {
	t := targetFor("good", b)
	t.SupportsRetry = true
	return t
}
func acceptRetry(t *testing.T, s *store.Store, request string, delay uint32) backend.Job {
	t.Helper()
	j, err := s.Submit(testActor(), request, retryTask(delay), "good", "binding-good")
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func beginAttempt(t *testing.T, s *store.Store, j backend.Job, now time.Time) backend.Job {
	t.Helper()
	started, ok, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now)
	if err != nil || !ok {
		t.Fatalf("begin attempt: %+v %v %v", started, ok, err)
	}
	return started
}
func failedStatus() backend.Status {
	code := int32(7)
	return backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "owned-one"}
}
func finishAttempt(t *testing.T, s *store.Store, j backend.Job, status backend.Status) backend.Job {
	t.Helper()
	out, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), status, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRetryNormalizationPrecedesReplayAndSelection(t *testing.T) {
	s := routeStore(t)
	b := &routeBackend{}
	c := routeControl(t, s, retryTarget(b))
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{9, 7, 9}}}
	j, err := c.Submit(testActor(), "one", task)
	if err != nil {
		t.Fatal(err)
	}
	if task.Retry.DelaySeconds != 0 || !reflect.DeepEqual(task.Retry.ExitCodes, []int32{9, 7, 9}) {
		t.Fatal("normalization mutated caller")
	}
	if j.Task.Retry.DelaySeconds != 1 || !reflect.DeepEqual(j.Task.Retry.ExitCodes, []int32{7, 9}) {
		t.Fatal("accepted policy not canonical")
	}
	b2 := &routeBackend{}
	replacement := targetFor("good", b2)
	replacement.Binding = "changed"
	other := routeControl(t, s, replacement)
	replay := cloneTask(task)
	replay.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7, 9}}
	got, err := other.Submit(testActor(), "one", replay)
	if err != nil || !reflect.DeepEqual(j, got) || b2.checks != 0 {
		t.Fatalf("normalized replay consulted current capability: %+v %v", got, err)
	}
	if _, err := other.Submit(testActor(), "fresh", replay); !errors.Is(err, ErrUnsupported) || b2.checks != 0 {
		t.Fatal("missing actual support fact was ignored", err)
	}
	other.failed.Store(true)
	if _, err := other.Submit(testActor(), "one", replay); err != nil {
		t.Fatal("durable replay blocked by control health", err)
	}
	single := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 1}}
	defaultControl := routeControl(t, s, targetFor("default", b2))
	implicit, err := defaultControl.Submit(testActor(), "implicit", single)
	if err != nil || implicit.Task.Retry != nil || len(implicit.Attempts) != 0 {
		t.Fatal("single policy changed default semantics", err)
	}
}

func TestRetryCancellationPrecedesAllEligibilityGates(t *testing.T) {
	for _, name := range []string{"future", "missing", "changed", "unsupported", "full32"} {
		t.Run(name, func(t *testing.T) {
			s := routeStore(t)
			j := acceptRetry(t, s, "retry", 3600)
			j = beginAttempt(t, s, j, time.Now().UTC())
			j = finishAttempt(t, s, j, failedStatus())
			if j.Status.State != backend.Retrying || j.NextAttemptAt == nil {
				t.Fatal("fixture did not enter real backoff")
			}
			history := cloneJob(j).Attempts
			b := &routeBackend{}
			target := retryTarget(b)
			switch name {
			case "missing":
				target.Name = "replacement"
			case "changed":
				target.Binding = "replacement"
			case "unsupported":
				target.SupportsRetry = false
			case "full32":
				for i := 0; i < 32; i++ {
					seedJob(t, s, fmt.Sprint("busy-", i), "busy", backend.Starting)
				}
			}
			c := routeControl(t, s, target)
			if _, err := c.Cancel(testActor(), j.ID); err != nil {
				t.Fatal(err)
			}
			loop := &loopState{workers: []targetWorker{{target: target, input: make(chan operation, 1)}}, due: make(map[string]time.Time)}
			if err := c.schedule(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			got, err := s.Get(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status.State != backend.Canceled || got.NextAttemptAt != nil || !reflect.DeepEqual(history, got.Attempts) || len(got.Attempts) != 1 || b.ioCalls() != 0 || loop.workers[0].pending != nil {
				t.Fatalf("cancel waited behind %s: %+v", name, got)
			}
		})
	}
}

func TestRetryHeldPlacementPreservesHistoryAndDeadline(t *testing.T) {
	for _, name := range []string{"missing", "changed", "unsupported"} {
		t.Run(name, func(t *testing.T) {
			s := routeStore(t)
			j := acceptRetry(t, s, "one", 3600)
			j = beginAttempt(t, s, j, time.Now().UTC())
			j = finishAttempt(t, s, j, failedStatus())
			before := cloneJob(j)
			b := &routeBackend{}
			target := retryTarget(b)
			switch name {
			case "missing":
				target.Name = "replacement"
			case "changed":
				target.Binding = "changed"
			case "unsupported":
				target.SupportsRetry = false
			}
			c := routeControl(t, s, target)
			loop := &loopState{workers: []targetWorker{{target: target, input: make(chan operation, 1)}}, due: map[string]time.Time{j.CurrentAttemptID(): time.Now().Add(time.Hour)}}
			if err := c.schedule(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			got, _ := s.Get(j.ID)
			if got.Status.State != backend.Retrying || !reflect.DeepEqual(before.Attempts, got.Attempts) || !reflect.DeepEqual(before.NextAttemptAt, got.NextAttemptAt) || b.ioCalls() != 0 || len(loop.due) != 0 {
				t.Fatal("holding retry rewrote execution history/deadline or retained poll slot")
			}
		})
	}
}

func TestStaleAttemptResultCannotClearNewPendingOrDeadline(t *testing.T) {
	s := routeStore(t)
	first := acceptRetry(t, s, "one", 1)
	first = beginAttempt(t, s, first, time.Now().UTC())
	waiting := finishAttempt(t, s, first, failedStatus())
	second := beginAttempt(t, s, waiting, *waiting.NextAttemptAt)
	b := &routeBackend{}
	target := retryTarget(b)
	c := routeControl(t, s, target)
	pending := &operation{kind: observeOperation, job: cloneJob(second)}
	deadline := time.Now().Add(time.Hour)
	loop := &loopState{workers: []targetWorker{{target: target, pending: pending}}, due: map[string]time.Time{second.CurrentAttemptID(): deadline}}
	for _, kind := range []operationKind{startOperation, observeOperation} {
		if err := c.integrate(loop, operationResult{operation: operation{kind: kind, job: first}, status: failedStatus()}); err != nil {
			t.Fatal(err)
		}
		got, _ := s.Get(first.ID)
		if loop.workers[0].pending != pending || !loop.due[second.CurrentAttemptID()].Equal(deadline) || !reflect.DeepEqual(second, got) {
			t.Fatal("old result mutated new attempt or scheduler identity")
		}
	}
}

type retryObservation struct {
	ref   backend.Reference
	reply chan observationReply
}
type observationReply struct {
	status backend.Status
	err    error
}
type retryBackend struct {
	starts   chan string
	observed chan retryObservation
	calls    atomic.Int32
}

func newRetryBackend() *retryBackend {
	return &retryBackend{starts: make(chan string, 16), observed: make(chan retryObservation, 16)}
}
func (*retryBackend) Check(backend.Task) error { return nil }
func (b *retryBackend) Start(_ context.Context, id string, _ backend.Task) error {
	b.calls.Add(1)
	b.starts <- id
	return context.DeadlineExceeded
}
func (b *retryBackend) Observe(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	q := retryObservation{ref, make(chan observationReply, 1)}
	select {
	case b.observed <- q:
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
	select {
	case r := <-q.reply:
		return r.status, r.err
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
}
func (*retryBackend) Cancel(context.Context, backend.Reference) error { return nil }
func (*retryBackend) Logs(_ context.Context, r backend.Reference, _ int64, _ int) (backend.LogChunk, error) {
	return backend.LogChunk{Data: []byte(r.ID), Complete: true}, nil
}

func TestRetryRunStartsEachDurableAttemptOnce(t *testing.T) {
	s := routeStore(t)
	b := newRetryBackend()
	c := routeControl(t, s, retryTarget(b))
	j, err := c.Submit(testActor(), "one", retryTask(1))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, c)
	defer cleanupRun(t, cancel, done)
	firstID := waitValue(t, b.starts)
	if firstID != j.ID {
		t.Fatal("first retry attempt changed legacy ID")
	}
	first := waitValue(t, b.observed)
	first.reply <- observationReply{status: failedStatus()}
	secondID := waitValue(t, b.starts)
	if secondID == firstID {
		t.Fatal("retry reused native execution ID")
	}
	current, _ := s.Get(j.ID)
	if len(current.Attempts) != 2 || current.CurrentAttemptID() != secondID || current.Status.State != backend.Starting {
		t.Fatal("Start lacked committed next intent")
	}
	second := waitValue(t, b.observed)
	if second.ref.ID != secondID || second.ref.Instance != "" || second.ref.Binding != j.TargetBinding || !reflect.DeepEqual(second.ref.Task, j.Task) {
		t.Fatal("new attempt inherited old identity")
	}
	code := int32(0)
	second.reply <- observationReply{status: backend.Status{State: backend.Succeeded, ExitCode: &code, Instance: "owned-two"}}
	// A later sentinel starts only after the success integrates; no same-ID Start.
	sentinel, err := c.Submit(testActor(), "sentinel", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	if id := waitValue(t, b.starts); id != sentinel.ID {
		t.Fatal("uncertain startup repeated Start")
	}
	final, _ := s.Get(j.ID)
	if final.Status.State != backend.Succeeded || len(final.Attempts) != 2 || final.NextAttemptAt != nil {
		t.Fatal("logical completion/history incorrect")
	}
	if b.calls.Load() != 3 {
		t.Fatal("unexpected external launch count")
	}
}

func TestRetryUncertaintyNeverSchedulesReplacement(t *testing.T) {
	for i, status := range []backend.Status{{State: backend.Unknown}, {State: backend.Failed}, {State: backend.Failed, ExitCode: func() *int32 { v := int32(7); return &v }()}, {State: backend.Failed, ExitCode: func() *int32 { v := int32(8); return &v }(), Termination: backend.StoppedWithExitCode}} {
		s := routeStore(t)
		j := acceptRetry(t, s, fmt.Sprintf("job-%d", i), 1)
		j = beginAttempt(t, s, j, time.Now().UTC())
		b := &routeBackend{}
		target := retryTarget(b)
		c := routeControl(t, s, target)
		op := &operation{kind: observeOperation, job: j}
		loop := &loopState{workers: []targetWorker{{target: target, pending: op}}, due: make(map[string]time.Time)}
		if err := c.integrate(loop, operationResult{operation: *op, status: status}); err != nil {
			t.Fatal(err)
		}
		got, _ := s.Get(j.ID)
		if got.Status.State == backend.Retrying || len(got.Attempts) != 1 || got.NextAttemptAt != nil {
			t.Fatal("missing proof/code authorized retry")
		}
	}
}

func TestRetryBackoffFreesOnlyProvedStoppedSlot(t *testing.T) {
	s := routeStore(t)
	bad := newBlockedBackend()
	good := newRetryBackend()
	for i := 0; i < 31; i++ {
		seedJob(t, s, fmt.Sprint("bad-", i), "bad", backend.Starting)
	}
	c := routeControl(t, s, targetFor("bad", bad), retryTarget(good))
	j, err := c.Submit(testActor(), "retry", retryTask(1))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, c)
	t.Cleanup(func() { cleanupRun(t, cancel, done); close(bad.release); waitValue(t, bad.exited) })
	waitValue(t, bad.entered)
	if id := waitValue(t, good.starts); id != j.ID {
		t.Fatal(id)
	}
	first := waitValue(t, good.observed)
	first.reply <- observationReply{status: failedStatus()}
	sentinel, err := c.Submit(testActor(), "sentinel", backend.Task{Argv: []string{"work"}, Target: "good"})
	if err != nil {
		t.Fatal(err)
	}
	if id := waitValue(t, good.starts); id != sentinel.ID {
		t.Fatal("backoff did not release its slot", id)
	}
	waiting, err := s.Get(j.ID)
	if err != nil || waiting.Status.State != backend.Retrying || len(waiting.Attempts) != 1 {
		t.Fatalf("retry began without capacity or proof: %+v %v", waiting, err)
	}
	peer := waitValue(t, good.observed)
	if peer.ref.ID != sentinel.ID {
		t.Fatal("backoff was observed as running", peer.ref.ID)
	}
	peer.reply <- observationReply{status: backend.Status{State: backend.Succeeded}}
	second := waitValue(t, good.starts)
	if second == j.ID || second == sentinel.ID {
		t.Fatal("retry did not receive unique durable execution", second)
	}
	got, _ := s.Get(j.ID)
	if len(got.Attempts) != 2 || got.CurrentAttemptID() != second || bad.calls.Load() != 1 {
		t.Fatal("blocked target starved retry or worker replaced")
	}
}
