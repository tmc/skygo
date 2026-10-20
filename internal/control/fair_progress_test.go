package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type fairObservation struct {
	id    string
	reply chan backend.Status
}
type fairBackend struct {
	starts       chan string
	observations chan fairObservation
	cancels      chan string
	observeErr   error
}

func newFairBackend() *fairBackend {
	return &fairBackend{starts: make(chan string, 64), observations: make(chan fairObservation, 64), cancels: make(chan string, 64)}
}
func (*fairBackend) Check(backend.Task) error { return nil }
func (b *fairBackend) Start(_ context.Context, id string, _ backend.Task) error {
	b.starts <- id
	return nil
}
func (b *fairBackend) Observe(ctx context.Context, r backend.Reference) (backend.Status, error) {
	q := fairObservation{r.ID, make(chan backend.Status, 1)}
	select {
	case b.observations <- q:
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
	select {
	case status := <-q.reply:
		return status, b.observeErr
	case <-ctx.Done():
		return backend.Status{}, ctx.Err()
	}
}
func (b *fairBackend) Cancel(_ context.Context, r backend.Reference) error {
	b.cancels <- r.ID
	return nil
}
func (*fairBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}

type blockedBackend struct {
	entered chan struct{}
	release chan struct{}
	exited  chan struct{}
	once    sync.Once
	calls   atomic.Int32
}

func newBlockedBackend() *blockedBackend {
	return &blockedBackend{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
}
func (*blockedBackend) Check(backend.Task) error                          { return nil }
func (*blockedBackend) Start(context.Context, string, backend.Task) error { return nil }
func (b *blockedBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.calls.Add(1)
	b.once.Do(func() { close(b.entered) })
	<-b.release
	close(b.exited)
	return backend.Status{State: backend.Running}, nil
}
func (*blockedBackend) Cancel(context.Context, backend.Reference) error { return nil }
func (*blockedBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, nil
}
func waitValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not complete within controlled three-second envelope")
		var zero T
		return zero
	}
}
func beginRun(t *testing.T, c *Control) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return cancel, done
}
func seedJob(t *testing.T, s *store.Store, request, target string, state backend.State) backend.Job {
	t.Helper()
	j, err := s.Submit(testActor(), request, backend.Task{Argv: []string{"work"}}, target, "binding-"+target)
	if err != nil {
		t.Fatal(err)
	}
	if state != backend.Queued {
		j, err = s.Update(j.ID, func(j *backend.Job) error { j.Status.State = state; return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	return j
}
func targetFor(name string, b backend.Backend) backend.Target {
	return backend.Target{Name: name, Binding: "binding-" + name, Backend: b}
}
func cleanupRun(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := waitValue(t, done); !errors.Is(err, context.Canceled) {
		t.Errorf("Run exit: %v", err)
	}
}

func TestBlockedTargetCannotStarveHealthyStartAndAdmission(t *testing.T) {
	s := routeStore(t)
	bad := newBlockedBackend()
	a, b := newFairBackend(), newFairBackend()
	for i := 0; i < 31; i++ {
		seedJob(t, s, fmt.Sprint("bad-", i), "bad", backend.Starting)
	}
	a1 := seedJob(t, s, "a1", "a", backend.Queued)
	a2 := seedJob(t, s, "a2", "a", backend.Queued)
	b1 := seedJob(t, s, "b1", "b", backend.Queued)
	seedJob(t, s, "b2", "b", backend.Queued)
	c := routeControl(t, s, targetFor("bad", bad), targetFor("a", a), targetFor("b", b))
	cancel, done := beginRun(t, c)
	t.Cleanup(func() { cleanupRun(t, cancel, done); close(bad.release); waitValue(t, bad.exited) })
	waitValue(t, bad.entered)
	if id := waitValue(t, a.starts); id != a1.ID {
		t.Fatal("unexpected first admission")
	}
	accepted, err := s.Get(a1.ID)
	if err != nil || accepted.Status.State != backend.Starting {
		t.Fatalf("Start preceded durable intent: %+v %v", accepted, err)
	}
	first := waitValue(t, a.observations)
	first.reply <- backend.Status{State: backend.Succeeded}
	if id := waitValue(t, b.starts); id != b1.ID {
		t.Fatal("healthy later target starved")
	}
	select {
	case id := <-a.starts:
		t.Fatalf("earlier target reused only slot before B: %s (next %s)", id, a2.ID)
	default:
	}
	current, _ := s.Get(a1.ID)
	if current.Status.State != backend.Succeeded || bad.calls.Load() != 1 {
		t.Fatalf("healthy result not integrated or bad worker replaced: %+v %d", current, bad.calls.Load())
	}
}

func TestBlockedTargetCannotStarveHealthyCancel(t *testing.T) {
	s := routeStore(t)
	bad := newBlockedBackend()
	good := newFairBackend()
	for i := 0; i < 31; i++ {
		seedJob(t, s, fmt.Sprint("bad-", i), "bad", backend.Starting)
	}
	job := seedJob(t, s, "cancel", "good", backend.Running)
	if _, err := s.Update(job.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
	c := routeControl(t, s, targetFor("bad", bad), targetFor("good", good))
	cancel, done := beginRun(t, c)
	t.Cleanup(func() { cleanupRun(t, cancel, done); close(bad.release); waitValue(t, bad.exited) })
	waitValue(t, bad.entered)
	if id := waitValue(t, good.cancels); id != job.ID {
		t.Fatal(id)
	}
	observation := waitValue(t, good.observations)
	observation.reply <- backend.Status{State: backend.Canceled}
	// A fresh queued sentinel proves the preceding cancellation result integrated.
	sentinel, err := c.Submit(testActor(), "sentinel", backend.Task{Argv: []string{"work"}, Target: "good"})
	if err != nil {
		t.Fatal(err)
	}
	if id := waitValue(t, good.starts); id != sentinel.ID {
		t.Fatal(id)
	}
	got, _ := s.Get(job.ID)
	if got.Status.State != backend.Canceled {
		t.Fatal(got)
	}
}

func TestRunCannotReplaceContextIgnoringWorker(t *testing.T) {
	s := routeStore(t)
	bad := newBlockedBackend()
	seedJob(t, s, "one", "bad", backend.Starting)
	c := routeControl(t, s, targetFor("bad", bad))
	cancel, done := beginRun(t, c)
	waitValue(t, bad.entered)
	concurrent := make(chan error, 1)
	go func() { concurrent <- c.Run(context.Background()) }()
	if err := waitValue(t, concurrent); !errors.Is(err, ErrUnavailable) {
		t.Fatal("concurrent Run admitted another worker set", err)
	}
	cancel()
	if err := waitValue(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("second Run admitted replacement workers", err)
	}
	if bad.calls.Load() != 1 {
		t.Fatal("backend call replaced")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	} // This blocked backend callback does not use the store.
	close(bad.release)
	waitValue(t, bad.exited)
}

func TestTargetCursorVisitsOtherActiveJobs(t *testing.T) {
	s := routeStore(t)
	good := newFairBackend()
	good.observeErr = errors.New("temporary remote error")
	var jobs []backend.Job
	for i := 0; i < 3; i++ {
		jobs = append(jobs, seedJob(t, s, fmt.Sprint(i), "good", backend.Running))
	}
	c := routeControl(t, s, targetFor("good", good))
	cancel, done := beginRun(t, c)
	defer cleanupRun(t, cancel, done)
	for i := 0; i < 4; i++ {
		q := waitValue(t, good.observations)
		if q.id != jobs[i%3].ID {
			t.Fatalf("cursor repeated before peers: %s want%s", q.id, jobs[i%3].ID)
		}
		q.reply <- backend.Status{}
	}
	if !c.Healthy() || !c.AuditHealthy() {
		t.Fatal("remote error became store/audit fault")
	}
}

func TestLateObservationCannotReplaceTerminalOrRestart(t *testing.T) {
	s := routeStore(t)
	good := newFairBackend()
	job := seedJob(t, s, "one", "good", backend.Starting)
	sentinel := seedJob(t, s, "two", "good", backend.Queued)
	c := routeControl(t, s, targetFor("good", good))
	cancel, done := beginRun(t, c)
	defer cleanupRun(t, cancel, done)
	q := waitValue(t, good.observations)
	if q.id != job.ID {
		t.Fatal(q.id)
	}
	if _, err := s.Update(job.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	q.reply <- backend.Status{State: backend.Running}
	if id := waitValue(t, good.starts); id != sentinel.ID {
		t.Fatal("uncertain execution restarted")
	}
	got, _ := s.Get(job.ID)
	if got.Status.State != backend.Succeeded {
		t.Fatal("late result replaced terminal state")
	}
}

func TestObservedQueuedCannotAuthorizeSecondStart(t *testing.T) {
	s := routeStore(t)
	good := newFairBackend()
	job := seedJob(t, s, "one", "good", backend.Starting)
	c := routeControl(t, s, targetFor("good", good))
	cancel, done := beginRun(t, c)
	defer cleanupRun(t, cancel, done)
	q := waitValue(t, good.observations)
	q.reply <- backend.Status{State: backend.Queued}
	next := waitValue(t, good.observations)
	if next.id != job.ID {
		t.Fatal(next.id)
	}
	select {
	case <-good.starts:
		t.Fatal("remote queued state authorized repeated Start")
	default:
	}
	got, _ := s.Get(job.ID)
	if got.Status.State != backend.Starting {
		t.Fatal(got)
	}
	next.reply <- backend.Status{State: backend.Running}
}

type delayedStartBackend struct {
	*fairBackend
	entered chan string
	release chan struct{}
}

func (b *delayedStartBackend) Start(_ context.Context, id string, _ backend.Task) error {
	b.entered <- id
	<-b.release
	return context.DeadlineExceeded
}

func TestCancellationDuringAmbiguousStartPreservesIntent(t *testing.T) {
	s := routeStore(t)
	b := &delayedStartBackend{fairBackend: newFairBackend(), entered: make(chan string, 8), release: make(chan struct{})}
	c := routeControl(t, s, targetFor("good", b))
	j, err := c.Submit(testActor(), "one", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, c)
	defer cleanupRun(t, cancel, done)
	if id := waitValue(t, b.entered); id != j.ID {
		t.Fatal(id)
	}
	if _, err := c.Cancel(testActor(), j.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(j.ID)
	if got.Status.State != backend.Starting || !got.CancelRequested {
		t.Fatal("ambiguous Start labeled canceled-before-execution", got)
	}
	close(b.release)
	if id := waitValue(t, b.cancels); id != j.ID {
		t.Fatal(id)
	}
	q := waitValue(t, b.observations)
	q.reply <- backend.Status{State: backend.Unknown, Reason: "missing after ambiguous start"}
	next, err := c.Submit(testActor(), "sentinel", backend.Task{Argv: []string{"work"}})
	if err != nil {
		t.Fatal(err)
	}
	if id := waitValue(t, b.entered); id != next.ID {
		t.Fatal("ambiguous Start was repeated", id)
	}
	got, _ = s.Get(j.ID)
	if got.Status.State != backend.Unknown {
		t.Fatal("uncertainty implied termination", got)
	}
}

func TestWorkerDoesNotBlockOnAbandonedResults(t *testing.T) {
	b := newBlockedBackend()
	ctx, cancel := context.WithCancel(context.Background())
	input := make(chan operation, 1)
	results := make(chan operationResult, 1) // The production one-pending bound.
	done := make(chan struct{})
	go func() { defer close(done); runWorker(ctx, b, input, results) }()
	input <- operation{kind: observeOperation, job: backend.Job{ID: "one"}}
	waitValue(t, b.entered)
	cancel()
	close(input)
	close(b.release)
	waitValue(t, done)
	if result := waitValue(t, results); result.operation.job.ID != "one" || result.status.State != backend.Running {
		t.Fatal("original returned observation was discarded", result)
	}
}

func TestForeignResultCannotClearPendingIdentity(t *testing.T) {
	s := routeStore(t)
	b := newFairBackend()
	j := seedJob(t, s, "one", "good", backend.Starting)
	c := routeControl(t, s, targetFor("good", b))
	pending := &operation{kind: observeOperation, job: j}
	loop := &loopState{workers: []targetWorker{{target: targetFor("good", b), pending: pending}}}
	foreign := *pending
	foreign.job.TargetBinding = "foreign"
	if err := c.integrate(loop, operationResult{operation: foreign, status: backend.Status{State: backend.Succeeded}}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(j.ID)
	if loop.workers[0].pending != pending || got.Status.State != backend.Starting {
		t.Fatal("foreign result cleared/overwrote accepted execution")
	}
}

func TestObservationPacingDoesNotBlockOtherJobs(t *testing.T) {
	s := routeStore(t)
	b := newFairBackend()
	first := seedJob(t, s, "first", "good", backend.Running)
	second := seedJob(t, s, "second", "good", backend.Running)
	c := routeControl(t, s, targetFor("good", b))
	op := operation{kind: observeOperation, job: first}
	loop := &loopState{workers: []targetWorker{{target: targetFor("good", b), input: make(chan operation, 1), pending: &op}}}
	if err := c.integrate(loop, operationResult{operation: op, status: backend.Status{State: backend.Running}}); err != nil {
		t.Fatal(err)
	}
	if !time.Now().Before(loop.due[first.ID]) {
		t.Fatal("successful observation had no poll deadline")
	}
	// A long explicit deadline makes the no-redelivery check deterministic; peer
	// jobs must remain eligible without waiting for the paced job's deadline.
	loop.due[first.ID] = time.Now().Add(time.Hour)
	if err := c.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	selected := <-loop.workers[0].input
	if selected.job.ID != second.ID {
		t.Fatal("paced job prevented peer observation")
	}
	loop.workers[0].pending = nil
	loop.due[second.ID] = time.Now().Add(time.Hour)
	if err := c.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	if loop.workers[0].pending != nil {
		t.Fatal("fast responses formed an immediate observation loop")
	}
	if _, err := s.Update(first.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	if _, ok := loop.due[first.ID]; ok {
		t.Fatal("retired job retained scheduler bookkeeping")
	}
}

func TestObservationBookkeepingHasThirtyTwoSlots(t *testing.T) {
	s := routeStore(t)
	b := newFairBackend()
	var jobs []backend.Job
	// Trusted fixture construction deliberately exceeds the normal admission cap
	// to ensure transient observation bookkeeping remains independently bounded.
	for i := 0; i < 33; i++ {
		jobs = append(jobs, seedJob(t, s, fmt.Sprint(i), "good", backend.Starting))
	}
	c := routeControl(t, s, targetFor("good", b))
	loop := &loopState{workers: []targetWorker{{target: targetFor("good", b), input: make(chan operation, 1), after: jobs[31].Sequence}}, due: make(map[string]time.Time)}
	for _, j := range jobs[:32] {
		loop.due[j.ID] = time.Now().Add(time.Hour)
	}
	if err := c.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	if len(loop.due) != 32 || loop.workers[0].pending != nil {
		t.Fatal("observation bookkeeping exceeded bound")
	}
	if _, err := s.Update(jobs[0].ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	op := <-loop.workers[0].input
	if op.job.ID != jobs[32].ID || len(loop.due) != 32 {
		t.Fatal("retired observation slot not reused")
	}
}

func TestDurableFailureDoesNotBecomeAuditFailure(t *testing.T) {
	s := routeStore(t)
	b := newFairBackend()
	c := routeControl(t, s, targetFor("good", b))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("closed durable store did not stop Run")
	}
	if c.Healthy() || !c.AuditHealthy() {
		t.Fatal("durable and audit health were conflated")
	}
	select {
	case <-b.starts:
		t.Fatal("durable failure dispatched work")
	default:
	}
}
