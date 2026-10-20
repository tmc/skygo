package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// Test coordinators that directly construct workers own their input closure;
// production input closure belongs only to the original Run.
func stopTestLoop(t *testing.T, cancel context.CancelFunc, s *loopState) {
	t.Helper()
	cancel()
	for i := range s.workers {
		close(s.workers[i].input)
	}
	for i := range s.workers {
		waitValue(t, s.workers[i].done)
	}
}

func TestJoinRetainsOriginalBlockedWorker(t *testing.T) {
	s := routeStore(t)
	b := newBlockedBackend()
	release := sync.OnceFunc(func() { close(b.release) })
	defer release()
	seedJob(t, s, "join", "good", backend.Starting)
	c := routeControl(t, s, targetFor("good", b))
	if err := c.Join(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("Join before Run", err)
	}
	cancel, done := beginRun(t, c)
	defer cancel()
	waitValue(t, b.entered)
	cancel()
	if err := waitValue(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := c.Join(canceled); !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatal("blocked callback became joined", err)
	}
	if err := c.Run(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatal("replacement Run admitted", err)
	}
	release()
	waitValue(t, b.exited)
	joined, end := context.WithTimeout(context.Background(), 3*time.Second)
	defer end()
	if err := c.Join(joined); err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != 1 {
		t.Fatal("original callback replaced", b.calls.Load())
	}
	result := waitValue(t, c.run.state.results)
	if result.status.State != backend.Running || result.operation.job.Target != "good" {
		t.Fatal("late original result lost", result)
	}
	if c.run.state.workers[0].pending == nil {
		t.Fatal("Join integrated a late result")
	}
}

func TestReceivedOperationSurvivesCancellation(t *testing.T) {
	s := routeStore(t)
	b := &routeBackend{}
	job := seedJob(t, s, "received", "good", backend.Running)
	c := routeControl(t, s, targetFor("good", b))
	ctx, cancel := context.WithCancel(context.Background())
	loop := c.newLoop(ctx)
	defer stopTestLoop(t, cancel, loop)
	if err := c.schedule(ctx, loop); err != nil {
		t.Fatal(err)
	}
	request := loop.workers[0].pending
	result := waitValue(t, loop.results) // Actual original worker has returned.
	cancel()                             // Cancellation occurs after receive, before the production consumer.
	if err := c.receiveOperation(ctx, loop, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	got := loop.workers[0].received
	if got == nil || got.request != request || got.result.operation.job.ID != job.ID || got.result.status.State != backend.Running {
		t.Fatal("dequeued original request/result pair lost", got)
	}
	if loop.workers[0].pending != request {
		t.Fatal("canceled receive integrated the request")
	}
}

func TestReceivedOperationSurvivesIntegrationError(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	seedJob(t, s, "failed-integration", "good", backend.Running)
	ctx, cancel := context.WithCancel(context.Background())
	loop := c.newLoop(ctx)
	defer stopTestLoop(t, cancel, loop)
	if err := c.schedule(ctx, loop); err != nil {
		t.Fatal(err)
	}
	request := loop.workers[0].pending
	result := waitValue(t, loop.results)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.receiveOperation(ctx, loop, result); err == nil {
		t.Fatal("closed Store integration succeeded")
	}
	got := loop.workers[0].received
	if loop.workers[0].pending != nil || got == nil || got.request != request || got.result.operation.job.ID != request.job.ID {
		t.Fatal("integration cleared pending and lost the original pair", got)
	}
}

func TestReceivedMaintenanceSurvivesIntegrationError(t *testing.T) {
	s := routeStore(t)
	c := routeControl(t, s, targetFor("good", &routeBackend{}))
	ctx, cancel := context.WithCancel(context.Background())
	m := newMaintenance(ctx)
	defer func() { cancel(); close(m.input); waitValue(t, m.done) }()
	// An invalid cleanup is returned by the actual validation path without any
	// backend cleanup. The closed Store then faults the real FailCleanup call.
	request := &maintenanceRequest{sequence: 1, record: &store.CleanupRecord{ID: "invalid"}}
	m.pending = request
	m.input <- *request
	result := waitValue(t, m.results)
	if result.err == nil {
		t.Fatal("invalid cleanup invoked no refusal")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.receiveMaintenance(ctx, m, result); err == nil {
		t.Fatal("closed Store integration succeeded")
	}
	if m.pending != nil || m.received == nil || m.received.request != request || m.received.result.err != result.err {
		t.Fatal("maintenance integration lost the original pair", m.received)
	}
}

func TestReceivedMaintenanceSurvivesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	m := newMaintenance(ctx)
	defer func() { cancel(); close(m.input); waitValue(t, m.done) }()
	request := &maintenanceRequest{sequence: 1, record: &store.CleanupRecord{ID: "invalid"}}
	m.pending = request
	m.input <- *request
	result := waitValue(t, m.results)
	cancel()
	var c Control
	if err := c.receiveMaintenance(ctx, m, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.pending != request || m.received == nil || m.received.request != request || m.received.result.err != result.err {
		t.Fatal("canceled receive lost the original maintenance pair", m.received)
	}
}

func TestQueuedWorkspaceOwnerReturnsBeforeWorkerExit(t *testing.T) {
	c := &Control{artifacts: &artifactService{workspaceIO: make(map[*artifactIO]struct{})}}
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	input, results := make(chan operation, 1), make(chan operationResult, 1)
	input <- operation{kind: startOperation, workspace: &workspaceStart{owner: owner}}
	close(input)
	exited := make(chan struct{})
	go func() { defer close(exited); runWorker(ctx, &routeBackend{}, input, results) }()
	waitValue(t, exited)
	result := waitValue(t, results)
	if !result.notStarted || !errors.Is(result.err, context.Canceled) || len(c.artifacts.workspaceIO) != 0 || !owner.finished {
		t.Fatal("queued unused owner abandoned", result, len(c.artifacts.workspaceIO))
	}
}

func TestAllOriginalResultsFitWithoutReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Control{}
	for i := 0; i < 32; i++ {
		c.targets = append(c.targets, targetFor(fmt.Sprintf("target%02d", i), &routeBackend{}))
	}
	loop := c.newLoop(ctx)
	for i := range loop.workers {
		w := &loop.workers[i]
		w.input <- operation{kind: startOperation, job: backend.Job{ID: fmt.Sprint(i), Target: w.target.Name}}
		close(w.input)
	}
	m := newMaintenance(ctx)
	m.input <- maintenanceRequest{sequence: 1}
	close(m.input)
	// No result is consumed until every original worker has actually returned.
	for i := range loop.workers {
		waitValue(t, loop.workers[i].done)
	}
	waitValue(t, m.done)
	if len(loop.results) != 32 || len(m.results) != 1 {
		t.Fatal("accepted requests lost", len(loop.results), len(m.results))
	}
	seen := make(map[string]bool)
	for i := 0; i < 32; i++ {
		r := <-loop.results
		if seen[r.operation.job.Target] || !r.notStarted || !errors.Is(r.err, context.Canceled) {
			t.Fatal("duplicated or fabricated result", r)
		}
		seen[r.operation.job.Target] = true
	}
	if result := <-m.results; result.request.sequence != 1 || !errors.Is(result.err, context.Canceled) {
		t.Fatal(result)
	}
}
