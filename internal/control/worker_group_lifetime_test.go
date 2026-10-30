//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func ExampleWorkers_PendingResults() {
	var workers *Workers
	fmt.Println(workers.PendingResults())
	var empty Workers
	fmt.Println(empty.PendingResults())
	// Output:
	// <nil>
	// <nil>
}

func pendingWorkerResult(t *testing.T, w *Workers, result *barrierResult) error {
	t.Helper()
	err := w.PendingResults()
	var pending *workerPendingResults
	if !errors.As(err, &pending) || pending.workers != w {
		t.Fatal("pending query lost its original Workers")
	}
	found := false
	for _, got := range pending.barriers {
		found = found || got == result
	}
	if !found {
		t.Fatal("pending query replaced its original result")
	}
	if result.cause != nil && !errors.Is(err, result.cause) {
		t.Fatal("pending query lost the saved native cause")
	}
	if result.recordErr != nil && !errors.Is(err, result.recordErr) {
		t.Fatal("pending query lost the first Store error")
	}
	return err
}

// This table models private local records only. The real Store paths below
// establish how actual Control creates and discharges those records.
func TestWorkerPendingResultsKindsModeled(t *testing.T) {
	for _, kind := range []barrierResultKind{barrierResultEffect, barrierResultResolve, barrierResultObservation, barrierResultVanished, barrierResultHeldRead} {
		for _, held := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind_%d/held_%v", kind, held), func(t *testing.T) {
				result := &barrierResult{kind: kind}
				b := &workerBarriers{}
				if held {
					b.held = result
				} else {
					b.pending = result
				}
				w := &Workers{barriers: map[string]*workerBarriers{"group": b}}
				if err := pendingWorkerResult(t, w, result); errors.Is(err, backend.ErrWorkerUnsettled) {
					t.Fatal("nil-cause local record manufactured a native marker")
				}
			})
		}
	}
}

// These causes are MODELED snapshot tokens, not an actual native Close fault.
func TestWorkerPendingResultsCauseSnapshot(t *testing.T) {
	original := errors.New("MODELED original local cause")
	firstStore := errors.New("MODELED first Store record error")
	later := errors.New("MODELED later cause")
	result := &barrierResult{kind: barrierResultObservation, cause: original, recordErr: firstStore}
	w := &Workers{barriers: map[string]*workerBarriers{"group": {pending: result}}}
	err := pendingWorkerResult(t, w, result)
	w.mu.Lock()
	result.cause, result.recordErr = later, later
	w.mu.Unlock()
	if !errors.Is(err, original) || !errors.Is(err, firstStore) || errors.Is(err, later) {
		t.Fatal("query error read mutable result fields after its snapshot")
	}
	if err.Error() != "worker results remain pending" {
		t.Fatal("pending query exposed private error text")
	}
}

// These are MODELED private records and causes, not native ownership proofs.
func TestWorkerPendingResultsAggregatesModeled(t *testing.T) {
	causes := make([]error, 6)
	for i := range causes {
		causes[i] = fmt.Errorf("MODELED saved cause %d", i)
	}
	first := &barrierResult{kind: barrierResultEffect, cause: causes[0], recordErr: causes[1]}
	second := &barrierResult{kind: barrierResultHeldRead, cause: causes[2], recordErr: causes[3], latched: true}
	released := 0
	log := &workerLogResult{cause: causes[4], recordErr: causes[5], latched: true, release: func() { released++ }}
	w := &Workers{
		barriers:    map[string]*workerBarriers{"first": {pending: first}, "second": {held: second}},
		logFailures: map[string][]*workerLogResult{"third": {log}},
	}
	err := w.PendingResults()
	var pending *workerPendingResults
	if !errors.As(err, &pending) || len(pending.barriers) != 2 || len(pending.logs) != 1 || pending.logs[0] != log {
		t.Fatal("query lost an original from another group or a latched log owner")
	}
	for _, cause := range causes {
		if !errors.Is(err, cause) {
			t.Fatal("query dropped a saved original cause")
		}
	}
	if released != 0 || !log.latched || !second.latched {
		t.Fatal("query released or rewrote an original owner")
	}
}

func pendingReopenStore(t *testing.T, f *submissionDataFixture, w *Workers) {
	t.Helper()
	s, err := store.Open(filepath.Join(f.directory, "state"))
	if err != nil {
		t.Fatal(err)
	}
	f.store, f.control.store = s, s
	for _, runner := range w.barriers {
		runner.store = storeWorkerBarriers{s}
	}
}

func TestWorkerPendingResultsBenignListFailure(t *testing.T) {
	f, w, b := g1Fixture(t)
	if err := f.store.Close(); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	f.store = nil
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("closed Store list unexpectedly succeeded")
	}
	if err := w.PendingResults(); err != nil || len(b.calls) != 0 {
		t.Fatalf("settled list failure became original custody: %v", err)
	}
}

func TestWorkerPendingResultsOriginalEffect(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	cause := errors.New("MODELED settled original Create response error")
	b.createErr = cause
	b.onCreate = func(rank uint8) {
		if rank == 0 {
			if err := f.store.Close(); err != nil {
				f.hold(err)
				t.Error(err)
			}
			f.store = nil
		}
	}
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("original result recorded on a closed Store")
	}
	runner := w.barriers[g.Job.ID]
	original := runner.pending
	if original == nil || original.kind != barrierResultEffect || original.receipt.Created.Pod.UID == "" || original.recordErr == nil || original.cause != cause {
		t.Fatal("original Create result was not pending")
	}
	snapshot := *original
	calls := b.count("create")
	if err := pendingWorkerResult(t, w, original); errors.Is(err, backend.ErrWorkerUnsettled) {
		t.Fatal("settled native error manufactured unsettled local ownership")
	}
	if !reflect.DeepEqual(snapshot, *original) || b.count("create") != calls {
		t.Fatal("query mutated or repeated the original")
	}
	pendingReopenStore(t, f, w)
	b.onCreate = nil
	if _, err := runner.commitPending(); err != nil {
		t.Fatal(err)
	}
	if err := w.PendingResults(); err != nil || b.count("create") != calls {
		t.Fatalf("Store-only exact ACK did not discharge settled local result: %v", err)
	}
}

// pendingResolveBackend alters only MODELED native fault timing; Store,
// authentication, delegation, Control and receipt recording are real.
type pendingResolveBackend struct {
	*g1Backend
	afterResolve func()
}

func (b *pendingResolveBackend) ResolveWorker(ctx context.Context, c backend.WorkerCreate) (backend.WorkerCreated, error) {
	got, err := b.g1Backend.ResolveWorker(ctx, c)
	b.afterResolve()
	return got, err
}

func TestWorkerPendingResultsOriginalResolve(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	b.createLost = true
	b.createErr = errors.New("MODELED locally settled lost Create")
	cause := errors.New("MODELED locally settled original Resolve response error")
	b.resolveErr = cause
	wrapped := &pendingResolveBackend{g1Backend: b, afterResolve: func() {
		if err := f.store.Close(); err != nil {
			f.hold(err)
			t.Error(err)
		}
		f.store = nil
	}}
	target := w.targets[g.Job.Target]
	target.backend = wrapped
	w.targets[g.Job.Target] = target
	g1Step(t, w)
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("Resolve result recorded on a closed Store")
	}
	runner := w.barriers[g.Job.ID]
	original := runner.pending
	if original == nil || original.kind != barrierResultResolve || original.rawCreated.Pod.UID == "" || original.recordErr == nil || original.cause != cause {
		t.Fatal("actual original Resolve record was lost")
	}
	pendingWorkerResult(t, w, original)
	creates, resolves := b.count("create"), b.resolves
	pendingReopenStore(t, f, w)
	if _, err := runner.commitPending(); err != nil {
		t.Fatal(err)
	}
	if err := w.PendingResults(); err != nil || b.count("create") != creates || b.resolves != resolves {
		t.Fatalf("query or Store-only ACK repeated Resolve: %v", err)
	}
}

func TestWorkerPendingResultsOriginalObservation(t *testing.T) {
	for _, vanished := range []bool{false, true} {
		t.Run(fmt.Sprint(vanished), func(t *testing.T) {
			f, w, b, g, original := g1FailedObservation(t, vanished)
			if !vanished && original.cause != nil {
				t.Fatal("positive observation unexpectedly had a native cause")
			}
			pendingWorkerResult(t, w, original)
			calls := len(b.calls)
			g1Step(t, w)
			if err := w.PendingResults(); err != nil || len(b.calls) != calls {
				t.Fatalf("exact observation ACK did not discharge local result: %v", err)
			}
			current := g1Read(t, f, g.Job.ID)
			if vanished && !current.Members[0].Barrier.Vanished || !vanished && current.Members[0].Barrier.Stop != backend.WorkerTerminated {
				t.Fatal("exact original native fact changed")
			}
		})
	}
}

func TestWorkerPendingResultsHeldAfterAck(t *testing.T) {
	for _, kind := range []string{"effect", "resolve", "read"} {
		t.Run(kind, func(t *testing.T) {
			f, w, b := g1Fixture(t)
			g := g1Submit(t, f, w, backend.WorkerIndependent)
			owner := &g1OwnerError{"MODELED retained original local owner"}
			raw := errors.New("MODELED raw local failure")
			cause := errors.Join(owner, raw)
			switch kind {
			case "effect":
				b.createErr = cause
			case "resolve":
				b.createLost = true
				b.createErr = errors.New("MODELED locally settled lost Create")
				b.resolveErr = cause
			}
			g1Step(t, w)
			g1Step(t, w)
			if kind == "read" {
				b.observeErr = errors.Join(backend.ErrWorkerVanished, cause)
				g1Step(t, w)
			}
			runner := w.barriers[g.Job.ID]
			if runner.held == nil || runner.pending != nil {
				t.Fatal("original owner was not retained after Store ACK")
			}
			err := pendingWorkerResult(t, w, runner.held)
			var got *g1OwnerError
			if !errors.Is(err, raw) || !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.As(err, &got) || got != owner {
				t.Fatal("query lost exact original owner/raw cause after ACK")
			}
			if !g1Read(t, f, g.Job.ID).Reserved {
				t.Fatal("query or original ACK refunded held group")
			}
		})
	}
}

func TestWorkerPendingResultsActiveLease(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	before, err := os.ReadFile(filepath.Join(f.directory, "state", "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	calls := len(b.calls)
	pending := w.PendingResults()
	var own *workerPendingResults
	if !errors.As(pending, &own) || own.workers != w || own.activeLogs != 1 || errors.Is(pending, backend.ErrWorkerUnsettled) {
		t.Fatal("healthy active lease was lost or manufactured a failed native owner")
	}
	release()
	if err := w.PendingResults(); err != nil {
		t.Fatal("the original released healthy lease stayed pending")
	}
	after, err := os.ReadFile(filepath.Join(f.directory, "state", "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || len(b.calls) != calls {
		t.Fatal("active/nil query or healthy lease release performed Store/native I/O")
	}
}

func TestWorkerPendingResultsHeldLogLatch(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	owner := &g1OwnerError{"MODELED actual original log owner token"}
	raw := errors.New("MODELED raw log reader failure")
	b.logErr = errors.Join(owner, raw)
	b.logsEntered, b.logsReturn = make(chan struct{}), make(chan struct{})
	returned := make(chan error, 1)
	join := func() error {
		t.Helper()
		select {
		case err := <-returned:
			return err
		case <-time.After(5 * time.Second):
			err := errors.New("original modeled log call not joined")
			f.hold(err)
			t.Fatal(err)
			return nil
		}
	}
	go func() {
		_, err := w.Logs(context.Background(), f.actor, g.Job.ID, g.Group.AttemptID, 0, 0, 10)
		returned <- err
	}()
	select {
	case <-b.logsEntered:
	case <-time.After(5 * time.Second):
		close(b.logsReturn)
		_ = join()
		t.Fatal("original modeled log call did not enter")
	}
	if err := w.PendingResults(); err == nil || errors.Is(err, backend.ErrWorkerUnsettled) {
		close(b.logsReturn)
		_ = join()
		t.Fatal("active original log was not held as a healthy active lease")
	}
	closeErr := f.store.Close()
	f.store = nil
	close(b.logsReturn)
	if err := join(); !errors.Is(err, owner) || !errors.Is(err, raw) {
		t.Fatal("original log return lost its owner/raw cause")
	}
	if closeErr != nil {
		f.hold(closeErr)
		t.Fatal(closeErr)
	}
	p := w.logFailures[g.Job.ID][0]
	if p.latched || p.recordErr == nil || p.release == nil {
		t.Fatal("failed real Store latch lost original log state")
	}
	firstStore := p.recordErr
	check := func() {
		t.Helper()
		err := w.PendingResults()
		var own *workerPendingResults
		var actual *g1OwnerError
		if !errors.As(err, &own) || len(own.logs) != 1 || own.logs[0] != p || !errors.As(err, &actual) || actual != owner || !errors.Is(err, raw) || !errors.Is(err, firstStore) {
			t.Fatal("query lost exact original log/result/Store cause")
		}
	}
	check()
	pendingReopenStore(t, f, w)
	g1Step(t, w)
	if !p.latched || p.recordErr != firstStore || w.logs[g.Job.ID] != 1 {
		t.Fatal("positive Store quarantine ACK released the original local owner")
	}
	check()
	if !g1Read(t, f, g.Job.ID).Reserved {
		t.Fatal("held log query refunded the group")
	}
}

func TestWorkerPendingResultsRecordedUnknown(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	b.createLost = true
	b.createErr = errors.New("MODELED locally settled lost native Create")
	g1Step(t, w)
	g1Step(t, w)
	current := g1Read(t, f, g.Job.ID)
	if current.Members[0].Create.State != store.WorkerUnknown || !current.Reserved || current.Phase == store.WorkerGroupDone {
		t.Fatal("fixture did not retain a native unknown obligation")
	}
	before, err := os.ReadFile(filepath.Join(f.directory, "state", "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	calls, resolves := len(b.calls), b.resolves
	for i := 0; i < 3; i++ {
		if err := w.PendingResults(); err != nil {
			t.Fatal("durable native uncertainty became a fictitious local owner")
		}
	}
	after, err := os.ReadFile(filepath.Join(f.directory, "state", "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || len(b.calls) != calls || b.resolves != resolves || !reflect.DeepEqual(current, g1Read(t, f, g.Job.ID)) {
		t.Fatal("nil local query rewrote native uncertainty or reservation")
	}
}

func TestWorkerPendingResultsUnavailableAuthority(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	f.stopAuth()
	select {
	case <-f.authDone:
	case <-time.After(5 * time.Second):
		t.Fatal("actual auth refresher not joined")
	}
	until := time.Now().Add(5 * time.Second)
	for f.engine.Ready() {
		if time.Now().After(until) {
			t.Fatal("actual auth snapshot stayed fresh")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := f.engine.CheckDelegation(g.Authority); !errors.Is(err, auth.ErrUnavailable) {
		t.Fatal("fixture did not become auth unavailable")
	}
	g1Step(t, w)
	if err := w.PendingResults(); err != nil || len(b.calls) != 0 {
		t.Fatalf("unavailable authority created a new owner/effect: %v", err)
	}
	if g1Read(t, f, g.Job.ID).Revoked {
		t.Fatal("unavailable authority was treated as true loss")
	}
}
