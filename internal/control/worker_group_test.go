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
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
	"github.com/tmc/skygo/internal/workeradmission"
)

// g1Backend models native receipts only. Store, authentication, Control and
// durable effects in these tests are the actual implementations.
type g1Backend struct {
	onObserve           func(uint8)
	lostServiceRemove   bool
	serviceRemoveSent   bool
	serviceRemoveErr    error
	recoverWrongService bool
	recoveries          []backend.WorkerRecovery
	createServiceOnly   bool
	onStart             func(uint8)
	startErr            error
	resolveErr          error
	onRemove            func(uint8)
	startLost           bool
	mainExec            int
	observeErr          error
	logErr              error
	removeErr           error
	createErr           error
	createLost          bool
	resolves            int
	workflowBackend
	mu                          sync.Mutex
	calls                       []string
	released, started, canceled map[uint8]bool
	prepared                    bool
	exit                        bool
	onCreate                    func(uint8)
	logsEntered, logsReturn     chan struct{}
	unknownRemove               bool
}

func newG1Backend() *g1Backend {
	return &g1Backend{released: map[uint8]bool{}, started: map[uint8]bool{}, canceled: map[uint8]bool{}, prepared: true}
}
func (b *g1Backend) note(s string, r uint8) { b.calls = append(b.calls, fmt.Sprintf("%s/%d", s, r)) }
func g1Pod(g backend.WorkerGroup, r uint8) backend.WorkerResource {
	return backend.WorkerResource{Name: fmt.Sprintf("worker-%s-%d", g.AttemptID, r), UID: fmt.Sprintf("uid-%s-%d", g.AttemptID, r)}
}
func (b *g1Backend) CreateWorker(_ context.Context, c backend.WorkerCreate) (backend.WorkerCreated, error) {
	b.mu.Lock()
	b.note("create", c.Rank)
	b.mu.Unlock()
	if b.onCreate != nil {
		b.onCreate(c.Rank)
	}
	out := backend.WorkerCreated{Pod: g1Pod(c.Group, c.Rank)}
	if c.Group.Spec.Mode == backend.WorkerTCP && c.Rank == 0 {
		out.Service = backend.WorkerResource{Name: "rendezvous-" + c.Group.AttemptID, UID: "service-" + c.Group.AttemptID}
	}
	if b.createServiceOnly && c.Group.Spec.Mode == backend.WorkerTCP && c.Rank == 0 {
		out.Pod = backend.WorkerResource{}
	}
	if b.createLost {
		return backend.WorkerCreated{}, b.createErr
	}
	return out, b.createErr
}
func (b *g1Backend) ReleaseWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerReleased, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("release", r.Rank)
	b.released[r.Rank] = true
	return backend.WorkerReleased{Pod: r.Pod, Acknowledged: true, Revision: "7"}, nil
}
func (b *g1Backend) ObserveBarrier(_ context.Context, r backend.WorkerReference) (backend.WorkerBarrier, error) {
	if b.onObserve != nil {
		defer b.onObserve(r.Rank)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("observe", r.Rank)
	o := backend.WorkerBarrier{Pod: r.Pod, Revision: "9", Gated: !b.released[r.Rank], Started: b.started[r.Rank]}
	if b.released[r.Rank] {
		o.Node = "node-a"
		o.Prepared = b.prepared
	}
	if r.Group.Spec.Mode == backend.WorkerTCP && r.Rank == 0 {
		o.Rendezvous = backend.WorkerRendezvous{Service: r.Service, Host: r.Service.Name, Port: 29500, Routed: true}
	}
	if b.canceled[r.Rank] || b.exit && b.started[r.Rank] {
		o.Stop = backend.WorkerTerminated
		o.Prepared = false
		start := time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC)
		code := int32(0)
		if b.canceled[r.Rank] {
			code = 143
		}
		o.Exit = &backend.ContainerExit{ContainerID: fmt.Sprintf("container-%d", r.Rank), ExitCode: code, StartedAt: start, FinishedAt: start.Add(time.Second)}
	}
	return o, b.observeErr
}
func (b *g1Backend) ObserveWorker(ctx context.Context, r backend.WorkerReference) (backend.WorkerObservation, error) {
	o, err := b.ObserveBarrier(ctx, r)
	out := backend.WorkerObservation{Pod: r.Pod, Gated: o.Gated, Node: o.Node}
	if o.Exit != nil {
		out.Status = backend.Status{State: backend.Succeeded}
		out.Exit = o.Exit
	} else {
		out.Status = backend.Status{State: backend.Running}
		out.Start = &backend.ContainerStart{ContainerID: "modeled", StartedAt: time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC)}
	}
	return out, err
}
func (b *g1Backend) StartWorker(_ context.Context, r backend.WorkerReference, token [32]byte) (backend.WorkerStarted, error) {
	if b.onStart != nil {
		defer b.onStart(r.Rank)
	}
	want, err := backend.WorkerStartToken(r)
	if err != nil || want != token {
		return backend.WorkerStarted{}, errors.New("wrong persisted start token")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("start", r.Rank)
	if !b.started[r.Rank] {
		b.mainExec++
	}
	b.started[r.Rank] = true
	if b.startLost {
		b.startLost = false
		return backend.WorkerStarted{Pod: r.Pod}, errors.New("MODELED settled local start call, response lost")
	}
	return backend.WorkerStarted{Pod: r.Pod, Acknowledged: true, Revision: "10"}, b.startErr
}
func (b *g1Backend) CancelWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerCanceled, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("cancel", r.Rank)
	b.canceled[r.Rank] = true
	return backend.WorkerCanceled{Pod: r.Pod, Acknowledged: true}, nil
}
func (b *g1Backend) RemoveWorker(_ context.Context, r backend.WorkerReference) (backend.WorkerRemoval, error) {
	if b.onRemove != nil {
		b.onRemove(r.Rank)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("remove", r.Rank)
	service := backend.WorkerResource{}
	if r.Group.Spec.Mode == backend.WorkerTCP && r.Rank == 0 {
		service = r.Service
	}
	if b.lostServiceRemove && r.Rank == 0 && r.Pod == (backend.WorkerResource{}) && service.UID != "" {
		cause := b.serviceRemoveErr
		if cause == nil {
			cause = errors.New("MODELED locally settled lost Service Remove response")
		}
		return backend.WorkerRemoval{Service: service, ServiceDispatched: b.serviceRemoveSent}, cause
	}
	if b.unknownRemove {
		return backend.WorkerRemoval{Pod: r.Pod, Service: service}, errors.New("MODELED lost original remove")
	}
	var callErr error
	if r.Rank == 0 {
		callErr = b.removeErr
	}
	return backend.WorkerRemoval{Pod: r.Pod, Service: service, PodRemoved: r.Pod.UID != "", ServiceRemoved: service.UID != ""}, callErr
}
func (b *g1Backend) RecoverWorkerRemoval(_ context.Context, recovery backend.WorkerRecovery) (backend.WorkerRemoval, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.note("recover", recovery.Ref.Rank)
	b.recoveries = append(b.recoveries, recovery)
	if err := backend.ValidateWorkerRecovery(recovery); err != nil {
		return backend.WorkerRemoval{}, err
	}
	ref := recovery.Ref
	if !b.lostServiceRemove || ref.Rank != 0 || ref.Pod != (backend.WorkerResource{}) || ref.Service.UID != "service-"+ref.Group.AttemptID || recovery.Evidence != backend.WorkerNeverReleased || !recovery.RemoveSent {
		return backend.WorkerRemoval{}, errors.New("MODELED recovery outside exact Service-only qualification")
	}
	service := ref.Service
	if b.recoverWrongService {
		service.UID = "foreign-" + service.UID
	}
	return backend.WorkerRemoval{Service: service, ServiceRemoved: true}, nil
}
func (b *g1Backend) ResolveWorker(_ context.Context, c backend.WorkerCreate) (backend.WorkerCreated, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resolves++
	return backend.WorkerCreated{Pod: g1Pod(c.Group, c.Rank)}, b.resolveErr
}
func (b *g1Backend) WorkerLogs(_ context.Context, r backend.WorkerReference, _ int64, _ int) (backend.LogChunk, error) {
	if r.Group.Spec.Mode == backend.WorkerTCP && r.Service.UID == "" {
		return backend.LogChunk{}, errors.New("missing rank-zero Service")
	}
	if b.logsEntered != nil {
		close(b.logsEntered)
		<-b.logsReturn
	}
	return backend.LogChunk{Data: []byte("log"), NextOffset: 3}, b.logErr
}
func (b *g1Backend) count(s string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, v := range b.calls {
		if len(v) > len(s) && v[:len(s)] == s {
			n++
		}
	}
	return n
}
func g1Fixture(t *testing.T) (*submissionDataFixture, *Workers, *g1Backend) {
	t.Helper()
	f := newSubmissionDataFixture(t, true, true)
	b := newG1Backend()
	target := backend.Target{Name: "gang", Binding: "gang-binding", Kind: "modeled", Backend: b, OutputStorage: backend.RemoteOutput, SupportsResourceLimits: true}
	c, err := NewWithConfig(f.store, []backend.Target{f.target(), target}, f.engine, Config{Artifacts: f.files})
	if err != nil {
		t.Fatal(err)
	}
	f.control = c
	f.sample(t)
	w, err := NewWorkers(c, WorkerConfig{Budgets: map[string]workeradmission.Budget{"gang": {Ranks: 4, CPUMillis: 4000, MemoryBytes: 4 << 30}}})
	if err != nil {
		t.Fatal(err)
	}
	return f, w, b
}
func g1Submit(t *testing.T, f *submissionDataFixture, w *Workers, mode backend.WorkerMode) store.WorkerGroup {
	t.Helper()
	j, _, err := w.Submit(f.actor, *f.domain, "group", backend.Task{Argv: []string{"train"}, Image: "trainer:v1", Target: "gang", CPUMillis: 1000, MemoryBytes: 1 << 30}, backend.WorkerSpec{Count: 2, Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	g, err := f.store.WorkerGroup(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func g1Step(t *testing.T, w *Workers) {
	t.Helper()
	if err := w.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func g1Read(t *testing.T, f *submissionDataFixture, id string) store.WorkerGroup {
	t.Helper()
	g, err := f.store.WorkerGroup(id)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestG1WorkerLifecycle(t *testing.T) {
	for _, mode := range []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP} {
		t.Run(string(mode), func(t *testing.T) {
			f, w, b := g1Fixture(t)
			g := g1Submit(t, f, w, mode)
			g1Step(t, w)
			g1Step(t, w)
			if b.count("start") != 0 {
				t.Fatal("start before prepared observations")
			}
			g1Step(t, w)
			g = g1Read(t, f, g.Job.ID)
			if g.Barrier.Decision != store.WorkerBarrierStart || b.count("start") != 2 {
				t.Fatalf("decision/start: %+v %d", g.Barrier, b.count("start"))
			}
			b.mu.Lock()
			b.exit = true
			b.mu.Unlock()
			for i := 0; i < 4; i++ {
				g1Step(t, w)
			}
			g = g1Read(t, f, g.Job.ID)
			if g.Phase != store.WorkerGroupDone || g.Reserved || g.Job.Status.State != backend.Succeeded {
				t.Fatalf("not settled: phase=%v reserve=%v status=%v", g.Phase, g.Reserved, g.Job.Status)
			}
			if b.count("create") != 2 {
				t.Fatal("created twice")
			}
		})
	}
}

func TestG1WorkerAllPreparedBeforeToken(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	b.prepared = false
	for i := 0; i < 4; i++ {
		g1Step(t, w)
	}
	g = g1Read(t, f, g.Job.ID)
	if g.Barrier.Decision != store.WorkerBarrierUndecided || b.count("start") != 0 {
		t.Fatal("unprepared rank received start")
	}
	for _, m := range g.Members {
		if m.Barrier.StartToken != ([32]byte{}) {
			t.Fatal("token before barrier")
		}
	}
}

func TestG1WorkerLateCreateAfterCancel(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	b.onCreate = func(rank uint8) {
		if rank == 0 {
			if _, err := f.control.Cancel(f.actor, g.Job.ID); err != nil {
				t.Error(err)
			}
		}
	}
	g1Step(t, w)
	g1Step(t, w)
	g = g1Read(t, f, g.Job.ID)
	if g.Members[0].Pod.UID == "" || g.Members[0].Create.State != store.WorkerRecorded || !g.Job.CancelRequested {
		t.Fatalf("late original lost: %+v", g)
	}
	if b.count("release") != 0 || b.count("start") != 0 {
		t.Fatal("started after cancel")
	}
}

func TestG1WorkerUnknownRemoveHolds(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	b.mu.Lock()
	b.exit = true
	b.unknownRemove = true
	b.mu.Unlock()
	for i := 0; i < 4; i++ {
		g1Step(t, w)
	}
	before := b.count("remove")
	g1Step(t, w)
	g1Step(t, w)
	g = g1Read(t, f, g.Job.ID)
	if b.count("remove") != before || !g.Reserved || g.Phase == store.WorkerGroupDone {
		t.Fatal("unknown remove retried or refunded")
	}
}

func TestG1WorkerLogOwnerBlocksRemoval(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	b.logsEntered = make(chan struct{})
	b.logsReturn = make(chan struct{})
	done := make(chan error, 1)
	var closeRead sync.Once
	joined := false
	t.Cleanup(func() {
		closeRead.Do(func() { close(b.logsReturn) })
		if !joined {
			select {
			case <-done:
				joined = true
				t.Log("original log read joined during failure cleanup")
			case <-time.After(5 * time.Second):
				f.hold(errors.New("original modeled log read did not join"))
				t.Error("retain original modeled log read")
			}
		}
	})
	go func() {
		_, err := w.Logs(context.Background(), f.actor, g.Job.ID, g.Group.AttemptID, 1, 0, 10)
		done <- err
	}()
	select {
	case <-b.logsEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("log original did not enter")
	}
	b.mu.Lock()
	b.exit = true
	b.mu.Unlock()
	g1Step(t, w)
	if b.count("remove") != 0 {
		t.Fatal("removed with original log owner")
	}
	release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
	if release != nil {
		release()
	}
	if !errors.Is(err, ErrLogsUnavailable) {
		t.Fatalf("new logs not fenced: %v", err)
	}
	closeRead.Do(func() { close(b.logsReturn) })
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("log original did not return")
	}
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	if g1Read(t, f, g.Job.ID).Phase != store.WorkerGroupDone {
		t.Fatal("settlement after actual log return failed")
	}
}

func TestG1WorkerAuthUnavailableHolds(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	f.stopAuth()
	select {
	case <-f.authDone:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh original did not join")
	}
	deadline := time.Now().Add(5 * time.Second)
	for f.engine.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("snapshot did not expire")
		}
		time.Sleep(20 * time.Millisecond)
	}
	before := g1Read(t, f, g.Job.ID)
	if err := f.engine.CheckDelegation(before.Authority); !errors.Is(err, auth.ErrUnavailable) {
		t.Fatal(err)
	}
	g1Step(t, w)
	after := g1Read(t, f, g.Job.ID)
	if !reflect.DeepEqual(before, after) || b.count("create") != 0 || b.count("observe") != 0 {
		t.Fatal("unavailable authority changed group or started effects")
	}
}

func TestG1WorkerReplayBeforePlanning(t *testing.T) {
	f, w, _ := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	w.targets = map[string]workerTarget{}
	replay, spec, err := w.Submit(f.actor, *f.domain, "group", g.Job.Task, g.Group.Spec)
	if err != nil || replay.ID != g.Job.ID || spec == nil || *spec != g.Group.Spec {
		t.Fatalf("canonical replay consulted removed placement: %v %+v", err, replay)
	}
}

// The original successful backend return is retained when the REAL Store is
// closed before its record. Reopen is actual bbolt, not a copied Store. This
// tests failed-record retry in one process, not another-generation recovery.
func TestG1WorkerRetainsOriginalThroughStoreReopen(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	b.onCreate = func(rank uint8) {
		if rank == 0 {
			if err := f.store.Close(); err != nil {
				t.Error(err)
			}
			f.store = nil
		}
	}
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("record on closed Store succeeded")
	}
	runner := w.barriers[g.Job.ID]
	if runner.pending == nil || runner.pending.receipt.Created.Pod.UID == "" || runner.pending.recordErr == nil || b.count("create") != 1 {
		t.Fatal("actual original result discarded")
	}
	original := runner.pending
	reopened, err := store.Open(f.directory + "/state")
	if err != nil {
		t.Fatal(err)
	}
	f.store = reopened
	f.control.store = reopened
	runner.store = storeWorkerBarriers{reopened}
	b.onCreate = nil
	g1Step(t, w)
	current := g1Read(t, f, g.Job.ID)
	if current.Members[0].Pod != original.receipt.Created.Pod || current.Members[0].Create.State != store.WorkerRecorded || b.count("create") != 2 {
		t.Fatalf("original repeated or replaced: %+v calls=%d", current.Members[0], b.count("create"))
	}
	if runner.pending != nil {
		t.Fatal("positive exact original record not discharged")
	}
}

func TestG1WorkerTrueAuthorityLossRevokes(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	f.prepareRevocation(t)
	if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(5 * time.Second)
	for !errors.Is(f.engine.CheckDelegation(g.Authority), auth.ErrUnauthenticated) {
		if time.Now().After(until) {
			t.Fatal("actual refresh did not observe revocation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	g1Step(t, w)
	g = g1Read(t, f, g.Job.ID)
	if !g.Revoked || g.Job.Status.State != backend.Failed || b.count("create") != 0 || b.count("start") != 0 {
		t.Fatalf("true loss not effective: %+v", g)
	}
}

func TestG1WorkerSettledLostCreateResolves(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	b.createLost = true
	b.createErr = errors.New("MODELED settled local call, native create response lost")
	for i := 0; i < 5; i++ {
		g1Step(t, w)
	}
	got := g1Read(t, f, g.Job.ID)
	if b.count("create") != 1 || b.resolves == 0 || got.Phase != store.WorkerGroupDone || got.Reserved || got.Job.Status.State != backend.Failed {
		t.Fatalf("unknown create not resolved cleanup-only: creates=%d resolves=%d phase=%d reserved=%v state=%v", b.count("create"), b.resolves, got.Phase, got.Reserved, got.Job.Status.State)
	}
}

// g1OwnerError is a MODELED owner token for the typed backend boundary. It
// asserts no actual native descriptor closure; the exact pointer must survive.
type g1OwnerError struct{ label string }

func (e *g1OwnerError) Error() string { return e.label }
func (e *g1OwnerError) Unwrap() error { return backend.ErrWorkerUnsettled }
func TestG1WorkerUnsettledOriginalSurvivesStoreAck(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	original := &g1OwnerError{"MODELED original reader owner not closed"}
	b.createErr = original
	g1Step(t, w)
	g1Step(t, w)
	runner := w.barriers[g.Job.ID]
	if runner.held == nil || runner.pending != nil {
		t.Fatal("unsettled original lost after Store ACK")
	}
	var owner *g1OwnerError
	if !errors.As(runner.held.cause, &owner) || owner != original {
		t.Fatal("original owner identity lost")
	}
	before := g1Read(t, f, g.Job.ID)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	after := g1Read(t, f, g.Job.ID)
	if !reflect.DeepEqual(before, after) || !after.Reserved || b.count("create") != 1 || b.resolves != 0 || b.count("remove") != 0 {
		t.Fatal("unsettled owner permitted next effect or refund")
	}
}

func TestG1WorkerUnsettledRemovalRetainsPartialFactsWithoutRefund(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	original := &g1OwnerError{"MODELED last original removal reader unsettled"}
	b.mu.Lock()
	b.exit = true
	b.removeErr = original
	b.mu.Unlock()
	g1Step(t, w)
	got := g1Read(t, f, g.Job.ID)
	runner := w.barriers[g.Job.ID]
	if !got.Members[0].Removed || !got.Members[1].Removed {
		t.Fatal("actual partial positive retirement facts discarded")
	}
	if got.Phase == store.WorkerGroupDone || !got.Reserved || runner.held == nil || !errors.Is(runner.held.cause, backend.ErrWorkerUnsettled) {
		t.Fatal("Store ACK discharged original unsettled owner or refunded group")
	}
}

func TestG1WorkerUnsettledLogRetainsOriginalPermit(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	original := &g1OwnerError{"MODELED original log reader not closed"}
	b.logErr = original
	if _, err := w.Logs(context.Background(), f.actor, g.Job.ID, g.Group.AttemptID, 1, 0, 10); !errors.Is(err, backend.ErrWorkerUnsettled) {
		t.Fatalf("typed original error lost: %v", err)
	}
	held := w.logFailures[g.Job.ID]
	if len(held) != 1 || held[0].cause != original || held[0].reference.Service.UID == "" || w.logs[g.Job.ID] != 1 {
		t.Fatal("exact original log owner or Service lost")
	}
	b.mu.Lock()
	b.exit = true
	beforeCalls := len(b.calls)
	b.mu.Unlock()
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	if len(b.calls) != beforeCalls || b.count("remove") != 0 || !g1Read(t, f, g.Job.ID).Reserved {
		t.Fatal("unsettled original log reader permitted a later native call or refund")
	}
}

func TestG1WorkerUnsettledObservationPrecedesVanished(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	g1Step(t, w)
	original := &g1OwnerError{"MODELED failed original observation reader"}
	b.observeErr = errors.Join(backend.ErrWorkerVanished, original)
	g1Step(t, w)
	runner := w.barriers[g.Job.ID]
	var owner *g1OwnerError
	if runner.held == nil || !errors.As(runner.held.cause, &owner) || owner != original || runner.held.observed.Pod.UID == "" {
		t.Fatal("joined vanished hid original unsettled reader")
	}
	current := g1Read(t, f, g.Job.ID)
	if current.Members[0].Barrier.Vanished {
		t.Fatal("unsettled observation recorded as vanished")
	}
	before := len(b.calls)
	g1Step(t, w)
	if len(b.calls) != before {
		t.Fatal("unsettled observation allowed another native call")
	}
}

func TestG1WorkerSameTokenRedeliveryDoesNotExecuteTwice(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	b.startLost = true
	for i := 0; i < 5; i++ {
		g1Step(t, w)
	}
	current := g1Read(t, f, g.Job.ID)
	if b.count("start") != 3 || b.mainExec != 2 {
		t.Fatalf("MODELED same-token dedup: deliveries=%d main=%d", b.count("start"), b.mainExec)
	}
	for _, m := range current.Members {
		if m.Barrier.Start.State != store.WorkerRecorded || !m.Barrier.StartAck {
			t.Fatal("settled lost response did not recover same stored token")
		}
	}
}

func TestG1WorkerLogAdmissionExcludesOriginalRemovalPass(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	b.onRemove = func(rank uint8) {
		if rank == 0 {
			close(entered)
			<-finish
		}
	}
	b.mu.Lock()
	b.exit = true
	b.mu.Unlock()
	stepped := make(chan error, 1)
	var admitted chan error
	stepJoined, admissionJoined := false, false
	t.Cleanup(func() {
		once.Do(func() { close(finish) })
		if !stepJoined {
			select {
			case <-stepped:
				stepJoined = true
				t.Log("original removal pass joined during failure cleanup")
			case <-time.After(5 * time.Second):
				f.hold(errors.New("original modeled removal pass did not join"))
				t.Error("retain original modeled removal pass")
			}
		}
		if admitted != nil && !admissionJoined {
			select {
			case <-admitted:
				admissionJoined = true
				t.Log("original log admission joined during failure cleanup")
			case <-time.After(5 * time.Second):
				f.hold(errors.New("original modeled log admission did not join"))
				t.Error("retain original modeled log admission")
			}
		}
	})
	go func() { stepped <- w.Step(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("original Remove did not enter")
	}
	admitted = make(chan error, 1)
	go func() {
		release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
		if release != nil {
			release()
		}
		admitted <- err
	}()
	// The external owner is visible before BeginLogs waits on the actual pass
	// mutex. No log lease may be admitted beneath the in-flight Remove.
	until := time.Now().Add(5 * time.Second)
	for {
		select {
		case err := <-admitted:
			admissionJoined = true
			t.Fatalf("log admission returned beneath original removal pass: %v", err)
		default:
		}
		w.c.shutdown.mu.Lock()
		n := w.c.shutdown.external
		w.c.shutdown.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("log admission did not reach pass lock")
		}
		time.Sleep(time.Millisecond)
	}
	w.logMu.Lock()
	n := w.logs[g.Job.ID]
	w.logMu.Unlock()
	if n != 0 {
		t.Fatal("new reader admitted beneath original Remove")
	}
	once.Do(func() { close(finish) })
	select {
	case err := <-stepped:
		stepJoined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original Remove/result pass not joined")
	}
	select {
	case err := <-admitted:
		admissionJoined = true
		if !errors.Is(err, ErrLogsUnavailable) {
			t.Fatalf("post-removal recheck: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("log admission original not joined")
	}
	if current := g1Read(t, f, g.Job.ID); current.Phase != store.WorkerGroupDone || current.Reserved {
		t.Fatal("positive original removal did not settle")
	}
}

func TestG1WorkerPendingRemovalRefusesLogAdmission(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	b.onRemove = func(rank uint8) {
		if rank == 0 {
			if err := f.store.Close(); err != nil {
				t.Error(err)
			}
			f.store = nil
		}
	}
	b.mu.Lock()
	b.exit = true
	b.mu.Unlock()
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("positive removal record on closed Store succeeded")
	}
	runner := w.barriers[g.Job.ID]
	if runner.pending == nil || !runner.pending.receipt.Removed.PodRemoved {
		t.Fatal("positive original removal receipt not retained")
	}
	reopened, err := store.Open(f.directory + "/state")
	if err != nil {
		t.Fatal(err)
	}
	f.store = reopened
	f.control.store = reopened
	runner.store = storeWorkerBarriers{reopened}
	if g1Read(t, f, g.Job.ID).Members[0].Removed {
		t.Fatal("failed record unexpectedly committed")
	}
	release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
	if release != nil {
		release()
	}
	if !errors.Is(err, ErrLogsUnavailable) {
		t.Fatalf("reader admitted beneath pending positive removal record: %v", err)
	}
	b.onRemove = nil
	before := b.count("remove")
	g1Step(t, w)
	current := g1Read(t, f, g.Job.ID)
	if current.Phase != store.WorkerGroupDone || current.Reserved || b.count("remove") != before {
		t.Fatal("exact original record retry repeated native removal or failed to settle")
	}
}

func TestG1WorkerUnsettledResolveRetainsRawFoundIdentity(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	original := &g1OwnerError{"MODELED original exact-intent resolve reader held"}
	b.createLost = true
	b.createErr = errors.New("MODELED settled create response loss")
	b.resolveErr = original
	g1Step(t, w)
	g1Step(t, w)
	runner := w.barriers[g.Job.ID]
	current := g1Read(t, f, g.Job.ID)
	if runner.held == nil || runner.held.cause != original || runner.held.kind != barrierResultResolve || runner.held.rawCreated.Pod != g1Pod(g.Group, 0) || runner.held.createPlan.Group.AttemptID != g.Group.AttemptID {
		t.Fatal("original raw Resolve return/plan/owner not retained")
	}
	if !current.Members[0].Barrier.LocalUnsettled || current.Members[0].Pod != runner.held.rawCreated.Pod || !current.Reserved {
		t.Fatal("typed resolve failed to preserve partial facts and persistent fence")
	}
	before := len(b.calls)
	resolves := b.resolves
	g1Step(t, w)
	if len(b.calls) != before || b.resolves != resolves {
		t.Fatal("unsettled resolve allowed native progress")
	}
}

func TestG1WorkerUnsettledCreatePrecedesRefusal(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	original := &g1OwnerError{"MODELED refused response with original local owner held"}
	b.createLost = true
	b.createErr = errors.Join(backend.ErrWorkerCreateRefused, original)
	g1Step(t, w)
	g1Step(t, w)
	current := g1Read(t, f, g.Job.ID)
	runner := w.barriers[g.Job.ID]
	if runner.held == nil || !errors.Is(runner.held.cause, backend.ErrWorkerUnsettled) || current.Members[0].Barrier.CreateRefused || current.Members[0].Create.State != store.WorkerUnknown || !current.Reserved {
		t.Fatal("CreateRefused hid original unsettled owner or granted absence")
	}
}

func TestG1WorkerPendingUnsettledStartRefusesLogAdmission(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	g1Step(t, w)
	g1Step(t, w)
	original := &g1OwnerError{"MODELED Start returned with original local reader held"}
	b.startErr = original
	b.onStart = func(rank uint8) {
		if rank == 0 {
			if err := f.store.Close(); err != nil {
				t.Error(err)
			}
			f.store = nil
		}
	}
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("original Start record on closed Store succeeded")
	}
	runner := w.barriers[g.Job.ID]
	if runner.pending == nil || !runner.pending.receipt.Unsettled || runner.pending.cause != original {
		t.Fatal("unsettled original Start result not retained")
	}
	reopened, err := store.Open(f.directory + "/state")
	if err != nil {
		t.Fatal(err)
	}
	f.store = reopened
	f.control.store = reopened
	runner.store = storeWorkerBarriers{reopened}
	current := g1Read(t, f, g.Job.ID)
	if current.Members[0].Barrier.LocalUnsettled || current.Members[0].Pod.UID == "" {
		t.Fatal("expected actual pre-record member with known Pod and no committed latch")
	}
	release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
	if release != nil {
		release()
	}
	if !errors.Is(err, ErrLogsUnavailable) {
		t.Fatalf("reader admitted beneath pending unsettled original Start: %v", err)
	}
	b.onStart = nil
	g1Step(t, w)
	current = g1Read(t, f, g.Job.ID)
	if !current.Members[0].Barrier.LocalUnsettled || runner.held == nil || b.count("start") != 1 || !current.Reserved {
		t.Fatal("exact original Store retry lost owner or launched another rank")
	}
}

// Only an ORIGINAL locally settled Create refusal proves the missing Pod was
// never created. Its returned Service remains an exact UID cleanup obligation.
func TestG1WorkerOriginalPartialServiceRefusalCleansKnownService(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	b.createServiceOnly = true
	b.createErr = backend.ErrWorkerCreateRefused
	for i := 0; i < 5; i++ {
		g1Step(t, w)
	}
	current := g1Read(t, f, g.Job.ID)
	if current.Phase != store.WorkerGroupDone || current.Reserved || current.Job.Status.State != backend.Failed {
		t.Fatalf("original partial refusal did not settle Failed: phase=%d reserved=%v state=%v", current.Phase, current.Reserved, current.Job.Status.State)
	}
	m := current.Members[0]
	if m.Pod != (backend.WorkerResource{}) || !m.Barrier.CreateRefused || m.Barrier.Service.UID != "service-"+g.Group.AttemptID || !m.Barrier.ServiceRemoved {
		t.Fatal("missing Pod inferred incorrectly or original Service obligation discarded")
	}
	if b.count("create") != 1 || b.count("remove") != 1 || b.resolves != 0 || b.count("cancel") != 0 || b.count("release") != 0 || b.count("start") != 0 {
		t.Fatalf("unexpected native MODEL calls: %v resolves=%d", b.calls, b.resolves)
	}
}

func TestG1WorkerOriginalPartialServiceRefusalWithUnsettledOwnerHolds(t *testing.T) {
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	original := &g1OwnerError{"MODELED original partial Create reader unsettled"}
	b.createServiceOnly = true
	b.createErr = errors.Join(backend.ErrWorkerCreateRefused, original)
	g1Step(t, w)
	g1Step(t, w)
	before := g1Read(t, f, g.Job.ID)
	runner := w.barriers[g.Job.ID]
	var owner *g1OwnerError
	if runner.held == nil || !errors.As(runner.held.cause, &owner) || owner != original || runner.held.receipt.Created.Service != before.Members[0].Barrier.Service {
		t.Fatal("original partial Service/owner/cause not retained")
	}
	if before.Members[0].Barrier.CreateRefused || !before.Members[0].Barrier.LocalUnsettled || before.Members[0].Pod != (backend.WorkerResource{}) {
		t.Fatal("joined marker granted missing-Pod absence or lost persistent local fence")
	}
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	after := g1Read(t, f, g.Job.ID)
	if !reflect.DeepEqual(before, after) || !after.Reserved || after.Phase == store.WorkerGroupDone || b.count("create") != 1 || b.count("remove") != 0 || b.resolves != 0 || b.count("cancel") != 0 {
		t.Fatal("unsettled partial original allowed another call or refund")
	}
}

// The legacy Store API historically admitted a task with unspecified resources
// and explicit positive demand. New resource rules must not reject its replay.
func TestG1WorkerLegacyReplayPrecedesNewDemandValidation(t *testing.T) {
	f, w, b := g1Fixture(t)
	task := backend.Task{Argv: []string{"legacy-train"}, Image: "trainer:v1", Target: "gang"}
	spec := backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent}
	authority, err := f.engine.Delegate(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := f.store.SubmitWorkerGroup(f.actor, store.WorkerGroupRequest{
		Domain: *f.domain, RequestID: "legacy-zero-resources", Task: task, Spec: spec,
		Target: "gang", Binding: "gang-binding", Authority: authority,
		Demand: workeradmission.Demand{Ranks: 2, CPUMillis: 1000, MemoryBytes: 1 << 30},
	})
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Barrier != nil || legacy.Job.Task.CPUMillis != 0 || legacy.Job.Task.MemoryBytes != 0 {
		t.Fatal("not a real legacy worker row")
	}
	before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	w.targets = map[string]workerTarget{} // Any new placement would now refuse.
	checks := b.checks.Load()
	replay, replayedSpec, err := w.Submit(f.actor, *f.domain, "legacy-zero-resources", task, spec)
	if err != nil || replay.ID != legacy.Job.ID || replayedSpec == nil || *replayedSpec != spec {
		t.Fatalf("legacy replay consulted new demand/placement: %v", err)
	}
	current := g1Read(t, f, legacy.Job.ID)
	after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if current.Barrier != nil || !reflect.DeepEqual(legacy, current) || !bytes.Equal(before, after) || b.checks.Load() != checks || b.count("create") != 0 {
		t.Fatal("legacy replay wrote, replanned or adopted v1 barrier")
	}
}

func g1ServiceOnlyLostRemove(t *testing.T, sent bool, cause error, wrongService bool) (*submissionDataFixture, *Workers, *g1Backend, store.WorkerGroup) {
	t.Helper()
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerTCP)
	b.createServiceOnly = true
	b.createErr = backend.ErrWorkerCreateRefused
	b.lostServiceRemove = true
	b.serviceRemoveSent = sent
	b.serviceRemoveErr = cause
	b.recoverWrongService = wrongService
	return f, w, b, g
}

func TestG1WorkerOriginalRefusedServiceOnlySentRemovalRecovers(t *testing.T) {
	f, w, b, g := g1ServiceOnlyLostRemove(t, true, nil, false)
	for i := 0; i < 5; i++ {
		g1Step(t, w)
	}
	current := g1Read(t, f, g.Job.ID)
	m := current.Members[0]
	if current.Phase != store.WorkerGroupDone || current.Reserved || current.Job.Status.State != backend.Failed || !m.Barrier.ServiceRemoved {
		t.Fatalf("qualified Service-only recovery did not settle Failed: phase=%d reserve=%v state=%v", current.Phase, current.Reserved, current.Job.Status.State)
	}
	if b.count("create") != 1 || b.count("remove") != 1 || b.count("recover") != 1 || b.count("cancel") != 0 || b.count("release") != 0 || b.count("start") != 0 || b.resolves != 0 {
		t.Fatalf("unexpected MODELED native operations: %v resolves=%d", b.calls, b.resolves)
	}
	if len(b.recoveries) != 1 {
		t.Fatal("missing original recovery attestation")
	}
	recovery := b.recoveries[0]
	if recovery.Ref.Pod != (backend.WorkerResource{}) || recovery.Ref.Service != m.Barrier.Service || recovery.Ref.Service.UID != "service-"+g.Group.AttemptID || recovery.Evidence != backend.WorkerNeverReleased || !recovery.RemoveSent {
		t.Fatal("recovery changed the exact Service-only attestation")
	}
	if m.Barrier.Stop != backend.WorkerStopUnproven || m.Barrier.RemoveHadPod || m.Barrier.RecoverHadPod || !m.Barrier.ServiceRemoveSent {
		t.Fatal("ephemeral no-Pod proof became persisted native Stop or changed original reference")
	}
}

func TestG1WorkerOriginalRefusedServiceOnlyWithoutSentHolds(t *testing.T) {
	f, w, b, g := g1ServiceOnlyLostRemove(t, false, nil, false)
	for i := 0; i < 5; i++ {
		g1Step(t, w)
	}
	current := g1Read(t, f, g.Job.ID)
	if !current.Reserved || current.Phase == store.WorkerGroupDone || current.Members[0].Barrier.ServiceRemoved || b.count("remove") != 1 || b.count("recover") != 0 || b.resolves != 0 {
		t.Fatal("unknown unsent Service removal gained absence/retirement authority")
	}
}

func TestG1WorkerOriginalRefusedServiceOnlyUnsettledRemoveHolds(t *testing.T) {
	original := &g1OwnerError{"MODELED original sent Service Remove reader unsettled"}
	f, w, b, g := g1ServiceOnlyLostRemove(t, true, errors.Join(backend.ErrWorkerCreateRefused, original), false)
	g1Step(t, w)
	g1Step(t, w)
	before := g1Read(t, f, g.Job.ID)
	runner := w.barriers[g.Job.ID]
	var owner *g1OwnerError
	if runner.held == nil || !errors.As(runner.held.cause, &owner) || owner != original || runner.held.reference.Pod != (backend.WorkerResource{}) || runner.held.reference.Service != before.Members[0].Barrier.Service {
		t.Fatal("original Service-only Remove result/owner not retained")
	}
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	after := g1Read(t, f, g.Job.ID)
	if !reflect.DeepEqual(before, after) || !after.Reserved || !after.Members[0].Barrier.LocalUnsettled || after.Members[0].Barrier.ServiceRemoved || b.count("recover") != 0 || b.count("remove") != 1 {
		t.Fatal("joined unsettled owner authorized Service-only recovery/refund")
	}
}

func TestG1WorkerServiceOnlyRecoveryRefusesForeignServiceReceipt(t *testing.T) {
	f, w, b, g := g1ServiceOnlyLostRemove(t, true, nil, true)
	g1Step(t, w)
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("foreign Service UID recovery receipt accepted")
	}
	current := g1Read(t, f, g.Job.ID)
	runner := w.barriers[g.Job.ID]
	if !current.Reserved || current.Members[0].Barrier.ServiceRemoved || runner.pending == nil || runner.pending.reference.Service != current.Members[0].Barrier.Service || runner.pending.receipt.Removed.Service == current.Members[0].Barrier.Service {
		t.Fatal("foreign receipt changed original Service or lost actual pending return")
	}
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("foreign result readback unexpectedly repaired")
	}
	if b.count("recover") != 1 {
		t.Fatal("invalid original result caused another native recovery call")
	}
}

// These are private reducer MODEL states, including some shapes the Store
// decoder cannot produce. They test only the narrow evidence predicate, not
// persistence, native dispatch or validity of the modeled enclosing group.
func TestG1WorkerServiceOnlyRecoveryPredicateModel(t *testing.T) {
	service := backend.WorkerResource{Name: "rendezvous", UID: "original-service"}
	base := barrierGroup{Decision: barrierAbort, Group: backend.WorkerGroup{Spec: backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}}, Members: []barrierMember{
		{Rank: 0, Service: service, Refused: true, Create: store.WorkerEffectState{State: store.WorkerUnknown}, Remove: store.WorkerEffectState{State: store.WorkerUnknown}, RemoveSent: true},
		{Rank: 1},
	}}
	if !barrierServiceOnlyRecovery(base, base.Members[0]) || barrierRecoveryEvidence(base, base.Members[0]) != backend.WorkerNeverReleased {
		t.Fatal("complete narrow MODEL qualification refused")
	}
	cases := []struct {
		name   string
		change func(*barrierGroup, *barrierMember)
	}{
		{"no sent original", func(_ *barrierGroup, m *barrierMember) { m.RemoveSent = false }},
		{"only Resolve refused", func(_ *barrierGroup, m *barrierMember) { m.Refused = false; m.Resolve.Refused = true }},
		{"missing Service", func(_ *barrierGroup, m *barrierMember) { m.Service = backend.WorkerResource{} }},
		{"foreign Service reference", func(_ *barrierGroup, m *barrierMember) { m.Service.UID = "another-service" }},
		{"known Pod without native Stop", func(_ *barrierGroup, m *barrierMember) {
			m.Pod = backend.WorkerResource{Name: "pod", UID: "original-pod"}
		}},
		{"original Remove had Pod", func(_ *barrierGroup, m *barrierMember) { m.RemoveHadPod = true }},
		{"original Recover had Pod", func(_ *barrierGroup, m *barrierMember) { m.RecoverHadPod = true }},
		{"release recorded", func(_ *barrierGroup, m *barrierMember) { m.Release.State = store.WorkerRecorded }},
		{"release unknown", func(_ *barrierGroup, m *barrierMember) { m.Release.State = store.WorkerUnknown }},
		{"no unknown Remove", func(_ *barrierGroup, m *barrierMember) { m.Remove.State = store.WorkerRecorded }},
		{"own local owner", func(_ *barrierGroup, m *barrierMember) { m.LocalUnsettled = true }},
		{"other local owner", func(g *barrierGroup, _ *barrierMember) { g.Members[1].LocalUnsettled = true }},
		{"other member not retired", func(g *barrierGroup, _ *barrierMember) {
			g.Members[1].Pod = backend.WorkerResource{Name: "other", UID: "other-uid"}
			g.Members[1].Create.State = store.WorkerRecorded
		}},
		{"already retired Service", func(_ *barrierGroup, m *barrierMember) { m.ServiceRemoved = true }},
		{"wrong mode", func(g *barrierGroup, _ *barrierMember) { g.Group.Spec.Mode = backend.WorkerIndependent }},
		{"wrong rank", func(_ *barrierGroup, m *barrierMember) { m.Rank = 1 }},
		{"already settled", func(g *barrierGroup, _ *barrierMember) { g.Done = true }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := base
			g.Members = append([]barrierMember(nil), base.Members...)
			m := g.Members[0]
			tc.change(&g, &m)
			if barrierServiceOnlyRecovery(g, m) || barrierRecoveryEvidence(g, m) == backend.WorkerNeverReleased {
				t.Fatal("missing prerequisite gained narrow no-Pod recovery evidence")
			}
		})
	}
}

// g1FailedObservation uses the established actual bbolt Close/reopen fault,
// after the MODELED original read returns its value, before its Store record.
func g1FailedObservation(t *testing.T, vanished bool) (*submissionDataFixture, *Workers, *g1Backend, store.WorkerGroup, *barrierResult) {
	t.Helper()
	f, w, b := g1Fixture(t)
	g := g1Submit(t, f, w, backend.WorkerIndependent)
	for i := 0; i < 3; i++ {
		g1Step(t, w)
	}
	current := g1Read(t, f, g.Job.ID)
	originalRef := workerReference(current, current.Members[0])
	if vanished {
		b.observeErr = fmt.Errorf("MODELED original consistent vanished read: %w", backend.ErrWorkerVanished)
	} else {
		b.exit = true
	}
	b.onObserve = func(rank uint8) {
		if rank == 0 {
			if err := f.store.Close(); err != nil {
				f.hold(err)
				t.Fatal(err)
			}
			f.store = nil
		}
	}
	if err := w.Step(context.Background()); err == nil {
		t.Fatal("original observation record on closed Store succeeded")
	}
	runner := w.barriers[g.Job.ID]
	pending := runner.pending
	if pending == nil || !pending.kind.isObservation() || pending.recordErr == nil || pending.at.IsZero() || !reflect.DeepEqual(pending.reference, originalRef) {
		t.Fatal("original read reference/result/time/first Store error discarded")
	}
	if vanished {
		if pending.kind != barrierResultVanished || !errors.Is(pending.cause, backend.ErrWorkerVanished) {
			t.Fatal("original Vanished cause lost")
		}
	} else if pending.kind != barrierResultObservation || pending.observed.Stop != backend.WorkerTerminated || pending.observed.Exit == nil {
		t.Fatal("original native Stop/Exit value lost")
	}
	b.onObserve = nil
	reopened, err := store.Open(filepath.Join(f.directory, "state"))
	if err != nil {
		t.Fatal(err)
	}
	f.store = reopened
	f.control.store = reopened
	runner.store = storeWorkerBarriers{reopened}
	b.observeErr = backend.ErrWorkerVanished // A new read must not replace the original.
	return f, w, b, g, pending
}

func g1RankReads(b *g1Backend, rank uint8) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	want := fmt.Sprintf("observe/%d", rank)
	n := 0
	for _, call := range b.calls {
		if call == want {
			n++
		}
	}
	return n
}

func TestG1WorkerOriginalObservationRetriedBeforeNativeWork(t *testing.T) {
	for _, change := range []string{"unchanged", "cancel", "authority unavailable", "revoked"} {
		t.Run(change, func(t *testing.T) {
			f, w, b, g, pending := g1FailedObservation(t, false)
			switch change {
			case "cancel":
				if _, err := f.control.Cancel(f.actor, g.Job.ID); err != nil {
					t.Fatal(err)
				}
			case "authority unavailable":
				f.stopAuth()
				select {
				case <-f.authDone:
				case <-time.After(5 * time.Second):
					t.Fatal("original auth refresh not joined")
				}
				until := time.Now().Add(5 * time.Second)
				for f.engine.Ready() {
					if time.Now().After(until) {
						t.Fatal("actual snapshot stayed fresh")
					}
					time.Sleep(20 * time.Millisecond)
				}
				if err := f.engine.CheckDelegation(g.Authority); !errors.Is(err, auth.ErrUnavailable) {
					t.Fatal(err)
				}
			case "revoked":
				f.prepareRevocation(t)
				if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
					t.Fatal(err)
				}
				until := time.Now().Add(5 * time.Second)
				for !errors.Is(f.engine.CheckDelegation(g.Authority), auth.ErrUnauthenticated) {
					if time.Now().After(until) {
						t.Fatal("actual refresh did not revoke")
					}
					time.Sleep(20 * time.Millisecond)
				}
				current := g1Read(t, f, g.Job.ID)
				if _, err := f.store.RevokeWorkerGroup(g.Job.ID, current.Revision, auth.ErrUnauthenticated, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			calls := len(b.calls)
			g1Step(t, w)
			current := g1Read(t, f, g.Job.ID)
			member := current.Members[0]
			if w.barriers[g.Job.ID].pending != nil || member.Barrier.Stop != backend.WorkerTerminated || member.Barrier.Observation == nil || !sameBarrierObservation(*member.Barrier.Observation, pending.observed) || !member.Barrier.ObservedAt.Equal(pending.at) || member.Barrier.Vanished || !reflect.DeepEqual(workerReference(current, member), pending.reference) {
				t.Fatal("original Stop/Exit/time/identity was not recorded exactly")
			}
			if len(b.calls) != calls {
				t.Fatal("original observation retry issued another backend operation")
			}
			if change == "cancel" && !current.Job.CancelRequested {
				t.Fatal("late original lost cancellation")
			}
			if change == "revoked" && !current.Revoked {
				t.Fatal("late original lost revocation")
			}
			if change == "unchanged" {
				reads := g1RankReads(b, 0)
				g1Step(t, w)
				if g1RankReads(b, 0) != reads {
					t.Fatal("already proven native Stop Pod reread to finish pending work")
				}
			}
		})
	}
}

func TestG1WorkerVanishedRecordFailureRetainsOriginal(t *testing.T) {
	f, w, b, g, pending := g1FailedObservation(t, true)
	cause, at := pending.cause, pending.at
	calls := len(b.calls)
	g1Step(t, w)
	current := g1Read(t, f, g.Job.ID)
	if w.barriers[g.Job.ID].pending != nil || !current.Members[0].Barrier.Vanished || current.Members[0].Barrier.Stop != backend.WorkerStopUnproven || len(b.calls) != calls || pending.cause != cause || !pending.at.Equal(at) {
		t.Fatal("Vanished original cause/time/ref lost or backend read repeated")
	}
	// Vanished persists only a sticky exact-resource fact, not an event time.
}

func TestG1WorkerObservationExactReadbackIsReadOnly(t *testing.T) {
	for _, vanished := range []bool{false, true} {
		t.Run(fmt.Sprint(vanished), func(t *testing.T) {
			f, w, b, g, pending := g1FailedObservation(t, vanished)
			current := g1Read(t, f, g.Job.ID)
			var err error
			if vanished {
				_, err = f.store.RecordWorkerVanished(g.Job.ID, current.Revision, 0, pending.at)
			} else {
				_, err = f.store.RecordWorkerBarrier(g.Job.ID, current.Revision, 0, pending.observed, pending.at)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			calls := len(b.calls)
			g1Step(t, w)
			after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if w.barriers[g.Job.ID].pending != nil || !bytes.Equal(before, after) || len(b.calls) != calls {
				t.Fatal("exact same-resource semantic readback wrote or called backend")
			}
		})
	}
}

func TestG1WorkerPendingObservationRefusesNewerOrConflictingFacts(t *testing.T) {
	for _, kind := range []string{"newer", "conflicting older", "foreign binding"} {
		t.Run(kind, func(t *testing.T) {
			f, w, b, g, pending := g1FailedObservation(t, false)
			current := g1Read(t, f, g.Job.ID)
			if kind == "foreign binding" {
				// Private MODEL corruption of the retained frame, not a Store mutation.
				pending.reference.Group.Execution.Binding = "foreign-binding"
			} else {
				newer := cloneBarrierObservation(pending.observed)
				at := pending.at.Add(time.Second)
				if kind == "conflicting older" {
					newer.Exit.ExitCode = 7
					at = pending.at.Add(-time.Nanosecond)
				}
				if _, err := f.store.RecordWorkerBarrier(g.Job.ID, current.Revision, 0, newer, at); err != nil {
					t.Fatal(err)
				}
			}
			firstError := pending.recordErr
			before, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			calls := len(b.calls)
			if err := w.Step(context.Background()); err == nil {
				t.Fatal("incompatible original read unexpectedly overwritten/acknowledged")
			}
			after, err := os.ReadFile(filepath.Join(f.store.Directory(), "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if w.barriers[g.Job.ID].pending != pending || pending.recordErr != firstError || !bytes.Equal(before, after) || len(b.calls) != calls {
				t.Fatal("negative semantic retry lost original/error or performed new work")
			}
			release, err := w.BeginLogs(f.actor, g.Job.ID, g.Group.AttemptID, 0)
			if release != nil {
				release()
			}
			if !errors.Is(err, ErrLogsUnavailable) {
				t.Fatal("new native log read admitted beneath unfinished observation")
			}
		})
	}
}
