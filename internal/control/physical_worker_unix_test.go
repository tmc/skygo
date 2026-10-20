//go:build linux || darwin

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// These component tests use the actual original maintenance worker and its
// production receive path. They remain UNRUN and prove no native/FD closure.
func physicalMaintenanceFixture(t *testing.T) (context.Context, *maintenanceState) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	m := newMaintenance(ctx)
	t.Cleanup(func() { cancel(); close(m.input); waitValue(t, m.done) })
	return ctx, m
}

func TestPhysicalMaintenanceWithoutRetentionAndPeriodicRefresh(t *testing.T) {
	c, _ := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	if c.StorageReady() {
		t.Fatal("missing artifact sample reported ready")
	}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.physicalOwner != c {
		t.Fatal("retention-off did not dispatch actual physical sample")
	}
	r := waitValue(t, m.results)
	if err := c.receiveMaintenance(ctx, m, r); err != nil {
		t.Fatal(err)
	}
	if r.physical == nil || !r.physical.Complete || r.physical.Registered {
		t.Fatalf("actual root interval: %+v", r.physical)
	}
	if c.StorageReady() != (c.cachedLinkGrowthCheck() == nil) {
		t.Fatal("readiness did not use actual same cached guard")
	}
	if _, enabled, err := c.store.RetentionPolicy(); err != nil || enabled {
		t.Fatal("physical sample enabled retention", enabled, err)
	}
	// A completed engine need not request another pass; adapter freshness still
	// schedules periodic work without setting retention or a request domain.
	p := &c.artifacts.physical
	p.mu.Lock()
	p.wantSample = false
	p.mu.Unlock()
	m.due, m.physicalDue = time.Time{}, time.Time{}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.physicalOwner != c {
		t.Fatal("periodic artifact refresh missing")
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalAndLegacySampleTurnsAlternate(t *testing.T) {
	c, _ := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	for i := 0; i < 4; i++ {
		c.retention.wantSample.Store(true) // Explicit legacy diagnostic demand.
		c.physicalChanged()
		m.due = time.Time{}
		if err := c.maintain(ctx, m); err != nil {
			t.Fatal(err)
		}
		if m.pending == nil {
			t.Fatal("sample turn omitted")
		}
		physical := m.pending.physicalOwner == c
		if physical != (i%2 == 0) {
			t.Fatal("two sample producers did not alternate", i, physical)
		}
		wantClass := maintenanceLegacySampleClass
		if !physical {
			wantClass = maintenanceRetainedCleanupClass
			if m.pending.workspaceRetirement != nil || m.pending.record != nil || m.pending.directory != c.store.Directory() {
				t.Fatal("legacy turn selected another concrete operation")
			}
		}
		if m.nextClass != wantClass {
			t.Fatal("sample did not advance its explicit ordinary fairness class")
		}
		if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPhysicalBusyKeepsOriginalFrameAndNoFatalWorker(t *testing.T) {
	c, _ := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		defer calls[i].Done()
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	busy := waitValue(t, m.results)
	if busy.physical == nil || !errors.Is(busy.physical.Err, ErrArtifactBusy) || !busy.physical.Registered {
		t.Fatal("fifth slot was invented", busy.physical)
	}
	if err := c.receiveMaintenance(ctx, m, busy); err != nil {
		t.Fatal("ordinary common-four contention became fatal", err)
	}
	serial := m.physicalSerial
	if m.pending != nil || physicalMaintenanceHeld(m) || serial == 0 {
		t.Fatal("busy return lost the actual registered frame")
	}
	for _, call := range calls {
		call.Done()
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	r := waitValue(t, m.results)
	if r.physical == nil || r.physical.Serial != serial {
		t.Fatal("busy frame replaced", r.physical)
	}
	if err := c.receiveMaintenance(ctx, m, r); err != nil {
		t.Fatal(err)
	}
}

func TestPhysicalPendingProtectsBeforeRegisterAndAfterReturn(t *testing.T) {
	for _, window := range []string{"before dequeue", "after return"} {
		t.Run(window, func(t *testing.T) {
			c, _ := physicalControlFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1), done: make(chan struct{})}
			if err := c.dispatchPhysical(m); err != nil {
				t.Fatal(err)
			}
			if window == "before dequeue" {
				if c.physicalRegistered() {
					t.Fatal("queued request fabricated a scan")
				}
				c.StopArtifactAdmission()
				if artifactTerminalClosed(c) {
					t.Fatal("terminal preceded actual queued owner")
				}
				if err := c.maintain(ctx, m); err != nil || len(m.input) != 1 {
					t.Fatal("stop dispatched a second operation beside its pending quantum", err)
				}
			}
			go func() { defer close(m.done); runMaintenance(ctx, m.input, m.results) }()
			defer func() { cancel(); close(m.input); waitValue(t, m.done) }()
			r := waitValue(t, m.results)
			if window == "after return" {
				if r.physical == nil || !r.physical.Complete {
					t.Fatal("missing actual returned interval")
				}
				c.StopArtifactAdmission()
			}
			if artifactTerminalClosed(c) {
				t.Fatal("terminal preceded actual result integration")
			}
			if err := c.receiveMaintenance(ctx, m, r); err != nil {
				t.Fatal(err)
			}
			if !artifactTerminalClosed(c) {
				t.Fatal("actual empty/closed frame did not retire its mirror")
			}
		})
	}
}

// Done observes the actual Drain select boundary. No production hook or timer
// manufactures an owner transition; the underlying context is never canceled
// before the asserted late error returns.
type physicalDrainContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *physicalDrainContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

// Negative custody vectors retain the actual resources and original worker
// state until process exit. An expected refusal grants no cleanup/join credit.
func physicalHeldFixture(t *testing.T) (context.Context, *uploadGrowthResources, *maintenanceState) {
	t.Helper()
	f := newUploadGrowthResources(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.maintenance = newMaintenance(ctx)
	return ctx, f, f.maintenance
}

func TestPhysicalLateHeldReturnWakesFinalDrain(t *testing.T) {
	ctx, f, m := physicalHeldFixture(t)
	c := f.control
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		defer calls[i].Done()
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	r := waitValue(t, m.results)
	if r.physical == nil || !errors.Is(r.physical.Err, ErrArtifactBusy) {
		t.Fatal("expected actual pre-IO contention")
	}
	if err := c.receiveMaintenance(ctx, m, r); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		call.Done()
	}
	// Model a conservative unknown-close latch on the actually registered
	// ZERO-FD frame. It tests real late engine/worker/Drain custody, not a kernel
	// Close failure or a fake native/FD receipt. No descriptor exists to retry.
	original := errors.New("modeled original physical uncertainty")
	p := &c.artifacts.physical
	p.mu.Lock()
	if p.frame == nil || p.frame.scan != nil {
		p.mu.Unlock()
		t.Fatal("expected the actual zero-FD refusal frame")
	}
	p.frame.uncertain = original
	p.mu.Unlock()
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	drainCtx := &physicalDrainContext{Context: base, entered: make(chan struct{})}
	drained := make(chan error, 1)
	go func() { drained <- c.DrainArtifacts(drainCtx) }()
	waitValue(t, drainCtx.entered) // No calls/downloads: this is its final wait.
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	late := waitValue(t, m.results)
	if late.physical == nil || !late.physical.Held || late.physical.Err != original {
		t.Fatal("actual engine late return lost original cause", late.physical)
	}
	if err := c.receiveMaintenance(ctx, m, late); err != nil {
		t.Fatal(err)
	}
	err := waitValue(t, drained)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, original) || base.Err() != nil {
		t.Fatal("late physical Held required unrelated cancellation", err)
	}
	if !physicalMaintenanceHeld(m) || !c.physicalRegistered() || artifactTerminalClosed(c) {
		t.Fatal("late original owner was falsely joined")
	}
	if err := c.dispatchPhysical(m); !errors.Is(err, ErrArtifactBusy) {
		t.Fatal("held scan was automatically redispatched", err)
	}
	f.expectedHeld = original
	f.retain(t, err)
}

func TestPhysicalWrongSerialRetainsReceivedPair(t *testing.T) {
	ctx, f, m := physicalHeldFixture(t)
	c := f.control
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	r := waitValue(t, m.results)
	if r.physical == nil {
		t.Fatal("missing actual result")
	}
	// Corruption is a result-provenance vector, never physical proof.
	m.physicalSerial = r.physical.Serial + 1
	request := m.pending
	err := c.receiveMaintenance(ctx, m, r)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal("changed original serial accepted", err)
	}
	if m.pending != request || m.received == nil || m.received.request != request || m.received.result.physical != r.physical {
		t.Fatal("serial refusal discarded original received custody")
	}
	c.artifacts.mu.Lock()
	pending := c.artifacts.physicalWorkPending
	c.artifacts.mu.Unlock()
	if !pending {
		t.Fatal("failed integration cleared physical dependency")
	}
	f.expectedHeld = err
	f.retain(t, err)
}

func TestPhysicalCanceledReceiveKeepsOriginalDependency(t *testing.T) {
	_, f, m := physicalHeldFixture(t)
	c := f.control
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	r := waitValue(t, m.results)
	request := m.pending
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.receiveMaintenance(ctx, m, r)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.pending != request || m.received == nil || m.received.request != request || m.received.result.physical != r.physical {
		t.Fatal("canceled receive discarded actual physical return")
	}
	c.StopArtifactAdmission()
	if artifactTerminalClosed(c) {
		t.Fatal("canceled integration manufactured a service join")
	}
	f.expectedHeld = err
	f.retain(t, err)
}

func TestPhysicalYieldedFDClosesOnlyThroughOriginalWorker(t *testing.T) {
	c, root := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	if err := root.Mkdir("bulk", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4097; i++ {
		file, err := root.OpenFile(fmt.Sprintf("bulk/%04d", i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	yielded := waitValue(t, m.results)
	if yielded.physical == nil || !yielded.physical.Registered || yielded.physical.Complete || yielded.physical.Err != nil {
		t.Fatalf("actual yielded census: %+v", yielded.physical)
	}
	if err := c.receiveMaintenance(ctx, m, yielded); err != nil {
		t.Fatal(err)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	original := p.frame
	hasScan := original != nil && original.scan != nil
	p.mu.Unlock()
	if !hasScan {
		t.Fatal("yield did not retain its original actual scan")
	}
	c.artifacts.mu.Lock()
	active := len(c.artifacts.workspaceIO)
	c.artifacts.mu.Unlock()
	if active != 0 {
		t.Fatal("positive returned quantum retained an active permit", active)
	}
	c.StopArtifactAdmission()
	if artifactTerminalClosed(c) {
		t.Fatal("zero active permits concealed yielded scan FDs")
	}
	// No Run coordinator is running in this component fixture: only its exact
	// next maintenance dispatch may close the cursor. Stop performs no Close.
	p.mu.Lock()
	same := p.frame == original && p.frame.scan != nil
	p.mu.Unlock()
	if !same {
		t.Fatal("Stop replaced or synchronously closed the original cursor")
	}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	closed := waitValue(t, m.results)
	if closed.physical == nil || closed.physical.Registered || closed.physical.Serial != yielded.physical.Serial {
		t.Fatal("original worker did not return exact scan Close", closed.physical)
	}
	if artifactTerminalClosed(c) {
		t.Fatal("unintegrated Close return retired service dependency")
	}
	if err := c.receiveMaintenance(ctx, m, closed); err != nil {
		t.Fatal(err)
	}
	if !artifactTerminalClosed(c) || c.physicalRegistered() {
		t.Fatal("actual Close/integration did not retire original scan")
	}
}

func TestPhysicalYieldedCloseBusyWaitsForExistingDeadline(t *testing.T) {
	c, root := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	if err := root.Mkdir("bulk", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4097; i++ {
		file, err := root.OpenFile(fmt.Sprintf("bulk/%04d", i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	yielded := waitValue(t, m.results)
	if yielded.physical == nil || !yielded.physical.Registered || yielded.physical.Complete || yielded.physical.Err != nil {
		t.Fatalf("actual yielded interval: %+v", yielded.physical)
	}
	if err := c.receiveMaintenance(ctx, m, yielded); err != nil {
		t.Fatal(err)
	}
	p := &c.artifacts.physical
	p.mu.Lock()
	original := p.frame
	if original == nil || original.scan == nil {
		p.mu.Unlock()
		t.Fatal("expected actual original scan with retained FDs")
	}
	scan := original.scan
	p.mu.Unlock()
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
		defer calls[i].Done()
	}
	if err := c.dispatchPhysical(m); err != nil {
		t.Fatal(err)
	}
	sampleBusy := waitValue(t, m.results)
	if sampleBusy.request.physicalClose || sampleBusy.physical == nil || !errors.Is(sampleBusy.physical.Err, ErrArtifactBusy) || sampleBusy.physical.Serial != yielded.physical.Serial {
		t.Fatal("expected actual ordinary sampling contention", sampleBusy.physical)
	}
	if err := c.receiveMaintenance(ctx, m, sampleBusy); err != nil {
		t.Fatal(err)
	}
	// A NEW close demand remains prompt even if normal sample deadlines are
	// in the future. No elapsed-time wait or product clock hook is needed.
	m.due, m.physicalDue = time.Now().Add(time.Hour), time.Now().Add(time.Hour)
	c.StopArtifactAdmission()
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || !m.pending.physicalClose {
		t.Fatal("first original close was delayed by a sampling deadline")
	}
	busy := waitValue(t, m.results)
	returnedAt := time.Now()
	if busy.physical == nil || !errors.Is(busy.physical.Err, ErrArtifactBusy) || !busy.physical.Registered || busy.physical.Held || busy.physical.Serial != yielded.physical.Serial {
		t.Fatalf("actual yielded close contention: %+v", busy.physical)
	}
	if err := c.receiveMaintenance(ctx, m, busy); err != nil {
		t.Fatal("ordinary close contention became fatal", err)
	}
	if !m.physicalDue.After(returnedAt) || m.physicalDue.After(time.Now().Add(time.Second)) {
		t.Fatal("actual returned Busy did not set the existing one-second deadline", m.physicalDue)
	}
	sequence := m.serial
	// Control the EXISTING deadline value to make consecutive-before-deadline
	// assertions deterministic. The actual Busy path above established its
	// production deadline; this is not a claim about wall-clock scheduling.
	m.physicalDue = time.Now().Add(time.Hour)
	for i := 0; i < 8; i++ {
		if err := c.maintain(ctx, m); err != nil {
			t.Fatal(err)
		}
		if m.pending != nil || len(m.input) != 0 || m.serial != sequence || physicalMaintenanceHeld(m) {
			t.Fatal("close Busy redispatched or manufactured Held before its deadline")
		}
	}
	p.mu.Lock()
	same := p.frame == original && p.frame.scan == scan
	p.mu.Unlock()
	if !same || m.physicalSerial != yielded.physical.Serial {
		t.Fatal("backoff replaced the original yielded scan")
	}
	c.artifacts.mu.Lock()
	protected := c.artifacts.physicalFrameRegistered && !c.artifacts.physicalWorkPending && c.artifacts.physicalHeld == nil
	c.artifacts.mu.Unlock()
	if !protected || artifactTerminalClosed(c) {
		t.Fatal("busy deadline discarded original registration")
	}
	// An actual original slot is released; making the same finite deadline due
	// permits the next SAME-worker close, not a replacement cursor or fifth slot.
	calls[0].Done()
	m.physicalDue = time.Time{}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || !m.pending.physicalClose {
		t.Fatal("due original close was not dispatched")
	}
	closed := waitValue(t, m.results)
	if closed.physical == nil || closed.physical.Registered || closed.physical.Held || closed.physical.Err != nil || closed.physical.Serial != yielded.physical.Serial {
		t.Fatalf("same original cursor close: %+v", closed.physical)
	}
	c.artifacts.mu.Lock()
	stillPending := c.artifacts.physicalWorkPending
	c.artifacts.mu.Unlock()
	if !stillPending {
		t.Fatal("unintegrated original Close cleared dependency")
	}
	if err := c.receiveMaintenance(ctx, m, closed); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		call.Done()
	}
	if err := c.DrainArtifacts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !artifactTerminalClosed(c) || c.physicalRegistered() {
		t.Fatal("positive original Close/integration did not retire cursor")
	}
}
