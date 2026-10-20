//go:build linux

package control

import (
	"context"
	"github.com/tmc/skygo/internal/store"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
)

// Constructor topology uses actual Local pointers and the actual Store owner.
// No native process is started; this is not a Local shutdown/FD oracle.
func TestShutdownPreparationConcreteLocalMembership(t *testing.T) {
	for _, name := range []string{"missing", "different pointer", "duplicate pointer", "wrong binding", "exact"} {
		t.Run(name, func(t *testing.T) {
			f := newShutdownPreparationFixture(t)
			owned, err := local.New(filepath.Join(f.directory, "owned-logs"))
			if owned != nil {
				f.locals = append(f.locals, owned)
			}
			if err != nil {
				f.held = err
				t.Fatal(err)
			}
			binding, err := backend.LocalBinding(f.store.Owner())
			if err != nil {
				t.Fatal(err)
			}
			targets := []backend.Target{{Name: "owned", Kind: "local", Binding: binding, Backend: owned}}
			switch name {
			case "missing":
				targets = []backend.Target{{Name: "remote", Kind: "local", Binding: binding, Backend: &routeBackend{}}}
			case "different pointer":
				other, err := local.New(filepath.Join(f.directory, "other-logs"))
				if other != nil {
					f.locals = append(f.locals, other)
				}
				if err != nil {
					f.held = err
					t.Fatal(err)
				}
				targets[0].Backend = other
			case "duplicate pointer":
				targets = append(targets, backend.Target{Name: "alias", Kind: "local", Binding: binding, Backend: owned})
			case "wrong binding":
				targets[0].Binding = "unrelated-installation"
			}
			c, err := NewWithConfig(f.store, targets, testEngine, Config{LocalBackend: owned})
			if name != "exact" {
				if err == nil {
					t.Fatal("constructor accepted unowned concrete Local topology")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.BeginShutdown(); err != nil {
				t.Fatal(err)
			}
			f.start(c)
			preparationWait(t, c)
			// Preparing a fresh owned Local requires its actual pure CheckShutdown;
			// merely a local Kind/name on a different Backend cannot supply it.
			if err := owned.CheckShutdown(); err != nil {
				t.Fatal("fresh actual Local failed its preparation query", err)
			}
		})
	}
}

// UNRUN native producer/query prerequisite: actual owned Local executes and is
// stopped only through original target Cancel→Observe; it is not a fake Backend
// receipt. Local C3/query source acceptance and native qualification remain due.
func TestShutdownPreparationOwnedLocalCancelObserve(t *testing.T) {
	f := newShutdownPreparationFixture(t)
	b, err := local.New(filepath.Join(f.directory, "logs"))
	if b != nil {
		f.locals = append(f.locals, b)
	}
	if err != nil {
		f.held = err
		t.Fatal(err)
	}
	binding, err := backend.LocalBinding(f.store.Owner())
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewWithConfig(f.store, []backend.Target{{Name: "owned", Binding: binding, Backend: b}}, testEngine, Config{LocalBackend: b})
	if err != nil {
		t.Fatal(err)
	}
	job, err := c.Submit(testActor(), "owned-local-stop", backend.Task{Argv: []string{"/bin/sh", "-c", "while :; do sleep 1; done"}})
	if err != nil {
		t.Fatal(err)
	}
	f.start(c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		current, err := f.store.Get(job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status.State == backend.Running {
			// Control persists Running after the original Local Observe. The
			// default execution lane does not publish an instance string.
			if current.Status.Instance != "" {
				t.Fatalf("default Local published an instance: %q", current.Status.Instance)
			}
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatalf("HOLD original Local running state was not observed: %+v", current.Status)
		}
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	preparationWait(t, c)
	current, err := f.store.Get(job.ID)
	if err != nil || !current.CancelRequested || current.Status.State != backend.Canceled {
		t.Fatal("owned Local stop did not integrate real terminal observation", current, err)
	}
	if err := b.CheckShutdown(); err != nil {
		t.Fatal(err)
	}
}

// Component handoff uses the same ORIGINAL target/maintenance workers and real
// Store/Local/Files fixture; its loop is manually driven, so this is NOT proof
// of the public original-Run prepared event or the outer server handshake.
func TestShutdownPreparationWorkspaceHandoffKeepsExactCharges(t *testing.T) {
	c, state, m, p, ctx := retirementFixture(t)
	before, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	state.preparing = true
	if err := c.maintainShutdownPreparation(state, m); err != nil {
		t.Fatal(err)
	}
	request := m.pending
	if request == nil || request.workspaceRetirement == nil || request.workspaceRetirement.step != workspaceRetirementHandoff {
		t.Fatal("original pre-Stop handoff was not dispatched")
	}
	result := waitValue(t, m.results)
	if err := c.receiveMaintenance(ctx, m, result); err != nil {
		t.Fatal(err)
	}
	if !positiveRetirementPair(p.retirement.handoff, p, workspaceRetirementHandoff) || p.retirement.handoff.request != request || p.retirement.root != nil || p.retirement.candidate != nil {
		t.Fatal("handoff replaced original pair or manufactured removal")
	}
	after, err := c.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("handle-only handoff changed durable reservations", err)
	}
	if state.workers[0].completion != p {
		t.Fatal("handoff released original completion")
	}
	if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
		t.Fatal("handoff released charged barrier")
	}
	replay, err := c.store.CompleteWorkspaceAttempt(p.job.ID, reference(p.job), p.status, p.completion, time.Now().UTC())
	if err != nil || replay.State != store.WorkspaceCommitMarked || replay.Cleanup == nil || *replay.Cleanup != *p.cleanup {
		t.Fatal("handoff credited RootRemoved or changed original intent", err)
	}
	if err := c.maintainShutdownPreparation(state, m); err != nil || m.pending != nil {
		t.Fatal("handoff reentered Local cleanup", err)
	}
	if err := c.localBackend.CheckShutdown(); err != nil {
		t.Fatal("actual handoff lacks positive Local query", err)
	}
}

// This tests the actual finite post-preparation dispatcher, not a manufactured
// prepared event: Run's source branch is independently required to select it.
// Four real calls make the installed physical engine retain its original Busy
// frame; after they return this same worker must close that frame and dispatch
// no ordinary fresh sample/cleanup despite explicit diagnostic sample demand.
func TestShutdownPreparationDrainDispatchesOnlyOriginalPhysicalClose(t *testing.T) {
	c, _ := physicalControlFixture(t)
	ctx, m := physicalMaintenanceFixture(t)
	c.retention.wantSample.Store(true)
	c.physicalChanged()
	if err := c.maintainShutdownDrain(m); err != nil || m.pending != nil {
		t.Fatal("post-prepared dispatcher admitted fresh sample", err)
	}
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
	actual := waitValue(t, m.results)
	if actual.physical == nil || !actual.physical.Registered {
		t.Fatal("actual Busy scan did not retain its original frame")
	}
	if err := c.receiveMaintenance(ctx, m, actual); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		call.Done()
	}
	c.requestPhysicalClose()
	if err := c.maintainShutdownDrain(m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || !m.pending.physicalClose || m.pending.workspaceRetirement != nil || m.pending.record != nil {
		t.Fatal("post-prepared dispatcher did not select original physical Close")
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
	if c.physicalRegistered() {
		t.Fatal("actual physical Close left frame registered")
	}
	if err := c.maintainShutdownDrain(m); err != nil || m.pending != nil || !c.retention.wantSample.Load() {
		t.Fatal("post-prepared dispatcher consumed ordinary maintenance demand", err)
	}
}

// UNRUN native fixture dependency. Root.Done must not be followed by a new
// Local handoff/Remove call: Local already retired and detached that frame.
// The independent unpublished candidate and exact Store obligation stay charged.
func TestShutdownPreparationDoesNotReenterActualRootDone(t *testing.T) {
	c, state, m, p, ctx, _ := retirementTaskFixture(t, "closed-prefix")
	for n := 0; p.retirement.root == nil; n++ {
		if n == 10 {
			t.Fatal("actual bounded root did not finish")
		}
		p.retirement.due = time.Time{}
		admitted, err := c.dispatchWorkspaceRetirement(m, time.Now())
		if err != nil || !admitted || m.pending == nil || m.pending.workspaceRetirement.step != workspaceRetirementRoot {
			t.Fatal("original root quantum missing", admitted, err)
		}
		if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
			t.Fatal(err)
		}
	}
	rootPair := p.retirement.root
	before, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	state.preparing = true
	if err := c.maintainShutdownPreparation(state, m); err != nil {
		t.Fatal(err)
	}
	after, err := c.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("root-only preparation credited independent candidate", err)
	}
	if m.pending != nil || p.retirement.root != rootPair || p.retirement.handoff != nil || p.retirement.candidate != nil || state.workers[0].completion != p {
		t.Fatal("root Done was reentered or replaced with handoff/candidate credit")
	}
	if err := c.localBackend.CheckShutdown(); err != nil {
		t.Fatal("accepted original detached receipt/query prerequisite not settled", err)
	}
}
