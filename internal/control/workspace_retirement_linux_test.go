//go:build linux

package control

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// retirementFixtureFrame is test-only acquisition custody, registered BEFORE
// acquiring any resource. No t.TempDir owns these namespaces: removal is allowed
// only after this concrete frame has positively joined and closed every owner.
// Failure retention is a test evidence list, not a production cleanup registry.
type retirementFixtureFrame struct {
	path                                                                         string
	acquisition                                                                  error
	held                                                                         error
	root                                                                         *os.Root
	probe                                                                        *os.Root // concrete constructor result in the early-error oracle
	probeClosed                                                                  bool
	files                                                                        *artifact.Files
	store                                                                        *store.Store
	local                                                                        *local.Backend
	control                                                                      *Control
	cancel                                                                       context.CancelFunc
	state                                                                        *loopState
	maintenance                                                                  *maintenanceState
	inputsClosed                                                                 bool
	localClosed, artifactsDrained, filesClosed, rootClosed, storeClosed, removed bool
	retained                                                                     bool
	expectedHold                                                                 bool // only explicit fixture-retention boundary oracles
}

var retirementFixtureHolds struct {
	sync.Mutex
	frames []*retirementFixtureFrame
}

func newRetirementFixtureFrame(t *testing.T) *retirementFixtureFrame {
	t.Helper()
	f := &retirementFixtureFrame{}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.close(ctx); err != nil {
			retirementFixtureHolds.Lock()
			if !f.retained {
				retirementFixtureHolds.frames = append(retirementFixtureHolds.frames, f)
				f.retained = true
			}
			retirementFixtureHolds.Unlock()
			if f.expectedHold {
				t.Logf("expected fixture HOLD %p path=%q acquisition=%v ownership=%v; namespace retained", f, f.path, f.acquisition, err)
			} else {
				t.Errorf("HOLD exact fixture %p path=%q acquisition=%v ownership=%v; namespace retained", f, f.path, f.acquisition, err)
			}
		}
	})
	home, err := os.UserHomeDir()
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261019-retirement-fixtures")
	if err := os.MkdirAll(parent, 0700); err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	f.path, err = os.MkdirTemp(parent, "fixture-")
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	return f
}

func (f *retirementFixtureFrame) close(ctx context.Context) error {
	// Never retry an uncertain original Close, substitute a new owner, or delete
	// a held namespace. A repeated call returns the SAME saved error/pointers.
	if f.held != nil {
		return f.held
	}
	// Initialize/Open can return no Files pointer while this exact error owns an
	// uncertain original descriptor and Files frame. Retain its borrowed root and
	// namespace BEFORE any downstream join/Close/removal; pointer absence is not
	// evidence of closure. Ordinary acquisition errors still unwind below.
	if errors.Is(f.acquisition, artifact.ErrFileCloseUncertain) {
		f.held = f.acquisition
		return f.held
	}
	hold := func(err error) error { f.held = err; return err }
	// Successful fixtures can still own an original read-only physical cursor.
	// Close it using the SAME existing maintenance worker before joining workers.
	// A failed/pending receive is not repaired by a new fixture late integrator.
	if f.control != nil && f.maintenance != nil && !f.inputsClosed {
		m := f.maintenance
		if m.pending != nil || m.received != nil {
			return hold(errors.New("original maintenance request/result remains unintegrated"))
		}
		if f.control.physicalRegistered() {
			f.control.requestPhysicalClose()
			if err := f.control.dispatchPhysical(m); err != nil {
				return hold(err)
			}
			if m.pending == nil {
				return hold(errors.New("original physical Close not dispatched"))
			}
			select {
			case result := <-m.results:
				if err := f.control.receiveMaintenance(ctx, m, result); err != nil {
					return hold(err)
				}
			case <-ctx.Done():
				return hold(fmt.Errorf("original physical Close remains pending: %w", ctx.Err()))
			}
			if f.control.physicalRegistered() || physicalMaintenanceHeld(m) {
				return hold(errors.New("original physical Close did not settle"))
			}
		}
	}
	if f.cancel != nil {
		f.cancel()
	}
	if !f.inputsClosed {
		if f.state != nil {
			for i := range f.state.workers {
				if f.state.workers[i].input != nil {
					close(f.state.workers[i].input)
				}
			}
		}
		if f.maintenance != nil && f.maintenance.input != nil {
			close(f.maintenance.input)
		}
		f.inputsClosed = true
	}
	join := func(done <-chan struct{}) error {
		if done == nil {
			return errors.New("original join channel unavailable")
		}
		select {
		case <-done:
			return nil
		default:
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.state != nil {
		for i := range f.state.workers {
			if err := join(f.state.workers[i].done); err != nil {
				return hold(fmt.Errorf("target %d original join: %w", i, err))
			}
		}
	}
	if f.maintenance != nil {
		if err := join(f.maintenance.done); err != nil {
			return hold(fmt.Errorf("maintenance original join: %w", err))
		}
	}
	// The original backend joins Local before artifact drain; Files borrows root.
	// Root and Store stay reachable until every preceding obligation is positive.
	if f.local != nil && !f.localClosed {
		if err := f.local.Close(); err != nil {
			return hold(fmt.Errorf("original Local close: %w", err))
		}
		f.localClosed = true
	}
	if f.control != nil && !f.artifactsDrained {
		if err := f.control.DrainArtifacts(ctx); err != nil {
			return hold(fmt.Errorf("original artifact drain: %w", err))
		}
		f.artifactsDrained = true
	}
	if f.files != nil && !f.filesClosed {
		if err := f.files.Close(); err != nil {
			return hold(fmt.Errorf("original Files close: %w", err))
		}
		f.filesClosed = true
	}
	if f.probe != nil && !f.probeClosed {
		if err := f.probe.Close(); err != nil {
			return hold(fmt.Errorf("original constructor probe root close: %w", err))
		}
		f.probeClosed = true
	}
	if f.root != nil && !f.rootClosed {
		if err := f.root.Close(); err != nil {
			return hold(fmt.Errorf("original root close: %w", err))
		}
		f.rootClosed = true
	}
	if f.store != nil && !f.storeClosed {
		if err := f.store.Close(); err != nil {
			return hold(fmt.Errorf("original Store close: %w", err))
		}
		f.storeClosed = true
	}
	if f.path != "" && !f.removed {
		if err := os.RemoveAll(f.path); err != nil {
			return hold(fmt.Errorf("retired fixture namespace: %w", err))
		}
		f.removed = true
	}
	return nil
}

// UNRUN. Actual Store, Files, original Local no-call sequencer and maintenance
// worker fixture. Literal Producer tests forwarding, not self capture or death.
// Publishing the original loop pointer below is test scaffolding; this is not
// qualification of Run/server shutdown ordering or native process retirement.
func retirementFixture(t *testing.T) (*Control, *loopState, *maintenanceState, *workspacePending, context.Context) {
	c, state, m, p, ctx, _ := retirementTaskFixture(t, "no-call")
	return c, state, m, p, ctx
}

func retirementTaskFixture(t *testing.T, mode string) (*Control, *loopState, *maintenanceState, *workspacePending, context.Context, *os.Root) {
	t.Helper()
	noCall := mode == "no-call" || mode == "no-call-zero"
	f := newRetirementFixtureFrame(t)
	name := filepath.Join(f.path, "artifacts")
	directory := filepath.Join(f.path, "store")
	localDirectory := filepath.Join(f.path, "local")
	for _, name := range []string{name, directory, localDirectory} {
		if err := os.Mkdir(name, 0700); err != nil {
			f.acquisition = err
			t.Fatal(err)
		}
	}
	var err error
	f.root, err = os.OpenRoot(name)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	root := f.root
	info, err := root.Lstat(".")
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		f.acquisition = errors.New("missing actual root identity")
		t.Fatal(f.acquisition)
	}
	identity := artifact.FileID{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	f.files, err = artifact.Open(root, identity)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	files := f.files
	if _, err = store.InstallArtifacts(directory, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	f.store, err = store.Open(directory)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	s := f.store
	f.local, err = local.New(localDirectory)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	b := f.local
	binding, err := backend.LocalBinding(s.Owner())
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	target := backend.Target{Name: "local", Binding: binding, Backend: b, OutputStorage: backend.RemoteOutput}
	var producer local.ServerLifetime
	// Literal identity tests exact forwarding/CAS, NOT self capture or death.
	if mode != "no-call-zero" {
		producer = local.ServerLifetime{Host: strings.Repeat("1", 32), Boot: strings.Repeat("2", 32), NamespaceInode: 9, PID: 42}
	}
	f.control, err = NewWithConfig(s, []backend.Target{target}, testEngine, Config{ServerLifetime: producer, Artifacts: files, LocalBackend: b, LocalWorkspace: &LocalWorkspaceConfig{Target: target.Name, Binding: binding, Backend: b, Roots: root}})
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	c := f.control
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	f.cancel = cancel
	f.state = c.newLoop(ctx)
	state := f.state
	f.maintenance = newMaintenance(ctx)
	maintenance := f.maintenance
	c.run = &runFrame{state: state, maintenance: maintenance, done: make(chan struct{})}
	if !c.WorkspaceTargetEnabled(target.Name, binding) {
		t.Fatal("HOLD actual Local ownership prerequisites unavailable")
	}
	input := actualWorkspaceInput(t, c)
	task := backend.Task{Argv: []string{"must-not-start"}, Workdir: &input}
	if !noCall {
		task.Argv = []string{"/bin/sh", "-c", "i=0; while [ \"$i\" -lt 130 ]; do : > entry-$i; i=$((i+1)); done; printf output > result.txt"}
		if mode == "closed-prefix" {
			task.Outputs = []string{"missing-output"}
		} else {
			task.Outputs = []string{"result.txt"}
		}
	}
	accepted, err := c.SubmitWorkspace(testActor(), "retire-original", task)
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	cancelled := false
	for {
		if err := c.schedule(ctx, state); err != nil {
			t.Fatal(err)
		}
		current, err := s.Get(accepted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if noCall && !cancelled && currentStatus(current).Instance != "" {
			if _, err := c.Cancel(testActor(), accepted.ID); err != nil {
				t.Fatal(err)
			}
			cancelled = true
		}
		if p := state.workers[0].completion; p != nil && p.acked {
			if noCall && (!cancelled || p.origin.RootState != local.WorkspaceRootNeverCreated || p.origin.CandidateState != local.WorkspacePhysicalNoOutputs) {
				t.Fatal("fixture lost actual no-call frontier")
			}
			if maintenance.pending != nil {
				if err := c.receiveMaintenance(ctx, maintenance, waitValue(t, maintenance.results)); err != nil {
					t.Fatal(err)
				}
			}
			return c, state, maintenance, p, ctx, root
		}
		if !noCall && maintenance.pending == nil {
			if err := c.dispatchPhysical(maintenance); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case physical := <-maintenance.results:
			if err := c.receiveMaintenance(ctx, maintenance, physical); err != nil {
				t.Fatal(err)
			}
		case result := <-state.results:
			if err := c.receiveOperation(ctx, state, result); err != nil {
				t.Fatal(err)
			}
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("HOLD original no-call completion did not settle")
		}
	}
}

func TestWorkspaceRetirementActualNoCallAndAmbiguousFinish(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "lost-finish-response"}[lost], func(t *testing.T) {
			c, state, m, p, ctx := retirementFixture(t)
			work := *p.cleanup
			admitted, err := c.dispatchWorkspaceRetirement(m, time.Now())
			if err != nil || !admitted {
				t.Fatal("original retirement not selected", admitted, err)
			}
			request := m.pending
			if request == nil || request.workspaceRetirement.original != p || request.workspaceRetirement.work != work || request.workspaceRetirement.step != workspaceRetirementRoot {
				t.Fatal("original concrete request changed")
			}
			result := waitValue(t, m.results)
			if result.retirement == nil || !result.retirement.invoked || !result.retirement.root.Done || result.err != nil {
				t.Fatalf("actual Local root return: %+v %v", result.retirement, result.err)
			}
			if state.workers[0].completion != p {
				t.Fatal("unintegrated physical return released completion")
			}
			if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
				t.Fatal("unintegrated result released barrier")
			}
			if lost {
				// Consumer lost-response model: use the ACTUAL returned Local root receipt,
				// then commit actual Store Finish before its consumer tries the same Finish.
				// No fake root result, fault hook or absence inference supplies physical proof.
				if err := c.store.FinishWorkspaceCleanup(work, store.WorkspaceCleanupReceipt{Work: work}); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.receiveMaintenance(ctx, m, result); err != nil {
				t.Fatal(err)
			}
			if lost {
				if state.workers[0].completion != p || !p.retirement.finishAttempted || !errors.Is(p.retirement.finishError, store.ErrNotFound) || !positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) {
					t.Fatal("ambiguous Finish dropped original positive pair")
				}
				if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
					t.Fatal("ErrNotFound alone released barrier")
				}
				p.retirement.due = time.Time{}
				admitted, err = c.dispatchWorkspaceRetirement(m, time.Now())
				if err != nil || !admitted {
					t.Fatal("original replay confirmation unreachable", admitted, err)
				}
				if m.pending != nil || len(m.input) != 0 {
					t.Fatal("confirmation repeated physical cleanup")
				}
			}
			if state.workers[0].completion != nil {
				t.Fatal("positive retirement retained target capacity")
			}
			if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; ok {
				t.Fatal("positive retirement retained exact barrier")
			}
			if p.retirement.root == nil || p.retirement.root.request != request || p.retirement.root.result.request.workspaceRetirement != request.workspaceRetirement {
				t.Fatal("actual received pair was copied or replaced")
			}
			replay, err := c.store.CompleteWorkspaceAttempt(p.job.ID, reference(p.job), p.status, p.completion, time.Now().UTC())
			if err != nil || replay.State != store.WorkspaceCommitRemoved {
				t.Fatal("actual durable RootRemoved missing", replay.State, err)
			}
			if _, found, err := c.store.NextArtifactCleanup(""); err != nil || found {
				t.Fatal("retired intent still selected", found, err)
			}
			if err := c.advanceWorkspaces(ctx, state); err != nil {
				t.Fatal(err)
			}
			if state.workers[0].pending != nil || len(state.workers[0].input) != 0 {
				t.Fatal("retired original reran execution or collection")
			}
		})
	}
}

func TestWorkspaceRetirementCanceledReceiveKeepsActualOriginalPair(t *testing.T) {
	c, state, m, p, _ := retirementFixture(t)
	if admitted, err := c.dispatchWorkspaceRetirement(m, time.Now()); err != nil || !admitted {
		t.Fatal(admitted, err)
	}
	request := m.pending
	result := waitValue(t, m.results)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.receiveMaintenance(canceled, m, result); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.pending != request || m.received == nil || m.received.request != request || m.received.result.request.workspaceRetirement != request.workspaceRetirement || p.retirement.root != nil || state.workers[0].completion != p {
		t.Fatal("canceled receive lost original custody or credited root")
	}
	// Explicitly integrate the SAME retained original pair to restore fixture
	// ownership. This tests the adapter, not a new production late-integrator API.
	if err := c.integrateMaintenance(m, m.received.result); err != nil {
		t.Fatal(err)
	}
	m.received = nil
	if state.workers[0].completion != nil {
		t.Fatal("actual retained pair did not finish")
	}
}

func TestWorkspaceRetirementStopAndCommonFourRefuseBeforeCall(t *testing.T) {
	c, _, m, p, _ := retirementFixture(t)
	calls := make([]*ArtifactCall, 4)
	for i := range calls {
		var err error
		calls[i], err = c.TryArtifactCall()
		if err != nil {
			t.Fatal(err)
		}
	}
	admitted, err := c.dispatchWorkspaceRetirement(m, time.Now())
	if err != nil || admitted || m.pending != nil || len(m.input) != 0 || !errors.Is(p.err, ErrArtifactBusy) {
		t.Fatal("retirement bypassed actual common4", admitted, err, p.err)
	}
	for _, call := range calls {
		call.Done()
	}
	// Test-only handle handoff has its actual common4 owner while ordinary
	// admission is still OPEN. This is not the unallocated server/Run handshake.
	before, err := c.store.ArtifactReservations()
	if err != nil {
		t.Fatal(err)
	}
	intent, err := workspaceLocalIntent(*p.cleanup)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := c.tryArtifactIO()
	if err != nil {
		t.Fatal(err)
	}
	err = c.workspace.backend.HandoffWorkspaceCleanup(context.Background(), intent)
	owner.finish(err)
	if err != nil {
		t.Fatal(err)
	}
	c.StopArtifactAdmission()
	p.retirement.due = time.Time{}
	admitted, err = c.dispatchWorkspaceRetirement(m, time.Now())
	if err != nil || admitted || m.pending != nil || len(m.input) != 0 {
		t.Fatal("new normal retirement admitted after Stop", admitted, err)
	}
	after, err := c.store.ArtifactReservations()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("handle-only handoff or stopped refusal credited durable charge", err)
	}
	if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
		t.Fatal("handle-only handoff fabricated barrier credit")
	}
	if p.retirement.root != nil || p.retirement.candidate != nil {
		t.Fatal("handle-only handoff manufactured physical removal pair")
	}

}

// UNRUN native qualification sources: real /bin/sh process, Local extraction,
// 130 private entries, real failed Collect prefix or real published result.
// These require the original Linux Local native/descriptor prerequisites; a
// failure is HOLD, never a skip or a manufactured closure receipt.
func TestWorkspaceRetirementActualTreeAndCandidateDispositions(t *testing.T) {
	for _, mode := range []string{"closed-prefix", "published"} {
		t.Run(mode, func(t *testing.T) {
			c, state, m, p, ctx, root := retirementTaskFixture(t, mode)
			work := *p.cleanup
			if work.Origin.RootState != store.WorkspaceRootOwned {
				t.Fatal("native fixture did not produce actual owned root")
			}
			if mode == "closed-prefix" && !candidateNeedsRemoval(p) {
				t.Fatal("failed Collect did not retain actual closed prefix")
			}
			if mode == "published" && (work.Origin.Disposition != store.WorkspaceResultPublishedIndependent || !candidateSettled(p)) {
				t.Fatal("published result lost its actual independent seal")
			}
			rounds, candidateCalls := 0, 0
			for state.workers[0].completion != nil {
				unavailable := p.retirement.root != nil && mode == "closed-prefix"
				if unavailable {
					// Root is physically gone while its Store group is still charged. The
					// real census cannot promote this interval; it is NOT a growth gate for
					// authorized decreasing cleanup. No fake cache/cohort facts are installed.
					if err := c.dispatchPhysical(m); err != nil {
						t.Fatal(err)
					}
					if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
						t.Fatal(err)
					}
					if err := c.cachedLinkGrowthCheck(); err == nil {
						t.Fatal("missing charged root unexpectedly admitted growth")
					}
					if state.workers[0].completion != p {
						t.Fatal("cold sample released candidate obligation")
					}
					if _, ok := c.retention.pending[p.job.CurrentAttemptID()]; !ok {
						t.Fatal("root-only Done released candidate barrier")
					}
				}
				p.retirement.due = time.Time{}
				admitted, err := c.dispatchWorkspaceRetirement(m, time.Now())
				if err != nil || !admitted || m.pending == nil {
					t.Fatal("original finite quantum missing", admitted, err)
				}
				request := m.pending
				if request.workspaceRetirement.step == workspaceRetirementRoot {
					if p.retirement.root != nil {
						t.Fatal("Local root called again after actual Done")
					}
					rounds++
				} else {
					if !positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) {
						t.Fatal("candidate consumed root absence instead of original Done")
					}
					candidateCalls++
				}
				result := waitValue(t, m.results)
				if result.err != nil || result.retirement == nil || !result.retirement.invoked {
					t.Fatalf("actual cleanup quantum failed: %+v %v", result.retirement, result.err)
				}
				if request.workspaceRetirement.step == workspaceRetirementRoot && (result.retirement.root.Visited > 100 || rounds == 1 && result.retirement.root.Done) {
					t.Fatal("actual 130-entry tree bypassed bounded Local quantum")
				}
				if err := c.receiveMaintenance(ctx, m, result); err != nil {
					t.Fatal(err)
				}
				if rounds+candidateCalls > 10 {
					t.Fatal("finite fixture did not settle")
				}
			}
			if rounds < 2 {
				t.Fatal("large root did not yield")
			}
			if mode == "closed-prefix" && candidateCalls != 1 || mode == "published" && candidateCalls != 0 {
				t.Fatal("candidate disposition conflated with root Done", candidateCalls)
			}
			if _, err := root.Lstat("workspace-" + p.job.CurrentAttemptID()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("actual private root remains", err)
			}
			id := work.Origin.Subject.ResultID
			if mode == "closed-prefix" {
				for _, suffix := range []string{".part", ".tar"} {
					if _, err := root.Lstat(id + suffix); !errors.Is(err, os.ErrNotExist) {
						t.Fatal("actual unpublished name remains", suffix, err)
					}
				}
			} else {
				ref, err := artifactPortableRef(work.Origin.Subject.ResultRef)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := c.artifacts.files.OpenObject(ref, artifact.FileID{Device: work.Origin.Subject.ResultFile.Device, Inode: work.Origin.Subject.ResultFile.Inode})
				if err != nil {
					t.Fatal("independent published result was removed", err)
				}
				if err := reader.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestWorkspaceRetirementUnavailableProducerNeedsActualLiveFrame(t *testing.T) {
	c, state, m, p, ctx, _ := retirementTaskFixture(t, "no-call-zero")
	if c.producer != (store.OriginalServerLifetime{}) || p.cleanup.Origin.Producer != c.producer {
		t.Fatal("fixture did not preserve actual zero Producer")
	}
	state.workers[0].completion = nil
	if admitted, err := c.dispatchWorkspaceRetirement(m, time.Now()); err != nil || admitted || m.pending != nil || len(m.input) != 0 {
		t.Fatal("durable intent adopted without original live frame", admitted, err)
	}
	state.workers[0].completion = p
	if admitted, err := c.dispatchWorkspaceRetirement(m, time.Now()); err != nil || !admitted {
		t.Fatal("actual live zero Producer was treated as restart admission", admitted, err)
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
	if state.workers[0].completion != nil {
		t.Fatal("actual live zero Producer did not retire")
	}
}

func TestWorkspaceRetirementUsesNewOrdinaryClass(t *testing.T) {
	c, state, m, p, ctx := retirementFixture(t)
	c.retention.wantSample.Store(true)
	c.physicalChanged()
	m.nextClass = maintenanceWorkspaceClass
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.workspaceRetirement == nil || m.pending.workspaceRetirement.original != p || m.nextClass != maintenanceArtifactExpiryClass {
		t.Fatal("workspace reused a sample class or skipped its actual request")
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
	if state.workers[0].completion != nil {
		t.Fatal("workspace class did not integrate actual retirement")
	}
	m.due = time.Time{}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.physicalOwner != c || m.nextClass != maintenanceLegacySampleClass {
		t.Fatal("actual physical class did not follow workspace")
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
	m.due = time.Time{}
	if err := c.maintain(ctx, m); err != nil {
		t.Fatal(err)
	}
	if m.pending == nil || m.pending.workspaceRetirement != nil || m.pending.physicalOwner != nil || m.pending.record != nil || m.nextClass != maintenanceRetainedCleanupClass {
		t.Fatal("actual legacy class did not follow physical")
	}
	if err := c.receiveMaintenance(ctx, m, waitValue(t, m.results)); err != nil {
		t.Fatal(err)
	}
}

// UNRUN fixture-policy boundary, not a native fault injection: an actual failed
// OpenRoot constructor follows an acquired original root. The early registered
// frame can positively close its sole acquired owner and only then remove path.
func TestRetirementFixtureEarlyAcquisitionErrorUnwindsOriginal(t *testing.T) {
	f := newRetirementFixtureFrame(t)
	var err error
	f.root, err = os.OpenRoot(f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.probe, err = os.OpenRoot(filepath.Join(f.path, "not-created"))
	if err == nil || f.probe != nil {
		t.Fatal("fixture did not reach real early constructor error")
	}
	f.acquisition = err
	original := f.root
	if err := f.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.root != original || !f.rootClosed || !f.removed || f.acquisition == nil {
		t.Fatal("early acquisition lost original custody or skipped positive close")
	}
	if _, err := os.Lstat(f.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("positively retired fixture namespace remains", err)
	}
	// Registered cleanup must observe flags, never raw-retry original Close.
	if err := f.close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// UNRUN fixture-policy boundary using the ACTUAL original maintenance worker
// blocked in its explicit test usage callback. It tests join-before-close and
// namespace retention, not a kernel stall, uncertain Close or native receipt.
// Its retained original root/path is intentionally reported and kept, even
// after the test releases and positively joins that worker. No raw Close retry
// or process-exit cleanup is asserted as application ownership proof.
func TestRetirementFixtureHeldOriginalJoinRetainsCloseBoundary(t *testing.T) {
	f := newRetirementFixtureFrame(t)
	f.expectedHold = true
	var err error
	f.root, err = os.OpenRoot(f.path)
	if err != nil {
		t.Fatal(err)
	}
	original := f.root
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.maintenance = newMaintenance(ctx)
	entered, release := make(chan struct{}), make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	f.maintenance.input <- maintenanceRequest{sequence: 1, directory: f.path, usage: func(context.Context) (uint64, uint64, error) { close(entered); <-release; return 0, 0, nil }}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("HOLD actual maintenance worker did not enter test usage callback")
	}
	stopped, stop := context.WithCancel(context.Background())
	stop()
	held := f.close(stopped)
	if !errors.Is(held, context.Canceled) || f.held != held || f.root != original || f.rootClosed || f.filesClosed || f.storeClosed || f.removed {
		t.Fatal("unknown original join advanced resource close/removal", held)
	}
	if _, err := original.Lstat("."); err != nil {
		t.Fatal("held original root was closed", err)
	}
	if _, err := os.Lstat(f.path); err != nil {
		t.Fatal("held namespace was deleted", err)
	}
	close(release)
	released = true
	waitValue(t, f.maintenance.done)
	if err := f.close(context.Background()); err != held || f.root != original || f.rootClosed || f.removed {
		t.Fatal("held error/pointer changed or original Close was retried", err)
	}
}

// UNRUN acquisition-classification policy oracle. The joined sentinel vector is
// CONTROLLED test input, not an actual FileCloseError, owned unknown descriptor,
// injected native Close failure or stopped reconciliation authority. The real
// borrowed root and Store remain retained solely to test this fixture's guard.
// In contrast, the early OpenRoot failure oracle has an ordinary error and must
// positively unwind. No private Files owner/error is fabricated by this test.
func TestRetirementFixtureAcquisitionUncertaintyRetainsExactCause(t *testing.T) {
	f := newRetirementFixtureFrame(t)
	f.expectedHold = true
	var err error
	f.root, err = os.OpenRoot(f.path)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	directory := filepath.Join(f.path, "store")
	if err := os.Mkdir(directory, 0700); err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	f.store, err = store.Open(directory)
	if err != nil {
		f.acquisition = err
		t.Fatal(err)
	}
	root, originalStore := f.root, f.store
	cause := errors.Join(errors.New("fixture acquisition diagnostic"), fmt.Errorf("controlled acquisition classification: %w", artifact.ErrFileCloseUncertain))
	f.acquisition = cause
	if f.files != nil {
		t.Fatal("oracle must model constructor uncertainty before Files pointer publication")
	}
	for i := 0; i < 2; i++ {
		if err := f.close(context.Background()); err != cause || f.held != cause || f.acquisition != cause {
			t.Fatal("acquisition cause was flattened, replaced or discarded", err)
		}
		if f.root != root || f.store != originalStore || f.files != nil || f.inputsClosed || f.localClosed || f.artifactsDrained || f.filesClosed || f.rootClosed || f.storeClosed || f.removed {
			t.Fatal("owner-retaining acquisition advanced downstream teardown")
		}
		if _, err := root.Lstat("."); err != nil {
			t.Fatal("acquisition uncertainty closed borrowed root", err)
		}
		if _, err := os.Lstat(f.path); err != nil {
			t.Fatal("acquisition uncertainty deleted namespace", err)
		}
	}
	// Registered cleanup retains this exact frame/cause/path in the test evidence
	// list. Expected HOLD is logged, never passed off as positive resource closure.
}
