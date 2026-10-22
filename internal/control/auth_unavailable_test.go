//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// These tests use the fixture's actual file Engine. A stale snapshot is made
// only by joining its original refresh loop and waiting for the real 2s
// freshness bound to lapse; a fresh snapshot only by an actual refresh.
// Native values are MODELED; no native, helper or cluster fact is implied.

// staleAuthority joins the fixture's refresh loop and waits until the actual
// snapshot is no longer fresh. It fails unless d then reports ErrUnavailable.
func staleAuthority(t *testing.T, f *dataControlFixture, d auth.Delegation) {
	t.Helper()
	f.stopAuth()
	select {
	case <-f.authDone:
	case <-time.After(30 * time.Second):
		f.hold(errors.New("original auth refresh did not join"))
		t.Fatal("auth refresh remains held")
	}
	start := time.Now()
	for f.engine.Ready() {
		if time.Since(start) > 30*time.Second {
			t.Fatal("snapshot stayed fresh without refresh")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := f.engine.CheckDelegation(d); !errors.Is(err, auth.ErrUnavailable) {
		t.Fatalf("stale snapshot delegation check: %v", err)
	}
	t.Logf("measured stale snapshot %v after joining refresh", time.Since(start))
}

// refreshAuthority restarts the original refresh loop and waits for an actual
// fresh read that accepts d.
func refreshAuthority(t *testing.T, f *dataControlFixture, d auth.Delegation) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	f.stopAuth, f.authDone = stop, done
	go func() { defer close(done); f.engine.Run(ctx) }()
	start := time.Now()
	for f.engine.CheckDelegation(d) != nil {
		if time.Since(start) > 30*time.Second {
			t.Fatal("auth refresh did not restore fresh delegation")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// revokeAuthority revokes the fixture credential and waits for the actual
// refresh to observe it. The refresh loop must be running.
func revokeAuthority(t *testing.T, f *dataControlFixture, d auth.Delegation) {
	t.Helper()
	f.prepareRevocation(t)
	changed := f.engine.Changed()
	if err := auth.Revoke(f.configPath, f.actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(30 * time.Second):
		f.hold(errors.New("original auth refresh did not observe revocation"))
		t.Fatal("auth refresh remains held")
	}
	if err := f.engine.CheckDelegation(d); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("revoked delegation check: %v", err)
	}
}

func remoteRow(t *testing.T, f *dataControlFixture, job backend.Job) store.RemoteAttempt {
	t.Helper()
	row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()})
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func currentJob(t *testing.T, f *dataControlFixture, id string) backend.Job {
	t.Helper()
	job, err := f.store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestAuthUnavailableQueuedScheduleNeitherStartsNorCancels(t *testing.T) {
	f := newDataControlFixture(t)
	held, err := f.submit("held-queued", submissionDataTask())
	if err != nil {
		t.Fatal(err)
	}
	lost, err := f.submit("lost-queued", submissionDataTask())
	if err != nil {
		t.Fatal(err)
	}
	row := remoteRow(t, f, held)
	loop := &loopState{workers: []targetWorker{{target: f.target(), input: make(chan operation, 1)}}, due: make(map[string]time.Time)}
	w := &loop.workers[0]
	staleAuthority(t, f, row.Authority)
	before := stateDigest(t, f)
	if err = f.control.schedule(context.Background(), loop); err != nil {
		t.Fatalf("unavailable authority stopped the scheduler: %v", err)
	}
	if !reflect.DeepEqual(stateDigest(t, f), before) || w.pending != nil || len(w.input) != 0 || f.backend.checks.Load() != 2 || f.backend.native.Load() != 0 {
		t.Fatal("unavailable authority canceled, began or dispatched queued work")
	}
	refreshAuthority(t, f, row.Authority)
	if err = f.control.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	if w.pending == nil || w.pending.kind != dataStartOperation || w.pending.data.key.JobID != held.ID || len(w.input) != 1 {
		t.Fatal("fresh authority did not start the SAME held queued job")
	}
	if currentJob(t, f, held.ID).CancelRequested || currentJob(t, f, lost.ID).CancelRequested {
		t.Fatal("held queued work was canceled")
	}
	revokeAuthority(t, f, row.Authority)
	if err = f.control.schedule(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
	if !currentJob(t, f, lost.ID).CancelRequested {
		t.Fatal("actual authority loss did not cancel queued work")
	}
}

func TestAuthUnavailableDispatchPreStartHoldsSameAttempt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		checks int32 // Backend.Check calls after submission, before the refusing check
		final  bool
	}{
		{"initial", 0, false},
		{"final", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDataControlFixture(t)
			job, err := f.submit("prestart-"+tc.name, submissionDataTask())
			if err != nil {
				t.Fatal(err)
			}
			row := remoteRow(t, f, job)
			checks := f.backend.checks.Load() + tc.checks
			if tc.final {
				// Check is the actual barrier between the two delegation checks.
				f.backend.check = func(task backend.Task) error {
					staleAuthority(t, f, row.Authority)
					return backend.ValidateDataTask(task, nil)
				}
			} else {
				staleAuthority(t, f, row.Authority)
			}
			before := stateDigest(t, f)
			w := &targetWorker{target: f.target(), input: make(chan operation, 1)}
			dispatched, err := f.control.dispatchData(context.Background(), &loopState{}, w, job, row, time.Now().UTC())
			if err != nil || dispatched || w.pending != nil || len(w.input) != 0 {
				t.Fatalf("unavailable authority dispatched: %v %v", dispatched, err)
			}
			if !reflect.DeepEqual(stateDigest(t, f), before) || f.backend.checks.Load() != checks || f.backend.native.Load() != 0 {
				t.Fatal("unavailable authority canceled, began or reached placement")
			}
			f.backend.check = nil
			refreshAuthority(t, f, row.Authority)
			if !reflect.DeepEqual(currentJob(t, f, job.ID), job) || !reflect.DeepEqual(remoteRow(t, f, job), row) {
				t.Fatal("held queued identity changed")
			}
			dispatched, err = f.control.dispatchData(context.Background(), &loopState{}, w, job, row, time.Now().UTC())
			if err != nil || !dispatched || w.pending == nil || w.pending.kind != dataStartOperation || w.pending.data.key.JobID != job.ID || w.pending.data.row.Authority != row.Authority {
				t.Fatalf("fresh authority did not start the SAME queued job: %v %v", dispatched, err)
			}
			if currentJob(t, f, job.ID).CancelRequested {
				t.Fatal("started job was canceled")
			}
		})
	}
	for _, tc := range []struct {
		name  string
		final bool
	}{
		{"lost-initial", false},
		{"lost-final", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDataControlFixture(t)
			job, err := f.submit(tc.name, submissionDataTask())
			if err != nil {
				t.Fatal(err)
			}
			row := remoteRow(t, f, job)
			if tc.final {
				f.backend.check = func(task backend.Task) error {
					revokeAuthority(t, f, row.Authority)
					return backend.ValidateDataTask(task, nil)
				}
			} else {
				revokeAuthority(t, f, row.Authority)
			}
			w := &targetWorker{target: f.target(), input: make(chan operation, 1)}
			dispatched, err := f.control.dispatchData(context.Background(), &loopState{}, w, job, row, time.Now().UTC())
			if err != nil || !dispatched || w.pending != nil || !currentJob(t, f, job.ID).CancelRequested {
				t.Fatalf("actual authority loss did not cancel queued work: %v %v", dispatched, err)
			}
		})
	}
}

func TestAuthUnavailableActiveAttemptClosesOnlyOnActualFacts(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, _ := remoteTransferAttempt(t, f, "active-held", ref, true)
	lostKey, _ := remoteTransferAttempt(t, f, "active-lost", ref, true)
	job := currentJob(t, f, key.JobID)
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	staleAuthority(t, f, row.Authority)
	before := stateDigest(t, f)
	w := &targetWorker{target: f.target(), input: make(chan operation, 1)}
	dispatched, err := f.control.dispatchData(context.Background(), &loopState{}, w, job, row, time.Now().UTC())
	if err != nil || dispatched || w.pending != nil || len(w.input) != 0 || !reflect.DeepEqual(stateDigest(t, f), before) {
		t.Fatalf("unavailable authority revoked or observed an open attempt: %v %v", dispatched, err)
	}
	refreshAuthority(t, f, row.Authority)
	dispatched, err = f.control.dispatchData(context.Background(), &loopState{}, w, job, row, time.Now().UTC())
	if err != nil || !dispatched || w.pending == nil || w.pending.kind != dataObserveOperation || w.pending.data.key != key {
		t.Fatalf("fresh authority did not observe the SAME attempt: %v %v", dispatched, err)
	}
	if after, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(after, row) {
		t.Fatal("observation dispatch changed the attempt")
	}

	// Actual stage expiry still closes while authority is unknown, and the
	// closed attempt is still observed for cleanup.
	staleAuthority(t, f, row.Authority)
	w = &targetWorker{target: f.target(), input: make(chan operation, 1)}
	dispatched, err = f.control.dispatchData(context.Background(), &loopState{}, w, job, row, row.Active.StageDeadline)
	if err != nil || !dispatched || w.pending != nil {
		t.Fatalf("stage expiry under unavailable authority: %v %v", dispatched, err)
	}
	closed, err := f.store.GetRemoteAttempt(key)
	if err != nil || closed.Active.ClosedReason != store.RemotePhaseTimeout {
		t.Fatalf("actual stage expiry did not close: %+v %v", closed.Active, err)
	}
	dispatched, err = f.control.dispatchData(context.Background(), &loopState{}, w, job, closed, time.Now().UTC())
	if err != nil || !dispatched || w.pending == nil || w.pending.kind != dataObserveOperation {
		t.Fatalf("closed attempt cleanup observation refused: %v %v", dispatched, err)
	}

	refreshAuthority(t, f, row.Authority)
	revokeAuthority(t, f, row.Authority)
	lostRow, err := f.store.GetRemoteAttempt(lostKey)
	if err != nil {
		t.Fatal(err)
	}
	w = &targetWorker{target: f.target(), input: make(chan operation, 1)}
	dispatched, err = f.control.dispatchData(context.Background(), &loopState{}, w, currentJob(t, f, lostKey.JobID), lostRow, time.Now().UTC())
	if err != nil || !dispatched || w.pending != nil {
		t.Fatalf("lost authority: %v %v", dispatched, err)
	}
	if got, err := f.store.GetRemoteAttempt(lostKey); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost {
		t.Fatalf("actual authority loss left the attempt open: %v", err)
	}
}

// A Retrying job whose current attempt awaits removal (State3) needs no
// authority for that cleanup. Its prompt scheduler removal is not gated.
func TestAuthUnavailableRetryCleanupStillRemoves(t *testing.T) {
	f := newDataControlFixture(t)
	input, archive := f.input(t)
	task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &input, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{75}}}
	job, err := f.control.SubmitData(f.actor, "retry-cleanup", task)
	if err != nil {
		t.Fatal(err)
	}
	// A second queued remote row keeps the bounded historical selector away
	// from the State3 row, so only the scheduler arm can claim it this turn.
	if _, err = f.submit("retry-bystander", submissionDataTask()); err != nil {
		t.Fatal(err)
	}
	key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
	now := time.Now().UTC()
	secret := sha256.Sum256([]byte(key.AttemptID))
	start, began, err := f.store.BeginRemoteAttempt(job.ID, key.AttemptID, store.RemoteHashes{Input: remoteTokenHash(store.RemoteInput, secret[:])}, backend.DefaultDataPolicy(), now)
	if err != nil || !began {
		t.Fatalf("original Begin: %v %v", began, err)
	}
	created := backend.DataCreated{Instance: "modeled-pod", Pod: backend.DataResource{Name: "pod", UID: "modeled-pod"}, InputSecret: backend.DataResource{Name: "input", UID: "modeled-input"}}
	row, err := f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, now)
	if err != nil {
		t.Fatal(err)
	}
	// The selected exit retries only after the complete original input: the
	// actual full ReadInput with the original scope token, its original Close,
	// and Store FinishRemoteInput.
	token, err := remoteToken(key, secret[:])
	clear(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	finish := fullRemoteInput(t, f, string(token), input, archive)
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
	if admitted, err := f.control.dispatchRemoteInputFinish(m, time.Now()); err != nil || !admitted {
		t.Fatalf("original input close not dispatched: %v %v", admitted, err)
	}
	request := <-m.input
	result := maintenanceResult{request: request}
	result.downloadClose, result.err = runArtifactDownloadClose(context.Background(), request)
	if err = f.control.receiveMaintenance(context.Background(), m, result); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	if !waitLocked(t, f, 10*time.Second, func() bool { return finish.download.finished }) {
		t.Fatal("original input close did not finish")
	}
	if row, err = f.store.GetRemoteAttempt(key); err != nil || row.Active == nil || row.Active.Input == nil || row.Active.Input.State != 4 || row.Active.ClosedReason != 0 {
		t.Fatalf("actual full read did not reach input State4: %v", err)
	}
	finished := time.Now().UTC()
	code := int32(75)
	stop := &backend.DataTaskStop{Stage: &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: row.Active.Began, FinishedAt: row.Active.Began}, Task: backend.ContainerExit{ContainerID: "modeled-main", ExitCode: code, StartedAt: row.Active.Began, FinishedAt: finished}}
	if _, err = f.store.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: created.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop}, finished); err != nil {
		t.Fatal(err)
	}
	if job, err = f.store.CompleteRemoteAttempt(key, store.RemoteCompletion{Outcome: store.DataMainExit, Stop: stop}, finished); err != nil {
		t.Fatal(err)
	}
	row, err = f.store.GetRemoteAttempt(key)
	if err != nil || job.Status.State != backend.Retrying || job.CurrentAttemptID() != key.AttemptID || row.State != 3 {
		t.Fatalf("fixture did not reach Retrying with State3 removal: %v %v", job.Status.State, err)
	}
	loop := &loopState{workers: []targetWorker{{target: f.target(), input: make(chan operation, 1)}}, due: make(map[string]time.Time), dataAfter: key.AttemptID}
	w := &loop.workers[0]
	staleAuthority(t, f, row.Authority)
	before := stateDigest(t, f)
	if err = f.control.schedule(context.Background(), loop); err != nil {
		t.Fatalf("unavailable authority stopped the scheduler: %v", err)
	}
	if p := w.dataCompletion; p == nil || p.key != key || p.step != dataRemove {
		t.Fatal("State3 cleanup was gated on unavailable authority")
	}
	if w.pending != nil || len(w.input) != 0 || currentJob(t, f, job.ID).CancelRequested || !reflect.DeepEqual(stateDigest(t, f), before) || f.backend.native.Load() != 0 {
		t.Fatal("cleanup selection started, canceled or mutated work")
	}
}

// heldFrame drives one original data operation through the actual worker
// function and receiveOperation, leaving its result in w.dataCompletion.
func heldFrame(t *testing.T, f *dataControlFixture, loop *loopState) *remotePending {
	t.Helper()
	w := &loop.workers[0]
	op := <-w.input
	if err := f.control.receiveOperation(context.Background(), loop, runDataOperation(context.Background(), op)); err != nil {
		t.Fatal(err)
	}
	p := w.dataCompletion
	if p == nil || p.step != dataRecord || p.original == nil || p.original.request.data != op.data {
		t.Fatal("original result was not retained for integration")
	}
	return p
}

func TestAuthUnavailableHoldsOriginalStartFrame(t *testing.T) {
	for _, name := range []string{"refresh", "expiry", "canceled", "lost"} {
		t.Run(name, func(t *testing.T) {
			f := newDataControlFixture(t)
			input, _ := f.input(t)
			var starts atomic.Int32
			var job backend.Job
			var authority auth.Delegation
			created := backend.DataCreated{Instance: "modeled-pod", Pod: backend.DataResource{Name: "original-pod", UID: "modeled-pod"}, InputSecret: backend.DataResource{Name: "original-input", UID: "modeled-input"}, OutputSecret: backend.DataResource{Name: "original-output", UID: "modeled-output"}}
			f.backend.startData = func(context.Context, backend.DataStart) (backend.DataCreated, error) {
				// The original call returns only after the actual authority change.
				starts.Add(1)
				switch name {
				case "canceled":
					if _, err := f.control.Cancel(f.actor, job.ID); err != nil {
						t.Error(err)
					}
					staleAuthority(t, f, authority)
				case "lost":
					revokeAuthority(t, f, authority)
				default:
					staleAuthority(t, f, authority)
				}
				return created, nil
			}
			var err error
			job, err = f.control.SubmitData(f.actor, "held-start-"+name, backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &input, Outputs: []string{"result.txt"}})
			if err != nil {
				t.Fatal(err)
			}
			authority = remoteRow(t, f, job).Authority
			loop := &loopState{workers: []targetWorker{{target: f.target(), input: make(chan operation, 1)}}, due: make(map[string]time.Time)}
			w := &loop.workers[0]
			if dispatched, err := f.control.dispatchData(context.Background(), loop, w, job, remoteRow(t, f, job), time.Now().UTC()); err != nil || !dispatched {
				t.Fatalf("original Start not dispatched: %v %v", dispatched, err)
			}
			p := heldFrame(t, f, loop)
			original := p.original
			request := original.request.data
			key := p.key
			begun, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if name == "lost" {
				if err = f.control.advanceData(context.Background(), loop); err != nil {
					t.Fatal(err)
				}
				if got, err := f.store.GetRemoteAttempt(key); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost || got.Creation != 0 {
					t.Fatalf("actual authority loss did not close before creation: %v", err)
				}
				return
			}
			if name == "canceled" {
				if err = f.control.advanceData(context.Background(), loop); err != nil {
					t.Fatal(err)
				}
				if got, err := f.store.GetRemoteAttempt(key); err != nil || got.Active.ClosedReason != store.RemoteCanceled || got.Creation != 0 {
					t.Fatalf("actual cancellation did not close under unavailable authority: %v", err)
				}
				return
			}
			before := stateDigest(t, f)
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatalf("unavailable authority stopped Control: %v", err)
			}
			if w.dataCompletion != p || p.original != original || p.step != dataRecord || p.held || !errors.Is(p.err, auth.ErrUnavailable) {
				t.Fatal("original Start frame was not held for reconciliation")
			}
			if !reflect.DeepEqual(stateDigest(t, f), before) || len(request.start.InputToken) == 0 || bytes.Equal(request.start.InputToken, make([]byte, len(request.start.InputToken))) {
				t.Fatal("unavailable authority admitted, closed or consumed the original Start")
			}
			if name == "expiry" {
				// The SAME frame is closed by its actual stage deadline and then
				// acknowledged cleanup-only, without fresh authority.
				if err = f.control.advanceDataPending(context.Background(), w, p, begun.Active.StageDeadline); err != nil {
					t.Fatal(err)
				}
				got, err := f.store.GetRemoteAttempt(key)
				if err != nil || got.Active.ClosedReason != store.RemotePhaseTimeout || got.Creation != 0 || w.dataCompletion != p {
					t.Fatalf("stage expiry under unavailable authority: %+v %v", got, err)
				}
				if err = f.control.advanceDataPending(context.Background(), w, p, begun.Active.StageDeadline); err != nil {
					t.Fatal(err)
				}
				got, err = f.store.GetRemoteAttempt(key)
				if err != nil || got.Active.ClosedReason == 0 || got.Creation != 1 || got.Resources != created || got.Active.Input.State != 3 || w.dataCompletion != nil || request.start.InputToken != nil {
					t.Fatalf("cleanup-only Start acknowledgement under unavailable authority: %+v %v", got, err)
				}
				return
			}
			refreshAuthority(t, f, authority)
			time.Sleep(110 * time.Millisecond) // Exceed the frame's actual retry interval.
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			got, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if got.Creation != 3 || got.Active.ClosedReason != 0 || got.Resources != created || got.Active.Input.State != 2 || got.Active.Output.State != 2 {
				t.Fatalf("fresh authority did not integrate the SAME Start: %+v", got)
			}
			if got.Active.Input.Hash != begun.Active.Input.Hash || got.Active.Output.Hash != begun.Active.Output.Hash || !got.Active.Input.Expires.Equal(begun.Active.Input.Expires) || !got.Active.StageDeadline.Equal(begun.Active.StageDeadline) {
				t.Fatal("held Start gained a new grant or clock")
			}
			if w.dataCompletion != nil || starts.Load() != 1 || request.start.InputToken != nil {
				t.Fatal("original Start was repeated or retained")
			}
		})
	}
}

func TestAuthUnavailableHoldsOriginalObserveFrame(t *testing.T) {
	for _, name := range []string{"refresh", "lost"} {
		t.Run(name, func(t *testing.T) {
			f := newDataControlFixture(t)
			ref, _ := f.input(t)
			key, _ := remoteTransferAttempt(t, f, "held-observe-"+name, ref, true)
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			var observes atomic.Int32
			started := time.Now().UTC()
			f.backend.observeData = func(_ context.Context, got backend.Reference) (backend.DataObservation, error) {
				observes.Add(1)
				if name == "lost" {
					revokeAuthority(t, f, row.Authority)
				} else {
					staleAuthority(t, f, row.Authority)
				}
				return backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: got.Instance}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: started}}, nil
			}
			loop := &loopState{workers: []targetWorker{{target: f.target(), input: make(chan operation, 1)}}, due: make(map[string]time.Time)}
			w := &loop.workers[0]
			if dispatched, err := f.control.dispatchData(context.Background(), loop, w, currentJob(t, f, key.JobID), row, time.Now().UTC()); err != nil || !dispatched {
				t.Fatalf("observation not dispatched: %v %v", dispatched, err)
			}
			p := heldFrame(t, f, loop)
			original := p.original
			if name == "lost" {
				if err = f.control.advanceData(context.Background(), loop); err != nil {
					t.Fatal(err)
				}
				if got, err := f.store.GetRemoteAttempt(key); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost || got.Active.Running != nil {
					t.Fatalf("actual authority loss did not close before integration: %v", err)
				}
				return
			}
			before := stateDigest(t, f)
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatalf("unavailable authority stopped Control: %v", err)
			}
			if w.dataCompletion != p || p.original != original || p.step != dataRecord || p.held || !errors.Is(p.err, auth.ErrUnavailable) {
				t.Fatal("original observation frame was not held for reconciliation")
			}
			if got, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(got, row) || !reflect.DeepEqual(stateDigest(t, f), before) {
				t.Fatal("unavailable authority integrated, renewed or closed the observation")
			}
			refreshAuthority(t, f, row.Authority)
			time.Sleep(110 * time.Millisecond)
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			got, err := f.store.GetRemoteAttempt(key)
			if err != nil || got.Active.ClosedReason != 0 || got.Active.Running == nil || got.Active.Running.ContainerID != "modeled-main" || !got.Active.Running.StartedAt.Equal(started) {
				t.Fatalf("fresh authority did not integrate the SAME observation: %v", err)
			}
			if w.dataCompletion != nil || observes.Load() != 1 {
				t.Fatal("original observation was repeated or retained")
			}
		})
	}
}

func TestAuthUnavailableHoldsOriginalCompletion(t *testing.T) {
	for _, name := range []string{"refresh", "lost"} {
		t.Run(name, func(t *testing.T) {
			f := newDataControlFixture(t)
			ref, _ := f.input(t)
			key, _ := remoteTransferAttempt(t, f, "held-complete-"+name, ref, false)
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			f.backend.observeData = func(_ context.Context, got backend.Reference) (backend.DataObservation, error) {
				finished := time.Now().UTC()
				code := int32(0)
				stop := &backend.DataTaskStop{Stage: &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: row.Active.Began, FinishedAt: row.Active.Began}, Task: backend.ContainerExit{ContainerID: "modeled-main", StartedAt: row.Active.Began, FinishedAt: finished}}
				return backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: got.Instance, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop}, nil
			}
			loop := &loopState{workers: []targetWorker{{target: f.target(), input: make(chan operation, 1)}}, due: make(map[string]time.Time)}
			w := &loop.workers[0]
			// Transfer settlement fences the completion frame of the current loop,
			// which Run otherwise publishes; no Run goroutine is started.
			f.control.runMu.Lock()
			f.control.run = &runFrame{state: loop}
			f.control.runMu.Unlock()
			if dispatched, err := f.control.dispatchData(context.Background(), loop, w, currentJob(t, f, key.JobID), row, time.Now().UTC()); err != nil || !dispatched {
				t.Fatalf("observation not dispatched: %v %v", dispatched, err)
			}
			p := heldFrame(t, f, loop)
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			if w.dataCompletion != p || p.step != dataComplete {
				t.Fatal("terminal observation did not reach completion")
			}
			if name == "lost" {
				revokeAuthority(t, f, row.Authority)
			} else {
				staleAuthority(t, f, row.Authority)
			}
			time.Sleep(110 * time.Millisecond) // Exceed the frame's actual retry interval.
			before := stateDigest(t, f)
			stopped, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatalf("authority change stopped Control: %v", err)
			}
			if name == "lost" {
				if got, err := f.store.GetRemoteAttempt(key); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost || got.State != 2 {
					t.Fatalf("actual authority loss did not close before completion: %v", err)
				}
				return
			}
			if w.dataCompletion != p || p.step != dataComplete || p.completion != nil || p.held || !errors.Is(p.err, auth.ErrUnavailable) {
				t.Fatal("original completion was not held for reconciliation")
			}
			if got, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(got, stopped) || !reflect.DeepEqual(stateDigest(t, f), before) {
				t.Fatal("unavailable authority completed or closed the attempt")
			}
			refreshAuthority(t, f, row.Authority)
			time.Sleep(110 * time.Millisecond)
			if err = f.control.advanceData(context.Background(), loop); err != nil {
				t.Fatal(err)
			}
			got, err := f.store.GetRemoteAttempt(key)
			if err != nil || got.State != 3 || p.step != dataRemove || p.completion == nil || p.completion.Outcome != store.DataMainExit {
				t.Fatalf("fresh authority did not complete the SAME attempt: %v", err)
			}
		})
	}
}

func TestAuthUnavailableRemoteCallRefusesWithoutClosing(t *testing.T) {
	f := newDataControlFixture(t)
	ref, _ := f.input(t)
	key, input := remoteTransferAttempt(t, f, "remote-held", ref, true)
	lostKey, lostInput := remoteTransferAttempt(t, f, "remote-lost", ref, true)
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	staleAuthority(t, f, row.Authority)
	before := stateDigest(t, f)
	call, err := f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	err = f.control.AuthorizeRemoteCall(context.Background(), call, input, RemoteReadInput)
	s := f.control.artifacts
	s.mu.Lock()
	attached := call.worker != nil
	s.mu.Unlock()
	if !attached {
		call.Done()
	}
	if !errors.Is(err, ErrUnavailable) || attached {
		t.Fatalf("unavailable authority authorized: %v", err)
	}
	for _, ctx := range []context.Context{context.Background(), canceled} {
		if err = f.control.recheckRemote(ctx, key, row.Active.Input.Hash, store.RemoteInput, row.Authority); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("unavailable recheck: %v", err)
		}
	}
	if got, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(got, row) || !reflect.DeepEqual(stateDigest(t, f), before) {
		t.Fatal("unavailable authority closed the remote scope")
	}
	refreshAuthority(t, f, row.Authority)
	if _, _, err = f.control.remoteCredential(context.Background(), key, row.Active.Input.Hash, RemoteReadInput); err != nil {
		t.Fatalf("fresh authority refused the SAME scope: %v", err)
	}
	if err = f.control.recheckRemote(context.Background(), key, row.Active.Input.Hash, store.RemoteInput, row.Authority); err != nil {
		t.Fatalf("fresh recheck: %v", err)
	}
	revokeAuthority(t, f, row.Authority)
	call, err = f.control.TryArtifactCall()
	if err != nil {
		t.Fatal(err)
	}
	if err = f.control.AuthorizeRemoteCall(context.Background(), call, lostInput, RemoteReadInput); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lost authority authorized: %v", err)
	}
	call.Done()
	if got, err := f.store.GetRemoteAttempt(lostKey); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost {
		t.Fatalf("lost authority left the initial-check scope open: %v", err)
	}
	// Lost delegation still closes despite a canceled request.
	if err = f.control.recheckRemote(canceled, key, row.Active.Input.Hash, store.RemoteInput, row.Authority); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lost recheck: %v", err)
	}
	if got, err := f.store.GetRemoteAttempt(key); err != nil || got.Active.ClosedReason != store.RemoteAuthorityLost {
		t.Fatalf("lost authority under cancellation left the scope open: %v", err)
	}
	if f.backend.native.Load() != 0 {
		t.Fatal("remote authorization invoked native work")
	}
}

// remoteInputAttempt is remoteTransferAttempt for an input-only task whose
// actual Store transitions are dated began. It models an attempt accepted
// earlier, so its real 120s input lease lapses at a known wall time.
func remoteInputAttempt(t *testing.T, f *dataControlFixture, id string, ref artifact.Ref, began time.Time, timeout int64) (store.RemoteKey, string) {
	t.Helper()
	authority, err := f.engine.Delegate(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	target := f.target()
	policy := backend.DefaultDataPolicy()
	task := backend.Task{Argv: []string{"reader"}, Image: "modeled-image", Workdir: &ref, TimeoutSeconds: timeout}
	job, err := f.store.SubmitData(f.actor, nil, id, task, target.Name, target.Binding, authority, policy, began)
	if err != nil {
		t.Fatal(err)
	}
	secret := sha256.Sum256([]byte(id))
	start, ok, err := f.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{Input: remoteTokenHash(store.RemoteInput, secret[:])}, policy, began)
	if err != nil || !ok {
		t.Fatalf("original start intent: began=%v err=%v", ok, err)
	}
	key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
	created := backend.DataCreated{Instance: "modeled-pod-" + id, Pod: backend.DataResource{Name: "modeled-pod", UID: "modeled-pod-" + id}, InputSecret: backend.DataResource{Name: "modeled-input", UID: "modeled-input-" + id}}
	if _, err = f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, began); err != nil {
		t.Fatal(err)
	}
	token, err := remoteToken(key, secret[:])
	clear(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	return key, string(token)
}

// fullRemoteInput serves one actual generated full ReadInput and returns its
// armed original finish frame. The HTTP caller is joined before return.
func fullRemoteInput(t *testing.T, f *dataControlFixture, token string, ref artifact.Ref, archive []byte) *remoteInputFinish {
	t.Helper()
	_, generated := skyv1connect.NewRemoteDataServiceHandler(remoteHTTPControlService{f.control}, connect.WithCompressMinBytes(1<<20), connect.WithReadMaxBytes(96<<10), connect.WithSendMaxBytes(96<<10))
	joined := make(chan error, 1)
	var finish *remoteInputFinish
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call, err := f.control.TryArtifactCall()
		if err != nil {
			joined <- err
			return
		}
		if err = f.control.AuthorizeRemoteCall(r.Context(), call, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), RemoteReadInput); err != nil {
			call.Done()
			joined <- err
			return
		}
		err = f.control.ServeRemoteHTTP(call, w, r.Clone(context.WithValue(r.Context(), remoteHTTPTestCallKey{}, call)), generated)
		s := f.control.artifacts
		s.mu.Lock()
		if p := call.worker; p != nil {
			finish = p.input
		}
		s.mu.Unlock()
		if err == nil {
			call.Done()
		}
		joined <- err
	}))
	defer server.Close()
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	request := connect.NewRequest(&skyv1.ReadInputRequest{Limit: 64 << 10})
	request.Header().Set("Authorization", "Bearer "+token)
	response, err := skyv1connect.NewRemoteDataServiceClient(server.Client(), server.URL).ReadInput(ctx, request)
	producerErr := waitValue(t, joined)
	if err != nil || producerErr != nil {
		failure := errors.Join(err, producerErr)
		f.hold(failure)
		t.Fatal(failure)
	}
	if !bytes.Equal(response.Msg.Data, archive) || !response.Msg.Complete || finish == nil || !finish.complete || finish.download.reader == nil {
		f.hold(errors.New("full input lacks its original finish frame"))
		t.Fatal("full response was not armed by the actual producer")
	}
	return finish
}

// waitLocked polls cond under the artifact service lock.
func waitLocked(t *testing.T, f *dataControlFixture, limit time.Duration, cond func() bool) bool {
	t.Helper()
	s := f.control.artifacts
	for start := time.Now(); time.Since(start) < limit; time.Sleep(20 * time.Millisecond) {
		s.mu.Lock()
		ok := cond()
		s.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

// The SAME invoked input close frame is held, not fatal, while authority is
// unavailable, and only its Store Finish is retried by the actual Run.
func TestAuthUnavailableHoldsOriginalInputClose(t *testing.T) {
	for _, name := range []string{"refresh", "expiry"} {
		t.Run(name, func(t *testing.T) {
			f := newDataControlFixture(t)
			ref, archive := f.input(t)
			began := time.Now().UTC()
			if name == "expiry" {
				began = began.Add(-80 * time.Second) // The actual 120s input lease lapses in about 40s.
			}
			key, token := remoteInputAttempt(t, f, "held-close-"+name, ref, began, 0)
			finish := fullRemoteInput(t, f, token, ref, archive)
			d := finish.download
			s := f.control.artifacts
			s.mu.Lock()
			reader := d.reader
			s.mu.Unlock()
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
				return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataStage}, nil
			}
			staleAuthority(t, f, row.Authority)
			cancel, done := beginRun(t, f.control)
			t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
			if !waitLocked(t, f, 30*time.Second, func() bool { return finish.held }) {
				t.Fatal("original input close was not held")
			}
			s.mu.Lock()
			frame := finish.close
			s.mu.Unlock()
			if frame == nil || frame.reader != reader {
				t.Fatal("held frame is not the original close")
			}
			before := stateDigest(t, f)
			time.Sleep(2500 * time.Millisecond) // Several actual paced Finish retries.
			select {
			case err := <-done:
				f.hold(err)
				t.Fatalf("unavailable authority stopped Run: %v", err)
			default:
			}
			if got, err := f.store.GetRemoteAttempt(key); err != nil || !reflect.DeepEqual(got, row) || !reflect.DeepEqual(stateDigest(t, f), before) {
				t.Fatal("held input close changed the Store")
			}
			if name == "refresh" {
				refreshAuthority(t, f, row.Authority)
			}
			if !waitLocked(t, f, 90*time.Second, func() bool { return d.finished }) {
				t.Fatal("held input close did not finish")
			}
			s.mu.Lock()
			same := finish.close == frame && frame.reader == reader && frame.err == nil && !d.uncertain && !finish.held
			s.mu.Unlock()
			if !same {
				t.Fatal("finish did not use the SAME single original close")
			}
			got, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			if name == "refresh" && (got.Active.Input.State != 4 || got.Active.ClosedReason != 0) {
				t.Fatalf("fresh authority did not commit the SAME complete input: %+v", got.Active.Input)
			}
			if name == "expiry" {
				if f.engine.CheckDelegation(row.Authority) == nil {
					t.Fatal("expiry case finished only after authority returned")
				}
				if got.Active.Input.State == 4 {
					t.Fatal("expired input credited completion")
				}
			}
		})
	}
}

// A Finish failure that does not carry auth.ErrUnavailable is not a hold: it
// still fails integration exactly as before.
func TestAuthUnavailableHoldMatchesOnlyAuthCause(t *testing.T) {
	f := newDataControlFixture(t)
	ref, archive := f.input(t)
	began := time.Now().UTC().Add(-10 * time.Second)
	key, token := remoteInputAttempt(t, f, "task-deadline", ref, began, 1)
	finish := fullRemoteInput(t, f, token, ref, archive)
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	// The actual task deadline (began+2s) has passed, so CheckRemoteGrant
	// refuses with a fresh authority while the attempt remains open.
	started := began.Add(time.Second)
	if _, err = f.store.ObserveRemote(key, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: started}}, started.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	m := &maintenanceState{input: make(chan maintenanceRequest, 1), results: make(chan maintenanceResult, 1)}
	if admitted, err := f.control.dispatchRemoteInputFinish(m, time.Now()); err != nil || !admitted {
		t.Fatalf("original close not dispatched: %v %v", admitted, err)
	}
	request := <-m.input
	result := maintenanceResult{request: request}
	result.downloadClose, result.err = runArtifactDownloadClose(context.Background(), request)
	err = f.control.receiveMaintenance(context.Background(), m, result)
	s := f.control.artifacts
	s.mu.Lock()
	held := finish.held
	s.mu.Unlock()
	if err == nil || errors.Is(err, auth.ErrUnavailable) || held {
		t.Fatalf("non-authority Finish failure became a hold: %v", err)
	}
	// Retire the SAME frame through its existing closed cleanup path.
	current, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.RevokeRemote(key, current.Revision, store.RemoteCanceled, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err = f.control.finishRemoteInput(finish.close); err != nil {
		f.hold(err)
		t.Fatal(err)
	}
	finish.close.release()
}
