//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// These tests drive the SAME original Run, target worker, Store, Files and
// auth through worker→Store→cleanup→next Start. Normal input State4 comes only
// from an actual generated ReadInput and its original close. Native receipts
// are MODELED integration outputs: no cluster, helper, Pod or clock is
// qualified, and the in-process worker serialization is not the Store guard.

// retryChain records the original native calls of one data job.
type retryChain struct {
	mu       sync.Mutex
	starts   []backend.DataStart
	began    map[string]time.Time
	tokens   map[string]string
	removes  []backend.DataResources
	previous []store.RemoteAttempt // first attempt's row at each later Start
	observes atomic.Int32
	read     chan string // attempt IDs whose input the test has read
	started  chan string
}

// dataRetryObservation builds the MODELED terminal facts of one attempt.
type dataRetryObservation func(f *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error)

func retryExit(task backend.Task, ref backend.Reference, began time.Time, code, collector int32) backend.DataObservation {
	finished := time.Now().UTC()
	stop := &backend.DataTaskStop{Task: backend.ContainerExit{ContainerID: "main-" + ref.ID, ExitCode: code, StartedAt: began, FinishedAt: finished}}
	if task.Workdir != nil {
		stop.Stage = &backend.ContainerExit{ContainerID: "stage-" + ref.ID, StartedAt: began, FinishedAt: began}
	}
	if len(task.Setup) != 0 {
		stop.Setup = &backend.ContainerExit{ContainerID: "setup-" + ref.ID, StartedAt: began, FinishedAt: began}
	}
	state := backend.Failed
	if code == 0 {
		state = backend.Succeeded
	}
	o := backend.DataObservation{Status: backend.Status{State: state, Instance: ref.Instance, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop}
	if len(task.Outputs) != 0 {
		o.Collector = &backend.ContainerExit{ContainerID: "collector-" + ref.ID, ExitCode: collector, StartedAt: finished, FinishedAt: finished}
	}
	return o
}

func retryRunning(ref backend.Reference, began time.Time) backend.DataObservation {
	return backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: ref.Instance}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "main-" + ref.ID, StartedAt: began}}
}

func retryNoLaunch(task backend.Task, ref backend.Reference, began time.Time, phase backend.DataInitPhase) backend.DataObservation {
	n := &backend.DataNoLaunch{FailedInit: phase, Stage: &backend.ContainerExit{ContainerID: "stage-" + ref.ID, StartedAt: began, FinishedAt: began}, TaskBlocked: &backend.DataNotStarted{Name: "task", WaitingReason: "PodInitializing"}}
	if len(task.Outputs) != 0 {
		n.CollectorBlocked = &backend.DataNotStarted{Name: "collector", WaitingReason: "PodInitializing"}
	}
	if phase == backend.StageFailure {
		n.Stage.ExitCode = 2
		if len(task.Setup) != 0 {
			n.SetupBlocked = &backend.DataNotStarted{Name: "setup", WaitingReason: "PodInitializing"}
		}
	} else {
		n.Setup = &backend.ContainerExit{ContainerID: "setup-" + ref.ID, ExitCode: 3, StartedAt: began, FinishedAt: began}
	}
	return backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: ref.Instance}, Phase: backend.DataSettled, NoLaunch: n}
}

// freshRetryFixture returns a fixture whose actual auth refresh loop has
// produced a fresh snapshot; a loaded host can otherwise lag the 2s bound.
func freshRetryFixture(t *testing.T) *dataControlFixture {
	t.Helper()
	f := newDataControlFixture(t)
	if !waitFor(30*time.Second, f.engine.Ready) {
		t.Fatal("actual auth snapshot never became fresh")
	}
	return f
}

// waitFor polls cond until it holds or limit elapses.
func waitFor(limit time.Duration, cond func() bool) bool {
	for start := time.Now(); time.Since(start) < limit; time.Sleep(20 * time.Millisecond) {
		if cond() {
			return true
		}
	}
	return cond()
}

// armRetryChain installs MODELED native hooks: the first attempt's observation
// waits for the test's actual input read and then reports first; every later
// attempt, whose input is never read, reports later (a successful main exit
// when nil).
func armRetryChain(f *dataControlFixture, task backend.Task, first, later dataRetryObservation) *retryChain {
	chain := &retryChain{began: map[string]time.Time{}, tokens: map[string]string{}, read: make(chan string, 4), started: make(chan string, 4)}
	var firstID string
	f.backend.startData = func(_ context.Context, start backend.DataStart) (backend.DataCreated, error) {
		chain.mu.Lock()
		if len(chain.starts) != 0 {
			row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: start.JobID, AttemptID: chain.starts[0].Execution.ID})
			if err != nil {
				row = store.RemoteAttempt{}
			}
			chain.previous = append(chain.previous, row)
		} else {
			firstID = start.Execution.ID
		}
		chain.starts = append(chain.starts, start)
		chain.began[start.Execution.ID] = time.Now().UTC()
		// Control clears the original token after its Created integration.
		chain.tokens[start.Execution.ID] = string(start.InputToken)
		chain.mu.Unlock()
		chain.started <- start.Execution.ID
		id := start.Execution.ID
		created := backend.DataCreated{Instance: "pod-" + id, Pod: backend.DataResource{Name: "original-pod", UID: "pod-" + id}}
		if task.Workdir != nil {
			created.InputSecret = backend.DataResource{Name: "original-input", UID: "input-" + id}
		}
		if len(task.Outputs) != 0 {
			created.OutputSecret = backend.DataResource{Name: "original-output", UID: "output-" + id}
		}
		return created, nil
	}
	var readDone sync.Map
	f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
		chain.observes.Add(1)
		chain.mu.Lock()
		began := chain.began[ref.ID]
		chain.mu.Unlock()
		if ref.ID != firstID {
			if later != nil {
				return later(f, task, ref, began)
			}
			return retryExit(task, ref, began, 0, 0), nil
		}
		if _, ok := readDone.Load(ref.ID); !ok {
			select {
			case id := <-chain.read:
				readDone.Store(id, true)
			case <-time.After(10 * time.Second):
				return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataStage}, nil
			}
		}
		return first(f, task, ref, began)
	}
	f.backend.removeData = func(_ context.Context, resources backend.DataResources) (backend.DataRemoval, error) {
		chain.mu.Lock()
		chain.removes = append(chain.removes, resources)
		chain.mu.Unlock()
		return backend.DataRemoval{Pod: resources.Pod, InputSecret: resources.InputSecret, OutputSecret: resources.OutputSecret, PodRemoved: true, InputRemoved: resources.InputSecret != (backend.DataResource{}), OutputRemoved: resources.OutputSecret != (backend.DataResource{})}, nil
	}
	f.backend.cancelData = func(context.Context, backend.Reference) error { return nil }
	return chain
}

// readStartedInput performs the actual full generated ReadInput of the
// attempt Run just started, once its Created is durable.
func readStartedInput(t *testing.T, f *dataControlFixture, chain *retryChain, jobID string, ref artifact.Ref, archive []byte) string {
	t.Helper()
	id := waitValue(t, chain.started)
	key := store.RemoteKey{JobID: jobID, AttemptID: id}
	if !waitFor(10*time.Second, func() bool {
		row, err := f.store.GetRemoteAttempt(key)
		return err == nil && row.Creation == 3
	}) {
		t.Fatal("original Created was not integrated")
	}
	chain.mu.Lock()
	token := chain.tokens[id]
	chain.mu.Unlock()
	finish := fullRemoteInput(t, f, token, ref, archive)
	if !waitLocked(t, f, 10*time.Second, func() bool { return finish.download.finished }) {
		t.Fatal("original complete input was not finished by Run")
	}
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil || row.Active == nil || row.Active.Input == nil || row.Active.Input.State != 4 {
		t.Fatalf("actual full read did not reach normal input State4: %+v %v", row.Active, err)
	}
	chain.read <- id
	return id
}

// Only a selected main exit after normal input completion starts another
// attempt, and only after the first attempt's actual Remove/Ack (State4). Every
// other actual outcome ends the job with zero next Start.
func TestDataRetryStartsOnlyAfterSelectedMainExit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		task    func(*backend.Task)
		first   dataRetryObservation
		later   dataRetryObservation
		act     func(t *testing.T, f *dataControlFixture, job backend.Job)
		retries bool
		final   backend.State
	}{
		{name: "selected", retries: true, final: backend.Succeeded, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryExit(task, ref, began, 7, 0), nil
		}},
		// The last allowed attempt's selected exit schedules nothing, so it
		// commits as a failed main exit without INPUT4, never as a timeout.
		{name: "exhausted", retries: true, final: backend.Failed, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryExit(task, ref, began, 7, 0), nil
		}, later: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryExit(task, ref, began, 7, 0), nil
		}},
		{name: "unselected", final: backend.Failed, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryExit(task, ref, began, 3, 0), nil
		}},
		{name: "stage", final: backend.Failed, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryNoLaunch(task, ref, began, backend.StageFailure), nil
		}},
		{name: "setup", task: func(task *backend.Task) { task.Setup = []string{"prepare"} }, final: backend.Failed, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryNoLaunch(task, ref, began, backend.SetupFailure), nil
		}},
		{name: "collector", task: func(task *backend.Task) { task.Outputs = []string{"result.txt"} }, final: backend.Failed, first: func(_ *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			return retryExit(task, ref, began, 0, 1), nil
		}},
		{name: "cancel", final: backend.Canceled, first: func(f *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			// The selected exit arrives late, after actual cancellation.
			job, err := f.store.Get(ref.ID)
			if err == nil && !job.CancelRequested {
				return retryRunning(ref, began), nil
			}
			return retryExit(task, ref, began, 7, 0), nil
		}, act: func(t *testing.T, f *dataControlFixture, job backend.Job) {
			if !waitFor(10*time.Second, func() bool { return remoteRow(t, f, job).Active.Running != nil }) {
				t.Fatal("original main did not run")
			}
			if _, err := f.control.Cancel(f.actor, job.ID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "timeout", task: func(task *backend.Task) { task.TimeoutSeconds = 1 }, final: backend.Failed, first: func(f *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
			// The selected exit arrives late, after the actual task deadline closed it.
			row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: ref.ID, AttemptID: ref.ID})
			if err == nil && row.Active != nil && row.Active.ClosedReason == 0 {
				return retryRunning(ref, began), nil
			}
			return retryExit(task, ref, began, 7, 0), nil
		}},
		{name: "unknown", final: backend.Unknown, first: func(_ *dataControlFixture, _ backend.Task, ref backend.Reference, _ time.Time) (backend.DataObservation, error) {
			return backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: ref.Instance}, Phase: backend.DataHeld}, errors.New("modeled unknown native state")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := freshRetryFixture(t)
			ref, archive := f.input(t)
			task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &ref, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
			if tc.task != nil {
				tc.task(&task)
			}
			chain := armRetryChain(f, task, tc.first, tc.later)
			job, err := f.control.SubmitData(f.actor, "retry-"+tc.name, task)
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := beginRun(t, f.control)
			t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
			first := readStartedInput(t, f, chain, job.ID, ref, archive)
			if first != job.ID {
				t.Fatal("first Start used another attempt")
			}
			if tc.act != nil {
				tc.act(t, f, job)
			}
			if tc.final == backend.Unknown {
				time.Sleep(2500 * time.Millisecond) // Several actual worker turns.
				current := currentJob(t, f, job.ID)
				row := remoteRow(t, f, current)
				if current.Status.State.Terminal() || current.Status.State == backend.Retrying || row.State != 2 || row.Active == nil || row.Active.Stop != nil {
					t.Fatalf("unknown native state completed or retried: %v %+v", current.Status, row.Active)
				}
			} else if !waitFor(20*time.Second, func() bool { return currentJob(t, f, job.ID).Status.State.Terminal() }) {
				t.Fatalf("job did not finish: %v", currentJob(t, f, job.ID).Status)
			}
			want := 1
			if tc.retries {
				want = 2
			}
			if tc.final != backend.Unknown && !waitFor(10*time.Second, func() bool {
				// Job state is committed at Complete; Remove/Ack follow it.
				for _, a := range currentJob(t, f, job.ID).Attempts {
					row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: job.ID, AttemptID: a.ID})
					if err != nil || row.State != 4 {
						return false
					}
				}
				return true
			}) {
				t.Fatal("finished attempts were not removed")
			}
			current := currentJob(t, f, job.ID)
			chain.mu.Lock()
			starts, removes, previous := len(chain.starts), len(chain.removes), chain.previous
			chain.mu.Unlock()
			if starts != want || len(current.Attempts) != want {
				t.Fatalf("%d native Starts and %d attempts, want %d", starts, len(current.Attempts), want)
			}
			if tc.final != backend.Unknown && (current.Status.State != tc.final || removes != want) {
				t.Fatalf("final %v with %d Removes, want %v and %d", current.Status.State, removes, tc.final, want)
			}
			if tc.retries {
				// The next Start followed the first attempt's positive Remove/Ack.
				if len(previous) != 1 || previous[0].State != 4 || previous[0].Done == nil || previous[0].Done.Outcome != store.DataMainExit {
					t.Fatalf("second Start preceded first retirement: %+v", previous)
				}
				last := current.Attempts[len(current.Attempts)-1]
				row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: job.ID, AttemptID: last.ID})
				if err != nil || row.Done == nil || row.Done.Outcome != store.DataMainExit {
					t.Fatalf("last attempt outcome: %+v %v", row.Done, err)
				}
				if tc.name == "exhausted" && (last.Status.ExitCode == nil || *last.Status.ExitCode != 7 || last.Status.Termination != backend.StoppedWithExitCode) {
					t.Fatalf("final selected exit was relabeled: %+v", last.Status)
				}
			}
		})
	}
}

// retryInputAttempt submits a Workdir retry job and begins its original
// attempt directly at began, so its actual input lease lapses on schedule.
func retryInputAttempt(t *testing.T, f *dataControlFixture, id string, task backend.Task, began time.Time) store.RemoteKey {
	t.Helper()
	authority, err := f.engine.Delegate(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	target := f.target()
	policy := backend.DefaultDataPolicy()
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
	created := backend.DataCreated{Instance: "pod-" + id, Pod: backend.DataResource{Name: "original-pod", UID: "pod-" + id}, InputSecret: backend.DataResource{Name: "original-input", UID: "input-" + id}}
	if _, err = f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, began); err != nil {
		t.Fatal(err)
	}
	return key
}

// A selected Workdir main exit after only a closed PARTIAL input read is not
// normal completion: it caches and commits nothing. A later actual input
// lease expiry or cancellation then finishes it through the existing cleanup
// outcome with zero next Start. Store renews the input lease on each healthy
// observation, so expiry lands about 120s after the last one.
func TestDataRetryPartialInputIsCleanupOnly(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    int32
		limit   time.Duration
		outcome store.DataOutcome
		final   backend.State
	}{
		{"expiry", 7, 180 * time.Second, store.DataTimeout, backend.Failed},
		{"cancel", 7, 30 * time.Second, store.DataCanceled, backend.Canceled},
		// An unselected exit schedules nothing: it commits promptly as the
		// actual failed main exit despite the partial read.
		{"unselected", 3, 30 * time.Second, store.DataMainExit, backend.Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := freshRetryFixture(t)
			ref, _ := f.input(t)
			task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &ref, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
			began := time.Now().UTC()
			key := retryInputAttempt(t, f, "partial-"+tc.name, task, began)
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := f.store.CheckRemoteGrant(key.AttemptID, store.RemoteInput, row.Active.Input.Hash, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			// One Store-only never-opened read, closed through the ordinary partial
			// path: no Reader/FD or HTTP response exists, and it cannot reach State4.
			lease, err := f.store.BeginRemoteInputRead(grant, store.OriginalServerLifetime{}, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			closing, err := f.store.MarkArtifactDownloadClosing(store.ArtifactDownloadClosure{Owner: f.actor.Principal(), LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer})
			if err != nil {
				t.Fatal(err)
			}
			if err = f.store.FinishArtifactDownload(closing, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			var starts, removes atomic.Int32
			f.backend.startData = func(context.Context, backend.DataStart) (backend.DataCreated, error) {
				starts.Add(1)
				return backend.DataCreated{}, errors.New("modeled refusal: no next Start is expected")
			}
			f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
				return retryExit(task, ref, began, tc.code, 0), nil
			}
			f.backend.removeData = func(_ context.Context, resources backend.DataResources) (backend.DataRemoval, error) {
				removes.Add(1)
				return backend.DataRemoval{Pod: resources.Pod, InputSecret: resources.InputSecret, PodRemoved: true, InputRemoved: true}, nil
			}
			f.backend.cancelData = func(context.Context, backend.Reference) error { return nil }
			cancel, done := beginRun(t, f.control)
			t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
			if !waitFor(5*time.Second, func() bool {
				row, err := f.store.GetRemoteAttempt(key)
				return err == nil && row.Active != nil && row.Active.Stop != nil
			}) {
				if tc.code == 7 {
					t.Fatal("original selected main exit was not observed")
				}
			}
			if tc.code == 7 {
				// The observed selected exit stays uncommitted while nothing closed it.
				time.Sleep(time.Second)
				if row, err = f.store.GetRemoteAttempt(key); err != nil || row.State != 2 || row.Active == nil || row.Active.ClosedReason != 0 || row.Active.Input.State != 2 {
					t.Fatalf("partial input committed a completion: %+v %v", row.Active, err)
				}
			}
			if tc.name == "cancel" {
				if _, err = f.control.Cancel(f.actor, key.JobID); err != nil {
					t.Fatal(err)
				}
			}
			if !waitFor(tc.limit, func() bool {
				row, err := f.store.GetRemoteAttempt(key)
				return err == nil && row.State == 4 && currentJob(t, f, key.JobID).Status.State.Terminal()
			}) {
				t.Fatal("actual close did not finish and remove the attempt")
			}
			job := currentJob(t, f, key.JobID)
			if row, err = f.store.GetRemoteAttempt(key); err != nil || row.Done == nil || row.Done.Outcome != tc.outcome {
				t.Fatalf("partial input completed as %+v %v", row.Done, err)
			}
			if job.Status.State != tc.final || len(job.Attempts) != 1 || starts.Load() != 0 || removes.Load() != 1 {
				t.Fatalf("partial input retried: %v attempts=%d starts=%d removes=%d", job.Status, len(job.Attempts), starts.Load(), removes.Load())
			}
		})
	}
}

// EXPECTED FAIL on C18: this depends on the S08 Store predecessor guard in
// BeginRemoteAttempt (pending; ROOT-RATIFICATION 974b15b9 hunk 1). After a
// selected main exit, the next attempt must not begin while the predecessor
// is still State3 (no positive Remove/Ack). Mount-only, so no private Input
// receipt is involved. Uses only the existing public Store API.
func TestDataRetryBeginRefusedBeforePredecessorRetired(t *testing.T) {
	f := freshRetryFixture(t)
	task := submissionDataTask()
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}
	job, err := f.control.SubmitData(f.actor, "predecessor", task)
	if err != nil {
		t.Fatal(err)
	}
	key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
	began := time.Now().UTC()
	start, ok, err := f.store.BeginRemoteAttempt(job.ID, key.AttemptID, store.RemoteHashes{}, backend.DefaultDataPolicy(), began)
	if err != nil || !ok {
		t.Fatalf("original Begin: %v %v", ok, err)
	}
	created := backend.DataCreated{Instance: "pod-predecessor", Pod: backend.DataResource{Name: "original-pod", UID: "pod-predecessor"}}
	row, err := f.store.RecordRemoteCreated(key, start.Revision, store.RemoteCreation{Succeeded: true, Resources: created}, began)
	if err != nil {
		t.Fatal(err)
	}
	o := retryExit(task, backend.Reference{ID: key.AttemptID, Instance: created.Pod.UID}, began, 7, 0)
	if _, err = f.store.ObserveRemote(key, row.Revision, o, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if job, err = f.store.CompleteRemoteAttempt(key, store.RemoteCompletion{Outcome: store.DataMainExit, Stop: o.TaskStop}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if row, err = f.store.GetRemoteAttempt(key); err != nil || job.Status.State != backend.Retrying || job.NextAttemptAt == nil || row.State != 3 {
		t.Fatalf("fixture did not reach Retrying with a State3 predecessor: %v %v", job.Status.State, err)
	}
	_, began2, err := f.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{}, backend.DefaultDataPolicy(), job.NextAttemptAt.Add(time.Nanosecond))
	after, e := f.store.GetRemoteAttempt(key)
	if began2 || e != nil || after.State != 3 || len(currentJob(t, f, job.ID).Attempts) != 1 {
		t.Fatalf("next attempt began before predecessor retirement: start=%v err=%v predecessor=%d", began2, err, after.State)
	}
}

// spareActor authenticates the spare administrative credential that
// revokeAuthority preserves: the same principal under fresh authority.
func spareActor(t *testing.T, f *dataControlFixture) auth.Actor {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.directory, "spare-token"))
	if err != nil {
		t.Fatal(err)
	}
	actor, err := f.engine.Authenticate("Bearer " + string(bytes.TrimSpace(raw)))
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

// revokeInCompletionGap is a MODELED scheduling stimulus, not a native or
// timing claim. Once the selected exit is recorded it holds Control.runMu,
// which the coordinator next takes inside remoteAttemptTransfersSettled, after
// its row read and authority check and before it builds and commits the
// completion. In that gap it performs the actual revocation and the actual
// authority close the transfer server performs, then releases the coordinator.
func revokeInCompletionGap(t *testing.T, f *dataControlFixture, key store.RemoteKey, authority auth.Delegation) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; runtime.Gosched() {
		row, err := f.store.GetRemoteAttempt(key)
		if err == nil && row.Active != nil && row.Active.Stop != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("selected exit was not recorded")
		}
	}
	f.control.runMu.Lock()
	locked := true
	defer func() {
		if locked {
			f.control.runMu.Unlock()
		}
	}()
	// The coordinator waits for runMu while holding the artifact service lock.
	for i, blocked := 0, 0; blocked < 5; i++ {
		if i == 500 {
			t.Fatal("coordinator did not reach the completion fence")
		}
		time.Sleep(10 * time.Millisecond)
		if f.control.artifacts.mu.TryLock() {
			f.control.artifacts.mu.Unlock()
			blocked = 0
			continue
		}
		blocked++
	}
	p := f.control.run.state.workers[0].dataCompletion
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil || p == nil || p.key != key || p.step != dataComplete || p.completion != nil || p.fenced || p.row.Active == nil || p.row.Active.ClosedReason != 0 ||
		row.State != 2 || row.Active == nil || row.Active.Stop == nil || row.Active.ClosedReason != 0 || row.Done != nil {
		t.Fatal("modeled stimulus missed the row-read/Complete gap")
	}
	revokeAuthority(t, f, authority)
	f.control.closeRemoteAuthority(key, row.Active.Input.Hash, store.RemoteInput)
	if row, err = f.store.GetRemoteAttempt(key); err != nil || row.State != 2 || row.Active.ClosedReason != store.RemoteAuthorityLost || row.Done != nil {
		t.Fatalf("actual close in the gap: %+v %v", row.Active, err)
	}
	locked = false
	f.control.runMu.Unlock()
}

// A selected main exit whose authority is lost, before Control builds the
// completion or between its row read and the Store commit, ends the job as a
// job-level authority failure. The SAME original frame completes, removes and
// acknowledges the original attempt; no second attempt launches; the target
// then serves a bystander under fresh authority for the same principal, and
// that fresh authority does not resurrect the old attempt.
//
// This composes with Store's remoteCompletionStatus authority-lost
// normalization (Phase-B Store FROZEN-2); before it, Store refuses the Complete
// and the cached frame stalls.
func TestDataRetryLostAuthorityEndsWithoutRetry(t *testing.T) {
	for _, name := range []string{"revoked", "gap"} {
		t.Run(name, func(t *testing.T) {
			f := freshRetryFixture(t)
			ref, archive := f.input(t)
			task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &ref, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
			chain := armRetryChain(f, task, func(f *dataControlFixture, task backend.Task, ref backend.Reference, began time.Time) (backend.DataObservation, error) {
				if name == "revoked" {
					// The selected exit arrives only after the actual authority close.
					row, err := f.store.GetRemoteAttempt(store.RemoteKey{JobID: ref.ID, AttemptID: ref.ID})
					if err != nil || row.Active == nil || row.Active.ClosedReason != store.RemoteAuthorityLost {
						return retryRunning(ref, began), nil
					}
				}
				return retryExit(task, ref, began, 7, 0), nil
			}, nil)
			job, err := f.control.SubmitData(f.actor, "lost-"+name, task)
			if err != nil {
				t.Fatal(err)
			}
			cancel, done := beginRun(t, f.control)
			t.Cleanup(func() { remoteTransferRetireRun(t, f, cancel, done) })
			if first := readStartedInput(t, f, chain, job.ID, ref, archive); first != job.ID {
				t.Fatal("first Start used another attempt")
			}
			key := store.RemoteKey{JobID: job.ID, AttemptID: job.ID}
			row, err := f.store.GetRemoteAttempt(key)
			if err != nil {
				t.Fatal(err)
			}
			authority := row.Authority
			if name == "revoked" {
				revokeAuthority(t, f, authority)
			} else {
				revokeInCompletionGap(t, f, key, authority)
			}
			if !waitFor(30*time.Second, func() bool {
				row, err := f.store.GetRemoteAttempt(key)
				return err == nil && row.State == 4 && currentJob(t, f, job.ID).Status.State.Terminal()
			}) {
				row, _ := f.store.GetRemoteAttempt(key)
				t.Fatalf("lost authority did not finish the attempt: %v state=%d %+v", currentJob(t, f, job.ID).Status, row.State, row.Done)
			}
			// The target clears only after the actual acknowledgement.
			if !waitFor(10*time.Second, func() bool {
				f.control.runMu.Lock()
				defer f.control.runMu.Unlock()
				p := f.control.run.state.workers[0].dataCompletion
				return p == nil || p.key != key
			}) {
				t.Fatal("retired attempt still occupies the target")
			}
			var retired *store.RemoteAttempt
			checkPrimary := func() {
				t.Helper()
				current := currentJob(t, f, job.ID)
				if current.Status.State != backend.Failed || current.Status.ExitCode != nil || current.Status.Termination != backend.UnknownTermination || current.NextAttemptAt != nil || len(current.Attempts) != 1 {
					t.Fatalf("lost authority retried or relabeled the job: %+v attempts=%d", current.Status, len(current.Attempts))
				}
				// The actual Ack compacts the native receipts away at State4. The
				// semantic digest, outcome and original exit remain, with the
				// original resources and their positive removal bits.
				row, err := f.store.GetRemoteAttempt(key)
				if err != nil || row.State != 4 || row.Ack != 3 || row.Resources.Pod.UID != "pod-"+job.ID || row.Resources.InputSecret.UID != "input-"+job.ID ||
					row.Done == nil || row.Done.Digest == ([32]byte{}) || row.Done.Outcome != store.DataMainExit || row.Done.OriginalExit == nil || *row.Done.OriginalExit != 7 ||
					row.Done.Stop != nil || row.Done.NoLaunch != nil || row.Done.Collector != nil || row.Done.ResultID != "" {
					t.Fatalf("original completion identity: state=%d ack=%d %+v %v", row.State, row.Ack, row.Done, err)
				}
				if retired == nil {
					retired = &row
				} else if !reflect.DeepEqual(row, *retired) {
					t.Fatalf("retired attempt changed: %+v, was %+v", row, *retired)
				}
				chain.mu.Lock()
				defer chain.mu.Unlock()
				starts, removes := 0, 0
				for _, s := range chain.starts {
					if s.JobID == job.ID {
						starts++
					}
				}
				for _, r := range chain.removes {
					if r.Pod.UID == "pod-"+job.ID {
						removes++
					}
				}
				if starts != 1 || removes != 1 {
					t.Fatalf("%d native Starts and %d Removes of the original attempt, want 1 and 1", starts, removes)
				}
			}
			checkPrimary()

			// Fresh authority for the same principal serves new work only.
			bystander, err := f.control.SubmitData(spareActor(t, f), "bystander-"+name, backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &ref})
			if err != nil {
				t.Fatal(err)
			}
			if !waitFor(30*time.Second, func() bool { return currentJob(t, f, bystander.ID).Status.State.Terminal() }) {
				t.Fatalf("bystander did not progress: %v", currentJob(t, f, bystander.ID).Status)
			}
			if got := currentJob(t, f, bystander.ID).Status.State; got != backend.Succeeded {
				t.Fatalf("bystander finished %v, want Succeeded", got)
			}
			checkPrimary()
			if err := f.engine.CheckDelegation(authority); !errors.Is(err, auth.ErrUnauthenticated) {
				t.Fatalf("revoked delegation after fresh authority: %v", err)
			}
		})
	}
}
