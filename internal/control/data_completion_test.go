//go:build darwin || linux

package control

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// This is the SAME original Run and existing target worker, with actual Store,
// Files/auth resources. Native receipts are MODELED integration outputs; the
// test establishes no runtime stop/no-launch, UID deletion or Pod absence fact.
func TestDataCompletionRetainsOriginalRemovalThroughAckTail(t *testing.T) {
	f := newDataControlFixture(t)
	input, _ := f.input(t)
	observeEntered := make(chan struct{}, 1)
	observeRelease := make(chan struct{})
	removeEntered := make(chan backend.DataResources, 1)
	removeRelease := make(chan struct{})
	var starts, observes, removes atomic.Int32
	var started time.Time // Used only by the same serial original target worker.
	created := backend.DataCreated{Instance: "modeled-pod", Pod: backend.DataResource{Name: "original-pod", UID: "modeled-pod"}, InputSecret: backend.DataResource{Name: "original-input", UID: "modeled-input"}}
	f.backend.startData = func(context.Context, backend.DataStart) (backend.DataCreated, error) {
		starts.Add(1)
		started = time.Now().UTC()
		return created, nil
	}
	f.backend.observeData = func(context.Context, backend.Reference) (backend.DataObservation, error) {
		observes.Add(1)
		observeEntered <- struct{}{}
		<-observeRelease
		stage := &backend.ContainerExit{ContainerID: "modeled-stage", ExitCode: 23, StartedAt: started, FinishedAt: started.Add(time.Millisecond)}
		return backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: created.Pod.UID}, Phase: backend.DataSettled, NoLaunch: &backend.DataNoLaunch{FailedInit: backend.StageFailure, Stage: stage, TaskBlocked: &backend.DataNotStarted{Name: "task", WaitingReason: "PodInitializing"}}}, nil
	}
	f.backend.removeData = func(_ context.Context, resources backend.DataResources) (backend.DataRemoval, error) {
		removes.Add(1)
		removeEntered <- resources
		<-removeRelease
		// An error does not erase the SAME original function's qualified independent
		// positive values; these modeled values still require the actual backend's
		// separate native/HTTP ownership qualification in a real deployment.
		return backend.DataRemoval{Pod: resources.Pod, InputSecret: resources.InputSecret, OutputSecret: resources.OutputSecret, PodRemoved: true, InputRemoved: true}, errors.New("modeled error after positive independent retirement")
	}
	task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &input, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{23}}}
	// The init failure is explicitly nonretryable despite this accepted policy.
	job, err := f.control.SubmitData(f.actor, "ack-tail", task)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, f.control)
	preparedBoundary := false
	releaseObserve := sync.OnceFunc(func() { close(observeRelease) })
	releaseRemove := sync.OnceFunc(func() { close(removeRelease) })
	t.Cleanup(func() {
		releaseObserve()
		releaseRemove()
		if preparedBoundary {
			drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
			if err := f.control.DrainArtifacts(drain); err != nil {
				f.hold(err)
				t.Error(err)
			}
			stop()
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			f.hold(errors.New("original completion Run did not join"))
			t.Error("original completion Run remains held")
			return
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := f.control.Join(ctx); e != nil {
			f.hold(e)
			t.Error(e)
		}
		if !preparedBoundary {
			f.hold(errors.New("test retained original remote frames before Prepared boundary"))
		}
	})
	waitValue(t, observeEntered)
	key := store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()}
	row, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := f.store.CheckRemoteGrant(key.AttemptID, store.RemoteInput, row.Active.Input.Hash, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	// Store-only never-opened reader closures model trusted completed metadata
	// handoffs. No Reader/FD or HTTP response is created, no normal INPUT4 is
	// asserted, and Control's actual per-attempt transfer query sees no admitted
	// transfer. The 65 original closed links require TWO bounded Ack quanta.
	for i := 0; i < 65; i++ {
		lease, e := f.store.BeginRemoteInputRead(grant, store.OriginalServerLifetime{}, time.Now().UTC())
		if e != nil {
			t.Fatal(e)
		}
		closure := store.ArtifactDownloadClosure{Owner: f.actor.Principal(), LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer}
		closing, e := f.store.MarkArtifactDownloadClosing(closure)
		if e != nil {
			t.Fatal(e)
		}
		if e = f.store.FinishArtifactDownload(closing, time.Now().UTC()); e != nil {
			t.Fatal(e)
		}
	}
	releaseObserve()
	resources := waitValue(t, removeEntered)
	if resources.Execution.ID != key.AttemptID || resources.Execution.Binding != job.TargetBinding || resources.Execution.Instance != created.Pod.UID || resources.Pod != created.Pod || resources.InputSecret != created.InputSecret {
		t.Fatal("cleanup used current/foreign reference or lost immutable resources")
	}
	before, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if before.State != 3 || before.Done == nil || before.Done.Outcome != store.DataStageFailure || before.Done.OriginalExit != nil || before.Done.NoLaunch == nil || before.Ack != 0 {
		t.Fatalf("native no-launch was not distinct durable completion: %#v", before)
	}
	finished, err := f.store.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status.State != backend.Failed || finished.Status.ExitCode != nil || finished.Status.Termination == backend.StoppedWithExitCode || len(finished.Attempts) != 1 {
		t.Fatalf("init failure invented main/retry semantics: %#v", finished)
	}
	if err = f.control.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	held, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = f.control.WaitShutdownPrepared(held)
	stop()
	if err == nil {
		t.Fatal("blocked original Remove became prepared")
	}
	releaseRemove()
	prepared, end := context.WithTimeout(context.Background(), 5*time.Second)
	err = f.control.WaitShutdownPrepared(prepared)
	end()
	if err != nil {
		t.Fatal(err)
	}
	preparedBoundary = true
	after, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if after.State != 4 || after.Ack != 3 || after.Revision < before.Revision+2 || after.Done == nil || after.Done.NoLaunch != nil {
		t.Fatalf("Ack nil/fullbits compacted before all bounded links detached: %#v", after)
	}
	if starts.Load() != 1 || observes.Load() != 1 || removes.Load() != 1 {
		t.Fatalf("metadata tail repeated native work: start=%d observe=%d remove=%d", starts.Load(), observes.Load(), removes.Load())
	}
}

func TestDataOperationsPreparedRetainsEveryOriginalFrame(t *testing.T) {
	c := &Control{}
	request := &dataRequest{}
	for _, tc := range []struct {
		name   string
		worker targetWorker
	}{
		{"pending", targetWorker{pending: &operation{data: request}}},
		{"received", targetWorker{received: &receivedOperation{request: &operation{data: request}}}},
		{"held-error", targetWorker{dataCompletion: &remotePending{held: true, err: errors.New("original ownership uncertain")}}},
		{"ack-tail", targetWorker{dataCompletion: &remotePending{step: dataAck, row: store.RemoteAttempt{State: 3, Ack: 7}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready, err := c.dataOperationsPrepared(&loopState{workers: []targetWorker{tc.worker}})
			if err != nil || ready {
				t.Fatalf("original frame forgotten: %v %v", ready, err)
			}
		})
	}
	if ready, err := c.dataOperationsPrepared(&loopState{}); err != nil || !ready {
		t.Fatalf("empty worker frontier: %v %v", ready, err)
	}
}

func TestDataCompletionRetryKeepsEachOriginalNativeReference(t *testing.T) {
	f := newDataControlFixture(t)
	starts := make(chan backend.DataStart, 2)
	removals := make(chan backend.DataResources, 2)
	releaseSecond := make(chan struct{})
	var startCount, observeCount, removeCount atomic.Int32
	began := make(map[string]time.Time) // Only the same serial target worker uses it.
	f.backend.startData = func(_ context.Context, start backend.DataStart) (backend.DataCreated, error) {
		startCount.Add(1)
		began[start.Execution.ID] = time.Now().UTC()
		starts <- start
		pod := backend.DataResource{Name: "original-pod", UID: "pod-" + start.Execution.ID}
		return backend.DataCreated{Instance: pod.UID, Pod: pod}, nil
	}
	f.backend.observeData = func(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
		observeCount.Add(1)
		code := int32(1)
		if startCount.Load() == 2 {
			code = 0
		}
		stop := &backend.DataTaskStop{Task: backend.ContainerExit{ContainerID: "main-" + ref.ID, ExitCode: code, StartedAt: began[ref.ID], FinishedAt: time.Now().UTC()}}
		state := backend.Failed
		if code == 0 {
			state = backend.Succeeded
		}
		return backend.DataObservation{Status: backend.Status{State: state, Instance: ref.Instance, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: stop}, nil
	}
	f.backend.removeData = func(_ context.Context, resources backend.DataResources) (backend.DataRemoval, error) {
		n := removeCount.Add(1)
		removals <- resources
		if n == 2 {
			<-releaseSecond
		}
		return backend.DataRemoval{Pod: resources.Pod, InputSecret: resources.InputSecret, OutputSecret: resources.OutputSecret, PodRemoved: true}, nil
	}
	task := submissionDataTask()
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}
	job, err := f.control.SubmitData(f.actor, "retry-references", task)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, f.control)
	preparedBoundary := false
	release := sync.OnceFunc(func() { close(releaseSecond) })
	t.Cleanup(func() {
		release()
		if preparedBoundary {
			drain, stop := context.WithTimeout(context.Background(), 5*time.Second)
			if err := f.control.DrainArtifacts(drain); err != nil {
				f.hold(err)
				t.Error(err)
			}
			stop()
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			f.hold(errors.New("retry original Run did not join"))
			t.Error("retry Run remains held")
			return
		}
		ctx, end := context.WithTimeout(context.Background(), 5*time.Second)
		defer end()
		if e := f.control.Join(ctx); e != nil {
			f.hold(e)
			t.Error(e)
		}
		if !preparedBoundary {
			f.hold(errors.New("test retained original remote frames before Prepared boundary"))
		}
	})
	first := waitValue(t, starts)
	firstRemoval := waitValue(t, removals)
	if firstRemoval.Execution.ID != first.Execution.ID || firstRemoval.Execution.Binding != job.TargetBinding || firstRemoval.Execution.Instance != "pod-"+first.Execution.ID || firstRemoval.Pod.UID != firstRemoval.Execution.Instance {
		t.Fatal("first cleanup changed accepted execution reference")
	}
	second := waitValue(t, starts)
	secondRemoval := waitValue(t, removals)
	if first.Execution.ID == second.Execution.ID || second.Execution.ID == job.ID || second.JobID != job.ID || secondRemoval.Execution.ID != second.Execution.ID || secondRemoval.Execution.Instance != "pod-"+second.Execution.ID || secondRemoval.Pod.UID != secondRemoval.Execution.Instance {
		t.Fatal("retry adopted old/current native reference")
	}
	// Shutdown joins the already accepted second Remove and its metadata tail.
	if err = f.control.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	release()
	prepared, end := context.WithTimeout(context.Background(), 5*time.Second)
	err = f.control.WaitShutdownPrepared(prepared)
	end()
	if err != nil {
		t.Fatal(err)
	}
	preparedBoundary = true
	for _, start := range []backend.DataStart{first, second} {
		row, e := f.store.GetRemoteAttempt(store.RemoteKey{JobID: job.ID, AttemptID: start.Execution.ID})
		if e != nil {
			t.Fatal(e)
		}
		if row.State != 4 || row.Ack != 1 {
			t.Fatalf("accepted execution not positively retired: %#v", row)
		}
	}
	current, err := f.store.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status.State != backend.Succeeded || current.CurrentAttemptID() != second.Execution.ID || len(current.Attempts) != 2 {
		t.Fatalf("retry core history changed: %#v", current)
	}
	if startCount.Load() != 2 || observeCount.Load() != 2 || removeCount.Load() != 2 {
		t.Fatalf("retry repeated a native function: %d/%d/%d", startCount.Load(), observeCount.Load(), removeCount.Load())
	}
}
