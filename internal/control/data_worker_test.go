//go:build darwin || linux

package control

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// Native callbacks below model integration-qualified values. These tests prove
// original Go worker/result and actual Store transitions, not native Pod/image,
// termination, no-launch, server transfer or filesystem custody qualification.
func TestDataWorkerInvokesCanceledAcceptedStartOnce(t *testing.T) {
	entered := make(chan error, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	originalErr := errors.New("modeled original start returned partial resources")
	created := backend.DataCreated{Instance: "original-pod", Pod: backend.DataResource{Name: "pod", UID: "original-pod"}}
	b := &submissionDataBackend{startData: func(ctx context.Context, start backend.DataStart) (backend.DataCreated, error) {
		calls.Add(1)
		entered <- ctx.Err()
		<-release
		return created, originalErr
	}}
	request := &dataRequest{target: &dataTarget{target: backend.Target{Backend: b}, backend: b}, key: store.RemoteKey{JobID: "job", AttemptID: "attempt"}}
	op := operation{kind: dataStartOperation, data: request}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan operationResult, 1)
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() {
		releaseOnce()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("original canceled Start callback did not join")
		}
	})
	go func() { done <- runDataOperation(ctx, op) }()
	if got := waitValue(t, entered); !errors.Is(got, context.Canceled) {
		t.Fatal("accepted Start was not passed original cancellation")
	}
	select {
	case <-done:
		t.Fatal("context cancellation fabricated an early callback return")
	default:
	}
	releaseOnce()
	result := waitValue(t, done)
	// Put the SAME result back for the registered exact-owner join, no rerun.
	done <- result
	if result.notStarted || result.data == nil || result.data.request != request || result.data.created != created || !errors.Is(result.data.err, originalErr) || !errors.Is(result.err, originalErr) || calls.Load() != 1 {
		t.Fatal("original partial request/result/error was replaced")
	}
}

func TestDataWorkerHealthyPreparationIntegratesOriginalStart(t *testing.T) {
	f := newDataControlFixture(t)
	input, _ := f.input(t)
	entered := make(chan backend.DataStart, 1)
	release := make(chan struct{})
	var starts, observes, removes atomic.Int32
	created := backend.DataCreated{Instance: "modeled-original-pod", Pod: backend.DataResource{Name: "original-pod", UID: "modeled-original-pod"}, InputSecret: backend.DataResource{Name: "original-input", UID: "modeled-original-input"}, OutputSecret: backend.DataResource{Name: "original-output", UID: "modeled-original-output"}}
	f.backend.startData = func(ctx context.Context, start backend.DataStart) (backend.DataCreated, error) {
		starts.Add(1)
		entered <- start
		<-release
		return created, nil
	}
	f.backend.observeData = func(context.Context, backend.Reference) (backend.DataObservation, error) {
		observes.Add(1)
		return backend.DataObservation{}, errors.New("unexpected new observation during preparation")
	}
	f.backend.removeData = func(context.Context, backend.DataResources) (backend.DataRemoval, error) {
		removes.Add(1)
		return backend.DataRemoval{}, errors.New("unexpected removal of healthy borrowed execution")
	}
	task := backend.Task{Argv: []string{"trainer"}, Image: "modeled-image", Workdir: &input, Outputs: []string{"result.txt"}}
	job, err := f.control.SubmitData(f.actor, "blocked-data-start", task)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := beginRun(t, f.control)
	preparedBoundary := false
	releaseOnce := sync.OnceFunc(func() { close(release) })
	// Registered after the actual fixture, so all original Run/native worker
	// functions join BEFORE its Files/root/auth/Store owners can retire.
	t.Cleanup(func() {
		releaseOnce()
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
			f.hold(errors.New("original data Run did not return"))
			t.Error("original Run remains held")
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
	original := waitValue(t, entered)
	key := store.RemoteKey{JobID: job.ID, AttemptID: original.Execution.ID}
	if original.JobID != job.ID || original.Execution.Binding != job.TargetBinding || original.Execution.Instance != "" || len(original.InputToken) != 114 || len(original.OutputToken) != 114 || bytes.Equal(original.InputToken, original.OutputToken) {
		t.Fatal("native Start did not receive exact scoped accepted request")
	}
	portable, err := backend.TaskBytes(original.Execution.Task)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(portable, original.InputToken) || bytes.Contains(portable, original.OutputToken) {
		t.Fatal("plaintext token entered portable Task")
	}
	before, err := f.store.GetRemoteAttempt(key)
	if err != nil {
		t.Fatal(err)
	}
	if before.Creation != 0 || before.State != 2 || before.Active == nil || before.Active.Input.State != 1 || before.Active.Output.State != 1 {
		t.Fatalf("blocked original Start gained creation authority: %#v", before)
	}
	if err = f.control.BeginShutdown(); err != nil {
		t.Fatal(err)
	}
	blocked, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err = f.control.WaitShutdownPrepared(blocked)
	stop()
	if err == nil {
		t.Fatal("preparation forgot blocked original Start")
	}
	releaseOnce()
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
	if after.Creation != 3 || after.Resources != created || after.Active == nil || after.Active.ClosedReason != 0 || after.Active.Input.State != 2 || after.Active.Output.State != 2 || after.Active.Input.Hash != before.Active.Input.Hash || after.Active.Output.Hash != before.Active.Output.Hash || !after.Active.Input.Expires.Equal(before.Active.Input.Expires) || !after.Active.Output.Expires.Equal(before.Active.Output.Expires) {
		t.Fatalf("healthy preparation revoked, renewed, or replaced original creation: %#v", after)
	}
	if starts.Load() != 1 || observes.Load() != 0 || removes.Load() != 0 {
		t.Fatal("preparation issued replacement/new native work")
	}
	if !bytes.Equal(original.InputToken, make([]byte, len(original.InputToken))) || !bytes.Equal(original.OutputToken, make([]byte, len(original.OutputToken))) {
		t.Fatal("durably consumed original Start retained plaintext")
	}
}

func TestDataIntegrateRejectsForeignOriginalPairWithoutRelease(t *testing.T) {
	c := &Control{retention: retentionState{pending: make(map[string]struct{})}}
	request := &dataRequest{key: store.RemoteKey{JobID: "job", AttemptID: "attempt"}}
	op := &operation{kind: dataStartOperation, data: request}
	foreign := &dataRequest{key: request.key}
	result := operationResult{operation: *op, data: &dataResult{request: foreign}}
	original := &receivedOperation{request: op, result: result}
	w := &targetWorker{pending: op, received: original}
	if err := c.integrateData(&loopState{}, w, result); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("foreign original accepted: %v", err)
	}
	if w.pending != op || w.received != original || w.dataCompletion != nil {
		t.Fatal("refusal discarded original unmatched custody")
	}
	result.data.request = request
	original.result = result
	if err := c.integrateData(&loopState{}, w, result); err != nil {
		t.Fatal(err)
	}
	if w.pending != nil || w.dataCompletion == nil || w.dataCompletion.original != original {
		t.Fatal("received pair was not transferred before clearing pending")
	}
}

func TestDataWorkerCancelKeepsSeparateOriginalObservationError(t *testing.T) {
	cancelErr := errors.New("modeled cancel uncertainty")
	observeErr := errors.New("modeled observation uncertainty")
	var order []string // This worker call is synchronous, with no other writer.
	ref := backend.Reference{ID: "historical-attempt", Binding: "accepted-binding", Instance: "original-pod", Task: backend.Task{Argv: []string{"original"}, Env: map[string]string{"A": "original"}}}
	b := &submissionDataBackend{}
	b.cancelData = func(_ context.Context, got backend.Reference) error {
		order = append(order, "cancel")
		if got.ID != ref.ID || got.Instance != ref.Instance || got.Binding != ref.Binding {
			t.Fatal("cancel used another execution")
		}
		// A trusted integration receives its own Task value. Even accidental caller
		// mutation cannot change the frozen original request or following Observe.
		got.Task.Env["A"] = "changed"
		return cancelErr
	}
	observation := backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: ref.Instance}, Phase: backend.DataHeld}
	b.observeData = func(_ context.Context, got backend.Reference) (backend.DataObservation, error) {
		order = append(order, "observe")
		if got.ID != ref.ID || got.Task.Env["A"] != "original" {
			t.Fatal("observation adopted canceled request mutation")
		}
		return observation, observeErr
	}
	request := &dataRequest{target: &dataTarget{target: backend.Target{Backend: b}, backend: b}, job: backend.Job{CancelRequested: true}, reference: ref}
	result := runDataOperation(context.Background(), operation{kind: dataObserveOperation, data: request})
	if len(order) != 2 || order[0] != "cancel" || order[1] != "observe" || result.data.request != request || !errors.Is(result.data.cancelErr, cancelErr) || !errors.Is(result.data.err, observeErr) || !errors.Is(result.err, cancelErr) || !errors.Is(result.err, observeErr) || request.reference.Task.Env["A"] != "original" {
		t.Fatal("same original cancel/observation pair or separate errors lost")
	}
}
