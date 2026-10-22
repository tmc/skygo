package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These tests use real Store transactions and real file-authenticated Actors.
// Root geometry, mount configuration, original Create UIDs and native container
// receipts are MODELED inputs. No native runtime, transfer, filesystem census,
// original backend owner join or physical retirement is qualified by them.
// The fixed clock exercises transaction boundaries, not elapsed-time behavior.
type remoteTransactionFixture struct {
	s           *Store
	directory   string
	authPath    string
	tokenPath   string
	authCleanup func() error
	actor       auth.Actor
	engine      *auth.Engine
	authority   auth.Delegation
	now         time.Time
	policy      backend.DataPolicy
	uncertain   bool
	originals   []*Store
	cause       error
	holding     bool
}

func newRemoteTransactionFixture(t *testing.T) *remoteTransactionFixture {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-remote-transactions")
	if err = os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	directory, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	f := &remoteTransactionFixture{directory: directory, now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC), policy: backend.DefaultDataPolicy()}
	s, err := Open(filepath.Join(directory, "store"))
	if err != nil {
		f.uncertain = true
		f.cause = err
		f.hold()
		t.Fatalf("open; retained %s: %v", directory, err)
	}
	f.s = s
	f.originals = append(f.originals, s)
	// Register immediately after the original acquisition. Never remove the path
	// until its original Store has returned a positive Close; retain uncertainty.
	t.Cleanup(func() {
		if f.s != nil {
			original := f.s
			f.s = nil
			if err := original.Close(); err != nil {
				f.uncertain = true
				f.cause = errors.Join(f.cause, err)
				t.Errorf("close; retained %s: %v", directory, err)
			}
		}
		if f.uncertain || t.Failed() {
			f.hold()
			t.Logf("retained Store/auth frame: %s; cause: %v", directory, f.cause)
			return
		}
		if f.authCleanup != nil {
			if err := f.authCleanup(); err != nil {
				f.cause = errors.Join(f.cause, err)
				f.hold()
				t.Errorf("auth cleanup; retained %s: %v", directory, err)
				return
			}
		}
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove closed fixture %s: %v", directory, err)
		}
	})
	s.now = func() time.Time { return f.now }
	// Public installer; these are modeled Files-root facts, not physical proof.
	if _, err = s.InstallArtifacts(ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}); err != nil {
		t.Fatal(err)
	}
	f.authPath = filepath.Join(directory, "auth.json")
	f.tokenPath = filepath.Join(directory, "initial.token")
	f.authCleanup, err = auth.Initialize(f.authPath, f.tokenPath, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	f.authenticate(t)
	if err = s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	return f
}

// hold retains actual original Store pointers and error chains on uncertainty;
// it is not an execution owner or an attempt to retry a failed Close.
func (f *remoteTransactionFixture) hold() {
	if f.holding {
		return
	}
	f.holding = true
	wait := make(chan struct{})
	go func() { <-wait; runtime.KeepAlive(f) }()
}

func (f *remoteTransactionFixture) authenticate(t *testing.T) {
	t.Helper()
	var err error
	f.engine, err = auth.NewFile(f.authPath, f.s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(f.tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	f.actor, err = f.engine.Authenticate("Bearer " + string(bytes.TrimSuffix(raw, []byte("\n"))))
	if err != nil {
		t.Fatal(err)
	}
	f.authority, err = f.engine.Delegate(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.engine.CheckDelegation(f.authority); err != nil {
		t.Fatal(err)
	}
}

func (f *remoteTransactionFixture) restart(t *testing.T) {
	t.Helper()
	original := f.s
	f.s = nil
	if err := original.Close(); err != nil {
		f.uncertain = true
		f.cause = errors.Join(f.cause, err)
		t.Fatalf("restart Close; retained %s: %v", f.directory, err)
	}
	var err error
	f.s, err = Open(filepath.Join(f.directory, "store"))
	if err != nil {
		f.uncertain = true
		f.cause = errors.Join(f.cause, err)
		t.Fatalf("restart Open; retained %s: %v", f.directory, err)
	}
	f.originals = append(f.originals, f.s)
	f.s.now = func() time.Time { return f.now }
	f.authenticate(t)
}

func (f *remoteTransactionFixture) task(t *testing.T) backend.Task {
	t.Helper()
	// Real upload transactions with explicitly modeled file and archive receipts;
	// helper receives this fixture's genuinely authenticated Actor.
	ref := artifactTestComplete(t, f.s, f.actor)
	return backend.Task{Argv: []string{"true"}, Image: "modeled-image", Workdir: &artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}, Outputs: []string{"result.txt"}}
}

func (f *remoteTransactionFixture) submit(t *testing.T, id string, task backend.Task) backend.Job {
	t.Helper()
	f.authenticate(t)
	j, err := f.s.SubmitData(f.actor, nil, id, task, "remote", "modeled-profile-v1", f.authority, f.policy, f.now)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func (f *remoteTransactionFixture) begin(t *testing.T, j backend.Job) (RemoteKey, RemoteStart, RemoteHashes) {
	t.Helper()
	h := RemoteHashes{Input: sha256.Sum256([]byte("modeled input scope " + j.ID)), Output: sha256.Sum256([]byte("modeled output scope " + j.ID))}
	started, run, err := f.s.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), h, f.policy, f.now)
	if err != nil || !run {
		t.Fatalf("begin: run=%v err=%v", run, err)
	}
	return RemoteKey{j.ID, started.Job.CurrentAttemptID()}, started, h
}
func remoteTransactionCreated() backend.DataCreated {
	return backend.DataCreated{Instance: "modeled-pod-uid", Pod: backend.DataResource{Name: "original-pod", UID: "modeled-pod-uid"}, InputSecret: backend.DataResource{Name: "original-input", UID: "modeled-input-uid"}, OutputSecret: backend.DataResource{Name: "original-output", UID: "modeled-output-uid"}}
}
func remoteTransactionGet(t *testing.T, s *Store, k RemoteKey) RemoteAttempt {
	t.Helper()
	r, err := s.GetRemoteAttempt(k)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func remoteTransactionUnchanged(t *testing.T, s *Store, k RemoteKey, before RemoteAttempt) {
	t.Helper()
	if after := remoteTransactionGet(t, s, k); !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected transaction mutated row\nbefore: %#v\nafter: %#v", before, after)
	}
}

func TestRemoteSubmitReplayAndBeginIsolation(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	j := f.submit(t, "same", task)
	// Exact authenticated replay precedes current placement, delegation and policy
	// validation. Zero delegation is deliberately invalid for a NEW acceptance.
	again, err := f.s.SubmitData(f.actor, nil, "same", task, "", "", auth.Delegation{}, backend.DataPolicy{}, time.Time{})
	if err != nil || !reflect.DeepEqual(j, again) {
		t.Fatalf("exact replay: %v %#v", err, again)
	}
	if _, err = f.s.SubmitData(f.actor, nil, "new-invalid", task, "remote", "modeled-profile-v1", auth.Delegation{}, f.policy, f.now); err == nil {
		t.Fatal("new acceptance accepted missing delegation")
	}
	changed := backend.CloneTask(task)
	changed.Argv = []string{"false"}
	if _, err = f.s.SubmitData(f.actor, nil, "same", changed, "remote", "modeled-profile-v1", f.authority, f.policy, f.now); err == nil {
		t.Fatal("request replay accepted changed task")
	}
	if _, _, err = f.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, f.now); !errors.Is(err, ErrRemoteAttemptRequired) {
		t.Fatalf("ordinary BeginAttempt: %v", err)
	}
	acceptedKey := RemoteKey{j.ID, j.CurrentAttemptID()}
	accepted := remoteTransactionGet(t, f.s, acceptedKey)
	if _, run, err := f.s.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), RemoteHashes{}, f.policy, f.now); err == nil || run {
		t.Fatalf("missing required hashes: run=%v err=%v", run, err)
	}
	remoteTransactionUnchanged(t, f.s, acceptedKey, accepted)
	shared := sha256.Sum256([]byte("same token for different purposes"))
	if _, run, err := f.s.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), RemoteHashes{Input: shared, Output: shared}, f.policy, f.now); err == nil || run {
		t.Fatalf("shared purpose hash: run=%v err=%v", run, err)
	}
	remoteTransactionUnchanged(t, f.s, acceptedKey, accepted)
	k, start, h := f.begin(t, j)
	before := remoteTransactionGet(t, f.s, k)
	if _, err = f.s.ObserveAttempt(j.ID, k.AttemptID, backend.Status{State: backend.Running, Instance: "model"}, f.now); !errors.Is(err, ErrRemoteAttemptRequired) {
		t.Fatalf("ordinary ObserveAttempt: %v", err)
	}
	h.Output[0] ^= 1
	replay, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, h, backend.DataPolicy{}, f.now.Add(time.Hour))
	if err != nil || run || replay.Revision != start.Revision || replay.Budgets != start.Budgets {
		t.Fatalf("begin replay: %#v run=%v err=%v", replay, run, err)
	}
	remoteTransactionUnchanged(t, f.s, k, before)
	// Snapshot mutation must never modify stored hashes or scope times.
	before.Active.Output.Hash[0] ^= 1
	before.Active.Output.Expires = f.now
	if stored := remoteTransactionGet(t, f.s, k); stored.Active.Output.Hash != sha256.Sum256([]byte("modeled output scope "+j.ID)) || !stored.Active.Output.Expires.Equal(f.now.Add(120*time.Second)) {
		t.Fatal("snapshot aliases stored scope")
	}
}

func TestRemoteOriginalCreationProvenanceSurvivesRestart(t *testing.T) {
	for _, tc := range []struct {
		name     string
		success  bool
		late     bool
		revoke   bool
		creation uint8
	}{
		{"original-error-with-all-resources", false, false, false, 2},
		{"late-original-success", true, true, false, 1},
		{"activated-then-revoked", true, false, true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			j := f.submit(t, "create", f.task(t))
			k, start, h := f.begin(t, j)
			now := f.now
			if tc.late {
				now = now.Add(120 * time.Second)
			}
			c := RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: tc.success}
			row, err := f.s.RecordRemoteCreated(k, start.Revision, c, now)
			if err != nil {
				t.Fatal(err)
			}
			if tc.revoke {
				row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, now)
				if err != nil {
					t.Fatal(err)
				}
			}
			if row.Creation != tc.creation || row.Active == nil || row.Active.Phase != backend.DataHeld || row.Active.Output.State != 3 || row.Active.ClosedReason == 0 {
				t.Fatalf("provenance/closure: %#v", row)
			}
			f.restart(t)
			remoteTransactionUnchanged(t, f.s, k, row)
			replay, err := f.s.RecordRemoteCreated(k, 1, c, now.Add(time.Hour))
			if err != nil || !reflect.DeepEqual(row, replay) {
				t.Fatalf("original replay: %#v %v", replay, err)
			}
			opposite := c
			opposite.Succeeded = !c.Succeeded
			if _, err = f.s.RecordRemoteCreated(k, row.Revision, opposite, now); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("contradictory outcome: %v", err)
			}
			opposite = c
			opposite.Resources.OutputSecret.UID = "different-original-uid"
			if _, err = f.s.RecordRemoteCreated(k, row.Revision, opposite, now); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("contradictory resource: %v", err)
			}
			remoteTransactionUnchanged(t, f.s, k, row)
			if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, now); err == nil {
				t.Fatal("closed scope granted bytes")
			}
			release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, now)
			if tc.creation == 3 {
				if err != nil || release.State != 3 {
					t.Fatalf("historically activated helper abort: %#v %v", release, err)
				}
			} else if err == nil {
				t.Fatalf("unactivated original outcome gained Poll authority: %#v", release)
			}
		})
	}
}

func TestRemoteRunningIdentityAndDeadlineNeverReopen(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	task.TimeoutSeconds = 60
	j := f.submit(t, "running", task)
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	began := f.now.Add(time.Second)
	observation := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: began}}
	row, err = f.s.ObserveRemote(k, row.Revision, observation, began)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, id string
		when     time.Time
	}{{"identity", "different-main", began}, {"timestamp", "modeled-main", began.Add(time.Nanosecond)}} {
		t.Run(tc.name, func(t *testing.T) {
			bad := observation
			bad.TaskStart = &backend.ContainerStart{ContainerID: tc.id, StartedAt: tc.when}
			if _, err := f.s.ObserveRemote(k, row.Revision, bad, began); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("mismatched start: %v", err)
			}
			remoteTransactionUnchanged(t, f.s, k, row)
		})
	}
	deadline := began.Add(60 * time.Second)
	row, err = f.s.ObserveRemote(k, row.Revision, observation, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.ClosedReason != RemotePhaseTimeout || row.Active.Output.State != 3 || row.Creation != 3 {
		t.Fatalf("deadline closure: %#v", row)
	}
	row, err = f.s.ObserveRemote(k, row.Revision, observation, deadline.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.Output.State != 3 || row.Active.ClosedReason != RemotePhaseTimeout {
		t.Fatal("observation reopened timeout")
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, deadline); err == nil {
		t.Fatal("deadline granted bytes")
	}
	release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, deadline)
	if err != nil || release.State != 3 {
		t.Fatalf("timeout abort: %#v %v", release, err)
	}
}

func remoteTransactionStop(f *remoteTransactionFixture, code int32) (backend.DataTaskStop, backend.ContainerExit) {
	stop := backend.DataTaskStop{Stage: &backend.ContainerExit{ContainerID: "modeled-stage", ExitCode: 0, StartedAt: f.now.Add(time.Millisecond), FinishedAt: f.now.Add(500 * time.Millisecond)}, Task: backend.ContainerExit{ContainerID: "modeled-main", ExitCode: code, StartedAt: f.now.Add(time.Second), FinishedAt: f.now.Add(2 * time.Second)}}
	collector := backend.ContainerExit{ContainerID: "modeled-collector", ExitCode: 0, StartedAt: f.now.Add(time.Second), FinishedAt: f.now.Add(3 * time.Second)}
	return stop, collector
}

func TestRemoteFullProofCannotAckBeforeCompletion(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "full-proof", f.task(t))
	k, start, _ := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 1)
	code := int32(1)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	if err = f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(5*time.Second)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("full proof precompletion Ack: %v", err)
	}
	remoteTransactionUnchanged(t, f.s, k, row)
}

func TestRemotePartialSecretAckKeepsUnknownPodCharged(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	j := f.submit(t, "partial", task)
	k, start, _ := f.begin(t, j)
	partial := backend.DataCreated{OutputSecret: remoteTransactionCreated().OutputSecret}
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: partial, Succeeded: false}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if row.Resources.Pod != (backend.DataResource{}) || row.Creation != 2 {
		t.Fatalf("partial create: %#v", row)
	}
	if err = f.s.AckRemoteRemoval(k, row.Revision, backend.DataRemoval{PodRemoved: true, OutputSecret: partial.OutputSecret, OutputRemoved: true}, f.now); err == nil {
		t.Fatal("unknown Pod acquired positive removal")
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	if err = f.s.AckRemoteRemoval(k, row.Revision, backend.DataRemoval{OutputSecret: partial.OutputSecret, OutputRemoved: true}, f.now); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	if row.Ack != 4 || row.State == 4 || row.Resources.Pod != (backend.DataResource{}) {
		t.Fatalf("partial Ack falsely retired unknown execution: %#v", row)
	}
	// Public accepting transactions demonstrate that the partial record still
	// occupies one of 64 active slots even after its only known Secret is gone.
	var queued backend.Job
	for i := 0; i < 63; i++ {
		accepted := f.submit(t, fmt.Sprintf("charged-%02d", i), task)
		if i == 0 {
			queued = accepted
		}
	}
	f.authenticate(t)
	_, err = f.s.SubmitData(f.actor, nil, "must-exceed-active", task, "remote", "modeled-profile-v1", f.authority, f.policy, f.now)
	var capacity *ArtifactCapacityError
	if !errors.As(err, &capacity) || capacity.Domain != "workspaces" {
		t.Fatalf("unknown Pod lost active charge: %v", err)
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	// Initial queued cancellation must still settle while the active quota is
	// full, releasing exactly its own unit without granting credit for unknown Pod.
	if _, err = f.s.UpdateFor(f.actor, queued.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
	canceled, run, err := f.s.BeginRemoteAttempt(queued.ID, queued.CurrentAttemptID(), RemoteHashes{}, backend.DataPolicy{}, f.now)
	if err != nil || run || canceled.Job.Status.State != backend.Canceled {
		t.Fatalf("full-quota initial cancel: %#v %v %v", canceled, run, err)
	}
	f.submit(t, "replacement-after-never-started", task)
	remoteTransactionUnchanged(t, f.s, k, row)
}

func TestRemoteCompletionReplayAfterRetryAndProofCompaction(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}
	j := f.submit(t, "retry-completion", task)
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	// The original complete input transfer goes through the real Store sequence;
	// transfer and owner joins are MODELED. A Workdir retry requires it.
	grant, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.s.BeginRemoteInputRead(grant, OriginalServerLifetime{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.FinishRemoteInput(grant, closing, f.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	stop, collector := remoteTransactionStop(f, 1)
	code := int32(1)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	completion := RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector}
	completed, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status.State != backend.Retrying || completed.NextAttemptAt == nil {
		t.Fatalf("main exit did not schedule retry: %#v", completed)
	}
	row = remoteTransactionGet(t, f.s, k)
	if row.State != 3 || row.Active != nil || row.Done == nil || row.Done.OriginalExit == nil || *row.Done.OriginalExit != 1 {
		t.Fatalf("completion: %#v", row)
	}
	// Positive retirement inputs remain MODELED native-owner handoffs. The test
	// checks exact UID matching/order, not whether any native resource was removed.
	wrong := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	wrong.Pod.UID = "different-original-pod"
	if err = f.s.AckRemoteRemoval(k, row.Revision, wrong, f.now.Add(4*time.Second)); err == nil {
		t.Fatal("retirement accepted another Pod UID")
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	if err = f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	retired := remoteTransactionGet(t, f.s, k)
	if retired.State != 4 || retired.Ack != 7 || retired.Done.Stop != nil || retired.Done.Collector != nil {
		t.Fatalf("proof compaction: %#v", retired)
	}
	// A repeated original retirement is an idempotent receipt even with its old
	// revision. No new UID, byte grant or original native call is introduced.
	if err = f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	next, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, h, f.policy, *completed.NextAttemptAt)
	if err != nil || !run || next.Job.CurrentAttemptID() == k.AttemptID {
		t.Fatalf("next attempt: %#v %v %v", next, run, err)
	}
	nextKey := RemoteKey{j.ID, next.Job.CurrentAttemptID()}
	nextBefore := remoteTransactionGet(t, f.s, nextKey)
	// The old attempt's output reservation has already been discarded and its
	// proofs compacted. Duplicate lookup must precede both those gates and the
	// current-attempt gate, returning the current core Job without rewinding it.
	replay, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(24*time.Hour))
	if err != nil || !reflect.DeepEqual(replay, next.Job) {
		t.Fatalf("old semantic duplicate rewound retry: %#v %v", replay, err)
	}
	remoteTransactionUnchanged(t, f.s, k, retired)
	remoteTransactionUnchanged(t, f.s, nextKey, nextBefore)
	changed := completion
	changedStop := stop
	changedStop.Task.ExitCode = 2
	changed.Stop = &changedStop
	if _, err = f.s.CompleteRemoteAttempt(k, changed, f.now.Add(24*time.Hour)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("compacted decision accepted changed exit: %v", err)
	}
	remoteTransactionUnchanged(t, f.s, k, retired)
	remoteTransactionUnchanged(t, f.s, nextKey, nextBefore)
}

func TestRemoteInitialCancellationOrdering(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, mountOnly := range []bool{false, true} {
			for _, beginFirst := range []bool{false, true} {
				name := fmt.Sprintf("retry=%v/mount-only=%v/begin-first=%v", retry, mountOnly, beginFirst)
				t.Run(name, func(t *testing.T) {
					f := newRemoteTransactionFixture(t)
					task := backend.Task{Argv: []string{"true"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}}
					if !mountOnly {
						task = f.task(t)
					}
					if retry {
						task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 3600, ExitCodes: []int32{1}}
					}
					j := f.submit(t, "initial-cancel", task)
					k := RemoteKey{j.ID, j.CurrentAttemptID()}
					h := RemoteHashes{}
					if !mountOnly {
						h.Input = sha256.Sum256([]byte("modeled input"))
						h.Output = sha256.Sum256([]byte("modeled output"))
					}
					if beginFirst {
						if _, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, h, f.policy, f.now); err != nil || !run {
							t.Fatalf("begin first: %v %v", run, err)
						}
					}
					before := remoteTransactionGet(t, f.s, k)
					f.authenticate(t)
					canceled, err := f.s.UpdateFor(f.actor, j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil })
					if err != nil {
						t.Fatal(err)
					}
					// A stale expected ID never settles even a queued canceled job.
					stale, run, err := f.s.BeginRemoteAttempt(j.ID, "not-the-attempt", RemoteHashes{}, backend.DataPolicy{}, f.now)
					if err != nil || run || !reflect.DeepEqual(stale.Job, canceled) {
						t.Fatalf("stale cancellation: %#v %v %v", stale, run, err)
					}
					remoteTransactionUnchanged(t, f.s, k, before)
					// Cancellation alone cannot fabricate a stopped native execution, including
					// one with an UNKNOWN original Pod/StartData return after launch intent.
					if _, err = f.s.CompleteRemoteAttempt(k, RemoteCompletion{Outcome: DataCanceled}, f.now); err == nil {
						t.Fatal("empty proof adopted queued/unknown-active cancellation")
					}
					remoteTransactionUnchanged(t, f.s, k, before)
					result, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, h, backend.DataPolicy{}, f.now)
					if err != nil || run {
						t.Fatalf("cancel reconciliation: %v %v", run, err)
					}
					row := remoteTransactionGet(t, f.s, k)
					if beginFirst {
						if result.Job.Status.State == backend.Canceled || row.State != 2 || row.Done != nil || row.Active == nil || row.Creation != 0 {
							t.Fatalf("active unknown execution became never-started: %#v %#v", result, row)
						}
						remoteTransactionUnchanged(t, f.s, k, before)
					} else {
						if result.Job.Status.State != backend.Canceled || result.Job.Status.Instance != "" || result.Job.Status.ExitCode != nil || result.Job.Status.Termination != backend.UnknownTermination || result.Job.NextAttemptAt != nil {
							t.Fatalf("queued cancellation fabricated execution: %#v", result.Job)
						}
						if row.State != 4 || row.Creation != 0 || row.Ack != 0 || row.Active != nil || row.Resources != (backend.DataCreated{}) || row.Done == nil || row.Done.Outcome != DataCanceled || row.Done.Stop != nil || row.Done.NoLaunch != nil || row.Done.Collector != nil || row.Done.OriginalExit != nil || row.Done.ResultID != "" {
							t.Fatalf("never-started shape: %#v", row)
						}
						for _, a := range result.Job.Attempts {
							if a.StartingAt != nil || a.Status.Instance != "" || a.Status.ExitCode != nil {
								t.Fatalf("never-started history has execution: %#v", a)
							}
						}
						f.restart(t)
						remoteTransactionUnchanged(t, f.s, k, row)
						replay, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, RemoteHashes{}, backend.DataPolicy{}, f.now.Add(time.Hour))
						if err != nil || run || !reflect.DeepEqual(replay.Job, result.Job) {
							t.Fatalf("never-started replay: %#v %v %v", replay, run, err)
						}
						remoteTransactionUnchanged(t, f.s, k, row)
					}
				})
			}
		}
	}
}

func TestRemoteRetryWaitingCancellationPreservesDecision(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 3600, ExitCodes: []int32{1}}
	j := f.submit(t, "retry-wait-cancel", task)
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	// The original complete input transfer goes through the real Store sequence;
	// transfer and owner joins are MODELED. A Workdir retry requires it.
	grant, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.s.BeginRemoteInputRead(grant, OriginalServerLifetime{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.FinishRemoteInput(grant, closing, f.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	stop, collector := remoteTransactionStop(f, 1)
	code := int32(1)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	completion := RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector}
	waiting, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Status.State != backend.Retrying || waiting.NextAttemptAt == nil {
		t.Fatalf("not retry-waiting: %#v", waiting)
	}
	prior := remoteTransactionGet(t, f.s, k)
	history := append([]backend.Attempt(nil), waiting.Attempts...)
	f.authenticate(t)
	if _, err = f.s.UpdateFor(f.actor, j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
	// Cancel before the future deadline, with deliberately unusable new-launch
	// policy/hashes: this path neither allocates nor authorizes another attempt.
	result, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, RemoteHashes{}, backend.DataPolicy{}, f.now.Add(5*time.Second))
	if err != nil || run || result.Job.Status.State != backend.Canceled || result.Job.NextAttemptAt != nil || !reflect.DeepEqual(result.Job.Attempts, history) {
		t.Fatalf("retry waiting cancellation: %#v %v %v", result, run, err)
	}
	remoteTransactionUnchanged(t, f.s, k, prior)
	f.restart(t)
	remoteTransactionUnchanged(t, f.s, k, prior)
	replay, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(24*time.Hour))
	if err != nil || !reflect.DeepEqual(replay, result.Job) {
		t.Fatalf("prior completion after waiting cancellation: %#v %v", replay, err)
	}
	remoteTransactionUnchanged(t, f.s, k, prior)
}

func (f *remoteTransactionFixture) enableRetention(t *testing.T) RetentionInfo {
	t.Helper()
	original := f.s
	f.s = nil
	if err := original.Close(); err != nil {
		f.uncertain = true
		f.cause = errors.Join(f.cause, err)
		t.Fatalf("close before retention; retained %s: %v", f.directory, err)
	}
	policy := DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	info, err := EnableRetention(filepath.Join(f.directory, "store"), policy)
	if err != nil {
		f.uncertain = true
		f.cause = errors.Join(f.cause, err)
		t.Fatalf("enable retention; retained %s: %v", f.directory, err)
	}
	f.s, err = Open(filepath.Join(f.directory, "store"))
	if err != nil {
		f.uncertain = true
		f.cause = errors.Join(f.cause, err)
		t.Fatalf("reopen after retention; retained %s: %v", f.directory, err)
	}
	f.originals = append(f.originals, f.s)
	// EnableRetention creates its domain using its real installation clock. Keep
	// subsequent modeled transaction times within that actual accepted domain.
	f.now = info.AcceptUntil.Add(-time.Hour).UTC()
	f.s.now = func() time.Time { return f.now }
	f.authenticate(t)
	current, err := f.s.RetentionInfo(f.actor)
	if err != nil || !current.Enabled || current.Domain != info.Domain {
		t.Fatalf("retention reopen: %#v %v", current, err)
	}
	return current
}

func TestRemoteRetainedDomainReplayAndAdmissionRollback(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	info := f.enableRetention(t)
	task := f.task(t)
	before, err := f.s.RetentionInfo(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.SubmitData(f.actor, &info.Domain, "new-invalid-delegation", task, "remote", "modeled-profile-v1", auth.Delegation{}, f.policy, f.now); err == nil {
		t.Fatal("retained acceptance accepted missing delegation")
	}
	after, err := f.s.RetentionInfo(f.actor)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("invalid retained acceptance charged quota: %#v %v", after, err)
	}
	// Reusing the SAME request after the rejected admission proves the failure
	// left no accepted request/job relation, as well as no quota charge.
	f.authenticate(t)
	j, err := f.s.SubmitData(f.actor, &info.Domain, "new-invalid-delegation", task, "remote", "modeled-profile-v1", f.authority, f.policy, f.now)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := f.s.RetentionInfo(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.s.SubmitData(f.actor, &info.Domain, "new-invalid-delegation", task, "", "", auth.Delegation{}, backend.DataPolicy{}, time.Time{})
	if err != nil || !reflect.DeepEqual(replay, j) {
		t.Fatalf("retained replay ran new placement/delegation gates: %#v %v", replay, err)
	}
	after, err = f.s.RetentionInfo(f.actor)
	if err != nil || !reflect.DeepEqual(after, accepted) {
		t.Fatalf("retained duplicate changed accounting: %#v %v", after, err)
	}
	f.restart(t)
	replay, err = f.s.SubmitData(f.actor, &info.Domain, "new-invalid-delegation", task, "", "", auth.Delegation{}, backend.DataPolicy{}, time.Time{})
	if err != nil || !reflect.DeepEqual(replay, j) {
		t.Fatalf("retained restart replay: %#v %v", replay, err)
	}
	key := RemoteKey{j.ID, j.CurrentAttemptID()}
	original := remoteTransactionGet(t, f.s, key)
	changed := backend.CloneTask(task)
	changed.Argv = []string{"different"}
	if _, err = f.s.SubmitData(f.actor, &info.Domain, "new-invalid-delegation", changed, "remote", "modeled-profile-v1", f.authority, f.policy, f.now); err == nil {
		t.Fatal("retained replay accepted changed task")
	}
	remoteTransactionUnchanged(t, f.s, key, original)
}

func remoteTransactionClosure(owner string, lease ArtifactLease) ArtifactDownloadClosure {
	return ArtifactDownloadClosure{Owner: owner, LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID, Producer: lease.Producer}
}

func TestRemoteScopedTransferPublishExpiryAndGCReplay(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	info := f.enableRetention(t)
	task := f.task(t)
	j, err := f.s.SubmitData(f.actor, &info.Domain, "scoped-lifecycle", task, "remote", "modeled-profile-v1", f.authority, f.policy, f.now)
	if err != nil {
		t.Fatal(err)
	}
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	input, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	// The optional producer is unavailable rather than a fabricated host/process.
	// File/response ownership and successful original Close are MODELED handoffs:
	// no real HTTP, FD, sync or native-container qualification follows this test.
	partial, err := f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	partialClosing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), partial))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.FinishArtifactDownload(partialClosing, f.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	if row.Active.Input.State != 2 {
		t.Fatal("ordinary partial response closure declared complete input")
	}
	partialBefore := remoteTransactionDatabaseBytes(t, f.s)
	if err = f.s.FinishRemoteInput(input, partialClosing, f.now.Add(time.Second)); err == nil {
		t.Fatal("ordinary closed partial response upgraded to normal completion")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, partialBefore)
	input, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	full, err := f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	original := remoteTransactionClosure(f.actor.Principal(), full)
	before := remoteTransactionGet(t, f.s, k)
	if err = f.s.FinishRemoteInput(input, original, f.now.Add(2*time.Second)); err == nil {
		t.Fatal("unjoined input bypassed closing identity")
	}
	remoteTransactionUnchanged(t, f.s, k, before)
	closing, err := f.s.MarkArtifactDownloadClosing(original)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.s.FinishRemoteInput(input, closing, f.now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	if row.Active.Input.State != 4 {
		t.Fatal("complete original input handoff lost normal-completion provenance")
	}
	if err = f.s.FinishRemoteInput(input, closing, f.now.Add(3*time.Second)); err != nil {
		t.Fatalf("exact input closure replay: %v", err)
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now.Add(3*time.Second)); err == nil {
		t.Fatal("finished input reacquired byte grant")
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, f.now); err == nil {
		t.Fatal("collector uploaded before successful main stop")
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, f.now.Add(3*time.Second))
	if err != nil || release.State != 2 || release.TaskContainerID != stop.Task.ContainerID || release.TaskExitCode == nil || *release.TaskExitCode != 0 {
		t.Fatalf("collector release: %#v %v", release, err)
	}
	output, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	upload, err := f.s.BeginRemoteOutput(output, strings.Repeat("d", 64), 1, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if upload.Reference.ID != row.Active.ResultID {
		t.Fatal("output replaced original reserved result identity")
	}
	// These are modeled ORIGINAL server part/publication/archive receipts, not
	// local Pod inode adoption and not actual Files IO performed by this test.
	file := ArtifactFile{Device: 17, Inode: 800}
	upload, err = f.s.RemotePartCreated(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	upload, err = f.s.RemoteChunkSynced(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, file, 0, 1, strings.Repeat("e", 64), f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	upload, err = f.s.BeginRemoteSeal(output, ArtifactCursor{upload.Reference.ID, upload.Revision}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	seal := RemoteOutputSeal{Reference: upload.Reference, File: file, Manifest: ArtifactManifest{Reference: upload.Reference, ExpandedBytes: 1, RegularFiles: 1}}
	cursor := ArtifactCursor{upload.Reference.ID, upload.Revision}
	if err = f.s.SealRemoteOutput(output, cursor, seal, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	result, err := f.s.ArtifactResultFor(f.actor, j.ID, k.AttemptID)
	if err != nil || result.State != "absent" || result.Reference != nil {
		t.Fatalf("private seal published result: %#v %v", result, err)
	}
	if _, err = f.s.BeginArtifactDownload(f.actor, seal.Reference, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private seal granted owner bytes: %v", err)
	}
	completion := RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector, Candidate: &seal}
	completed, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(4*time.Second))
	if err != nil || completed.Status.State != backend.Succeeded {
		t.Fatalf("publish: %#v %v", completed, err)
	}
	result, err = f.s.ArtifactResultFor(f.actor, j.ID, k.AttemptID)
	if err != nil || result.State != "available" || result.Reference == nil || *result.Reference != seal.Reference {
		t.Fatalf("completed result: %#v %v", result, err)
	}
	row = remoteTransactionGet(t, f.s, k)
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	if err = f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	pruneAt := f.now.Add(25 * time.Hour) // selected result remains pinned past its initial TTL
	selection := PruneSelection{Before: pruneAt.Add(time.Second), Mode: ResultOnly, Attempt: 1, Max: j.Sequence, Limit: 1}
	page, err := f.s.Prune(f.actor, selection, true, nil, nil, pruneAt)
	if err != nil || page.Marked != 1 {
		t.Fatalf("ResultOnly: %#v %v", page, err)
	}
	result, err = f.s.ArtifactResultFor(f.actor, j.ID, k.AttemptID)
	if err != nil || result.State != "expired" || result.Reference != nil {
		t.Fatalf("expired result witness: %#v %v", result, err)
	}
	if _, err = f.s.BeginArtifactDownload(f.actor, seal.Reference, OriginalServerLifetime{}); err == nil {
		t.Fatal("cached result reference restored revoked grant")
	}
	replay, err := f.s.CompleteRemoteAttempt(k, completion, pruneAt.Add(time.Second))
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("semantic duplicate after ResultOnly: %#v %v", replay, err)
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	// Parent explicitly authorized this EXISTING real private maintenance Store
	// transaction because public RemoveArtifact requires a grant that ResultOnly
	// has revoked. It is not a raw DB seed, replacement API or execution shortcut.
	operation := strings.Repeat("f", 32)
	deadline := pruneAt.Add(time.Hour) // release grace now dominates the expired initial TTL
	if err = f.s.artifactUpdate(func(st *artifactState) error {
		return artifactMarkObjectDelete(st, seal.Reference.ID, operation, deadline.Add(-time.Nanosecond))
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("GC bypassed original deletion floor: %v", err)
	}
	if err = f.s.artifactUpdate(func(st *artifactState) error {
		return artifactMarkObjectDelete(st, seal.Reference.ID, operation, deadline)
	}); err != nil {
		t.Fatal(err)
	}
	work, found, err := f.s.NextArtifactCleanup("")
	if err != nil || !found || work.Delete == nil || work.Delete.Ref != seal.Reference || work.Delete.File != file {
		t.Fatalf("GC work: %#v %v %v", work, found, err)
	}
	// Receipt models original unlink+directory Sync after actual owner joins.
	// Only the transaction/accounting consequence is tested here.
	retired, err := f.s.FinishArtifactDelete(*work.Delete, ArtifactDeleteReceipt{Work: *work.Delete})
	if err != nil || !retired {
		t.Fatalf("GC finish: %v %v", retired, err)
	}
	f.restart(t)
	replay, err = f.s.CompleteRemoteAttempt(k, completion, deadline.Add(time.Hour))
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("semantic duplicate after actual metadata GC/reopen: %#v %v", replay, err)
	}
	remoteTransactionUnchanged(t, f.s, k, row)
	result, err = f.s.ArtifactResultFor(f.actor, j.ID, k.AttemptID)
	if err != nil || result.State != "expired" || result.Reference != nil {
		t.Fatalf("GC replay resurrected result: %#v %v", result, err)
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, deadline); err == nil {
		t.Fatal("GC replay resurrected scope")
	}
}

// These read-only helpers inspect actual committed Store rows. They create no
// records, synthetic delete intent, fixture seed or physical/native authority.
func remoteTransactionDeleteWork(t *testing.T, s *Store, operation string) ArtifactDeleteWork {
	t.Helper()
	var work ArtifactDeleteWork
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		deletion, ok := st.Deletes[operation]
		if !ok {
			return ErrNotFound
		}
		work, err = artifactDeleteWork(st, operation, deletion)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return work
}
func remoteTransactionWholeReady(t *testing.T, s *Store, id string) bool {
	t.Helper()
	var ready bool
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		j, err := get(tx, id)
		if err != nil {
			return err
		}
		ready = artifactWholeReady(st, j)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return ready
}

// Snapshot logical bucket entries and bucket sequence counters, not bbolt's
// physical pages/transaction IDs: a successful no-op Update may advance those.
func remoteTransactionDatabaseBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	type entry struct {
		Path       string
		Key, Value []byte
		Sequence   uint64
		Bucket     bool
	}
	var entries []entry
	var walk func(*bolt.Bucket, string) error
	walk = func(b *bolt.Bucket, path string) error {
		entries = append(entries, entry{Path: path, Sequence: b.Sequence(), Bucket: true})
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				return walk(b.Bucket(k), path+"/"+fmt.Sprintf("%x", k))
			}
			entries = append(entries, entry{Path: path, Key: bytes.Clone(k), Value: bytes.Clone(v)})
			return nil
		})
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error { return walk(b, fmt.Sprintf("%x", name)) })
	}); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
func remoteTransactionDatabaseUnchanged(t *testing.T, s *Store, before []byte) {
	t.Helper()
	if !bytes.Equal(before, remoteTransactionDatabaseBytes(t, s)) {
		t.Fatal("refused/replayed transaction changed committed metadata or charges")
	}
}

func TestRemoteLateKnownOutputCreateRetainsCleanupUntilCompletion(t *testing.T) {
	for _, ackBeforeGC := range []bool{false, true} {
		t.Run(fmt.Sprintf("native-ack-before-gc=%v", ackBeforeGC), func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			task := f.task(t)
			j := f.submit(t, "late-known-create", task)
			k, start, h := f.begin(t, j)
			row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			stop, collector := remoteTransactionStop(f, 0)
			zero := int32(0)
			row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			grant, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, f.now.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			upload, err := f.s.BeginRemoteOutput(grant, strings.Repeat("d", 64), 1, f.now.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, f.now.Add(4*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			// The original server's exclusive Create returned this MODELED known File
			// only after revocation. Its original FD Close/HTTP owners have positively
			// joined before Abandon. Field equality is not that real ownership proof.
			file := ArtifactFile{Device: 17, Inode: 900}
			cursor := ArtifactCursor{upload.Reference.ID, upload.Revision}
			before := remoteTransactionDatabaseBytes(t, f.s)
			if _, err = f.s.RemotePartCreated(grant, cursor, file, f.now.Add(5*time.Second)); err == nil {
				t.Fatal("late part restored revoked writable admission")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			completion := RemoteCompletion{Outcome: DataCollectionFailure, Stop: &stop, Collector: &collector}
			if _, err = f.s.CompleteRemoteAttempt(k, completion, f.now.Add(5*time.Second)); err == nil {
				t.Fatal("unknown creating File0 became absent output")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			original := ArtifactCreateAbandonment{Owner: f.actor.Principal(), Cursor: cursor, Ref: upload.Reference, File: file, RootID: strings.Repeat("a", 32)}
			for _, name := range []string{"root", "file-device", "reference", "cursor", "unknown-file"} {
				t.Run(name, func(t *testing.T) {
					bad := original
					switch name {
					case "root":
						bad.RootID = strings.Repeat("b", 32)
					case "file-device":
						bad.File.Device++
					case "reference":
						bad.Ref.SHA256 = strings.Repeat("e", 64)
					case "cursor":
						bad.Cursor.Revision++
					case "unknown-file":
						bad.File = ArtifactFile{}
					}
					if _, err := f.s.AbandonArtifactCreate(bad); err == nil {
						t.Fatalf("invalid late original handoff accepted: %s", name)
					}
					remoteTransactionDatabaseUnchanged(t, f.s, before)
				})
			}
			operation, err := f.s.AbandonArtifactCreate(original)
			if err != nil || operation == "" {
				t.Fatalf("late known create abandonment: %s %v", operation, err)
			}
			actual := remoteTransactionDeleteWork(t, f.s, operation)
			if actual.Kind != "upload" || actual.Ref != upload.Reference || actual.File != file || actual.Revision != cursor.Revision+1 {
				t.Fatalf("late original cleanup intent: %#v", actual)
			}
			// The only removed object must be the original no-File reserved envelope;
			// the exact original known part/upload/intent remain durably linked/charged.
			if err = f.s.db.View(func(tx *bolt.Tx) error {
				st, err := artifactLoad(tx)
				if err != nil {
					return err
				}
				if _, exists := st.Objects[upload.Reference.ID]; exists {
					t.Fatal("abandon retained the unmaterialized remote reservation object")
				}
				u, exists := st.Uploads[upload.Reference.ID]
				if !exists || u.RemoteAttemptID != k.AttemptID || u.State != "deleting" || u.Part != file || u.Ref != original.Ref {
					t.Fatal("abandon lost actual original part linkage")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			before = remoteTransactionDatabaseBytes(t, f.s)
			replay, err := f.s.AbandonArtifactCreate(original)
			if err != nil || replay != operation {
				t.Fatalf("exact abandonment replay changed intent: %s %v", replay, err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			bad := original
			bad.File.Inode++
			if _, err = f.s.AbandonArtifactCreate(bad); err == nil {
				t.Fatal("abandon replay substituted known original inode")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			f.restart(t)
			replay, err = f.s.AbandonArtifactCreate(original)
			if err != nil || replay != operation {
				t.Fatalf("reopened original abandonment: %s %v", replay, err)
			}
			if reopened := remoteTransactionDeleteWork(t, f.s, operation); reopened != actual {
				t.Fatal("restart reconstructed another cleanup identity")
			}
			// No active deferred remote intent is physically dispatched or directly
			// finished, even when supplied its actual committed original Work snapshot.
			before = remoteTransactionDatabaseBytes(t, f.s)
			if work, found, err := f.s.NextArtifactCleanup(""); err != nil || found {
				t.Fatalf("State2 intent dispatched: %#v %v %v", work, found, err)
			}
			retired, err := f.s.FinishArtifactDelete(actual, ArtifactDeleteReceipt{Work: actual})
			if err == nil || retired {
				t.Fatalf("State2 direct finish credited original part: %v %v", retired, err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			if remoteTransactionWholeReady(t, f.s, j.ID) {
				t.Fatal("State2 declared Whole-ready")
			}
			// A real ordinary upload provides eligible work beside the deferred remote
			// row. Selection must skip/wrap fairly, rather than stop at the remote row.
			ordinary, err := f.s.BeginArtifactUpload(f.actor, strings.Repeat("c", 64), 1)
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err = f.s.ArtifactPartCreated(f.actor, ArtifactCursor{ordinary.Reference.ID, ordinary.Revision}, ArtifactFile{Device: 17, Inode: 901})
			if err != nil {
				t.Fatal(err)
			}
			ordinaryOperation, err := f.s.CancelArtifactUpload(f.actor, ArtifactCursor{ordinary.Reference.ID, ordinary.Revision})
			if err != nil {
				t.Fatal(err)
			}
			work, found, err := f.s.NextArtifactCleanup(operation)
			if err != nil || !found || work.Delete == nil || work.Delete.ID != ordinaryOperation {
				t.Fatalf("deferred remote blocked fair ordinary cleanup: %#v %v %v", work, found, err)
			}
			// MODELED original ordinary upload unlink+parent Sync, not actual file IO.
			retired, err = f.s.FinishArtifactDelete(*work.Delete, ArtifactDeleteReceipt{Work: *work.Delete})
			if err != nil || !retired {
				t.Fatalf("ordinary cleanup: %v %v", retired, err)
			}
			completed, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(6*time.Second))
			if err != nil || completed.Status.State != backend.Failed || completed.Status.ExitCode != nil || completed.Status.Termination != backend.UnknownTermination {
				t.Fatalf("explicit collection failure: %#v %v", completed, err)
			}
			row = remoteTransactionGet(t, f.s, k)
			if row.State != 3 || row.Done == nil || row.Done.ResultID != upload.Reference.ID || row.Done.Outcome != DataCollectionFailure {
				t.Fatalf("completion lost original cleanup/output identity: %#v", row)
			}
			if retained := remoteTransactionDeleteWork(t, f.s, operation); retained != actual {
				t.Fatal("completion replaced original abandonment intent")
			}
			if remoteTransactionWholeReady(t, f.s, j.ID) {
				t.Fatal("State3 native obligation declared Whole-ready")
			}
			ack := func() {
				t.Helper()
				row = remoteTransactionGet(t, f.s, k)
				removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
				if err := f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(7*time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			if ackBeforeGC {
				ack()
				if remoteTransactionWholeReady(t, f.s, j.ID) {
					t.Fatal("native State4 discarded reverse-linked artifact intent")
				}
			}
			work, found, err = f.s.NextArtifactCleanup("")
			if err != nil || !found || work.Delete == nil || *work.Delete != actual {
				t.Fatalf("completed original intent not eligible: %#v %v %v", work, found, err)
			}
			// MODELED original remote-output part unlink+Sync only after completion and
			// all original native/server owners joined. Real metadata GC follows here.
			retired, err = f.s.FinishArtifactDelete(actual, ArtifactDeleteReceipt{Work: actual})
			if err != nil || !retired {
				t.Fatalf("completed original part cleanup: %v %v", retired, err)
			}
			if !ackBeforeGC {
				if remoteTransactionWholeReady(t, f.s, j.ID) {
					t.Fatal("artifact GC bypassed native State3 obligation")
				}
				ack()
			}
			if !remoteTransactionWholeReady(t, f.s, j.ID) {
				t.Fatal("native State4 with no artifact reverse joins remains blocked")
			}
			f.restart(t)
			replayJob, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(8*time.Second))
			if err != nil || !reflect.DeepEqual(replayJob, completed) {
				t.Fatalf("late create completion replay after GC: %#v %v", replayJob, err)
			}
			if !remoteTransactionWholeReady(t, f.s, j.ID) {
				t.Fatal("reopen restored a retired artifact reverse join")
			}
		})
	}
}

func TestRemoteInputNormalCompletionSurvivesExpiryAndRevocation(t *testing.T) {
	for _, revokeFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("revoke-first=%v", revokeFirst), func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			j := f.submit(t, "normal-input", f.task(t))
			k, start, h := f.begin(t, j)
			row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			input, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
			if err != nil {
				t.Fatal(err)
			}
			// This is the SAME original reader/HTTP/FD lifetime. The successful normal
			// transfer and positive original Close/owner joins are MODELED handoffs,
			// not inferred from identity, EOF, cancellation or a copied scalar state.
			if revokeFirst {
				row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, f.now.Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				before := remoteTransactionDatabaseBytes(t, f.s)
				if err = f.s.FinishRemoteInput(input, closing, f.now.Add(2*time.Second)); err == nil {
					t.Fatal("revoke-first committed normal input completion")
				}
				remoteTransactionDatabaseUnchanged(t, f.s, before)
				if row.Active.Input.State != 3 {
					t.Fatalf("revoked input became normal complete: %#v", row.Active.Input)
				}
				return
			}
			if err = f.s.FinishRemoteInput(input, closing, f.now.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			row = remoteTransactionGet(t, f.s, k)
			if row.Active.Input.State != 4 {
				t.Fatalf("normal input provenance: %#v", row.Active.Input)
			}
			originalScope := *row.Active.Input
			before := remoteTransactionDatabaseBytes(t, f.s)
			if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now.Add(2*time.Second)); err == nil {
				t.Fatal("normal-complete input hash granted new bytes")
			}
			if _, err = f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now.Add(2*time.Second)); err == nil {
				t.Fatal("normal-complete input admitted another reader")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			// Keep ONLY the still-open output scope renewed across the old input's
			// original120s expiry. Repeated actual Running receipts preserve one start.
			observation := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: f.now.Add(2 * time.Second)}}
			for _, seconds := range []int64{30, 60, 90, 121} {
				row, err = f.s.ObserveRemote(k, row.Revision, observation, f.now.Add(time.Duration(seconds)*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if *row.Active.Input != originalScope {
					t.Fatalf("normal input identity/expiry renewed at %ds", seconds)
				}
				if row.Active.ClosedReason != 0 || row.Active.Output.State != 2 || !row.Active.Output.Expires.After(f.now.Add(time.Duration(seconds)*time.Second)) {
					t.Fatalf("normal input's old expiry revoked active output at %ds: %#v", seconds, row.Active)
				}
			}
			// Completion history is retained only while Active exists; these are all
			// BEFORE semantic Complete, with no promise after Complete or Whole/GC.
			row, err = f.s.RevokeRemote(k, row.Revision, RemoteAuthorityLost, f.now.Add(122*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if *row.Active.Input != originalScope || row.Active.Output.State != 3 || row.Active.ClosedReason != RemoteAuthorityLost {
				t.Fatalf("revoke changed normal completion identity: %#v", row.Active)
			}
			before = remoteTransactionDatabaseBytes(t, f.s)
			if err = f.s.FinishRemoteInput(input, closing, f.now.Add(123*time.Second)); err != nil {
				t.Fatalf("exact original normal closure replay after revoke/old expiry: %v", err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			badGrant := input
			badGrant.Epoch++
			if err = f.s.FinishRemoteInput(badGrant, closing, f.now.Add(123*time.Second)); err == nil {
				t.Fatal("normal closure replay accepted another epoch")
			}
			badClosing := closing
			badClosing.File.Inode++
			if err = f.s.FinishRemoteInput(input, badClosing, f.now.Add(123*time.Second)); err == nil {
				t.Fatal("normal closure replay accepted another original file")
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			f.restart(t)
			if got := remoteTransactionGet(t, f.s, k); got.Active == nil || got.Active.Input == nil || *got.Active.Input != originalScope {
				t.Fatalf("reopen lost exact normal input provenance: %#v", got)
			}
			before = remoteTransactionDatabaseBytes(t, f.s)
			if err = f.s.FinishRemoteInput(input, closing, f.now.Add(124*time.Second)); err != nil {
				t.Fatalf("normal original closure replay after reopen: %v", err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
		})
	}
}

// Capture actual read rows without replacing or synthesizing their identities.
// Only RemoteAttemptID is allowed to change during the bounded detach step.
func remoteTransactionReadRows(t *testing.T, s *Store) map[string]artifactRead {
	t.Helper()
	out := make(map[string]artifactRead)
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		for id, r := range st.Reads {
			r.Producer = artifactCloneProducer(r.Producer)
			out[id] = r
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
func remoteTransactionReadTail(t *testing.T, s *Store, k RemoteKey, original map[string]artifactRead, wantLinked int) {
	t.Helper()
	current := remoteTransactionReadRows(t, s)
	if len(current) != len(original) {
		t.Fatalf("detach removed charged read records: got %d want %d", len(current), len(original))
	}
	linked := 0
	for id, want := range original {
		got, ok := current[id]
		if !ok || !got.Closed || !got.Closing {
			t.Fatalf("original closed read lost: %s", id)
		}
		switch got.RemoteAttemptID {
		case k.AttemptID:
			linked++
		case "":
		default:
			t.Fatalf("read adopted foreign attempt: %s", id)
		}
		got.RemoteAttemptID = want.RemoteAttemptID
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("detach changed original read identity/closure/producer/TTL: %s\ngot %#v\nwant %#v", id, got, want)
		}
	}
	if linked != wantLinked {
		t.Fatalf("bounded detach tail: got %d want %d", linked, wantLinked)
	}
}

func TestRemoteRemovalReplayDrainsClosedReadTailWithoutNativeReplay(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "closed-read-tail", f.task(t))
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 65; i++ {
		lease, err := f.s.BeginRemoteInputRead(grant, OriginalServerLifetime{}, f.now)
		if err != nil {
			t.Fatalf("read %d begin: %v", i, err)
		}
		closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
		if err != nil {
			t.Fatal(err)
		}
		// MODELED original PARTIAL response, all reader/HTTP uses joined and actual
		// original FD Close positive. This is ordinary cleanup, never normal input4.
		if err = f.s.FinishArtifactDownload(closing, f.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	original := remoteTransactionReadRows(t, f.s)
	if len(original) != 65 {
		t.Fatalf("closed reader fixture count: %d", len(original))
	}
	remoteTransactionReadTail(t, f.s, k, original, 65)
	row = remoteTransactionGet(t, f.s, k)
	if row.Active.Input.State != 2 {
		t.Fatal("partial closed-reader tail fabricated full input completion")
	}
	beforeUsage := artifactTestUsage(t, f.s)
	if beforeUsage[aDownloads] != 0 {
		t.Fatal("closed receipts still counted as active downloads")
	}
	stop, collector := remoteTransactionStop(f, 1)
	code := int32(1)
	row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	completion := RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector}
	completed, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	if row.State != 3 || row.Done == nil {
		t.Fatalf("closed-reader completion: %#v", row)
	}
	originalRevision := row.Revision
	beforeAckUsage := artifactTestUsage(t, f.s)
	// This SAME immutable frame represents ONE received positive original native
	// RemoveData return, after native/server/Finish handoffs joined. Subsequent
	// calls below drain metadata only; there is no second native invocation or
	// reconstructed/model-counted backend return anywhere in this test.
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	before := remoteTransactionDatabaseBytes(t, f.s)
	wrong := removal
	wrong.InputSecret.UID = "foreign-original-secret"
	if err = f.s.AckRemoteRemoval(k, originalRevision, wrong, f.now.Add(5*time.Second)); err == nil {
		t.Fatal("wrong original echo detached closed reads")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	if err = f.s.AckRemoteRemoval(k, originalRevision-1, removal, f.now.Add(5*time.Second)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("stale NEW native bits bypassed CAS: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	if err = f.s.AckRemoteRemoval(k, originalRevision, removal, f.now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	first := remoteTransactionGet(t, f.s, k)
	if first.State != 3 || first.Ack != 7 || first.Done == nil || first.Done.Stop == nil || first.Revision <= originalRevision {
		t.Fatalf("first64 quantum prematurely compacted native proof: %#v", first)
	}
	remoteTransactionReadTail(t, f.s, k, original, 1)
	afterFirst := artifactTestUsage(t, f.s)
	if afterFirst != beforeAckUsage || afterFirst[aWorkspaces] != 1 {
		t.Fatalf("metadata-only first quantum credited charges: before %v after %v", beforeAckUsage, afterFirst)
	}
	if remoteTransactionWholeReady(t, f.s, j.ID) {
		t.Fatal("WholeReady ignored remaining closed-reader reverse link")
	}
	f.restart(t)
	remoteTransactionReadTail(t, f.s, k, original, 1)
	remoteTransactionUnchanged(t, f.s, k, first)
	// SAME original receipt, SAME now-stale expected revision, no NEW bits:
	// detach the final1 without reissuing any native cleanup or field adoption.
	if err = f.s.AckRemoteRemoval(k, originalRevision, removal, f.now.Add(6*time.Second)); err != nil {
		t.Fatalf("stale same original receipt could not drain final link: %v", err)
	}
	retired := remoteTransactionGet(t, f.s, k)
	if retired.State != 4 || retired.Ack != 7 || retired.Done == nil || retired.Done.Stop != nil || retired.Done.Collector != nil {
		t.Fatalf("last metadata link did not retire/compact: %#v", retired)
	}
	remoteTransactionReadTail(t, f.s, k, original, 0)
	afterLast := artifactTestUsage(t, f.s)
	// All read records/1024+32 fixed charges remain. Only the independently
	// retired remote active unit is released; no read/physical allocation credit.
	if afterLast[aWorkspaces]+1 != afterFirst[aWorkspaces] {
		t.Fatalf("active-unit retirement: before %v after %v", afterFirst, afterLast)
	}
	comparable := afterLast
	comparable[aWorkspaces] = afterFirst[aWorkspaces]
	if comparable != afterFirst {
		t.Fatalf("detach changed fixed read/metadata/physical charges: before %v after %v", afterFirst, afterLast)
	}
	if !remoteTransactionWholeReady(t, f.s, j.ID) {
		t.Fatal("retired native execution with detached closed tail remains Whole-blocked")
	}
	before = remoteTransactionDatabaseBytes(t, f.s)
	if err = f.s.AckRemoteRemoval(k, originalRevision, removal, f.now.Add(7*time.Second)); err != nil {
		t.Fatal(err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	replay, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(8*time.Second))
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("semantic replay after metadata detach: %#v %v", replay, err)
	}
	remoteTransactionReadTail(t, f.s, k, original, 0)
}

func TestRemoteCollectionBudgetStartsAtDurableRelease(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "durable-release", f.task(t))
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	stop, collector := remoteTransactionStop(f, 0)
	zero := int32(0)
	observation := backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}
	released := f.now.Add(100 * time.Second)
	row, err = f.s.ObserveRemote(k, row.Revision, observation, released)
	if err != nil {
		t.Fatal(err)
	}
	deadline := released.Add(time.Duration(f.policy.CollectionSeconds) * time.Second)
	if row.Active.CollectionDeadline == nil || !row.Active.CollectionDeadline.Equal(deadline) || row.Active.CollectionDeadline.Equal(stop.Task.FinishedAt.Add(time.Duration(f.policy.CollectionSeconds)*time.Second)) || row.Active.ReleaseRevision == 0 {
		t.Fatalf("collection budget did not begin at durable release: %#v", row.Active)
	}
	revision := row.Active.ReleaseRevision
	for _, seconds := range []int64{190, 280, 370, 460, 550, 640, 699} {
		row, err = f.s.ObserveRemote(k, row.Revision, observation, f.now.Add(time.Duration(seconds)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if row.Active.CollectionDeadline == nil || !row.Active.CollectionDeadline.Equal(deadline) || row.Active.ReleaseRevision != revision || row.Active.ClosedReason != 0 {
			t.Fatalf("reobservation moved/closed durable release: %#v", row.Active)
		}
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, deadline.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("full durable collection budget unavailable: %v", err)
	}
	release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, deadline.Add(-time.Nanosecond))
	if err != nil || release.State != 2 || release.Revision != revision {
		t.Fatalf("before deadline: %#v %v", release, err)
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, deadline); err == nil {
		t.Fatal("collection deadline admitted bytes")
	}
	release, err = f.s.PollRemoteRelease(k.AttemptID, h.Output, deadline)
	if err != nil || release.State != 3 {
		t.Fatalf("at deadline: %#v %v", release, err)
	}
	row, err = f.s.ObserveRemote(k, row.Revision, observation, deadline)
	if err != nil || row.Active.ClosedReason != RemotePhaseTimeout {
		t.Fatalf("deadline not durably closed: %#v %v", row, err)
	}
	row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, deadline.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if row.Active.CollectionDeadline == nil || !row.Active.CollectionDeadline.Equal(deadline) || row.Active.ReleaseRevision != revision {
		t.Fatal("closure moved durable release")
	}
	f.restart(t)
	remoteTransactionUnchanged(t, f.s, k, row)
}

func TestRemoteFirstReceiptTimeMismatchClosesAdmissionDurably(t *testing.T) {
	for _, name := range []string{"running-before-began", "running-after-now", "running-at-stage-deadline", "stop-before-began", "stop-after-now", "stop-at-stage-deadline", "stop-future-finish", "stage-before-began", "collector-future-finish", "nolaunch-stage-future-finish", "nolaunch-setup-future-finish"} {
		t.Run(name, func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			task := f.task(t)
			if strings.HasPrefix(name, "nolaunch") {
				task.Setup = []string{"true"}
			}
			j := f.submit(t, "first-time", task)
			k, start, h := f.begin(t, j)
			row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			original := row
			usage := artifactTestUsage(t, f.s)
			now := f.now.Add(20 * time.Second)
			observation := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: f.now.Add(time.Second)}}
			if strings.HasPrefix(name, "running") {
				switch name {
				case "running-before-began":
					observation.TaskStart.StartedAt = f.now.Add(-time.Nanosecond)
				case "running-after-now":
					observation.TaskStart.StartedAt = now.Add(time.Nanosecond)
				case "running-at-stage-deadline":
					observation.TaskStart.StartedAt = row.Active.StageDeadline
					now = row.Active.StageDeadline
				}
			} else if strings.HasPrefix(name, "nolaunch") {
				n := &backend.DataNoLaunch{FailedInit: backend.StageFailure, Stage: &backend.ContainerExit{ContainerID: "modeled-stage", ExitCode: 7, StartedAt: f.now.Add(time.Millisecond), FinishedAt: now.Add(time.Nanosecond)}, SetupBlocked: &backend.DataNotStarted{Name: "setup", WaitingReason: "PodInitializing"}, TaskBlocked: &backend.DataNotStarted{Name: "task", WaitingReason: "PodInitializing"}, CollectorBlocked: &backend.DataNotStarted{Name: "collector", WaitingReason: "PodInitializing"}}
				if name == "nolaunch-setup-future-finish" {
					n.FailedInit = backend.SetupFailure
					n.Stage.ExitCode = 0
					n.Stage.FinishedAt = f.now.Add(500 * time.Millisecond)
					n.SetupBlocked = nil
					n.Setup = &backend.ContainerExit{ContainerID: "modeled-setup", ExitCode: 9, StartedAt: f.now.Add(time.Second), FinishedAt: now.Add(time.Nanosecond)}
				}
				observation = backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID}, Phase: backend.DataSettled, NoLaunch: n}
			} else {
				stop, collector := remoteTransactionStop(f, 0)
				zero := int32(0)
				switch name {
				case "stop-before-began":
					stop.Stage.StartedAt = f.now.Add(-3 * time.Second)
					stop.Stage.FinishedAt = f.now.Add(-2 * time.Second)
					stop.Task.StartedAt = f.now.Add(-time.Second)
				case "stop-after-now":
					stop.Task.StartedAt = now.Add(time.Second)
					stop.Task.FinishedAt = now.Add(2 * time.Second)
					collector.StartedAt = stop.Task.StartedAt
					collector.FinishedAt = now.Add(3 * time.Second)
				case "stop-at-stage-deadline":
					stop.Task.StartedAt = row.Active.StageDeadline
					stop.Task.FinishedAt = stop.Task.StartedAt
					collector.StartedAt = stop.Task.StartedAt
					collector.FinishedAt = stop.Task.StartedAt
					now = row.Active.StageDeadline
				case "stop-future-finish":
					stop.Task.FinishedAt = now.Add(time.Second)
					collector.FinishedAt = now.Add(2 * time.Second)
				case "stage-before-began":
					stop.Stage.StartedAt = f.now.Add(-time.Nanosecond)
				case "collector-future-finish":
					collector.FinishedAt = now.Add(time.Nanosecond)
				}
				observation = backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}
			}
			row, err = f.s.ObserveRemote(k, row.Revision, observation, now)
			if err != nil || row.Active == nil || row.Active.Phase != backend.DataHeld || row.Active.ClosedReason != RemoteIdentityMismatch || row.Revision != original.Revision+1 {
				t.Fatalf("first invalid receipt rolled back closure: %#v %v", row, err)
			}
			if row.Active.Running != nil || row.Active.Stop != nil || row.Active.NoLaunch != nil || row.Active.Collector != nil || row.Active.TaskDeadline != nil || row.Active.CollectionDeadline != nil || row.Active.ReleaseRevision != 0 || !reflect.DeepEqual(row.Resources, original.Resources) {
				t.Fatalf("invalid evidence was retained/released: %#v", row)
			}
			for _, pair := range [][2]*RemoteScope{{original.Active.Input, row.Active.Input}, {original.Active.Output, row.Active.Output}} {
				want := *pair[0]
				want.State = 3
				if *pair[1] != want {
					t.Fatalf("invalid first receipt renewed/replaced scope: %#v %#v", pair[0], pair[1])
				}
			}
			if got := artifactTestUsage(t, f.s); got != usage {
				t.Fatalf("invalid receipt changed reservations: %v %v", usage, got)
			}
			if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, now); err == nil {
				t.Fatal("invalid receipt retained input authority")
			}
			release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, now)
			if err != nil || release.State != 3 {
				t.Fatalf("invalid receipt did not abort activated helper: %#v %v", release, err)
			}
			f.restart(t)
			remoteTransactionUnchanged(t, f.s, k, row)
			// A subsequently qualified original receipt may settle cleanup only; it
			// never restores grants or allocates a delayed collection release.
			stop, collector := remoteTransactionStop(f, 0)
			zero := int32(0)
			if len(task.Setup) != 0 {
				stop.Setup = &backend.ContainerExit{ContainerID: "modeled-setup", ExitCode: 0, StartedAt: f.now.Add(600 * time.Millisecond), FinishedAt: f.now.Add(700 * time.Millisecond)}
			}
			cleaned, err := f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, now.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if cleaned.Active.ClosedReason != RemoteIdentityMismatch || cleaned.Active.ReleaseRevision != 0 || cleaned.Active.CollectionDeadline != nil || cleaned.Active.Input.State != 3 || cleaned.Active.Output.State != 3 {
				t.Fatalf("cleanup observation reopened invalid admission: %#v", cleaned)
			}
		})
	}
	for _, boundary := range []string{"began", "now"} {
		t.Run("accepted-"+boundary, func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			j := f.submit(t, "boundary", f.task(t))
			k, start, _ := f.begin(t, j)
			row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			now := f.now.Add(20 * time.Second)
			started := f.now
			if boundary == "now" {
				started = now
			}
			row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: started}}, now)
			if err != nil || row.Active.Running == nil || row.Active.ClosedReason != 0 {
				t.Fatalf("inclusive trusted endpoint rejected: %#v %v", row, err)
			}
		})
	}
}

func TestRemoteQualifiedNoLaunchPublicLifecycle(t *testing.T) {
	for _, failed := range []backend.DataInitPhase{backend.StageFailure, backend.SetupFailure} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			task := f.task(t)
			task.Setup = []string{"true"}
			task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7, 9}, DelaySeconds: 1}
			j := f.submit(t, "no-launch", task)
			k, start, h := f.begin(t, j)
			row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
			if err != nil {
				t.Fatal(err)
			}
			stage := &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: f.now.Add(time.Millisecond), FinishedAt: f.now.Add(500 * time.Millisecond)}
			noLaunch := &backend.DataNoLaunch{FailedInit: failed, Stage: stage, TaskBlocked: &backend.DataNotStarted{Name: "task", WaitingReason: "PodInitializing"}, CollectorBlocked: &backend.DataNotStarted{Name: "collector", WaitingReason: "PodInitializing"}}
			outcome := DataStageFailure
			if failed == backend.StageFailure {
				stage.ExitCode = 7
				noLaunch.SetupBlocked = &backend.DataNotStarted{Name: "setup", WaitingReason: "PodInitializing"}
			} else {
				outcome = DataSetupFailure
				noLaunch.Setup = &backend.ContainerExit{ContainerID: "modeled-setup", ExitCode: 9, StartedAt: f.now.Add(time.Second), FinishedAt: f.now.Add(2 * time.Second)}
			}
			observation := backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID}, Phase: backend.DataSettled, NoLaunch: noLaunch}
			// Explicit Waiting/native init receipts are MODELED qualified inputs, not
			// proof from a missing status, Pod phase or merely requested cancellation.
			for _, name := range []string{"missing-stage", "wrong-blocked-name", "wrong-original-pod", "missing-collector-block"} {
				t.Run(name, func(t *testing.T) {
					bad := backend.CloneDataObservation(observation)
					switch name {
					case "missing-stage":
						bad.NoLaunch.Stage = nil
					case "wrong-blocked-name":
						bad.NoLaunch.TaskBlocked.Name = "another-task"
					case "wrong-original-pod":
						bad.Status.Instance = "another-pod"
					case "missing-collector-block":
						bad.NoLaunch.CollectorBlocked = nil
					}
					before := remoteTransactionDatabaseBytes(t, f.s)
					if _, err := f.s.ObserveRemote(k, row.Revision, bad, f.now.Add(3*time.Second)); err == nil {
						t.Fatalf("malformed no-launch accepted: %s", name)
					}
					remoteTransactionDatabaseUnchanged(t, f.s, before)
				})
			}
			row, err = f.s.ObserveRemote(k, row.Revision, observation, f.now.Add(3*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if row.Active.NoLaunch == nil || !reflect.DeepEqual(row.Active.NoLaunch, noLaunch) || row.Active.Stop != nil || row.Active.Running != nil || row.Active.TaskDeadline != nil || row.Active.ReleaseRevision != 0 || row.Active.CollectionDeadline != nil || row.Active.Input.State != 3 || row.Active.Output.State != 3 {
				t.Fatalf("qualified no-launch fabricated main/release evidence: %#v", row)
			}
			if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now.Add(3*time.Second)); err == nil {
				t.Fatal("no-launch retained input bytes")
			}
			release, err := f.s.PollRemoteRelease(k.AttemptID, h.Output, f.now.Add(3*time.Second))
			if err != nil || release.State != 3 {
				t.Fatalf("no-launch collector abort: %#v %v", release, err)
			}
			completion := RemoteCompletion{Outcome: outcome, NoLaunch: noLaunch}
			completed, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(4*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if completed.Status.State != backend.Failed || completed.Status.ExitCode != nil || completed.Status.Termination != backend.UnknownTermination || completed.NextAttemptAt != nil || len(completed.Attempts) != 1 {
				t.Fatalf("init failure invented main exit/retry: %#v", completed)
			}
			row = remoteTransactionGet(t, f.s, k)
			if row.Done == nil || row.Done.Outcome != outcome || row.Done.OriginalExit != nil || row.Done.Stop != nil || row.Done.Collector != nil || !reflect.DeepEqual(row.Done.NoLaunch, noLaunch) {
				t.Fatalf("no-launch semantic decision: %#v", row)
			}
			result, err := f.s.ArtifactResultFor(f.actor, j.ID, k.AttemptID)
			if err != nil || result.State != "absent" || result.Reference != nil {
				t.Fatalf("init failure published result: %#v %v", result, err)
			}
			replay, err := f.s.CompleteRemoteAttempt(k, completion, f.now.Add(5*time.Second))
			if err != nil || !reflect.DeepEqual(replay, completed) {
				t.Fatalf("no-launch duplicate: %#v %v", replay, err)
			}
			removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
			if err = f.s.AckRemoteRemoval(k, row.Revision, removal, f.now.Add(6*time.Second)); err != nil {
				t.Fatal(err)
			}
			retired := remoteTransactionGet(t, f.s, k)
			if retired.State != 4 || retired.Done.NoLaunch != nil {
				t.Fatalf("no-launch cleanup did not compact: %#v", retired)
			}
			f.restart(t)
			remoteTransactionUnchanged(t, f.s, k, retired)
			replay, err = f.s.CompleteRemoteAttempt(k, completion, f.now.Add(7*time.Second))
			if err != nil || !reflect.DeepEqual(replay, completed) {
				t.Fatalf("compacted/reopened no-launch duplicate: %#v %v", replay, err)
			}
		})
	}
}

func TestRemoteMountOnlyWholePruneUsesDurableRoute(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%v", retry), func(t *testing.T) {
			f := newRemoteTransactionFixture(t)
			info := f.enableRetention(t)
			task := backend.Task{Argv: []string{"true"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}}
			if retry {
				task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}
			}
			j, err := f.s.SubmitData(f.actor, &info.Domain, "mount-prune", task, "remote", "modeled-profile-v1", f.authority, f.policy, f.now)
			if err != nil {
				t.Fatal(err)
			}
			now := f.now
			var keys []RemoteKey
			var final RemoteAttempt
			count := 1
			if retry {
				count = 2
			}
			for attempt := 0; attempt < count; attempt++ {
				started, run, err := f.s.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), RemoteHashes{}, f.policy, now)
				if err != nil || !run {
					t.Fatalf("mount begin: %v %v", run, err)
				}
				k := RemoteKey{j.ID, started.Job.CurrentAttemptID()}
				keys = append(keys, k)
				pod := backend.DataResource{Name: fmt.Sprintf("original-pod-%d", attempt), UID: fmt.Sprintf("modeled-pod-%d", attempt)}
				row, err := f.s.RecordRemoteCreated(k, started.Revision, RemoteCreation{Resources: backend.DataCreated{Instance: pod.UID, Pod: pod}, Succeeded: true}, now)
				if err != nil {
					t.Fatal(err)
				}
				code := int32(0)
				if retry && attempt == 0 {
					code = 1
				}
				stop := backend.DataTaskStop{Task: backend.ContainerExit{ContainerID: fmt.Sprintf("modeled-main-%d", attempt), ExitCode: code, StartedAt: now.Add(time.Second), FinishedAt: now.Add(2 * time.Second)}}
				state := backend.Succeeded
				if code != 0 {
					state = backend.Failed
				}
				row, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: state, Instance: pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop}, now.Add(3*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				j, err = f.s.CompleteRemoteAttempt(k, RemoteCompletion{Outcome: DataMainExit, Stop: &stop}, now.Add(4*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				row = remoteTransactionGet(t, f.s, k)
				if attempt == count-1 {
					final = row
					break
				}
				if err = f.s.AckRemoteRemoval(k, row.Revision, backend.DataRemoval{Pod: pod, PodRemoved: true}, now.Add(4*time.Second)); err != nil {
					t.Fatal(err)
				}
				if j.NextAttemptAt == nil {
					t.Fatal("mount retry missing deadline")
				}
				now = *j.NextAttemptAt
			}
			selection := PruneSelection{Before: now.Add(time.Minute), Mode: Whole, Max: j.Sequence, Limit: 1}
			before := remoteTransactionDatabaseBytes(t, f.s)
			dry, err := f.s.Prune(f.actor, selection, false, nil, nil, now.Add(10*time.Second))
			if err != nil || dry.Selected != 0 {
				t.Fatalf("nonretired mount-only selected: %#v %v", dry, err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			executed, err := f.s.Prune(f.actor, selection, true, nil, nil, now.Add(10*time.Second))
			if err != nil || executed.Marked != 0 {
				t.Fatalf("nonretired mount-only pruned: %#v %v", executed, err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			if err = f.s.AckRemoteRemoval(final.Key, final.Revision, backend.DataRemoval{Pod: final.Resources.Pod, PodRemoved: true}, now.Add(11*time.Second)); err != nil {
				t.Fatal(err)
			}
			before = remoteTransactionDatabaseBytes(t, f.s)
			dry, err = f.s.Prune(f.actor, selection, false, nil, nil, now.Add(12*time.Second))
			if err != nil || dry.Selected != 1 {
				t.Fatalf("retired mount-only dry selection: %#v %v", dry, err)
			}
			remoteTransactionDatabaseUnchanged(t, f.s, before)
			executed, err = f.s.Prune(f.actor, selection, true, nil, nil, now.Add(12*time.Second))
			if err != nil || executed.Marked != 1 || executed.Selected != dry.Selected {
				t.Fatalf("retired mount-only execute: %#v %v", executed, err)
			}
			if cleanup, found, err := f.s.NextCleanup(0); err != nil || found {
				t.Fatalf("retired remote was assigned second ordinary cleanup/task: %#v %v %v", cleanup, found, err)
			}
			if obligations, err := f.s.ListObligations(f.actor, 0, 0, 10); err != nil || len(obligations.Items) != 0 {
				t.Fatalf("retired remote retained ordinary task obligation: %#v %v", obligations, err)
			}
			_, err = f.s.GetFor(f.actor, j.ID)
			var expired *ExpiryError
			if !errors.As(err, &expired) || expired.Reason != ResultExpired || expired.JobID != j.ID {
				t.Fatalf("Whole did not preserve public expiry tombstone: %v", err)
			}
			if _, err = f.s.Get(j.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Whole retained remote core job: %v", err)
			}
			f.restart(t)
			for _, k := range keys {
				if _, err = f.s.GetRemoteAttempt(k); !errors.Is(err, ErrNotFound) {
					t.Fatalf("Whole retained remote attempt after reopen: %v", err)
				}
			}
		})
	}
}

func TestRemoteWholeCorrectionPreservesOrdinaryNoWorkdirCleanup(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	info := f.enableRetention(t)
	task := backend.Task{Argv: []string{"ordinary"}}
	j, err := f.s.SubmitRetained(f.actor, info.Domain, "ordinary-prune", task, "remote", "modeled-ordinary-profile", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	j, run, err := f.s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, f.now)
	if err != nil || !run {
		t.Fatalf("ordinary begin: %v %v", run, err)
	}
	code := int32(0)
	j, err = f.s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "modeled-ordinary-instance", ExitCode: &code, Termination: backend.StoppedWithExitCode}, f.now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	selection := PruneSelection{Before: f.now.Add(time.Minute), Mode: Whole, Max: j.Sequence, Limit: 1}
	page, err := f.s.Prune(f.actor, selection, true, nil, nil, f.now.Add(2*time.Second))
	if err != nil || page.Marked != 1 {
		t.Fatalf("ordinary Whole selection: %#v %v", page, err)
	}
	cleanup, found, err := f.s.NextCleanup(0)
	if err != nil || !found || cleanup.Operation != NativeCleanup || cleanup.Execution.ID != j.CurrentAttemptID() || cleanup.Execution.Instance != "modeled-ordinary-instance" || !reflect.DeepEqual(cleanup.Execution.Task, task) {
		t.Fatalf("ordinary native cleanup/task erased by remote route repair: %#v %v %v", cleanup, found, err)
	}
}

func TestRemoteLateInvalidReceiptPreservesClosedFacts(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	j := f.submit(t, "late-invalid", f.task(t))
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	input, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
	if err != nil {
		t.Fatal(err)
	}
	// Successful transfer and the SAME original reader/HTTP/FD joins are MODELED.
	if err = f.s.FinishRemoteInput(input, closing, f.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	observed := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: f.now.Add(2 * time.Second)}}
	row, err = f.s.ObserveRemote(k, row.Revision, observed, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	row, err = f.s.RevokeRemote(k, row.Revision, RemoteShutdown, f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	original := row
	usage := artifactTestUsage(t, f.s)
	observed.Collector = &backend.ContainerExit{ContainerID: "modeled-collector", ExitCode: 0, StartedAt: f.now.Add(3 * time.Second), FinishedAt: f.now.Add(7 * time.Second)}
	for n := 0; n < 2; n++ {
		before := row
		row, err = f.s.ObserveRemote(k, row.Revision, observed, f.now.Add(5*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if row.Revision != before.Revision+1 || row.Active.ClosedReason != RemoteShutdown || row.Active.Input.State != 4 || !reflect.DeepEqual(row.Active, original.Active) || !reflect.DeepEqual(row.Resources, original.Resources) {
			t.Fatalf("late invalid observation rewrote closed reason/evidence/input4: %#v", row)
		}
		if got := artifactTestUsage(t, f.s); got != usage {
			t.Fatalf("late invalid receipt released charges: %v %v", usage, got)
		}
	}
	before := remoteTransactionDatabaseBytes(t, f.s)
	if _, err = f.s.ObserveRemote(k, original.Revision, observed, f.now.Add(5*time.Second)); err == nil {
		t.Fatal("stale invalid observation accepted")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	observed.Status.Instance = "foreign-pod"
	if _, err = f.s.ObserveRemote(k, row.Revision, observed, f.now.Add(5*time.Second)); err == nil {
		t.Fatal("foreign invalid observation accepted")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	f.restart(t)
	remoteTransactionUnchanged(t, f.s, k, row)
}

func TestRemoteCalendarRenewalOverflowClosesAdmission(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	end := time.Date(9999, 12, 31, 23, 59, 50, 0, time.UTC)
	budgets, err := f.policy.Budgets(0)
	if err != nil {
		t.Fatal(err)
	}
	total, err := budgets.TotalSeconds()
	if err != nil {
		t.Fatal(err)
	}
	f.now = end.Add(-time.Duration(total) * time.Second)
	j := f.submit(t, "calendar-renewal", f.task(t))
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	input, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.s.BeginRemoteInputRead(input, OriginalServerLifetime{}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), reader))
	if err != nil {
		t.Fatal(err)
	}
	// Native transfer and original reader/HTTP/FD joins are MODELED. All grants,
	// lease renewals, release and fixed-policy clocks below use public Store CAS.
	if err = f.s.FinishRemoteInput(input, closing, f.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	row = remoteTransactionGet(t, f.s, k)
	observe := func(o backend.DataObservation, now time.Time) {
		t.Helper()
		var err error
		row, err = f.s.ObserveRemote(k, row.Revision, o, now)
		if err != nil {
			t.Fatal(err)
		}
		if row.Active.ClosedReason != 0 {
			t.Fatalf("precondition closed at %s: %#v", now, row.Active)
		}
	}
	staging := backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: row.Resources.Pod.UID}, Phase: backend.DataStage}
	started := row.Active.StageDeadline.Add(-time.Nanosecond)
	for now := f.now.Add(90 * time.Second); now.Before(started); now = now.Add(90 * time.Second) {
		observe(staging, now)
	}
	running := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: row.Resources.Pod.UID}, Phase: backend.DataTask, TaskStart: &backend.ContainerStart{ContainerID: "modeled-main", StartedAt: started}}
	observe(running, started)
	lastRunning := end.Add(-721 * time.Second)
	for now := started.Add(90 * time.Second); now.Before(lastRunning); now = now.Add(90 * time.Second) {
		observe(running, now)
	}
	observe(running, lastRunning)
	stop := backend.DataTaskStop{Stage: &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: f.now.Add(time.Millisecond), FinishedAt: f.now.Add(time.Second)}, Task: backend.ContainerExit{ContainerID: "modeled-main", StartedAt: started, FinishedAt: lastRunning}}
	collector := backend.ContainerExit{ContainerID: "modeled-collector", StartedAt: started, FinishedAt: lastRunning.Add(time.Second)}
	zero := int32(0)
	stopped := backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: row.Resources.Pod.UID, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}
	released := end.Add(-602 * time.Second)
	observe(stopped, released)
	for now := released.Add(90 * time.Second); now.Before(end.Add(-122 * time.Second)); now = now.Add(90 * time.Second) {
		observe(stopped, now)
	}
	prior := end.Add(-122 * time.Second)
	observe(stopped, prior)
	now := end.Add(-82 * time.Second)
	original := row
	usage := artifactTestUsage(t, f.s)
	if row.Active.CollectionDeadline == nil || !row.Active.CollectionDeadline.Equal(end.Add(-2*time.Second)) || !row.Active.ResourceDeadline.Equal(end) || !row.Active.Output.Renewed.Equal(prior) || !row.Active.Output.Expires.Equal(end.Add(-2*time.Second)) || row.Active.Input.State != 4 {
		t.Fatalf("invalid released boundary fixture: %#v", row.Active)
	}
	if !now.Before(row.Active.ResourceDeadline) || !now.Before(*row.Active.CollectionDeadline) || !now.Before(row.Active.Output.Expires) || now.Before(row.Active.Output.Renewed.Add(30*time.Second)) {
		t.Fatal("an earlier expiry/not-due guard would satisfy oracle")
	}
	if _, err = backend.DataDeadline(now, 120); err == nil {
		t.Fatal("new renewal deadline unexpectedly representable")
	}
	if _, err = f.s.CheckRemoteGrant(k.AttemptID, RemoteOutput, h.Output, now); err != nil {
		t.Fatalf("otherwise current live scope rejected: %v", err)
	}
	row, err = f.s.ObserveRemote(k, row.Revision, stopped, now)
	if err != nil || row.Active == nil || row.Active.Phase != backend.DataHeld || row.Active.ClosedReason != RemotePhaseTimeout || row.Revision != original.Revision+1 {
		t.Fatalf("calendar renewal rolled back closure: %#v %v", row, err)
	}
	expected := *original.Active
	expected.Phase = backend.DataHeld
	expected.ClosedReason = RemotePhaseTimeout
	output := *original.Active.Output
	output.State = 3
	expected.Output = &output
	if !reflect.DeepEqual(*row.Active, expected) || !reflect.DeepEqual(row.Resources, original.Resources) {
		t.Fatalf("overflow changed original facts/normal4/renewal/release: %#v %#v", row.Active, &expected)
	}
	if got := artifactTestUsage(t, f.s); got != usage {
		t.Fatalf("overflow released charges: %v %v", usage, got)
	}
	f.restart(t)
	remoteTransactionUnchanged(t, f.s, k, row)
}
