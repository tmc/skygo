package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions. Native creation, container exits,
// reader closures and removal results are MODELED inputs as in the other
// remote tests; nothing here qualifies a native two-attempt retry.

// remoteRetry is one public attempt of a retrying Workdir task, settled at a
// selected main exit and ready for CompleteRemoteAttempt.
type remoteRetry struct {
	f          *remoteTransactionFixture
	j          backend.Job
	k          RemoteKey
	h          RemoteHashes
	completion RemoteCompletion
}

// newRemoteRetry closes partial ordinary reads and, if complete, consumes the
// original full input transfer before the main container exits with code.
func newRemoteRetry(t *testing.T, id string, code int32, partial int, complete bool) *remoteRetry {
	t.Helper()
	f := newRemoteTransactionFixture(t)
	task := f.task(t)
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}
	j := f.submit(t, id, task)
	k, start, h := f.begin(t, j)
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: remoteTransactionCreated(), Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := f.s.CheckRemoteGrant(k.AttemptID, RemoteInput, h.Input, f.now)
	if err != nil {
		t.Fatal(err)
	}
	read := func() ArtifactDownloadClosure {
		lease, err := f.s.BeginRemoteInputRead(grant, OriginalServerLifetime{}, f.now)
		if err != nil {
			t.Fatal(err)
		}
		closing, err := f.s.MarkArtifactDownloadClosing(remoteTransactionClosure(f.actor.Principal(), lease))
		if err != nil {
			t.Fatal(err)
		}
		return closing
	}
	for i := 0; i < partial; i++ {
		if err = f.s.FinishArtifactDownload(read(), f.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if complete {
		if err = f.s.FinishRemoteInput(grant, read(), f.now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	row = remoteTransactionGet(t, f.s, k)
	stop, collector := remoteTransactionStop(f, code)
	_, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: row.Resources.Pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop, Collector: &collector}, f.now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return &remoteRetry{f: f, j: j, k: k, h: h, completion: RemoteCompletion{Outcome: DataMainExit, Stop: &stop, Collector: &collector}}
}

func (c *remoteRetry) complete(t *testing.T) backend.Job {
	t.Helper()
	j, err := c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return j
}

// retire acknowledges the SAME original removal result until State 4.
func (c *remoteRetry) retire(t *testing.T) RemoteAttempt {
	t.Helper()
	row := remoteTransactionGet(t, c.f.s, c.k)
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: row.Resources.InputSecret != backend.DataResource{}, OutputRemoved: row.Resources.OutputSecret != backend.DataResource{}}
	for i := 0; i < 4 && row.State != 4; i++ {
		if err := c.f.s.AckRemoteRemoval(c.k, row.Revision, removal, c.f.now.Add(5*time.Second)); err != nil {
			t.Fatal(err)
		}
		row = remoteTransactionGet(t, c.f.s, c.k)
	}
	if row.State != 4 {
		t.Fatalf("not retired: %#v", row)
	}
	return row
}

func remoteRetryHashes(j backend.Job, purpose string) RemoteHashes {
	return RemoteHashes{Input: sha256.Sum256([]byte(purpose + " input " + j.ID)), Output: sha256.Sum256([]byte(purpose + " output " + j.ID))}
}

// remoteRetryRow returns the stored private record, including Done.Input.
func remoteRetryRow(t *testing.T, s *Store, id string) remoteRecord {
	t.Helper()
	var r remoteRecord
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		v, ok := st.Remote[id]
		if !ok {
			return ErrNotFound
		}
		r = remoteClone(v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

// remoteRetryRewrite is a MODELED private seam: it rewrites one remote record
// through the validating artifact transaction, as an older writer might have.
func remoteRetryRewrite(s *Store, id string, edit func(*remoteRecord)) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		_, err := artifactChange(tx, maxChangeBytes, func(st *artifactState) error {
			r := remoteClone(st.Remote[id])
			edit(&r)
			st.Remote[id] = r
			return nil
		})
		return err
	})
}

// remoteRetryOutputs returns the stored remote result objects and the number
// of delete intents.
func remoteRetryOutputs(t *testing.T, s *Store) (map[string]string, int) {
	t.Helper()
	out := make(map[string]string)
	var deletes int
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		for id, o := range st.Objects {
			if o.Origin == "remote_result" {
				out[id] = o.Source
			}
		}
		deletes = len(st.Deletes)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out, deletes
}

// Oracle 1: the next attempt waits for the actual metadata tail and begins once.
func TestRemoteRetryBeginsAfterRetirement(t *testing.T) {
	c := newRemoteRetry(t, "retry-retired", 1, 65, true)
	f := c.f
	original := remoteRetryRow(t, f.s, c.k.AttemptID).Active.Input
	if original == nil || original.State != 4 {
		t.Fatalf("full transfer not consumed: %#v", original)
	}
	completed := c.complete(t)
	if completed.Status.State != backend.Retrying || completed.NextAttemptAt == nil {
		t.Fatalf("selected exit did not schedule retry: %#v", completed)
	}
	due := *completed.NextAttemptAt
	raw := remoteRetryRow(t, f.s, c.k.AttemptID)
	if raw.State != 3 || raw.Active != nil || raw.Done == nil || !reflect.DeepEqual(raw.Done.Input, original) {
		t.Fatalf("completion lost original input receipt: %#v", raw.Done)
	}
	next := remoteRetryHashes(c.j, "second")
	waiting := func(label string) {
		t.Helper()
		before := remoteTransactionDatabaseBytes(t, f.s)
		out, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, next, f.policy, due)
		if err != nil || run || out.Job.CurrentAttemptID() != c.k.AttemptID {
			t.Fatalf("%s: began before retirement: run=%v err=%v", label, run, err)
		}
		remoteTransactionDatabaseUnchanged(t, f.s, before)
	}
	waiting("State3")
	row := remoteTransactionGet(t, f.s, c.k)
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	if err := f.s.AckRemoteRemoval(c.k, row.Revision, removal, f.now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if first := remoteTransactionGet(t, f.s, c.k); first.State != 3 || first.Ack != 7 {
		t.Fatalf("66 closed reads retired in one quantum: %#v", first)
	}
	waiting("closed-read tail")
	if err := f.s.AckRemoteRemoval(c.k, row.Revision, removal, f.now.Add(6*time.Second)); err != nil {
		t.Fatal(err)
	}
	raw = remoteRetryRow(t, f.s, c.k.AttemptID)
	if raw.State != 4 || raw.Done.Stop != nil || raw.Done.Collector != nil || !reflect.DeepEqual(raw.Done.Input, original) {
		t.Fatalf("retirement compaction: %#v", raw.Done)
	}
	f.restart(t)
	if got := remoteRetryRow(t, f.s, c.k.AttemptID); !reflect.DeepEqual(got, raw) {
		t.Fatalf("reopen changed retired record: %#v", got)
	}
	started, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, next, f.policy, due)
	if err != nil || !run || started.Job.CurrentAttemptID() == c.k.AttemptID || len(started.Job.Attempts) != 2 {
		t.Fatalf("retired predecessor: %#v %v %v", started, run, err)
	}
	k2 := RemoteKey{c.j.ID, started.Job.CurrentAttemptID()}
	second := remoteRetryRow(t, f.s, k2.AttemptID)
	if second.State != 2 || second.Active.Input == nil || second.Active.Input.Hash != next.Input || second.Active.Input.State != 1 || second.Active.Output.Hash != next.Output {
		t.Fatalf("next attempt scopes: %#v", second.Active)
	}
	before := remoteTransactionDatabaseBytes(t, f.s)
	again, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, remoteRetryHashes(c.j, "third"), f.policy, due)
	if err != nil || run || again.Job.CurrentAttemptID() != k2.AttemptID {
		t.Fatalf("old expected attempt began again: %v %v", run, err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	replay, err := f.s.CompleteRemoteAttempt(c.k, c.completion, due.Add(time.Hour))
	if err != nil || !reflect.DeepEqual(replay, started.Job) {
		t.Fatalf("old completion replay: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
}

// Oracle 2: MODELED values of a real retired predecessor exercise each guard.
func TestRemoteRetryGuard(t *testing.T) {
	c := newRemoteRetry(t, "retry-guard", 1, 0, true)
	completed := c.complete(t)
	c.retire(t)
	base := remoteRetryRow(t, c.f.s, c.k.AttemptID)
	job, err := c.f.s.Get(c.j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(job, completed) {
		t.Fatalf("retirement changed job: %#v", job)
	}
	tests := []struct {
		name    string
		expect  string
		edit    func(*backend.Job, *remoteRecord)
		retired bool
		err     error
	}{
		{name: "retired", retired: true},
		{name: "completed", edit: func(j *backend.Job, r *remoteRecord) { r.State = 3 }},
		{name: "active", edit: func(j *backend.Job, r *remoteRecord) { r.Active = &remoteActive{} }},
		{name: "accepted", edit: func(j *backend.Job, r *remoteRecord) { r.State = 1 }, err: ErrArtifactConflict},
		{name: "missing", edit: func(j *backend.Job, r *remoteRecord) { r.Job = "" }, err: ErrArtifactConflict},
		{name: "foreign expected", expect: strings.Repeat("f", 32), err: ErrArtifactConflict},
		{name: "foreign job", edit: func(j *backend.Job, r *remoteRecord) { r.Job = strings.Repeat("e", 32) }, err: ErrArtifactConflict},
		{name: "no retry policy", edit: func(j *backend.Job, r *remoteRecord) { j.Task.Retry = nil }, err: ErrArtifactConflict},
		{name: "max attempts", edit: func(j *backend.Job, r *remoteRecord) { j.Task.Retry.MaxAttempts = 1 }, err: ErrArtifactConflict},
		{name: "nonselected exit", edit: func(j *backend.Job, r *remoteRecord) { j.Task.Retry.ExitCodes = []int32{2} }, err: ErrArtifactConflict},
		{name: "unknown status", edit: func(j *backend.Job, r *remoteRecord) { j.Attempts[0].Status.State = backend.Unknown }, err: ErrArtifactConflict},
		{name: "unfinished", edit: func(j *backend.Job, r *remoteRecord) { j.Attempts[0].FinishedAt = nil }, err: ErrArtifactConflict},
		{name: "instance", edit: func(j *backend.Job, r *remoteRecord) { j.Attempts[0].Status.Instance = "other-pod-uid" }, err: errRemoteRetryUnproven},
		{name: "pod uid", edit: func(j *backend.Job, r *remoteRecord) { r.Resources.Pod.UID = "other-pod-uid" }, err: errRemoteRetryUnproven},
		{name: "no pod", edit: func(j *backend.Job, r *remoteRecord) { r.Resources.Pod = nil }, err: errRemoteRetryUnproven},
		{name: "creation", edit: func(j *backend.Job, r *remoteRecord) { r.Creation = 2 }, err: errRemoteRetryUnproven},
		{name: "no decision", edit: func(j *backend.Job, r *remoteRecord) { r.Done = nil }, err: errRemoteRetryUnproven},
		{name: "original exit", edit: func(j *backend.Job, r *remoteRecord) { x := int32(2); r.Done.OriginalExit = &x }, err: errRemoteRetryUnproven},
		{name: "no original exit", edit: func(j *backend.Job, r *remoteRecord) { r.Done.OriginalExit = nil }, err: errRemoteRetryUnproven},
		{name: "stage failure", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Outcome = uint8(DataStageFailure) }, err: errRemoteRetryUnproven},
		{name: "setup failure", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Outcome = uint8(DataSetupFailure) }, err: errRemoteRetryUnproven},
		{name: "collection failure", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Outcome = uint8(DataCollectionFailure) }, err: errRemoteRetryUnproven},
		{name: "canceled", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Outcome = uint8(DataCanceled) }, err: errRemoteRetryUnproven},
		{name: "timeout", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Outcome = uint8(DataTimeout) }, err: errRemoteRetryUnproven},
		{name: "missing ack", edit: func(j *backend.Job, r *remoteRecord) { r.Ack = 5 }, err: errRemoteRetryUnproven},
		{name: "missing input secret", edit: func(j *backend.Job, r *remoteRecord) { r.Resources.Input, r.Ack = nil, 5 }, err: errRemoteRetryUnproven},
		{name: "missing output secret", edit: func(j *backend.Job, r *remoteRecord) { r.Resources.Output, r.Ack = nil, 3 }, err: errRemoteRetryUnproven},
		{name: "extra output secret", edit: func(j *backend.Job, r *remoteRecord) { j.Task.Outputs = nil }, err: errRemoteRetryUnproven},
		{name: "no input receipt", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Input = nil }, err: errRemoteRetryUnproven},
		{name: "partial input receipt", edit: func(j *backend.Job, r *remoteRecord) { r.Done.Input.State = 3 }, err: errRemoteRetryUnproven},
		{name: "receipt without workdir", edit: func(j *backend.Job, r *remoteRecord) { j.Task.Workdir, r.Resources.Input, r.Ack = nil, nil, 5 }, err: errRemoteRetryUnproven},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var j backend.Job
			raw, err := json.Marshal(job)
			if err != nil || json.Unmarshal(raw, &j) != nil {
				t.Fatal("copy job")
			}
			r := remoteClone(base)
			if tt.edit != nil {
				tt.edit(&j, &r)
			}
			expect := c.k.AttemptID
			if tt.expect != "" {
				expect = tt.expect
			}
			st := &artifactState{Remote: map[string]remoteRecord{c.k.AttemptID: r}}
			retired, err := remoteRetryRetired(st, j, expect)
			if retired != tt.retired || !errors.Is(err, tt.err) || tt.err == nil && err != nil {
				t.Fatalf("remoteRetryRetired = %v, %v; want %v, %v", retired, err, tt.retired, tt.err)
			}
		})
	}
}

// remoteRetryUnprovenRefuses selects the Begin form for a retired predecessor
// that cannot prove its retry: false holds the job Retrying with a nil error
// (option a); true expects ErrArtifactConflict (option b). It must match the
// errRemoteRetryUnproven branch in BeginRemoteAttempt.
const remoteRetryUnprovenRefuses = false

// remoteRetryHeld checks that Begin started nothing for an unprovable
// predecessor expected and left the job Retrying.
func remoteRetryHeld(t *testing.T, out RemoteStart, run bool, err error, expected string) {
	t.Helper()
	if run {
		t.Fatal("unprovable predecessor began retry")
	}
	if remoteRetryUnprovenRefuses {
		if !errors.Is(err, ErrArtifactConflict) {
			t.Fatalf("unprovable predecessor: %v, want ErrArtifactConflict", err)
		}
		return
	}
	if err != nil || out.Job.Status.State != backend.Retrying || out.Job.CurrentAttemptID() != expected {
		t.Fatalf("unprovable predecessor not held: %v %#v", err, out.Job.Status)
	}
}

// Oracle 3: a receipt-less retired Workdir row stays readable, replayable and
// cancelable, but cannot establish a retry: Begin holds it without writes.
// Nothing backfills it.
func TestRemoteRetryReceiptlessRow(t *testing.T) {
	c := newRemoteRetry(t, "retry-receiptless", 1, 2, true)
	f := c.f
	completed := c.complete(t)
	c.retire(t)
	if err := remoteRetryRewrite(f.s, c.k.AttemptID, func(r *remoteRecord) { r.Done.Input = nil }); err != nil {
		t.Fatalf("receipt-less retired row is invalid: %v", err)
	}
	f.restart(t)
	old := remoteRetryRow(t, f.s, c.k.AttemptID)
	view := remoteTransactionGet(t, f.s, c.k)
	before := remoteTransactionDatabaseBytes(t, f.s)
	due := *completed.NextAttemptAt
	held, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, remoteRetryHashes(c.j, "second"), f.policy, due)
	remoteRetryHeld(t, held, run, err, c.k.AttemptID)
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	replay, err := f.s.CompleteRemoteAttempt(c.k, c.completion, due)
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("receipt-less replay: %v", err)
	}
	row := remoteTransactionGet(t, f.s, c.k)
	removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: true, InputRemoved: true, OutputRemoved: true}
	if err = f.s.AckRemoteRemoval(c.k, row.Revision, removal, due); err != nil {
		t.Fatal(err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	if got := remoteRetryRow(t, f.s, c.k.AttemptID); !reflect.DeepEqual(got, old) || got.Done.Input != nil {
		t.Fatalf("receipt backfilled: %#v", got.Done)
	}
	remoteTransactionUnchanged(t, f.s, c.k, view)
	f.authenticate(t)
	if _, err = f.s.UpdateFor(f.actor, c.j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
	canceled, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, RemoteHashes{}, backend.DataPolicy{}, due)
	if err != nil || run || canceled.Job.Status.State != backend.Canceled {
		t.Fatalf("waiting receipt-less retry did not cancel: %#v %v %v", canceled.Job.Status, run, err)
	}
}

// Oracles 3 and 4: only the original complete transfer establishes a Workdir
// retry, and outcomes that schedule no retry never need it.
func TestRemoteRetryCompletionRequiresInput(t *testing.T) {
	refused := func(t *testing.T, c *remoteRetry) {
		t.Helper()
		before := remoteTransactionDatabaseBytes(t, c.f.s)
		if _, err := c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(4*time.Second)); !errors.Is(err, ErrArtifactConflict) {
			t.Fatalf("retry without complete input transfer: %v", err)
		}
		remoteTransactionDatabaseUnchanged(t, c.f.s, before)
	}
	settle := func(t *testing.T, c *remoteRetry, input bool) {
		t.Helper()
		j := c.complete(t)
		if j.Status.State == backend.Retrying {
			t.Fatalf("scheduled retry: %#v", j.Status)
		}
		if raw := remoteRetryRow(t, c.f.s, c.k.AttemptID); (raw.Done.Input != nil) != input {
			t.Fatalf("receipt = %#v, want present %v", raw.Done.Input, input)
		}
		c.retire(t)
		if raw := remoteRetryRow(t, c.f.s, c.k.AttemptID); (raw.Done.Input != nil) != input {
			t.Fatalf("retired receipt = %#v, want present %v", raw.Done.Input, input)
		}
	}
	t.Run("partial reads then cancel", func(t *testing.T) {
		c := newRemoteRetry(t, "retry-partial", 1, 3, false)
		refused(t, c)
		c.f.authenticate(t)
		if _, err := c.f.s.UpdateFor(c.f.actor, c.j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
			t.Fatal(err)
		}
		settle(t, c, false)
	})
	t.Run("no reads", func(t *testing.T) {
		refused(t, newRemoteRetry(t, "retry-unread", 1, 0, false))
	})
	t.Run("closed admission", func(t *testing.T) {
		c := newRemoteRetry(t, "retry-closed", 1, 0, true)
		row := remoteTransactionGet(t, c.f.s, c.k)
		if _, err := c.f.s.RevokeRemote(c.k, row.Revision, RemoteShutdown, c.f.now.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
		refused(t, c)
	})
	t.Run("nonselected exit", func(t *testing.T) {
		settle(t, newRemoteRetry(t, "retry-exit-two", 2, 1, false), false)
	})
	t.Run("nonselected exit keeps receipt", func(t *testing.T) {
		settle(t, newRemoteRetry(t, "retry-exit-two-input", 2, 0, true), true)
	})
}

// A retry without Workdir needs no input receipt and may not carry one: a
// receipt requires the original input Secret, which requires Workdir. The
// task is mount-only, so it has no scopes, Secrets or collector.
func TestRemoteRetryWithoutWorkdir(t *testing.T) {
	f := newRemoteTransactionFixture(t)
	task := backend.Task{Argv: []string{"true"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}}
	j := f.submit(t, "retry-no-workdir", task)
	start, run, err := f.s.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), RemoteHashes{}, f.policy, f.now)
	if err != nil || !run {
		t.Fatalf("begin: %v %v", run, err)
	}
	k := RemoteKey{j.ID, start.Job.CurrentAttemptID()}
	pod := backend.DataResource{Name: "original-pod", UID: "modeled-pod-uid"}
	row, err := f.s.RecordRemoteCreated(k, start.Revision, RemoteCreation{Resources: backend.DataCreated{Instance: pod.UID, Pod: pod}, Succeeded: true}, f.now)
	if err != nil {
		t.Fatal(err)
	}
	code := int32(1)
	stop := backend.DataTaskStop{Task: backend.ContainerExit{ContainerID: "modeled-main", ExitCode: code, StartedAt: f.now.Add(time.Second), FinishedAt: f.now.Add(2 * time.Second)}}
	if _, err = f.s.ObserveRemote(k, row.Revision, backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: pod.UID, ExitCode: &code, Termination: backend.StoppedWithExitCode}, Phase: backend.DataSettled, TaskStop: &stop}, f.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	c := &remoteRetry{f: f, j: j, k: k, completion: RemoteCompletion{Outcome: DataMainExit, Stop: &stop}}
	completed := c.complete(t)
	if completed.Status.State != backend.Retrying || completed.NextAttemptAt == nil {
		t.Fatalf("no-Workdir retry: %#v", completed.Status)
	}
	if raw := remoteRetryRow(t, f.s, k.AttemptID); raw.Done == nil || raw.Done.Input != nil {
		t.Fatalf("no-Workdir receipt: %#v", raw.Done)
	}
	before := remoteTransactionDatabaseBytes(t, f.s)
	replay, err := f.s.CompleteRemoteAttempt(k, c.completion, f.now.Add(10*time.Second))
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("lost-response replay: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	c.retire(t)
	before = remoteTransactionDatabaseBytes(t, f.s)
	lease := remoteLease{Hash: sha256.Sum256([]byte("forged")), Epoch: 1, State: 4, Renewed: f.now, Expires: f.now.Add(120 * time.Second)}
	if err = remoteRetryRewrite(f.s, k.AttemptID, func(r *remoteRecord) { r.Done.Input = &lease }); err == nil {
		t.Fatal("receipt accepted without Workdir")
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	started, run, err := f.s.BeginRemoteAttempt(j.ID, k.AttemptID, RemoteHashes{}, f.policy, *completed.NextAttemptAt)
	if err != nil || !run || started.Job.CurrentAttemptID() == k.AttemptID || len(started.Job.Attempts) != 2 {
		t.Fatalf("no-Workdir next attempt: %v %v", run, err)
	}
}

// Losing authority after a selected nonzero exit is a job-level failure:
// the job cannot retry, while the completion proof keeps the native exit and
// Stop. Every other admission closure is unchanged.
func TestRemoteRetryAuthorityLost(t *testing.T) {
	revoke := func(t *testing.T, c *remoteRetry, reason RemoteCloseReason) {
		t.Helper()
		row := remoteTransactionGet(t, c.f.s, c.k)
		if _, err := c.f.s.RevokeRemote(c.k, row.Revision, reason, c.f.now.Add(3*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	failed := func(t *testing.T, c *remoteRetry, input bool) {
		t.Helper()
		completed := c.complete(t)
		s := completed.Status
		if s.State != backend.Failed || s.ExitCode != nil || s.Termination != backend.UnknownTermination || s.Reason != "remote authority lost" || completed.NextAttemptAt != nil || len(completed.Attempts) != 1 {
			t.Fatalf("authority-lost completion: %#v", completed)
		}
		raw := remoteRetryRow(t, c.f.s, c.k.AttemptID)
		if d := raw.Done; d == nil || d.Outcome != uint8(DataMainExit) || d.OriginalExit == nil || *d.OriginalExit != 1 || !reflect.DeepEqual(d.Stop, remoteStopRecord(c.completion.Stop)) || (d.Input != nil) != input {
			t.Fatalf("completion proof: %#v", d)
		}
		before := remoteTransactionDatabaseBytes(t, c.f.s)
		replay, err := c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(10*time.Second))
		if err != nil || !reflect.DeepEqual(replay, completed) {
			t.Fatalf("duplicate completion: %v", err)
		}
		remoteTransactionDatabaseUnchanged(t, c.f.s, before)
		c.f.restart(t)
		if got := remoteRetryRow(t, c.f.s, c.k.AttemptID); !reflect.DeepEqual(got, raw) {
			t.Fatalf("reopen changed completed record: %#v", got)
		}
		before = remoteTransactionDatabaseBytes(t, c.f.s)
		if replay, err = c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(11*time.Second)); err != nil || !reflect.DeepEqual(replay, completed) {
			t.Fatalf("duplicate completion after reopen: %v", err)
		}
		remoteTransactionDatabaseUnchanged(t, c.f.s, before)
		c.retire(t)
		before = remoteTransactionDatabaseBytes(t, c.f.s)
		out, run, err := c.f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, remoteRetryHashes(c.j, "second"), c.f.policy, c.f.now.Add(time.Hour))
		if err != nil || run || out.Job.CurrentAttemptID() != c.k.AttemptID || len(out.Job.Attempts) != 1 {
			t.Fatalf("began after authority loss: %v %v", run, err)
		}
		remoteTransactionDatabaseUnchanged(t, c.f.s, before)
	}
	t.Run("complete input", func(t *testing.T) {
		c := newRemoteRetry(t, "authority-input", 1, 0, true)
		revoke(t, c, RemoteAuthorityLost)
		failed(t, c, true)
	})
	t.Run("missing input", func(t *testing.T) {
		c := newRemoteRetry(t, "authority-unread", 1, 0, false)
		revoke(t, c, RemoteAuthorityLost)
		failed(t, c, false)
	})
	t.Run("healthy missing input", func(t *testing.T) {
		c := newRemoteRetry(t, "healthy-unread", 1, 0, false)
		before := remoteTransactionDatabaseBytes(t, c.f.s)
		if _, err := c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(4*time.Second)); !errors.Is(err, ErrArtifactConflict) {
			t.Fatalf("healthy retry without input: %v", err)
		}
		remoteTransactionDatabaseUnchanged(t, c.f.s, before)
	})
	for _, reason := range []RemoteCloseReason{RemoteCanceled, RemoteExpired, RemotePhaseTimeout, RemoteIdentityMismatch, RemoteShutdown} {
		t.Run(fmt.Sprintf("closed %d", reason), func(t *testing.T) {
			c := newRemoteRetry(t, "other-closure", 1, 0, true)
			revoke(t, c, reason)
			before := remoteTransactionDatabaseBytes(t, c.f.s)
			if _, err := c.f.s.CompleteRemoteAttempt(c.k, c.completion, c.f.now.Add(4*time.Second)); !errors.Is(err, ErrArtifactConflict) {
				t.Fatalf("closure %d: %v", reason, err)
			}
			remoteTransactionDatabaseUnchanged(t, c.f.s, before)
		})
	}
	t.Run("canceled outcome", func(t *testing.T) {
		c := newRemoteRetry(t, "authority-canceled", 1, 0, true)
		revoke(t, c, RemoteAuthorityLost)
		c.completion.Outcome = DataCanceled
		if j := c.complete(t); j.Status.State != backend.Canceled || j.Status.Reason != "remote execution canceled" {
			t.Fatalf("canceled outcome: %#v", j.Status)
		}
	})
	t.Run("timeout outcome", func(t *testing.T) {
		c := newRemoteRetry(t, "authority-timeout", 1, 0, true)
		revoke(t, c, RemoteAuthorityLost)
		c.completion.Outcome = DataTimeout
		if j := c.complete(t); j.Status.State != backend.Failed || j.Status.Reason != "remote execution timed out" {
			t.Fatalf("timeout outcome: %#v", j.Status)
		}
	})
}

// Oracle 5: replay never adds or changes the receipt. A selected nonzero exit
// never released output, so its result is still only a reservation: Complete
// discards it outright and the next attempt reserves a fresh one.
func TestRemoteRetryReplayKeepsReceipt(t *testing.T) {
	c := newRemoteRetry(t, "retry-replay", 1, 0, true)
	f := c.f
	reserved, deletes := remoteRetryOutputs(t, f.s)
	if len(reserved) != 1 {
		t.Fatalf("result reservations: %v", reserved)
	}
	completed := c.complete(t)
	if after, n := remoteRetryOutputs(t, f.s); len(after) != 0 || n != deletes {
		t.Fatalf("reserved result not discarded: %v deletes %d->%d", after, deletes, n)
	}
	raw := remoteRetryRow(t, f.s, c.k.AttemptID)
	before := remoteTransactionDatabaseBytes(t, f.s)
	replay, err := f.s.CompleteRemoteAttempt(c.k, c.completion, f.now.Add(10*time.Second))
	if err != nil || !reflect.DeepEqual(replay, completed) {
		t.Fatalf("lost-response replay: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	changed := c.completion
	stop := *changed.Stop
	stop.Task.ExitCode = 2
	changed.Stop = &stop
	if _, err = f.s.CompleteRemoteAttempt(c.k, changed, f.now.Add(10*time.Second)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("changed replay: %v", err)
	}
	remoteTransactionDatabaseUnchanged(t, f.s, before)
	c.retire(t)
	if got := remoteRetryRow(t, f.s, c.k.AttemptID); !reflect.DeepEqual(got.Done.Input, raw.Done.Input) {
		t.Fatalf("retirement changed receipt: %#v", got.Done.Input)
	}
	started, run, err := f.s.BeginRemoteAttempt(c.j.ID, c.k.AttemptID, remoteRetryHashes(c.j, "second"), f.policy, *completed.NextAttemptAt)
	if err != nil || !run {
		t.Fatalf("next attempt: %v %v", run, err)
	}
	after, _ := remoteRetryOutputs(t, f.s)
	if len(after) != 1 {
		t.Fatalf("next attempt result reservations: %v", after)
	}
	for id, source := range after {
		if reserved[id] != "" || source != started.Job.CurrentAttemptID() {
			t.Fatalf("next attempt result reservation: %v", after)
		}
	}
}

func remoteRetryMaximumReceipt(year int) *remoteLease {
	t := remoteTestWhen(year)
	return &remoteLease{remoteTestDigest(), math.MaxUint64, 4, t.Add(120 * time.Second), t}
}

func TestRemoteRetryReceiptCodec(t *testing.T) {
	largest, ran := 0, 0
	for _, lexical := range []bool{false, true} {
		for _, year := range []int{1, 9999} {
			for _, ch := range []byte{'<', '>', '&', '"', '\\'} {
				vectors := remoteTestVectors(ch, year, lexical)
				for _, name := range []string{"completed-stop-pending-resources", "retired-proof-compacted"} {
					t.Run(fmt.Sprintf("%t/%d/%c/%s", lexical, year, ch, name), func(t *testing.T) {
						r := remoteClone(vectors[name])
						legacy, err := remoteEncode(r)
						if err != nil {
							t.Fatal(err)
						}
						// The receipt must be absent from the exact Done object, not
						// merely from some later or nested field.
						var top, done map[string]json.RawMessage
						if err := json.Unmarshal(legacy, &top); err != nil {
							t.Fatal(err)
						}
						if err := json.Unmarshal(top["d"], &done); err != nil || done == nil {
							t.Fatalf("legacy Done: %s %v", top["d"], err)
						}
						if _, ok := done["i"]; ok {
							t.Fatalf("nil receipt changed legacy bytes: %s", legacy)
						}
						r.Done.Input = remoteRetryMaximumReceipt(year)
						st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
						if _, err := remoteRecordOwner(st, r.Job, r); err != nil {
							t.Fatalf("populated receipt: %v", err)
						}
						raw, err := remoteEncode(r)
						if err != nil {
							t.Fatalf("populated receipt exceeds cap: %v", err)
						}
						largest = max(largest, len(raw))
						ran++
						got, err := remoteDecode(raw)
						if err != nil || !reflect.DeepEqual(got, r) {
							t.Fatalf("round trip: %v", err)
						}
						clone := remoteClone(got)
						clone.Done.Input.Hash[0]--
						clone.Done.Input.Expires = clone.Done.Input.Expires.Add(time.Second)
						if again, err := remoteEncode(got); err != nil || !bytes.Equal(again, raw) {
							t.Fatalf("clone aliased receipt: %v", err)
						}
						// MODELED old strict decoder: the same record type without the field.
						type decision struct {
							Digest       [32]byte        `json:"h"`
							Outcome      uint8           `json:"o"`
							OriginalExit *int32          `json:"x,omitempty"`
							Stop         *remoteTaskStop `json:"s,omitempty"`
							NoLaunch     *remoteNoLaunch `json:"n,omitempty"`
							Collector    *remoteExit     `json:"c,omitempty"`
							ResultID     string          `json:"r,omitempty"`
						}
						old := struct {
							Job       string           `json:"j"`
							Revision  uint64           `json:"r"`
							State     uint8            `json:"s"`
							Creation  uint8            `json:"c,omitempty"`
							Authority *remoteAuthority `json:"a,omitempty"`
							Resources remoteResources  `json:"k"`
							Active    *remoteActive    `json:"v,omitempty"`
							Done      *decision        `json:"d,omitempty"`
							Ack       uint8            `json:"x,omitempty"`
						}{}
						strict := func(raw []byte) error {
							d := json.NewDecoder(bytes.NewReader(raw))
							d.DisallowUnknownFields()
							if err := d.Decode(&old); err != nil {
								return err
							}
							if d.Decode(new(any)) != io.EOF {
								return errors.New("trailing data")
							}
							return nil
						}
						if err := strict(legacy); err != nil {
							t.Fatalf("old decoder refused nil receipt: %v", err)
						}
						if err := strict(raw); err == nil {
							t.Fatal("old decoder accepted populated receipt")
						}
					})
				}
			}
		}
	}
	if ran != 40 {
		t.Fatalf("%d of 40 vectors reached the populated receipt", ran)
	}
	t.Logf("populated receipt maximum=%d bytes over all 40 vectors; fixed cap=4096", largest)
}

func TestRemoteRetryReceiptValidation(t *testing.T) {
	base := remoteTestVectors('"', 9999, true)["retired-proof-compacted"]
	tests := []struct {
		name string
		edit func(*remoteRecord)
		ok   bool
	}{
		{"valid", func(r *remoteRecord) {}, true},
		{"partial", func(r *remoteRecord) { r.Done.Input.State = 3 }, false},
		{"revoked", func(r *remoteRecord) { r.Done.Input.State = 2 }, false},
		{"zero hash", func(r *remoteRecord) { r.Done.Input.Hash = [32]byte{} }, false},
		{"zero epoch", func(r *remoteRecord) { r.Done.Input.Epoch = 0 }, false},
		{"renewed expiry", func(r *remoteRecord) { r.Done.Input.Expires = r.Done.Input.Expires.Add(time.Nanosecond) }, false},
		{"zero time", func(r *remoteRecord) {
			r.Done.Input.Renewed, r.Done.Input.Expires = time.Time{}, time.Time{}.Add(120*time.Second)
		}, false},
		{"local time", func(r *remoteRecord) {
			r.Done.Input.Renewed = r.Done.Input.Renewed.In(time.FixedZone("x", 0))
			r.Done.Input.Expires = r.Done.Input.Renewed.Add(120 * time.Second)
		}, false},
		{"creation", func(r *remoteRecord) { r.Creation = 2 }, false},
		{"no input secret", func(r *remoteRecord) { r.Resources.Input, r.Ack = nil, 5 }, false},
		{"completed", func(r *remoteRecord) { r.State, r.Ack = 3, 6 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := remoteClone(base)
			r.Done.Input = remoteRetryMaximumReceipt(9999)
			tt.edit(&r)
			st := &artifactState{Remote: map[string]remoteRecord{r.Job: r}}
			if _, err := remoteRecordOwner(st, r.Job, r); (err == nil) != tt.ok {
				t.Fatalf("remoteRecordOwner: %v, want ok %v", err, tt.ok)
			}
		})
	}
}

// TestRemoteRetryOracleFixture writes a genuine stopped Store whose retired
// row carries a populated receipt, for the pinned old-binary refusal oracle.
// It runs only when SKYGO_RETRY_ORACLE_DIR names a new empty directory.
func TestRemoteRetryOracleFixture(t *testing.T) {
	dir := os.Getenv("SKYGO_RETRY_ORACLE_DIR")
	if dir == "" {
		t.Skip("SKYGO_RETRY_ORACLE_DIR not set")
	}
	c := newRemoteRetry(t, "retry-oracle", 1, 0, true)
	c.complete(t)
	c.retire(t)
	raw := remoteRetryRow(t, c.f.s, c.k.AttemptID)
	if raw.State != 4 || raw.Done.Input == nil || raw.Done.Input.State != 4 {
		t.Fatalf("fixture receipt: %#v", raw.Done)
	}
	original := c.f.s
	c.f.s = nil
	if err := original.Close(); err != nil {
		c.f.uncertain = true
		t.Fatalf("close: %v", err)
	}
	src := filepath.Join(c.f.directory, "store")
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(dir, "store"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			t.Fatalf("unexpected store entry %s", e.Name())
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "store", e.Name()), data, 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s sha256=%x bytes=%d", e.Name(), sha256.Sum256(data), len(data))
	}
	auth, err := os.ReadFile(c.f.authPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "auth.json"), auth, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("auth.json sha256=%x bytes=%d", sha256.Sum256(auth), len(auth))
	t.Logf("job=%s attempt=%s", c.j.ID, c.k.AttemptID)
}
