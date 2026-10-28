package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These tests drive real Store transactions and database files. backend.Task
// has no Workers field, so no typed route can write a task that requests
// workers: every such row below is MODELED raw JSON corruption written with
// workersJobEdit, workersHeaderEdit or workersGraphEdit. None is a genuine
// old-writer fixture; genuine old9/old10 writers kept Workers nil, and their
// migration tests are unchanged. Each refusal is checked to leave the whole
// database, snapshot or transaction unchanged. Each accepted variant shows
// that the fence is confined to the workers members of the root task.

// workersSpec is a raw worker request. Every ordinary route refused one, as
// given, including a single independent worker.
const workersSpec = `{"count":2,"mode":"independent"}`

const (
	workersRefusal   = "requests workers"
	workersMalformed = "malformed stored record"
	workersShape     = "cannot unmarshal"
)

// A workersCase is a raw edit of stored job or cleanup header bytes. A nil
// edit leaves the row as written. want is the expected refusal, or "" for an
// accepted row. jobOnly marks an edit that changes a decoded header, whose
// Task is a pointer, and so is not a header case.
type workersCase struct {
	name    string
	edit    func(raw []byte) []byte
	want    string
	jobOnly bool
}

// workersTask inserts member first in the first task object of raw.
func workersTask(member string) func([]byte) []byte {
	return func(raw []byte) []byte {
		return bytes.Replace(raw, []byte(`"task":{`), []byte(`"task":{`+member+`,`), 1)
	}
}

// workersFirst inserts member first in the root object of raw.
func workersFirst(member string) func([]byte) []byte {
	return func(raw []byte) []byte {
		return bytes.Replace(raw, []byte("{"), []byte("{"+member+","), 1)
	}
}

// workersLast appends member to the root object of raw.
func workersLast(member string) func([]byte) []byte {
	return func(raw []byte) []byte {
		return []byte(strings.TrimSuffix(string(raw), "}") + "," + member + "}")
	}
}

func workersBoth(a, b func([]byte) []byte) func([]byte) []byte {
	return func(raw []byte) []byte { return b(a(raw)) }
}

// workersPlain is the task of every job and header case. A duplicate task
// object repeats its argv, so a merged decode yields the same Task.
func workersPlain() backend.Task { return backend.Task{Argv: []string{"work"}} }

const workersPlainTask = `{"argv":["work"]}`

var workersCases = []workersCase{
	{"absent", nil, "", false},
	{"null", workersTask(`"workers":null`), "", false},
	{"null duplicates", workersTask(`"workers":null,"Workers":null`), "", false},
	{"near miss key", workersTask(`"worker":` + workersSpec), "", false},
	{"root member", workersLast(`"workers":` + workersSpec), "", false},
	{"nested task member", workersTask(`"extra":{"workers":` + workersSpec + `}`), "", false},
	{"other root object", workersLast(`"other":{"task":{"workers":` + workersSpec + `}}`), "", false},
	{"null task duplicate", workersLast(`"task":null`), "", true},

	{"workers", workersTask(`"workers":` + workersSpec), workersRefusal, false},
	{"one independent", workersTask(`"workers":{"count":1,"mode":"independent"}`), workersRefusal, false},
	{"empty object", workersTask(`"workers":{}`), workersRefusal, false},
	{"false", workersTask(`"workers":false`), workersRefusal, false},
	{"zero", workersTask(`"workers":0`), workersRefusal, false},
	{"array", workersTask(`"workers":[]`), workersRefusal, false},
	{"string", workersTask(`"workers":""`), workersRefusal, false},
	{"title case", workersTask(`"Workers":` + workersSpec), workersRefusal, false},
	{"upper case", workersTask(`"WORKERS":` + workersSpec), workersRefusal, false},
	{"escaped key", workersTask(`"wor\u006bers":` + workersSpec), workersRefusal, false},
	{"escaped kelvin sign", workersTask(`"wor\u212aers":` + workersSpec), workersRefusal, false},
	{"kelvin sign", workersTask("\"worKers\":" + workersSpec), workersRefusal, false},
	{"later null", workersTask(`"workers":` + workersSpec + `,"workers":null`), workersRefusal, false},
	{"earlier null", workersTask(`"workers":null,"WoRkErS":` + workersSpec), workersRefusal, false},
	{"later plain task", workersBoth(workersTask(`"workers":`+workersSpec), workersLast(`"Task":`+workersPlainTask)), workersRefusal, false},
	{"later null task", workersBoth(workersTask(`"workers":`+workersSpec), workersLast(`"task":null`)), workersRefusal, false},
	{"earlier task", workersFirst(`"task":{"argv":["work"],"workers":` + workersSpec + `}`), workersRefusal, false},
	{"escaped task key", workersLast(`"ta\u0073k":{"argv":["work"],"workers":` + workersSpec + `}`), workersRefusal, false},
	{"long s task key", workersLast("\"taſk\":{\"argv\":[\"work\"],\"workers\":" + workersSpec + "}"), workersRefusal, false},

	{"string null", workersTask(`"workers":"null"`), workersRefusal, false},
	{"middle task only", workersBoth(workersTask(`"workers":`+workersSpec), workersBoth(workersFirst(`"Task":`+workersPlainTask), workersLast(`"task":`+workersPlainTask))), workersRefusal, false},
	{"kelvin sign task key", workersLast("\"tasK\":{\"argv\":[\"work\"],\"workers\":" + workersSpec + "}"), workersRefusal, false},

	// Wrong shapes pass the fence, which examines only object members, and
	// are refused by the ordinary typed decoder.
	{"task array", workersLast(`"task":[]`), workersShape, false},
	{"task string", workersLast(`"task":"workers"`), workersShape, false},
	{"task number", workersLast(`"task":0`), workersShape, false},
	{"root array", func([]byte) []byte { return []byte(`[{"task":{"workers":` + workersSpec + `}}]`) }, workersShape, false},
	{"root scalar", func([]byte) []byte { return []byte(`1`) }, workersShape, false},

	{"truncated", func(raw []byte) []byte { return raw[:len(raw)-1] }, workersMalformed, false},
	{"missing value", workersTask(`"workers":`), workersMalformed, false},
	{"trailing value", func(raw []byte) []byte { return append(raw, "{}"...) }, workersMalformed, false},
}

// workersRow returns a raw edit of the row key in the bucket named by table,
// checking that edit changes it. A nil edit changes nothing.
func workersRow(table func(*bolt.Tx) *bolt.Bucket, key string, edit func([]byte) []byte) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		if edit == nil {
			return nil
		}
		b := table(tx)
		old := b.Get([]byte(key))
		if old == nil {
			return fmt.Errorf("no row %s", key)
		}
		raw := edit(bytes.Clone(old))
		if bytes.Equal(raw, old) {
			return errors.New("raw edit changed nothing")
		}
		return b.Put([]byte(key), raw)
	}
}

func workersJobEdit(id string, edit func([]byte) []byte) func(*bolt.Tx) error {
	return workersRow(func(tx *bolt.Tx) *bolt.Bucket { return tx.Bucket(jobsBucket) }, id, edit)
}

// workersHeaderEdit edits a cleanup header. A header's charge is that of its
// decoded form, so an unknown member changes no accounting.
func workersHeaderEdit(id string, edit func([]byte) []byte) func(*bolt.Tx) error {
	return workersRow(func(tx *bolt.Tx) *bolt.Bucket { return retentionTable(tx, cleanupHeadersBucket) }, id, edit)
}

func workersJobBytes(t *testing.T, s *Store, id string) []byte {
	t.Helper()
	var raw []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		raw = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(id)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return raw
}

// workersRestoreRefused checks that Restore refuses snapshot with want,
// changes no byte of it and creates no destination.
func workersRestoreRefused(t *testing.T, snapshot, want string) {
	t.Helper()
	before, err := os.ReadFile(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	refused := Restore(snapshot, restored)
	if refused == nil || !strings.Contains(refused.Error(), want) {
		t.Fatalf("restore: %v, want %q", refused, want)
	}
	if _, err := os.Stat(restored); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused restore created its destination: %v", err)
	}
	if after, err := os.ReadFile(snapshot); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused restore changed its snapshot: %v", err)
	}
}

// workersBackupRefused checks that Backup refuses the stopped installation
// in dir with want, changes no byte of it and creates no output.
func workersBackupRefused(t *testing.T, dir, want string) {
	t.Helper()
	path := filepath.Join(dir, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "backup.db")
	refused := Backup(dir, output)
	if refused == nil || !strings.Contains(refused.Error(), want) {
		t.Fatalf("backup: %v, want %q", refused, want)
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused backup created its output: %v", err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused backup changed its source: %v", err)
	}
}

// workersClosed applies edit to the stopped installation in dir and to a
// snapshot taken from it first, since Backup validates its source. A refused
// edit is refused by Open, Restore and Backup with an error containing want,
// with no byte changed; otherwise all three accept it.
func workersClosed(t *testing.T, dir string, edit func(*bolt.Tx) error, refused bool, want string) {
	t.Helper()
	snapshots := t.TempDir()
	snapshot := filepath.Join(snapshots, "jobs.db")
	if err := Backup(dir, snapshot); err != nil {
		t.Fatal(err)
	}
	editFixture(t, dir, false, edit)
	editFixture(t, snapshots, false, edit)
	if refused {
		environmentRefused(t, dir, want)
		workersRestoreRefused(t, snapshot, want)
		workersBackupRefused(t, dir, want)
		return
	}
	if err := Restore(snapshot, filepath.Join(t.TempDir(), "restored")); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := Backup(dir, filepath.Join(t.TempDir(), "backup.db")); err != nil {
		t.Fatalf("backup: %v", err)
	}
}

func TestWorkersStoredJobClosed(t *testing.T) {
	for _, tt := range workersCases {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			j, err := s.Submit(testActor(), "job", workersPlain(), "local", "test-binding")
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			var edited []byte
			edit := workersJobEdit(j.ID, tt.edit)
			workersClosed(t, dir, func(tx *bolt.Tx) error {
				if err := edit(tx); err != nil {
					return err
				}
				edited = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(j.ID)))
				return nil
			}, tt.want != "", tt.want)
			if tt.want != "" {
				return
			}
			s, err = Open(dir)
			if err != nil {
				t.Fatalf("accepted row refused: %v", err)
			}
			defer s.Close()
			got, err := s.Get(j.ID)
			if err != nil {
				t.Fatal(err)
			}
			have, _ := backend.TaskBytes(got.Task)
			want, _ := backend.TaskBytes(workersPlain())
			if !bytes.Equal(have, want) {
				t.Fatalf("task %s, want %s", have, want)
			}
			// Open neither normalizes nor rewrites an accepted row.
			if raw := workersJobBytes(t, s, j.ID); !bytes.Equal(raw, edited) {
				t.Fatalf("open rewrote the row:\n%s\nwant\n%s", raw, edited)
			}
		})
	}
}

// TestWorkersOrdinaryEnvironment is a genuine ordinary job, not a modeled
// row: a workers key or value inside the task's environment is data.
func TestWorkersOrdinaryEnvironment(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Env: map[string]string{"workers": workersSpec, "Workers": "null"}}
	j, err := s.Submit(testActor(), "env", task, "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	workersClosed(t, dir, func(*bolt.Tx) error { return nil }, false, "")
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	have, _ := backend.TaskBytes(got.Task)
	want, _ := backend.TaskBytes(task)
	if !bytes.Equal(have, want) {
		t.Fatalf("task %s, want %s", have, want)
	}
}

// TestWorkersStoredJobLive models a row corrupted after Open. Every job read
// and effect refuses it before any branch: queued, canceled before
// execution, retrying and terminal jobs alike, so no row is changed.
func TestWorkersStoredJobLive(t *testing.T) {
	retry := &backend.RetryPolicy{MaxAttempts: 3, DelaySeconds: 1, ExitCodes: []int32{7}}
	for _, state := range []string{"queued", "canceled", "retrying", "terminal"} {
		for _, tt := range workersCases {
			if tt.want != workersRefusal {
				continue
			}
			t.Run(state+"/"+tt.name, func(t *testing.T) {
				s := openTest(t)
				task := workersPlain()
				if state == "retrying" {
					task.Retry = retry
				}
				j, err := s.Submit(testActor(), "job", task, "local", "test-binding")
				if err != nil {
					t.Fatal(err)
				}
				now := j.CreatedAt.Add(time.Second)
				switch state {
				case "canceled":
					// Queued with intent: an unfenced core would cancel it.
					if j, err = s.Update(j.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
						t.Fatal(err)
					}
				case "retrying":
					j = failTest(t, s, beginTest(t, s, j, now), now.Add(time.Second))
					if j.Status.State != backend.Retrying || j.NextAttemptAt == nil {
						t.Fatalf("not retrying: %+v", j.Status)
					}
					now = *j.NextAttemptAt
				case "terminal":
					j = beginTest(t, s, j, now)
					code := int32(0)
					if j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "modeled-instance", ExitCode: &code, Termination: backend.StoppedWithExitCode}, now.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
				}
				if err := s.db.Update(workersJobEdit(j.ID, tt.edit)); err != nil {
					t.Fatal(err)
				}
				before := workflowDatabase(t, s)
				if _, err := s.Get(j.ID); !errors.Is(err, errWorkerRoute) {
					t.Fatalf("get: %v, want worker refusal", err)
				}
				if _, err := s.List(0, j.Sequence, 10); !errors.Is(err, errWorkerRoute) {
					t.Fatalf("list: %v, want worker refusal", err)
				}
				if got, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now); !errors.Is(err, errWorkerRoute) || start {
					t.Fatalf("begin: start=%v err=%v %+v, want worker refusal", start, err, got.Status)
				}
				called := false
				if _, err := s.Update(j.ID, func(j *backend.Job) error { called = true; j.CancelRequested = true; return nil }); !errors.Is(err, errWorkerRoute) || called {
					t.Fatalf("update: called=%v err=%v, want worker refusal", called, err)
				}
				code := int32(1)
				if _, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Failed, Instance: "modeled-instance", ExitCode: &code, Termination: backend.StoppedWithExitCode}, now.Add(time.Second)); err == nil {
					t.Fatal("observe accepted a worker row")
				}
				workersCoreRefused(t, s, j, now)
				workflowUnchanged(t, s, before)
			})
		}
	}
}

// workersCoreRefused checks the refusal in beginAttemptCore itself, which
// BeginAttempt and every BeginRemoteAttempt start path share, within one
// transaction that is then rolled back.
func workersCoreRefused(t *testing.T, s *Store, j backend.Job, now time.Time) {
	t.Helper()
	candidate, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	rollback := errors.New("rollback")
	var start, same bool
	var coreErr error
	err = s.db.Update(func(tx *bolt.Tx) error {
		before := bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(j.ID)))
		_, start, coreErr = beginAttemptCore(tx, j.ID, j.CurrentAttemptID(), candidate, 0, now)
		same = bytes.Equal(before, tx.Bucket(jobsBucket).Get([]byte(j.ID)))
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !errors.Is(coreErr, errWorkerRoute) || start || !same {
		t.Fatalf("core: start=%v err=%v unchanged=%v, want worker refusal", start, coreErr, same)
	}
}

// TestWorkersStoredJobLiveNull is the accepted twin of TestWorkersStoredJobLive.
func TestWorkersStoredJobLiveNull(t *testing.T) {
	s := openTest(t)
	j, err := s.Submit(testActor(), "job", workersPlain(), "local", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(workersJobEdit(j.ID, workersTask(`"workers":null,"Workers":null`))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(j.ID); err != nil {
		t.Fatal(err)
	}
	got, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, j.CreatedAt.Add(time.Second))
	if err != nil || !start || got.Status.State != backend.Starting {
		t.Fatalf("null-worker begin: start=%v err=%v %+v", start, err, got.Status)
	}
}

// workersDataTask is a remote data task without a workdir, so that no
// artifact pin or completion holds its digest.
func workersDataTask() backend.Task {
	return backend.Task{Argv: []string{"true"}, Image: "modeled-image", DataMounts: []backend.DataMount{{Reference: "dataset", Destination: "/mnt/skygo/dataset", ReadOnly: true}}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1}}}
}

// workersRemoteRetry settles the first attempt of j at a selected exit and
// retires it, as TestRemoteRetryWithoutWorkdir does. It returns the attempt
// and the job, which is then Retrying.
func workersRemoteRetry(t *testing.T, f *remoteTransactionFixture, j backend.Job) (RemoteKey, backend.Job) {
	t.Helper()
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
		t.Fatalf("not retrying: %+v", completed.Status)
	}
	c.retire(t)
	return k, completed
}

func TestWorkersBeginRemoteAttemptRefused(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, workers := range []bool{false, true} {
			t.Run(fmt.Sprintf("retry=%v workers=%v", retry, workers), func(t *testing.T) {
				f := newRemoteTransactionFixture(t)
				j := f.submit(t, "data", workersDataTask())
				expected, now := j.ID, f.now
				if retry {
					k, completed := workersRemoteRetry(t, f, j)
					expected, now = k.AttemptID, *completed.NextAttemptAt
				}
				member := `"workers":null`
				if workers {
					member = `"Workers":` + workersSpec + `,"workers":null`
				}
				if err := f.s.db.Update(workersJobEdit(j.ID, workersTask(member))); err != nil {
					t.Fatal(err)
				}
				before := workflowDatabase(t, f.s)
				out, run, err := f.s.BeginRemoteAttempt(j.ID, expected, RemoteHashes{}, f.policy, now)
				if !workers {
					if err != nil || !run || out.Job.Status.State != backend.Starting {
						t.Fatalf("null-worker begin: run=%v err=%v %+v", run, err, out.Job.Status)
					}
					return
				}
				if !errors.Is(err, errWorkerRoute) || run {
					t.Fatalf("worker begin: run=%v err=%v, want worker refusal", run, err)
				}
				workflowUnchanged(t, f.s, before)
			})
		}
	}
}

// workersHeaderFixture returns a stopped retained installation in dir whose
// only job was pruned Whole after a native exit, leaving a dangling cleanup
// header that holds a copy of the task and one native cleanup.
func workersHeaderFixture(t *testing.T) (dir, id string) {
	t.Helper()
	dir = t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := EnableRetention(dir, DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	j, err := s.SubmitRetained(testActor(), info.Domain, "header", workersPlain(), "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	j, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now)
	if err != nil || !start {
		t.Fatalf("begin: %v %v", start, err)
	}
	code := int32(0)
	if _, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "modeled-instance", ExitCode: &code, Termination: backend.StoppedWithExitCode}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	page, err := s.Prune(testActor(), PruneSelection{Before: now.Add(time.Minute), Mode: Whole, Max: j.Sequence, Limit: 1}, true, nil, nil, now.Add(2*time.Second))
	if err != nil || page.Marked != 1 {
		t.Fatalf("Whole: %+v %v", page, err)
	}
	if _, err := s.Get(j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Whole kept the job: %v", err)
	}
	var h cleanupHeader
	if err := s.db.View(func(tx *bolt.Tx) error { return loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(j.ID), &h) }); err != nil || h.Task == nil {
		t.Fatalf("no dangling header with a task: %+v %v", h, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, j.ID
}

// TestWorkersCleanupHeaderClosed refuses a dangling cleanup header whose job
// is gone, so no job fence can refuse it. The decoded header is unchanged by
// every well-formed edit, so neither its digest nor its charge can refuse
// first. The header fence runs where the charges are rederived, before any
// charge of a malformed or worker header is computed.
func TestWorkersCleanupHeaderClosed(t *testing.T) {
	for _, tt := range workersCases {
		if tt.jobOnly {
			continue
		}
		t.Run(tt.name, func(t *testing.T) {
			dir, id := workersHeaderFixture(t)
			workersClosed(t, dir, workersHeaderEdit(id, tt.edit), tt.want != "", tt.want)
			if tt.want != "" {
				return
			}
			s, err := Open(dir)
			if err != nil {
				t.Fatalf("accepted header refused: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWorkersCleanupHeaderLive models a header corrupted after Open. Native
// cleanup would hand its task to a backend, and finishing it would rewrite
// the header from its decoded form; both refuse before any effect.
func TestWorkersCleanupHeaderLive(t *testing.T) {
	for _, tt := range []struct {
		name    string
		member  string
		refused bool
	}{
		{"null", `"workers":null`, false},
		{"workers", `"workers":` + workersSpec, true},
		{"escaped later null", `"\u0057orkers":` + workersSpec + `,"workers":null`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, id := workersHeaderFixture(t)
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			rec, found, err := s.NextCleanup(0)
			if err != nil || !found || rec.HeaderID != id {
				t.Fatalf("no native cleanup: %+v %v %v", rec, found, err)
			}
			if err := s.db.Update(workersHeaderEdit(id, workersTask(tt.member))); err != nil {
				t.Fatal(err)
			}
			before := workflowDatabase(t, s)
			now := time.Now().UTC()
			if !tt.refused {
				again, found, err := s.NextCleanup(0)
				if err != nil || !found || again.ID != rec.ID || again.Digest != rec.Digest {
					t.Fatalf("null-worker header: %+v %v %v", again, found, err)
				}
				if err := s.FinishCleanup(rec.ID, rec.Operation, rec.Digest, now); err != nil {
					t.Fatalf("null-worker finish: %v", err)
				}
				return
			}
			if _, _, err := s.NextCleanup(0); !errors.Is(err, errWorkerRoute) {
				t.Fatalf("next: %v, want worker refusal", err)
			}
			if err := s.FinishCleanup(rec.ID, rec.Operation, rec.Digest, now); !errors.Is(err, errWorkerRoute) {
				t.Fatalf("finish: %v, want worker refusal", err)
			}
			workflowUnchanged(t, s, before)
		})
	}
}

// TestWorkersCleanupHeaderPrune drives the actual prune planner and the output
// closure mutation at a dangling header corrupted after Open. Both reach the
// header only through subject, which must refuse it before any planned or
// accounting write; before the header fence, a header that does not need
// manual cleanup was reported as not found.
func TestWorkersCleanupHeaderPrune(t *testing.T) {
	for _, tt := range []struct {
		name    string
		member  string
		refused bool
	}{
		{"null", `"workers":null`, false},
		{"workers", `"workers":` + workersSpec, true},
		{"title case later null", `"Workers":` + workersSpec + `,"workers":null`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir, id := workersHeaderFixture(t)
			s, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.db.Update(workersHeaderEdit(id, workersTask(tt.member))); err != nil {
				t.Fatal(err)
			}
			before := workflowDatabase(t, s)
			now := time.Now().UTC()
			_, prune := s.Prune(testActor(), PruneSelection{ObligationID: id, Mode: OutputOnly}, true, nil, nil, now)
			closure := s.RecordOutputClosure(backend.OutputReference{Owner: s.owner, JobID: id, AttemptID: id, ClosureID: "modeled-closure"}, [32]byte{})
			if !tt.refused {
				// This valid automatic-cleanup header has a task and is not
				// a manual obligation: both APIs must reach that established
				// post-decode refusal, not fail at the raw workers fence.
				if !errors.Is(prune, ErrNotFound) || !errors.Is(closure, ErrNotFound) {
					t.Fatalf("null-worker header: %v, %v, want not found", prune, closure)
				}
				workflowUnchanged(t, s, before)
				return
			}
			if !errors.Is(prune, errWorkerRoute) {
				t.Fatalf("prune: %v, want worker refusal", prune)
			}
			if !errors.Is(closure, errWorkerRoute) {
				t.Fatalf("closure: %v, want worker refusal", closure)
			}
			workflowUnchanged(t, s, before)
		})
	}
}

// TestWorkersBindOutputStorage drives the actual placement declaration, which
// scans every stored job before writing any retention metadata.
func TestWorkersBindOutputStorage(t *testing.T) {
	for _, tt := range []struct {
		name    string
		member  string
		refused bool
	}{
		{"null", `"workers":null`, false},
		{"workers", `"workers":` + workersSpec, true},
		{"escaped later null", `"\u0077orkers":` + workersSpec + `,"workers":null`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, info := enableTest(t, DefaultRetentionPolicy())
			j, err := s.SubmitRetained(testActor(), info.Domain, "bind", workersPlain(), "remote", "test-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.db.Update(workersJobEdit(j.ID, workersTask(tt.member))); err != nil {
				t.Fatal(err)
			}
			before := workflowDatabase(t, s)
			err = s.BindOutputStorage("remote", "test-binding", backend.RemoteOutput, 0)
			if !tt.refused {
				if err != nil {
					t.Fatalf("null-worker bind: %v", err)
				}
				return
			}
			if !errors.Is(err, errWorkerRoute) {
				t.Fatalf("bind: %v, want worker refusal", err)
			}
			workflowUnchanged(t, s, before)
		})
	}
}

// workersLastTask inserts member first in the last task object of raw: in a
// graph, that of the last node.
func workersLastTask(member string) func([]byte) []byte {
	return func(raw []byte) []byte {
		i := bytes.LastIndex(raw, []byte(`"task":{`))
		if i < 0 {
			return raw
		}
		i += len(`"task":{`)
		return []byte(string(raw[:i]) + member + "," + string(raw[i:]))
	}
}

// workersGraphEdit edits the stored graph of workflow id and, as a writer
// of that graph would, its digest in the state and request rows and the task
// byte charge, so that only the exact canonical comparison can refuse it.
func workersGraphEdit(id string, edit func([]byte) []byte) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		b := tx.Bucket(workflowsBucket)
		old := b.Get(workflowGraphKey(id))
		raw := edit(bytes.Clone(old))
		if old == nil || bytes.Equal(raw, old) {
			return errors.New("raw edit changed nothing")
		}
		st, err := loadWorkflowState(tx, id)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(raw)
		st.Graph = hex.EncodeToString(digest[:])
		key := requestKey(st.Owner, st.Domain, st.RequestID)
		var r requestRecord
		if err := loadJSON(retentionTable(tx, retainedRequestsBucket), key, &r); err != nil {
			return err
		}
		r.GraphDigest = st.Graph
		if err := putJSON(retentionTable(tx, retainedRequestsBucket), key, r); err != nil {
			return err
		}
		if err := putWorkflowState(tx, id, st); err != nil {
			return err
		}
		u, err := usage(tx)
		if err != nil {
			return err
		}
		u[TaskBytesQuota] += uint64(len(raw) - len(old))
		if err := putJSON(retentionRoot(tx), retentionUsageKey, u); err != nil {
			return err
		}
		return b.Put(workflowGraphKey(id), raw)
	}
}

// TestWorkersStoredWorkflowGraph refuses a graph node task with workers in
// every disposition, including a pending node that has no child job for the
// job fence to refuse. The graph keeps its exact canonical comparison, which
// is stricter than the job fence: even a raw null workers member, or any
// other member Graph lacks, is not canonical and is refused. Nothing
// normalizes the graph.
func TestWorkersStoredWorkflowGraph(t *testing.T) {
	const want = "workflow graph mismatch"
	for _, tt := range []struct {
		name   string
		settle func(*testing.T, *workflowFixture, Workflow)
		want   WorkflowDisposition
	}{
		{"pending", func(*testing.T, *workflowFixture, Workflow) {}, WorkflowPending},
		{"canceled", func(t *testing.T, f *workflowFixture, w Workflow) {
			if _, err := f.s.CancelWorkflowFor(f.alice, w.ID); err != nil {
				t.Fatal(err)
			}
		}, WorkflowCanceled},
		{"skipped", func(t *testing.T, f *workflowFixture, w Workflow) {
			a := f.admitOne(t, w, "a")
			f.finish(t, a.ID, backend.Failed)
			f.candidates(t)
		}, WorkflowSkipped},
	} {
		for _, member := range []string{"", `"workers":null`, `"workers":` + workersSpec, `"WORKERS":` + workersSpec + `,"workers":null`} {
			t.Run(fmt.Sprintf("%s/%s", tt.name, member), func(t *testing.T) {
				f := newWorkflowFixture(t)
				w := f.submit(t, "graph", workflowGraph("a", "w<a"))
				tt.settle(t, f, w)
				if got := f.get(t, w.ID).Nodes[1].Disposition; got != tt.want {
					t.Fatalf("node w disposition %v, want %v", got, tt.want)
				}
				if err := f.s.Close(); err != nil {
					t.Fatal(err)
				}
				if member == "" {
					workersClosed(t, f.dir, func(*bolt.Tx) error { return nil }, false, "")
					return
				}
				workersClosed(t, f.dir, workersGraphEdit(w.ID, workersLastTask(member)), true, want)
			})
		}
	}
}

// TestWorkersStoredWorkflowGraphLive models a graph corrupted after Open
// whose pending node has no child job. Candidate evaluation and child
// admission decode the graph with loadWorkflowGraph, which refuses it, so
// the node is never admitted as an ordinary child.
func TestWorkersStoredWorkflowGraphLive(t *testing.T) {
	f := newWorkflowFixture(t)
	w := f.submit(t, "graph", workflowGraph("w"))
	var c WorkflowCandidate
	for range 4 {
		for _, got := range f.candidates(t) {
			if got.Workflow == w.ID {
				c = got
			}
		}
	}
	if c.Workflow == "" {
		t.Fatal("node w never became a candidate")
	}
	if err := f.s.db.Update(workersGraphEdit(w.ID, workersLastTask(`"workers":`+workersSpec))); err != nil {
		t.Fatal(err)
	}
	before := workflowDatabase(t, f.s)
	if _, err := f.s.WorkflowCandidates(); err == nil || !strings.Contains(err.Error(), "workflow graph mismatch") {
		t.Fatalf("candidates: %v, want graph refusal", err)
	}
	workflowUnchanged(t, f.s, before)
	// The edit changed the graph digest, so admission may refuse the earlier
	// candidate as stale before it loads the graph; either way it refuses.
	if j, err := f.s.AdmitWorkflowChild(f.admission(c)); err == nil {
		t.Fatalf("admitted %+v from a non-canonical graph", j)
	}
	workflowUnchanged(t, f.s, before)
}
