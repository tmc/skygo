package store

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These independent test-only observations walk a bounded tiny fixture, never
// a production database. They do not call the production ledger for their oracle.
// All tests are UNRUN; physical/archive/writer receipts remain MODELED.
type artifactObservedRows struct {
	rows      map[string][]byte
	sequences map[string]uint64
	buckets   map[string]bool
}

func artifactObserveRows(t *testing.T, tx *bolt.Tx) artifactObservedRows {
	t.Helper()
	out := artifactObservedRows{map[string][]byte{}, map[string]uint64{}, map[string]bool{}}
	count, size := 0, 0
	var walk func(*bolt.Bucket, string)
	walk = func(b *bolt.Bucket, path string) {
		out.buckets[path] = true
		out.sequences[path] = b.Sequence()
		if err := b.ForEach(func(k, v []byte) error {
			if v == nil {
				walk(b.Bucket(k), path+"/"+string(k))
				return nil
			}
			count++
			size += len(k) + len(v)
			if count > 256 || size > 2<<20 {
				t.Fatal("observation fixture exceeded finite bound")
			}
			out.rows[path+"\x00"+string(k)] = bytes.Clone(v)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error { walk(b, string(name)); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}
func artifactObservedDelta(before, after artifactObservedRows, artifacts bool) (n uint64, created int) {
	selected := func(path string) bool {
		return (path == string(artifactsBucket) || strings.HasPrefix(path, string(artifactsBucket)+"/") || strings.HasPrefix(path, string(artifactsBucket)+"\x00")) == artifacts
	}
	keys := map[string]bool{}
	for k := range before.rows {
		keys[k] = true
	}
	for k := range after.rows {
		keys[k] = true
	}
	for k := range keys {
		if !selected(k) || bytes.Equal(before.rows[k], after.rows[k]) {
			continue
		}
		_, key, ok := strings.Cut(k, "\x00")
		if !ok {
			continue
		}
		n += uint64(len(key) + len(before.rows[k]) + len(after.rows[k]))
		if before.rows[k] == nil && after.rows[k] != nil {
			created++
		}
	}
	for path, sequence := range after.sequences {
		if !selected(path) {
			continue
		}
		if !before.buckets[path] {
			_, name, _ := strings.Cut(path, "/")
			n += uint64(len(name) + 16)
			created++
		} else if sequence != before.sequences[path] {
			n += 16
		}
	}
	return
}
func artifactObserveStore(t *testing.T, s *Store) (out artifactObservedRows) {
	t.Helper()
	if err := s.db.View(func(tx *bolt.Tx) error { out = artifactObserveRows(t, tx); return nil }); err != nil {
		t.Fatal(err)
	}
	return
}

func TestArtifactCoreLedgerActualEntryObservations(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[retained], func(t *testing.T) {
			var s *Store
			var domain *RequestDomain
			if retained {
				p := DefaultRetentionPolicy()
				p.LocalOutputEnabled = false
				var info RetentionInfo
				s, info = enableTest(t, p)
				domain = &info.Domain
			} else {
				s = openTest(t)
			}
			artifactTestInstall(t, s)
			input := artifactTestComplete(t, s, testActor())
			task := artifactInputTask(input)
			before := artifactObserveStore(t, s)
			j, err := s.SubmitWorkspace(testActor(), domain, "ledger-accept", task, "remote", "test-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			check := func(phase string) {
				t.Helper()
				after := artifactObserveStore(t, s)
				core, _ := artifactObservedDelta(before, after, false)
				artifact, _ := artifactObservedDelta(before, after, true)
				if core == 0 || artifact == 0 || core+artifact > maxChangeBytes {
					t.Fatalf("%s independent bytes core=%d artifact=%d", phase, core, artifact)
				}
				t.Logf("%s actual logical changes core=%d artifact=%d", phase, core, artifact)
				before = after
			}
			check("accept")
			var start bool
			j, start, err = s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, s.now())
			if err != nil || !start {
				t.Fatalf("begin %v %v", start, err)
			}
			check("begin")
			j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, s.now())
			if err != nil {
				t.Fatal(err)
			}
			check("observe")
			artifactRegressionComplete(t, s, j, false)
			check("complete")
		})
	}
}

func TestArtifactCoreLedgerCombinedBoundaryRollback(t *testing.T) {
	// Exercise an actual second attempt: prospective core writes include its
	// new history/index entry, metadata counters and Starting job transition.
	s := openTest(t)
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, testActor())
	task := artifactInputTask(input)
	task.Outputs = []string{"out"}
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}, DelaySeconds: 1}
	j, err := s.SubmitWorkspace(testActor(), nil, "ledger-retry", task, "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	j, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, s.now())
	if err != nil || !start {
		t.Fatal(start, err)
	}
	j, err = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, s.now())
	if err != nil {
		t.Fatal(err)
	}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	execution := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: "i"}
	one := int32(1)
	completion := WorkspaceCompletion{Outcome: WorkspaceTaskExit, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: j.ID, Execution: execution}, Origin: maintenancePhysicalOrigin(true)}
	commit, err := s.CompleteWorkspaceAttempt(j.ID, execution, backend.Status{State: backend.Failed, Instance: "i", ExitCode: &one, Termination: backend.StoppedWithExitCode}, completion, s.now())
	if err != nil {
		t.Fatal(err)
	}
	j = commit.Job
	artifactRegressionRemove(t, s, execution.ID, s.now())
	if j.NextAttemptAt == nil {
		t.Fatal("retry deadline missing")
	}
	candidate, result := strings.Repeat("d", 32), strings.Repeat("e", 32)
	rollback := errors.New("preserve prospective measurement")
	var observed uint64
	var created int
	apply := func(tx *bolt.Tx, budget uint64, records int) error {
		ledger := artifactAttemptLedger(tx, j.ID, candidate)
		before := artifactObserveRows(t, tx)
		core, err := artifactCoreChange(tx, ledger, budget, records, func(st *artifactState) error {
			next, start, e := beginAttemptCore(tx, j.ID, j.CurrentAttemptID(), candidate, 0, *j.NextAttemptAt)
			if e != nil {
				return e
			}
			if !start {
				return ErrArtifactConflict
			}
			return artifactBeginWorkspace(tx, st, j.ID, next.CurrentAttemptID(), result, true, *j.NextAttemptAt)
		})
		if err != nil {
			return err
		}
		after := artifactObserveRows(t, tx)
		coreBytes, coreRecords := artifactObservedDelta(before, after, false)
		artifactBytes, artifactRecords := artifactObservedDelta(before, after, true)
		if core.bytes != coreBytes || core.created != coreRecords {
			t.Fatalf("missed actual core changes: got%+v rawBytes=%d rawRecords=%d", core, coreBytes, coreRecords)
		}
		observed = coreBytes + artifactBytes
		created = coreRecords + artifactRecords
		return rollback
	}
	original := artifactDatabaseBytes(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, maxChangeBytes, 100) }); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if observed == 0 || observed >= maxChangeBytes || created < 2 {
		t.Fatalf("invalid measured boundary %d/%d", observed, created)
	}
	for _, tt := range []struct {
		name    string
		bytes   uint64
		records int
		want    error
	}{
		{"exact", observed, created, rollback},
		{"one-byte-short", observed - 1, 100, errArtifactMutationBudget},
		{"one-record-short", maxChangeBytes, created - 1, errArtifactMutationBudget},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, tt.bytes, tt.records) })
			if !errors.Is(err, tt.want) {
				t.Fatalf("boundary %s: %v", tt.name, err)
			}
			if !bytes.Equal(original, artifactDatabaseBytes(t, s)) {
				t.Fatal("shared guard failed to roll back job/history/index/counters/result")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArtifactCoreLedgerScopedAcceptObservations(t *testing.T) {
	f := newScopeFixture(t)
	s := f.s
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, f.actors["alice"])
	before := artifactObserveStore(t, s)
	j, err := s.SubmitWorkspace(f.actors["alice"], nil, "scope-ledger", artifactInputTask(input), "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	after := artifactObserveStore(t, s)
	for _, root := range [][]byte{principalIndexBucket, principalRequestsBucket} {
		path := string(root) + "/alice"
		if before.buckets[path] || !after.buckets[path] {
			t.Fatalf("fixture did not exercise new scoped bucket %s", path)
		}
	}
	core, created := artifactObservedDelta(before, after, false)
	artifact, _ := artifactObservedDelta(before, after, true)
	if core == 0 || artifact == 0 || created < 2 || core+artifact > maxChangeBytes {
		t.Fatalf("scoped accept ledger bytes=%d+%d created=%d", core, artifact, created)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if string(tx.Bucket(jobPrincipalsBucket).Get([]byte(j.ID))) != "alice" {
			t.Fatal("owner missing")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}
