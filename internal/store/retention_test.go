package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func enableTest(t *testing.T, p RetentionPolicy) (*Store, RetentionInfo) {
	t.Helper()
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	info, e := EnableRetention(dir, p)
	if e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, info
}
func retainedSubmit(t *testing.T, s *Store, d RequestDomain, id string) backend.Job {
	t.Helper()
	j, e := s.SubmitRetained(testActor(), d, id, backend.Task{Argv: []string{"work"}}, "remote", "test-binding", backend.RemoteOutput, 0)
	if e != nil {
		t.Fatal(e)
	}
	return j
}
func expiryIs(t *testing.T, err error, reason ExpiryReason) {
	t.Helper()
	var e *ExpiryError
	if !errors.As(err, &e) || e.Reason != reason {
		t.Fatalf("got %v, want expiry %d", err, reason)
	}
}
func TestRetentionPruneRecoversQuotaAndPreservesRequest(t *testing.T) {
	p := DefaultRetentionPolicy()
	p.Jobs = 1
	s, info := enableTest(t, p)
	j := retainedSubmit(t, s, info.Domain, "old")
	if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
		t.Fatal(e)
	}
	_, e := s.SubmitRetained(testActor(), info.Domain, "full", j.Task, "remote", "test-binding", backend.RemoteOutput, 0)
	var cap *CapacityError
	if !errors.As(e, &cap) || cap.Domain != JobsQuota {
		t.Fatalf("capacity: %v", e)
	}
	sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	before, e := os.ReadFile(filepath.Join(filepath.Dir(s.db.Path()), "jobs.db"))
	if e != nil {
		t.Fatal(e)
	}
	dry, e := s.Prune(testActor(), sel, false, nil, nil, time.Now())
	if e != nil || dry.Selected != 1 || dry.Marked != 0 {
		t.Fatalf("dry-run %+v %v", dry, e)
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("dry-run changed database: %v", e)
	}
	done, e := s.Prune(testActor(), sel, true, nil, nil, time.Now())
	if e != nil || done.Marked != 1 {
		t.Fatalf("prune %+v %v", done, e)
	}
	_, _, e = s.LookupRetained(testActor(), info.Domain, "old", j.Task)
	expiryIs(t, e, ResultExpired)
	fresh := retainedSubmit(t, s, info.Domain, "new")
	if fresh.Sequence != 2 {
		t.Fatalf("sequence reused: %d", fresh.Sequence)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionAcceptClosedSurvivesClockRollback(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "known")
	s.now = func() time.Time { return info.AcceptUntil }
	_, _, e := s.LookupRetained(testActor(), info.Domain, "absent", j.Task)
	expiryIs(t, e, RequestEpochExpired)
	s.now = func() time.Time { return info.AcceptUntil.Add(-time.Hour) }
	_, _, e = s.LookupRetained(testActor(), info.Domain, "still-absent", j.Task)
	expiryIs(t, e, RequestEpochExpired)
	again, found, e := s.LookupRetained(testActor(), info.Domain, "known", j.Task)
	if e != nil || !found || again.ID != j.ID {
		t.Fatalf("known retry: %+v %v %v", again, found, e)
	}
	s.now = func() time.Time { return info.RetryUntil }
	_, _, e = s.LookupRetained(testActor(), info.Domain, "known", j.Task)
	expiryIs(t, e, RequestEpochExpired)
	s.now = func() time.Time { return info.AcceptUntil.Add(-time.Hour) }
	_, _, e = s.LookupRetained(testActor(), info.Domain, "known", j.Task)
	expiryIs(t, e, RequestEpochExpired)
}
func TestRetentionConcurrentRequestSingleAcceptance(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, e := s.SubmitRetained(testActor(), info.Domain, "one", backend.Task{Argv: []string{"work"}}, "remote", "test-binding", backend.RemoteOutput, 0)
			ids <- j.ID
			errs <- e
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("duplicate logical acceptance %s %s", first, id)
		}
	}
	if e := s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionUnknownAndBackoffProtected(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "unknown")
	if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Unknown; return nil }); e != nil {
		t.Fatal(e)
	}
	out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, time.Now())
	if e != nil || out.Marked != 0 || out.Skipped != 1 {
		t.Fatalf("unknown pruned %+v %v", out, e)
	}
	if _, e = s.Get(j.ID); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionOutputReserveFailureDoesNotAllocateAttempt(t *testing.T) {
	p := DefaultRetentionPolicy()
	p.OutputBytes = 1
	s, info := enableTest(t, p)
	var st retentionState
	if e := s.db.View(func(tx *bolt.Tx) error { var e error; st, e = loadRetention(tx); return e }); e != nil {
		t.Fatal(e)
	}
	r := uint64(backend.MaxLogBytes) + 4*st.Policy.OutputUnit
	j, e := s.SubmitRetained(testActor(), info.Domain, "local", backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}}, "local", "test-binding", backend.LocalOutput, r)
	if e != nil {
		t.Fatal(e)
	}
	_, start, e := s.BeginAttempt(j.ID, j.ID, r, time.Now())
	var cap *CapacityError
	if !errors.As(e, &cap) || start {
		t.Fatalf("reservation accepted %v %v", start, e)
	}
	got, e := s.Get(j.ID)
	if e != nil || got.Status.State != backend.Queued || len(got.Attempts) != 1 || got.Attempts[0].StartingAt != nil {
		t.Fatalf("quota allocated intent %+v %v", got, e)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionSparsePagesUseLastReturnedOrdinal(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	var jobs []backend.Job
	for _, id := range []string{"a", "b", "c"} {
		j := retainedSubmit(t, s, info.Domain, id)
		jobs = append(jobs, j)
	}
	if _, e := s.Update(jobs[1].ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
		t.Fatal(e)
	}
	_, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	page, e := s.ListFor(testActor(), 0, 3, 1)
	if e != nil || len(page.Jobs) != 1 || page.After != 1 || !page.More {
		t.Fatalf("first sparse %+v %v", page, e)
	}
	page, e = s.ListFor(testActor(), page.After, 3, 1)
	if e != nil || len(page.Jobs) != 1 || page.After != 3 || page.More {
		t.Fatalf("second sparse %+v %v", page, e)
	}
}
func TestRetentionCleanupRejectsUnprovedNative(t *testing.T) {
	s, _ := enableTest(t, DefaultRetentionPolicy())
	e := s.db.View(func(tx *bolt.Tx) error {
		h := cleanupHeader{ID: "11111111111111111111111111111111", JobID: "11111111111111111111111111111111", ExecutionIDs: []string{"22222222222222222222222222222222"}, NeedsManual: true}
		op := cleanupOperation{ID: "33333333333333333333333333333333", HeaderID: h.ID, Sequence: 1, Operation: NativeCleanup, AttemptID: h.ExecutionIDs[0], Stopped: false}
		return validateCleanupOperation(tx, h, op)
	})
	if e == nil {
		t.Fatal("unproved missing execution authorized native cleanup")
	}
}

func TestRetentionPreservesInheritedTaskCounterUntilEnable(t *testing.T) {
	dir := routingFixture(t, "schema1")
	editFixture(t, dir, true, func(tx *bolt.Tx) error { return tx.Bucket(metadataBucket).Put(taskBytesKey, sequenceKey(maxTaskBytes)) })
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		if !bytes.Equal(tx.Bucket(metadataBucket).Get(taskBytesKey), sequenceKey(maxTaskBytes)) {
			t.Fatal("migration changed conservative historical counter")
		}
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if st.TaskCounterOffset == 0 {
			t.Fatal("historical offset not recorded")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = EnableRetention(dir, DefaultRetentionPolicy()); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.db.View(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if st.TaskCounterOffset != 0 || st.TaskCounterDeficit != 0 {
			t.Fatal("activation did not reconcile explicit historical accounting")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionDryRunProjectsCumulativeCapacity(t *testing.T) {
	p := DefaultRetentionPolicy()
	p.CleanupRecords = 1
	s, info := enableTest(t, p)
	for _, id := range []string{"a", "b"} {
		j := retainedSubmit(t, s, info.Domain, id)
		if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
			t.Fatal(e)
		}
	}
	sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	dry, e := s.Prune(testActor(), sel, false, nil, nil, time.Now())
	if e != nil || dry.Selected != 1 {
		t.Fatalf("cumulative dry-run %+v %v", dry, e)
	}
	done, e := s.Prune(testActor(), sel, true, nil, nil, time.Now())
	if e != nil || done.Selected != dry.Selected || done.Marked != 1 {
		t.Fatalf("execute diverged %+v versus %+v: %v", done, dry, e)
	}
}
