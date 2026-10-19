package store

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func completedLocal(t *testing.T) (*Store, backend.Job, backend.OutputReference) {
	t.Helper()
	s, info := enableTest(t, DefaultRetentionPolicy())
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*p.OutputUnit
	j, e := s.SubmitRetained(testActor(), info.Domain, "closed", backend.Task{Argv: []string{"work"}}, "local", "test-binding", backend.LocalOutput, reserve)
	if e != nil {
		t.Fatal(e)
	}
	j, started, e := s.BeginAttempt(j.ID, j.ID, reserve, j.CreatedAt.Add(time.Second))
	if e != nil || !started {
		t.Fatalf("begin %v %v", started, e)
	}
	code := int32(0)
	j, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Succeeded, ExitCode: &code}, j.CreatedAt.Add(2*time.Second))
	if e != nil {
		t.Fatal(e)
	}
	sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: OutputOnly}
	subjects, e := s.PruneSubjects(testActor(), sel)
	if e != nil || len(subjects) != 1 || len(subjects[0].Output) != 1 {
		t.Fatalf("subject %+v %v", subjects, e)
	}
	r := subjects[0].Output[0]
	r.ClosureID = "fixture-closed"
	r.PayloadReleased = true
	if e = s.RecordOutputClosure(r, subjects[0].Digest); e != nil {
		t.Fatal(e)
	}
	return s, j, r
}
func TestRetentionClosureCorruptionRefusedAtOpen(t *testing.T) {
	s, _, ref := completedLocal(t)
	if e := s.db.Update(func(tx *bolt.Tx) error {
		var c outputClosure
		if e := loadJSON(retentionTable(tx, retainedClosuresBucket), []byte(ref.AttemptID), &c); e != nil {
			return e
		}
		c.Reference.Generation = 2
		return putJSON(retentionTable(tx, retainedClosuresBucket), []byte(ref.AttemptID), c)
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}
func assertRetentionRefusal(t *testing.T, s *Store) {
	t.Helper()
	dir := s.Directory()
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil {
		t.Fatal(e)
	}
	other, e := Open(dir)
	if e == nil {
		other.Close()
		t.Fatal("corrupt retention side record accepted")
	}
	after, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("refusal changed state: %v", e)
	}
}
func TestRetentionStaleClosureMayReopenButCannotMark(t *testing.T) {
	s, j, _ := completedLocal(t)
	if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.Reason = "new diagnostic"; return nil }); e != nil {
		t.Fatal(e)
	}
	if e := s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
	out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: OutputOnly}, false, nil, nil, time.Now())
	if e != nil || out.Selected != 0 || out.Skipped != 1 {
		t.Fatalf("stale closure used %+v %v", out, e)
	}
}
func TestRetentionPendingOutputReceiptSurvivesRestart(t *testing.T) {
	s, _, ref := completedLocal(t)
	out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, time.Now())
	if e != nil || out.Marked != 1 {
		t.Fatalf("mark %+v %v", out, e)
	}
	// Metadata-only creates no automatic cleanup. A fresh output-only proof
	// binds the closed domain to the taskless header before explicit log expiry.
	subjects, e := s.PruneSubjects(testActor(), PruneSelection{Mode: OutputOnly, ObligationID: ref.JobID})
	if e != nil || len(subjects) != 1 || subjects[0].Obligation == nil {
		t.Fatalf("manual subject %v %v", subjects, e)
	}
	if _, found, e := s.NextCleanup(0); e != nil || found {
		t.Fatalf("metadata-only queued work %v %v", found, e)
	}
	if e = s.RecordOutputClosure(ref, subjects[0].Digest); e != nil {
		t.Fatal(e)
	}
	out, e = s.Prune(testActor(), PruneSelection{Mode: OutputOnly, ObligationID: ref.JobID}, true, nil, nil, time.Now())
	if e != nil || out.Marked != 1 {
		t.Fatalf("taskless output mark %+v %v", out, e)
	}
	dir := s.Directory()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	refs, _, e := s.ListOutputReferences("", 100)
	if e != nil || len(refs) != 1 || refs[0].ClosureID != ref.ClosureID {
		t.Fatalf("pending proof lost %+v %v", refs, e)
	}
	record, ok, e := s.NextCleanup(0)
	if e != nil || !ok || record.Output.ClosureID != ref.ClosureID {
		t.Fatalf("pending dispatch %+v %v %v", record, ok, e)
	}
	if e = s.FinishCleanup(record.ID, record.Operation, record.Digest, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionNetFreeingPruneAtMetadataLimit(t *testing.T) {
	for _, over := range []uint64{0, 1} {
		t.Run(string(rune('a'+over)), func(t *testing.T) {
			s, info := enableTest(t, DefaultRetentionPolicy())
			j := retainedSubmit(t, s, info.Domain, "finished")
			if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
				t.Fatal(e)
			}
			if e := s.db.Update(func(tx *bolt.Tx) error {
				st, e := loadRetention(tx)
				if e != nil {
					return e
				}
				u, e := usage(tx)
				if e != nil {
					return e
				}
				st.Policy.MetadataBytes = u[MetadataBytesQuota] - over
				return putJSON(retentionRoot(tx), retentionStateKey, st)
			}); e != nil {
				t.Fatal(e)
			}
			sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}
			now := time.Now()
			dry, e := s.Prune(testActor(), sel, false, nil, nil, now)
			if e != nil || dry.Selected != 1 {
				t.Fatalf("dry %+v %v", dry, e)
			}
			done, e := s.Prune(testActor(), sel, true, nil, nil, now)
			if e != nil || done.Marked != 1 || !reflect.DeepEqual(done.Transfers, dry.Transfers) {
				t.Fatalf("net-freeing mark %+v dry %+v %v", done, dry, e)
			}
			if e = s.db.View(validateSnapshot); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestRetentionInvalidRequestRefusedBeforeMutation(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	for _, id := range []string{"", strings.Repeat("a", 129), "bad/request"} {
		before, e := os.ReadFile(s.db.Path())
		if e != nil {
			t.Fatal(e)
		}
		_, e = s.SubmitRetained(testActor(), info.Domain, id, backend.Task{Argv: []string{"work"}}, "remote", "test-binding", backend.RemoteOutput, 0)
		if e == nil {
			t.Fatalf("invalid request accepted %q", id)
		}
		after, e := os.ReadFile(s.db.Path())
		if e != nil || !bytes.Equal(before, after) {
			t.Fatalf("invalid request changed state %q %v", id, e)
		}
	}
}
func TestRetentionExpiredOutputMarkerCorruptionRefused(t *testing.T) {
	s, _, _ := completedLocal(t)
	out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: OutputOnly}, true, nil, nil, time.Now())
	if e != nil || out.Marked != 1 {
		t.Fatalf("mark %+v %v", out, e)
	}
	op, ok, e := s.NextCleanup(0)
	if e != nil || !ok {
		t.Fatalf("cleanup %v %v", ok, e)
	}
	if e = s.FinishCleanup(op.ID, op.Operation, op.Digest, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e = s.db.Update(func(tx *bolt.Tx) error {
		return retentionTable(tx, expiredOutputBucket).Put([]byte(op.Output.AttemptID), []byte("null"))
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}
func TestRetentionResultWitnessCorruptionRefused(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "finished")
	if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, time.Now()); e != nil {
		t.Fatal(e)
	}
	if e := s.db.Update(func(tx *bolt.Tx) error {
		return putJSON(retentionTable(tx, resultWitnessBucket), []byte(j.ID), "foobar")
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}
func TestRetentionOverheadOverflowCorruptionRefused(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "held")
	if e := s.db.Update(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		m.Overhead = ^uint64(0)
		raw, e := json.Marshal(m)
		if e != nil {
			return e
		}
		return retentionTable(tx, retainedJobsBucket).Put([]byte(j.ID), raw)
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}

func TestRetentionRemoteOnlyActivation(t *testing.T) {
	p := DefaultRetentionPolicy()
	p.LocalOutputEnabled = false
	s, info := enableTest(t, p)
	policy, enabled, e := s.RetentionPolicy()
	if e != nil || !enabled || policy.LocalOutputEnabled || policy.OutputUnit != 0 || policy.Volume != 0 {
		t.Fatalf("remote-only policy %+v %v", policy, e)
	}
	j := retainedSubmit(t, s, info.Domain, "remote")
	if _, start, e := s.BeginAttempt(j.ID, j.ID, 0, time.Now()); e != nil || !start {
		t.Fatalf("remote start %v %v", start, e)
	}
	if _, e := s.SubmitRetained(testActor(), info.Domain, "local", j.Task, "local", "test-binding", backend.LocalOutput, backend.MaxLogBytes); e == nil {
		t.Fatal("local submit accepted under disabled output")
	}
	if e := s.BindOutputStorage("local", "test-binding", backend.LocalOutput, backend.MaxLogBytes); e == nil {
		t.Fatal("local config bound under disabled output")
	}
	if e := s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionVirtualSequencesMatchDryRun(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	for _, id := range []string{"a", "b"} {
		j := retainedSubmit(t, s, info.Domain, id)
		if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
			t.Fatal(e)
		}
	}
	if e := s.db.Update(func(tx *bolt.Tx) error { return retentionTable(tx, cleanupHeaderIndexBucket).SetSequence(8) }); e != nil {
		t.Fatal(e)
	}
	sel := PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}
	now := time.Now()
	dry, e := s.Prune(testActor(), sel, false, nil, nil, now)
	if e != nil {
		t.Fatal(e)
	}
	done, e := s.Prune(testActor(), sel, true, nil, nil, now)
	if e != nil || done.Marked != 2 || !reflect.DeepEqual(dry.Transfers, done.Transfers) {
		t.Fatalf("virtual sequence accounting dry %+v execute %+v %v", dry, done, e)
	}
}

func TestRetentionMigratedOversizeMetadataShrinks(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	j.Status.Reason = strings.Repeat("x", 24000)
	raw, e := json.Marshal(j)
	if e != nil {
		t.Fatal(e)
	}
	editFixture(t, dir, false, func(tx *bolt.Tx) error { return tx.Bucket(jobsBucket).Put([]byte(j.ID), raw) })
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	got, e := s.Get(j.ID)
	if e != nil || got.Status.Reason != j.Status.Reason {
		t.Fatalf("migration changed row %v", e)
	}
	var old uint64
	if e = s.db.View(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		old = m.Overhead
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if old <= jobOverheadReserve {
		t.Fatal("fixture is not grandfathered oversize")
	}
	if _, e = s.Update(j.ID, func(j *backend.Job) error { j.Status.Reason = "finished"; return nil }); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.db.View(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		if m.Overhead != jobOverheadReserve || m.Overhead >= old {
			t.Fatalf("overhead not released %d -> %d", old, m.Overhead)
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionRestoreClosesLegacyReplayBeforeCutoff(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	info, e := EnableRetention(dir, DefaultRetentionPolicy())
	if e != nil {
		t.Fatal(e)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if e = Backup(dir, backup); e != nil {
		t.Fatal(e)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if e = Restore(backup, restored); e != nil {
		t.Fatal(e)
	}
	read, e := InspectRetention(restored)
	if e != nil {
		t.Fatal(e)
	}
	if !read.Enabled || read.Domain.Lineage == info.Domain.Lineage || read.LegacyReplayOpen || read.LegacyCutoff.IsZero() {
		t.Fatalf("restore facts %+v", read)
	}
	s, e := Open(restored)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.now = func() time.Time { return info.LegacyCutoff.Add(-time.Hour) }
	_, e = s.Submit(testActor(), "original-request", j.Task, "local", "test-binding")
	expiryIs(t, e, RequestEpochExpired)
	_, _, e = s.LookupRetained(testActor(), info.Domain, "original-request", j.Task)
	expiryIs(t, e, RequestLineageChanged)
	got, e := s.Get(j.ID)
	if e != nil || got.ID != j.ID {
		t.Fatalf("restored result lost %v", e)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionPlannedCleanupCollisionRollsBack(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "done")
	if _, e := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); e != nil {
		t.Fatal(e)
	}
	e := s.db.Update(func(tx *bolt.Tx) error {
		m, e := newRetentionMutation(tx)
		if e != nil {
			return e
		}
		x, e := subject(tx, j.ID)
		if e != nil {
			return e
		}
		h := cleanupHeader{ID: j.ID, JobID: j.ID, Principal: testActor().Principal(), Binding: j.TargetBinding, Target: j.Target, ExecutionIDs: []string{j.ID}, NeedsManual: true, CreatedAt: time.Now()}
		id := strings.Repeat("1", 32)
		// The first planned value has not reached bbolt; the next mark must still see it.
		if e = m.put(cleanupOperationsBucket, []byte(id), cleanupOperation{ID: id}); e != nil {
			return e
		}
		e = markPrune(m, x, PruneSelection{Mode: OutputOnly}, h, []cleanupOperation{{ID: id, HeaderID: j.ID}}, nil)
		if e == nil {
			t.Fatal("planned duplicate ID accepted")
		}
		return e
	})
	if e == nil {
		t.Fatal("collision did not abort transaction")
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionMissingLiveRequestRefused(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "same")
	if e := s.db.Update(func(tx *bolt.Tx) error {
		return deleteRetentionRecord(tx, retainedRequestsBucket, requestKey(testActor().Principal(), info.Domain, "same"))
	}); e != nil {
		t.Fatal(e)
	}
	_ = j
	assertRetentionRefusal(t, s)
}
func TestRetentionMissingStartedOutputRefused(t *testing.T) {
	s, _, ref := completedLocal(t)
	if e := s.db.Update(func(tx *bolt.Tx) error {
		var r outputReservation
		if e := loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(ref.AttemptID), &r); e != nil {
			return e
		}
		for _, x := range []struct {
			q QuotaDomain
			n uint64
		}{{OutputBytesQuota, r.Bytes}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
			if e := adjust(tx, x.q, 0, x.n); e != nil {
				return e
			}
		}
		if e := deleteRetentionRecord(tx, retainedClosuresBucket, []byte(ref.AttemptID)); e != nil {
			return e
		}
		return deleteRetentionRecord(tx, retainedOutputsBucket, []byte(ref.AttemptID))
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}
func TestRetentionRetiredLiveResultMayLoseRequest(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	j := retainedSubmit(t, s, info.Domain, "retire")
	if e := s.MaintainRetention(info.RetryUntil.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e := s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Get(j.ID); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionMigratedMissingHistoricalOutputAccepted(t *testing.T) {
	dir := t.TempDir()
	j := legacyFixture(t, dir, false)
	if _, e := EnableRetention(dir, DefaultRetentionPolicy()); e != nil {
		t.Fatal(e)
	}
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	if e = s.BindOutputStorage("local", "test-binding", backend.LocalOutput, uint64(backend.MaxLogBytes)+4*p.OutputUnit); e != nil {
		t.Fatal(e)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
	got, e := s.Get(j.ID)
	if e != nil || got.Status.State != backend.Succeeded {
		t.Fatalf("historical row changed %v", e)
	}
}

func TestRetentionBackoffProtectsWholeAndMetadata(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}}
	j, e := s.SubmitRetained(testActor(), info.Domain, "backoff", task, "remote", "test-binding", backend.RemoteOutput, 0)
	if e != nil {
		t.Fatal(e)
	}
	now := j.CreatedAt.Add(time.Second)
	if _, started, e := s.BeginAttempt(j.ID, j.ID, 0, now); e != nil || !started {
		t.Fatalf("begin %v %v", started, e)
	}
	code := int32(7)
	j, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "pinned-fixture"}, now.Add(time.Second))
	if e != nil || j.Status.State != backend.Retrying {
		t.Fatalf("backoff %+v %v", j.Status, e)
	}
	for _, mode := range []PruneMode{Whole, MetadataOnly} {
		out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: mode, AcknowledgeUnverifiedTerminal: mode == MetadataOnly}, true, nil, nil, time.Now())
		if e != nil || out.Marked != 0 || len(out.Skips) != 1 || out.Skips[0].Reason != "retry_backoff" {
			t.Fatalf("backoff prune %+v %v", out, e)
		}
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionOutputBoundaryTamperingRefused(t *testing.T) {
	for _, boundary := range []uint32{0, 11} {
		t.Run(string(rune('a'+boundary)), func(t *testing.T) {
			s, j, _ := completedLocal(t)
			if e := s.db.Update(func(tx *bolt.Tx) error {
				var m retainedJob
				if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
					return e
				}
				m.OutputStart = boundary
				return putRetentionRecord(tx, retainedJobsBucket, []byte(j.ID), m)
			}); e != nil {
				t.Fatal(e)
			}
			assertRetentionRefusal(t, s)
		})
	}
}

func TestRetentionLegacyRetryStartsTrackingAtLaterAttempt(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 3, ExitCodes: []int32{7}}}
	j, e := s.Submit(testActor(), "legacy-retry", task, "local", "test-binding")
	if e != nil {
		t.Fatal(e)
	}
	now := j.CreatedAt.Add(time.Second)
	if _, started, e := s.BeginAttempt(j.ID, j.ID, 0, now); e != nil || !started {
		t.Fatalf("first begin %v %v", started, e)
	}
	code := int32(7)
	j, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "historical-store-fixture"}, now.Add(time.Second))
	if e != nil || j.Status.State != backend.Retrying {
		t.Fatalf("first exit %v %v", j.Status, e)
	}
	// This actual pre-activation execution had no local output inventory. It is
	// not fabricated as closed or expired when retention is later enabled.
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
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*p.OutputUnit
	if e = s.BindOutputStorage("local", "test-binding", backend.LocalOutput, reserve); e != nil {
		t.Fatal(e)
	}
	j, started, e := s.BeginAttempt(j.ID, j.ID, reserve, *j.NextAttemptAt)
	if e != nil || !started || len(j.Attempts) != 2 {
		t.Fatalf("second begin %+v %v %v", j, started, e)
	}
	if e = s.db.View(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		if !m.Legacy || m.OutputStart != 2 {
			t.Fatalf("boundary %+v", m)
		}
		if retentionTable(tx, retainedOutputsBucket).Get([]byte(j.ID)) != nil || retentionTable(tx, expiredOutputBucket).Get([]byte(j.ID)) != nil {
			t.Fatal("historical absence became invented domain/proof")
		}
		return validateSnapshot(tx)
	}); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionOutputBoundaryCannotSkipFirstAttempt(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*p.OutputUnit
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}}
	j, e := s.SubmitRetained(testActor(), info.Domain, "two-local", task, "local", "test-binding", backend.LocalOutput, reserve)
	if e != nil {
		t.Fatal(e)
	}
	now := j.CreatedAt.Add(time.Second)
	if _, started, e := s.BeginAttempt(j.ID, j.ID, reserve, now); e != nil || !started {
		t.Fatalf("first %v %v", started, e)
	}
	code := int32(7)
	j, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "store-fixture-1"}, now.Add(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	j, started, e := s.BeginAttempt(j.ID, j.ID, reserve, *j.NextAttemptAt)
	if e != nil || !started || len(j.Attempts) != 2 {
		t.Fatalf("second %v %v", started, e)
	}
	if e = s.db.Update(func(tx *bolt.Tx) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
			return e
		}
		m.OutputStart = 2
		if e := putRetentionRecord(tx, retainedJobsBucket, []byte(j.ID), m); e != nil {
			return e
		}
		var r outputReservation
		if e := loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(j.ID), &r); e != nil {
			return e
		}
		for _, x := range []struct {
			q QuotaDomain
			n uint64
		}{{OutputBytesQuota, r.Bytes}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
			if e := adjust(tx, x.q, 0, x.n); e != nil {
				return e
			}
		}
		return deleteRetentionRecord(tx, retainedOutputsBucket, []byte(j.ID))
	}); e != nil {
		t.Fatal(e)
	}
	assertRetentionRefusal(t, s)
}
