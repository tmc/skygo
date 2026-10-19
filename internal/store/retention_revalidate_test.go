package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestRetentionRevalidateDisabledRefusesUnchanged(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = RevalidateLocalOutput(dir); e == nil {
		t.Fatal("disabled revalidation accepted")
	}
	after, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("disabled refusal changed DB %v", e)
	}
}
func TestRetentionRevalidateNoopPreservesLogicalAndPhysicalBytes(t *testing.T) {
	s, _, _ := completedLocal(t)
	dir := s.Directory()
	logical := routingState(t, s.db)
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	info, e := InspectRetention(dir)
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil {
		t.Fatal(e)
	}
	got, e := RevalidateLocalOutput(dir)
	if e != nil || !reflect.DeepEqual(got, info) {
		t.Fatalf("noop info %+v %+v %v", got, info, e)
	}
	after, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("noop changed DB %v", e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(logical, routingState(t, s.db)) {
		t.Fatal("noop changed logical rows")
	}
}

// installPriorOutputFacts records a coherent earlier filesystem fact set. It is
// a store fixture, not evidence of native volume migration or writer closure.
func installPriorOutputFacts(t *testing.T, s *Store) {
	t.Helper()
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	unit := uint64(4096)
	if p.OutputUnit == unit {
		unit = 8192
	}
	if e = s.db.Update(func(tx *bolt.Tx) error {
		plan, e := planOutputRevalidation(tx, unit, p.Volume^1, map[string]uint64{})
		if e != nil {
			return e
		}
		return applyOutputRevalidation(tx, plan)
	}); e != nil {
		t.Fatal(e)
	}
}
func stableRevalidationRows(t *testing.T, s *Store) []byte {
	t.Helper()
	var out []byte
	if e := s.db.View(func(tx *bolt.Tx) error {
		for _, b := range []*bolt.Bucket{tx.Bucket(jobsBucket), retentionTable(tx, retainedClosuresBucket), retentionTable(tx, cleanupHeadersBucket), retentionTable(tx, cleanupOperationsBucket)} {
			if e := b.ForEach(func(k, v []byte) error {
				out = append(out, k...)
				out = append(out, 0)
				out = append(out, v...)
				out = append(out, 0)
				return nil
			}); e != nil {
				return e
			}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	return out
}
func TestRetentionRevalidateChangedFactsOverquotaPreservesProof(t *testing.T) {
	s, j, ref := completedLocal(t)
	dir := s.Directory()
	original := stableRevalidationRows(t, s)
	info, e := s.RetentionInfo(testActor())
	if e != nil {
		t.Fatal(e)
	}
	installPriorOutputFacts(t, s)
	if e = s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		st.Policy.OutputBytes = 1
		return putJSON(retentionRoot(tx), retentionStateKey, st)
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	got, e := RevalidateLocalOutput(dir)
	if e != nil {
		t.Fatal(e)
	}
	if got.Domain != info.Domain || got.Policy.OutputUnit != info.Policy.OutputUnit || got.Policy.Volume != info.Policy.Volume {
		t.Fatalf("wrong reconciliation %+v", got)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(original, stableRevalidationRows(t, s)) {
		t.Fatal("revalidation changed original rows/closure proof")
	}
	refs, _, e := s.ListOutputReferences("", 100)
	if e != nil || len(refs) != 1 || refs[0] != ref {
		t.Fatalf("closure changed %+v %v", refs, e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*got.Policy.OutputUnit
	next, e := s.SubmitRetained(testActor(), got.Domain, "held", j.Task, "local", "test-binding", backend.LocalOutput, reserve)
	if e != nil {
		t.Fatal(e)
	}
	_, started, e := s.BeginAttempt(next.ID, next.ID, reserve, time.Now())
	var cap *CapacityError
	if started || !errors.As(e, &cap) || cap.Domain != OutputBytesQuota {
		t.Fatalf("overquota start %v %v", started, e)
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}
func TestRetentionRevalidateMissingUnclosedDomainStaysUnknown(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*p.OutputUnit
	j, e := s.SubmitRetained(testActor(), info.Domain, "unknown", backend.Task{Argv: []string{"work"}}, "local", "test-binding", backend.LocalOutput, reserve)
	if e != nil {
		t.Fatal(e)
	}
	if _, start, e := s.BeginAttempt(j.ID, j.ID, reserve, j.CreatedAt.Add(time.Second)); e != nil || !start {
		t.Fatalf("begin %v %v", start, e)
	}
	if _, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Unknown}, j.CreatedAt.Add(2*time.Second)); e != nil {
		t.Fatal(e)
	}
	installPriorOutputFacts(t, s)
	dir := s.Directory()
	before := stableRevalidationRows(t, s)
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = RevalidateLocalOutput(dir); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	refs, _, e := s.ListOutputReferences("", 100)
	if e != nil || len(refs) != 1 || refs[0].ClosureID != "" || refs[0].PayloadReleased {
		t.Fatalf("unknown adopted %+v %v", refs, e)
	}
	if !bytes.Equal(before, stableRevalidationRows(t, s)) {
		t.Fatal("unknown state/proof changed")
	}
}
func TestRetentionRevalidateInvalidInventoryRefusesUnchanged(t *testing.T) {
	for _, kind := range []string{"orphan", "symlink", "oversize", "unexpected"} {
		t.Run(kind, func(t *testing.T) {
			s, _, ref := completedLocal(t)
			dir := s.Directory()
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			logs := filepath.Join(dir, "logs")
			if e := os.Mkdir(logs, 0700); e != nil {
				t.Fatal(e)
			}
			name := filepath.Join(logs, ref.AttemptID+".log")
			var e error
			switch kind {
			case "orphan":
				e = os.WriteFile(filepath.Join(logs, strings.Repeat("f", 32)+".log"), nil, 0600)
			case "symlink":
				e = os.Symlink(filepath.Join(dir, "jobs.db"), name)
			case "oversize":
				var f *os.File
				f, e = os.Create(name)
				if e == nil {
					e = f.Truncate(backend.MaxLogBytes + 1)
					e = errors.Join(e, f.Close())
				}
			case "unexpected":
				e = os.WriteFile(filepath.Join(logs, "unknown"), nil, 0600)
			}
			if e != nil {
				t.Fatal(e)
			}
			before, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if e != nil {
				t.Fatal(e)
			}
			if _, e = RevalidateLocalOutput(dir); e == nil {
				t.Fatal("invalid inventory accepted")
			}
			after, e := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if e != nil || !bytes.Equal(before, after) {
				t.Fatalf("inventory refusal changed DB %v", e)
			}
		})
	}
}
func TestRetentionRevalidationRollbackAndOverflow(t *testing.T) {
	s, _, _ := completedLocal(t)
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(s.db.Path())
	if e != nil {
		t.Fatal(e)
	}
	sentinel := errors.New("abort after actual reconciliation")
	e = s.db.Update(func(tx *bolt.Tx) error {
		plan, e := planOutputRevalidation(tx, p.OutputUnit, p.Volume^1, map[string]uint64{})
		if e != nil {
			return e
		}
		if e = applyOutputRevalidation(tx, plan); e != nil {
			return e
		}
		return sentinel
	})
	if !errors.Is(e, sentinel) {
		t.Fatalf("rollback %v", e)
	}
	after, e := os.ReadFile(s.db.Path())
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("rollback changed DB %v", e)
	}
	if _, e = revalidationSpace(^uint64(0)); e == nil {
		t.Fatal("space arithmetic overflow accepted")
	}
}

func TestRetentionRevalidatePreservesPendingNativeReceipts(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	p, _, e := s.RetentionPolicy()
	if e != nil {
		t.Fatal(e)
	}
	reserve := uint64(backend.MaxLogBytes) + 4*p.OutputUnit
	task := backend.Task{Argv: []string{"work"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}}
	j, e := s.SubmitRetained(testActor(), info.Domain, "receipts", task, "local", "test-binding", backend.LocalOutput, reserve)
	if e != nil {
		t.Fatal(e)
	}
	now := j.CreatedAt.Add(time.Second)
	if _, start, e := s.BeginAttempt(j.ID, j.ID, reserve, now); e != nil || !start {
		t.Fatalf("first %v %v", start, e)
	}
	code := int32(7)
	j, e = s.ObserveAttempt(j.ID, j.ID, backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "store-fixture-1"}, now.Add(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	j, start, e := s.BeginAttempt(j.ID, j.ID, reserve, *j.NextAttemptAt)
	if e != nil || !start {
		t.Fatalf("second %v %v", start, e)
	}
	code = 0
	j, e = s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: "store-fixture-2"}, j.UpdatedAt.Add(time.Second))
	if e != nil {
		t.Fatal(e)
	}
	subjects, e := s.PruneSubjects(testActor(), PruneSelection{Before: time.Now().Add(time.Hour)})
	if e != nil || len(subjects) != 1 {
		t.Fatalf("subjects %v", e)
	}
	for _, ref := range subjects[0].Output {
		ref.ClosureID = "store-fixture-closed"
		ref.PayloadReleased = true
		if e = s.RecordOutputClosure(ref, subjects[0].Digest); e != nil {
			t.Fatal(e)
		}
	}
	out, e := s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour)}, true, nil, nil, time.Now())
	if e != nil || out.Marked != 1 {
		t.Fatalf("mark %+v %v", out, e)
	}
	before := stableRevalidationRows(t, s)
	installPriorOutputFacts(t, s)
	dir := s.Directory()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = RevalidateLocalOutput(dir); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if !bytes.Equal(before, stableRevalidationRows(t, s)) {
		t.Fatal("pending closure/native/shared Task changed")
	}
	if e = s.db.View(validateSnapshot); e != nil {
		t.Fatal(e)
	}
}

func TestRetentionRevalidateMissingAndEmptyNeverInitialize(t *testing.T) {
	for _, kind := range []string{"missing", "empty", "short"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "jobs.db")
			var before []byte
			if kind != "missing" {
				if kind == "short" {
					before = []byte{1}
				}
				if e := os.WriteFile(path, before, 0600); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := RevalidateLocalOutput(dir); e == nil {
				t.Fatal("invalid database accepted")
			}
			after, e := os.ReadFile(path)
			if kind == "missing" {
				if !errors.Is(e, os.ErrNotExist) {
					t.Fatalf("missing file created %v", e)
				}
			} else if e != nil || !bytes.Equal(before, after) {
				t.Fatalf("malformed file initialized %v (%d -> %d)", e, len(before), len(after))
			}
		})
	}
}
