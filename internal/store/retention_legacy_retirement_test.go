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
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

type legacyRetirementFixture struct {
	s    *Store
	dir  string
	info RetentionInfo
	jobs []backend.Job
	ids  []string
}

// All positive rows come from actual Store/auth APIs. Negative corruption tests
// below deliberately change private fields and require refusal, never credit.
func legacyRetirementSetup(t *testing.T, count int, scoped bool) *legacyRetirementFixture {
	t.Helper()
	f := &legacyRetirementFixture{dir: t.TempDir()}
	var err error
	f.s, err = Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.s != nil {
			if err := f.s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	for i := 0; i < count; i++ {
		id := "x"
		if i > 0 {
			id = fmt.Sprintf("legacy-%d", i)
		}
		j, err := f.s.Submit(testActor(), id, backend.Task{Argv: []string{"never-executed"}}, "remote", "test-binding")
		if err != nil {
			t.Fatal(err)
		}
		f.ids, f.jobs = append(f.ids, id), append(f.jobs, j)
	}
	if scoped {
		private := t.TempDir()
		if err := os.Chmod(private, 0700); err != nil {
			t.Fatal(err)
		}
		if _, err := auth.Initialize(filepath.Join(private, "auth.json"), filepath.Join(private, "admin.token"), f.s.Owner()); err != nil {
			t.Fatal(err)
		}
		if err := f.s.ActivateScoped(); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s = nil
	f.info, err = EnableRetention(f.dir, DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.open(t)
	return f
}
func (f *legacyRetirementFixture) open(t *testing.T) {
	t.Helper()
	var err error
	f.s, err = Open(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	f.s.now = func() time.Time { return f.info.LegacyCutoff.Add(-time.Second) }
}
func (f *legacyRetirementFixture) close(t *testing.T) {
	t.Helper()
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s = nil
}
func legacyRetirementMeta(t *testing.T, s *Store, id string) (retainedJob, []byte) {
	t.Helper()
	var m retainedJob
	var raw []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		raw = bytes.Clone(retentionTable(tx, retainedJobsBucket).Get([]byte(id)))
		return json.Unmarshal(raw, &m)
	}); err != nil {
		t.Fatal(err)
	}
	return m, raw
}
func legacyRetirementCanonical(t *testing.T, s *Store) []byte {
	t.Helper()
	var rows [][]byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).ForEach(func(k, v []byte) error {
			rows = append(rows, bytes.Clone(k), bytes.Clone(v))
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func legacyRetirementUsage(t *testing.T, s *Store) retentionUsage {
	t.Helper()
	var u retentionUsage
	if err := s.db.View(func(tx *bolt.Tx) error { var err error; u, err = usage(tx); return err }); err != nil {
		t.Fatal(err)
	}
	return u
}

func TestLegacyIdentityRetirementCutoffAndRollback(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("scoped=%t", scoped), func(t *testing.T) {
			f := legacyRetirementSetup(t, 1, scoped)
			canonical := legacyRetirementCanonical(t, f.s)
			old, oldRaw := legacyRetirementMeta(t, f.s, f.jobs[0].ID)
			before := legacyRetirementUsage(t, f.s)
			var requestBytes uint64
			if err := f.s.db.View(func(tx *bolt.Tx) error {
				key := requestKey(old.Principal, old.Domain, old.requestID())
				requestBytes = uint64(len(key) + len(retentionTable(tx, retainedRequestsBucket).Get(key)))
				// Count actual distinct namespace entries, not a mirrored charge helper.
				b := requestNamespace(tx, old.Principal)
				if v := b.Get(old.LegacyRequestID); v != nil {
					requestBytes += uint64(len(old.LegacyRequestID) + len(v))
				}
				if scoped {
					if v := tx.Bucket(requestsBucket).Get(old.LegacyRequestID); v != nil {
						requestBytes += uint64(len(old.LegacyRequestID) + len(v))
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.s.MaintainRetention(f.info.LegacyCutoff.Add(-time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			m, _ := legacyRetirementMeta(t, f.s, f.jobs[0].ID)
			if m.LegacyRequestRetired || !bytes.Equal(m.LegacyRequestID, []byte("x")) {
				t.Fatal("request bytes shed before the exact cutoff")
			}
			// Abort after the actual full maintenance body, including staged writes.
			snapshot := routingState(t, f.s.db)
			abort := errors.New("abort complete maintenance transaction")
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				if err := maintainRetention(tx, f.info.LegacyCutoff); err != nil {
					return err
				}
				return abort
			}); !errors.Is(err, abort) || !bytes.Equal(snapshot, routingState(t, f.s.db)) {
				t.Fatal("rollback changed closure, raw identity, cursors or accounting", err)
			}
			if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
				t.Fatal(err)
			}
			m, raw := legacyRetirementMeta(t, f.s, f.jobs[0].ID)
			want := old
			want.LegacyRequestID, want.LegacyRequestRetired = nil, true
			if !reflect.DeepEqual(m, want) || !bytes.Equal(canonical, legacyRetirementCanonical(t, f.s)) {
				t.Fatal("shedding changed canonical work, overhead or unrelated metadata")
			}
			after := legacyRetirementUsage(t, f.s)
			if before[MetadataBytesQuota]-after[MetadataBytesQuota] != uint64(len(oldRaw)-len(raw)) || before[RequestBytesQuota]-after[RequestBytesQuota] != requestBytes || before[RequestRecordsQuota]-after[RequestRecordsQuota] != 1 {
				t.Fatal("actual encoded row/binding deltas disagree with released charges")
			}
			f.close(t)
			f.open(t) // Store clock is deliberately back before cutoff.
			if _, _, err := f.s.Lookup(testActor(), "x", f.jobs[0].Task); !legacyRetirementExpiry(err, RequestEpochExpired) {
				t.Fatalf("clock rollback reopened legacy replay: %v", err)
			}
			if err := f.s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyIdentityRetirementUnknownRemainsProtected(t *testing.T) {
	f := legacyRetirementSetup(t, 1, false)
	j, err := f.s.UpdateFor(testActor(), f.jobs[0].ID, func(j *backend.Job) error {
		j.Status = backend.Status{State: backend.Unknown, Reason: "execution is not owned"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	before := legacyRetirementCanonical(t, f.s)
	if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, legacyRetirementCanonical(t, f.s)) {
		t.Fatal("identity shedding changed Unknown execution")
	}
	if got, err := f.s.GetFor(testActor(), j.ID); err != nil || !reflect.DeepEqual(got, j) {
		t.Fatal("identity shedding changed authenticated direct result read", got, err)
	}
	p, err := f.s.Prune(testActor(), PruneSelection{Before: j.UpdatedAt.Add(time.Second), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, j.UpdatedAt.Add(time.Second))
	if err != nil || p.Marked != 0 || p.Selected != 0 || p.Skipped != 1 || len(p.Skips) != 1 || p.Skips[0].Reason != "unknown" {
		t.Fatal("identity shedding authorized Unknown collection", p, err)
	}
	if _, found, err := f.s.NextCleanup(0); err != nil || found {
		t.Fatal("identity shedding invented execution cleanup", found, err)
	}
}

func legacyRetirementExpiry(err error, reason ExpiryReason) bool {
	var expired *ExpiryError
	return errors.As(err, &expired) && expired.Reason == reason
}

func TestLegacyIdentityRetirementAuthorityPredicate(t *testing.T) {
	lineage := "11111111111111111111111111111111"
	old := "22222222222222222222222222222222"
	for _, tc := range []struct {
		name    string
		enabled bool
		open    bool
		domain  RequestDomain
		want    bool
	}{
		{"disabled", false, false, RequestDomain{lineage, 0}, false},
		{"open_current", true, true, RequestDomain{lineage, 0}, false},
		{"closed_current", true, false, RequestDomain{lineage, 0}, true},
		{"old_lineage", true, true, RequestDomain{old, 0}, true},
		{"modern", true, false, RequestDomain{old, 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := retentionState{Enabled: tc.enabled, Lineage: lineage, LegacyReplayOpen: tc.open, Current: 99, RetiredThrough: 98}
			if got := legacyClosed(st, tc.domain); got != tc.want {
				t.Fatalf("closure=%t want %t", got, tc.want)
			}
		})
	}
}

func TestLegacyIdentityRetirementRestoreAndPrune(t *testing.T) {
	f := legacyRetirementSetup(t, 1, true)
	canonical := legacyRetirementCanonical(t, f.s)
	owner := f.s.Owner()
	f.close(t)
	before, err := os.ReadFile(filepath.Join(f.dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(t.TempDir(), "backup.db")
	if err := Backup(f.dir, snapshot); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(snapshot, restored); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(f.dir, "jobs.db")); err != nil || sha256.Sum256(raw) != sha256.Sum256(before) {
		t.Fatal("explicit restore changed stopped original", err)
	}
	f.dir = restored
	f.open(t)
	if err := f.s.MaintainRetention(f.info.LegacyCutoff.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	m, _ := legacyRetirementMeta(t, f.s, f.jobs[0].ID)
	if !m.LegacyRequestRetired || f.s.Owner() != owner || !bytes.Equal(canonical, legacyRetirementCanonical(t, f.s)) {
		t.Fatal("existing Restore closure failed to shed identity unchanged")
	}
	markedState := routingState(t, f.s.db)
	f.close(t)
	snapshot2 := filepath.Join(t.TempDir(), "marked.db")
	// Compaction validates and preserves the marked format and the permanent
	// acceptance sequence before a separately authorized second restore.
	stopped, err := os.ReadFile(filepath.Join(f.dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Compact(f.dir, snapshot2); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(f.dir, "jobs.db")); err != nil || sha256.Sum256(raw) != sha256.Sum256(stopped) {
		t.Fatal("compaction changed stopped original", err)
	}
	compacted, err := openSnapshot(snapshot2)
	if err != nil {
		t.Fatal(err)
	}
	compactedState := routingState(t, compacted)
	if err := compacted.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(markedState, compactedState) {
		t.Fatal("compaction changed marker, cursor, proofs, charges or bucket sequences")
	}
	f.dir = filepath.Join(t.TempDir(), "restored-again")
	if err := Restore(snapshot2, f.dir); err != nil {
		t.Fatal(err)
	}
	f.open(t)
	m, _ = legacyRetirementMeta(t, f.s, f.jobs[0].ID)
	if !m.LegacyRequestRetired || m.LegacyRequestID != nil {
		t.Fatal("second restore recreated raw legacy identity")
	}
	// Marking the already-retired result must not hash an empty request identity
	// or manufacture its tombstone. No stopped/native proof is supplied.
	j, err := f.s.UpdateFor(testActor(), f.jobs[0].ID, func(j *backend.Job) error { j.Status.State = backend.Canceled; return nil })
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.s.Prune(testActor(), PruneSelection{Before: j.UpdatedAt.Add(time.Second), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, j.UpdatedAt.Add(time.Second))
	if err != nil || p.Marked != 1 {
		t.Fatal("marked legacy prune", p, err)
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		if retentionTable(tx, retainedRequestsBucket).Stats().KeyN != 0 {
			return errors.New("prune invented an empty-identity request record")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	obligations, err := f.s.ListObligations(testActor(), 0, 0, 10)
	if err != nil || len(obligations.Items) != 1 || !obligations.Items[0].NeedsManual || !obligations.Items[0].ExecutionPending {
		t.Fatal("retired identity fabricated native cleanup closure", obligations, err)
	}
}
