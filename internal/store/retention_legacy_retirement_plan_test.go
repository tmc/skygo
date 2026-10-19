package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func TestLegacyIdentityRetirementSharedSubjectBudget(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(fmt.Sprintf("closed=%t", closed), func(t *testing.T) {
			f := legacyRetirementSetup(t, 120, true)
			before := legacyRetirementCanonical(t, f.s)
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				st, err := loadRetention(tx)
				if err != nil {
					return err
				}
				if closed {
					advancePhases(&st, f.info.LegacyCutoff)
				}
				m, err := newRetentionMutation(tx)
				if err != nil {
					return err
				}
				if err := m.rootPut(retentionStateKey, st); err != nil {
					return err
				}
				m, requestSubjects, err := retireRequestLane(m, &st)
				if err != nil {
					return err
				}
				requestBytes, err := m.changedBytes()
				if err != nil || requestBytes > maxChangeBytes/2 || requestSubjects != 50 {
					return fmt.Errorf("request lane subjects=%d bytes=%d err=%v", requestSubjects, requestBytes, err)
				}
				m, jobSubjects, err := retireLegacyJobLane(m, &st, 100-requestSubjects)
				if err != nil {
					return err
				}
				if requestSubjects+jobSubjects != 100 || st.LegacyJobsAfter != 50 || st.LegacyJobsMax != 120 {
					return fmt.Errorf("shared budget/cursor: request=%d jobs=%d state=%+v", requestSubjects, jobSubjects, st)
				}
				size, err := m.changedBytes()
				if err != nil || size > maxChangeBytes {
					return fmt.Errorf("complete ledger bytes=%d err=%v", size, err)
				}
				return m.commit()
			}); err != nil {
				t.Fatal(err)
			}
			marked := 0
			for _, j := range f.jobs {
				m, _ := legacyRetirementMeta(t, f.s, j.ID)
				if m.LegacyRequestRetired {
					marked++
				}
			}
			want := 0
			if closed {
				want = 25
			}
			if marked != want || !bytes.Equal(before, legacyRetirementCanonical(t, f.s)) {
				t.Fatalf("retired=%d want=%d; canonical job mutation=%t", marked, want, !bytes.Equal(before, legacyRetirementCanonical(t, f.s)))
			}
			if err := f.s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLegacyIdentityRetirementSparsePinnedPass(t *testing.T) {
	f := legacyRetirementSetup(t, 120, false)
	now := f.info.LegacyCutoff.Add(-time.Second)
	if err := f.s.MaintainRetention(now); err != nil {
		t.Fatal(err)
	}
	var first retentionState
	if err := f.s.db.View(func(tx *bolt.Tx) error { var err error; first, err = loadRetention(tx); return err }); err != nil {
		t.Fatal(err)
	}
	if first.LegacyJobsAfter != 50 || first.LegacyJobsMax != 120 {
		t.Fatal("initial pass did not persist a finite actual index cursor", first)
	}
	for _, j := range f.jobs[:70] {
		if _, err := f.s.UpdateFor(testActor(), j.ID, func(j *backend.Job) error { j.Status.State = backend.Canceled; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true, Limit: 70}, true, nil, nil, now)
	if err != nil || page.Marked != 70 {
		t.Fatal("failed to make real sparse index holes", page, err)
	}
	// Keep both following request scans genuinely at fifty subjects. The
	// original suffix has seventy requests. Add31 real modern requests beyond
	// its saved hash boundary, so the second scan leaves51 and the third can
	// inspect50. Otherwise the third scan may legitimately leave79 job visits.
	added := 0
	for candidate := 0; candidate < 4096 && added < 31; candidate++ {
		id := fmt.Sprintf("after-pinned-max-%d", candidate)
		if string(requestKey("legacy", f.info.Domain, id)) <= first.RequestAfter {
			continue
		}
		later, err := f.s.SubmitRetained(testActor(), f.info.Domain, id, backend.Task{Argv: []string{"never-executed"}}, "remote", "test-binding", backend.RemoteOutput, 0)
		if err != nil || later.Sequence != uint64(121+added) {
			t.Fatal("new acceptance above finite pass", later.Sequence, err)
		}
		added++
	}
	if added != 31 {
		t.Fatal("bounded real request hash suffix search exhausted", added)
	}
	countSuffix := func(after string) int {
		t.Helper()
		n := 0
		if err := f.s.db.View(func(tx *bolt.Tx) error {
			c := retentionTable(tx, retainedRequestsBucket).Cursor()
			for k, _ := c.Seek(append([]byte(after), 0)); k != nil; k, _ = c.Next() {
				n++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := countSuffix(first.RequestAfter); n != 101 {
		t.Fatal("actual request suffix does not establish two50-subject scans", n)
	}
	f.close(t)
	f.open(t)
	if err := f.s.MaintainRetention(now); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if st.LegacyJobsAfter != 0 || st.LegacyJobsMax != 0 {
			return fmt.Errorf("sparse pass did not finish original high-water: After=%d Max=%d", st.LegacyJobsAfter, st.LegacyJobsMax)
		}
		first = st
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if n := countSuffix(first.RequestAfter); n != 51 {
		t.Fatal("actual second request scan did not leave51 suffix subjects", n)
	}
	if err := f.s.MaintainRetention(now); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if st.LegacyJobsMax != 151 || st.LegacyJobsAfter != 120 {
			return fmt.Errorf("new pass boundary: After=%d Max=%d want After120 Max151", st.LegacyJobsAfter, st.LegacyJobsMax)
		}
		first = st
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n := countSuffix(first.RequestAfter); n != 1 {
		t.Fatal("actual third request scan did not consume exactly50 suffix subjects", n)
	}
	if err := f.s.MaintainRetention(now); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if st.LegacyJobsAfter != 0 || st.LegacyJobsMax != 0 {
			return fmt.Errorf("later acceptance tail failed completion: After=%d Max=%d", st.LegacyJobsAfter, st.LegacyJobsMax)
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyIdentityRetirementFormatVectors(t *testing.T) {
	lineage := strings.Repeat("a", 32)
	for _, size := range []int{1, bolt.MaxKeySize} {
		m := retainedJob{Principal: strings.Repeat("p", 64), Domain: RequestDomain{lineage, 0}, LegacyRequestID: bytes.Repeat([]byte{0xff}, size), Legacy: true, Storage: backend.LocalOutput, OutputReserve: ^uint64(0), Overhead: ^uint64(0), Started: true, OutputStart: 10, Ordinal: ^uint64(0)}
		old, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		m.LegacyRequestID, m.LegacyRequestRetired = nil, true
		retired, err := json.Marshal(m)
		if err != nil || len(old)-len(retired) < 5 {
			t.Fatalf("raw length%d did not shrink: old=%d retired=%d err=%v", size, len(old), len(retired), err)
		}
		t.Logf("actual encoded raw=%d retained=%d retired=%d saved=%d", size, len(old), len(retired), len(old)-len(retired))
	}
	// This is an encoded upper vector, not fabricated persisted state or claims
	// that all usage maxima coexist in a real database.
	max := ^uint64(0)
	accept := time.Date(9999, 1, 1, 23, 59, 59, 999999999, time.UTC)
	policy := DefaultRetentionPolicy()
	policy.Window, policy.OutputUnit, policy.Volume = 90*24*time.Hour, 65536, max
	st := retentionState{Enabled: true, Policy: policy, Lineage: lineage, Current: max, RetiredThrough: max - 2, Epochs: []requestEpoch{{Number: max - 1, AcceptUntil: accept, RetryUntil: accept.Add(policy.Window)}, {Number: max, AcceptUntil: accept, RetryUntil: accept.Add(policy.Window)}}, LegacyCutoff: accept, RequestAfter: strings.Repeat("f", 64), LegacyJobsAfter: max, LegacyJobsMax: max, TaskCounterOffset: max, TaskCounterDeficit: max, LineageSerial: max}
	var u retentionUsage
	for i := range u {
		u[i] = max
	}
	state, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	used, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	n := len(retentionStateKey) + len(state) + len(retentionUsageKey) + len(used)
	if n > 2048 {
		t.Fatalf("actual scalar upper encoding=%d exceeds2048", n)
	}
	if !validLegacyJobsCursor(st, max) {
		t.Fatal("maximum completed cursor was rejected")
	}
	st.LegacyJobsMax = 0
	if validLegacyJobsCursor(st, max) {
		t.Fatal("After without Max accepted")
	}
	t.Logf("actual scalar bytes=%d reservation=2048", n)
}

func TestLegacyIdentityRetirementFallbackDefersRequest(t *testing.T) {
	f := legacyRetirementSetup(t, 1, true)
	before := routingState(t, f.s.db)
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		advancePhases(&st, f.info.LegacyCutoff)
		m, err := newRetentionMutation(tx)
		if err != nil {
			return err
		}
		// The request still exists: fallback must leave its exact raw binding
		// and side record for the coupled path, despite the closed domain.
		if err := m.retireLegacyJob(st, f.jobs[0]); err != nil {
			return err
		}
		if len(m.changes) != 0 {
			return errors.New("fallback changed a job with an extant matching request")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, routingState(t, f.s.db)) {
		t.Fatal("read-only fallback trial mutated its input")
	}
}

func TestLegacyIdentityRetirementAllHoles(t *testing.T) {
	f := legacyRetirementSetup(t, 2, false)
	for _, j := range f.jobs {
		if _, err := f.s.UpdateFor(testActor(), j.ID, func(j *backend.Job) error { j.Status.State = backend.Canceled; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	now := f.info.LegacyCutoff.Add(-time.Second)
	p, err := f.s.Prune(testActor(), PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, now)
	if err != nil || p.Marked != 2 {
		t.Fatal("failed to remove actual acceptance keys", p, err)
	}
	if err := f.s.MaintainRetention(now); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if tx.Bucket(sequenceBucket).Sequence() != 2 || tx.Bucket(sequenceBucket).Stats().KeyN != 0 || st.LegacyJobsAfter != 0 || st.LegacyJobsMax != 0 {
			return errors.New("all-hole pass failed to preserve high-water and finish")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyIdentityRetirementCorruption(t *testing.T) {
	for _, name := range []string{"missing_raw", "marker_and_raw", "modern_marker", "reopened_authority", "raw_alias", "root_alias", "live_request", "wrong_principal", "invalid_domain", "wrong_storage", "wrong_overhead", "cursor_after_without_max", "cursor_after_max", "cursor_future", "wrong_usage"} {
		t.Run(name, func(t *testing.T) {
			f := legacyRetirementSetup(t, 1, true)
			original, _ := legacyRetirementMeta(t, f.s, f.jobs[0].ID)
			if err := f.s.MaintainRetention(f.info.LegacyCutoff); err != nil {
				t.Fatal(err)
			}
			if err := f.s.db.Update(func(tx *bolt.Tx) error {
				st, err := loadRetention(tx)
				if err != nil {
					return err
				}
				var m retainedJob
				if err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(f.jobs[0].ID), &m); err != nil {
					return err
				}
				switch name {
				case "missing_raw":
					m.LegacyRequestRetired = false
				case "marker_and_raw":
					m.LegacyRequestID = []byte("x")
				case "modern_marker":
					m.Legacy, m.Domain.Epoch = false, 1
				case "reopened_authority":
					st.LegacyReplayOpen = true
				case "raw_alias":
					if err := tx.Bucket(principalRequestsBucket).Bucket([]byte("legacy")).Put([]byte("alias"), []byte(f.jobs[0].ID)); err != nil {
						return err
					}
				case "root_alias":
					if err := tx.Bucket(requestsBucket).Put([]byte("alias"), []byte(f.jobs[0].ID)); err != nil {
						return err
					}
				case "wrong_principal":
					m.Principal = "another-owner"
				case "invalid_domain":
					m.Domain.Lineage = "invalid-lineage"
				case "wrong_storage":
					m.Storage = backend.OutputStorage(255)
				case "wrong_overhead":
					m.Overhead++
				case "live_request":
					digest, err := taskDigest(f.jobs[0].Task)
					if err != nil {
						return err
					}
					r := requestRecord{Principal: original.Principal, Domain: original.Domain, LegacyRequestID: original.LegacyRequestID, JobID: f.jobs[0].ID, TaskDigest: digest}
					if err := putRetentionRecord(tx, retainedRequestsBucket, requestKey(r.Principal, r.Domain, r.requestID()), r); err != nil {
						return err
					}
				case "cursor_after_without_max":
					st.LegacyJobsAfter, st.LegacyJobsMax = 1, 0
				case "cursor_after_max":
					st.LegacyJobsAfter, st.LegacyJobsMax = 2, 1
				case "cursor_future":
					st.LegacyJobsAfter, st.LegacyJobsMax = 0, 2
				case "wrong_usage":
					u, err := usage(tx)
					if err != nil {
						return err
					}
					u[MetadataBytesQuota]++
					return putJSON(retentionRoot(tx), retentionUsageKey, u)
				}
				if err := putRetentionRecord(tx, retainedJobsBucket, []byte(f.jobs[0].ID), m); err != nil {
					return err
				}
				return putJSON(retentionRoot(tx), retentionStateKey, st)
			}); err != nil {
				t.Fatal(err)
			}
			if err := f.s.db.View(validateSnapshot); err == nil {
				t.Fatal("corrupt retired identity/cursor/accounting accepted")
			}
			// Corruption must also be refused by the real normal reopen path.
			f.close(t)
			if s, err := Open(f.dir); err == nil {
				s.Close()
				t.Fatal("corrupt retired format reopened")
			}
		})
	}
}
