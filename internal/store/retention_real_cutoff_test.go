//go:build linux || darwin

package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// TestRetentionRealLegacyCutoff waits a full real hour. It requires the ratified
// R1 private identity-retirement amendment; the unamended C11 base cannot pass.
func TestRetentionRealLegacyCutoff(t *testing.T) {
	if os.Getenv("SKYGO_REAL_CUTOFF") != "1" {
		t.Skip("explicit one-hour manual real-clock gate required")
	}
	root := os.Getenv("SKYGO_REAL_CUTOFF_ROOT")
	if !filepath.IsAbs(root) {
		t.Fatal("private absolute evidence root required")
	}
	fi, err := os.Stat(root)
	if err != nil || !fi.IsDir() || fi.Mode().Perm()&0077 != 0 {
		t.Fatalf("private0700 evidence root: %v", err)
	}
	log, err := os.OpenFile(filepath.Join(root, "real-cutoff.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Error(err)
		}
	}()
	var start time.Time
	t.Cleanup(func() {
		if !start.IsZero() && time.Since(start) > 65*time.Minute {
			t.Error("HOLD: observed final test completion exceeded65minutes")
		}
	})
	type extent struct {
		Path              string
		Length, Allocated int64
	}
	record := func(phase string, value any, floor uint64) {
		t.Helper()
		var fs unix.Statfs_t
		if err := unix.Statfs(root, &fs); err != nil {
			t.Fatal(err)
		}
		free := uint64(fs.Bavail) * uint64(fs.Bsize)
		var files []extent
		var boundary string
		if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".db" {
				return nil
			}
			var st unix.Stat_t
			if err := unix.Stat(path, &st); err != nil {
				return err
			}
			files = append(files, extent{path, st.Size, st.Blocks * 512})
			if st.Size >= 320<<20 {
				boundary = path
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		now := time.Now()
		elapsed := time.Duration(0)
		if !start.IsZero() {
			elapsed = time.Since(start)
		}
		entry := struct {
			Phase, Clock     string
			UTC              time.Time
			MonotonicElapsed time.Duration
			Free             uint64
			Files            []extent
			Value            any
		}{phase, "REAL_WALL_AND_MONOTONIC_STORE_COMPONENT", now.UTC(), elapsed, free, files, value}
		if err := json.NewEncoder(log).Encode(entry); err != nil {
			t.Fatal(err)
		}
		if err := log.Sync(); err != nil {
			t.Fatal(err)
		}
		if boundary != "" || free < floor {
			t.Fatalf("HOLD: physical boundary path=%s free=%d floor=%d", boundary, free, floor)
		}
	}
	record("initial-preflight", nil, 8<<30)
	state := filepath.Join(root, "state")
	s, err := Open(state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	// This real explicit legacy authority mints its Actor. No auth-file freshness
	// or HTTP qualification is claimed by these direct Store API calls.
	actor, err := auth.NewLegacy("").Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"never-executed"}}
	const known = "real-cutoff-known"
	const absent = "real-cutoff-absent"
	j, err := s.Submit(actor, known, task, "remote", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	owner := s.Owner()
	var canonical []byte
	if err = s.db.View(func(tx *bolt.Tx) error {
		canonical = bytes.Clone(tx.Bucket(jobsBucket).Get([]byte(j.ID)))
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canonical)
	jobHash := hex.EncodeToString(sum[:])
	taskBytes, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	assertProtected := func() {
		t.Helper()
		got, err := s.GetFor(actor, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		current, err := backend.TaskBytes(got.Task)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status.State != backend.Queued || len(got.Attempts) != 0 || got.CancelRequested || got.Status.Instance != "" || !bytes.Equal(current, taskBytes) || s.Owner() != owner {
			t.Fatal("protected queued job/task/owner/proof changed")
		}
		if err = s.db.View(func(tx *bolt.Tx) error {
			if !bytes.Equal(tx.Bucket(jobsBucket).Get([]byte(j.ID)), canonical) {
				return errors.New("canonical accepted Job bytes changed")
			}
			return validateSnapshot(tx)
		}); err != nil {
			t.Fatal(err)
		}
	}
	assertProtected()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	policy := DefaultRetentionPolicy()
	policy.Window = time.Hour
	policy.LocalOutputEnabled = false
	record("before-enable-one-hour-window", policy, 2<<30)
	enableStarted := time.Now()
	info, err := EnableRetention(state, policy)
	if err != nil {
		t.Fatal(err)
	}
	// Starting AFTER activation returns makes the independent monotonic wait at
	// least one complete promised window, even if activation itself was slow.
	start = time.Now()
	if info.Policy != policy || info.Policy.Window != time.Hour || info.LegacyCutoff.IsZero() || !info.LegacyReplayOpen || !info.LegacyCutoff.After(start.UTC()) || info.LegacyCutoff.Before(enableStarted.UTC().Add(time.Hour)) || info.LegacyCutoff.After(start.UTC().Add(time.Hour)) {
		t.Fatal("persisted one-hour legacy cutoff missing")
	}
	record("persisted-cutoff", struct {
		Cutoff, StartUTC, EnableStartedUTC time.Time
		Window                             time.Duration
		JobSHA256, Lineage                 string
	}{info.LegacyCutoff, start.UTC(), enableStarted.UTC(), policy.Window, jobHash, info.Domain.Lineage}, 2<<30)
	s, err = Open(state)
	if err != nil {
		t.Fatal(err)
	}
	assertProtected()
	replay, err := s.Submit(actor, known, task, "ignored", "ignored")
	if err != nil || replay.ID != j.ID {
		t.Fatalf("before-cutoff known replay: %v", err)
	}
	_, err = s.Submit(actor, absent, task, "ignored", "ignored")
	var expiry *ExpiryError
	if !errors.As(err, &expiry) || expiry.Reason != RequestDomainRequired {
		t.Fatalf("unknown legacy admission before cutoff: %v", err)
	}
	assertProtected()
	record("pre-cutoff-replay-and-refusal", nil, 2<<30)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	deadline := time.NewTimer(65*time.Minute - time.Since(start))
	defer deadline.Stop()
	for {
		if time.Since(start) > 65*time.Minute {
			t.Fatal("HOLD: observed waiting completion exceeded65minutes")
		}
		now := time.Now()
		if time.Since(start) >= time.Hour && !now.UTC().Before(info.LegacyCutoff) && now.UTC().Sub(start.UTC()) >= time.Hour {
			break
		}
		select {
		case <-ticker.C:
			assertProtected()
			record("real-minute-wait", struct {
				Cutoff        time.Time
				RemainingWall time.Duration
			}{info.LegacyCutoff, time.Until(info.LegacyCutoff)}, 2<<30)
		case <-deadline.C:
			record("real-deadline-HOLD", info.LegacyCutoff, 2<<30)
			t.Fatal("HOLD: full monotonic hour and persisted UTC cutoff not both reached within65minutes")
		}
	}
	if time.Since(start) > 65*time.Minute {
		t.Fatal("HOLD: observed cutoff completion exceeded65minutes")
	}
	record("real-cutoff-reached", struct {
		Cutoff      time.Time
		WallElapsed time.Duration
	}{info.LegacyCutoff, time.Now().UTC().Sub(start.UTC())}, 2<<30)
	for _, id := range []string{known, absent} {
		_, err = s.Submit(actor, id, task, "ignored", "ignored")
		if !errors.As(err, &expiry) || expiry.Reason != RequestEpochExpired {
			t.Fatalf("after-cutoff %s: %v", id, err)
		}
	}
	closed, err := s.RetentionInfo(actor)
	if err != nil || closed.LegacyReplayOpen || !closed.LegacyCutoff.Equal(info.LegacyCutoff) || closed.Domain.Lineage != info.Domain.Lineage {
		t.Fatalf("cutoff closure not durable: %v", err)
	}
	assertProtected()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	record("before-closed-cutoff-reopen", closed.LegacyCutoff, 2<<30)
	s, err = Open(state)
	if err != nil {
		t.Fatal(err)
	}
	assertProtected()
	reopenedClosure, err := s.RetentionInfo(actor)
	if err != nil || reopenedClosure.LegacyReplayOpen || !reopenedClosure.LegacyCutoff.Equal(info.LegacyCutoff) || reopenedClosure.Domain.Lineage != info.Domain.Lineage {
		t.Fatalf("actual cutoff closure reopened: %v", err)
	}
	record("observed-reopened-closure", reopenedClosure, 2<<30)
	for _, id := range []string{known, absent} {
		_, err = s.Submit(actor, id, task, "ignored", "ignored")
		if !errors.As(err, &expiry) || expiry.Reason != RequestEpochExpired {
			t.Fatalf("durable pre-maintenance expiry %s: %v", id, err)
		}
	}
	// Only actual current wall time enters the existing maintenance API. No
	// Store.now assignment, synthetic future timestamp, Restore or epoch shortcut.
	retired := false
	readRetired := func() bool {
		if err := s.db.View(func(tx *bolt.Tx) error {
			var m struct {
				Legacy               bool   `json:"legacy"`
				LegacyRequestID      []byte `json:"legacy_request_id"`
				RequestID            string `json:"request_id"`
				LegacyRequestRetired bool   `json:"legacy_retired"`
			}
			if err := json.Unmarshal(retentionTable(tx, retainedJobsBucket).Get([]byte(j.ID)), &m); err != nil {
				return err
			}
			retired = m.Legacy && m.LegacyRequestRetired && len(m.LegacyRequestID) == 0 && m.RequestID == ""
			if retired && (tx.Bucket(requestsBucket).Get([]byte(known)) != nil || retentionTable(tx, retainedRequestsBucket).Stats().KeyN != 0) {
				return errors.New("retired marker retains request/raw binding")
			}
			return validateSnapshot(tx)
		}); err != nil {
			t.Fatal(err)
		}
		return retired
	}
	for call := 1; call <= 4; call++ {
		record("before-real-maintenance", call, 2<<30)
		if err = s.MaintainRetention(time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		observed, err := s.RetentionInfo(actor)
		if err != nil || !observed.LegacyCutoff.Equal(info.LegacyCutoff) || observed.Domain.Lineage != info.Domain.Lineage || observed.LegacyReplayOpen {
			t.Fatalf("maintenance changed persisted cutoff/lineage: %v", err)
		}
		retired = readRetired()
		assertProtected()
		record("real-maintenance-result", struct {
			Call                   int
			PrivateIdentityRetired bool
			Observed               RetentionInfo
		}{call, retired, observed}, 2<<30)
		if retired {
			break
		}
	}
	if !retired {
		t.Fatal("R1 dependency missing/incorrect: canonical job private legacy identity not retired")
	}
	// Queued work remains protected even after dedup identity retirement. This is
	// a read-only actual prune classification, not native stop or cleanup proof.
	record("before-protected-prune", nil, 2<<30)
	dry, err := s.Prune(actor, PruneSelection{Before: time.Now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, false, nil, nil, time.Now().UTC())
	if err != nil || dry.Selected != 0 || dry.Marked != 0 || dry.Skipped != 1 {
		t.Fatalf("queued protection=%+v error=%v", dry, err)
	}
	assertProtected()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	record("before-durable-reopen", nil, 2<<30)
	s, err = Open(state)
	if err != nil {
		t.Fatal(err)
	}
	assertProtected()
	reopened, err := s.RetentionInfo(actor)
	if err != nil || reopened.LegacyReplayOpen || reopened.Domain.Lineage != info.Domain.Lineage || !reopened.LegacyCutoff.Equal(info.LegacyCutoff) {
		t.Fatalf("reopen changed closure/lineage: %v", err)
	}
	for _, id := range []string{known, absent} {
		_, err = s.Submit(actor, id, task, "ignored", "ignored")
		if !errors.As(err, &expiry) || expiry.Reason != RequestEpochExpired {
			t.Fatalf("reopened %s: %v", id, err)
		}
	}
	if !readRetired() {
		t.Fatal("reopen resurrected private legacy identity")
	}
	final, err := s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	if !final.LegacyCutoff.Equal(info.LegacyCutoff) || final.Domain.Lineage != info.Domain.Lineage || final.LegacyReplayOpen {
		t.Fatal("final actual cutoff/lineage differs")
	}
	seen := map[QuotaDomain]bool{}
	for _, u := range final.Usage {
		if u.Domain == JobsQuota || u.Domain == RequestRecordsQuota || u.Domain == CleanupRecordsQuota {
			seen[u.Domain] = true
		}
		if u.Domain == JobsQuota && u.Used != 1 || u.Domain == RequestRecordsQuota && u.Used != 0 || u.Domain == CleanupRecordsQuota && u.Used != 0 {
			t.Fatalf("protected final usage=%+v", u)
		}
	}
	if len(seen) != 3 {
		t.Fatal("final protected usage domains missing")
	}
	if time.Since(start) > 65*time.Minute {
		t.Fatal("HOLD: observed test work exceeded65minutes")
	}
	record("PASS-real-store-phase-clock", struct {
		Cutoff                       time.Time
		MonotonicElapsed, UTCElapsed time.Duration
		JobSHA256, Backend           string
		Info                         RetentionInfo
	}{final.LegacyCutoff, time.Since(start), time.Now().UTC().Sub(start.UTC()), jobHash, "NOT_CONSTRUCTED_NO_EXECUTION_PATH", final}, 2<<30)
	// Deliberately no rollback simulation: durable actual reopen is this gate.
	// Modeled rollback remains a separate R1 test with a separate evidence label.
}
