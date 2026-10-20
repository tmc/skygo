//go:build linux || darwin

package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These tests exercise the real Store policy/transaction boundaries with the
// existing MODELED physical handoffs. They do not prove native closure or bytes.
// Real filesystem round trips and original-worker workflow tests are separate.
func TestCompanionQualifiedDefaultCompletion(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, true, true)
	artifactRegressionPublish(t, s, j)
	current, err := s.Get(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status.Termination != backend.UnknownTermination || safeStatuses(current) {
		t.Fatal("fixture fabricated stopped retry evidence", current.Status)
	}
	before := artifactDatabaseBytes(t, s)
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := companionEligible(tx)
		if err != nil {
			return err
		}
		if !artifactWholeReady(st, current) || len(st.Results) != 1 {
			t.Fatal("missing ordinary completed result")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("eligibility wrote source")
	}
	// Removing proof from an otherwise canonical metadata corpus must not turn
	// terminal status plus missing maps into physical retirement authority.
	if err := s.artifactUpdate(func(st *artifactState) error { delete(st.Completions, j.CurrentAttemptID()); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error { _, err := companionEligible(tx); return err }); err == nil {
		t.Fatal("accepted missing original completion")
	}
}

func TestCompanionQualifiedManualHistory(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "qualified", true: "missing-proof"}[missing], func(t *testing.T) {
			s, j, input := artifactRegressionWorkspace(t, true, true)
			artifactRegressionPublish(t, s, j)
			artifactRegressionTransferManual(t, s, j)
			if missing {
				if err := s.artifactUpdate(func(st *artifactState) error { delete(st.Completions, j.CurrentAttemptID()); return nil }); err != nil {
					t.Fatal(err)
				}
			}
			before := artifactDatabaseBytes(t, s)
			err := s.db.View(func(tx *bolt.Tx) error {
				st, err := companionEligible(tx)
				if err != nil {
					return err
				}
				p, ok := st.ManualInputs[j.ID]
				if !ok || p.Ref != input || p.JobID != j.ID || len(st.Results) != 1 {
					t.Fatal("manual history lost its original pin/result")
				}
				return nil
			})
			if (err != nil) != missing {
				t.Fatalf("missing=%v: %v", missing, err)
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("manual qualification wrote source")
			}
		})
	}
}

func TestCompanionOutputPlanPrecedesFreshLineage(t *testing.T) {
	s, info := enableTest(t, DefaultRetentionPolicy())
	clock := s.now()
	artifactTestInstall(t, s)
	s.now = func() time.Time { return clock }
	ref := artifactTestComplete(t, s, testActor())
	policy, _, err := s.RetentionPolicy()
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.SubmitWorkspace(testActor(), &info.Domain, "queued-companion", artifactInputTask(ref), "local", "test-binding", backend.LocalOutput, uint64(backend.MaxLogBytes)+4*policy.OutputUnit)
	if err != nil {
		t.Fatal(err)
	}
	var plan outputRevalidation
	// Geometry values here are deliberately MODELED, not a different-volume
	// native test. The real planner and all validators remain in use.
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		plan, err = planOutputRevalidation(tx, 16384, 987, map[string]uint64{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return applyOutputRevalidation(tx, plan) }); err != nil {
		t.Fatal(err)
	}
	lineage := strings.Repeat("d", 32)
	when := s.now().Add(time.Minute)
	if err := s.db.Update(func(tx *bolt.Tx) error { return freshenRestoredTx(tx, lineage, when) }); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if st.Lineage != lineage || st.Lineage == info.Domain.Lineage || st.LegacyReplayOpen || !st.Restored || st.Policy.OutputUnit != 16384 || st.Policy.Volume != 987 {
			t.Fatal("geometry application overwrote restored lineage", st)
		}
		var m retainedJob
		if err := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(job.ID), &m); err != nil {
			return err
		}
		if m.OutputReserve != uint64(backend.MaxLogBytes)+4*16384 || m.Started || m.OutputStart != 0 {
			t.Fatal("future output reserve or launch history changed", m)
		}
		u, err := usage(tx)
		if err != nil {
			return err
		}
		if u[OutputBytesQuota] != 0 || u[OutputFilesQuota] != 0 {
			t.Fatal("empty output reconciliation invented payload")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitWorkspace(testActor(), &info.Domain, "old-domain", artifactInputTask(ref), "local", "test-binding", backend.LocalOutput, uint64(backend.MaxLogBytes)+4*16384); err == nil {
		t.Fatal("restored copy accepted original request domain")
	}
}

func TestCompanionOriginalCloseErrorIsSticky(t *testing.T) {
	fixture := &companionFixture{}
	base, err := companionExampleBase()
	if err != nil {
		fixture.fatal(t, err)
	}
	fixture.data = base
	op := &companionOperation{ctx: context.Background()}
	path := filepath.Join(base, "owned")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	op.input = companionFile{file: file}
	if err != nil {
		fixture.fatal(t, op.finish(err))
	}
	sentinel := errors.New("modeled close result")
	calls := 0
	op.input.closeFile = func(original *os.File) error {
		calls++
		if original != file {
			return errors.New("reconstructed owner")
		}
		// Positively close the real fixture, then MODEL a reported failure.
		// This is not a native Close-failure or retained-FD proof.
		if err := original.Close(); err != nil {
			return err
		}
		return sentinel
	}
	for i := 0; i < 2; i++ {
		actual := op.input.close()
		// Cache the actual result in companionFile, then retain its original
		// operation before any assertion can exit the test goroutine.
		kept := fixture.operation(op.finish(actual))
		if !errors.Is(actual, sentinel) {
			fixture.fatal(t, errors.Join(errors.New("lost original close cause"), kept))
		}
	}
	if calls != 1 || op.input.file != file || !op.input.attempted {
		fixture.fatal(t, errors.New("close retried or original frame lost"))
	}
	if err := op.input.take(nil); err == nil {
		fixture.fatal(t, errors.New("uncertain frame accepted another acquisition"))
	}
	t.Logf("retained modeled uncertain-owner case: %s", base)
}

func TestCompanionPublishedParentCloseErrorRetainsDestination(t *testing.T) {
	fixture := &companionFixture{}
	base, err := companionExampleBase()
	if err != nil {
		fixture.fatal(t, err)
	}
	fixture.data = base
	destination := filepath.Join(base, "published")
	op, err := newCompanion(context.Background(), destination)
	if err != nil {
		fixture.fatal(t, op.finish(err))
	}
	// This tests private publication custody, not eligibility or a backup round
	// trip. The other tests exercise complete real bundles through public APIs.
	if err := op.makeStage("objects"); err != nil {
		fixture.fatal(t, op.finish(err))
	}
	original := op.parent.file
	sentinel := errors.New("modeled publication-parent Close result")
	calls := 0
	op.parent.closeFile = func(file *os.File) error {
		calls++
		if file != original {
			return errors.New("publication reconstructed its parent owner")
		}
		if err := file.Close(); err != nil {
			return err
		}
		return sentinel
	}
	if err := op.publish(); err != nil {
		fixture.fatal(t, op.finish(err))
	}
	err = op.finish(nil)
	fixture.operation(err)
	var held *companionError
	if !errors.As(err, &held) || !errors.Is(err, sentinel) || held.operation != op || !op.published || op.parent.file != original {
		fixture.fatal(t, errors.Join(errors.New("lost published uncertainty"), err))
	}
	if _, statErr := os.Stat(destination); statErr != nil {
		fixture.fatal(t, errors.Join(errors.New("published destination removed"), statErr))
	}
	if !strings.Contains(err.Error(), destination) {
		fixture.fatal(t, errors.Join(errors.New("published diagnostic omits actual destination"), err))
	}
	if !errors.Is(op.finish(nil), sentinel) || calls != 1 {
		fixture.fatal(t, errors.New("original parent Close retried"))
	}
	t.Logf("retained modeled published-but-unsettled case: %s", base)
}

func TestCompanionSettledArtifactsRefuseOutputAndNativeObligations(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "output-reservation", true: "native-cleanup"}[native], func(t *testing.T) {
			s, info := enableTest(t, DefaultRetentionPolicy())
			clock := s.now()
			artifactTestInstall(t, s)
			s.now = func() time.Time { return clock }
			artifactTestComplete(t, s, testActor())
			storage, target, reserve := backend.LocalOutput, "local", uint64(backend.MaxLogBytes)+4*info.Policy.OutputUnit
			if native {
				storage, target, reserve = backend.RemoteOutput, "remote", 0
			}
			j, err := s.SubmitRetained(testActor(), info.Domain, "obligation", backend.Task{Argv: []string{"work"}}, target, "test-binding", storage, reserve)
			if err != nil {
				t.Fatal(err)
			}
			j, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), reserve, s.now())
			if err != nil || !start {
				t.Fatal(start, err)
			}
			zero := int32(0)
			// Positive status here is MODELED trusted backend evidence. There is
			// no process; these are actual Store admission/cleanup transactions.
			if _, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "original", ExitCode: &zero, Termination: backend.StoppedWithExitCode}, s.now()); err != nil {
				t.Fatal(err)
			}
			if native {
				page, err := s.Prune(testActor(), PruneSelection{Before: s.now().Add(time.Hour), Mode: Whole}, true, nil, nil, s.now())
				if err != nil || page.Marked != 1 {
					t.Fatal(page, err)
				}
				record, found, err := s.NextCleanup(0)
				if err != nil || !found || record.Operation != NativeCleanup {
					t.Fatal(record, found, err)
				}
			}
			before := artifactDatabaseBytes(t, s)
			if err := s.db.View(func(tx *bolt.Tx) error { _, err := companionEligible(tx); return err }); err == nil {
				t.Fatal("settled artifacts hid unrelocated physical obligation")
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("refusal rewrote pending obligation")
			}
		})
	}
}
