//go:build linux || darwin

package store

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// UNRUN source. These are metadata admission tests, not backup round trips.
// Root geometry and the stopped backend status are MODELED; no archive bytes,
// original process, filesystem closure or external writer death is qualified.
// Jobs, activation, placement and validation use the existing Store APIs. No
// private edit clears Legacy, creates a completion or manufactures a start bit.
func TestCompanionLegacyQueuedIsNotPositiveNeverStarted(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "queued"
		if terminal {
			name = "terminal-without-proof"
		}
		t.Run(name, func(t *testing.T) {
			// Existing public pre-activation Submit -> EnableRetention fixture.
			// Inherited Started=false is intentionally not positive no-launch proof.
			f := legacyRetirementSetup(t, 1, false)
			companionLegacyInstall(t, f.s)
			if err := f.s.BindOutputStorage("remote", "test-binding", backend.RemoteOutput, 0); err != nil {
				t.Fatal(err)
			}
			job := f.jobs[0]
			if terminal {
				var err error
				job, err = f.s.UpdateFor(testActor(), job.ID, func(j *backend.Job) error {
					j.Status = backend.Status{State: backend.Canceled}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			m, _ := legacyRetirementMeta(t, f.s, job.ID)
			if !m.Legacy || m.Started || m.OutputStart != 0 || m.Storage != backend.RemoteOutput || job.Status.Instance != "" || len(job.Attempts) != 0 || safeStatuses(job) {
				t.Fatal("fixture does not isolate inherited never-start ambiguity", m, job)
			}
			// Remote placement isolates F1 from the separate unspecified-output
			// guard: declaring placement must not mint never-start provenance.
			companionLegacyEligibilityUnchanged(t, f.s, false)
		})
	}
}

func TestCompanionModernNeverStartedQueuedRemainsEligible(t *testing.T) {
	policy := DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	s, info := enableTest(t, policy)
	companionLegacyInstall(t, s)
	job, err := s.SubmitRetained(testActor(), info.Domain, "modern-never-started", backend.Task{Argv: []string{"never-executed"}}, "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	m, _ := legacyRetirementMeta(t, s, job.ID)
	if m.Legacy || m.Started || m.OutputStart != 0 || job.Status.State != backend.Queued || job.Status.Instance != "" || len(job.Attempts) != 0 || job.NextAttemptAt != nil || job.CancelRequested {
		t.Fatal("fixture is not a modern unstarted acceptance", m, job)
	}
	companionLegacyEligibilityUnchanged(t, s, true)
}

func TestCompanionLegacyStoppedNeedsExplicitOutputPlacement(t *testing.T) {
	// Execution transitions happen while retention is disabled. After activation
	// their retained row still has Legacy=true and UnspecifiedOutput, exactly as
	// a historical execution with no declared output placement does in production.
	base, err := companionExampleBase()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(base, "state")
	f := &companionFixture{data: directory}
	t.Cleanup(func() {
		if f.held != nil {
			t.Logf("retaining held legacy fixture %s: %v", base, f.held)
			return
		}
		if err := f.stop(); err != nil {
			t.Errorf("close original legacy Store (retaining %s): %v", base, err)
			return
		}
		if err := f.closeRestored(); err != nil {
			t.Errorf("close reopened legacy Store (retaining %s): %v", base, err)
			return
		}
		if t.Failed() || f.keeping {
			t.Logf("retaining failed legacy fixture: %s", base)
			return
		}
		if err := os.RemoveAll(base); err != nil {
			f.hold(err)
			t.Error(err)
		}
	})
	f.s, err = Open(directory)
	if err != nil {
		f.fatal(t, err)
	}
	s := f.s
	job, err := s.Submit(testActor(), "legacy-stopped-unspecified", backend.Task{Argv: []string{"modeled-execution"}}, "remote", "test-binding")
	if err != nil {
		t.Fatal(err)
	}
	now := job.CreatedAt.Add(time.Second)
	job, started, err := s.BeginAttempt(job.ID, job.CurrentAttemptID(), 0, now)
	if err != nil || !started {
		t.Fatal("public pre-activation launch intent", started, err)
	}
	zero := int32(0)
	// Trusted backend evidence is MODELED. A real process is not launched; the
	// purpose is to prove that stopped status alone cannot declare placement.
	job, err = s.ObserveAttempt(job.ID, job.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "modeled-original", ExitCode: &zero, Termination: backend.StoppedWithExitCode}, now.Add(time.Second))
	if err != nil || !safeStatuses(job) {
		t.Fatal("modeled original stopped status", job, err)
	}
	// stop marks its sole original Close attempted before entering Close. A
	// failure keeps f.s and the exact cause; cleanup cannot retry or remove it.
	if err := f.stop(); err != nil {
		f.fatal(t, err)
	}
	policy := DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	if _, err := EnableRetention(directory, policy); err != nil {
		f.fatal(t, err)
	}
	f.restored, err = Open(directory)
	if err != nil {
		f.fatal(t, err)
	}
	// The reopened Store has a separate original slot and one-shot Close flag.
	s = f.restored
	companionLegacyInstall(t, s)
	m, _ := legacyRetirementMeta(t, s, job.ID)
	if !m.Legacy || m.Started || m.Storage != backend.UnspecifiedOutput || job.Status.State != backend.Succeeded || job.Status.Instance == "" || !safeStatuses(job) {
		t.Fatal("fixture does not isolate stopped unspecified historical output", m, job)
	}
	companionLegacyEligibilityUnchanged(t, s, false)

	// This public declaration changes only placement metadata. Remote output is
	// explicitly borrowed external state; this does not claim to relocate bytes
	// or prove that any remote writer has died.
	beforeJob, err := s.Get(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindOutputStorage("remote", "test-binding", backend.RemoteOutput, 0); err != nil {
		t.Fatal(err)
	}
	bound, _ := legacyRetirementMeta(t, s, job.ID)
	want := m
	want.Storage = backend.RemoteOutput
	if !reflect.DeepEqual(bound, want) {
		t.Fatal("placement declaration changed execution provenance", bound)
	}
	afterJob, err := s.Get(job.ID)
	if err != nil || !reflect.DeepEqual(afterJob, beforeJob) {
		t.Fatal("placement declaration rewrote accepted work or stopped evidence", afterJob, err)
	}
	companionLegacyEligibilityUnchanged(t, s, true)
}

func companionLegacyInstall(t *testing.T, s *Store) {
	t.Helper()
	// MODELED verified-root handoff, passed through the actual public install
	// transaction. The installed root is empty; no object/archive receipt exists.
	if _, err := s.InstallArtifacts(ArtifactInstallation{Enabled: true, ID: strings.Repeat("a", 32), Unit: 4096, Volume: 17}); err != nil {
		t.Fatal(err)
	}
}

func companionLegacyEligibilityUnchanged(t *testing.T, s *Store, eligible bool) {
	t.Helper()
	// Validate independently so a corrupt setup cannot satisfy a refusal oracle.
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal("invalid legacy admission fixture", err)
	}
	before := artifactDatabaseBytes(t, s)
	err := s.db.View(func(tx *bolt.Tx) error {
		_, err := companionEligible(tx)
		return err
	})
	if (err == nil) != eligible {
		t.Fatalf("eligible=%v: %v", eligible, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("companion eligibility changed original database bytes")
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal("eligibility changed validated source state", err)
	}
}
