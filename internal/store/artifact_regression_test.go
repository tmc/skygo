package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These are source-only regression tests. Clock, archive/file and writer
// receipts are MODELED; they make no native or real-elapsed-time claim.
func artifactRegressionWorkspace(t *testing.T, retained, outputs bool) (*Store, backend.Job, ArtifactRef) {
	t.Helper()
	var s *Store
	var domain *RequestDomain
	if retained {
		policy := DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		var info RetentionInfo
		s, info = enableTest(t, policy)
		domain = &info.Domain
	} else {
		s = openTest(t)
	}
	clock := s.now().UTC()
	artifactTestInstall(t, s)
	if retained {
		s.now = func() time.Time { return clock }
	}
	input := artifactTestComplete(t, s, testActor())
	task := backend.Task{Argv: []string{"true"}, Workdir: &artifact.Ref{ID: input.ID, SHA256: input.SHA256, Size: int64(input.Size)}}
	if outputs {
		task.Outputs = []string{"result"}
	}
	job, err := s.SubmitWorkspace(testActor(), domain, "workspace-fence", task, "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	job, start, err := s.BeginAttempt(job.ID, job.CurrentAttemptID(), 0, s.now())
	if err != nil || !start {
		t.Fatalf("begin: %v %v", start, err)
	}
	job, err = s.ObserveAttempt(job.ID, job.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, s.now())
	if err != nil {
		t.Fatal(err)
	}
	return s, job, input
}

// This helper exercises the real atomic Store completion boundary with MODELED
// writer and seal receipts. It proves no actual filesystem/process quiescence.
func artifactRegressionComplete(t *testing.T, s *Store, job backend.Job, remove bool) ArtifactRef {
	t.Helper()
	execution := backend.Reference{ID: job.CurrentAttemptID(), Task: job.Task, Binding: job.TargetBinding, Instance: "i"}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	completion := WorkspaceCompletion{Outcome: WorkspaceTaskExit, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: job.ID, Execution: execution}, Origin: maintenancePhysicalOrigin(false)}
	var ref ArtifactRef
	if len(job.Task.Outputs) != 0 {
		if err := s.db.View(func(tx *bolt.Tx) error {
			st, e := artifactLoad(tx)
			if e != nil {
				return e
			}
			ref = ArtifactRef{ID: st.Workspaces[execution.ID].ResultID, SHA256: strings.Repeat("f", 64), Size: 1}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		completion.Candidate = &WorkspaceCandidate{Reference: ref, File: ArtifactFile{17, 43}, Manifest: ArtifactManifest{Reference: ref, ExpandedBytes: 1, RegularFiles: 1}}
		completion.Origin.CandidateState = WorkspacePhysicalSealed
	}
	zero := int32(0)
	if _, err := s.CompleteWorkspaceAttempt(job.ID, execution, backend.Status{State: backend.Succeeded, Instance: "i", ExitCode: &zero}, completion, s.now()); err != nil {
		t.Fatal(err)
	}
	if remove {
		artifactRegressionRemove(t, s, execution.ID, s.now())
	}
	return ref
}

func artifactRegressionRemove(t *testing.T, s *Store, attempt string, now time.Time) {
	t.Helper()
	var job string
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		job = st.Workspaces[attempt].JobID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	work := maintenanceWork(t, s, job, attempt)
	oldNow := s.now
	s.now = func() time.Time { return now }
	defer func() { s.now = oldNow }()
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{Work: work}); err != nil {
		t.Fatal(err)
	}
}

func artifactRegressionPublish(t *testing.T, s *Store, job backend.Job) ArtifactRef {
	return artifactRegressionComplete(t, s, job, true)
}

func artifactRegressionObject(t *testing.T, s *Store, id string) artifactObject {
	t.Helper()
	var out artifactObject
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		var ok bool
		out, ok = st.Objects[id]
		if !ok {
			return ErrNotFound
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestArtifactResultGraceSurvivesRevocationRollbackAndReopen(t *testing.T) {
	for _, hold := range []time.Duration{0, 25 * time.Hour} {
		t.Run(hold.String(), func(t *testing.T) {
			s, job, _ := artifactRegressionWorkspace(t, false, true)
			ref := artifactRegressionPublish(t, s, job)
			original := artifactRegressionObject(t, s, ref.ID)
			now := s.now().Add(hold)
			s.now = func() time.Time { return now }
			before := artifactTestUsage(t, s)
			rollback := errors.New("after result revocation")
			err := s.db.Update(func(tx *bolt.Tx) error {
				_, err := artifactChange(tx, 1<<20, func(st *artifactState) error {
					return artifactExpireResult(st, "legacy", job.ID, job.CurrentAttemptID(), now)
				})
				if err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if got := artifactRegressionObject(t, s, ref.ID); got != original {
				t.Fatal("rollback changed object deadline")
			}
			if got := artifactTestUsage(t, s); got != before {
				t.Fatal("rollback changed counters")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, err := artifactLoad(tx)
				if err != nil {
					return err
				}
				return artifactGranted(st, "legacy", ref, now)
			}); err != nil {
				t.Fatal("rollback lost pinned grant", err)
			}
			if err := s.artifactUpdate(func(st *artifactState) error {
				return artifactExpireResult(st, "legacy", job.ID, job.CurrentAttemptID(), now)
			}); err != nil {
				t.Fatal(err)
			}
			deadline := now.Add(time.Hour)
			if deadline.Before(original.Expires) {
				deadline = original.Expires
			}
			if got := artifactRegressionObject(t, s, ref.ID); !got.DeleteAfter.Equal(deadline) {
				t.Fatalf("delete floor %v, want %v", got.DeleteAfter, deadline)
			}
			dir := s.Directory()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			s.now = func() time.Time { return now }
			if got := artifactRegressionObject(t, s, ref.ID); !got.DeleteAfter.Equal(deadline) {
				t.Fatal("reopen lost deletion floor")
			}
			if _, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("revoked grant renewed: %v", err)
			}
			result, err := s.ArtifactResultFor(testActor(), job.ID, job.CurrentAttemptID())
			if err != nil || result.State != "expired" || result.Reference != nil {
				t.Fatalf("expiry witness: %+v %v", result, err)
			}
			operation := strings.Repeat("d", 32)
			for _, at := range []time.Time{now, deadline.Add(-time.Nanosecond)} {
				if err := s.artifactUpdate(func(st *artifactState) error { return artifactMarkObjectDelete(st, ref.ID, operation, at) }); !errors.Is(err, ErrArtifactConflict) {
					t.Fatalf("early mark %v: %v", at, err)
				}
			}
			if got := artifactTestUsage(t, s); got[aCompletedBytes] != before[aCompletedBytes] || got[aCompletedCount] != before[aCompletedCount] {
				t.Fatal("grace credited object capacity")
			}
			if err := s.artifactUpdate(func(st *artifactState) error { return artifactMarkObjectDelete(st, ref.ID, operation, deadline) }); err != nil {
				t.Fatal(err)
			}
			if err := s.artifactUpdate(func(st *artifactState) error { return artifactFinishDelete(st, operation, st.Deletes[operation]) }); err != nil {
				t.Fatal(err)
			}
			if got := artifactTestUsage(t, s); got[aCompletedBytes] != before[aCompletedBytes]-ref.Size || got[aCompletedCount] != before[aCompletedCount]-1 {
				t.Fatal("trusted cleanup did not release exact object capacity")
			}
		})
	}
}

func TestArtifactInputGraceAndManualTransfer(t *testing.T) {
	for _, manual := range []bool{false, true} {
		t.Run(map[bool]string{false: "whole-final-release", true: "manual-transfer-retains-pin"}[manual], func(t *testing.T) {
			s, job, input := artifactRegressionWorkspace(t, true, false)
			original := artifactRegressionObject(t, s, input.ID)
			artifactRegressionComplete(t, s, job, false)
			now := s.now().Add(25 * time.Hour)
			s.now = func() time.Time { return now }
			if manual {
				artifactRegressionTransferManual(t, s, job)
			}
			artifactRegressionRemove(t, s, job.CurrentAttemptID(), now)
			if !manual {
				page, err := s.Prune(testActor(), PruneSelection{Before: now.Add(time.Hour), Mode: Whole}, true, nil, nil, now)
				if err != nil || page.Marked != 1 {
					t.Fatalf("whole: %+v %v", page, err)
				}
			}
			got := artifactRegressionObject(t, s, input.ID)
			if manual {
				if !got.DeleteAfter.Equal(original.DeleteAfter) {
					t.Fatal("manual transfer treated retained pin as release")
				}
			} else if !got.DeleteAfter.Equal(now.Add(time.Hour)) {
				t.Fatal("last input release lost grace")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, err := artifactLoad(tx)
				if err != nil {
					return err
				}
				g, ok := st.Grants[input.ID]
				if !ok || !g.Expires.Equal(original.Expires) {
					t.Fatal("GC grace changed read-grant expiry")
				}
				if manual && (!st.pinned(input.ID) || st.ManualInputs[job.ID].Ref != input) {
					t.Fatal("manual transfer lost bounded concrete pin")
				}
				return validateSnapshot(tx)
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.artifactUpdate(func(st *artifactState) error {
				return artifactMarkObjectDelete(st, input.ID, strings.Repeat("d", 32), now)
			}); !errors.Is(err, ErrArtifactConflict) {
				t.Fatal("premature input deletion", err)
			}
		})
	}
}

// Public prune performs the real shared read-only forecast and commit. A live
// workspace is transferred with the taskless manual header, never inferred dead.
func artifactRegressionTransferManual(t *testing.T, s *Store, job backend.Job) {
	t.Helper()
	current, err := s.GetFor(testActor(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Status.State.Terminal() {
		artifactRegressionComplete(t, s, current, false)
	}
	page, err := s.Prune(testActor(), PruneSelection{Before: s.now().Add(time.Hour), Mode: MetadataOnly, AcknowledgeUnverifiedTerminal: true}, true, nil, nil, s.now())
	if err != nil || page.Marked != 1 {
		t.Fatalf("manual transfer: %+v %v", page, err)
	}
}

func TestArtifactWorkspaceBindingAndInstanceCorruptionRefuses(t *testing.T) {
	for _, test := range []struct {
		name       string
		manual     bool
		change     func(*artifactWorkspace)
		sameLength bool
	}{
		{"live-binding", false, func(w *artifactWorkspace) { w.Binding = "rest-binding" }, true},
		{"manual-binding", true, func(w *artifactWorkspace) { w.Binding = "rest-binding" }, true},
		{"stored-instance", false, func(w *artifactWorkspace) { w.Instance = "\x7f" }, true},
		{"closed-unbound", false, func(w *artifactWorkspace) { w.Instance = ""; w.WriterClosed = true; w.WriterReceipt = [32]byte{1} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, job, _ := artifactRegressionWorkspace(t, test.manual, false)
			if test.manual {
				artifactRegressionTransferManual(t, s, job)
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal("valid pre-tamper snapshot", err)
			}
			var oldUsage, badRow []byte
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(artifactsBucket)
				oldUsage = bytes.Clone(b.Get([]byte("usage")))
				rows := b.Bucket([]byte("workspaces"))
				old := bytes.Clone(rows.Get([]byte(job.CurrentAttemptID())))
				var w artifactWorkspace
				if err := artifactDecode(old, &w); err != nil {
					return err
				}
				test.change(&w)
				var err error
				badRow, err = json.Marshal(w)
				if err != nil {
					return err
				}
				if test.sameLength && len(badRow) != len(old) {
					t.Fatal("tamper fixture changed encoded width")
				}
				return rows.Put([]byte(job.CurrentAttemptID()), badRow)
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error { _, err := artifactLoad(tx); return err }); err == nil {
				t.Fatal("corrupt workspace loaded")
			}
			if err := s.db.View(validateSnapshot); err == nil {
				t.Fatal("corrupt snapshot accepted")
			}
			if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1); err == nil {
				t.Fatal("mutation accepted corrupt workspace")
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				b := tx.Bucket(artifactsBucket)
				if !bytes.Equal(b.Get([]byte("usage")), oldUsage) || !bytes.Equal(b.Bucket([]byte("workspaces")).Get([]byte(job.CurrentAttemptID())), badRow) {
					t.Fatal("refusal repaired rows/counters")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			dir := s.Directory()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil {
				t.Fatal(err)
			}
			if reopened, err := Open(dir); err == nil {
				reopened.Close()
				t.Fatal("corrupt installation opened")
			}
			after, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused Open wrote DB: %v", err)
			}
		})
	}
}
