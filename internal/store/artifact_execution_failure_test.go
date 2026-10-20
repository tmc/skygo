package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// All tests are UNRUN. Writer receipts model only Store's trust boundary;
// they do not claim Local can produce qualified signal/start-failure closure.
func TestArtifactExecutionFailureOutcomeAllocation(t *testing.T) {
	if WorkspaceTaskExit != 1 || WorkspaceSetupFailure != 2 || WorkspaceMaterializationFailure != 3 || WorkspaceCollectionFailure != 4 || WorkspaceCanceled != 5 || WorkspaceTimeout != 6 || WorkspaceUncertain != 7 || WorkspaceExecutionFailure != 8 {
		t.Fatal("workspace outcome values renumbered")
	}
	negative, zero := int32(-1), int32(0)
	for _, tt := range []struct {
		name   string
		status backend.Status
		bad    bool
	}{
		{"signal", backend.Status{State: backend.Failed, Reason: "task terminated by signal: killed"}, false},
		{"start", backend.Status{State: backend.Failed, Reason: "start task: executable file not found"}, false},
		{"negative-exit", backend.Status{State: backend.Failed, Reason: "task signaled", ExitCode: &negative}, true},
		{"fabricated-exit", backend.Status{State: backend.Failed, Reason: "task signaled", ExitCode: &zero}, true},
		{"missing-reason", backend.Status{State: backend.Failed}, true},
		{"blank-reason", backend.Status{State: backend.Failed, Reason: " \t"}, true},
		{"succeeded", backend.Status{State: backend.Succeeded, Reason: "start task failed"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, original, err := artifactCompletionStatus(WorkspaceExecutionFailure, tt.status)
			if (err != nil) != tt.bad {
				t.Fatalf("classification: %+v %v", status, err)
			}
			if err == nil && (status.ExitCode != nil || original != nil || status.Termination != backend.UnknownTermination) {
				t.Fatal("execution failure created task-exit/retry authority")
			}
		})
	}
}

func TestArtifactExecutionFailureNeedsWriterAndRejectsCandidate(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	execution := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: "i"}
	status := backend.Status{State: backend.Failed, Instance: "i", Reason: "task terminated by signal: killed"}
	completion := WorkspaceCompletion{Outcome: WorkspaceExecutionFailure}
	beforeUsage := artifactTestUsage(t, s)
	got, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
	if err != nil || got.Job.Status.State.Terminal() {
		t.Fatalf("missing writer became terminal: %+v %v", got, err)
	}
	if usage := artifactTestUsage(t, s); usage[aCompletedBytes] != beforeUsage[aCompletedBytes] || usage[aWorkspaces] != beforeUsage[aWorkspaces] {
		t.Fatal("unproved signal released reservation")
	}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	completion.Writer = &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: j.ID, Execution: execution}
	completion.Origin = maintenancePhysicalOrigin(true)
	reservation, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID())
	if err != nil {
		t.Fatal(err)
	}
	ref := ArtifactRef{ID: reservation.ResultID, SHA256: strings.Repeat("f", 64), Size: 1}
	completion.Candidate = &WorkspaceCandidate{Reference: ref, File: ArtifactFile{17, 43}, Manifest: ArtifactManifest{Reference: ref, ExpandedBytes: 1, RegularFiles: 1}}
	before := artifactDatabaseBytes(t, s)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("execution failure published a candidate", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("candidate refusal escaped rollback")
	}
	completion.Candidate = nil
	got, err = s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
	if err != nil || got.Job.Status.State != backend.Failed || got.Job.Status.ExitCode != nil || got.Job.Status.Termination != backend.UnknownTermination || got.Job.NextAttemptAt != nil {
		t.Fatalf("known execution failure: %+v %v", got, err)
	}
	result, err := s.ArtifactResultFor(testActor(), j.ID, j.CurrentAttemptID())
	if err != nil || result.State != "absent" || result.Reference != nil {
		t.Fatalf("execution failure result: %+v %v", result, err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		c := st.Completions[execution.ID]
		if c.Outcome != WorkspaceExecutionFailure || c.OriginalTaskExit != nil || c.RootRemoved {
			t.Fatal("completion record misclassified execution failure")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
	usage := artifactTestUsage(t, s)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now()); err != nil {
		t.Fatal("duplicate", err)
	}
	if artifactTestUsage(t, s) != usage {
		t.Fatal("duplicate changed charges")
	}
	completion.Outcome = WorkspaceSetupFailure
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("conflicting outcome", err)
	}
}

func TestArtifactExecutionFailureRetryAndRowDecoder(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, testActor())
	task := artifactInputTask(input)
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{1, 137}}
	j, err := s.SubmitWorkspace(testActor(), nil, "execution-failure", task, "remote", "test-binding", backend.RemoteOutput, 0)
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
	completion := WorkspaceCompletion{Outcome: WorkspaceExecutionFailure, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: j.ID, Execution: execution}, Origin: maintenancePhysicalOrigin(false)}
	commit, err := s.CompleteWorkspaceAttempt(j.ID, execution, backend.Status{State: backend.Failed, Instance: "i", Reason: "start task: permission denied"}, completion, s.now())
	if err != nil {
		t.Fatal(err)
	}
	j = commit.Job
	if j.Status.State != backend.Failed || j.Status.ExitCode != nil || j.NextAttemptAt != nil || len(j.Attempts) != 1 || j.Attempts[0].Status.ExitCode != nil {
		t.Fatal("known execution failure acquired retry deadline or synthetic signal exit")
	}
	dir := s.Directory()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal("new outcome did not reopen", err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		rows := tx.Bucket(artifactsBucket).Bucket([]byte("completions"))
		var c artifactCompletion
		if e := artifactDecode(rows.Get([]byte(execution.ID)), &c); e != nil {
			return e
		}
		c.Outcome = WorkspaceExecutionFailure + 1 // Same encoded digit width; no counter camouflage.
		bad, e := json.Marshal(c)
		if e != nil {
			return e
		}
		return rows.Put([]byte(execution.ID), bad)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(validateSnapshot); err == nil {
		t.Fatal("unknown private outcome decoded")
	}
	before := artifactDatabaseBytes(t, s)
	if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1); err == nil {
		t.Fatal("mutation accepted unknown completion outcome")
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("refusal repaired unknown outcome")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err = os.ReadFile(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir); err == nil {
		reopened.Close()
		t.Fatal("unknown outcome opened")
	}
	after, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused Open wrote DB: %v", err)
	}
}
