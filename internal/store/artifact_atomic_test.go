package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// These exercise actual metadata transactions with MODELED trusted physical
// handoffs. No test creates a native writer or proves filesystem quiescence.
// All are UNRUN under the S06 execution hold.
func atomicWorkspace(t *testing.T, outputs, retry bool) (*Store, backend.Job, backend.Reference, backend.Status, WorkspaceCompletion) {
	t.Helper()
	s := openTest(t)
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, testActor())
	task := artifactInputTask(input)
	if outputs {
		task.Outputs = []string{"result"}
	}
	if retry {
		task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}, DelaySeconds: 1}
	}
	j, err := s.SubmitWorkspace(testActor(), nil, "atomic", task, "remote", "test-binding", backend.RemoteOutput, 0)
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
	i, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	exec := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: "i"}
	one := int32(1)
	status := backend.Status{State: backend.Failed, Instance: "i", ExitCode: &one, Termination: backend.StoppedWithExitCode}
	kind := WorkspacePhysicalNoOutputs
	if outputs {
		kind = WorkspacePhysicalNeverAttempted
	}
	c := WorkspaceCompletion{Outcome: WorkspaceTaskExit, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: i.ID, JobID: j.ID, Execution: exec}, Origin: &WorkspacePhysicalOrigin{Parent: ArtifactFile{17, 700}, Root: ArtifactFile{17, 701}, RootState: WorkspaceRootOwned, CandidateState: kind}}
	return s, j, exec, status, c
}

func TestAtomicWorkspaceMissingOriginalFactsStaysHeld(t *testing.T) {
	for _, missing := range []string{"writer", "origin"} {
		t.Run(missing, func(t *testing.T) {
			s, j, exec, status, c := atomicWorkspace(t, true, false)
			if missing == "writer" {
				c.Writer = nil
			} else {
				c.Origin = nil
			}
			before := artifactTestUsage(t, s)
			out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
			if err != nil {
				t.Fatal(err)
			}
			if out.State != WorkspaceCommitHeld || out.Cleanup != nil || out.Job.Status.State != backend.Running {
				t.Fatalf("missing %s became terminal: %+v", missing, out)
			}
			if artifactTestUsage(t, s) != before {
				t.Fatal("Held released reservations")
			}
			if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
				t.Fatal("Held created cleanup authority", found, err)
			}
			r, err := s.ArtifactResultFor(testActor(), j.ID, exec.ID)
			if err != nil || r.State != "held" || r.Reference != nil {
				t.Fatal(r, err)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, e := artifactLoad(tx)
				if e != nil {
					return e
				}
				if len(st.Completions) != 0 {
					t.Fatal("Held stored completion")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAtomicWorkspaceDerivesDispositionAndMarksWithCompletion(t *testing.T) {
	for _, tt := range []struct {
		name    string
		outputs bool
		state   WorkspacePhysicalCandidateState
		want    WorkspaceCandidateDisposition
	}{
		{"no-output", false, WorkspacePhysicalNoOutputs, WorkspaceNoOutputs},
		{"never-attempted", true, WorkspacePhysicalNeverAttempted, WorkspaceCandidateNeverCreated},
		{"empty-malformed-prefix", true, WorkspacePhysicalClosedPrefix, WorkspaceCandidateClosedUnpublished},
		{"sealed-result", true, WorkspacePhysicalSealed, WorkspaceResultPublishedIndependent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, j, exec, status, c := atomicWorkspace(t, tt.outputs, false)
			c.Origin.CandidateState = tt.state
			r, err := s.GetWorkspaceReservation(j.ID, exec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tt.state == WorkspacePhysicalClosedPrefix {
				c.Origin.Prefix = &WorkspaceCandidatePrefix{ID: r.ResultID, File: ArtifactFile{17, 702}, SHA256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
			}
			if tt.state == WorkspacePhysicalSealed {
				zero := int32(0)
				status.State = backend.Succeeded
				status.ExitCode = &zero
				ref := ArtifactRef{ID: r.ResultID, SHA256: strings.Repeat("f", 64), Size: 1}
				c.Candidate = &WorkspaceCandidate{Reference: ref, File: ArtifactFile{17, 703}, Manifest: ArtifactManifest{Reference: ref, ExpandedBytes: 1, RegularFiles: 1}}
			}
			before := artifactTestUsage(t, s)
			out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
			if err != nil {
				t.Fatal(err)
			}
			if out.State != WorkspaceCommitMarked || out.Cleanup == nil || !out.Job.Status.State.Terminal() {
				t.Fatal(out)
			}
			work := *out.Cleanup
			if work.Origin.Disposition != tt.want || work.Origin.Subject.CompletionDigest == ([32]byte{}) || work.Origin.Subject.WriterReceipt == ([32]byte{}) || work.Origin.Producer != (OriginalServerLifetime{}) {
				t.Fatal("Store-derived subject missing", work)
			}
			if err := s.db.View(func(tx *bolt.Tx) error {
				st, e := artifactLoad(tx)
				if e != nil {
					return e
				}
				if len(st.Completions) != 1 || len(st.Deletes) != 1 || st.Deletes[work.ID].Workspace.Producer != nil {
					t.Fatal("terminal without exact optional-identity mark")
				}
				return validateSnapshot(tx)
			}); err != nil {
				t.Fatal(err)
			}
			if got := artifactTestUsage(t, s); got[aWorkspaces] != before[aWorkspaces] {
				t.Fatal("mark credited root")
			}
			result, err := s.ArtifactResultFor(testActor(), j.ID, exec.ID)
			if err != nil {
				t.Fatal(err)
			}
			if (result.Reference != nil) != (tt.state == WorkspacePhysicalSealed) {
				t.Fatal("caller candidate disposition created wrong grant", result)
			}
		})
	}
}

func TestAtomicWorkspaceReplayAfterNewAttemptAndVerifiedRemoval(t *testing.T) {
	s, j, exec, status, c := atomicWorkspace(t, true, true)
	out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil || out.Cleanup == nil || out.Job.NextAttemptAt == nil {
		t.Fatal(out, err)
	}
	work := *out.Cleanup
	next, start, err := s.BeginAttempt(j.ID, exec.ID, 0, *out.Job.NextAttemptAt)
	if err != nil || !start || next.CurrentAttemptID() == exec.ID {
		t.Fatal(next, start, err)
	}
	before := artifactDatabaseBytes(t, s)
	replay, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil || replay.State != WorkspaceCommitMarked || replay.Cleanup == nil || *replay.Cleanup != work || replay.Job.CurrentAttemptID() != next.CurrentAttemptID() {
		t.Fatal("old exact completion lost replay", replay, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("exact callback replay wrote database")
	}
	// DTO mutation cannot alter persisted source facts or become a replay upgrade.
	replay.Cleanup.Origin.Producer = OriginalServerLifetime{Host: strings.Repeat("a", 32)}
	c.Origin.Producer = OriginalServerLifetime{Host: strings.Repeat("a", 32), Boot: strings.Repeat("b", 32), NamespaceInode: 1, PID: 1}
	if _, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("unknown producer upgraded on replay", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("conflicting callback changed state")
	}
	c.Origin.Producer = OriginalServerLifetime{}
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{Work: work}); err != nil {
		t.Fatal(err)
	}
	before = artifactDatabaseBytes(t, s)
	replay, err = s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil || replay.State != WorkspaceCommitRemoved || replay.Cleanup != nil {
		t.Fatal("removed intent resurrected", replay, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("Removed replay wrote database")
	}
}

func TestAtomicWorkspaceInvalidOriginRollsBackAllMetadata(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*WorkspaceCompletion)
	}{
		{"foreign-volume", func(c *WorkspaceCompletion) { c.Origin.Parent.Device++ }},
		{"aliased-root", func(c *WorkspaceCompletion) { c.Origin.Root = c.Origin.Parent }},
		{"unknown-root", func(c *WorkspaceCompletion) { c.Origin.RootState = 0 }},
		{"invented-published", func(c *WorkspaceCompletion) { c.Origin.CandidateState = WorkspacePhysicalSealed }},
		{"unknown-prefix", func(c *WorkspaceCompletion) { c.Origin.CandidateState = WorkspacePhysicalClosedPrefix }},
		{"partial-producer", func(c *WorkspaceCompletion) { c.Origin.Producer.PID = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, j, exec, status, c := atomicWorkspace(t, true, false)
			tt.change(&c)
			before := artifactDatabaseBytes(t, s)
			out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
			if !errors.Is(err, ErrArtifactConflict) || out.Cleanup != nil {
				t.Fatal(out, err)
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("refused origin leaked terminal/result/gc/counters")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAtomicWorkspaceCombinedActualWriteBoundary(t *testing.T) {
	s, j, exec, status, c := atomicWorkspace(t, true, true)
	one := int32(1)
	change := workspaceCompletionChange{jobID: j.ID, execution: exec, status: status, completion: c, original: &one, digest: [32]byte{1}, operation: strings.Repeat("d", 32), now: s.now()}
	rollback := errors.New("retain real completion transaction measurement")
	var measured uint64
	var created int
	apply := func(tx *bolt.Tx, budget uint64, records int) error {
		before := artifactObserveRows(t, tx)
		out, err := change.apply(tx, s.Owner(), budget, records)
		if err != nil {
			return err
		}
		if out.State != WorkspaceCommitMarked || out.Cleanup == nil {
			t.Fatal(out)
		}
		after := artifactObserveRows(t, tx)
		core, coreRecords := artifactObservedDelta(before, after, false)
		artifacts, artifactRecords := artifactObservedDelta(before, after, true)
		measured = core + artifacts
		created = coreRecords + artifactRecords
		if core == 0 || artifacts == 0 || created < 2 || measured >= maxChangeBytes {
			t.Fatal("measurement omitted core/completion/gc writes", core, artifacts, created)
		}
		if err := validateSnapshot(tx); err != nil {
			return err
		}
		return rollback
	}
	original := artifactDatabaseBytes(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, maxChangeBytes, 100) }); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	exact, rows := measured, created // Actual raw changes, not production forecast.
	for _, tt := range []struct {
		name    string
		bytes   uint64
		records int
		want    error
	}{
		{"exact", exact, rows, rollback},
		{"one-byte-short", exact - 1, 100, errArtifactMutationBudget},
		{"one-record-short", maxChangeBytes, rows - 1, errArtifactMutationBudget},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, tt.bytes, tt.records) }); !errors.Is(err, tt.want) {
				t.Fatal(err)
			}
			if !bytes.Equal(original, artifactDatabaseBytes(t, s)) {
				t.Fatal("failed actual completion transaction leaked writes")
			}
		})
	}
}

func TestAtomicWorkspaceLegacyCompletionWithoutMarkIsNotRepaired(t *testing.T) {
	s, j, exec, status, c := atomicWorkspace(t, false, false)
	out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil {
		t.Fatal(err)
	}
	// Model an old canonical prototype completion predating atomic marks.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, err := artifactChange(tx, maxChangeBytes, func(st *artifactState) error { delete(st.Deletes, out.Cleanup.ID); return nil })
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("missing legacy mark repaired", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("replay invented a physical origin")
	}
}

func TestAtomicWorkspaceProducerCanonicalCorruptionRefuses(t *testing.T) {
	s, j, exec, status, c := atomicWorkspace(t, false, false)
	out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(artifactsBucket).Bucket([]byte("gc"))
		var d artifactDelete
		if err := artifactDecode(b.Get([]byte(out.Cleanup.ID)), &d); err != nil {
			return err
		}
		d.Workspace.Producer = &OriginalServerLifetime{} // A nonnil unknown is invalid.
		raw, err := json.Marshal(d)
		if err != nil {
			return err
		}
		return b.Put([]byte(out.Cleanup.ID), raw)
	}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	if err := s.db.View(validateSnapshot); err == nil {
		t.Fatal("nonnil-zero producer accepted")
	}
	if _, _, err := s.NextArtifactCleanup(""); err == nil {
		t.Fatal("corrupt origin dispatched")
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("refusal repaired malformed producer")
	}
}

func TestAtomicWorkspaceKnownProducerRetainedWithoutAliasing(t *testing.T) {
	s, j, exec, status, c := atomicWorkspace(t, false, false)
	producer := artifactKnownProducer()
	c.Origin.Producer = producer
	out, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil || out.Cleanup == nil {
		t.Fatal(out, err)
	}
	work := *out.Cleanup
	if work.Origin.Producer != producer {
		t.Fatal("mark omitted original producer")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		copy := st.clone()
		copy.Deletes[work.ID].Workspace.Producer.StartTicks++
		if *st.Deletes[work.ID].Workspace.Producer != producer {
			t.Fatal("cloned gc aliased original producer")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
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
	replay, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, c, s.now())
	if err != nil || replay.Cleanup == nil || *replay.Cleanup != work {
		t.Fatal("restart lost exact marked callback", replay, err)
	}
	wrong := work
	wrong.Origin.Producer.StartTicks++
	before := artifactDatabaseBytes(t, s)
	if err := s.FinishWorkspaceCleanup(wrong, WorkspaceCleanupReceipt{Work: wrong}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("changed producer finished original root", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("producer conflict changed root charges")
	}
}
