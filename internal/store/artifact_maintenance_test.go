package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// UNRUN Store behavior sources. Native retirement, root/candidate inode and
// unlink/Sync receipts are MODELED here, not platform/filesystem qualification.
func maintenanceWork(t *testing.T, s *Store, job, attempt string) WorkspaceCleanupWork {
	t.Helper()
	before := artifactDatabaseBytes(t, s)
	var out WorkspaceCleanupWork
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		for id, d := range st.Deletes {
			if d.Kind == "workspace" && d.ObjectID == attempt {
				out, err = artifactWorkspaceWork(st, id, d)
				if err != nil {
					return err
				}
				if out.Origin.Subject.JobID != job {
					return ErrArtifactConflict
				}
				return nil
			}
		}
		return ErrNotFound
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("committed-work selection wrote state")
	}
	return out
}

func maintenancePhysicalOrigin(outputs bool) *WorkspacePhysicalOrigin {
	o := &WorkspacePhysicalOrigin{Parent: ArtifactFile{17, 700}, Root: ArtifactFile{17, 701}, RootState: WorkspaceRootOwned, CandidateState: WorkspacePhysicalNoOutputs}
	if outputs {
		o.CandidateState = WorkspacePhysicalNeverAttempted
	}
	return o
}

func maintenanceFailedResult(t *testing.T, retained bool) (*Store, backend.Job) {
	t.Helper()
	return maintenanceFailedOrigin(t, retained, maintenancePhysicalOrigin(true))
}

func maintenanceFailedOrigin(t *testing.T, retained bool, origin *WorkspacePhysicalOrigin) (*Store, backend.Job) {
	t.Helper()
	s, j, _ := artifactRegressionWorkspace(t, retained, true)
	exec, status, completion := maintenanceFailureCallback(t, s, j, origin)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, exec, status, completion, s.now()); err != nil {
		t.Fatal(err)
	}
	return s, j
}

func maintenanceFailureCallback(t *testing.T, s *Store, j backend.Job, origin *WorkspacePhysicalOrigin) (backend.Reference, backend.Status, WorkspaceCompletion) {
	t.Helper()
	exec := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: "i"}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	if origin.Prefix != nil {
		reservation, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID())
		if err != nil {
			t.Fatal(err)
		}
		prefix := *origin.Prefix
		prefix.ID = reservation.ResultID
		copy := *origin
		copy.Prefix = &prefix
		origin = &copy
	}
	completion := WorkspaceCompletion{Outcome: WorkspaceSetupFailure, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: j.ID, Execution: exec}, Origin: origin}
	one := int32(1)
	return exec, backend.Status{State: backend.Failed, Instance: "i", ExitCode: &one}, completion
}

func TestWorkspaceCleanupUsesCompletedManualMembership(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, true, false)
	before := artifactDatabaseBytes(t, s)
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		_, err = artifactCleanupSubject(st, j.ID, j.CurrentAttemptID())
		return err
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("uncompleted subject", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("subject query wrote state")
	}
	artifactRegressionComplete(t, s, j, false)
	live := maintenanceWork(t, s, j.ID, j.CurrentAttemptID()).Origin.Subject
	if live.TaskDigest == ([32]byte{}) || live.CompletionDigest == ([32]byte{}) || live.WriterReceipt == ([32]byte{}) || live.Instance != "i" || live.ResultState != "none" || live.ResultRef != (ArtifactRef{}) || live.ResultFile != (ArtifactFile{}) {
		t.Fatalf("incomplete subject: %+v", live)
	}
	artifactRegressionTransferManual(t, s, j)
	if _, err := s.Get(j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("fixture retained job", err)
	}
	manual := maintenanceWork(t, s, j.ID, j.CurrentAttemptID()).Origin.Subject
	if manual != live {
		t.Fatalf("manual subject: %+v", manual)
	}
	if _, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID()); err == nil {
		t.Fatal("cleanup snapshot became launch authority")
	}
	before = artifactDatabaseBytes(t, s)
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		_, err = artifactCleanupSubject(st, strings.Repeat("c", 32), j.CurrentAttemptID())
		return err
	}); !errors.Is(err, ErrNotFound) {
		t.Fatal("foreign job borrowed completion", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("foreign subject query wrote state")
	}
	work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
		t.Fatal(err)
	}
	before = artifactDatabaseBytes(t, s)
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, err := artifactLoad(tx)
		if err != nil {
			return err
		}
		_, err = artifactCleanupSubject(st, j.ID, j.CurrentAttemptID())
		return err
	}); !errors.Is(err, ErrNotFound) {
		t.Fatal("retired subject survived", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("retired subject query wrote state")
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupPrepaymentCASAndNoCreditHandoff(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	execution, status, completion := maintenanceFailureCallback(t, s, j, maintenancePhysicalOrigin(true))
	beforeUsage := artifactTestUsage(t, s)
	commit, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
	if err != nil || commit.Cleanup == nil {
		t.Fatal(commit, err)
	}
	work := *commit.Cleanup
	o := work.Origin
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("prepaid completion/intent changed charge", beforeUsage, got)
	}
	if o.Subject.ResultState != "reserved" || o.Subject.ResultID == "" || o.Subject.ResultRef != (ArtifactRef{}) || o.Subject.ResultFile != (ArtifactFile{}) {
		t.Fatalf("reserved is not absent: %+v", o.Subject)
	}
	for _, change := range []func(*WorkspaceCleanupOrigin){
		func(o *WorkspaceCleanupOrigin) { o.Subject.CompletionDigest[0] ^= 1 },
		func(o *WorkspaceCleanupOrigin) { o.Subject.TaskDigest[0] ^= 1 },
		func(o *WorkspaceCleanupOrigin) { o.Subject.WriterReceipt[0] ^= 1 },
		func(o *WorkspaceCleanupOrigin) { o.Subject.Instance = "replacement" },
		func(o *WorkspaceCleanupOrigin) { o.Parent.Device++ },
		func(o *WorkspaceCleanupOrigin) { o.Root = o.Parent },
		func(o *WorkspaceCleanupOrigin) { o.Disposition = WorkspaceNoOutputs },
	} {
		bad := o
		change(&bad)
		before := artifactDatabaseBytes(t, s)
		changed := work
		changed.Origin = bad
		if err := s.FinishWorkspaceCleanup(changed, WorkspaceCleanupReceipt{changed}); !errors.Is(err, ErrArtifactConflict) {
			t.Fatal("stale/bad intent", err)
		}
		if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
			t.Fatal("refused intent finish wrote state")
		}
	}
	before := artifactDatabaseBytes(t, s)
	replay, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
	if err != nil || replay.Cleanup == nil || *replay.Cleanup != work {
		t.Fatalf("completion replay changed intent: %+v %v", replay, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("exact completion replay committed a DB write")
	}
	selected, found, err := s.NextArtifactCleanup("")
	if err != nil || !found || selected.Workspace == nil || selected.Delete != nil || *selected.Workspace != work {
		t.Fatalf("selection: %+v %v %v", selected, found, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("selector wrote state")
	}
	// A source-level Local shutdown handoff has NO Store finish call. Closing
	// live handles cannot release durable workspace/result/candidate charges.
	if got := artifactTestUsage(t, s); got != beforeUsage || got[aWorkspaces] != 1 {
		t.Fatal("selection/handoff credited roots")
	}
	wrong := WorkspaceCleanupReceipt{work}
	wrong.Work.Origin.CandidateID = strings.Repeat("a", 32)
	if err := s.FinishWorkspaceCleanup(work, wrong); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("wrong receipt", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("wrong finish changed state")
	}
	bad := o
	bad.Root.Inode++
	changed := work
	changed.Origin = bad
	if err := s.FinishWorkspaceCleanup(changed, WorkspaceCleanupReceipt{changed}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("changed origin reused intent", err)
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
	selected, found, err = s.NextArtifactCleanup(work.ID)
	if err != nil || !found || selected.Workspace == nil || *selected.Workspace != work {
		t.Fatalf("restart lost exact intent: %+v %v", selected, err)
	}
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
		t.Fatal(err)
	}
	after := artifactTestUsage(t, s)
	if after[aWorkspaces] != 0 || after[aCompletedCount] != beforeUsage[aCompletedCount]-1 || after[aCompletedBytes] != beforeUsage[aCompletedBytes]-artifactMaxSize {
		t.Fatal("independent reserved result not settled", beforeUsage, after)
	}
	if after[aMetadataRecords] >= beforeUsage[aMetadataRecords] || after[aMetadataBytes] >= beforeUsage[aMetadataBytes] {
		t.Fatal("root cleanup reserve not released")
	}
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); !errors.Is(err, ErrNotFound) {
		t.Fatal("finish fabricated duplicate history", err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestWorkspaceCleanupKnownMalformedPrefixAndPublishedResult(t *testing.T) {
	for _, size := range []int64{0, 32 << 20} {
		t.Run(map[int64]string{0: "empty-malformed", 32 << 20: "max-prefix"}[size], func(t *testing.T) {
			o := maintenancePhysicalOrigin(true)
			o.CandidateState = WorkspacePhysicalClosedPrefix
			o.Prefix = &WorkspaceCandidatePrefix{File: ArtifactFile{17, 702}, Size: size, SHA256: strings.Repeat("f", 64)}
			if size == 0 {
				o.Prefix.SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
			}
			s, j := maintenanceFailedOrigin(t, false, o)
			before := artifactTestUsage(t, s)
			work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
			if got := artifactTestUsage(t, s); got[aCompletedBytes] != before[aCompletedBytes] || got[aFiles] != before[aFiles] {
				t.Fatal("intent selection credited external candidate")
			}
			bad := work
			bad.Origin.CandidateFile.Inode++
			if err := s.FinishWorkspaceCleanup(bad, WorkspaceCleanupReceipt{bad}); !errors.Is(err, ErrArtifactConflict) {
				t.Fatal("changed prefix receipt", err)
			}
			if got := artifactTestUsage(t, s); got != before {
				t.Fatal("refused prefix receipt released charges")
			}
			if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
				t.Fatal(err)
			}
		})
	}
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	ref := artifactRegressionComplete(t, s, j, false)
	work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
	o := work.Origin
	if o.Subject.ResultState != "complete" || o.Subject.ResultRef != ref || !artifactFileValid(o.Subject.ResultFile) {
		t.Fatal("completed result missing exact file")
	}
	original := artifactRegressionObject(t, s, ref.ID)
	before := artifactTestUsage(t, s)
	if err := s.FinishWorkspaceCleanup(work, WorkspaceCleanupReceipt{work}); err != nil {
		t.Fatal(err)
	}
	if got := artifactRegressionObject(t, s, ref.ID); got != original {
		t.Fatal("root cleanup changed independent published object")
	}
	if got := artifactTestUsage(t, s); got[aCompletedBytes] != before[aCompletedBytes] || got[aCompletedCount] != before[aCompletedCount] {
		t.Fatal("root cleanup credited published result")
	}
	result, err := s.ArtifactResultFor(testActor(), j.ID, j.CurrentAttemptID())
	if err != nil || result.Reference == nil || *result.Reference != ref {
		t.Fatalf("lost result: %+v %v", result, err)
	}
}

func TestArtifactDeleteClosedTailAndFairSelection(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	ref := artifactTestComplete(t, s, testActor())
	for i := 0; i < 130; i++ {
		lease, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{})
		if err != nil {
			t.Fatal(err)
		}
		c := ArtifactDownloadClosure{Owner: "legacy", LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID}
		c, err = s.MarkArtifactDownloadClosing(c)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.FinishArtifactDownload(c, s.now()); err != nil {
			t.Fatal(err)
		}
	}
	now := s.now().Add(25 * time.Hour)
	s.now = func() time.Time { return now }
	op, err := s.RemoveArtifact(testActor(), ref)
	if err != nil {
		t.Fatal(err)
	}
	// Another valid deletion must remain selectable between tail batches.
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("d", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	u, err = s.ArtifactPartCreated(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}, ArtifactFile{17, 800})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CancelArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision})
	if err != nil {
		t.Fatal(err)
	}
	for batch, wantLeft := range []int{66, 2, 0} {
		selected, found, err := s.NextArtifactCleanup(other)
		if err != nil || !found || selected.Delete == nil || selected.Delete.ID != op {
			t.Fatalf("select tail: %+v %v", selected, err)
		}
		work := *selected.Delete
		before := artifactTestUsage(t, s)
		bad := work
		bad.File.Inode++
		if retired, err := s.FinishArtifactDelete(work, ArtifactDeleteReceipt{bad}); !errors.Is(err, ErrArtifactConflict) || retired {
			t.Fatal("conflicting object receipt", retired, err)
		}
		retired, err := s.FinishArtifactDelete(work, ArtifactDeleteReceipt{work})
		if err != nil || retired != (wantLeft == 0) {
			t.Fatalf("batch%d retired=%v err=%v", batch, retired, err)
		}
		if err := s.db.View(func(tx *bolt.Tx) error {
			st, e := artifactLoad(tx)
			if e != nil {
				return e
			}
			n := 0
			for _, r := range st.Reads {
				if r.Ref.ID == ref.ID {
					n++
				}
			}
			if n != wantLeft {
				t.Fatalf("tail=%d want%d", n, wantLeft)
			}
			_, exists := st.Objects[ref.ID]
			if exists == retired {
				t.Fatal("retirement did not match durable object")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !retired {
			if got := artifactTestUsage(t, s); got[aCompletedBytes] != before[aCompletedBytes] || got[aCompletedCount] != before[aCompletedCount] {
				t.Fatal("partial tail credited object")
			}
		}
		next, found, err := s.NextArtifactCleanup(op)
		if err != nil || !found || next.Delete == nil || next.Delete.ID != other {
			t.Fatalf("other work starved: %+v %v", next, err)
		}
	}
	selected, found, err := s.NextArtifactCleanup("")
	if err != nil || !found || selected.Delete == nil || selected.Delete.ID != other {
		t.Fatal(err)
	}
	if retired, err := s.FinishArtifactDelete(*selected.Delete, ArtifactDeleteReceipt{*selected.Delete}); err != nil || !retired {
		t.Fatal(retired, err)
	}
	if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
		t.Fatal("phantom work", found, err)
	}
}

func TestArtifactCleanupUnknownCreateAndCursorRefusal(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("f", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CancelArtifactUpload(testActor(), ArtifactCursor{u.Reference.ID, u.Revision}); err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	usage := artifactTestUsage(t, s)
	if _, found, err := s.NextArtifactCleanup(""); err != nil || found {
		t.Fatal("unknown inode dispatched", found, err)
	}
	if _, _, err := s.NextArtifactCleanup("../root"); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) || artifactTestUsage(t, s) != usage {
		t.Fatal("unknown inode selection released charge")
	}
}

func TestWorkspaceCleanupSharedBudgetRollback(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, true)
	execution, status, completion := maintenanceFailureCallback(t, s, j, maintenancePhysicalOrigin(true))
	status, originalExit, err := artifactCompletionStatus(completion.Outcome, status)
	if err != nil {
		t.Fatal(err)
	}
	envelope := struct {
		JobID      string
		Execution  backend.Reference
		Status     backend.Status
		Completion WorkspaceCompletion
	}{j.ID, execution, status, completion}
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	change := workspaceCompletionChange{jobID: j.ID, execution: execution, status: status, completion: completion, original: originalExit, digest: sha256.Sum256(raw), operation: strings.Repeat("d", 32), now: s.now()}
	rollback := errors.New("preserve actual maintenance measurement")
	var observed uint64
	apply := func(tx *bolt.Tx, budget uint64) error {
		before := artifactObserveRows(t, tx)
		commit, err := change.apply(tx, s.Owner(), budget, 100)
		if err != nil {
			return err
		}
		if commit.State != WorkspaceCommitMarked || commit.Cleanup == nil || commit.Cleanup.ID != change.operation {
			t.Fatal("atomic completion omitted exact intent")
		}
		// Observe ALL actual stored row values, including artifacts/usage,
		// owner counters and any scheduled scalar writes. This independent
		// raw walker never uses the production forecast for its expectation.
		after := artifactObserveRows(t, tx)
		core, _ := artifactObservedDelta(before, after, false)
		actual, created := artifactObservedDelta(before, after, true)
		if core == 0 || actual == 0 || core+actual >= maxChangeBytes || created != 2 {
			t.Fatalf("invalid atomic completion delta core=%d artifact=%d created=%d", core, actual, created)
		}
		observed = core + actual
		return rollback // Applied real writes remain unpublished in every probe.
	}
	original := artifactDatabaseBytes(t, s)
	if err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, maxChangeBytes) }); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if !bytes.Equal(original, artifactDatabaseBytes(t, s)) {
		t.Fatal("measurement failed to roll back actual mark/counters")
	}
	exact := observed // Independent raw before/after delta, not forecast.changed.
	for _, tt := range []struct {
		name   string
		budget uint64
		want   error
	}{
		{"exact", exact, rollback},
		{"one-byte-short", exact - 1, errArtifactMutationBudget},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := s.db.Update(func(tx *bolt.Tx) error { return apply(tx, tt.budget) })
			if !errors.Is(err, tt.want) {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if !bytes.Equal(original, artifactDatabaseBytes(t, s)) {
				t.Fatal("boundary transaction leaked mark/scalar/counter writes")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkspaceCleanupCanonicalCorruptionRefuses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*artifactDelete)
	}{
		{"owner", func(d *artifactDelete) { d.Owner = "legacz" }},
		{"orphan-attempt", func(d *artifactDelete) { d.ObjectID = strings.Repeat("d", 32) }},
		{"volume", func(d *artifactDelete) { d.Workspace.Root.Device++ }},
		{"aliased-root", func(d *artifactDelete) { d.Workspace.Root = d.Workspace.Parent }},
		{"missing-payload", func(d *artifactDelete) { d.Workspace = nil }},
		{"wrong-kind", func(d *artifactDelete) { d.Kind = "object" }},
		{"false-never-created", func(d *artifactDelete) { d.Workspace.RootState = WorkspaceRootNeverCreated }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, j := maintenanceFailedResult(t, false)
			work := maintenanceWork(t, s, j.ID, j.CurrentAttemptID())
			if err := s.db.Update(func(tx *bolt.Tx) error {
				b := tx.Bucket(artifactsBucket).Bucket([]byte("gc"))
				var d artifactDelete
				if err := artifactDecode(b.Get([]byte(work.ID)), &d); err != nil {
					return err
				}
				tt.change(&d)
				raw, err := json.Marshal(d)
				if err != nil {
					return err
				}
				return b.Put([]byte(work.ID), raw)
			}); err != nil {
				t.Fatal(err)
			}
			before := artifactDatabaseBytes(t, s)
			if err := s.db.View(validateSnapshot); err == nil {
				t.Fatal("corrupt snapshot accepted")
			}
			if _, _, err := s.NextArtifactCleanup(""); err == nil {
				t.Fatal("corrupt intent selected")
			}
			if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("b", 64), 1); err == nil {
				t.Fatal("corrupt metadata mutated")
			}
			if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
				t.Fatal("refusal repaired corrupt state")
			}
			dir := s.Directory()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(dir); err == nil {
				opened.Close()
				t.Fatal("corrupt intent reopened")
			}
		})
	}
}
