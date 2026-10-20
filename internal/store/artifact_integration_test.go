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

// All tests in this file are UNRUN source. Files/closure receipts and clocks are
// MODELED. Public Store transactions are real; native qualification is separate.
func artifactDatabaseBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(s.Directory(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func artifactInputTask(ref ArtifactRef) backend.Task {
	return backend.Task{Argv: []string{"true"}, Workdir: &artifact.Ref{ID: ref.ID, SHA256: ref.SHA256, Size: int64(ref.Size)}}
}

func TestArtifactSubmitReplayAndOldBoundary(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[retained], func(t *testing.T) {
			var s *Store
			var domain *RequestDomain
			if retained {
				p := DefaultRetentionPolicy()
				p.LocalOutputEnabled = false
				var info RetentionInfo
				s, info = enableTest(t, p)
				domain = &info.Domain
			} else {
				s = openTest(t)
			}
			artifactTestInstall(t, s)
			ref := artifactTestComplete(t, s, testActor())
			task := artifactInputTask(ref)
			if _, err := s.Submit(testActor(), "old", task, "remote", "test-binding"); !errors.Is(err, ErrWorkspaceSubmitRequired) {
				t.Fatal(err)
			}
			if retained {
				if _, err := s.SubmitRetained(testActor(), *domain, "old", task, "remote", "test-binding", backend.RemoteOutput, 0); !errors.Is(err, ErrWorkspaceSubmitRequired) {
					t.Fatal(err)
				}
			}
			j, err := s.SubmitWorkspace(testActor(), domain, "same", task, "remote", "test-binding", backend.RemoteOutput, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Existing pin preserves availability beyond owner grant expiry.
			now := s.now().Add(25 * time.Hour)
			s.now = func() time.Time { return now }
			before := artifactTestUsage(t, s)
			again, err := s.SubmitWorkspace(testActor(), domain, "same", task, "", "", backend.LocalOutput, ^uint64(0))
			if err != nil || again.ID != j.ID || again.TargetBinding != j.TargetBinding {
				t.Fatalf("replay rechecked route: %+v %v", again, err)
			}
			if got := artifactTestUsage(t, s); got != before {
				t.Fatal("replay changed artifact reservations")
			}
			if err := s.db.View(validateSnapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestArtifactConsumerPreventsResultExpiry(t *testing.T) {
	s, a, _ := artifactRegressionWorkspace(t, true, true)
	ref := artifactRegressionPublish(t, s, a)
	info, err := s.RetentionInfo(testActor())
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SubmitWorkspace(testActor(), &info.Domain, "downstream", artifactInputTask(ref), "remote", "test-binding", backend.RemoteOutput, 0)
	if err != nil {
		t.Fatal(err)
	}
	before := artifactDatabaseBytes(t, s)
	if err := s.artifactUpdate(func(st *artifactState) error {
		return artifactExpireResult(st, "legacy", a.ID, a.CurrentAttemptID(), s.now())
	}); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("consumer expiry accepted", err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("consumer refusal changed database")
	}
	beforeUsage := artifactTestUsage(t, s)
	selection := PruneSelection{Before: s.now().Add(time.Hour), Mode: ResultOnly, Attempt: 1}
	page, err := s.Prune(testActor(), selection, true, nil, nil, s.now())
	if err != nil || page.Marked != 0 {
		t.Fatalf("consumer expiry: %+v %v", page, err)
	}
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("skipped prune changed artifact counters")
	}
	// This is the L1 regression: accepted B must still begin after A's failed
	// expiry, including after original package grant time has elapsed.
	now := s.now().Add(25 * time.Hour)
	s.now = func() time.Time { return now }
	b, start, err := s.BeginAttempt(b.ID, b.CurrentAttemptID(), 0, now)
	if err != nil || !start {
		t.Fatalf("consumer begin lost grant: %v %v", start, err)
	}
	b, err = s.ObserveAttempt(b.ID, b.CurrentAttemptID(), backend.Status{State: backend.Running, Instance: "i"}, now)
	if err != nil {
		t.Fatal(err)
	}
	artifactRegressionComplete(t, s, b, true)
	// The terminal consumer still owns its input. Only real Whole removal
	// releases that pin; A is skipped while B remains a consumer in this page.
	page, err = s.Prune(testActor(), PruneSelection{Before: now.Add(time.Hour), Mode: Whole}, true, nil, nil, now)
	if err != nil || page.Marked != 1 {
		t.Fatalf("consumer whole: %+v %v", page, err)
	}
	page, err = s.Prune(testActor(), selection, true, nil, nil, now)
	if err != nil || page.Marked != 1 {
		t.Fatalf("expiry after consumer release: %+v %v", page, err)
	}
	result, err := s.ArtifactResultFor(testActor(), a.ID, a.CurrentAttemptID())
	if err != nil || result.State != "expired" || result.Reference != nil {
		t.Fatalf("expired result: %+v %v", result, err)
	}
	if _, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatal("revoked raw reference", err)
	}
}

func TestArtifactBeginCapacityRollsBackAndCancelPrecedesReservation(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	input := artifactTestComplete(t, s, testActor())
	task := artifactInputTask(input)
	task.Outputs = []string{"out"}
	var held backend.Job
	for i := 0; i < 4; i++ {
		j, err := s.SubmitWorkspace(testActor(), nil, string(rune('a'+i)), task, "remote", "test-binding", backend.RemoteOutput, 0)
		if err != nil {
			t.Fatal(err)
		}
		before := artifactDatabaseBytes(t, s)
		got, start, err := s.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, s.now())
		if i < 3 {
			if err != nil || !start {
				t.Fatalf("begin%d: %v %v", i, start, err)
			}
			continue
		}
		var capacity *ArtifactCapacityError
		if !errors.As(err, &capacity) || start {
			t.Fatalf("result capacity: %+v %v %v", got, start, err)
		}
		if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
			t.Fatal("held start wrote attempt/reservation")
		}
		held = j
	}
	if _, err := s.UpdateFor(testActor(), held.ID, func(j *backend.Job) error { j.CancelRequested = true; return nil }); err != nil {
		t.Fatal(err)
	}
	j, start, err := s.BeginAttempt(held.ID, held.CurrentAttemptID(), 0, s.now())
	if err != nil || start || j.Status.State != backend.Canceled {
		t.Fatalf("cancel did not precede full quota: %+v %v %v", j, start, err)
	}
	if err := s.db.View(validateSnapshot); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactOrdinaryTerminalPublicationRefuses(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, false)
	before := artifactDatabaseBytes(t, s)
	if _, err := s.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Succeeded, Instance: "i"}, s.now()); err == nil {
		t.Fatal("ordinary Observe published terminal workspace")
	}
	for _, update := range []func(func(*backend.Job) error) (backend.Job, error){
		func(fn func(*backend.Job) error) (backend.Job, error) { return s.Update(j.ID, fn) },
		func(fn func(*backend.Job) error) (backend.Job, error) { return s.UpdateFor(testActor(), j.ID, fn) },
	} {
		if _, err := update(func(j *backend.Job) error { j.Status.State = backend.Succeeded; return nil }); err == nil {
			t.Fatal("ordinary Update published terminal workspace")
		}
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("refused terminal update wrote database")
	}
}

func TestArtifactCompletionCASAndUncertainHold(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, false, false)
	execution := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: "i"}
	zero := int32(0)
	status := backend.Status{State: backend.Succeeded, Instance: "i", ExitCode: &zero}
	// An exit without joined writer ownership is not terminal authority.
	got, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, WorkspaceCompletion{Outcome: WorkspaceTaskExit}, s.now())
	if err != nil || got.Job.Status.State.Terminal() {
		t.Fatalf("unproved exit: %+v %v", got, err)
	}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	completion := WorkspaceCompletion{Outcome: WorkspaceTaskExit, Writer: &WorkspaceWriterClosure{InstallationID: s.Owner(), RootID: installation.ID, JobID: j.ID, Execution: execution}, Origin: maintenancePhysicalOrigin(false)}
	wrong := completion
	w := *completion.Writer
	w.Execution.Instance = "replacement"
	wrong.Writer = &w
	before := artifactDatabaseBytes(t, s)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, wrong, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("conflicting writer mutated database")
	}
	got, err = s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now())
	if err != nil || got.Job.Status.State != backend.Succeeded {
		t.Fatalf("complete: %+v %v", got, err)
	}
	beforeUsage := artifactTestUsage(t, s)
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now().Add(time.Hour)); err != nil {
		t.Fatal("exact callback duplicate", err)
	}
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("duplicate callback changed charges")
	}
	completion.Outcome = WorkspaceSetupFailure
	status.State = backend.Failed
	if _, err := s.CompleteWorkspaceAttempt(j.ID, execution, status, completion, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("conflicting outcome", err)
	}
}

func TestArtifactCompletionClassification(t *testing.T) {
	zero, one := int32(0), int32(1)
	for _, tt := range []struct {
		name    string
		outcome WorkspaceOutcome
		status  backend.Status
		bad     bool
		retry   bool
	}{
		{"task", WorkspaceTaskExit, backend.Status{State: backend.Failed, ExitCode: &one, Termination: backend.StoppedWithExitCode}, false, true},
		{"collection", WorkspaceCollectionFailure, backend.Status{State: backend.Failed, ExitCode: &zero, Termination: backend.StoppedWithExitCode}, false, false},
		{"setup", WorkspaceSetupFailure, backend.Status{State: backend.Failed, ExitCode: &one, Termination: backend.StoppedWithExitCode}, false, false},
		{"zero", 0, backend.Status{State: backend.Failed}, true, false},
		{"collection-nonzero", WorkspaceCollectionFailure, backend.Status{State: backend.Failed, ExitCode: &one}, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := artifactCompletionStatus(tt.outcome, tt.status)
			if (err != nil) != tt.bad {
				t.Fatal(err)
			}
			if err == nil && (got.Termination == backend.StoppedWithExitCode) != tt.retry {
				t.Fatal("non-task outcome acquired retry evidence")
			}
		})
	}
}

func TestArtifactCreateAbandonmentAndDownloadCloseCAS(t *testing.T) {
	s := openTest(t)
	artifactTestInstall(t, s)
	ref := artifactTestComplete(t, s, testActor())
	u, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1)
	if err != nil {
		t.Fatal(err)
	}
	installation, err := s.ArtifactInstallation()
	if err != nil {
		t.Fatal(err)
	}
	c := ArtifactCreateAbandonment{Owner: "legacy", Cursor: ArtifactCursor{u.Reference.ID, u.Revision}, Ref: u.Reference, File: ArtifactFile{17, 71}, RootID: installation.ID}
	// Models fresh authorization denial AFTER original Create returned nil and
	// all of its actual owners settled. No Actor is reused to publish writable.
	op, err := s.AbandonArtifactCreate(c)
	if err != nil || op == "" {
		t.Fatalf("abandon: %q %v", op, err)
	}
	beforeUsage := artifactTestUsage(t, s)
	again, err := s.AbandonArtifactCreate(c)
	if err != nil || again != op {
		t.Fatalf("duplicate: %q %v", again, err)
	}
	bad := c
	bad.File.Inode++
	if _, err := s.AbandonArtifactCreate(bad); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("cleanup duplicate/conflict changed charges")
	}
	if got, _, err := s.GetArtifactUploadFile(testActor(), c.Cursor); err == nil || got.State == "writable" {
		t.Fatal("cleanup receipt created upload authority")
	}
	lease, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{})
	if err != nil {
		t.Fatal(err)
	}
	closed := ArtifactDownloadClosure{Owner: "legacy", LeaseID: lease.ID, Revision: lease.Revision, Ref: lease.Reference, File: lease.File, RootID: lease.RootID}
	closed, err = s.MarkArtifactDownloadClosing(closed)
	if err != nil {
		t.Fatal(err)
	}
	beforeUsage = artifactTestUsage(t, s)
	badClose := closed
	badClose.File.Inode++
	if err := s.FinishArtifactDownload(badClose, s.now()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("conflicting close credited reader")
	}
	if err := s.FinishArtifactDownload(closed, s.now()); err != nil {
		t.Fatal(err)
	}
	beforeUsage = artifactTestUsage(t, s)
	if err := s.FinishArtifactDownload(closed, s.now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if got := artifactTestUsage(t, s); got != beforeUsage {
		t.Fatal("duplicate close changed charges")
	}
}

func TestArtifactPruneDryRunWholeAndCollectedWitness(t *testing.T) {
	s, j, input := artifactRegressionWorkspace(t, true, true)
	ref := artifactRegressionPublish(t, s, j)
	now := s.now().Add(25 * time.Hour)
	s.now = func() time.Time { return now }
	sel := PruneSelection{Before: now.Add(time.Hour), Mode: Whole}
	before := artifactDatabaseBytes(t, s)
	usage := artifactTestUsage(t, s)
	dry, err := s.Prune(testActor(), sel, false, nil, nil, now)
	if err != nil || dry.Selected != 1 || dry.Marked != 0 {
		t.Fatalf("dry: %+v %v", dry, err)
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) || artifactTestUsage(t, s) != usage {
		t.Fatal("dry-run wrote work/proof/counter state")
	}
	page, err := s.Prune(testActor(), sel, true, nil, nil, now)
	if err != nil || page.Marked != 1 {
		t.Fatalf("whole: %+v %v", page, err)
	}
	if _, err := s.Get(j.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("whole retained job", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		w, ok := st.Expired[j.CurrentAttemptID()]
		if !ok || !w.Collected || w.ObjectID != ref.ID || len(st.Completions) != 0 || st.pinned(input.ID) {
			t.Fatal("whole lost exact collected witness or retained pin")
		}
		return validateSnapshot(tx)
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
	deadline := now.Add(time.Hour)
	op := strings.Repeat("d", 32)
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactMarkObjectDelete(st, ref.ID, op, deadline) }); err != nil {
		t.Fatal(err)
	}
	if err := s.artifactUpdate(func(st *artifactState) error { return artifactFinishDelete(st, op, st.Deletes[op]) }); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		if _, ok := st.Expired[j.CurrentAttemptID()]; ok {
			t.Fatal("verified object GC kept collected witness")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactManualResultSelectionAfterMetadataTransfer(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, true, true)
	ref := artifactRegressionComplete(t, s, j, false)
	artifactRegressionTransferManual(t, s, j)
	sel := PruneSelection{Mode: ResultOnly, Attempt: 1, ObligationID: j.ID}
	page, err := s.Prune(testActor(), sel, true, nil, nil, s.now())
	if err != nil || page.Marked != 0 {
		t.Fatalf("unsettled workspace result: %+v %v", page, err)
	}
	artifactRegressionRemove(t, s, j.CurrentAttemptID(), s.now())
	before := artifactDatabaseBytes(t, s)
	page, err = s.Prune(testActor(), sel, false, nil, nil, s.now())
	if err != nil || page.Selected != 1 || !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatalf("manual dry: %+v %v", page, err)
	}
	page, err = s.Prune(testActor(), sel, true, nil, nil, s.now())
	if err != nil || page.Marked != 1 {
		t.Fatalf("manual result: %+v %v", page, err)
	}
	if _, err := s.BeginArtifactDownload(testActor(), ref, OriginalServerLifetime{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		if _, ok := st.ManualInputs[j.ID]; !ok {
			t.Fatal("result-only released manual input")
		}
		var h cleanupHeader
		if e := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(j.ID), &h); e != nil {
			return e
		}
		if !h.NeedsManual || h.Task != nil {
			t.Fatal("result-only changed execution obligation")
		}
		return validateSnapshot(tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactCollectedResultObjectSubjectRefuses(t *testing.T) {
	s, j, _ := artifactRegressionWorkspace(t, true, true)
	ref := artifactRegressionPublish(t, s, j)
	now := s.now().Add(25 * time.Hour)
	page, err := s.Prune(testActor(), PruneSelection{Before: now.Add(time.Hour), Mode: Whole}, true, nil, nil, now)
	if err != nil || page.Marked != 1 {
		t.Fatalf("whole: %+v %v", page, err)
	}
	before := artifactDatabaseBytes(t, s)
	if err := s.artifactUpdate(func(st *artifactState) error {
		o := st.Objects[ref.ID]
		o.JobID = strings.Repeat("x", 33)
		st.Objects[ref.ID] = o
		return nil
	}); err == nil {
		t.Fatal("oversized/invalid result subject committed")
	}
	if !bytes.Equal(before, artifactDatabaseBytes(t, s)) {
		t.Fatal("refused object mutation escaped rollback")
	}
	// Same-width canonical tamper must not borrow arbitrary absent-job
	// authority merely because the witness still has a valid owner/attempt.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(artifactsBucket).Bucket([]byte("objects"))
		var o artifactObject
		if e := artifactDecode(b.Get([]byte(ref.ID)), &o); e != nil {
			return e
		}
		o.JobID = strings.Repeat("c", 32)
		bad, e := json.Marshal(o)
		if e != nil {
			return e
		}
		return b.Put([]byte(ref.ID), bad)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.View(validateSnapshot); err == nil {
		t.Fatal("collected object rebound to unrelated absent job")
	}
	if _, err := s.BeginArtifactUpload(testActor(), strings.Repeat("a", 64), 1); err == nil {
		t.Fatal("mutation accepted corrupted collected subject")
	}
	dir := s.Directory()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err = os.ReadFile(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir); err == nil {
		reopened.Close()
		t.Fatal("corrupt collected subject opened")
	}
	after, err := os.ReadFile(filepath.Join(dir, "jobs.db"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused Open changed bytes: %v", err)
	}
}

func TestArtifactWorkspaceReservationSnapshot(t *testing.T) {
	s, j, input := artifactRegressionWorkspace(t, false, true)
	got, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID())
	if err != nil || got.JobID != j.ID || got.AttemptID != j.CurrentAttemptID() || got.InputRef != input || got.Instance != "i" || got.Binding != j.TargetBinding || got.ResultID == "" || got.ResultID == j.CurrentAttemptID() || got.InputFile != (ArtifactFile{17, 42}) {
		t.Fatalf("reservation: %+v %v", got, err)
	}
	if _, err := s.GetWorkspaceReservation(j.ID, strings.Repeat("f", 32)); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("stale reservation", err)
	}
	artifactRegressionComplete(t, s, j, false)
	if _, err := s.GetWorkspaceReservation(j.ID, j.CurrentAttemptID()); !errors.Is(err, ErrArtifactConflict) {
		t.Fatal("terminal reservation granted materialization", err)
	}
}
