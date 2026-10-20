package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// WorkspaceOutcome distinguishes actual task exit from phases that must never
// obtain retry authority merely because they reported a selected exit code.
type WorkspaceOutcome uint8

const (
	WorkspaceTaskExit WorkspaceOutcome = iota + 1
	WorkspaceSetupFailure
	WorkspaceMaterializationFailure
	WorkspaceCollectionFailure
	WorkspaceCanceled
	WorkspaceTimeout
	WorkspaceUncertain
	// WorkspaceExecutionFailure is a known signal or task exec-start failure,
	// after positively joined original owners. It has no task exit code.
	WorkspaceExecutionFailure
)

func (o WorkspaceOutcome) valid() bool {
	return o >= WorkspaceTaskExit && o <= WorkspaceExecutionFailure
}

// WorkspaceWriterClosure is supplied ONLY by the concrete original local
// owner after all its owners join AND independently qualified workspace writer
// quiescence. It is not an RPC/context/disk-presence assertion or native retry
// proof. No constructor here manufactures that physical event.
type WorkspaceWriterClosure struct {
	InstallationID string
	RootID         string
	JobID          string
	Execution      backend.Reference
}

// WorkspaceCandidate is supplied ONLY after the concrete Files seal operation
// returns success. Its fields bind already validated bytes, not permission to
// trust a client-selected file or to infer that an owned writer has stopped.
type WorkspaceCandidate struct {
	Reference ArtifactRef
	File      ArtifactFile
	Manifest  ArtifactManifest
}

// WorkspaceCompletion is a trusted coordinator handoff, never a wire request.
type WorkspaceCompletion struct {
	Outcome   WorkspaceOutcome
	Writer    *WorkspaceWriterClosure
	Candidate *WorkspaceCandidate
	Origin    *WorkspacePhysicalOrigin
}

// WorkspacePhysicalOrigin is the immutable original owner's precommit handoff.
// It contains no final Store subject or publication decision. Actual original
// writer/native/phase/log and descriptor settlement remain caller prerequisites.
type WorkspacePhysicalOrigin struct {
	Parent, Root   ArtifactFile
	RootState      WorkspaceRootState
	CandidateState WorkspacePhysicalCandidateState
	Prefix         *WorkspaceCandidatePrefix
	Producer       OriginalServerLifetime
}

// WorkspaceCandidatePrefix describes a positively closed original candidate,
// including an empty or malformed archive. It is not a published object.
type WorkspaceCandidatePrefix struct {
	ID     string
	File   ArtifactFile
	Size   int64
	SHA256 string
}

// WorkspacePhysicalCandidateState distinguishes actual creation/publication
// frontiers. A missing handle after an error is never NeverAttempted.
type WorkspacePhysicalCandidateState uint8

const (
	WorkspacePhysicalNoOutputs WorkspacePhysicalCandidateState = iota + 1
	WorkspacePhysicalNeverAttempted
	WorkspacePhysicalClosedPrefix
	WorkspacePhysicalSealed
)

// WorkspaceCommitState is an atomic metadata result, not physical cleanup proof.
type WorkspaceCommitState uint8

const (
	WorkspaceCommitHeld WorkspaceCommitState = iota
	WorkspaceCommitMarked
	WorkspaceCommitRemoved
)

// WorkspaceCommit returns the original committed intent on exact replay.
// Cleanup is nonnil only for Marked; Removed never resurrects a collected root.
type WorkspaceCommit struct {
	Job     backend.Job
	State   WorkspaceCommitState
	Cleanup *WorkspaceCleanupWork
}

func artifactCompletionOrigin(st *artifactState, w artifactWorkspace, c WorkspaceCompletion, success bool) (WorkspaceCleanupOrigin, error) {
	o := c.Origin
	if o == nil {
		return WorkspaceCleanupOrigin{}, ErrArtifactConflict
	}
	if _, err := artifactProducer(o.Producer); err != nil {
		return WorkspaceCleanupOrigin{}, err
	}
	out := WorkspaceCleanupOrigin{Parent: o.Parent, Root: o.Root, RootState: o.RootState, Producer: o.Producer}
	if !artifactFileValid(o.Parent) || o.Parent.Device != st.Root.Volume {
		return out, ErrArtifactConflict
	}
	switch o.RootState {
	case WorkspaceRootNeverCreated:
		if o.Root != (ArtifactFile{}) {
			return out, ErrArtifactConflict
		}
	case WorkspaceRootOwned:
		if !artifactFileValid(o.Root) || o.Root.Device != st.Root.Volume || o.Root == o.Parent {
			return out, ErrArtifactConflict
		}
	default:
		return out, ErrArtifactConflict
	}
	if o.CandidateState != WorkspacePhysicalClosedPrefix && o.Prefix != nil {
		return out, ErrArtifactConflict
	}
	switch o.CandidateState {
	case WorkspacePhysicalNoOutputs:
		if w.ResultID != "" || c.Candidate != nil {
			return out, ErrArtifactConflict
		}
		out.Disposition = WorkspaceNoOutputs
	case WorkspacePhysicalNeverAttempted:
		if w.ResultID == "" || c.Candidate != nil || success {
			return out, ErrArtifactConflict
		}
		out.Disposition = WorkspaceCandidateNeverCreated
	case WorkspacePhysicalClosedPrefix:
		p := o.Prefix
		if success || c.Candidate != nil || o.RootState != WorkspaceRootOwned || p == nil || p.ID != w.ResultID || !artifactHex(p.ID, 32) || !artifactFileValid(p.File) || p.File.Device != st.Root.Volume || p.File == o.Parent || p.File == o.Root || p.Size < 0 || p.Size > artifactMaxSize || !artifactHex(p.SHA256, 64) {
			return out, ErrArtifactConflict
		}
		out.Disposition = WorkspaceCandidateClosedUnpublished
		out.CandidateID = p.ID
		out.CandidateFile = p.File
		out.CandidateSize = p.Size
		out.CandidateSHA256 = p.SHA256
	case WorkspacePhysicalSealed:
		if !success || w.ResultID == "" || c.Candidate == nil || o.RootState != WorkspaceRootOwned || c.Candidate.File == o.Parent || c.Candidate.File == o.Root {
			return out, ErrArtifactConflict
		}
		out.Disposition = WorkspaceResultPublishedIndependent
	default:
		return out, ErrArtifactConflict
	}
	return out, nil
}

func artifactCompletionCount(st *artifactState) int {
	n := len(st.Completions)
	for id := range st.Workspaces {
		if _, ok := st.Completions[id]; !ok {
			n++
		}
	}
	return n
}

func artifactSameExecution(a, b backend.Reference) bool {
	if a.ID != b.ID || a.Binding != b.Binding || a.Instance != b.Instance {
		return false
	}
	x, e := backend.TaskBytes(a.Task)
	if e != nil {
		return false
	}
	y, e := backend.TaskBytes(b.Task)
	return e == nil && bytes.Equal(x, y)
}

func artifactCompletionStatus(outcome WorkspaceOutcome, status backend.Status) (backend.Status, *int32, error) {
	if !outcome.valid() {
		return status, nil, errors.New("invalid workspace outcome")
	}
	if status.ExitCode != nil && (*status.ExitCode < 0 || *status.ExitCode > 255) {
		return status, nil, errors.New("invalid workspace exit code")
	}
	if err := validStatus(status); err != nil {
		return status, nil, err
	}
	var original *int32
	switch outcome {
	case WorkspaceTaskExit:
		if status.State != backend.Succeeded && status.State != backend.Failed || status.ExitCode == nil || (status.State == backend.Succeeded) != (*status.ExitCode == 0) {
			return status, nil, errors.New("invalid task exit observation")
		}
		code := *status.ExitCode
		original = &code
	case WorkspaceCollectionFailure:
		if status.State != backend.Failed || status.ExitCode == nil || *status.ExitCode != 0 {
			return status, nil, errors.New("collection failure requires original task exit zero")
		}
		code := *status.ExitCode
		original = &code
	case WorkspaceSetupFailure, WorkspaceMaterializationFailure, WorkspaceTimeout:
		if status.State != backend.Failed {
			return status, nil, errors.New("phase failure requires failed observation")
		}
	case WorkspaceExecutionFailure:
		if status.State != backend.Failed || status.ExitCode != nil || strings.TrimSpace(status.Reason) == "" {
			return status, nil, errors.New("execution failure requires failed state, reason and no exit code")
		}
	case WorkspaceCanceled:
		if status.State != backend.Canceled {
			return status, nil, errors.New("canceled outcome requires canceled observation")
		}
	case WorkspaceUncertain:
		if status.State != backend.Unknown {
			return status, nil, errors.New("uncertain outcome requires unknown observation")
		}
	}
	if outcome != WorkspaceTaskExit {
		status.Termination = backend.UnknownTermination
	}
	return status, original, nil
}

// CompleteWorkspaceAttempt commits a trusted result, terminal/retry decision
// and prepaid root intent in one transaction. Missing writer closure or physical
// origin keeps the job nonterminal and Held with every reservation intact.
// Unavailable server lifetime does not block a positively qualified original
// owner. Only actual task exit retains existing positive stopped retry evidence.
func (s *Store) CompleteWorkspaceAttempt(jobID string, execution backend.Reference, status backend.Status, completion WorkspaceCompletion, now time.Time) (out WorkspaceCommit, err error) {
	execution.Task = backend.CloneTask(execution.Task)
	if status.ExitCode != nil {
		code := *status.ExitCode
		status.ExitCode = &code
	}
	status.Reason = boundedReason(status.Reason)
	if completion.Writer != nil {
		writer := *completion.Writer
		writer.Execution.Task = backend.CloneTask(writer.Execution.Task)
		completion.Writer = &writer
	}
	if completion.Candidate != nil {
		candidate := *completion.Candidate
		completion.Candidate = &candidate
	}
	if completion.Origin != nil {
		origin := *completion.Origin
		if origin.Prefix != nil {
			prefix := *origin.Prefix
			origin.Prefix = &prefix
		}
		completion.Origin = &origin
	}
	if now.IsZero() || execution.Instance == "" || !validInstance(execution.Instance) {
		return out, ErrArtifactConflict
	}
	if status.Instance == "" {
		status.Instance = execution.Instance
	}
	if status.Instance != execution.Instance {
		return out, ErrArtifactConflict
	}
	status, original, e := artifactCompletionStatus(completion.Outcome, status)
	if e != nil {
		return out, e
	}
	if completion.Outcome == WorkspaceUncertain && (completion.Writer != nil || completion.Candidate != nil || completion.Origin != nil) || completion.Writer == nil && completion.Candidate != nil {
		return out, ErrArtifactConflict
	}
	if completion.Origin != nil {
		if _, e := artifactProducer(completion.Origin.Producer); e != nil {
			return out, e
		}
	}
	// The operation insertion candidate is deliberately outside this semantic
	// envelope. A lost reply can never allocate a second root intent.
	envelope := struct {
		JobID      string
		Execution  backend.Reference
		Status     backend.Status
		Completion WorkspaceCompletion
	}{jobID, execution, status, completion}
	raw, e := json.Marshal(envelope)
	if e != nil {
		return out, e
	}
	digest := sha256.Sum256(raw)
	change := workspaceCompletionChange{jobID: jobID, execution: execution, status: status, completion: completion, original: original, digest: digest, now: now}
	found := false
	err = s.db.View(func(tx *bolt.Tx) error {
		job, e := get(tx, jobID)
		if e != nil {
			return e
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		out, found, e = change.replay(st, job)
		return e
	})
	if err != nil || found {
		return
	}
	change.operation, e = newID()
	if e != nil {
		return out, e
	} // No entropy inside Update.
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = change.apply(tx, s.owner, maxChangeBytes, 100)
		return e
	})
	if err != nil {
		out = WorkspaceCommit{}
	} // Rolled-back output is not success.
	return
}

// workspaceCompletionChange holds one normalized semantic callback and its
// independent insertion candidate. apply uses the actual shared write ledger;
// callers must commit its enclosing transaction before acknowledging the output.
type workspaceCompletionChange struct {
	jobID      string
	execution  backend.Reference
	status     backend.Status
	completion WorkspaceCompletion
	original   *int32
	digest     [32]byte
	operation  string
	now        time.Time
}

func (change workspaceCompletionChange) replay(st *artifactState, job backend.Job) (out WorkspaceCommit, found bool, err error) {
	done, found := st.Completions[change.execution.ID]
	if !found {
		return out, false, nil
	}
	taskHash, e := taskDigest(job.Task)
	if e != nil {
		return out, true, e
	}
	accepted := backend.Reference{ID: change.execution.ID, Task: job.Task, Binding: job.TargetBinding, Instance: change.execution.Instance}
	if !artifactSameExecution(change.execution, accepted) || done.JobID != change.jobID || done.TaskDigest != taskHash || done.Digest != change.digest {
		return out, true, ErrArtifactConflict
	}
	out.Job = job
	if done.RootRemoved {
		for _, d := range st.Deletes {
			if d.Kind == "workspace" && d.ObjectID == change.execution.ID {
				return WorkspaceCommit{}, true, ErrArtifactConflict
			}
		}
		out.State = WorkspaceCommitRemoved
		return out, true, nil
	}
	for id, d := range st.Deletes {
		if d.Kind != "workspace" || d.ObjectID != change.execution.ID {
			continue
		}
		work, e := artifactWorkspaceWork(st, id, d)
		if e != nil {
			return WorkspaceCommit{}, true, e
		}
		if work.Origin.Subject.Owner != done.Owner {
			return WorkspaceCommit{}, true, ErrArtifactConflict
		}
		out.State = WorkspaceCommitMarked
		out.Cleanup = &work
		return out, true, nil
	}
	return WorkspaceCommit{}, true, ErrArtifactConflict // Legacy missing origin is never repaired.
}

func (change workspaceCompletionChange) apply(tx *bolt.Tx, owner string, budget uint64, records int) (out WorkspaceCommit, err error) {
	jobID, execution, status, completion := change.jobID, change.execution, change.status, change.completion
	original, digest, operation, now := change.original, change.digest, change.operation, change.now
	job, e := get(tx, jobID)
	if e != nil {
		return out, e
	}
	if job.Task.Workdir == nil {
		return out, ErrArtifactConflict
	}
	taskHash, e := taskDigest(job.Task)
	if e != nil {
		return out, e
	}
	out.Job = job
	_, e = artifactCoreChange(tx, artifactAttemptLedger(tx, jobID, ""), budget, records, func(st *artifactState) error {
		accepted := backend.Reference{ID: execution.ID, Task: job.Task, Binding: job.TargetBinding, Instance: execution.Instance}
		if !artifactSameExecution(execution, accepted) {
			return ErrArtifactConflict
		}
		// Replay is checked before current-attempt/Starting/Running gates.
		// The existing immutable digest binds status, Instance and receipts.
		if replay, found, e := change.replay(st, job); found || e != nil {
			out = replay
			return e
		}
		if job.CurrentAttemptID() != execution.ID || job.Status.State != backend.Starting && job.Status.State != backend.Running {
			return ErrArtifactConflict
		}
		w, ok := st.Workspaces[execution.ID]
		if !ok || w.JobID != jobID || w.Binding != execution.Binding || w.Instance == "" || w.Instance != execution.Instance {
			return ErrArtifactConflict
		}
		if completion.Writer == nil || completion.Origin == nil {
			w.Held = true
			w.Diagnostic = artifactDiagnostic(status.Reason)
			st.Workspaces[execution.ID] = w
			return nil
		}
		writer := completion.Writer
		if writer.InstallationID != owner || writer.RootID != st.Root.ID || writer.JobID != jobID || !artifactSameExecution(writer.Execution, execution) {
			return ErrArtifactConflict
		}
		success := completion.Outcome == WorkspaceTaskExit && status.State == backend.Succeeded
		origin, e := artifactCompletionOrigin(st, w, completion, success)
		if e != nil {
			return e
		}
		writerBytes, e := json.Marshal(writer)
		if e != nil {
			return e
		}
		writerDigest := sha256.Sum256(writerBytes) // identity fence, NOT proof
		if e := artifactWorkspaceQuiescent(st, execution.ID, execution.Instance, writerDigest); e != nil {
			return e
		}
		w = st.Workspaces[execution.ID]
		w.Held = false
		w.Diagnostic = artifactDiagnostic(status.Reason)
		st.Workspaces[execution.ID] = w
		if success && len(job.Task.Outputs) != 0 {
			candidate := completion.Candidate
			if candidate == nil {
				return ErrArtifactConflict
			}
			if e := artifactPublishResult(st, execution.ID, execution.Instance, candidate.Reference, candidate.File, candidate.Manifest, digest, now); e != nil {
				return e
			}
		} else if completion.Candidate != nil {
			return ErrArtifactConflict
		}
		st.Completions[execution.ID] = artifactCompletion{Owner: w.Owner, JobID: jobID, AttemptID: execution.ID, TaskDigest: taskHash, Digest: digest, Outcome: completion.Outcome, OriginalTaskExit: original}
		origin.Subject, e = artifactCleanupSubject(st, jobID, execution.ID)
		if e != nil {
			return e
		}
		if _, exists := st.Deletes[operation]; exists {
			return ErrArtifactConflict
		}
		payload := workspaceDeletePayload(origin)
		d := artifactDelete{Owner: w.Owner, Kind: "workspace", ObjectID: execution.ID, Revision: 1, Workspace: &payload}
		if e := artifactWorkspaceDeleteValid(st, d); e != nil {
			return e
		}
		st.Deletes[operation] = d
		work, e := artifactWorkspaceWork(st, operation, d)
		if e != nil {
			return e
		}
		job, e = observeAttemptCore(tx, jobID, execution.ID, status, now.UTC())
		if e != nil {
			return e
		}
		out = WorkspaceCommit{Job: job, State: WorkspaceCommitMarked, Cleanup: &work}
		return nil
	})
	if e != nil {
		return WorkspaceCommit{}, e
	}
	return out, nil
}
