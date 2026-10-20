package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// workspaceStart is one already-reserved attempt, with the original unused
// materialization owner. Bind transfers that owner only on actual nil success.
type workspaceStart struct {
	reservation store.ArtifactWorkspaceReservation
	owner       *artifactIO
}

// workspacePending survives removal from Active after atomic completion. Each
// target has at most one original candidate; retries repeat metadata/ack only.
type workspacePending struct {
	job         backend.Job
	completion  store.WorkspaceCompletion
	status      backend.Status
	observation local.WorkspaceObservation // retained Writer has no second Task
	origin      local.WorkspacePhysicalOrigin
	prepared    bool
	held        bool
	commitState store.WorkspaceCommitState
	cleanup     *store.WorkspaceCleanupWork // exact actual atomic Store result only
	durable     bool
	acked       bool
	retirement  workspaceRetirement
	after       time.Time
	err         error
}

func bindWorkspaceStart(target *workspaceTarget, op operation) error {
	if target == nil || op.workspace == nil || op.workspace.owner == nil {
		return ErrUnsupported
	}
	r := op.workspace.reservation
	j := op.job
	if target.name != j.Target || target.binding != j.TargetBinding || r.JobID != j.ID || r.AttemptID != j.CurrentAttemptID() || r.Binding != j.TargetBinding || r.Instance != "" || j.Task.Workdir == nil {
		return backend.ErrIdentityMismatch
	}
	input, err := artifactPortableRef(r.InputRef)
	if err != nil {
		return err
	}
	if input != *j.Task.Workdir {
		return backend.ErrIdentityMismatch
	}
	return target.backend.BindWorkspace(local.WorkspaceBinding{
		Owner: r.Owner, JobID: r.JobID, AttemptID: r.AttemptID, Binding: r.Binding, RootID: r.RootID,
		Task: cloneTask(j.Task), Input: input, InputFile: artifact.FileID{Device: r.InputFile.Device, Inode: r.InputFile.Inode}, ResultID: r.ResultID,
	}, func() { op.workspace.owner.finish(nil) })
}

func sameWorkspaceReference(a, b backend.Reference) bool {
	if a.ID != b.ID || a.Binding != b.Binding || a.Instance != b.Instance {
		return false
	}
	x, e1 := backend.TaskBytes(a.Task)
	y, e2 := backend.TaskBytes(b.Task)
	return e1 == nil && e2 == nil && bytes.Equal(x, y)
}

// workspaceCompletion copies only the actual configured Local observation.
// Identity checks are fences, not manufactured writer or filesystem proof.
func (c *Control) workspaceCompletion(j backend.Job, o local.WorkspaceObservation, origin local.WorkspacePhysicalOrigin) (store.WorkspaceCompletion, error) {
	var result store.WorkspaceCompletion
	ref := reference(j)
	if !o.Ready || o.Held || o.Writer == nil || ref.Instance == "" || o.Status.Instance != ref.Instance || c.artifacts == nil {
		return result, ErrUnavailable
	}
	writer := o.Writer
	if writer.InstallationID != c.store.Owner() || writer.RootID != c.artifacts.root.ID || writer.JobID != j.ID || !sameWorkspaceReference(writer.Execution, ref) {
		return result, backend.ErrIdentityMismatch
	}
	switch o.Outcome {
	case local.WorkspaceTaskExit:
		result.Outcome = store.WorkspaceTaskExit
	case local.WorkspaceSetupFailure:
		result.Outcome = store.WorkspaceSetupFailure
	case local.WorkspaceMaterializationFailure:
		result.Outcome = store.WorkspaceMaterializationFailure
	case local.WorkspaceCollectionFailure:
		result.Outcome = store.WorkspaceCollectionFailure
	case local.WorkspaceCanceled:
		result.Outcome = store.WorkspaceCanceled
	case local.WorkspaceTimeout:
		result.Outcome = store.WorkspaceTimeout
	case local.WorkspaceExecutionFailure:
		result.Outcome = store.WorkspaceExecutionFailure
	default:
		return result, ErrUnavailable
	}
	physical, err := workspaceStoreOrigin(origin)
	if err != nil {
		return result, err
	}
	if physical.Parent != physicalStoreFile(c.artifacts.root.File) || physical.Producer != c.producer {
		return result, backend.ErrIdentityMismatch
	}
	result.Origin = &physical
	// The pending accepted job already owns this immutable Task. Do not retain
	// a second task in observation/origin or manufacture a new semantic envelope.
	ref.Task = j.Task
	result.Writer = &store.WorkspaceWriterClosure{InstallationID: writer.InstallationID, RootID: writer.RootID, JobID: writer.JobID, Execution: ref}
	if o.Candidate != nil {
		if o.Outcome != local.WorkspaceTaskExit || o.Status.State != backend.Succeeded || len(j.Task.Outputs) == 0 || o.Candidate.File.Device == 0 || o.Candidate.File.Inode == 0 {
			return store.WorkspaceCompletion{}, backend.ErrIdentityMismatch
		}
		sealed := o.Candidate
		converted, err := artifactStoreRef(sealed.Manifest.Reference)
		if err != nil {
			return store.WorkspaceCompletion{}, err
		}
		result.Candidate = &store.WorkspaceCandidate{Reference: converted, File: store.ArtifactFile{Device: sealed.File.Device, Inode: sealed.File.Inode}, Manifest: store.ArtifactManifest{Reference: converted, ExpandedBytes: sealed.Manifest.ExpandedBytes, RegularFiles: sealed.Manifest.RegularFiles, Directories: sealed.Manifest.Directories}}
	} else if o.Outcome == local.WorkspaceTaskExit && o.Status.State == backend.Succeeded && len(j.Task.Outputs) != 0 {
		return store.WorkspaceCompletion{}, ErrUnavailable
	}
	return result, nil
}

func (c *Control) holdWorkspace(j backend.Job, reason string) error {
	if j.Status.Reason == reason {
		return nil
	}
	_, err := c.store.Update(j.ID, func(current *backend.Job) error {
		if current.CurrentAttemptID() != j.CurrentAttemptID() || current.Status.State.Terminal() {
			return nil
		}
		current.Status.Reason = reason
		return nil
	})
	return err
}

func (c *Control) clearWorkspacePending(w *targetWorker, id string) {
	c.retention.mu.Lock()
	defer c.retention.mu.Unlock()
	w.pending = nil
	delete(c.retention.pending, id)
}

func (c *Control) integrateWorkspace(s *loopState, w *targetWorker, r operationResult) error {
	if r.operation.kind == workspacePrepareOperation {
		p := w.completion
		if p == nil || p.prepared || p.durable || !sameExecution(p.job, r.operation.job) {
			return fmt.Errorf("%w: original workspace preparation differs", ErrUnavailable)
		}
		if r.err != nil {
			p.err = r.err
			p.after = time.Now().Add(100 * time.Millisecond)
			p.held = errors.Is(r.err, artifact.ErrFileCloseUncertain)
			w.pending = nil
			return nil
		}
		if r.prepared == nil {
			return fmt.Errorf("%w: original workspace preparation missing", ErrUnavailable)
		}
		completion, err := c.workspaceCompletion(p.job, r.prepared.observation, r.prepared.origin)
		if err != nil {
			return fmt.Errorf("integrate original workspace preparation: %w", err)
		}
		// Keep the actual returned original observation/origin before any Store call.
		// Pointer copies isolate result retention; Task stays in the accepted job only.
		p.observation = retainWorkspaceObservation(r.prepared.observation)
		p.origin = retainWorkspaceOrigin(r.prepared.origin)
		p.completion = completion
		p.status = cloneStatus(r.prepared.observation.Status)
		p.prepared = true
		p.err = nil
		w.pending = nil
		return nil
	}
	if r.operation.kind == workspaceCommitOperation {
		p := w.completion
		if p == nil || !p.durable || p.commitState != store.WorkspaceCommitMarked || p.cleanup == nil || !sameExecution(p.job, r.operation.job) || r.operation.intent == nil {
			return fmt.Errorf("%w: original workspace acknowledgement differs", ErrUnavailable)
		}
		expected, err := workspaceLocalIntent(*p.cleanup)
		if err != nil {
			return err
		}
		if *r.operation.intent != expected {
			return fmt.Errorf("%w: acknowledgement intent differs", ErrUnavailable)
		}
		if r.err != nil {
			p.err = r.err
			p.after = time.Now().Add(100 * time.Millisecond)
			w.pending = nil
			return nil
		}
		p.err = nil
		p.acked = true
		w.pending = nil
		// Actual acknowledgement binds the SAME marked intent. It is not positive
		// root retirement or authority to clear the durable/current pending frame.
		delete(s.due, p.job.CurrentAttemptID())
		return nil
	}
	c.clearWorkspacePending(w, r.operation.job.CurrentAttemptID())
	j, err := c.store.Get(r.operation.job.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if j.Status.State.Terminal() || j.Status.State != r.operation.job.Status.State || !sameExecution(j, r.operation.job) {
		return nil
	}
	if _, reason := c.boundTarget(j); reason != "" {
		return nil
	}
	if r.operation.kind == startOperation {
		if r.notStarted {
			return c.holdWorkspace(j, "workspace binding unavailable; execution held")
		}
		return nil // Neither an error nor a lost Start reply permits another Start.
	}
	if s.due == nil {
		s.due = make(map[string]time.Time)
	}
	s.due[j.CurrentAttemptID()] = time.Now().Add(100 * time.Millisecond)
	if r.err != nil {
		return c.holdWorkspace(j, "workspace observation unavailable; execution held")
	}
	if currentStatus(j).Instance == "" {
		// This status is the real Local Observe result, never synthesized from a
		// fast terminal outcome. Store pins the same Instance in job and workspace.
		status := r.status
		if status.Instance == "" || status.State != backend.Starting && status.State != backend.Running {
			return c.holdWorkspace(j, "workspace original instance unavailable; execution held")
		}
		_, err = c.store.ObserveAttempt(j.ID, j.CurrentAttemptID(), cloneStatus(status), time.Now().UTC())
		return err
	}
	if r.workspace == nil {
		return c.holdWorkspace(j, "workspace owner observation unavailable; execution held")
	}
	if !r.workspace.Ready || r.workspace.Held || r.workspace.Writer == nil {
		if !r.workspace.Ready && !r.workspace.Held {
			return nil
		}
		return c.holdWorkspace(j, "workspace writer ownership remains held")
	}
	// Observation readiness alone is not a physical origin. Prepare must run on
	// this same original target worker under an actual common4 I/O owner first.
	w.completion = &workspacePending{job: cloneJob(j), observation: retainWorkspaceObservation(*r.workspace), status: cloneStatus(r.workspace.Status)}
	c.retention.mu.Lock()
	c.retention.pending[j.CurrentAttemptID()] = struct{}{}
	c.retention.mu.Unlock()
	return nil
}

func (c *Control) advanceWorkspaces(ctx context.Context, s *loopState) error {
	now := time.Now()
	for i := range s.workers {
		if ctx.Err() != nil {
			return nil
		}
		w := &s.workers[i]
		p := w.completion
		if p == nil || p.acked || p.held || w.pending != nil || now.Before(p.after) {
			continue
		}
		if !p.prepared {
			owner, err := c.tryArtifactIO()
			if err != nil {
				p.err = err
				p.after = now.Add(100 * time.Millisecond)
				continue
			}
			op := operation{kind: workspacePrepareOperation, job: cloneJob(p.job), prepareOwner: owner}
			w.pending = &op
			w.input <- op
			continue
		}
		if !p.durable {
			// The original immutable semantic handoff is retried verbatim. No Start,
			// setup, Collect, origin synthesis or second postcommit mark occurs here.
			committed, err := c.store.CompleteWorkspaceAttempt(p.job.ID, reference(p.job), cloneStatus(p.status), p.completion, now.UTC())
			if err != nil {
				p.err = err
				p.after = now.Add(100 * time.Millisecond)
				continue
			}
			if err := c.acceptWorkspaceCommit(p, committed); err != nil {
				p.err = err
				p.after = now.Add(100 * time.Millisecond)
				continue
			}
			if !p.durable {
				continue
			}
			c.physicalChanged()
		}
		intent, err := workspaceLocalIntent(*p.cleanup)
		if err != nil {
			p.err = err
			p.held = true
			continue
		}
		op := operation{kind: workspaceCommitOperation, job: cloneJob(p.job), intent: &intent}
		w.pending = &op
		w.input <- op
	}
	return nil
}

// acceptWorkspaceCommit distinguishes actual metadata state from physical
// retirement. Removed cannot settle a still-owned original Local frame or be
// acknowledged using an empty intent. It stays charged/Held for reconciliation.
func (c *Control) acceptWorkspaceCommit(p *workspacePending, committed store.WorkspaceCommit) error {
	p.commitState = committed.State
	switch committed.State {
	case store.WorkspaceCommitHeld:
		if committed.Cleanup != nil {
			return fmt.Errorf("%w: held workspace returned cleanup", ErrUnavailable)
		}
		return fmt.Errorf("%w: workspace completion remains held", ErrUnavailable)
	case store.WorkspaceCommitRemoved:
		p.held = true
		return fmt.Errorf("%w: workspace intent removed before original owner retirement", ErrUnavailable)
	case store.WorkspaceCommitMarked:
		if committed.Cleanup == nil {
			return fmt.Errorf("%w: marked workspace has no original intent", ErrUnavailable)
		}
		// Store's exact replay may expose a later current attempt. The returned
		// intent must still bind THIS original accepted attempt and immutable origin.
		intent, err := workspaceLocalIntent(*committed.Cleanup)
		if err != nil {
			return err
		}
		if intent.Origin.Subject.JobID != p.job.ID || intent.Origin.Subject.AttemptID != p.job.CurrentAttemptID() || intent.Origin.Subject.Binding != p.job.TargetBinding || intent.Origin.Subject.Instance != currentStatus(p.job).Instance {
			return backend.ErrIdentityMismatch
		}
		cleanup := *committed.Cleanup
		p.cleanup = &cleanup
		p.durable = true
		p.err = nil
		return nil
	default:
		p.held = true
		return fmt.Errorf("%w: unknown workspace commit state", ErrUnavailable)
	}
}
