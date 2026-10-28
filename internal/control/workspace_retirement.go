package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

// workspaceRetirement belongs to the original target completion. Positive
// physical results remain paired with their original maintenance requests until
// the exact durable intent is retired. No path lookup can replace either pair.
type workspaceRetirement struct {
	handoff         *receivedMaintenance
	root            *receivedMaintenance
	candidate       *receivedMaintenance
	last            *receivedMaintenance
	due             time.Time
	held            bool
	finishAttempted bool
	finishError     error
}

type workspaceRetirementStep uint8

const (
	workspaceRetirementRoot workspaceRetirementStep = iota + 1
	workspaceRetirementCandidate
	workspaceRetirementHandoff
)

type workspaceRetirementRequest struct {
	original *workspacePending // identity only; worker never reads mutable pending
	control  *Control
	backend  *local.Backend
	work     store.WorkspaceCleanupWork
	intent   local.WorkspaceRootIntent
	owner    *artifactIO
	step     workspaceRetirementStep
}

type workspaceRetirementResult struct {
	invoked bool
	root    local.WorkspaceRootProgress
}

func workspaceRetirementShape(r maintenanceRequest) bool {
	w := r.workspaceRetirement
	t := r.target
	return r.readerRecovery == nil && r.downloadClose == nil && r.closeOwner == nil && r.artifactDelete == nil && w != nil && w.original != nil && w.control != nil && w.backend != nil && w.owner != nil &&
		(w.step == workspaceRetirementRoot || w.step == workspaceRetirementCandidate || w.step == workspaceRetirementHandoff) &&
		r.sequence != 0 && r.physicalOwner == nil && !r.physicalClose && r.record == nil && r.directory == "" && r.usage == nil && r.reusable == 0 && r.reservedBytes == 0 && r.reservedFiles == 0 &&
		t.Name == "" && t.Backend == nil && t.Kind == "" && t.Binding == "" && t.Advice == nil && t.Labels == nil && t.AcceleratorNames == nil && !t.SupportsResourceLimits && !t.SupportsNodeSelector && !t.SupportsRetry && !t.SupportsPersistentEnvironments && !t.SupportsEnvironmentArtifacts && t.Retention == nil && t.DataPolicy == nil && !t.SupportsRemoteData && !t.SupportsDataMounts && !t.SupportsRemoteDataRetry && t.OutputStorage == 0 && t.OutputReservation == 0
}

func runWorkspaceRetirement(ctx context.Context, r maintenanceRequest) (*workspaceRetirementResult, error) {
	w := r.workspaceRetirement
	if !workspaceRetirementShape(r) {
		if w != nil && w.owner != nil {
			w.owner.finish(nil)
		}
		return nil, ErrInvalid
	}
	result := &workspaceRetirementResult{}
	var err error
	// The worker consumes immutable request values only. Cancellation before the
	// actual call is not a physical receipt. The same common4 owner ends on return.
	if err = ctx.Err(); err == nil {
		result.invoked = true
		switch w.step {
		case workspaceRetirementRoot:
			result.root, err = w.backend.RemoveWorkspaceRoot(ctx, w.intent)
		case workspaceRetirementHandoff:
			err = w.backend.HandoffWorkspaceCleanup(ctx, w.intent)
		case workspaceRetirementCandidate:
			o := w.work.Origin
			if o.Disposition != store.WorkspaceCandidateClosedUnpublished {
				result.invoked = false
				err = ErrInvalid
				break
			}
			// This is a cache-only preinvalidation, before EVERY actual mutation retry.
			// It never applies a pressure gate to retirement or calls Local/Store.
			w.control.physicalChanged()
			err = w.control.artifacts.files.RemoveCandidateNames(artifact.CandidateFile{ID: o.CandidateID, File: artifact.FileID{Device: o.CandidateFile.Device, Inode: o.CandidateFile.Inode}, Size: o.CandidateSize, SHA256: o.CandidateSHA256})
		}
	}
	w.owner.finish(err)
	return result, err
}

func (c *Control) liveWorkspaceState() *loopState {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.run == nil {
		return nil
	}
	return c.run.state
}

func (c *Control) retirementOriginal(s *loopState, p *workspacePending) *targetWorker {
	if s == nil || p == nil || !p.prepared || !p.durable || !p.acked || p.held || p.cleanup == nil || p.commitState != store.WorkspaceCommitMarked || p.retirement.held || p.retirement.handoff != nil || c.workspace == nil || c.artifacts == nil {
		return nil
	}
	if p.job.Target != c.workspace.name || p.job.TargetBinding != c.workspace.binding || p.cleanup.Origin.Producer != c.producer {
		return nil
	}
	for i := range s.workers {
		w := &s.workers[i]
		if w.completion == p && w.pending == nil && c.workspace.matches(w.target) {
			return w
		}
	}
	return nil
}

func positiveRetirementPair(pair *receivedMaintenance, p *workspacePending, step workspaceRetirementStep) bool {
	if pair == nil || pair.request == nil || !workspaceRetirementShape(*pair.request) {
		return false
	}
	request := pair.request.workspaceRetirement
	result := pair.result
	return request.original == p && request.step == step && p.cleanup != nil && request.work == *p.cleanup && result.request.workspaceRetirement == request && result.request.sequence == pair.request.sequence && result.err == nil && result.retirement != nil && result.retirement.invoked && (step != workspaceRetirementRoot || result.retirement.root.Done)
}

func candidateNeedsRemoval(p *workspacePending) bool {
	if p == nil || p.cleanup == nil {
		return false
	}
	o := p.cleanup.Origin
	prefix := p.origin.Prefix
	return o.Disposition == store.WorkspaceCandidateClosedUnpublished && p.origin.CandidateState == local.WorkspacePhysicalClosedPrefix && prefix != nil && prefix.ID == o.CandidateID && physicalStoreFile(prefix.File) == o.CandidateFile && prefix.Size == o.CandidateSize && prefix.SHA256 == o.CandidateSHA256
}

// candidateSettled uses the original accepted frontier, never absence. A
// published result is independent from its staging root and stays published.
func candidateSettled(p *workspacePending) bool {
	if p == nil || p.cleanup == nil {
		return false
	}
	o := p.cleanup.Origin
	switch o.Disposition {
	case store.WorkspaceNoOutputs:
		return p.origin.CandidateState == local.WorkspacePhysicalNoOutputs && p.origin.Prefix == nil && o.Subject.ResultID == "" && o.CandidateID == "" && o.CandidateFile == (store.ArtifactFile{})
	case store.WorkspaceCandidateNeverCreated:
		return p.origin.CandidateState == local.WorkspacePhysicalNeverAttempted && p.origin.Prefix == nil && o.Subject.ResultState == "reserved" && o.CandidateID == "" && o.CandidateFile == (store.ArtifactFile{})
	case store.WorkspaceCandidateClosedUnpublished:
		return candidateNeedsRemoval(p) && positiveRetirementPair(p.retirement.candidate, p, workspaceRetirementCandidate)
	case store.WorkspaceResultPublishedIndependent:
		sealed := p.observation.Candidate
		if p.origin.CandidateState != local.WorkspacePhysicalSealed || sealed == nil {
			return false
		}
		ref, err := artifactStoreRef(sealed.Manifest.Reference)
		return err == nil && o.Subject.ResultState == "complete" && ref == o.Subject.ResultRef && ref.ID == o.Subject.ResultID && physicalStoreFile(sealed.File) == o.Subject.ResultFile
	}
	return false
}

func (c *Control) dispatchWorkspaceRetirement(m *maintenanceState, now time.Time) (bool, error) {
	s := c.liveWorkspaceState()
	if s == nil || c.artifactAdmissionStopped() {
		return false, nil
	}
	// Metadata confirmation must remain reachable after an ambiguous Finish has
	// actually removed gc. Alternate it with fresh gc selection; scan only the
	// existing bounded target set, without a second pending registry.
	retry := func() (bool, error) {
		for n := 0; n < len(s.workers); n++ {
			i := (m.workspaceFinishAfter + n) % len(s.workers)
			p := s.workers[i].completion
			if c.retirementOriginal(s, p) == nil || !p.retirement.finishAttempted || !positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) || !candidateSettled(p) || now.Before(p.retirement.due) {
				continue
			}
			m.workspaceFinishAfter = (i + 1) % len(s.workers)
			m.workspaceFinishLast = true
			return true, c.finishWorkspaceRetirement(s, p, now)
		}
		return false, nil
	}
	if !m.workspaceFinishLast {
		if admitted, err := retry(); admitted || err != nil {
			return admitted, err
		}
	}
	selected, found, err := c.store.NextArtifactCleanup(m.workspaceAfter)
	if err != nil {
		return false, err
	}
	if found {
		if selected.Workspace != nil {
			m.workspaceAfter = selected.Workspace.ID
		} else if selected.Delete != nil {
			m.workspaceAfter = selected.Delete.ID
			return c.dispatchArtifactDelete(m, *selected.Delete)
		}
		if work := selected.Workspace; work != nil {
			for i := range s.workers {
				p := s.workers[i].completion
				if c.retirementOriginal(s, p) == nil || *p.cleanup != *work || now.Before(p.retirement.due) {
					continue
				}
				m.workspaceFinishLast = false
				if positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) && candidateSettled(p) {
					return true, c.finishWorkspaceRetirement(s, p, now)
				}
				step := workspaceRetirementRoot
				if p.retirement.root != nil {
					step = workspaceRetirementCandidate
					if !candidateNeedsRemoval(p) {
						p.retirement.held = true
						return true, nil
					}
				}
				intent, err := workspaceLocalIntent(*work)
				if err != nil {
					p.retirement.held = true
					p.err = err
					return true, nil
				}
				if m.serial == ^uint64(0) {
					return false, ErrUnavailable
				}
				owner, err := c.tryArtifactIO()
				if err != nil {
					p.err = err
					p.retirement.due = now.Add(time.Second)
					return false, nil
				}
				request := maintenanceRequest{sequence: m.serial + 1, workspaceRetirement: &workspaceRetirementRequest{original: p, control: c, backend: c.workspace.backend, work: *work, intent: intent, owner: owner, step: step}}
				m.serial = request.sequence
				m.pending = &request
				m.input <- request
				return true, nil
			}
		}
	}
	return retry()
}

func (c *Control) integrateWorkspaceRetirement(m *maintenanceState, r maintenanceResult) error {
	if m.pending == nil || m.received == nil || m.received.request != m.pending || !workspaceRetirementShape(*m.pending) || !workspaceRetirementShape(r.request) || m.pending.sequence != r.request.sequence || m.pending.workspaceRetirement != r.request.workspaceRetirement || r.readerRecovery != nil || r.downloadClose != nil || r.deletion != nil || r.physical != nil || r.sample != nil {
		return fmt.Errorf("%w: workspace result has no matching original request", ErrUnavailable)
	}
	request := m.pending.workspaceRetirement
	p := request.original
	s := c.liveWorkspaceState()
	intent, err := workspaceLocalIntent(request.work)
	if err != nil || intent != request.intent || request.control != c || c.retirementOriginal(s, p) == nil || *p.cleanup != request.work || request.backend != c.workspace.backend {
		return fmt.Errorf("%w: workspace retirement original changed", ErrUnavailable)
	}
	if r.retirement == nil {
		if r.err == nil {
			return fmt.Errorf("%w: workspace retirement result missing", ErrUnavailable)
		}
	} else if request.step != workspaceRetirementRoot && r.retirement.root != (local.WorkspaceRootProgress{}) || request.step == workspaceRetirementRoot && r.retirement != nil && (r.retirement.root.Visited < 0 || r.retirement.root.Visited > 100) {
		return fmt.Errorf("%w: workspace retirement result shape differs", ErrUnavailable)
	}
	// Transfer the SAME received pair before clearing pending. Failed matching
	// leaves the existing F1 received slot intact, including during cancellation.
	p.retirement.last = m.received
	if r.err == nil && r.retirement != nil && r.retirement.invoked {
		if request.step == workspaceRetirementRoot && r.retirement.root.Done {
			if p.retirement.root != nil {
				return fmt.Errorf("%w: original root already retired", ErrUnavailable)
			}
			p.retirement.root = m.received
		} else if request.step == workspaceRetirementCandidate {
			p.retirement.candidate = m.received
		} else if request.step == workspaceRetirementHandoff {
			p.retirement.handoff = m.received
		}
	}
	p.err = r.err
	p.retirement.due = time.Now().Add(time.Second)
	if errors.Is(r.err, artifact.ErrFileCloseUncertain) {
		p.retirement.held = true
	}
	m.pending = nil
	if request.step != workspaceRetirementHandoff && positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) && candidateSettled(p) {
		return c.finishWorkspaceRetirement(s, p, time.Now())
	}
	return nil
}

func (c *Control) finishWorkspaceRetirement(s *loopState, p *workspacePending, now time.Time) error {
	w := c.retirementOriginal(s, p)
	if w == nil || !positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) || !candidateSettled(p) {
		return fmt.Errorf("%w: original physical retirement missing", ErrUnavailable)
	}
	if p.retirement.finishAttempted {
		// A lost Finish response can remove gc. Only replay of THIS original atomic
		// completion, after retained positive physical results, confirms Removed.
		replay, err := c.store.CompleteWorkspaceAttempt(p.job.ID, reference(p.job), cloneStatus(p.status), p.completion, now.UTC())
		if err != nil {
			p.retirement.finishError = err
			p.retirement.due = now.Add(time.Second)
			return nil
		}
		switch replay.State {
		case store.WorkspaceCommitRemoved:
			if replay.Cleanup != nil {
				p.retirement.held = true
				return nil
			}
			c.releaseRetiredWorkspace(w, p)
			return nil
		case store.WorkspaceCommitMarked:
			if replay.Cleanup == nil || *replay.Cleanup != *p.cleanup {
				p.retirement.held = true
				return nil
			}
		default:
			p.retirement.held = true
			return nil
		}
	}
	p.retirement.finishAttempted = true
	err := c.store.FinishWorkspaceCleanup(*p.cleanup, store.WorkspaceCleanupReceipt{Work: *p.cleanup})
	if err != nil {
		p.retirement.finishError = err
		p.retirement.due = now.Add(time.Second)
		return nil
	}
	c.releaseRetiredWorkspace(w, p)
	return nil
}

func (c *Control) releaseRetiredWorkspace(w *targetWorker, p *workspacePending) {
	// Caller has actual Finish or qualified Removed proof for this exact pointer.
	// No root/candidate operation runs after this point, and no newer completion
	// can be cleared by the old attempt's receipt.
	if w.completion != p {
		return
	}
	c.retention.mu.Lock()
	delete(c.retention.pending, p.job.CurrentAttemptID())
	w.completion = nil
	c.retention.mu.Unlock()
	c.physicalChanged()
}
