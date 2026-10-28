package control

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// ownedLocalJob reports whether j runs under this server's local backend
// and stops with it. Durable local executions detach instead.
func (c *Control) ownedLocalJob(j backend.Job) bool {
	return c.configuredLocalJob(j) && !c.localBackend.Detaches(j.CurrentAttemptID())
}

// configuredLocalJob checks the actual configured local route, independently of
// whether a started execution remains owned by this API server or detaches.
func (c *Control) configuredLocalJob(j backend.Job) bool {
	if c.localBackend == nil {
		return false
	}
	for _, t := range c.targets {
		if t.Backend == c.localBackend && t.Name == j.Target && t.Binding == j.TargetBinding {
			return true
		}
	}
	return false
}

// This is only an original returned Start error at the ratified canceled-before-
// parent seam. It grants no no-call authority: actual Local.CheckShutdown must
// independently witness the original callback-return frontier before prepared.
func shutdownNoCall(w *targetWorker, j backend.Job) bool {
	pair := w.shutdownStart
	if pair == nil || pair.request == nil {
		return false
	}
	r := pair.result
	return pair.request.kind == startOperation && r.operation.kind == startOperation && pair.request.workspace == r.operation.workspace && r.operation.workspace != nil && !r.notStarted && currentStatus(j).Instance == "" && sameExecution(pair.request.job, j) && sameExecution(r.operation.job, j) && (errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded))
}

func (c *Control) scheduleShutdownLocal(ctx context.Context, s *loopState) error {
	if c.localBackend == nil {
		return nil
	}
	jobs, err := c.store.Active()
	if err != nil {
		return err
	}
	now := time.Now()
	for i := range s.workers {
		w := &s.workers[i]
		if w.target.Backend != c.localBackend || w.pending != nil || w.received != nil || w.completion != nil {
			continue
		}
		first := 0
		for first < len(jobs) && jobs[first].Sequence <= w.after {
			first++
		}
		for n := 0; n < len(jobs); n++ {
			j := jobs[(first+n)%len(jobs)]
			if !c.ownedLocalJob(j) || j.Status.State != backend.Starting && j.Status.State != backend.Running || shutdownNoCall(w, j) || now.Before(s.due[j.CurrentAttemptID()]) {
				continue
			}
			// Only the concrete OWNED Local route receives stop intent. Remote jobs
			// keep their workload; queued/retrying acceptance remains durable/unstarted.
			if !j.CancelRequested {
				j, err = c.store.Update(j.ID, func(current *backend.Job) error {
					if !sameExecution(*current, j) || !c.ownedLocalJob(*current) {
						return backend.ErrIdentityMismatch
					}
					if current.Status.State == backend.Starting || current.Status.State == backend.Running {
						current.CancelRequested = true
					}
					return nil
				})
				if err != nil {
					return err
				}
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			op := operation{kind: observeOperation, job: cloneJob(j)}
			c.retention.mu.Lock()
			w.pending = &op
			c.retention.pending[j.CurrentAttemptID()] = struct{}{}
			c.retention.mu.Unlock()
			w.after = j.Sequence
			s.due[j.CurrentAttemptID()] = now.Add(100 * time.Millisecond)
			w.input <- op
			break
		}
	}
	return nil
}

func (c *Control) tryShutdownPrepared(s *loopState, m *maintenanceState) error {
	if !s.preparing || s.prepared {
		return nil
	}
	lifecycle := c.shutdown
	lifecycle.mu.Lock()
	externalReady := lifecycle.externalClosed && lifecycle.external == 0 && lifecycle.failure == nil
	lifecycle.mu.Unlock()
	if !externalReady {
		return nil
	}
	if s.unmatched != nil || m.pending != nil || m.received != nil {
		return nil
	}
	dataReady, err := c.dataOperationsPrepared(s)
	if err != nil {
		return err
	}
	if !dataReady {
		return nil
	}
	transfersReady, err := c.remoteTransfersPrepared()
	if err != nil {
		return err
	}
	if !transfersReady {
		return nil
	}
	for i := range s.workers {
		w := &s.workers[i]
		if w.pending != nil || w.received != nil {
			return nil
		}
		if p := w.completion; p != nil {
			if p.held || p.retirement.held {
				return fmt.Errorf("original workspace remains held: %w", errors.Join(ErrUnavailable, p.err, p.retirement.finishError))
			}
			if !p.prepared || !p.durable || !p.acked || p.cleanup == nil {
				return nil
			}
			// Root.Done already joined Local and must NEVER reenter its postmark path.
			// Otherwise actual handle-only Handoff must have returned on this worker.
			if !positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) && !positiveRetirementPair(p.retirement.handoff, p, workspaceRetirementHandoff) {
				return nil
			}
		}
	}
	if c.localBackend != nil {
		jobs, err := c.store.Active()
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if !c.ownedLocalJob(j) || j.Status.State != backend.Starting && j.Status.State != backend.Running {
				continue
			}
			allowed := false
			for i := range s.workers {
				if shutdownNoCall(&s.workers[i], j) {
					allowed = true
					break
				}
			}
			if !allowed {
				return nil
			}
		}
		// PENDING_LOCAL_QUERY_ACCEPTANCE: this exact ratified pure Local API is a
		// required accepted source dependency, not provided/faked by Control.
		if err := c.localBackend.CheckShutdown(); err != nil {
			return fmt.Errorf("%w: original Local shutdown query: %w", ErrUnavailable, err)
		}
	}
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if !lifecycle.externalClosed || lifecycle.external != 0 || lifecycle.failure != nil {
		return nil
	}
	s.prepared = true
	lifecycle.preparedClosed = true
	close(lifecycle.prepared)
	lifecycle.notifyLocked()
	return nil
}

func (c *Control) maintainShutdownPreparation(s *loopState, m *maintenanceState) error {
	if m.pending != nil || m.received != nil {
		return nil
	}
	if physicalMaintenanceHeld(m) {
		return fmt.Errorf("%w: original physical frame remains held", ErrUnavailable)
	}
	now := time.Now()
	first := m.preparationFamily % 2
	m.preparationFamily = (first + 1) % 2
	for n := uint8(0); n < 2; n++ {
		var dispatched bool
		var err error
		if (first+n)%2 == 0 {
			dispatched, err = c.dispatchRemoteInputFinish(m, now)
		} else {
			dispatched, err = c.dispatchShutdownWorkspaceHandoff(s, m, now)
		}
		if err != nil {
			return err
		}
		if dispatched {
			return nil
		}
	}
	return nil
}

func (c *Control) dispatchShutdownWorkspaceHandoff(s *loopState, m *maintenanceState, now time.Time) (bool, error) {
	for i := range s.workers {
		w := &s.workers[i]
		p := w.completion
		if p == nil {
			continue
		}
		if p.held || p.retirement.held {
			return false, fmt.Errorf("original workspace remains held: %w", errors.Join(ErrUnavailable, p.err, p.retirement.finishError))
		}
		if !p.prepared || !p.durable || !p.acked || p.cleanup == nil || w.pending != nil || w.received != nil {
			continue
		}
		if positiveRetirementPair(p.retirement.root, p, workspaceRetirementRoot) || positiveRetirementPair(p.retirement.handoff, p, workspaceRetirementHandoff) {
			continue
		}
		if now.Before(p.retirement.due) {
			continue
		}
		if c.retirementOriginal(s, p) == nil {
			return false, fmt.Errorf("%w: original workspace handoff route differs", ErrUnavailable)
		}
		intent, err := workspaceLocalIntent(*p.cleanup)
		if err != nil {
			return false, err
		}
		if m.serial == ^uint64(0) {
			return false, ErrUnavailable
		}
		owner, err := c.tryArtifactIO()
		if err != nil {
			p.err = err
			p.retirement.due = now.Add(time.Second)
			if errors.Is(err, ErrArtifactBusy) {
				return false, nil
			}
			return false, fmt.Errorf("%w: pre-Stop original handoff admission: %w", ErrUnavailable, err)
		}
		request := maintenanceRequest{sequence: m.serial + 1, workspaceRetirement: &workspaceRetirementRequest{original: p, control: c, backend: c.workspace.backend, work: *p.cleanup, intent: intent, owner: owner, step: workspaceRetirementHandoff}}
		m.serial = request.sequence
		m.pending = &request
		m.input <- request
		return true, nil
	}
	return false, nil
}

// After actual preparation the original loop performs only physical Close for
// terminal artifact drain. Keep C15's returned-Busy deadline and first demand.
func (c *Control) maintainShutdownDrain(m *maintenanceState) error {
	if m.pending != nil || m.received != nil {
		return nil
	}
	if c.physicalCloseWanted() && c.physicalRegistered() && !physicalMaintenanceHeld(m) {
		if previous := m.physicalResult; previous != nil && previous.request != nil && previous.request.physicalClose && previous.result.physical != nil && previous.result.physical.Registered && errors.Is(previous.result.physical.Err, ErrArtifactBusy) && time.Now().Before(m.physicalDue) {
			return nil
		}
		return c.dispatchPhysical(m)
	}
	return nil
}
