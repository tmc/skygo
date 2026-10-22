package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/store"
)

type operationKind uint8

const (
	startOperation operationKind = iota
	observeOperation
	workspaceCommitOperation
	workspacePrepareOperation
	dataStartOperation
	dataObserveOperation
	dataRemoveOperation
	workflowCheckOperation
)

type operation struct {
	data         *dataRequest
	kind         operationKind
	job          backend.Job
	output       *backend.OutputReference
	workspace    *workspaceStart
	prepareOwner *artifactIO
	intent       *local.WorkspaceRootIntent
	workflow     *workflowCheck
}
type operationResult struct {
	data       *dataResult
	operation  operation
	status     backend.Status
	err        error
	notStarted bool
	workspace  *local.WorkspaceObservation
	prepared   *workspacePrepared
}
type targetWorker struct {
	dataCompletion *remotePending
	target         backend.Target
	input          chan operation
	pending        *operation
	after          uint64
	bindOutput     func(backend.OutputReference) error
	completion     *workspacePending
	done           chan struct{}
	received       *receivedOperation
	shutdownStart  *receivedOperation // original possible canceled-before-parent pair
}
type loopState struct {
	dataAfter           string
	workers             []targetWorker
	results             chan operationResult
	nextTarget          int
	due                 map[string]time.Time
	unmatched           *operationResult
	preparing, prepared bool
	workflows           []*workflowCheck
	workflowHeld        map[workflowHold]time.Time
	workflowDue         time.Time
	workflowPoll        func() ([]store.WorkflowCandidate, error) // nil polls c.store
}

// Run reconciles until canceled. It may be called only once per Control,
// including after cancellation or failure. Calls already inside a backend may
// outlive Run if that backend ignores cancellation; no replacement is started.
// Join waits for the original Run and workers separately. Returned results and
// their requests remain retained if cancellation or integration fails.
func (c *Control) Run(ctx context.Context) (result error) {
	if c == nil || c.shutdown == nil {
		return fmt.Errorf("%w: control lifecycle is not initialized", ErrUnavailable)
	}
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: control loop already started", ErrUnavailable)
	}
	lifetime, cancel := context.WithCancel(ctx)
	state := c.newLoop(lifetime)
	maintenance := newMaintenance(lifetime)
	frame := &runFrame{state: state, maintenance: maintenance, done: make(chan struct{})}
	c.runMu.Lock()
	c.run = frame
	c.runMu.Unlock()
	defer func() {
		cancel()
		for i := range state.workers {
			close(state.workers[i].input)
		}
		close(maintenance.input)
		frame.err = result
		c.shutdownRunReturned(result)
		close(frame.done)
	}()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		request, changed := c.shutdownEvents()
		if !state.preparing {
			select {
			case <-request:
				state.preparing = true
			default:
			}
		}
		if state.preparing {
			request = nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !state.prepared {
			if err := c.schedule(ctx, state); err != nil {
				if ctx.Err() == nil {
					c.failed.Store(true)
				}
				return err
			}
		}
		var maintenanceErr error
		switch {
		case state.prepared:
			maintenanceErr = c.maintainShutdownDrain(maintenance)
		case state.preparing:
			maintenanceErr = c.maintainShutdownPreparation(state, maintenance)
		default:
			maintenanceErr = c.maintain(ctx, maintenance)
		}
		if maintenanceErr != nil {
			c.failed.Store(true)
			return maintenanceErr
		}
		if state.preparing && !state.prepared {
			if err := c.tryShutdownPrepared(state, maintenance); err != nil {
				c.failed.Store(true)
				return err
			}
		}
		if !state.prepared {
			c.drainAccess(ctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-maintenance.results:
			if err := c.receiveMaintenance(ctx, maintenance, result); err != nil {
				if ctx.Err() == nil {
					c.failed.Store(true)
				}
				return err
			}
		case result := <-state.results:
			if err := c.receiveOperation(ctx, state, result); err != nil {
				if ctx.Err() == nil {
					c.failed.Store(true)
				}
				return err
			}
		case <-request:
			state.preparing = true
		case <-changed:
		case <-tick.C:
		}
	}
}
func (c *Control) newLoop(ctx context.Context) *loopState {
	s := &loopState{workers: make([]targetWorker, len(c.targets)), results: make(chan operationResult, len(c.targets)), due: make(map[string]time.Time), workflowHeld: make(map[workflowHold]time.Time)}
	for i, t := range c.targets {
		s.workers[i] = targetWorker{target: t, input: make(chan operation, 1), done: make(chan struct{})}
		if t.Name == c.retention.local.Target && t.Binding == c.retention.local.Binding && t.OutputStorage == backend.LocalOutput {
			s.workers[i].bindOutput = c.retention.local.BindOutput
		}
		var workspace *workspaceTarget
		if c.workspace != nil && c.workspace.name == t.Name && c.workspace.binding == t.Binding && t.Backend == c.workspace.backend {
			workspace = c.workspace
		}
		w := &s.workers[i]
		go func() {
			defer close(w.done)
			runWorkspaceWorker(ctx, w.target.Backend, w.bindOutput, workspace, w.input, s.results)
		}()
	}
	return s
}
func runWorker(ctx context.Context, b backend.Backend, input <-chan operation, results chan<- operationResult) {
	runWorkerWithOutput(ctx, b, nil, input, results)
}
func runWorkerWithOutput(ctx context.Context, b backend.Backend, bind func(backend.OutputReference) error, input <-chan operation, results chan<- operationResult) {
	runWorkspaceWorker(ctx, b, bind, nil, input, results)
}

func runWorkspaceWorker(ctx context.Context, b backend.Backend, bind func(backend.OutputReference) error, workspace *workspaceTarget, input <-chan operation, results chan<- operationResult) {
	for op := range input {
		result := operationResult{operation: op}
		if op.kind == dataStartOperation || op.kind == dataObserveOperation || op.kind == dataRemoveOperation {
			results <- runDataOperation(ctx, op)
			continue
		}
		if op.kind == workflowCheckOperation {
			if op.workflow == nil || op.data != nil {
				result.err = ErrInvalid
			} else if result.err = ctx.Err(); result.err == nil {
				result.err = b.Check(cloneTask(op.workflow.task))
			}
			results <- result
			continue
		}
		if op.data != nil {
			result.err = ErrInvalid
			results <- result
			continue
		}
		if ctx.Err() != nil {
			if op.workspace != nil {
				op.workspace.owner.finish(nil)
			}
			if op.prepareOwner != nil {
				op.prepareOwner.finish(nil)
			}
			result.err = ctx.Err()
			result.notStarted = op.kind == startOperation
			results <- result
			continue
		}
		if op.kind == workspaceCommitOperation {
			if workspace == nil || op.intent == nil {
				result.err = ErrUnsupported
			} else {
				result.err = workspace.backend.CommitWorkspace(reference(op.job), *op.intent)
			}
		} else if op.kind == workspacePrepareOperation {
			if workspace == nil || op.prepareOwner == nil {
				result.err = ErrUnsupported
			} else {
				observation, origin, err := workspace.backend.PrepareWorkspaceCompletion(reference(op.job))
				result.prepared = &workspacePrepared{observation: observation, origin: origin}
				result.err = err
			}
			if op.prepareOwner != nil {
				op.prepareOwner.finish(result.err)
			}
		} else if op.kind == startOperation {
			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			if op.output != nil {
				if bind == nil {
					result.err = errors.New("local output binding unavailable")
				} else {
					result.err = bind(*op.output)
				}
				result.notStarted = result.err != nil
			}
			if result.err == nil && call.Err() != nil {
				result.err = call.Err()
				result.notStarted = true
			}
			if op.workspace != nil {
				if result.err == nil {
					result.err = bindWorkspaceStart(workspace, op)
				}
				if result.err != nil {
					// No ownership transferred unless Bind returned nil.
					op.workspace.owner.finish(result.err)
					result.notStarted = true
				}
			}
			if result.err == nil {
				result.err = b.Start(call, op.job.CurrentAttemptID(), cloneTask(op.job.Task))
			}
			cancel()
		} else {
			if op.job.CancelRequested {
				call, cancel := context.WithTimeout(ctx, 10*time.Second)
				result.err = b.Cancel(call, reference(op.job))
				cancel()
			}
			if ctx.Err() != nil {
				result.err = errors.Join(result.err, ctx.Err())
				results <- result
				continue
			}
			call, cancel := context.WithTimeout(ctx, 10*time.Second)
			cancelErr := result.err
			if op.job.Task.Workdir != nil && currentStatus(op.job).Instance != "" {
				if workspace == nil {
					result.err = ErrUnsupported
				} else {
					if err := workspace.backend.ResumeWorkspacePublication(reference(op.job)); err != nil {
						result.err = err
					} else {
						observation, err := workspace.backend.WorkspaceObservation(reference(op.job))
						result.workspace, result.err = &observation, err
					}
				}
			} else {
				result.status, result.err = b.Observe(call, reference(op.job))
			}
			result.status = cloneStatus(result.status)
			result.err = errors.Join(cancelErr, result.err)
			cancel()
		}
		// One pending request per worker bounds outstanding results by the
		// existing channel capacity, even after Run stops receiving.
		results <- result
	}
}
func (c *Control) schedule(ctx context.Context, s *loopState) error {
	// Integrate completions before accounting for new starts. The bounded drain
	// cannot be replenished indefinitely: workers need another dispatch first.
	for i := 0; i < len(s.workers); i++ {
		select {
		case result := <-s.results:
			if err := c.receiveOperation(ctx, s, result); err != nil {
				return err
			}
		default:
			i = len(s.workers)
		}
	}
	if err := c.advanceData(ctx, s); err != nil {
		return err
	}
	if err := c.advanceWorkspaces(ctx, s); err != nil {
		return err
	}
	if s.preparing {
		return c.scheduleShutdownLocal(ctx, s)
	}
	if err := c.advanceWorkflows(ctx, s); err != nil {
		return err
	}
	jobs, err := c.store.Active()
	if err != nil {
		return err
	}
	active := 0
	for _, j := range jobs {
		if j.Status.State == backend.Starting || j.Status.State == backend.Running {
			active++
		}
	}
	eligible := make(map[string][]backend.Job, len(s.workers))
	retained := make(map[string]bool, len(s.due))
	for _, j := range jobs {
		if j.Status.State == backend.Starting || j.Status.State == backend.Running {
			if _, ok := s.due[j.CurrentAttemptID()]; ok {
				retained[j.CurrentAttemptID()] = true
			}
		}
	}
	now := time.Now()
	pending := make(map[string]bool, len(s.workers))
	for _, w := range s.workers {
		if w.pending != nil && w.pending.kind == observeOperation {
			pending[w.pending.job.CurrentAttemptID()] = true
		}
	}
	for id, deadline := range s.due {
		if !retained[id] || (!pending[id] && !now.Before(deadline)) {
			delete(s.due, id)
		}
	}
	for _, snapshot := range jobs {
		if ctx.Err() != nil {
			return nil
		}
		j, err := c.store.Get(snapshot.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		row, remote, err := c.remoteRoute(j)
		if err != nil {
			return err
		}
		if remote {
			var authErr error
			if needsStart(j) && !j.CancelRequested {
				authErr = c.engine.CheckDelegation(row.Authority)
			}
			if errors.Is(authErr, auth.ErrUnavailable) {
				if row.State != 3 {
					continue // Unknown authority neither starts nor cancels queued work.
				}
				authErr = nil // State3 removal is cleanup-only; dispatch starts nothing.
			}
			if needsStart(j) && (j.CancelRequested || authErr != nil) {
				if j.CancelRequested {
					_, _, err = c.store.BeginRemoteAttempt(j.ID, j.CurrentAttemptID(), store.RemoteHashes{}, row.Policy, now.UTC())
				} else {
					err = c.cancelQueuedData(j, row, now.UTC())
				}
				if err != nil {
					return err
				}
				continue
			}
			if !j.Status.State.Terminal() {
				eligible[j.Target] = append(eligible[j.Target], j)
			}
			continue
		}
		if j.Status.State.Terminal() {
			continue
		}
		if needsStart(j) && j.CancelRequested {
			if _, _, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now.UTC()); err != nil {
				return err
			}
			continue
		}
		target, reason := c.boundTarget(j)
		if reason == "" && j.Task.Workdir != nil && !c.WorkspaceTargetEnabled(j.Target, j.TargetBinding) {
			reason = "local workspace service is unsupported"
		}
		if reason == "" && needsStart(j) && j.Task.Retry != nil && !target.SupportsRetry {
			reason = unsupportedRetryTarget
		}
		if reason == "" && needsStart(j) && c.retention.enabled && target.OutputStorage == backend.UnspecifiedOutput {
			reason = unspecifiedOutputTarget
		}
		if reason != "" {
			if j.Status.Reason != reason {
				if _, err := c.store.Update(j.ID, func(current *backend.Job) error { current.Status.Reason = reason; return nil }); err != nil {
					return err
				}
			}
			continue
		}
		if routingHeld(j.Status.Reason) {
			j, err = c.store.Update(j.ID, func(current *backend.Job) error { current.Status.Reason = ""; return nil })
			if err != nil {
				return err
			}
		}
		eligible[j.Target] = append(eligible[j.Target], j)
	}
	begin := s.nextTarget
	for n := 0; n < len(s.workers); n++ {
		if ctx.Err() != nil {
			return nil
		}
		i := (begin + n) % len(s.workers)
		w := &s.workers[i]
		if w.pending != nil || w.dataCompletion != nil || w.completion != nil && !w.completion.acked {
			continue
		}
		candidates := eligible[w.target.Name]
		if len(candidates) == 0 {
			continue
		}
		// Begin after the last dispatched sequence, then wrap once.
		first := 0
		for first < len(candidates) && candidates[first].Sequence <= w.after {
			first++
		}
		for k := 0; k < len(candidates); k++ {
			j := candidates[(first+k)%len(candidates)]
			current, err := c.store.Get(j.ID)
			if err != nil {
				return err
			}
			j = current
			row, remote, err := c.remoteRoute(j)
			if err != nil {
				return err
			}
			if remote {
				dispatched, err := c.dispatchData(ctx, s, w, j, row, now.UTC())
				if err != nil {
					return err
				}
				if dispatched {
					if w.pending != nil && w.pending.kind == dataStartOperation {
						active++
					}
					s.nextTarget = (i + 1) % len(s.workers)
					break
				}
				continue
			}
			if j.Task.Workdir != nil && w.completion != nil {
				continue
			}
			if j.Status.State.Terminal() {
				continue
			}
			// Cancellation is independent of due time, placement and capacity.
			if needsStart(j) && j.CancelRequested {
				if _, _, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), 0, now.UTC()); err != nil {
					return err
				}
				continue
			}
			target, reason := c.boundTarget(j)
			if reason != "" {
				continue
			}
			kind := observeOperation
			var materialization *artifactIO
			if needsStart(j) {
				if active >= 32 || (j.Task.Retry != nil && !target.SupportsRetry) {
					continue
				}
				if j.Status.State == backend.Retrying && (j.NextAttemptAt == nil || now.Before(*j.NextAttemptAt)) {
					continue
				}
				if c.retention.enabled && target.OutputStorage == backend.UnspecifiedOutput {
					continue
				}
				if err := c.storageGuard(false, target.OutputStorage == backend.LocalOutput); err != nil {
					const held = "storage observation unavailable; execution held"
					if j.Status.Reason != held {
						if _, e := c.store.Update(j.ID, func(current *backend.Job) error { current.Status.Reason = held; return nil }); e != nil && !errors.Is(e, store.ErrNotFound) {
							return e
						}
					}
					continue
				}
				reserve := uint64(0)
				if c.retention.enabled {
					reserve = target.OutputReservation
				}
				if j.Task.Workdir != nil {
					materialization, err = c.tryArtifactIO()
					if err != nil {
						continue
					} // No durable attempt before the shared slot.
				}
				previousReason := j.Status.Reason
				begun, began, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), reserve, now.UTC())
				if err != nil {
					materialization.finish(nil)
					var capacity *store.CapacityError
					var artifactCapacity *store.ArtifactCapacityError
					if errors.As(err, &capacity) || errors.As(err, &artifactCapacity) {
						const held = "retention capacity unavailable; execution held"
						if previousReason != held {
							if _, e := c.store.Update(j.ID, func(current *backend.Job) error { current.Status.Reason = held; return nil }); e != nil && !errors.Is(e, store.ErrNotFound) {
								return e
							}
						}
						continue
					}
					return err
				}
				if !began {
					materialization.finish(nil)
					continue
				}
				j = begun
				active++
				if c.retention.enabled {
					c.retention.wantSample.Store(true)
				}
				kind = startOperation
				s.nextTarget = (i + 1) % len(s.workers)
			} else {
				deadline, tracked := s.due[j.CurrentAttemptID()]
				if now.Before(deadline) || (!tracked && len(s.due) >= 32) {
					continue
				}
			}
			op := operation{kind: kind, job: cloneJob(j)}
			if kind == startOperation {
				w.pending = &op
			}
			if kind == startOperation && j.Task.Workdir != nil {
				reservation, e := c.store.GetWorkspaceReservation(j.ID, j.CurrentAttemptID())
				if e != nil {
					materialization.finish(nil)                                                                 // No filesystem owner was created.
					return fmt.Errorf("%w: committed workspace reservation unavailable: %w", ErrUnavailable, e) // Original pending Start remains retained.
				}
				op.workspace = &workspaceStart{reservation: reservation, owner: materialization}
			}
			if kind == startOperation && c.retention.enabled && target.OutputStorage == backend.LocalOutput {
				ref, e := c.store.OutputReference(j.ID, j.CurrentAttemptID())
				if e != nil {
					materialization.finish(nil)
					return e
				}
				op.output = &ref
			}
			if kind == observeOperation {
				s.due[j.CurrentAttemptID()] = now.Add(100 * time.Millisecond)
			}
			c.retention.mu.Lock()
			w.pending = &op
			c.retention.pending[j.CurrentAttemptID()] = struct{}{}
			c.retention.mu.Unlock()
			w.after = j.Sequence
			w.input <- op // One pending operation per worker; its one-slot queue is empty.
			break
		}
	}
	return nil
}
func sameExecution(a, b backend.Job) bool {
	if a.ID != b.ID || a.Target != b.Target || a.TargetBinding != b.TargetBinding || a.CurrentAttemptID() != b.CurrentAttemptID() || currentStatus(a).Instance != currentStatus(b).Instance {
		return false
	}
	x, e1 := backend.TaskBytes(a.Task)
	y, e2 := backend.TaskBytes(b.Task)
	return e1 == nil && e2 == nil && bytes.Equal(x, y)
}
func (c *Control) integrate(s *loopState, r operationResult) error {
	var worker *targetWorker
	for i := range s.workers {
		w := &s.workers[i]
		if w.target.Name == r.operation.job.Target {
			worker = w
			break
		}
	}
	if worker == nil || worker.pending == nil || worker.pending.kind != r.operation.kind || !sameExecution(worker.pending.job, r.operation.job) {
		return nil
	}
	if r.operation.kind == dataStartOperation || r.operation.kind == dataObserveOperation || r.operation.kind == dataRemoveOperation {
		return c.integrateData(s, worker, r)
	}
	if r.operation.data != nil || r.data != nil {
		return ErrUnavailable
	}
	if c.ownedLocalJob(r.operation.job) && r.operation.kind == startOperation && r.operation.workspace != nil && !r.notStarted && worker.received != nil && (errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded)) {
		worker.shutdownStart = worker.received
	}
	if s.preparing && c.ownedLocalJob(r.operation.job) && r.err != nil && r.operation.kind != workspaceCommitOperation {
		noCall := r.operation.kind == startOperation && r.operation.workspace != nil && !r.notStarted && (errors.Is(r.err, context.Canceled) || errors.Is(r.err, context.DeadlineExceeded))
		if !noCall {
			return fmt.Errorf("%w: original owned Local operation failed: %w", ErrUnavailable, r.err)
		}
	}
	if r.operation.job.Task.Workdir != nil {
		return c.integrateWorkspace(s, worker, r)
	}
	c.retention.mu.Lock()
	worker.pending = nil
	delete(c.retention.pending, r.operation.job.CurrentAttemptID())
	c.retention.mu.Unlock()
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
			_, err = c.store.ObserveAttempt(j.ID, j.CurrentAttemptID(), backend.Status{State: backend.Unknown, Reason: "local output binding unavailable; execution not started"}, time.Now().UTC())
			return err
		}
		return nil
	} // Ambiguity never retries Start.
	if s.due == nil {
		s.due = make(map[string]time.Time)
	}
	s.due[j.CurrentAttemptID()] = time.Now().Add(100 * time.Millisecond)
	status := r.status
	if errors.Is(r.err, backend.ErrIdentityMismatch) {
		status = backend.Status{State: backend.Unknown, Reason: "execution identity mismatch; conflicting workload left untouched"}
	} else if r.err != nil || status.State == "" {
		return nil
	}
	switch status.State {
	case backend.Starting, backend.Running, backend.Succeeded, backend.Failed, backend.Canceled, backend.Unknown:
	default:
		return nil
	}
	if j.Status.State == backend.Running && status.State == backend.Starting {
		return nil // An older observation cannot move durable execution backward.
	}
	observed, err := c.store.ObserveAttempt(j.ID, j.CurrentAttemptID(), cloneStatus(status), time.Now().UTC())
	if err == nil && (observed.Status.State != backend.Starting && observed.Status.State != backend.Running) {
		delete(s.due, j.CurrentAttemptID())
	}
	return err
}
