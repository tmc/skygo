package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

type operationKind uint8

const (
	startOperation operationKind = iota
	observeOperation
)

type operation struct {
	kind operationKind
	job  backend.Job
}
type operationResult struct {
	operation operation
	status    backend.Status
	err       error
}
type targetWorker struct {
	target  backend.Target
	input   chan operation
	pending *operation
	after   uint64
}
type loopState struct {
	workers    []targetWorker
	results    chan operationResult
	nextTarget int
	due        map[string]time.Time
}

// Run reconciles until canceled. It may be called only once per Control,
// including after cancellation or failure. Calls already inside a backend may
// outlive Run if that backend ignores cancellation; no replacement is started.
func (c *Control) Run(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: control loop already started", ErrUnavailable)
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	state := c.newLoop(lifetime)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := c.schedule(ctx, state); err != nil {
			c.failed.Store(true)
			return err
		}
		c.drainAccess(ctx)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-state.results:
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := c.integrate(state, result); err != nil {
				c.failed.Store(true)
				return err
			}
		case <-tick.C:
		}
	}
}
func (c *Control) newLoop(ctx context.Context) *loopState {
	s := &loopState{workers: make([]targetWorker, len(c.targets)), results: make(chan operationResult, len(c.targets)), due: make(map[string]time.Time)}
	for i, t := range c.targets {
		s.workers[i] = targetWorker{target: t, input: make(chan operation, 1)}
		go runWorker(ctx, t.Backend, s.workers[i].input, s.results)
	}
	return s
}
func runWorker(ctx context.Context, b backend.Backend, input <-chan operation, results chan<- operationResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case op := <-input:
			if ctx.Err() != nil {
				return
			}
			result := operationResult{operation: op}
			if op.kind == startOperation {
				call, cancel := context.WithTimeout(ctx, 10*time.Second)
				result.err = b.Start(call, op.job.CurrentAttemptID(), cloneTask(op.job.Task))
				cancel()
			} else {
				if op.job.CancelRequested {
					call, cancel := context.WithTimeout(ctx, 10*time.Second)
					_ = b.Cancel(call, reference(op.job))
					cancel()
				}
				if ctx.Err() != nil {
					return
				}
				call, cancel := context.WithTimeout(ctx, 10*time.Second)
				result.status, result.err = b.Observe(call, reference(op.job))
				result.status = cloneStatus(result.status)
				cancel()
			}
			select {
			case <-ctx.Done():
				return
			case results <- result:
			}
		}
	}
}
func (c *Control) schedule(ctx context.Context, s *loopState) error {
	// Integrate completions before accounting for new starts. The bounded drain
	// cannot be replenished indefinitely: workers need another dispatch first.
	for i := 0; i < len(s.workers); i++ {
		select {
		case result := <-s.results:
			if err := c.integrate(s, result); err != nil {
				return err
			}
		default:
			i = len(s.workers)
		}
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
		if err != nil {
			return err
		}
		if j.Status.State.Terminal() {
			continue
		}
		if needsStart(j) && j.CancelRequested {
			if _, _, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), now.UTC()); err != nil {
				return err
			}
			continue
		}
		target, reason := c.boundTarget(j)
		if reason == "" && needsStart(j) && j.Task.Retry != nil && !target.SupportsRetry {
			reason = unsupportedRetryTarget
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
		if w.pending != nil {
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
			if j.Status.State.Terminal() {
				continue
			}
			// Cancellation is independent of due time, placement and capacity.
			if needsStart(j) && j.CancelRequested {
				if _, _, err := c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), now.UTC()); err != nil {
					return err
				}
				continue
			}
			target, reason := c.boundTarget(j)
			if reason != "" {
				continue
			}
			kind := observeOperation
			if needsStart(j) {
				if active >= 32 || (j.Task.Retry != nil && !target.SupportsRetry) {
					continue
				}
				if j.Status.State == backend.Retrying && (j.NextAttemptAt == nil || now.Before(*j.NextAttemptAt)) {
					continue
				}
				var began bool
				j, began, err = c.store.BeginAttempt(j.ID, j.CurrentAttemptID(), now.UTC())
				if err != nil {
					return err
				}
				if !began {
					continue
				}
				active++
				kind = startOperation
				s.nextTarget = (i + 1) % len(s.workers)
			} else {
				deadline, tracked := s.due[j.CurrentAttemptID()]
				if now.Before(deadline) || (!tracked && len(s.due) >= 32) {
					continue
				}
			}
			op := operation{kind: kind, job: cloneJob(j)}
			if kind == observeOperation {
				s.due[j.CurrentAttemptID()] = now.Add(100 * time.Millisecond)
			}
			w.pending = &op
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
	worker.pending = nil
	j, err := c.store.Get(r.operation.job.ID)
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
