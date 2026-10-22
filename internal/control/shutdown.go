package control

import (
	"context"
	"fmt"
)

// runFrame retains the one original coordinator and worker ownership after Run
// returns. Only Run mutates scheduling/received state. After done closes those
// fields are stable; workers may still publish their one buffered result.
type runFrame struct {
	state       *loopState
	maintenance *maintenanceState
	done        chan struct{}
	err         error
}

type receivedOperation struct {
	request *operation
	result  operationResult
}

type receivedMaintenance struct {
	request *maintenanceRequest
	result  maintenanceResult
}

// Join waits for the original Run and target/maintenance worker functions to
// return. Run must have started. Cancellation leaves the same frame retained.
// Join performs no late integration or permit release, and does not join Local
// sequencers or prove descriptor/native closure or durable result integration.
func (c *Control) Join(ctx context.Context) error {
	c.runMu.Lock()
	frame := c.run
	c.runMu.Unlock()
	if frame == nil {
		return fmt.Errorf("%w: control loop has not published its original frame", ErrUnavailable)
	}
	wait := func(done <-chan struct{}) error {
		select {
		case <-done:
			return nil
		default:
		}
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("%w: original control owner remains held: %w", ErrUnavailable, ctx.Err())
		}
	}
	if err := wait(frame.done); err != nil {
		return err
	}
	for i := range frame.state.workers {
		if err := wait(frame.state.workers[i].done); err != nil {
			return err
		}
	}
	return wait(frame.maintenance.done)
}

// Receive ownership transfers before cancellation checks or integration. The
// bounded unmatched slot also preserves an unexpected result as an integrity
// failure; no new dispatch is possible after that failure returns from Run.
func (c *Control) receiveOperation(ctx context.Context, s *loopState, result operationResult) error {
	if s.unmatched != nil {
		return fmt.Errorf("%w: previous target result remains held", ErrUnavailable)
	}
	s.unmatched = &result
	check := result.operation.workflow
	var worker *targetWorker
	for i := range s.workers {
		w := &s.workers[i]
		// A workflow check precedes any durable Job: only its original request
		// pointer, pending on its own route's worker, identifies it.
		if check != nil && w.pending != nil && w.pending.workflow == check || check == nil && w.target.Name == result.operation.job.Target {
			worker = w
			break
		}
	}
	matched := worker != nil && worker.received == nil && worker.pending != nil && worker.pending.kind == result.operation.kind && worker.pending.workflow == check
	if matched && check != nil {
		route := check.routes[0].target
		matched = result.operation.kind == workflowCheckOperation && worker.target.Name == route.Name && worker.target.Binding == route.Binding
	} else if matched {
		matched = sameExecution(worker.pending.job, result.operation.job)
	}
	if !matched {
		return fmt.Errorf("%w: target result has no matching original request", ErrUnavailable)
	}
	if worker.pending.data != result.operation.data || (result.operation.data == nil) != (result.data == nil) {
		return fmt.Errorf("%w: target data result has no matching original request", ErrUnavailable)
	}
	worker.received = &receivedOperation{request: worker.pending, result: result}
	s.unmatched = nil
	if err := ctx.Err(); err != nil {
		return err
	}
	if check != nil {
		c.integrateWorkflowCheck(s, worker, result)
	} else if err := c.integrate(s, result); err != nil {
		return err
	}
	if worker.pending != nil {
		return fmt.Errorf("%w: target result did not settle its original request", ErrUnavailable)
	}
	worker.received = nil
	return nil
}

func (c *Control) receiveMaintenance(ctx context.Context, m *maintenanceState, result maintenanceResult) error {
	if m.received != nil {
		return fmt.Errorf("%w: previous maintenance result remains held", ErrUnavailable)
	}
	m.received = &receivedMaintenance{request: m.pending, result: result}
	if m.pending == nil || m.pending.sequence != result.request.sequence {
		return fmt.Errorf("%w: maintenance result has no matching original request", ErrUnavailable)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.integrateMaintenance(m, result); err != nil {
		return err
	}
	if m.pending != nil {
		return fmt.Errorf("%w: maintenance result did not settle its original request", ErrUnavailable)
	}
	m.received = nil
	return nil
}
