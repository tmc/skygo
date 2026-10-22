package control

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// dataRequest is the single original accepted invocation. Only Start carries
// plaintext; no method formats the request or reconstructs an issued secret.
type dataRequest struct {
	target    *dataTarget
	job       backend.Job
	key       store.RemoteKey
	row       store.RemoteAttempt
	start     backend.DataStart
	reference backend.Reference
	resources backend.DataResources
}

type dataResult struct {
	request     *dataRequest
	created     backend.DataCreated
	observation backend.DataObservation
	removal     backend.DataRemoval
	err         error
	cancelErr   error
}

// runDataOperation never mutates Store. Even a canceled accepted Start enters
// its one original native function and preserves its partial result and error.
func runDataOperation(ctx context.Context, op operation) operationResult {
	result := operationResult{operation: op}
	request := op.data
	returned := &dataResult{request: request}
	result.data = returned
	if request == nil || request.target == nil || request.target.backend == nil {
		returned.err = ErrUnsupported
		result.err = returned.err
		return result
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	switch op.kind {
	case dataStartOperation:
		start := request.start
		start.Execution.Task = cloneTask(start.Execution.Task)
		returned.created, returned.err = request.target.backend.StartData(call, start)
	case dataObserveOperation:
		// Cancellation and observation share the original existing target operation.
		// Neither the Cancel return nor context cancellation manufactures stop proof.
		if request.job.CancelRequested {
			returned.cancelErr = request.target.target.Backend.Cancel(call, cloneDataReference(request.reference))
		}
		returned.observation, returned.err = request.target.backend.ObserveData(call, cloneDataReference(request.reference))
		returned.observation = backend.CloneDataObservation(returned.observation)
	case dataRemoveOperation:
		resources := request.resources
		resources.Execution = cloneDataReference(resources.Execution)
		returned.removal, returned.err = request.target.backend.RemoveData(call, resources)
	default:
		returned.err = ErrUnsupported
	}
	result.err = errors.Join(returned.cancelErr, returned.err)
	return result
}

// remoteRoute uses accepted linkage before any portable Task shape test.
func (c *Control) remoteRoute(job backend.Job) (store.RemoteAttempt, bool, error) {
	row, err := c.store.GetRemoteAttempt(store.RemoteKey{JobID: job.ID, AttemptID: job.CurrentAttemptID()})
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrArtifactsDisabled) {
		return store.RemoteAttempt{}, false, nil
	}
	if err != nil {
		return store.RemoteAttempt{}, false, err
	}
	return row, true, nil
}

func cloneDataReference(ref backend.Reference) backend.Reference {
	ref.Task = cloneTask(ref.Task)
	return ref
}

func dataReference(job backend.Job, row store.RemoteAttempt) backend.Reference {
	return backend.Reference{ID: row.Key.AttemptID, Binding: job.TargetBinding, Instance: row.Resources.Pod.UID, Task: cloneTask(job.Task)}
}
func dataResources(job backend.Job, row store.RemoteAttempt) backend.DataResources {
	return backend.DataResources{Execution: dataReference(job, row), Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret}
}

func dataRequiredAck(job backend.Job) uint8 {
	bits := uint8(1)
	if job.Task.Workdir != nil {
		bits |= 2
	}
	if len(job.Task.Outputs) != 0 {
		bits |= 4
	}
	return bits
}

func (c *Control) queueData(w *targetWorker, op operation) {
	c.retention.mu.Lock()
	w.pending = &op
	c.retention.pending[op.data.key.AttemptID] = struct{}{}
	c.retention.mu.Unlock()
	w.after = op.job.Sequence
	w.input <- op // Existing single pending operation and one-slot original queue.
}

// cancelQueuedData sets only the monotone cancellation bit against the exact
// current queued/retry snapshot. A Start that won the race is revoked instead.
func (c *Control) cancelQueuedData(job backend.Job, row store.RemoteAttempt, now time.Time) error {
	updated, err := c.store.Update(job.ID, func(current *backend.Job) error {
		if current.CurrentAttemptID() != job.CurrentAttemptID() {
			return store.ErrArtifactConflict
		}
		if current.Status.State != backend.Queued && current.Status.State != backend.Retrying {
			return store.ErrArtifactConflict
		}
		current.CancelRequested = true
		return nil
	})
	if err != nil {
		if !errors.Is(err, store.ErrArtifactConflict) {
			return err
		}
		current, e := c.store.Get(job.ID)
		if e != nil {
			return e
		}
		latest, e := c.store.GetRemoteAttempt(store.RemoteKey{JobID: current.ID, AttemptID: current.CurrentAttemptID()})
		if e != nil {
			return e
		}
		if latest.State == 2 && latest.Active != nil && latest.Active.ClosedReason == 0 {
			_, e = c.store.RevokeRemote(latest.Key, latest.Revision, store.RemoteAuthorityLost, now)
		}
		return e
	}
	_, _, err = c.store.BeginRemoteAttempt(updated.ID, updated.CurrentAttemptID(), store.RemoteHashes{}, row.Policy, now)
	return err
}

// dispatchData admits at most one original operation on the existing worker.
func (c *Control) dispatchData(ctx context.Context, s *loopState, w *targetWorker, job backend.Job, row store.RemoteAttempt, now time.Time) (bool, error) {
	if row.Key.JobID != job.ID {
		return false, backend.ErrIdentityMismatch
	}
	now = now.UTC()
	// Queued cancellation/actual authority loss is metadata-only and precedes
	// placement, worker availability, retry due time and capacity admission.
	// Unavailable authority is unknown, not lost: it starts and cancels nothing.
	if needsStart(job) {
		if job.CancelRequested {
			_, _, err := c.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), store.RemoteHashes{}, row.Policy, now)
			return true, err
		}
		switch err := c.engine.CheckDelegation(row.Authority); {
		case errors.Is(err, auth.ErrUnavailable):
			if row.State != 3 {
				return false, nil
			} // State3 removal below is cleanup-only and starts nothing.
		case err != nil:
			return true, c.cancelQueuedData(job, row, now)
		}
	}
	if job.Target != w.target.Name || job.TargetBinding != w.target.Binding {
		return false, backend.ErrIdentityMismatch
	}
	if w.pending != nil || w.dataCompletion != nil || w.completion != nil && !w.completion.acked {
		return false, nil
	}
	target, err := c.dataTargetFor(job)
	if err != nil {
		return false, err
	}
	if row.State == 4 && !needsStart(job) {
		return false, nil
	}
	if row.State == 3 {
		c.setDataCompletion(w, &remotePending{key: row.Key, job: cloneJob(job), row: row, step: dataRemove, after: now})
		return true, nil
	}
	if row.State == 1 || needsStart(job) {
		if s.preparing {
			return false, nil
		}
		if job.Status.State == backend.Retrying && (job.NextAttemptAt == nil || now.Before(*job.NextAttemptAt)) {
			return false, nil
		}
		if job.Task.Retry != nil && (!target.target.SupportsRetry || !target.target.SupportsRemoteDataRetry) {
			return false, nil
		}
		active, err := c.store.Active()
		if err != nil {
			return false, err
		}
		count := 0
		for _, j := range active {
			if j.Status.State == backend.Starting || j.Status.State == backend.Running {
				count++
			}
		}
		if count >= 32 {
			return false, nil
		}
		if err = c.checkDataArtifacts(); err != nil {
			return false, err
		}
		if err = c.storageGuard(false, false); err != nil {
			return false, nil
		}
		if err = target.target.Backend.Check(cloneTask(job.Task)); err != nil {
			return false, err
		}
		// Generate only the needed purpose-separated raw secrets before the Begin
		// CAS; the actual accepted attempt ID is not known until Begin returns.
		var input, output []byte
		defer func() { clear(input); clear(output) }()
		hashes := store.RemoteHashes{}
		if job.Task.Workdir != nil {
			input = make([]byte, 32)
			if _, err = rand.Read(input); err != nil {
				return false, err
			}
			hashes.Input = remoteTokenHash(store.RemoteInput, input)
		}
		if len(job.Task.Outputs) != 0 {
			output = make([]byte, 32)
			if _, err = rand.Read(output); err != nil {
				return false, err
			}
			hashes.Output = remoteTokenHash(store.RemoteOutput, output)
		}
		switch err = c.engine.CheckDelegation(row.Authority); {
		case errors.Is(err, auth.ErrUnavailable):
			return false, nil
		case err != nil:
			return true, c.cancelQueuedData(job, row, now)
		}
		begun, start, err := c.store.BeginRemoteAttempt(job.ID, job.CurrentAttemptID(), hashes, row.Policy, now)
		if err != nil {
			var coreCapacity *store.CapacityError
			var artifactCapacity *store.ArtifactCapacityError
			if errors.As(err, &coreCapacity) || errors.As(err, &artifactCapacity) {
				return false, nil
			}
			return false, err
		}
		if !start {
			return false, nil
		}
		key := store.RemoteKey{JobID: begun.Job.ID, AttemptID: begun.Job.CurrentAttemptID()}
		request := &dataRequest{target: target, job: cloneJob(begun.Job), key: key, row: row}
		request.reference = backend.Reference{ID: key.AttemptID, Binding: begun.Job.TargetBinding, Task: cloneTask(begun.Job.Task)}
		request.start = backend.DataStart{JobID: key.JobID, Execution: request.reference, Budgets: begun.Budgets}
		// Publish custody BEFORE any post-CAS formatting/snapshot failure. A failed
		// format is retained, never permission to repeat or skip the original Start.
		op := operation{kind: dataStartOperation, job: cloneJob(begun.Job), data: request}
		w.pending = &op
		if len(input) != 0 {
			request.start.InputToken, err = remoteToken(key, input)
			if err != nil {
				return false, err
			}
		}
		if len(output) != 0 {
			request.start.OutputToken, err = remoteToken(key, output)
			if err != nil {
				return false, err
			}
		}
		request.row = store.RemoteAttempt{Key: key, Revision: begun.Revision, Authority: row.Authority, Policy: row.Policy, State: 2}
		c.queueData(w, op)
		return true, nil
	}
	if row.State != 2 || row.Active == nil {
		return false, ErrUnavailable
	}
	if row.Active.Stop != nil && (len(job.Task.Outputs) == 0 || row.Active.Collector != nil) || row.Active.NoLaunch != nil {
		c.setDataCompletion(w, &remotePending{key: row.Key, job: cloneJob(job), row: row, step: dataComplete, after: now})
		return true, nil
	}
	if row.Resources.Pod == (backend.DataResource{}) {
		if row.Active.Input != nil && row.Active.Input.State != 3 && row.Active.Input.State != 4 || row.Active.Output != nil && row.Active.Output.State != 3 {
			return false, nil
		}
		if row.Resources.InputSecret != (backend.DataResource{}) || row.Resources.OutputSecret != (backend.DataResource{}) {
			c.setDataCompletion(w, &remotePending{key: row.Key, job: cloneJob(job), row: row, step: dataRemove, after: now})
			return true, nil
		}
		return false, nil // Unknown original Pod never authorizes another Start.
	}
	if s.preparing {
		return false, nil
	}
	if job.CancelRequested && row.Active.ClosedReason == 0 {
		_, err = c.store.RevokeRemote(row.Key, row.Revision, store.RemoteCanceled, now)
		return true, err
	}
	if err = c.engine.CheckDelegation(row.Authority); err != nil && row.Active.ClosedReason == 0 {
		if errors.Is(err, auth.ErrUnavailable) {
			// Unknown authority dispatches no Observe whose result could not be
			// integrated, but actual expiry still closes the attempt.
			closed, e := c.closeDataAdmission(&remotePending{job: job, row: row}, now)
			if errors.Is(e, auth.ErrUnavailable) {
				return false, nil
			}
			return closed, e
		}
		_, e := c.store.RevokeRemote(row.Key, row.Revision, store.RemoteAuthorityLost, now)
		return true, e
	}
	if s.due == nil {
		s.due = make(map[string]time.Time)
	}
	if deadline, ok := s.due[row.Key.AttemptID]; ok && now.Before(deadline) {
		return false, nil
	}
	if _, ok := s.due[row.Key.AttemptID]; !ok && len(s.due) >= 32 {
		return false, nil
	}
	request := &dataRequest{target: target, job: cloneJob(job), key: row.Key, row: row, reference: dataReference(job, row)}
	s.due[row.Key.AttemptID] = now.Add(100 * time.Millisecond)
	c.queueData(w, operation{kind: dataObserveOperation, job: cloneJob(job), data: request})
	return true, nil
}

// integrateData transfers the actual received pair before releasing pending.
// No transaction failure or canceled caller can make the original result vanish.
func (c *Control) integrateData(s *loopState, w *targetWorker, result operationResult) error {
	original := w.received
	if original == nil || original.request != w.pending || original.request == nil || result.data == nil || result.data.request != original.request.data || result.operation.data != original.request.data || result.operation.kind != original.request.kind || result.data.request == nil || result.data.request.key != original.request.data.key {
		return fmt.Errorf("%w: remote result has no matching original invocation", ErrUnavailable)
	}
	if w.dataCompletion != nil {
		p := w.dataCompletion
		if result.operation.kind != dataRemoveOperation || p.step != dataRemove || p.original != nil {
			return fmt.Errorf("%w: remote completion still owns another original result", ErrUnavailable)
		}
		p.original = original
		p.step = dataRecord
		p.row = result.data.request.row
	} else {
		c.setDataCompletion(w, &remotePending{key: result.data.request.key, original: original, job: cloneJob(result.data.request.job), row: result.data.request.row, step: dataRecord})
	}
	c.retention.mu.Lock()
	w.pending = nil
	c.retention.mu.Unlock()
	return nil
}
