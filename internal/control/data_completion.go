package control

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type dataStep uint8

const (
	dataRecord dataStep = iota + 1
	dataComplete
	dataRemove
	dataAck
)

// remotePending retains the actual received pair until its durable handoff.
// In particular, a nil Ack or State3/fullbits is not retirement and cannot drop
// the original Removal or trigger a replacement native call.
type remotePending struct {
	key        store.RemoteKey // immutable admission-fence identity, including historical selection
	fenced     bool            // guarded by artifactService.mu then Control.runMu
	original   *receivedOperation
	job        backend.Job
	row        store.RemoteAttempt
	step       dataStep
	completion *store.RemoteCompletion
	after      time.Time
	held       bool
	err        error
}

func (c *Control) setDataCompletion(w *targetWorker, p *remotePending) {
	c.runMu.Lock()
	w.dataCompletion = p
	c.runMu.Unlock()
}

func (c *Control) clearDataPending(w *targetWorker) {
	p := w.dataCompletion
	c.setDataCompletion(w, nil)
	c.retention.mu.Lock()
	if p != nil {
		delete(c.retention.pending, p.key.AttemptID)
	}
	c.retention.mu.Unlock()
}

func dataTerminal(job backend.Job, row store.RemoteAttempt) bool {
	return row.Active != nil && (row.Active.NoLaunch != nil || row.Active.Stop != nil && (len(job.Task.Outputs) == 0 || row.Active.Collector != nil))
}

// closeDataAdmission performs at most one strict closure mutation per turn.
// Actual cancellation, final expiry and phase timeout remain enforced while
// original server owners drain; healthy preparation alone closes nothing.
// Unavailable authority is unknown, not lost: it closes nothing and returns
// auth.ErrUnavailable so the caller holds the SAME frame without renewal.
func (c *Control) closeDataAdmission(p *remotePending, now time.Time) (bool, error) {
	if p.row.State != 2 || p.row.Active == nil || p.row.Active.ClosedReason != 0 {
		return false, nil
	}
	a := p.row.Active
	reason := store.RemoteCloseReason(0)
	switch {
	case p.job.CancelRequested:
		reason = store.RemoteCanceled
	case !now.Before(a.ResourceDeadline):
		reason = store.RemoteExpired
	case a.TaskDeadline != nil && a.Stop == nil && !now.Before(*a.TaskDeadline):
		reason = store.RemotePhaseTimeout
	case a.CollectionDeadline != nil && !now.Before(*a.CollectionDeadline):
		reason = store.RemotePhaseTimeout
	case a.Running == nil && a.Stop == nil && a.NoLaunch == nil && !now.Before(a.StageDeadline):
		reason = store.RemotePhaseTimeout
	}
	for _, scope := range []*store.RemoteScope{a.Input, a.Output} {
		if reason == 0 && scope != nil && (scope.State == 1 || scope.State == 2) && !now.Before(scope.Expires) {
			reason = store.RemoteExpired
		}
	}
	if reason == 0 {
		switch err := c.engine.CheckDelegation(p.row.Authority); {
		case errors.Is(err, auth.ErrUnavailable):
			return false, err
		case err != nil:
			reason = store.RemoteAuthorityLost
		}
	}
	if reason == 0 {
		return false, nil
	}
	row, err := c.store.RevokeRemote(p.row.Key, p.row.Revision, reason, now)
	if err == nil {
		p.row = row
	}
	return true, err
}

func clearDataSecrets(request *dataRequest) {
	if request == nil {
		return
	}
	clear(request.start.InputToken)
	clear(request.start.OutputToken)
	request.start.InputToken = nil
	request.start.OutputToken = nil
}

func (c *Control) recordDataResult(w *targetWorker, p *remotePending, now time.Time) error {
	if p.original == nil || p.original.request == nil || p.original.request.data == nil || p.original.result.data == nil || p.original.result.data.request != p.original.request.data {
		return ErrUnavailable
	}
	result := p.original.result.data
	op := p.original.request
	switch op.kind {
	case dataStartOperation:
		if changed, err := c.closeDataAdmission(p, now); changed || err != nil {
			return err
		}
		row, err := c.store.RecordRemoteCreated(p.row.Key, p.row.Revision, store.RemoteCreation{Resources: result.created, Succeeded: result.err == nil}, now)
		if err != nil {
			return err
		}
		p.row = row
		// Only durable consumption of the SAME original result releases plaintext.
		clearDataSecrets(result.request)
		c.clearDataPending(w)
		return nil
	case dataObserveOperation:
		if changed, err := c.closeDataAdmission(p, now); changed || err != nil {
			return err
		}
		if errors.Is(result.err, backend.ErrIdentityMismatch) && p.row.Active != nil && p.row.Active.ClosedReason == 0 {
			row, err := c.store.RevokeRemote(p.row.Key, p.row.Revision, store.RemoteIdentityMismatch, now)
			if err == nil {
				p.row = row
			}
			return err
		}
		if err := backend.ValidateDataObservation(result.request.reference, result.observation); err != nil {
			// Keep the actual error/partial value in its original received frame. It
			// cannot become stop, no-launch, absence or a renewable synthetic status.
			p.held = true
			p.err = errors.Join(result.cancelErr, result.err, err)
			return nil
		}
		row, err := c.store.ObserveRemote(p.row.Key, p.row.Revision, result.observation, now)
		if err != nil {
			return err
		}
		p.row = row
		if dataTerminal(p.job, row) {
			p.step = dataComplete
			return nil
		}
		c.clearDataPending(w)
		return nil
	case dataRemoveOperation:
		// Removal is cleanup-only: fresh authority cannot erase returned independent
		// positive facts. Original resource echoes and joins remain mandatory.
		if err := backend.ValidateDataRemoval(result.request.resources, result.removal); err != nil {
			p.held = true
			p.err = errors.Join(result.err, err)
			return nil
		}
		p.step = dataAck
		return c.ackDataRemoval(w, p, now)
	}
	return ErrUnavailable
}

func (c *Control) ackDataRemoval(w *targetWorker, p *remotePending, now time.Time) error {
	if p.original == nil || p.original.result.data == nil || p.original.request.kind != dataRemoveOperation {
		return ErrUnavailable
	}
	settled, err := c.remoteAttemptTransfersSettled(p.row.Key)
	if err != nil || !settled {
		return err
	}
	result := p.original.result.data
	if err = c.store.AckRemoteRemoval(p.row.Key, p.row.Revision, result.removal, now); err != nil {
		return err
	}
	row, err := c.store.GetRemoteAttempt(p.row.Key)
	if err != nil {
		return err
	}
	p.row = row
	if row.State == 4 {
		c.clearDataPending(w)
		return nil
	}
	required := dataRequiredAck(p.job)
	if row.State == 3 && row.Ack&required == required {
		// Bounded closed-read detachment continues with this SAME actual value.
		p.step = dataAck
		return nil
	}
	// A partial/error native result is retained. A second Remove would pretend
	// the ambiguous original operation supplied facts it did not return.
	p.held = true
	p.err = result.err
	return nil
}

func (c *Control) buildDataCompletion(p *remotePending, now time.Time) (*store.RemoteCompletion, error) {
	a := p.row.Active
	if a == nil || !dataTerminal(p.job, p.row) {
		return nil, nil
	}
	result := &store.RemoteCompletion{Stop: a.Stop, NoLaunch: a.NoLaunch, Collector: a.Collector}
	if a.NoLaunch != nil {
		if a.NoLaunch.FailedInit == backend.StageFailure {
			result.Outcome = store.DataStageFailure
		} else {
			result.Outcome = store.DataSetupFailure
		}
		return result, nil
	}
	switch {
	case p.job.CancelRequested || a.ClosedReason == store.RemoteCanceled:
		result.Outcome = store.DataCanceled
	case a.ClosedReason == store.RemoteExpired || a.ClosedReason == store.RemotePhaseTimeout:
		result.Outcome = store.DataTimeout
	case a.ClosedReason == store.RemoteAuthorityLost && a.Stop.Task.ExitCode != 0:
		// The positively recorded nonzero main exit after lost authority. Store
		// commits it as a job-level authority failure, which never retries, so
		// no complete input transfer is required.
		result.Outcome = store.DataMainExit
	case a.ClosedReason != 0:
		result.Outcome = store.DataUncertain
	case a.Stop.Task.ExitCode != 0 || len(p.job.Task.Outputs) == 0:
		result.Outcome = store.DataMainExit
		if dataRetryNeedsInput(p.job, a.Stop) && (a.Input == nil || a.Input.State != 4) {
			// A retry-eligible Workdir main exit is normal completion only after the
			// complete input transfer (State4). A partial or absent read commits
			// nothing; actual close or expiry later finishes it without retry.
			return nil, nil
		}
	default:
		// A successful native main may never rerun because output collection failed.
		result.Outcome = store.DataCollectionFailure
		if a.Output == nil || a.Output.State != 2 || a.Collector == nil || a.Collector.ExitCode != 0 {
			return result, nil
		}
		grant := store.RemoteGrant{Key: p.row.Key, Kind: store.RemoteOutput, Epoch: a.Output.Epoch, Expires: a.Output.Expires}
		upload, file, manifest, err := c.store.GetRemoteOutput(grant, now)
		if errors.Is(err, store.ErrNotFound) {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		switch upload.State {
		case "creating":
			if file == (store.ArtifactFile{}) {
				return nil, nil
			} // UNKNOWN original create.
			return nil, ErrUnavailable
		case "writable", "finalizing":
			// The strict original transfer query already joined the actual server
			// owner; the exact known part remains charged in Store's discard intent.
			return result, nil
		case "complete":
			result.Outcome = store.DataMainExit
			result.Candidate = &store.RemoteOutputSeal{Reference: upload.Reference, File: file, Manifest: manifest}
		default:
			return nil, ErrUnavailable
		}
	}
	return result, nil
}

// dataRetryNeedsInput reports whether a DataMainExit completion of stop would
// schedule another attempt of a Workdir job: Store's remoteCompletionStatus
// makes it Failed/StoppedWithExitCode with this exit code, which Store retries
// when selected, within MaxAttempts and not canceled.
func dataRetryNeedsInput(job backend.Job, stop *backend.DataTaskStop) bool {
	p := job.Task.Retry
	return job.Task.Workdir != nil && p != nil && stop != nil && !job.CancelRequested && stop.Task.ExitCode > 0 && slices.Contains(p.ExitCodes, stop.Task.ExitCode) && len(job.Attempts) < int(p.MaxAttempts)
}

func (c *Control) advanceDataPending(ctx context.Context, w *targetWorker, p *remotePending, now time.Time) error {
	row, err := c.store.GetRemoteAttempt(p.row.Key)
	if err != nil {
		return err
	}
	p.row = row
	current, err := c.store.Get(p.key.JobID)
	if err != nil {
		return err
	}
	p.job.CancelRequested = current.CancelRequested
	switch p.step {
	case dataRecord:
		return c.recordDataResult(w, p, now)
	case dataAck:
		return c.ackDataRemoval(w, p, now)
	case dataComplete:
		if row.State == 3 || row.State == 4 {
			// A successful Complete may have returned a metadata error to its caller.
			// Retry the SAME semantic handoff before dropping the received pair.
			if p.completion != nil {
				if _, err = c.store.CompleteRemoteAttempt(row.Key, *p.completion, now); err != nil {
					return err
				}
			}
			p.original = nil
			p.step = dataRemove
			return nil
		}
		if changed, e := c.closeDataAdmission(p, now); changed || e != nil {
			return e
		}
		settled, e := c.remoteAttemptTransfersSettled(row.Key)
		if e != nil || !settled {
			return e
		}
		if p.completion == nil {
			p.completion, e = c.buildDataCompletion(p, now)
			if e != nil || p.completion == nil {
				return e
			}
		}
		if _, err = c.store.CompleteRemoteAttempt(row.Key, *p.completion, now); err != nil {
			return err
		}
		p.original = nil
		p.step = dataRemove
		return nil
	case dataRemove:
		if row.State == 4 {
			c.clearDataPending(w)
			return nil
		}
		settled, e := c.remoteAttemptTransfersSettled(row.Key)
		if e != nil || !settled {
			return e
		}
		if row.State != 3 && !(row.State == 2 && row.Active != nil && row.Active.ClosedReason != 0 && row.Resources.Pod == (backend.DataResource{})) {
			p.held = true
			return nil
		}
		required := dataRequiredAck(p.job)
		if row.State == 3 && row.Ack&required == required {
			// Qualified restart reuses ONLY already committed bits, never claims an
			// old function returned. The new finite frame is metadata-only.
			removal := backend.DataRemoval{Pod: row.Resources.Pod, InputSecret: row.Resources.InputSecret, OutputSecret: row.Resources.OutputSecret, PodRemoved: row.Ack&1 != 0, InputRemoved: row.Ack&2 != 0, OutputRemoved: row.Ack&4 != 0}
			if err = c.store.AckRemoteRemoval(row.Key, row.Revision, removal, now); err != nil {
				return err
			}
			latest, e := c.store.GetRemoteAttempt(row.Key)
			if e != nil {
				return e
			}
			p.row = latest
			if latest.State == 4 {
				c.clearDataPending(w)
			}
			return nil
		}
		target, e := c.dataTargetFor(p.job)
		if e != nil {
			return e
		}
		resources := dataResources(p.job, row)
		if e = backend.ValidateDataResources(resources); e != nil {
			p.held = true
			p.err = e
			return nil
		}
		request := &dataRequest{target: target, job: cloneJob(p.job), key: row.Key, row: row, reference: resources.Execution, resources: resources}
		c.queueData(w, operation{kind: dataRemoveOperation, job: cloneJob(p.job), data: request})
		return nil
	}
	return fmt.Errorf("%w: invalid remote metadata step", ErrUnavailable)
}

// advanceData takes one metadata/dispatch quantum per configured target. The
// bounded historical selector adds no worker, queue, registry or native grant.
func (c *Control) advanceData(ctx context.Context, s *loopState) error {
	now := time.Now().UTC()
	for i := range s.workers {
		if ctx.Err() != nil {
			return nil
		}
		w := &s.workers[i]
		p := w.dataCompletion
		if p == nil || p.held || w.pending != nil || now.Before(p.after) {
			continue
		}
		p.after = now.Add(100 * time.Millisecond)
		if err := c.advanceDataPending(ctx, w, p, now); err != nil {
			p.err = err
			// The held frame retries after p.after; unknown authority is not fatal.
			if errors.Is(err, store.ErrArtifactConflict) || errors.Is(err, auth.ErrUnavailable) {
				continue
			}
			return err
		}
	}
	if s.preparing || ctx.Err() != nil {
		return nil
	}
	row, found, err := c.store.NextRemoteAttempt(s.dataAfter)
	if errors.Is(err, store.ErrArtifactsDisabled) {
		return nil
	}
	if err != nil || !found {
		return err
	}
	s.dataAfter = row.Key.AttemptID
	if row.State != 3 {
		return nil
	}
	job, err := c.store.Get(row.Key.JobID)
	if err != nil {
		return err
	}
	for i := range s.workers {
		w := &s.workers[i]
		if w.target.Name != job.Target || w.pending != nil || w.dataCompletion != nil || w.completion != nil && !w.completion.acked {
			continue
		}
		if _, err = c.dataTargetFor(job); err != nil {
			return err
		}
		c.setDataCompletion(w, &remotePending{key: row.Key, job: cloneJob(job), row: row, step: dataRemove, after: now.Add(100 * time.Millisecond)})
		break
	}
	return nil
}

// dataOperationsPrepared is only the current original worker/result query.
// The separate strict server-transfer query remains necessary before Prepared.
func (c *Control) dataOperationsPrepared(s *loopState) (bool, error) {
	for i := range s.workers {
		w := &s.workers[i]
		if w.dataCompletion != nil || w.pending != nil && w.pending.data != nil || w.received != nil && w.received.request != nil && w.received.request.data != nil {
			return false, nil
		}
	}
	if s.unmatched != nil && s.unmatched.data != nil {
		return false, nil
	}
	return true, nil
}
