package store

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
)

// ErrWorkerGroupStale reports a stale group revision or a token that does not
// name the current intent. Nothing is written.
var ErrWorkerGroupStale = errors.New("stale worker group revision or intent")

// errWorkerIntentLive refuses to declare an intent of this process lost: its
// original call may still be in flight.
var errWorkerIntentLive = errors.New("worker intent may still be in flight")

// WorkerGroupRequest is one dedicated group submission. Spec is normalized
// with Count 2..backend.MaxWorkers and mode WorkerIndependent; a single
// worker never reaches the Store. Spec alone carries the worker request; the
// task has no retry policy, workspace, setup, outputs or data mounts. Authority comes
// from Engine.Delegate for the submitting actor. Demand.Ranks equals
// Spec.Count, and its CPU and memory are nonzero.
type WorkerGroupRequest struct {
	Domain          RequestDomain
	RequestID       string
	Task            backend.Task
	Spec            backend.WorkerSpec
	Target, Binding string
	Authority       auth.Delegation
	Demand          workeradmission.Demand
}

// WorkerGroupPhase is a group's stored phase.
type WorkerGroupPhase uint8

const (
	// WorkerGroupQueued waits for admission and holds no reservation.
	WorkerGroupQueued WorkerGroupPhase = iota
	// WorkerGroupAdmitted holds its reservation; no member is created.
	WorkerGroupAdmitted
	// WorkerGroupActive has at least one Create intent and no unknown Create.
	WorkerGroupActive
	// WorkerGroupHeld has an unknown Create. It keeps its reservation and
	// live slot until an operator resolution exists; it never settles.
	WorkerGroupHeld
	// WorkerGroupDone is settled. Its rows remain until Prune retires them.
	WorkerGroupDone
)

// WorkerIntent is the state of one member effect.
type WorkerIntent uint8

const (
	WorkerNone WorkerIntent = iota
	WorkerIntended
	WorkerRecorded
	WorkerUnknown
)

// WorkerEffect names one member operation.
type WorkerEffect uint8

const (
	WorkerCreate WorkerEffect = iota
	WorkerRelease
	WorkerCancel
	WorkerRemove
)

// WorkerToken names one durable intent: one effect of one member at that
// effect's sequence, created by process generation Gen.
type WorkerToken struct {
	Job    string
	Rank   uint8
	Effect WorkerEffect
	Seq    uint64
	Gen    [16]byte
}

// WorkerGroup is a copy of one group's durable state. Job's task requests no
// workers, and Job keeps an empty Status.Instance; its status is the group aggregate.
// Stopping is a projection: Phase Active with cancellation, revocation or a
// terminal aggregate. Revision is the group compare-and-swap token.
type WorkerGroup struct {
	Job       backend.Job
	Group     backend.WorkerGroup
	Principal string
	Domain    RequestDomain
	RequestID string
	Authority auth.Delegation
	Demand    workeradmission.Demand
	Phase     WorkerGroupPhase
	Stopping  bool
	Reserved  bool
	Revoked   bool
	Revision  uint64
	Members   []WorkerMember
}

// WorkerEffectState is one member effect. Seq, Gen and At are nonzero exactly
// when State is not WorkerNone. Error is sanitized text.
type WorkerEffectState struct {
	State WorkerIntent
	Seq   uint64
	Gen   [16]byte
	At    time.Time
	Error string
}

// WorkerMember is one rank. Pod is the original Create return and never
// changes once set. Removed records a UID-qualified removal of exactly Pod.
type WorkerMember struct {
	Rank       uint8
	Counter    uint64
	Create     WorkerEffectState
	Release    WorkerEffectState
	Cancel     WorkerEffectState
	Remove     WorkerEffectState
	Pod        backend.WorkerResource
	ReleaseAck bool
	Revision   string
	CancelAck  bool
	Removed    bool
	Observed   *WorkerObserved
}

// WorkerObserved is a rank's last accepted observation. ExitCode is set only
// from a native exit receipt, after which the observation is frozen.
type WorkerObserved struct {
	State     backend.State
	Gated     bool
	Container string
	Started   time.Time
	ExitCode  *int32
	Finished  time.Time
	Node      string
	At        time.Time
}

// SubmitWorkerGroup atomically accepts r as a Queued group in the actor's
// request namespace: its job row, retained metadata, request record, group
// reserve and member rows commit together. The same request with the same
// task and spec returns the original group; a different task or spec, or a
// request that names an ordinary or workflow submission, is ErrConflict.
// It requires enabled retention and returns ErrFull while 64 groups are not
// Done. A Done group keeps its rows and reserve until Prune retires it with
// its job, so reclaiming group storage requires configured pruning.
func (s *Store) SubmitWorkerGroup(actor auth.Actor, r WorkerGroupRequest) (out WorkerGroup, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(r.RequestID) {
		return out, errors.New("invalid request ID")
	}
	if !validAttemptID(r.Domain.Lineage) || r.Domain.Epoch == 0 {
		return out, ErrInvalidRequestDomain
	}
	if r.Target == "" || len(r.Target) > 128 || !validBinding(r.Binding) {
		return out, errors.New("invalid worker group target")
	}
	if !workerTaskValid(r.Task) {
		return out, errors.New("worker group tasks do not support workers, retry, workspaces or data")
	}
	raw, err := backend.TaskBytes(r.Task)
	if err != nil {
		return
	}
	if len(raw) > 256<<10 {
		return out, errors.New("task exceeds limit")
	}
	if !workerSpecValid(r.Spec) {
		return out, errors.New("invalid worker group spec")
	}
	if r.Demand.Ranks != uint32(r.Spec.Count) || r.Demand.CPUMillis == 0 || r.Demand.MemoryBytes == 0 {
		return out, errors.New("invalid worker demand")
	}
	if err = workeradmission.Fits(workerDemandLimit, r.Demand); err != nil {
		return out, fmt.Errorf("worker demand: %w", err)
	}
	if !validWorkflowAuthority(r.Authority, actor.Principal()) {
		return out, errors.New("invalid worker group authority")
	}
	id, err := newID()
	if err != nil {
		return
	}
	task := backend.CloneTask(r.Task)
	var result error
	err = s.db.Update(func(tx *bolt.Tx) error {
		now := s.now().UTC()
		st, res, e := gateDomain(tx, r.Domain, now)
		if e != nil {
			return e
		}
		if res != nil {
			result = res
			return nil
		}
		var found bool
		out, found, result = lookupWorkerGroupRecord(tx, actor, r.Domain, r.RequestID, task, r.Spec)
		if found || result != nil {
			return nil
		}
		if r.Domain.Epoch == 0 {
			return errors.New("zero request epoch cannot accept")
		}
		for _, ep := range st.Epochs {
			if ep.Number == r.Domain.Epoch && ep.AcceptClosed {
				result = &ExpiryError{Reason: RequestEpochExpired}
				return nil
			}
		}
		live, e := workerGroupsLive(tx)
		if e != nil {
			return e
		}
		if live >= maxLiveWorkerGroups {
			result = ErrFull
			return nil
		}
		out, e = acceptWorkerGroup(tx, actor.Principal(), r, task, id, now)
		return e
	})
	if err == nil {
		err = result
	}
	return
}

// lookupWorkerGroupRecord finds the actor's request in domain d. Only the
// same group task and spec replay; an expired result reports ResultExpired.
func lookupWorkerGroupRecord(tx *bolt.Tx, actor auth.Actor, d RequestDomain, id string, t backend.Task, spec backend.WorkerSpec) (out WorkerGroup, found bool, err error) {
	var r requestRecord
	e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(actor.Principal(), d, id), &r)
	if errors.Is(e, ErrNotFound) {
		return out, false, nil
	}
	if e != nil {
		return out, false, e
	}
	if r.Principal != actor.Principal() || r.Domain != d || r.requestID() != id {
		return out, true, errors.New("request identity hash collision")
	}
	if r.WorkflowID != "" {
		return out, true, ErrConflict
	}
	digest, e := taskDigest(t)
	if e != nil {
		return out, true, e
	}
	if digest != r.TaskDigest {
		return out, true, ErrConflict
	}
	// A pruned group leaves only its expired request, so expiry precedes the
	// group relation.
	if r.Expired {
		return out, true, &ExpiryError{Reason: ResultExpired, JobID: r.JobID}
	}
	if !workerGroupJob(tx, r.JobID) {
		return out, true, ErrConflict
	}
	w, e := loadWorkerState(tx, r.JobID)
	if e != nil {
		return out, true, e
	}
	if w.g.SpecDigest != workerSpecDigest(spec) {
		return out, true, ErrConflict
	}
	have, e := backend.TaskBytes(w.job.Task)
	if e != nil {
		return out, true, e
	}
	want, e := backend.TaskBytes(t)
	if e != nil {
		return out, true, e
	}
	if !bytes.Equal(have, want) {
		return out, true, ErrConflict
	}
	return w.view(), true, nil
}

func acceptWorkerGroup(tx *bolt.Tx, principal string, r WorkerGroupRequest, task backend.Task, id string, now time.Time) (WorkerGroup, error) {
	b := tx.Bucket(workerGroupsBucket)
	if b == nil {
		return WorkerGroup{}, errors.New("missing worker group metadata")
	}
	j, err := acceptJobRow(tx, id, principal, task, r.Target, r.Binding, now)
	if err != nil {
		return WorkerGroup{}, err
	}
	digest, err := taskDigest(task)
	if err != nil {
		return WorkerGroup{}, err
	}
	if err = trackJob(tx, j, retainedJob{Principal: principal, Domain: r.Domain, RequestID: r.RequestID, Storage: backend.RemoteOutput}, requestRecord{Principal: principal, Domain: r.Domain, RequestID: r.RequestID, JobID: j.ID, TaskDigest: digest}); err != nil {
		return WorkerGroup{}, err
	}
	if err = adjust(tx, MetadataBytesQuota, workerGroupReserve(r.Spec.Count), 0); err != nil {
		return WorkerGroup{}, err
	}
	w := &workerState{job: j, status: j.Status, g: workerGroupRecord{Job: j.ID, Principal: principal, Domain: r.Domain, RequestID: r.RequestID, Authority: r.Authority, TaskDigest: hex.EncodeToString(digest[:]), Spec: r.Spec, SpecDigest: workerSpecDigest(r.Spec), Demand: r.Demand, Phase: WorkerGroupQueued, CreatedAt: now}}
	for rank := range r.Spec.Count {
		w.m = append(w.m, workerMemberRecord{Job: j.ID, Rank: rank})
	}
	if err = w.save(tx, now); err != nil {
		return WorkerGroup{}, err
	}
	if _, err = b.NextSequence(); err != nil {
		return WorkerGroup{}, err
	}
	return w.view(), nil
}

// WorkerGroupFor reads an authorized group. A foreign, missing or ordinary
// job returns ErrNotFound.
func (s *Store) WorkerGroupFor(actor auth.Actor, id string) (out WorkerGroup, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(jobsBucket).Get([]byte(id)) == nil || !visible(tx, actor, id) || !workerGroupJob(tx, id) {
			return ErrNotFound
		}
		w, e := loadWorkerState(tx, id)
		if e == nil {
			out = w.view()
		}
		return e
	})
	return
}

// WorkerGroup reads one group for trusted reconciliation.
func (s *Store) WorkerGroup(id string) (out WorkerGroup, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		w, e := loadWorkerState(tx, id)
		if e == nil {
			out = w.view()
		}
		return e
	})
	return
}

// WorkerGroups reads up to limit groups that are not Done, after job
// sequence after, in acceptance order, for trusted reconciliation.
func (s *Store) WorkerGroups(after uint64, limit int) (out []WorkerGroup, err error) {
	if limit <= 0 {
		return nil, errors.New("list limit must be positive")
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(sequenceBucket).Cursor()
		for k, id := c.Seek(sequenceKey(after + 1)); k != nil && len(out) < limit; k, id = c.Next() {
			if !workerGroupJob(tx, string(id)) {
				continue
			}
			w, e := loadWorkerState(tx, string(id))
			if e != nil {
				return e
			}
			if w.g.Phase != WorkerGroupDone {
				out = append(out, w.view())
			}
		}
		return nil
	})
	return
}

// AdmitWorkerGroup admits the Queued group id under budget, the Binding's
// operator-declared gang budget. A canceled or revoked Queued group settles
// as Done instead. Groups on the Binding are admitted in acceptance order
// with no skipping: only the first admissible group is admitted, and only
// when its whole demand fits beside the held reservations. When the head's
// whole demand can never fit budget, it returns an error wrapping
// workeradmission.ErrExceedsBudget and writes nothing, so a corrected budget
// or an explicit cancel resumes the queue. A budget with fewer than 2 ranks,
// or no CPU or memory, fits no group and is refused. The result reports
// whether id was admitted.
func (s *Store) AdmitWorkerGroup(id string, rev uint64, budget workeradmission.Budget, now time.Time) (out WorkerGroup, admitted bool, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		out = w.view()
		if w.g.Phase != WorkerGroupQueued {
			return nil
		}
		if w.job.CancelRequested || w.g.Revoked {
			w.aggregate()
			w.settle()
			if e := w.save(tx, now); e != nil {
				return e
			}
			out = w.view()
			return nil
		}
		if budget.Ranks < 2 || budget.CPUMillis == 0 || budget.MemoryBytes == 0 {
			return errors.New("invalid worker budget")
		}
		held, queue, e := workerAdmissionQueue(tx, w.job.TargetBinding)
		if e != nil {
			return e
		}
		if len(queue) > 0 {
			if e := workeradmission.Fits(budget, queue[0].Demand); e != nil {
				return fmt.Errorf("worker group %s: %w", queue[0].ID, e)
			}
		}
		ids, e := workeradmission.Admit(budget, held, queue)
		if e != nil {
			return e
		}
		// Only the head is admitted. A later group that would fit beside it
		// waits, so durable admission follows acceptance order.
		if len(ids) == 0 || ids[0] != id {
			return nil
		}
		w.g.Phase, w.g.Reserved = WorkerGroupAdmitted, true
		if e := w.save(tx, now); e != nil {
			return e
		}
		out, admitted = w.view(), true
		return nil
	})
	return
}

// workerAdmissionQueue returns the reservations held on binding and its
// admissible queue in acceptance order. Canceled and revoked Queued groups
// wait for their own settlement and never block the queue.
func workerAdmissionQueue(tx *bolt.Tx, binding string) (held []workeradmission.Held, queue []workeradmission.Queued, err error) {
	type queued struct {
		seq uint64
		q   workeradmission.Queued
	}
	var waiting []queued
	c := tx.Bucket(workerGroupsBucket).Cursor()
	for k, v := c.Seek([]byte("g/")); k != nil && bytes.HasPrefix(k, []byte("g/")); k, v = c.Next() {
		g, e := decodeWorkerGroupRecord(v)
		if e != nil {
			return nil, nil, e
		}
		j, e := get(tx, g.Job)
		if e != nil {
			return nil, nil, e
		}
		switch {
		case j.TargetBinding != binding:
		case g.Reserved:
			held = append(held, workeradmission.Held{ID: g.Job, Demand: g.Demand})
		case g.Phase == WorkerGroupQueued && !j.CancelRequested && !g.Revoked:
			waiting = append(waiting, queued{j.Sequence, workeradmission.Queued{ID: g.Job, Demand: g.Demand}})
		}
	}
	sort.Slice(waiting, func(a, b int) bool { return waiting[a].seq < waiting[b].seq })
	for _, w := range waiting {
		queue = append(queue, w.q)
	}
	return held, queue, nil
}

// SettleWorkerGroup recomputes the aggregate status and settles the group:
// a Queued or Admitted group with no effects that is canceled or revoked, or
// an Active group whose members are all settled and whose aggregate is
// terminal, becomes Done and releases its admission reservation.
func (s *Store) SettleWorkerGroup(id string, rev uint64, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		phase, status := w.g.Phase, w.job.Status
		w.aggregate()
		w.settle()
		if w.g.Phase != phase || w.job.Status != status {
			if e := w.save(tx, now); e != nil {
				return e
			}
		}
		out = w.view()
		return nil
	})
	return
}

// IntendWorker durably records the intent of effect e on member rank and
// returns its token. The caller performs the backend call only after it
// returns, at most once per token, and records the outcome with the token.
// Create and Release require an admitted or active group that is not
// canceled, revoked or terminal; Create happens at most once per member, and
// Release only after every member's Create is recorded. Cancel and Remove
// require the member's original Pod, and Remove also stop proof or a member
// that was never released.
func (s *Store) IntendWorker(id string, rev uint64, rank uint8, e WorkerEffect, now time.Time) (tok WorkerToken, out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if e > WorkerRemove {
		return tok, out, errors.New("invalid worker effect")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, err := loadWorkerRevision(tx, id, rev)
		if err != nil {
			return err
		}
		if int(rank) >= len(w.m) {
			return errors.New("worker rank out of range")
		}
		if w.g.Phase == WorkerGroupDone {
			return errors.New("worker group is done")
		}
		m := &w.m[rank]
		x := m.effect(e)
		if x.State == WorkerIntended {
			return errors.New("worker effect already intended")
		}
		switch e {
		case WorkerCreate, WorkerRelease:
			switch {
			case w.g.Phase != WorkerGroupAdmitted && w.g.Phase != WorkerGroupActive:
				return errors.New("worker group is not active")
			case w.job.CancelRequested:
				return errors.New("worker group cancellation requested")
			case w.g.Revoked:
				return errors.New("worker group authority revoked")
			case w.job.Status.State.Terminal():
				return errors.New("worker group is terminal")
			}
			if e == WorkerCreate && m.Create.State != WorkerNone {
				return errors.New("worker member already created")
			}
			if e == WorkerRelease {
				for _, o := range w.m {
					if o.Create.State != WorkerRecorded {
						return errors.New("worker release before every create")
					}
				}
				if m.Release.State != WorkerNone || m.Cancel.State != WorkerNone || m.Remove.State != WorkerNone || m.Removed {
					return errors.New("worker member cannot be released")
				}
			}
		case WorkerCancel:
			if m.Pod.UID == "" {
				return errors.New("worker cancel requires the original pod")
			}
		case WorkerRemove:
			if m.Pod.UID == "" {
				return errors.New("worker removal requires the original pod")
			}
			if !(m.Observed != nil && m.Observed.ExitCode != nil || m.Release.State == WorkerNone) {
				return errors.New("worker removal requires stop proof")
			}
			if m.Removed {
				return errors.New("worker member already removed")
			}
		}
		m.Counter++
		*x = workerEffectRecord{State: WorkerIntended, Seq: m.Counter, Gen: workerGenString(workerGen()), At: now}
		if e == WorkerCancel {
			m.CancelAck = false
		}
		if e == WorkerCreate && w.g.Phase == WorkerGroupAdmitted {
			w.g.Phase = WorkerGroupActive
		}
		w.aggregate()
		if err := w.save(tx, now); err != nil {
			return err
		}
		tok = WorkerToken{Job: id, Rank: rank, Effect: e, Seq: x.Seq, Gen: workerGen()}
		out = w.view()
		return nil
	})
	return
}

// RecordWorkerCreated resolves a Create intent with the original receipt. A
// complete receipt is recorded even with a call error; anything else is
// Unknown, keeps a valid partial receipt, and holds the group.
func (s *Store) RecordWorkerCreated(tok WorkerToken, got backend.WorkerCreated, callErr string, now time.Time) (WorkerGroup, error) {
	return s.recordWorker(tok, WorkerCreate, now, func(w *workerState, m *workerMemberRecord, x *workerEffectRecord) error {
		c := backend.WorkerCreate{Group: workerGroupIdentity(w.job, w.g), Rank: m.Rank}
		if err := backend.ValidateWorkerCreated(c, got, false); err == nil {
			x.State, x.Error = WorkerRecorded, workerText(callErr, workerErrorLimit)
			m.Pod = workerPodRecord{got.Pod.Name, got.Pod.UID}
			return nil
		} else if callErr == "" {
			callErr = err.Error()
		}
		x.State, x.Error = WorkerUnknown, workerError(callErr)
		if backend.ValidateWorkerCreated(c, got, true) == nil {
			m.Pod = workerPodRecord{got.Pod.Name, got.Pod.UID}
		}
		w.g.Phase = WorkerGroupHeld
		return nil
	})
}

// RecordWorkerReleased resolves a Release intent. An acknowledgement is not
// evidence that the member runs.
func (s *Store) RecordWorkerReleased(tok WorkerToken, got backend.WorkerReleased, callErr string, now time.Time) (WorkerGroup, error) {
	return s.recordWorker(tok, WorkerRelease, now, func(w *workerState, m *workerMemberRecord, x *workerEffectRecord) error {
		if got.Pod != m.pod() || callErr != "" {
			x.State, x.Error = WorkerUnknown, workerError(callErr)
			return nil
		}
		x.State, m.ReleaseAck, m.Revision = WorkerRecorded, got.Acknowledged, workerText(got.Revision, workerRevisionLimit)
		return nil
	})
}

// RecordWorkerCanceled resolves a Cancel intent. An acknowledgement is not
// evidence that the member stopped.
func (s *Store) RecordWorkerCanceled(tok WorkerToken, got backend.WorkerCanceled, callErr string, now time.Time) (WorkerGroup, error) {
	return s.recordWorker(tok, WorkerCancel, now, func(w *workerState, m *workerMemberRecord, x *workerEffectRecord) error {
		if got.Pod != m.pod() || callErr != "" {
			x.State, x.Error = WorkerUnknown, workerError(callErr)
			return nil
		}
		x.State, m.CancelAck = WorkerRecorded, got.Acknowledged
		return nil
	})
}

// RecordWorkerRemoved resolves a Remove intent. Removed, once true, is never
// cleared.
func (s *Store) RecordWorkerRemoved(tok WorkerToken, got backend.WorkerRemoval, callErr string, now time.Time) (WorkerGroup, error) {
	return s.recordWorker(tok, WorkerRemove, now, func(w *workerState, m *workerMemberRecord, x *workerEffectRecord) error {
		ref := backend.WorkerReference{Group: workerGroupIdentity(w.job, w.g), Rank: m.Rank, Pod: m.pod()}
		if backend.ValidateWorkerRemoval(ref, got) != nil || got.Pod != m.pod() || callErr != "" {
			x.State, x.Error = WorkerUnknown, workerError(callErr)
			return nil
		}
		x.State, m.Removed = WorkerRecorded, m.Removed || got.PodRemoved
		return nil
	})
}

// RecordWorkerLost resolves an intent of another process generation whose
// original call can no longer return. A lost Create holds the group. An
// intent of this process is refused, since its call may still be in flight.
func (s *Store) RecordWorkerLost(tok WorkerToken, now time.Time) (WorkerGroup, error) {
	return s.recordWorker(tok, tok.Effect, now, func(w *workerState, m *workerMemberRecord, x *workerEffectRecord) error {
		if x.Gen == workerGenString(workerGen()) {
			return errWorkerIntentLive
		}
		x.State, x.Error = WorkerUnknown, "lost"
		if tok.Effect == WorkerCreate {
			w.g.Phase = WorkerGroupHeld
		}
		return nil
	})
}

// recordWorker resolves the current intent named by tok with fn, then
// recomputes the aggregate and settles. A receipt is recorded whatever the
// group revision, phase, revocation or cancellation.
func (s *Store) recordWorker(tok WorkerToken, e WorkerEffect, now time.Time, fn func(*workerState, *workerMemberRecord, *workerEffectRecord) error) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if tok.Effect != e {
		return out, errors.New("worker token names another effect")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, err := loadWorkerState(tx, tok.Job)
		if err != nil {
			return err
		}
		if int(tok.Rank) >= len(w.m) || e > WorkerRemove {
			return ErrWorkerGroupStale
		}
		m := &w.m[tok.Rank]
		x := m.effect(e)
		if x.State != WorkerIntended || x.Seq != tok.Seq || x.Gen != workerGenString(tok.Gen) {
			return ErrWorkerGroupStale
		}
		if err := fn(w, m, x); err != nil {
			return err
		}
		w.aggregate()
		w.settle()
		if err := w.save(tx, now); err != nil {
			return err
		}
		out = w.view()
		return nil
	})
	return
}

// RecordWorkerObservation records one observation of member rank's original
// Pod. A native exit receipt sets the rank's exit code, derived from the
// receipt, and freezes the observation. A terminal state without one is
// stored as Unknown. A rank that ran never reports Queued or Starting again.
func (s *Store) RecordWorkerObservation(id string, rank uint8, o backend.WorkerObservation, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, err := loadWorkerState(tx, id)
		if err != nil {
			return err
		}
		if w.g.Phase == WorkerGroupDone {
			return errors.New("worker group is done")
		}
		if int(rank) >= len(w.m) {
			return errors.New("worker rank out of range")
		}
		m := &w.m[rank]
		if m.Pod.UID == "" || o.Pod != m.pod() || o.Status.Instance != "" && o.Status.Instance != m.Pod.UID {
			return errors.New("worker observation identity mismatch")
		}
		switch o.Status.State {
		case backend.Queued, backend.Starting, backend.Running, backend.Succeeded, backend.Failed, backend.Canceled, backend.Unknown:
		default:
			return errors.New("invalid worker observation state")
		}
		var prev workerObservedRecord
		if m.Observed != nil {
			prev = *m.Observed
		}
		next := workerObservedRecord{State: backend.Unknown, Gated: o.Gated, Container: prev.Container, Started: prev.Started, Node: workerText(o.Node, workerNodeLimit), At: now}
		switch {
		case prev.ExitCode != nil:
			next = prev
			next.At = now
		case o.Exit != nil:
			x := *o.Exit
			if o.Status.Termination != backend.StoppedWithExitCode || !workerExitValid(x) || prev.Container != "" && x.ContainerID != prev.Container {
				return errors.New("invalid worker exit receipt")
			}
			code := x.ExitCode
			next.State = backend.Failed
			if code == 0 {
				next.State = backend.Succeeded
			}
			next.Container, next.Started, next.Finished, next.ExitCode = x.ContainerID, x.StartedAt.UTC(), x.FinishedAt.UTC(), &code
		case o.Status.State == backend.Succeeded || o.Status.State == backend.Failed || o.Status.State == backend.Canceled:
			// Without a native exit receipt the rank has no stop proof.
		case o.Status.State == backend.Running:
			if o.Start == nil || o.Start.ContainerID == "" || !validInstance(o.Start.ContainerID) || !workerMoment(o.Start.StartedAt) || prev.Container != "" && o.Start.ContainerID != prev.Container {
				return errors.New("invalid worker start receipt")
			}
			next.State, next.Container, next.Started = backend.Running, o.Start.ContainerID, o.Start.StartedAt.UTC()
		case o.Status.State == backend.Queued || o.Status.State == backend.Starting:
			if prev.Container == "" {
				next.State = o.Status.State
			}
		}
		m.Observed = &next
		w.aggregate()
		w.settle()
		if err := w.save(tx, now); err != nil {
			return err
		}
		out = w.view()
		return nil
	})
	return
}

// RevokeWorkerGroup permanently records that the group's delegation was
// refused. Only an error wrapping auth.ErrUnauthenticated or
// auth.ErrPermissionDenied revokes; any other cause, including
// auth.ErrUnavailable, is refused with no write. Revocation blocks Create and
// Release, but not observation, Cancel or Remove.
func (s *Store) RevokeWorkerGroup(id string, rev uint64, cause error, now time.Time) (out WorkerGroup, err error) {
	if !errors.Is(cause, auth.ErrUnauthenticated) && !errors.Is(cause, auth.ErrPermissionDenied) {
		return out, errors.New("worker group revocation requires an authentication refusal")
	}
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Phase != WorkerGroupDone {
			w.g.Revoked = true
			w.aggregate()
			w.settle()
			if e := w.save(tx, now); e != nil {
				return e
			}
		}
		out = w.view()
		return nil
	})
	return
}

// workerState is one group's rows inside a transaction. status is the job
// status as read.
type workerState struct {
	job    backend.Job
	status backend.Status
	g      workerGroupRecord
	m      []workerMemberRecord
}

func loadWorkerState(tx *bolt.Tx, id string) (*workerState, error) {
	b := tx.Bucket(workerGroupsBucket)
	if b == nil || b.Get(workerGroupKey(id)) == nil {
		return nil, ErrNotFound
	}
	g, err := decodeWorkerGroupRecord(b.Get(workerGroupKey(id)))
	if err != nil {
		return nil, err
	}
	j, err := get(tx, id)
	if err != nil {
		return nil, err
	}
	w := &workerState{job: j, status: j.Status, g: g}
	for rank := range g.Spec.Count {
		m, err := decodeWorkerMemberRecord(b.Get(workerMemberKey(id, rank)))
		if err != nil {
			return nil, err
		}
		w.m = append(w.m, m)
	}
	return w, nil
}

func loadWorkerRevision(tx *bolt.Tx, id string, rev uint64) (*workerState, error) {
	w, err := loadWorkerState(tx, id)
	if err == nil && w.g.Revision != rev {
		err = ErrWorkerGroupStale
	}
	return w, err
}

// save writes the group's rows with the next revision, and the job row if
// its status changed.
func (w *workerState) save(tx *bolt.Tx, now time.Time) error {
	w.g.Revision++
	if w.job.Status != w.status {
		if now.After(w.job.UpdatedAt) {
			w.job.UpdatedAt = now
		}
		if err := putTrackedJob(tx, w.job); err != nil {
			return err
		}
		w.status = w.job.Status
	}
	b := tx.Bucket(workerGroupsBucket)
	data, err := encodeWorkerGroupRecord(w.g)
	if err != nil {
		return err
	}
	if err := b.Put(workerGroupKey(w.g.Job), data); err != nil {
		return err
	}
	for _, m := range w.m {
		data, err := encodeWorkerMemberRecord(m)
		if err != nil {
			return err
		}
		if err := b.Put(workerMemberKey(m.Job, m.Rank), data); err != nil {
			return err
		}
	}
	return nil
}

// aggregate recomputes a nonterminal job status from cancellation,
// revocation and the members. The first matching rule wins: every rank
// exited 0; canceled; a rank exited nonzero; revoked; a rank was seen
// running; a Create was intended; queued. A terminal status never changes.
func (w *workerState) aggregate() {
	if w.job.Status.State.Terminal() {
		return
	}
	all, started, creating := true, false, false
	failed := -1
	var code int32
	for i, m := range w.m {
		o := m.Observed
		exited := o != nil && o.ExitCode != nil
		if !exited || *o.ExitCode != 0 {
			all = false
		}
		if exited && *o.ExitCode != 0 && failed < 0 {
			failed, code = i, *o.ExitCode
		}
		if o != nil && !o.Started.IsZero() {
			started = true
		}
		if m.Create.State != WorkerNone {
			creating = true
		}
	}
	var s backend.Status
	switch {
	case all:
		s.State = backend.Succeeded
	case w.job.CancelRequested:
		s = backend.Status{State: backend.Canceled, Reason: "canceled"}
	case failed >= 0:
		s = backend.Status{State: backend.Failed, Reason: fmt.Sprintf("worker rank %d exited with code %d", failed, code)}
	case w.g.Revoked:
		s = backend.Status{State: backend.Failed, Reason: "worker group authority lost"}
	case started:
		s.State = backend.Running
	case creating:
		s.State = backend.Starting
	default:
		s.State = backend.Queued
	}
	w.job.Status = s
}

// settle applies the two settling rules. A Held group never settles.
func (w *workerState) settle() {
	terminal := w.job.Status.State.Terminal()
	switch w.g.Phase {
	case WorkerGroupQueued, WorkerGroupAdmitted:
		for _, m := range w.m {
			if m.Counter != 0 {
				return
			}
		}
		if terminal && (w.job.CancelRequested || w.g.Revoked) {
			w.g.Phase, w.g.Reserved = WorkerGroupDone, false
		}
	case WorkerGroupActive:
		for _, m := range w.m {
			if !m.settled() {
				return
			}
		}
		if terminal {
			w.g.Phase, w.g.Reserved = WorkerGroupDone, false
		}
	}
}

func (w *workerState) view() WorkerGroup {
	j := w.job
	j.Task = backend.CloneTask(j.Task)
	if j.Status.ExitCode != nil {
		code := *j.Status.ExitCode
		j.Status.ExitCode = &code
	}
	out := WorkerGroup{Job: j, Group: workerGroupIdentity(w.job, w.g), Principal: w.g.Principal, Domain: w.g.Domain, RequestID: w.g.RequestID, Authority: w.g.Authority, Demand: w.g.Demand, Phase: w.g.Phase, Reserved: w.g.Reserved, Revoked: w.g.Revoked, Revision: w.g.Revision}
	out.Stopping = w.g.Phase == WorkerGroupActive && (j.CancelRequested || w.g.Revoked || j.Status.State.Terminal())
	for _, m := range w.m {
		v := WorkerMember{Rank: m.Rank, Counter: m.Counter, Create: workerEffectView(m.Create), Release: workerEffectView(m.Release), Cancel: workerEffectView(m.Cancel), Remove: workerEffectView(m.Remove), Pod: m.pod(), ReleaseAck: m.ReleaseAck, Revision: m.Revision, CancelAck: m.CancelAck, Removed: m.Removed}
		if o := m.Observed; o != nil {
			v.Observed = &WorkerObserved{State: o.State, Gated: o.Gated, Container: o.Container, Started: o.Started, Finished: o.Finished, Node: o.Node, At: o.At}
			if o.ExitCode != nil {
				code := *o.ExitCode
				v.Observed.ExitCode = &code
			}
		}
		out.Members = append(out.Members, v)
	}
	return out
}

func workerEffectView(e workerEffectRecord) WorkerEffectState {
	out := WorkerEffectState{State: e.State, Seq: e.Seq, At: e.At, Error: e.Error}
	g, _ := hex.DecodeString(e.Gen)
	copy(out.Gen[:], g)
	return out
}

func workerGroupsLive(tx *bolt.Tx) (n int, err error) {
	c := tx.Bucket(workerGroupsBucket).Cursor()
	for k, v := c.Seek([]byte("g/")); k != nil && bytes.HasPrefix(k, []byte("g/")); k, v = c.Next() {
		g, e := decodeWorkerGroupRecord(v)
		if e != nil {
			return 0, e
		}
		if g.Phase != WorkerGroupDone {
			n++
		}
	}
	return n, nil
}

// workerNow returns now in UTC, refusing a zero time or one outside years
// 1..9999.
func workerNow(now time.Time) (time.Time, error) {
	if !workerMoment(now) {
		return now, errors.New("invalid worker time")
	}
	return now.UTC(), nil
}

func workerMoment(t time.Time) bool {
	return !t.IsZero() && t.UTC().Year() >= 1 && t.UTC().Year() <= 9999
}

func workerExitValid(x backend.ContainerExit) bool {
	return x.ContainerID != "" && validInstance(x.ContainerID) && x.ExitCode >= 0 && x.ExitCode <= 255 && workerMoment(x.StartedAt) && workerMoment(x.FinishedAt) && !x.FinishedAt.Before(x.StartedAt)
}

// workerError is the sanitized error of an Unknown outcome, never empty.
func workerError(s string) string {
	if s = workerText(s, workerErrorLimit); s == "" {
		s = "unknown"
	}
	return s
}
