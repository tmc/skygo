package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/workeradmission"
	bolt "go.etcd.io/bbolt"
)

// WorkerBarrierDecision is the immutable whole-group main-start decision.
type WorkerBarrierDecision uint8

const (
	WorkerBarrierUndecided WorkerBarrierDecision = iota
	WorkerBarrierStart
	WorkerBarrierAbort
)

// WorkerResolveClass records the outcome of looking for an unknown Create.
type WorkerResolveClass uint8

const (
	WorkerResolveNone WorkerResolveClass = iota
	WorkerResolveClean
	WorkerResolveUncertain
	WorkerResolveInterrupted
	WorkerResolveMismatch
)

// WorkerResolveOutcome keeps typed lookup evidence, not a serialized error.
// Refused means Control rejected a receipt; it never proves resource absence.
// Callers leave At zero; Store stamps the accepted operation time.
type WorkerResolveOutcome struct {
	// Unsettled latches the original resolve call's local owner.
	Unsettled bool
	Class     WorkerResolveClass
	Refused   bool
	At        time.Time
}

// WorkerBarrierGroup is the versioned barrier extension of a worker group.
// A nil extension denotes the unchanged legacy worker protocol.
type WorkerBarrierGroup struct {
	Version   uint8
	Decision  WorkerBarrierDecision
	DecidedAt time.Time
}

// WorkerBarrierMember is one member's durable main-start and cleanup evidence.
// Dispatch flags never follow merely from recording a removal intent.
type WorkerBarrierMember struct {
	// LocalUnsettled is a permanent hold for an original local owner.
	LocalUnsettled                                                bool
	Version                                                       uint8
	Service                                                       backend.WorkerResource
	Start, Recover                                                WorkerEffectState
	StartToken                                                    [32]byte
	StartAck                                                      bool
	StartRevision                                                 string
	Observation                                                   *backend.WorkerBarrier
	ObservedAt                                                    time.Time
	Stop                                                          backend.WorkerStop
	StopAt                                                        time.Time
	RemoveHadPod, RecoverHadPod, PodRemoveSent, ServiceRemoveSent bool
	ServiceRemoved, CreateRefused                                 bool
	Resolve                                                       WorkerResolveOutcome
	Vanished                                                      bool
}

// WorkerBarrierReceipt holds the original return of one member effect.
// Only the field for the token's effect may be nonzero; Refused is Create-only
// and requires the original backend.ErrWorkerCreateRefused witness.
type WorkerBarrierReceipt struct {
	// Unsettled reports a typed original local-owner failure, never error text.
	Unsettled bool
	Created   backend.WorkerCreated
	Refused   bool
	Released  backend.WorkerReleased
	Started   backend.WorkerStarted
	Canceled  backend.WorkerCanceled
	Removed   backend.WorkerRemoval
}

// LookupWorkerGroup reads an authorized canonical request before placement or
// delegation. Only existing monotonic domain closure fences may be written.
func (s *Store) LookupWorkerGroup(actor auth.Actor, domain RequestDomain, requestID string, task backend.Task, spec backend.WorkerSpec) (out WorkerGroup, found bool, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if !validRequestDomain(domain) || !validRetainedRequestID(requestID) || !workerBarrierSpecValid(spec) || !workerTaskValid(task) {
		return out, false, ErrConflict
	}
	if _, err = backend.TaskBytes(task); err != nil {
		return
	}
	var refusal error
	err = s.db.Update(func(tx *bolt.Tx) error {
		old := bytes.Clone(retentionRoot(tx).Get(retentionStateKey))
		_, result, e := gateDomain(tx, domain, s.now().UTC())
		if e != nil {
			return e
		}
		refusal = result
		if refusal == nil {
			out, found, refusal = lookupWorkerGroupRecord(tx, actor, domain, requestID, task, spec)
		}
		current := retentionRoot(tx).Get(retentionStateKey)
		if bytes.Equal(old, current) {
			return errArtifactBeginNoChange
		}
		if uint64(len(retentionStateKey)+len(old)+len(current)) > maxChangeBytes {
			return errors.New("worker barrier domain mutation exceeds byte limit")
		}
		return nil
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	if err == nil {
		err = refusal
	}
	if err != nil {
		out = WorkerGroup{}
	}
	return
}

// SubmitWorkerBarrierGroup accepts the versioned barrier protocol in the same
// request namespace and FIFO queue as legacy worker groups. A legacy replay is
// returned unchanged; no existing row is converted to the barrier protocol.
func (s *Store) SubmitWorkerBarrierGroup(actor auth.Actor, r WorkerGroupRequest) (WorkerGroup, error) {
	if err := s.authorize(actor, auth.Mutate); err != nil {
		return WorkerGroup{}, err
	}
	if previous, found, err := s.LookupWorkerGroup(actor, r.Domain, r.RequestID, r.Task, r.Spec); err != nil || found {
		return previous, err
	}
	raw, encodeErr := backend.TaskBytes(r.Task)
	if encodeErr != nil {
		return WorkerGroup{}, encodeErr
	}
	// Reserve future progress as well as this admission. Two maximal worker
	// images and two job images (canonical task plus bounded job overhead),
	// with all bounded request/quota/retained writes, must fit one mutation.
	// Every accepted job ID is 32 bytes; measure the actual serialized key
	// layout instead of duplicating its prefix and rank widths.
	var identity [32]byte
	groupKeyBytes := len(workerGroupKey(string(identity[:])))
	memberKeyBytes := len(workerMemberKey(string(identity[:]), 0))
	rowBytes := uint64(2*(workerBarrierGroupLimit+groupKeyBytes)) + uint64(2*int(r.Spec.Count)*(workerBarrierMemberLimit+memberKeyBytes))
	if rowBytes+2*uint64(len(raw))+64<<10 > maxChangeBytes {
		return WorkerGroup{}, errors.New("worker task cannot fit future barrier mutations")
	}
	d, err := workerBarrierDemand(r.Task, r.Spec)
	if err != nil {
		return WorkerGroup{}, err
	}
	if d != r.Demand {
		return WorkerGroup{}, errors.New("worker demand differs from task")
	}
	return s.submitWorkerGroup(actor, r, true)
}

func workerBarrierDemand(t backend.Task, spec backend.WorkerSpec) (workeradmission.Demand, error) {
	if !workerBarrierSpecValid(spec) || t.CPUMillis <= 0 || t.MemoryBytes <= 0 || t.CPULimitMillis < 0 || t.MemoryLimitBytes < 0 || t.AcceleratorCount < 0 {
		return workeradmission.Demand{}, errors.New("invalid worker task resources")
	}
	return workeradmission.Demand{Ranks: uint32(spec.Count), CPUMillis: uint64(max(t.CPUMillis, t.CPULimitMillis)), MemoryBytes: uint64(max(t.MemoryBytes, t.MemoryLimitBytes)), Accelerators: uint32(t.AcceleratorCount)}, nil
}

func (w *workerState) barrierRef(rank uint8, removal bool) backend.WorkerReference {
	m := w.m[rank]
	r := backend.WorkerReference{Group: workerGroupIdentity(w.job, w.g), Rank: rank, Pod: m.pod()}
	if w.g.Spec.Mode == backend.WorkerTCP {
		b := w.m[0].Barrier
		r.Service = backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}
	}
	if removal {
		if !m.Barrier.RemoveHadPod {
			r.Pod = backend.WorkerResource{}
		}

	}
	return r
}

func (w *workerState) barrierEffectRef(rank uint8, e WorkerEffect) backend.WorkerReference {
	r := w.barrierRef(rank, true)
	if e == WorkerRecover {
		if w.m[rank].Barrier.RecoverHadPod {
			r.Pod = w.m[rank].pod()
		} else {
			r.Pod = backend.WorkerResource{}
		}
	}
	return r
}

// IntendWorkerBarrier commits an original effect before its backend call.
// A token identifies one sequence and process generation; intent is never proof
// that an HTTP mutation was dispatched or that a member started or stopped.
func (s *Store) IntendWorkerBarrier(id string, rev uint64, rank uint8, e WorkerEffect, now time.Time) (tok WorkerToken, out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if e > WorkerRecover {
		return tok, out, ErrWorkerGroupStale
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, x := loadWorkerRevision(tx, id, rev)
		if x != nil {
			return x
		}
		if w.g.Barrier == nil || int(rank) >= len(w.m) || w.g.Phase == WorkerGroupDone {
			return ErrWorkerGroupStale
		}
		if w.barrierLocalUnsettled() {
			return errors.New("worker group has an unsettled original local owner")
		}
		m := &w.m[rank]
		b := m.Barrier
		effect := m.effect(e)
		if effect.State == WorkerIntended {
			return ErrWorkerGroupStale
		}
		for _, original := range m.effects() {
			if original.State == WorkerIntended {
				return errors.New("worker original effect is still in flight")
			}
		}
		active := w.g.Phase == WorkerGroupAdmitted || w.g.Phase == WorkerGroupActive
		switch e {
		case WorkerCreate, WorkerRelease, WorkerStart:
			for _, other := range w.m {
				if other.Barrier.Stop != backend.WorkerStopUnproven || other.Cancel.State != WorkerNone || other.Remove.State != WorkerNone || other.Removed {
					return errors.New("worker group has begun stopping")
				}
			}
			if !active || w.job.CancelRequested || w.g.Revoked || w.job.Status.State.Terminal() || w.g.Barrier.Decision == WorkerBarrierAbort {
				return errors.New("worker group cannot start effects")
			}
			if b.Resolve.Class != WorkerResolveNone || b.Vanished {
				return errors.New("worker member is cleanup only")
			}
			switch e {
			case WorkerCreate:
				if m.Create.State != WorkerNone || w.g.Barrier.Decision != WorkerBarrierUndecided {
					return ErrWorkerGroupStale
				}
			case WorkerRelease:
				if m.Release.State != WorkerNone || w.g.Barrier.Decision != WorkerBarrierUndecided {
					return ErrWorkerGroupStale
				}
				for _, o := range w.m {
					if o.Create.State != WorkerRecorded || o.Barrier.CreateRefused {
						return errors.New("worker release before every create")
					}
				}
			case WorkerStart:
				if w.g.Barrier.Decision != WorkerBarrierStart || b.StartToken == "" || (b.Start.State != WorkerNone && b.Start.State != WorkerUnknown) || b.Observation == nil || b.Stop != backend.WorkerStopUnproven {
					return errors.New("worker start lacks barrier decision")
				}
			}
		case WorkerCancel:
			if m.Pod.UID == "" || m.Removed {
				return errors.New("worker cancel requires original pod")
			}
		case WorkerRemove:
			if m.Removed && (rank != 0 || w.g.Spec.Mode != backend.WorkerTCP || b.ServiceRemoved) {
				return errors.New("worker member already removed")
			}
			if m.Pod.UID == "" && b.Service.UID == "" {
				return errors.New("worker removal lacks original resource")
			}
			if m.Pod.UID == "" && !b.CreateRefused {
				return errors.New("worker service removal has an unresolved original pod")
			}
			if m.Pod.UID != "" && !workerBarrierRecordedStop(b) {
				return errors.New("worker removal lacks recorded positive stop evidence")
			}
			if effect.State != WorkerNone && effect.State != WorkerRecorded {
				return errors.New("unknown original removal must remain held")
			}
			// Capture the exact original effect reference before any dispatch.
			b.RemoveHadPod = m.Pod.UID != "" && !m.Removed
			if rank == 0 && w.g.Spec.Mode == backend.WorkerTCP && b.Service.UID != "" && !b.ServiceRemoved {
				for i, o := range w.m {
					if i != 0 && !workerBarrierMemberSettled(o) {
						return errors.New("worker service removal precedes other ranks")
					}
				}
			}
		case WorkerRecover:
			if m.Pod.UID == "" && !b.CreateRefused {
				return errors.New("worker recovery has an unresolved original pod")
			}
			if b.Recover.State != WorkerNone || m.Remove.State != WorkerUnknown {
				return errors.New("worker recovery lacks unknown removal")
			}
			b.RecoverHadPod = b.RemoveHadPod && !m.Removed
			ref := w.barrierEffectRef(rank, WorkerRecover)
			sent := (ref.Pod.UID == "" || b.PodRemoveSent) && (ref.Service.UID == "" || rank != 0 || b.ServiceRemoveSent)
			evidence := workerBarrierRecoveryEvidence(*m)
			if backend.ValidateWorkerRecovery(backend.WorkerRecovery{Ref: ref, Evidence: evidence, RemoveSent: sent}) != nil {
				return errors.New("worker recovery lacks sent removal and stop evidence")
			}
		}
		if m.Counter == ^uint64(0) {
			return errors.New("worker effect sequence exhausted")
		}
		m.Counter++
		*effect = workerEffectRecord{State: WorkerIntended, Seq: m.Counter, Gen: workerGenString(workerGen()), At: now}
		if e == WorkerCreate && w.g.Phase == WorkerGroupAdmitted {
			w.g.Phase = WorkerGroupActive
		}
		if e == WorkerCancel {
			m.CancelAck = false
		}
		if e == WorkerStart {
			b.StartAck = false
			b.StartRevision = ""
		}
		w.aggregate()
		if x := w.save(tx, now); x != nil {
			return x
		}
		tok = WorkerToken{Job: id, Rank: rank, Effect: e, Seq: effect.Seq, Gen: workerGen()}
		out = w.view()
		return nil
	})
	return
}

// DecideWorkerBarrier atomically records one immutable decision and, for Start,
// every member's token. Start requires one prepared observation of every exact
// original resource, with no prior stop, deletion or token delivery.
func (s *Store) DecideWorkerBarrier(id string, rev uint64, d WorkerBarrierDecision, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if d != WorkerBarrierStart && d != WorkerBarrierAbort {
		return out, errors.New("invalid worker barrier decision")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil {
			return ErrWorkerGroupStale
		}
		b := w.g.Barrier
		if b.Decision != WorkerBarrierUndecided {
			if b.Decision != d {
				return ErrConflict
			}
			out = w.view()
			return errArtifactBeginNoChange
		}
		if d == WorkerBarrierStart {
			if w.barrierLocalUnsettled() {
				return errors.New("worker group has an unsettled original local owner")
			}
			if w.g.Phase != WorkerGroupActive || w.job.CancelRequested || w.g.Revoked || w.job.Status.State.Terminal() {
				return errors.New("worker group cannot decide start")
			}
			var refs []backend.WorkerReference
			var obs []backend.WorkerBarrier
			for i, m := range w.m {
				if m.Cancel.State != WorkerNone || m.Remove.State != WorkerNone || m.Barrier.Recover.State != WorkerNone || m.Removed || m.Barrier.ServiceRemoved {
					return errors.New("worker barrier follows cleanup history")
				}
				for _, original := range m.effects() {
					if original.State == WorkerIntended {
						return errors.New("worker barrier has an original effect in flight")
					}
				}
				if m.Create.State != WorkerRecorded || !m.ReleaseAck || m.Release.State != WorkerRecorded || m.Barrier.Observation == nil || m.Barrier.StartToken != "" || m.Barrier.Resolve.Class != WorkerResolveNone || m.Barrier.Vanished {
					return errors.New("worker barrier lacks every prepared member")
				}
				refs = append(refs, w.barrierRef(uint8(i), false))
				obs = append(obs, *workerBarrierObservation(m))
			}
			if w.g.Spec.Mode == backend.WorkerTCP && (obs[0].Rendezvous.Host == "" || obs[0].Rendezvous.Port != 29500) {
				return errors.New("worker rendezvous address unavailable")
			}
			if e := backend.WorkerBarrierReady(refs, obs); e != nil {
				return e
			}
			for i := range w.m {
				tok, e := backend.WorkerStartToken(refs[i])
				if e != nil {
					return e
				}
				w.m[i].Barrier.StartToken = hex.EncodeToString(tok[:])
			}
		}
		b.Decision, b.DecidedAt = d, now
		w.aggregate()
		w.settle()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	return
}

// RecordWorkerBarrierEffect records the original return even after cancellation
// or a newer group revision. An exact retry is read-only; a differing result for
// the same token is refused. No recorded result authorizes repeating its call.
func (s *Store) RecordWorkerBarrierEffect(tok WorkerToken, r WorkerBarrierReceipt, callErr string, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if tok.Effect > WorkerRecover {
		return out, ErrWorkerGroupStale
	}
	if r.Unsettled && callErr == "" {
		return out, errors.New("unsettled worker result requires its original cause")
	}
	if !workerReceiptShape(tok.Effect, r) {
		return out, errors.New("worker receipt names another effect")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerState(tx, tok.Job)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(tok.Rank) >= len(w.m) {
			return ErrWorkerGroupStale
		}
		m := &w.m[tok.Rank]
		x := m.effect(tok.Effect)
		if x.State == WorkerNone || x.Seq != tok.Seq || x.Gen != workerGenString(tok.Gen) {
			return ErrWorkerGroupStale
		}
		before, _ := json.Marshal(m)
		intended := x.State == WorkerIntended
		if e := w.applyBarrierReceipt(m, x, tok.Effect, r, callErr); e != nil {
			return e
		}
		if !intended {
			after, _ := json.Marshal(m)
			if !bytes.Equal(before, after) {
				return ErrWorkerGroupStale
			}
			out = w.view()
			return errArtifactBeginNoChange
		}
		if tok.Effect == WorkerCreate && x.State == WorkerUnknown {
			w.g.Phase = WorkerGroupHeld
		}
		w.aggregate()
		w.settle()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	return
}

func workerReceiptShape(e WorkerEffect, r WorkerBarrierReceipt) bool {
	want := WorkerBarrierReceipt{Unsettled: r.Unsettled}
	switch e {
	case WorkerCreate:
		want.Created = r.Created
		want.Refused = r.Refused
	case WorkerRelease:
		want.Released = r.Released
	case WorkerStart:
		want.Started = r.Started
	case WorkerCancel:
		want.Canceled = r.Canceled
	case WorkerRemove, WorkerRecover:
		want.Removed = r.Removed
	default:
		return false
	}
	return reflect.DeepEqual(want, r)
}

func (w *workerState) applyBarrierReceipt(m *workerMemberRecord, x *workerEffectRecord, e WorkerEffect, r WorkerBarrierReceipt, callErr string) error {
	b := m.Barrier
	b.LocalUnsettled = b.LocalUnsettled || r.Unsettled
	x.Unsettled = r.Unsettled
	x.State, x.Error = WorkerRecorded, workerText(callErr, workerErrorLimit)
	unknown := func() { x.State, x.Error = WorkerUnknown, workerError(callErr) }
	switch e {
	case WorkerCreate:
		c := backend.WorkerCreate{Group: workerGroupIdentity(w.job, w.g), Rank: m.Rank}
		if err := backend.ValidateWorkerCreated(c, r.Created, true); err != nil {
			return err
		}
		if m.Pod.UID != "" && m.pod() != r.Created.Pod || b.Service.UID != "" && (backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}) != r.Created.Service {
			return ErrWorkerGroupStale
		}
		m.Pod = workerPodRecord{r.Created.Pod.Name, r.Created.Pod.UID}
		b.Service = workerPodRecord{r.Created.Service.Name, r.Created.Service.UID}
		b.CreateRefused = r.Refused
		if backend.ValidateWorkerCreated(c, r.Created, false) != nil {
			unknown()
		}
	case WorkerRelease:
		if r.Released.Pod != m.pod() {
			if r.Released == (backend.WorkerReleased{}) && callErr != "" {
				unknown()
				return nil
			}
			return errors.New("worker release identity mismatch")
		}
		if r.Released.Revision != workerText(r.Released.Revision, workerRevisionLimit) {
			return errors.New("invalid worker release revision")
		}
		m.ReleaseAck, m.Revision = r.Released.Acknowledged, r.Released.Revision
		if callErr != "" && !r.Released.Acknowledged {
			m.Revision = ""
			unknown()
		}
	case WorkerStart:
		if r.Started.Pod != m.pod() {
			if r.Started == (backend.WorkerStarted{}) && callErr != "" {
				unknown()
				return nil
			}
			return errors.New("worker start identity mismatch")
		}
		if r.Started.Revision != workerText(r.Started.Revision, workerRevisionLimit) {
			return errors.New("invalid worker start revision")
		}
		b.StartAck = r.Started.Acknowledged
		if b.StartAck {
			b.StartRevision = r.Started.Revision
		} else {
			b.StartRevision = ""
		}
		if callErr != "" && !r.Started.Acknowledged {
			b.StartRevision = ""
			unknown()
		}
	case WorkerCancel:
		if r.Canceled.Pod != m.pod() {
			if r.Canceled == (backend.WorkerCanceled{}) && callErr != "" {
				unknown()
				return nil
			}
			return errors.New("worker cancel identity mismatch")
		}
		m.CancelAck = r.Canceled.Acknowledged
		if callErr != "" && !r.Canceled.Acknowledged {
			unknown()
		}
	case WorkerRemove, WorkerRecover:
		if r.Removed == (backend.WorkerRemoval{}) && callErr != "" {
			unknown()
			return nil
		}
		ref := w.barrierEffectRef(m.Rank, e)
		if err := backend.ValidateWorkerRemoval(ref, r.Removed); err != nil {
			return err
		}
		if r.Removed.ServiceRemoved && ref.Pod.UID != "" && !m.Removed && !r.Removed.PodRemoved {
			return errors.New("worker service removal precedes original pod")
		}
		m.Removed = m.Removed || r.Removed.PodRemoved
		b.ServiceRemoved = b.ServiceRemoved || r.Removed.ServiceRemoved
		b.PodRemoveSent = b.PodRemoveSent || r.Removed.PodDispatched
		b.ServiceRemoveSent = b.ServiceRemoveSent || r.Removed.ServiceDispatched
		if callErr != "" || ref.Pod.UID != "" && !r.Removed.PodRemoved || m.Rank == 0 && ref.Service.UID != "" && !r.Removed.ServiceRemoved {
			unknown()
		}
	}
	return nil
}

// RecordWorkerBarrier records one observation of the exact original Pod.
// Stop and native exit evidence are monotonic. Token presence is not execution.
func (s *Store) RecordWorkerBarrier(id string, rev uint64, rank uint8, o backend.WorkerBarrier, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(rank) >= len(w.m) || w.g.Phase == WorkerGroupDone {
			return ErrWorkerGroupStale
		}
		m := &w.m[rank]
		b := m.Barrier
		ref := w.barrierRef(rank, false)
		if e := backend.ValidateWorkerBarrier(ref, o); e != nil {
			return e
		}
		if b.Stop != backend.WorkerStopUnproven && b.Stop != o.Stop {
			return errors.New("worker stop evidence cannot disappear")
		}
		if prev := b.Observation; prev != nil {
			if prev.Node != "" && prev.Node != o.Node {
				return errors.New("worker bound node cannot change")
			}
			if prev.Started && !o.Started {
				return errors.New("worker observed token cannot disappear")
			}
		}
		if o.Stop == backend.WorkerNeverBound && m.Observed != nil && m.Observed.Container != "" {
			return errors.New("running worker cannot become never bound")
		}
		if o.Started && b.StartToken == "" {
			return errors.New("worker token observed before decision")
		}
		if o.Revision != workerText(o.Revision, workerRevisionLimit) || o.Node != "" && !workerDNS(o.Node, workerNodeLimit) || o.Rendezvous.Host != "" && !workerDNS(o.Rendezvous.Host, 253) || o.Rendezvous.Port != 0 && o.Rendezvous.Port != 29500 {
			return errors.New("invalid worker barrier observation bounds")
		}
		if o.Exit != nil {
			if !workerExitValid(*o.Exit) {
				return errors.New("invalid worker barrier exit")
			}
			if m.Observed != nil && m.Observed.ExitCode != nil {
				prev := workerBarrierObservation(*m)
				if !reflect.DeepEqual(prev.Exit, o.Exit) {
					return errors.New("worker exit cannot change")
				}
			} else {
				code := o.Exit.ExitCode
				state := backend.Failed
				if code == 0 {
					state = backend.Succeeded
				}
				m.Observed = &workerObservedRecord{State: state, Gated: o.Gated, Container: o.Exit.ContainerID, Started: o.Exit.StartedAt.UTC(), ExitCode: &code, Finished: o.Exit.FinishedAt.UTC(), Node: o.Node, At: now}
			}
		}
		if o.Stop != backend.WorkerStopUnproven && b.Stop == backend.WorkerStopUnproven {
			b.Stop, b.StopAt = o.Stop, now
		}
		b.Observation = &workerBarrierObservationRecord{Revision: o.Revision, Gated: o.Gated, Node: o.Node, Prepared: o.Prepared, Started: o.Started, Deleting: o.Deleting, RendezvousHost: o.Rendezvous.Host, RendezvousPort: o.Rendezvous.Port, Routed: o.Rendezvous.Routed, At: now}
		w.aggregate()
		w.settle()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	return
}

// RecordWorkerResolved preserves identities and a typed outcome together for
// an unknown Create. A resolved member permits cleanup only, never Start.
func (s *Store) RecordWorkerResolved(id string, rev uint64, rank uint8, r backend.WorkerCreated, result WorkerResolveOutcome, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	if result.Class == WorkerResolveNone || result.Class > WorkerResolveMismatch || !result.At.IsZero() {
		return out, errors.New("invalid worker resolution")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(rank) >= len(w.m) {
			return ErrWorkerGroupStale
		}
		m := &w.m[rank]
		b := m.Barrier
		if m.Create.State != WorkerUnknown {
			return errors.New("worker resolution requires unknown create")
		}
		if e := backend.ValidateWorkerCreated(backend.WorkerCreate{Group: workerGroupIdentity(w.job, w.g), Rank: rank}, r, true); e != nil {
			return e
		}
		if r.Pod.UID != "" && m.Pod.UID != "" && r.Pod != m.pod() || r.Service.UID != "" && b.Service.UID != "" && r.Service != (backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}) {
			return errors.New("worker resolution changed original identity")
		}
		known := backend.WorkerCreated{Pod: m.pod(), Service: backend.WorkerResource{Name: b.Service.Name, UID: b.Service.UID}}
		if (result.Refused || b.CreateRefused) && r != known {
			return errors.New("refused worker resolution cannot add identities")
		}
		if b.Resolve.Unsettled && !result.Unsettled {
			return errors.New("worker resolution cannot clear original local owner marker")
		}
		if r.Pod.UID != "" {
			m.Pod = workerPodRecord{r.Pod.Name, r.Pod.UID}
		}
		if r.Service.UID != "" {
			b.Service = workerPodRecord{r.Service.Name, r.Service.UID}
		}
		b.Resolve = workerResolveRecord{Unsettled: result.Unsettled, Class: result.Class, Refused: result.Refused, At: now}
		b.LocalUnsettled = b.LocalUnsettled || result.Unsettled
		w.aggregate()
		w.settle()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	return
}

// RecordWorkerVanished records a missing original UID without interpreting it
// as execution stop or retirement. The whole group's reservation stays held.
func (s *Store) RecordWorkerVanished(id string, rev uint64, rank uint8, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(rank) >= len(w.m) {
			return ErrWorkerGroupStale
		}
		w.m[rank].Barrier.Vanished = true
		w.aggregate()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	return
}

func (s *Store) recordWorkerBarrierLost(tok WorkerToken, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerState(tx, tok.Job)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(tok.Rank) >= len(w.m) || tok.Effect > WorkerRecover {
			return ErrWorkerGroupStale
		}
		m := &w.m[tok.Rank]
		x := m.effect(tok.Effect)
		if x.State != WorkerIntended || x.Seq != tok.Seq || x.Gen != workerGenString(tok.Gen) {
			return ErrWorkerGroupStale
		}
		if x.Gen == workerGenString(workerGen()) {
			return errWorkerIntentLive
		}
		x.State, x.Error = WorkerUnknown, "lost"
		if tok.Effect == WorkerCreate {
			w.g.Phase = WorkerGroupHeld
		}
		w.aggregate()
		w.settle()
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	return
}

// workerBarrierRecordedStop accepts only a persisted native stop fact. The
// absence of a Release intent is not evidence that an original Pod stopped.
func workerBarrierRecordedStop(b *workerBarrierMemberRecord) bool {
	return b != nil && (b.Stop == backend.WorkerNeverBound || b.Stop == backend.WorkerTerminated) && !b.StopAt.IsZero()
}

// workerBarrierNoPodRecovery records a historical qualification, not live
// call eligibility. Later local-owner or Service-retirement facts must not
// invalidate a recovery that was already intended under these immutable facts.
func workerBarrierNoPodRecovery(m workerMemberRecord) bool {
	b := m.Barrier
	return b != nil && m.Rank == 0 && b.CreateRefused && m.Pod == (workerPodRecord{}) && b.Service.UID != "" && !b.RemoveHadPod && !b.RecoverHadPod && b.ServiceRemoveSent && m.Release.State == WorkerNone
}

// workerBarrierRecoveryEvidence never persists NeverReleased as native Stop.
// It derives that ephemeral attestation only for an original-refused missing
// Pod and its exact known Service whose original removal was dispatched.
func workerBarrierRecoveryEvidence(m workerMemberRecord) backend.WorkerStop {
	if workerBarrierRecordedStop(m.Barrier) {
		return m.Barrier.Stop
	}
	if workerBarrierNoPodRecovery(m) {
		return backend.WorkerNeverReleased
	}
	return backend.WorkerStopUnproven
}

func workerBarrierMemberSettled(m workerMemberRecord) bool {
	b := m.Barrier
	if b == nil || b.LocalUnsettled {
		return false
	}
	for _, e := range m.effects() {
		if e.State == WorkerIntended {
			return false
		}
	}
	if m.Create.State == WorkerNone {
		return true
	}
	if m.Create.State == WorkerUnknown && !b.CreateRefused && !(b.Resolve.Class != WorkerResolveNone && m.Pod.UID != "") {
		return false
	}
	if m.Pod.UID == "" {
		if !b.CreateRefused {
			return false
		}
	} else if !m.Removed || !workerBarrierRecordedStop(b) {
		return false
	}
	if b.Service.UID != "" && !b.ServiceRemoved {
		return false
	}
	return !b.Vanished || m.Removed && (b.Service.UID == "" || b.ServiceRemoved)
}

func (w *workerState) barrierLocalUnsettled() bool {
	for _, m := range w.m {
		if m.Barrier.LocalUnsettled {
			return true
		}
	}
	return false
}

func (w *workerState) aggregateBarrier() {
	if w.barrierLocalUnsettled() {
		return
	}
	if w.job.Status.State.Terminal() {
		return
	}
	all := w.g.Barrier.Decision == WorkerBarrierStart
	started, creating, failed := false, false, false
	for _, m := range w.m {
		all = all && m.Barrier.Start.State != WorkerNone && m.Observed != nil && m.Observed.ExitCode != nil && *m.Observed.ExitCode == 0
		creating = creating || m.Create.State != WorkerNone
		started = started || m.Observed != nil && !m.Observed.Started.IsZero()
		failed = failed || m.Observed != nil && m.Observed.ExitCode != nil && *m.Observed.ExitCode != 0 || m.Barrier.Resolve.Class != WorkerResolveNone || m.Barrier.CreateRefused
	}
	switch {
	case w.job.CancelRequested:
		w.job.Status = backend.Status{State: backend.Canceled, Reason: "canceled"}
	case w.g.Revoked:
		w.job.Status = backend.Status{State: backend.Failed, Reason: "worker group authority lost"}
	case w.g.Barrier.Decision == WorkerBarrierAbort || failed:
		w.job.Status = backend.Status{State: backend.Failed, Reason: "worker barrier aborted"}
	case all:
		w.job.Status = backend.Status{State: backend.Succeeded}
	case started:
		w.job.Status = backend.Status{State: backend.Running}
	case creating:
		w.job.Status = backend.Status{State: backend.Starting}
	default:
		w.job.Status = backend.Status{State: backend.Queued}
	}
}

func (w *workerState) settleBarrier() {
	if w.barrierLocalUnsettled() {
		return
	}
	if !w.job.Status.State.Terminal() {
		return
	}
	for _, m := range w.m {
		if !workerBarrierMemberSettled(m) {
			return
		}
	}
	w.g.Phase, w.g.Reserved = WorkerGroupDone, false
}

// LatchWorkerUnsettled retains a trusted read call's original local owner. The
// caller must hold the actual error chain bearing backend.ErrWorkerUnsettled;
// this boolean fence is not a substitute for that owner. It checks the exact
// original reference and never clears, refunds or retroactively reopens a Done
// group. Repeating the same latch at the current revision is read-only.
func (s *Store) LatchWorkerUnsettled(id string, rev uint64, rank uint8, original backend.WorkerReference, now time.Time) (out WorkerGroup, err error) {
	if now, err = workerNow(now); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		w, e := loadWorkerRevision(tx, id, rev)
		if e != nil {
			return e
		}
		if w.g.Barrier == nil || int(rank) >= len(w.m) || w.g.Phase == WorkerGroupDone {
			return ErrWorkerGroupStale
		}
		if e := backend.ValidateWorkerReference(original, true); e != nil {
			return e
		}
		if !reflect.DeepEqual(original, w.barrierRef(rank, false)) {
			return errors.New("unsettled worker read changed original reference")
		}
		if w.m[rank].Barrier.LocalUnsettled {
			out = w.view()
			return errArtifactBeginNoChange
		}
		w.m[rank].Barrier.LocalUnsettled = true
		if e := w.save(tx, now); e != nil {
			return e
		}
		out = w.view()
		return nil
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	return
}
