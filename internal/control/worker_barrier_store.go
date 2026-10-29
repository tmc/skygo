package control

import (
	"errors"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

// storeWorkerBarriers translates Store-owned values into the reducer's private
// protocol. Neither package depends on the other's private representation.
type storeWorkerBarriers struct{ store *store.Store }

func (s storeWorkerBarriers) BarrierGroup(id string) (barrierGroup, error) {
	g, err := s.store.WorkerGroup(id)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) IntendBarrier(id string, rev uint64, rank uint8, e barrierEffect, now time.Time) (barrierToken, barrierGroup, error) {
	effect, err := storeBarrierEffect(e)
	if err != nil {
		return barrierToken{}, barrierGroup{}, err
	}
	tok, g, err := s.store.IntendWorkerBarrier(id, rev, rank, effect, now)
	if err != nil {
		return barrierToken{}, barrierGroup{}, err
	}
	out, err := barrierView(g)
	if err != nil {
		return barrierToken{}, barrierGroup{}, err
	}
	effectBack, err := controlBarrierEffect(tok.Effect)
	if err != nil {
		return barrierToken{}, barrierGroup{}, err
	}
	return barrierToken{Job: tok.Job, Rank: tok.Rank, Effect: effectBack, Seq: tok.Seq, Gen: tok.Gen}, out, nil
}
func (s storeWorkerBarriers) RecordBarrierEffect(tok barrierToken, r barrierReceipt, cause string, now time.Time) (barrierGroup, error) {
	t, err := storeBarrierToken(tok)
	if err != nil {
		return barrierGroup{}, err
	}
	g, err := s.store.RecordWorkerBarrierEffect(t, store.WorkerBarrierReceipt{Created: r.Created, Refused: r.Refused, Released: r.Released, Started: r.Started, Canceled: r.Canceled, Removed: r.Removed, Unsettled: r.Unsettled}, cause, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) RecordBarrierLost(tok barrierToken, now time.Time) (barrierGroup, error) {
	t, err := storeBarrierToken(tok)
	if err != nil {
		return barrierGroup{}, err
	}
	g, err := s.store.RecordWorkerLost(t, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) RecordWorkerBarrier(id string, rev uint64, rank uint8, o backend.WorkerBarrier, now time.Time) (barrierGroup, error) {
	g, err := s.store.RecordWorkerBarrier(id, rev, rank, o, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) RecordWorkerResolved(id string, rev uint64, rank uint8, r backend.WorkerCreated, o resolveOutcome, now time.Time) (barrierGroup, error) {
	var class store.WorkerResolveClass
	switch o.Class {
	case resolveClean:
		class = store.WorkerResolveClean
	case resolveUncertain:
		class = store.WorkerResolveUncertain
	case resolveInterrupted:
		class = store.WorkerResolveInterrupted
	case resolveMismatch:
		class = store.WorkerResolveMismatch
	default:
		return barrierGroup{}, ErrInvalid
	}
	g, err := s.store.RecordWorkerResolved(id, rev, rank, r, store.WorkerResolveOutcome{Class: class, Refused: o.Refused, At: o.At, Unsettled: o.Unsettled}, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) RecordWorkerVanished(id string, rev uint64, rank uint8, now time.Time) (barrierGroup, error) {
	g, err := s.store.RecordWorkerVanished(id, rev, rank, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) DecideWorkerBarrier(id string, rev uint64, d barrierDecision, now time.Time) (barrierGroup, error) {
	var decision store.WorkerBarrierDecision
	switch d {
	case barrierStart:
		decision = store.WorkerBarrierStart
	case barrierAbort:
		decision = store.WorkerBarrierAbort
	default:
		return barrierGroup{}, ErrInvalid
	}
	g, err := s.store.DecideWorkerBarrier(id, rev, decision, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) SettleBarrierGroup(id string, rev uint64, now time.Time) (barrierGroup, error) {
	g, err := s.store.SettleWorkerGroup(id, rev, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func (s storeWorkerBarriers) RevokeBarrierGroup(id string, rev uint64, cause error, now time.Time) (barrierGroup, error) {
	g, err := s.store.RevokeWorkerGroup(id, rev, cause, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
func storeBarrierToken(t barrierToken) (store.WorkerToken, error) {
	e, err := storeBarrierEffect(t.Effect)
	return store.WorkerToken{Job: t.Job, Rank: t.Rank, Effect: e, Seq: t.Seq, Gen: t.Gen}, err
}
func storeBarrierEffect(e barrierEffect) (store.WorkerEffect, error) {
	switch e {
	case barrierCreate:
		return store.WorkerCreate, nil
	case barrierRelease:
		return store.WorkerRelease, nil
	case barrierStartRank:
		return store.WorkerStart, nil
	case barrierCancel:
		return store.WorkerCancel, nil
	case barrierRemove:
		return store.WorkerRemove, nil
	case barrierRecover:
		return store.WorkerRecover, nil
	}
	return 0, ErrInvalid
}
func controlBarrierEffect(e store.WorkerEffect) (barrierEffect, error) {
	switch e {
	case store.WorkerCreate:
		return barrierCreate, nil
	case store.WorkerRelease:
		return barrierRelease, nil
	case store.WorkerStart:
		return barrierStartRank, nil
	case store.WorkerCancel:
		return barrierCancel, nil
	case store.WorkerRemove:
		return barrierRemove, nil
	case store.WorkerRecover:
		return barrierRecover, nil
	}
	return 0, ErrInvalid
}

func barrierView(g store.WorkerGroup) (barrierGroup, error) {
	if g.Barrier == nil || g.Barrier.Version != 1 {
		return barrierGroup{}, errors.New("worker group has no supported barrier")
	}
	out := barrierGroup{Job: cloneJob(g.Job), Group: g.Group, Authority: g.Authority, Revision: g.Revision, Revoked: g.Revoked, Done: g.Phase == store.WorkerGroupDone}
	out.Group.Execution.Task = cloneTask(g.Group.Execution.Task)
	switch g.Barrier.Decision {
	case store.WorkerBarrierUndecided:
		out.Decision = barrierUndecided
	case store.WorkerBarrierStart:
		out.Decision = barrierStart
	case store.WorkerBarrierAbort:
		out.Decision = barrierAbort
	default:
		return barrierGroup{}, ErrInvalid
	}
	for _, m := range g.Members {
		b := m.Barrier
		if b == nil || b.Version != 1 {
			return barrierGroup{}, ErrInvalid
		}
		v := barrierMember{Rank: m.Rank, Pod: m.Pod, Service: b.Service, Create: m.Create, Release: m.Release, Start: b.Start, Cancel: m.Cancel, Remove: m.Remove, Recover: b.Recover, Refused: b.CreateRefused, LocalUnsettled: b.LocalUnsettled, Stop: b.Stop, Removed: m.Removed, ServiceRemoved: b.ServiceRemoved, Vanished: b.Vanished, RemoveHadPod: b.RemoveHadPod, RecoverHadPod: b.RecoverHadPod, StartToken: b.StartToken, ObservedAt: b.ObservedAt}
		switch b.Resolve.Class {
		case store.WorkerResolveNone:
		case store.WorkerResolveClean:
			v.Resolve.Class = resolveClean
		case store.WorkerResolveUncertain:
			v.Resolve.Class = resolveUncertain
		case store.WorkerResolveInterrupted:
			v.Resolve.Class = resolveInterrupted
		case store.WorkerResolveMismatch:
			v.Resolve.Class = resolveMismatch
		default:
			return barrierGroup{}, ErrInvalid
		}
		v.Resolve.Refused, v.Resolve.At, v.Resolve.Unsettled = b.Resolve.Refused, b.Resolve.At, b.Resolve.Unsettled
		if b.Observation != nil {
			o := *b.Observation
			if o.Exit != nil {
				x := *o.Exit
				o.Exit = &x
			}
			v.Observed = &o
		}
		// Sent is true only for EVERY nonzero kind of the original recovery
		// reference. A Pod dispatch never proves a Service mutation was sent.
		pod := m.Pod.UID != "" && !m.Removed
		service := g.Group.Spec.Mode == backend.WorkerTCP && m.Rank == 0 && b.Service.UID != "" && !b.ServiceRemoved
		v.RemoveSent = (pod || service) && (!pod || b.PodRemoveSent) && (!service || b.ServiceRemoveSent)
		out.Members = append(out.Members, v)
	}
	return out, nil
}

func (s storeWorkerBarriers) LatchWorkerUnsettled(id string, rev uint64, rank uint8, original backend.WorkerReference, now time.Time) (barrierGroup, error) {
	g, err := s.store.LatchWorkerUnsettled(id, rev, rank, original, now)
	if err != nil {
		return barrierGroup{}, err
	}
	return barrierView(g)
}
