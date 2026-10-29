package backend

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// WorkerBarrierBackend is the optional main-start barrier of a WorkerBackend.
// A member created by its CreateWorker runs the skygo worker helper, not the
// task: after ReleaseWorker removes the scheduling gate, the scheduler binds
// the member, and the helper prepares, reports Prepared and waits. It executes
// the task argv only once StartWorker has delivered the member's own
// WorkerStartToken. The caller decides the barrier, with WorkerBarrierReady
// over one observation of every original member, and persists that decision
// before any StartWorker. A repeated StartWorker delivers the same token.
//
// WorkerBackend's error-ownership contract applies to every barrier method.
// ErrWorkerUnsettled keeps the actual original local owner and cause held and
// forbids all later effects and refunds, including ResolveWorker, regardless of
// a Store acknowledgment. Other errors positively settle local call, body and
// reader ownership but may leave an unknown native effect for exact-intent
// resolution. They never authorize repeating the original effect. Unqualified
// producers and adapters conservatively mark every error unsettled.
//
// No method binds members atomically. A prepared member can still fail
// before its StartWorker, so a released barrier does not prove that every
// main started. No exit code, including the helper's 143 on termination
// before its start, is no-launch evidence: the caller proves that a main
// never ran only by WorkerNeverBound, or by WorkerTerminated together with
// its own durable record that no start was ever intended for the member.
//
// Every read is a consistent read. A member whose original UID such a read
// does not find has vanished: the method returns an error wrapping
// ErrWorkerVanished, which is never stop evidence.
//
//   - ObserveBarrier reports the member's native barrier facts and any
//     positive stop evidence. It writes nothing.
//   - StartWorker delivers the token to the exact original Pod UID at the
//     observed revision. It writes nothing unless that read shows the Pod
//     ungated, bound, prepared, not deleting and without stop evidence. Its
//     acknowledgement is delivery, not a start.
//   - ResolveWorker looks for what a Create with an unknown result may have
//     made: objects whose name, every label, the task, binding, group and
//     profile annotations, the finalizer and the specification equal the
//     plan of that exact Create, and for rank 0 of a WorkerTCP group the
//     Service by the same rule. A resolved resource permits only
//     ObserveBarrier, ObserveWorker, CancelWorker, WorkerLogs, RemoveWorker
//     and RecoverWorkerRemoval, never ReleaseWorker or StartWorker, and its
//     attempt cannot succeed. Not found is not absence; the Create stays
//     unresolved.
//   - RecoverWorkerRemoval completes a removal under the caller's
//     WorkerRecovery attestation; see ValidateWorkerRecovery.
type WorkerBarrierBackend interface {
	WorkerBackend
	ObserveBarrier(context.Context, WorkerReference) (WorkerBarrier, error)
	StartWorker(context.Context, WorkerReference, [32]byte) (WorkerStarted, error)
	ResolveWorker(context.Context, WorkerCreate) (WorkerCreated, error)
	RecoverWorkerRemoval(context.Context, WorkerRecovery) (WorkerRemoval, error)
}

// ErrWorkerCreateRefused reports that the target definitively refused the
// Create of the member resource that the receipts returned alongside it do
// not hold, so that resource was not created. A Create is refused only by a
// response proving that the request was not applied: a 4xx status other
// than 408 Request Timeout and 429 Too Many Requests, such as a validation,
// admission or quota refusal or AlreadyExists. AlreadyExists creates no
// ownership of the existing object. A lost response, a transport error, a
// timeout, a throttled response and a 5xx are never refused.
var ErrWorkerCreateRefused = errors.New("worker create refused")

// ErrWorkerVanished reports that a consistent read found no object with a
// member resource's original UID, either nothing or another object at its
// name. It is never stop or no-launch evidence: Kubernetes cannot make the
// skygo finalizer exclusive, and force deletion, namespace deletion or an
// operator can remove it. A vanished member stays held, with its group's
// reservation, for operator resolution, except as ValidateWorkerRecovery
// permits.
var ErrWorkerVanished = errors.New("worker resource vanished")

// WorkerStop is positive evidence that a member's task cannot run or has
// stopped. The zero value is no evidence. A missing member, an elapsed time,
// an exit code alone, a gate acknowledgement and a cancel acknowledgement
// are never evidence.
type WorkerStop uint8

const (
	// WorkerStopUnproven is no evidence.
	WorkerStopUnproven WorkerStop = iota
	// WorkerNeverBound: the original Pod is being deleted and has no node.
	// A node assignment is never removed and a Pod being deleted can no
	// longer be bound, so no container of it ever ran.
	WorkerNeverBound
	// WorkerTerminated: the original Pod is in a terminal phase and each of
	// its containers either has a never-restarted native termination state
	// or was never created. No container of it can run again.
	WorkerTerminated
	// WorkerNeverReleased is the caller's own durable record that no
	// ReleaseWorker was ever intended for the member. It is accepted only in
	// a WorkerRecovery; an observation never reports it.
	WorkerNeverReleased
)

// WorkerRecovery is the caller's durable attestation for
// RecoverWorkerRemoval. Evidence is the stop evidence the caller durably
// recorded for exactly Ref's resources before RemoveSent, and RemoveSent
// reports that an original UID-qualified RemoveWorker request was actually
// dispatched after that record for every nonzero resource in Ref. A durable
// intent is not dispatch. If dispatch is unknown, RemoveSent must remain false;
// in particular, a crash between intent and call grants no recovery authority.
type WorkerRecovery struct {
	Ref        WorkerReference
	Evidence   WorkerStop
	RemoveSent bool
}

// ValidateWorkerRecovery reports whether r permits retirement by absence:
// Ref is a valid cleanup reference, Evidence is WorkerNeverBound,
// WorkerTerminated or WorkerNeverReleased, and RemoveSent. When it does,
// RecoverWorkerRemoval reports a resource retired if a consistent read finds
// no object with its original UID; otherwise absence is ErrWorkerVanished.
// The attestation is the caller's: the backend cannot verify it.
func ValidateWorkerRecovery(r WorkerRecovery) error {
	if err := ValidateWorkerReference(r.Ref, true); err != nil {
		return err
	}
	switch r.Evidence {
	case WorkerNeverBound, WorkerTerminated, WorkerNeverReleased:
	default:
		return errors.New("worker recovery requires recorded stop evidence")
	}
	if !r.RemoveSent {
		return errors.New("worker recovery requires a sent removal")
	}
	return nil
}

// WorkerBarrier is trusted in-process output about one original member.
// Revision is the observed resource revision. Node is the bound node, if any.
// Prepared reports that the kubelet reports the helper's prepared check as
// ready for this Pod. Started reports that the member's start token is
// present on the Pod; it is not evidence that the task ran. Rendezvous is
// nonzero only for rank 0 of a WorkerTCP group. Exit is the task container's
// termination receipt and is set only with WorkerTerminated, and only when
// the task container ran.
type WorkerBarrier struct {
	Pod        WorkerResource
	Revision   string
	Gated      bool
	Node       string
	Prepared   bool
	Started    bool
	Deleting   bool
	Rendezvous WorkerRendezvous
	Stop       WorkerStop
	Exit       *ContainerExit
}

// WorkerRendezvous is the observed rendezvous of a WorkerTCP group, reported
// by rank 0. Host and Port are the address every rank receives as
// MASTER_ADDR and MASTER_PORT. Routed reports that the Service has at least
// one ready endpoint and that every endpoint is the original rank 0 Pod.
type WorkerRendezvous struct {
	Service WorkerResource
	Host    string
	Port    uint16
	Routed  bool
}

// WorkerStarted is the original acknowledgement of a token delivery.
// Revision is the resource revision the update returned.
type WorkerStarted struct {
	Pod          WorkerResource
	Acknowledged bool
	Revision     string
}

// workerStartDomain separates start tokens from every other skygo hash.
const workerStartDomain = "skygo-worker-start-v1"

// WorkerStartToken returns WorkerStartDigest of member r, which must be
// valid for ValidateWorkerRelease. The token is specific to one attempt, rank
// and original Pod UID, so it cannot start a stale attempt or a replacement
// Pod. It is not a secret.
func WorkerStartToken(r WorkerReference) ([32]byte, error) {
	if err := ValidateWorkerRelease(r); err != nil {
		return [32]byte{}, err
	}
	g := r.Group
	return WorkerStartDigest(g.Execution.ID, g.AttemptID, g.Spec.Mode, r.Rank, g.Spec.Count, r.Pod.UID, g.TaskDigest), nil
}

// WorkerStartDigest is the start token encoding, the SHA-256 of
//
//	"skygo-worker-start-v1" || 0x00 ||
//	str(job) || str(attempt) || str(mode) || uint8(rank) || uint8(count) ||
//	str(pod uid) || task
//
// where str(s) is uint64be(len(s)) || s and task is the SHA-256 of the
// task's TaskBytes. It validates nothing; a member helper recomputes its own
// token with it from its environment.
func WorkerStartDigest(job, attempt string, mode WorkerMode, rank, count uint8, podUID string, task [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(workerStartDomain))
	h.Write([]byte{0})
	str := func(s string) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(s)))
		h.Write(n[:])
		h.Write([]byte(s))
	}
	str(job)
	str(attempt)
	str(string(mode))
	h.Write([]byte{rank, count})
	str(podUID)
	h.Write(task[:])
	var t [32]byte
	h.Sum(t[:0])
	return t
}

// ValidateWorkerBarrier validates an observation of member r: an exact echo
// of r.Pod, a rendezvous only for rank 0 of a WorkerTCP group, and evidence
// consistent with the observed facts. It does not prove the facts.
func ValidateWorkerBarrier(r WorkerReference, got WorkerBarrier) error {
	if err := ValidateWorkerReference(r, false); err != nil {
		return err
	}
	if got.Pod != r.Pod {
		return errors.New("worker barrier identity mismatch")
	}
	if !workerServiceOwner(r.Group, r.Rank) && got.Rendezvous != (WorkerRendezvous{}) {
		return errors.New("unexpected worker rendezvous")
	}
	if got.Rendezvous != (WorkerRendezvous{}) && got.Rendezvous.Service != r.Service {
		return errors.New("worker rendezvous service mismatch")
	}
	switch got.Stop {
	case WorkerStopUnproven:
		if got.Exit != nil {
			return errors.New("worker exit without termination")
		}
	case WorkerNeverBound:
		if !got.Deleting || got.Node != "" || got.Prepared || got.Exit != nil {
			return errors.New("invalid worker never-bound evidence")
		}
	case WorkerTerminated:
		if got.Prepared {
			return errors.New("terminated worker reports prepared")
		}
		if got.Exit != nil && !dataExit(*got.Exit) {
			return errors.New("invalid worker termination receipt")
		}
	default:
		return errors.New("invalid worker stop evidence")
	}
	return nil
}

// WorkerBarrierReady reports whether one observation of every original
// member of a group permits its barrier: got[i] observes refs[i], the
// complete reference of rank i; every reference names the same execution,
// binding, attempt, task and spec, and in a WorkerTCP group the same
// original Service as rank 0; every member is ungated, bound, prepared, not
// deleting, without stop evidence and without a start token; and in a
// WorkerTCP group rank 0 reports its own Service routed to its original Pod.
// It returns the first refusal. A nil result is validation of a decision
// input, not a durable decision or authorization to start.
func WorkerBarrierReady(refs []WorkerReference, got []WorkerBarrier) error {
	if len(refs) == 0 || len(refs) != len(got) {
		return errors.New("worker barrier requires every member")
	}
	g := refs[0].Group
	if len(refs) != int(g.Spec.Count) {
		return errors.New("worker barrier requires every member")
	}
	uids := make(map[string]bool, len(refs))
	for i, r := range refs {
		q := r.Group
		if q.Execution.ID != g.Execution.ID || q.AttemptID != g.AttemptID || q.TaskDigest != g.TaskDigest ||
			q.Spec != g.Spec || r.Rank != uint8(i) {
			return errors.New("worker barrier members differ")
		}
		if q.Execution.Binding != g.Execution.Binding {
			return errors.New("worker barrier members differ in binding")
		}
		if g.Spec.Mode == WorkerTCP && r.Service != refs[0].Service {
			return errors.New("worker barrier members differ in rendezvous service")
		}
		if err := ValidateWorkerRelease(r); err != nil {
			return fmt.Errorf("worker rank %d: %w", i, err)
		}
		if err := ValidateWorkerBarrier(r, got[i]); err != nil {
			return fmt.Errorf("worker rank %d: %w", i, err)
		}
		if uids[r.Pod.UID] {
			return errors.New("worker barrier members share a uid")
		}
		uids[r.Pod.UID] = true
		o := got[i]
		switch {
		case o.Gated || o.Node == "":
			return fmt.Errorf("worker rank %d is not bound", i)
		case o.Deleting || o.Stop != WorkerStopUnproven:
			return fmt.Errorf("worker rank %d is stopping", i)
		case o.Started:
			// A token before the durable decision is a foreign or corrupt write.
			return fmt.Errorf("worker rank %d already holds a start token", i)
		case !o.Prepared:
			return fmt.Errorf("worker rank %d is not prepared", i)
		case workerServiceOwner(g, r.Rank) && !o.Rendezvous.Routed:
			return errors.New("worker rendezvous is not routed to rank 0")
		}
	}
	return nil
}
