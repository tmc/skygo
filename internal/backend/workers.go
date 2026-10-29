package backend

import (
	"context"
	"crypto/sha256"
	"errors"
)

// MaxWorkers is the largest worker group.
const MaxWorkers = 16

// WorkerMode selects how the ranks of a worker group find each other. The zero
// value is invalid; there is no default mode.
type WorkerMode string

const (
	// WorkerIndependent ranks share no rendezvous and no Service.
	WorkerIndependent WorkerMode = "independent"
	// WorkerTCP ranks rendezvous at rank 0 through the group's one Service,
	// which rank 0's CreateWorker owns.
	WorkerTCP WorkerMode = "tcp"
)

// WorkerSpec requests Count ranks of one accepted task. It is not a Task
// field. NormalizeWorkers validates and copies a spec before it becomes
// accepted data.
type WorkerSpec struct {
	Count uint8      `json:"count" yaml:"count"`
	Mode  WorkerMode `json:"mode" yaml:"mode"`
}

// NewWorkerSpec returns NormalizeWorkers of count ranks in mode. It checks a
// wire count against MaxWorkers before narrowing it, so 257 is refused rather
// than wrapping to 1.
func NewWorkerSpec(count uint32, mode WorkerMode) (*WorkerSpec, error) {
	if count > MaxWorkers {
		return nil, errors.New("worker count must be between 1 and 16")
	}
	return NormalizeWorkers(&WorkerSpec{Count: uint8(count), Mode: mode})
}

// NormalizeWorkers returns an independent copy of w. Nil and a single
// independent worker mean the existing one-worker behavior and return nil.
// Otherwise Count must be 2..MaxWorkers and Mode WorkerIndependent or
// WorkerTCP. A zero count, a zero or unknown mode and a single TCP worker are
// refused. It does not change w or prove that a target supports workers.
func NormalizeWorkers(w *WorkerSpec) (*WorkerSpec, error) {
	if w == nil {
		return nil, nil
	}
	if w.Mode != WorkerIndependent && w.Mode != WorkerTCP {
		return nil, errors.New("invalid worker mode")
	}
	switch {
	case w.Count == 0 || w.Count > MaxWorkers:
		return nil, errors.New("worker count must be between 1 and 16")
	case w.Count == 1 && w.Mode == WorkerTCP:
		return nil, errors.New("tcp rendezvous requires at least 2 workers")
	case w.Count == 1:
		return nil, nil
	}
	return &WorkerSpec{Count: w.Count, Mode: w.Mode}, nil
}

// ExecutionUnits returns the number of executions w requests: 1 for nil or a
// single independent worker and Count otherwise. An invalid spec returns an
// error, never a smaller charge.
func ExecutionUnits(w *WorkerSpec) (int, error) {
	n, err := NormalizeWorkers(w)
	if err != nil {
		return 0, err
	}
	if n == nil {
		return 1, nil
	}
	return int(n.Count), nil
}

// ErrWorkerUnsettled reports that an original worker call, response body or
// reader still has unsettled local ownership. The error chain must retain the
// actual original owner and cause. Use errors.Is; the error text is not a
// classifier. A durable Store acknowledgment does not discharge that owner.
// The caller must retain the error and block all further effects and refunds;
// it must not retry Close or repeat the original native effect.
var ErrWorkerUnsettled = errors.New("worker call ownership unsettled")

// WorkerBackend is the optional per-member worker group contract. It
// supplements Backend; a configured route must assert both interfaces before
// accepting worker work. Each method is one original operation on one member;
// none owns a polling or retry loop, creates on observation or adopts by name.
// Original calls and their partial results remain owned through actual
// return, and canceling a method's context never erases an original result or
// the cleanup duty it creates. No method infers termination or cleanup from a
// missing object, Unknown, a Delete acknowledgement or a gate update.
//
// Returned errors wrapping ErrWorkerUnsettled retain unsettled local call,
// response-body or reader ownership and block every later effect and refund,
// even after the original result is durably recorded. Every other returned
// error certifies that these local owners positively settled; it may still
// mean the native effect is unknown. Such an effect requires exact-intent
// recovery, never blind repetition. Until every error path is qualified, a
// producer or adapter must conservatively wrap all errors in ErrWorkerUnsettled
// while preserving the original owner and cause.
//
// The caller creates each member after its durable creating intent, at most
// once: an unknown Create never becomes a new Create or an adoption by name.
// A resource whose Create result is unknown or lost, such as a member Pod or
// rank 0's Service that may exist with an unknown UID, remains an unresolved
// cleanup subject under that intent. It blocks completion of the group's
// cleanup; absence is never inferred. Resolving it requires the producer's
// exact-intent recovery, which this contract does not define.
//
// Every member is created with one fixed scheduling gate and runs nothing until
// ReleaseWorker, which the caller issues only after the member's Create
// receipt and the group's activation are durable.
//
// In a WorkerTCP group, rank 0's CreateWorker alone creates the group's one
// rendezvous Service, before its own Pod, and returns both original
// identities. No other operation creates or replaces it. A lost or failed
// Service Create creates no rank 0 Pod and leaves the Service UID unknown; no
// rank of that group can then be released. RemoveWorker of rank 0 retires the
// Service, and the caller issues it only after every rank's positive stop or
// no-launch evidence and every original call has returned.
//
//   - CreateWorker creates the gated member and returns every original
//     receipt it obtained, even alongside an error. It invents no UID.
//   - ReleaseWorker removes only the skygo gate from the exact original Pod
//     UID. It returns the original acknowledgement even alongside an error.
//   - ObserveWorker reports the member's current native facts. It reports
//     Unknown for a missing member and an error for temporary uncertainty.
//   - CancelWorker requests termination of the exact original member. Its
//     acknowledgement, returned even alongside an error, is not termination.
//   - WorkerLogs reads the member's own retained log prefix, as Backend.Logs.
//   - RemoveWorker retires the member's recorded resources after their
//     ownership prerequisites. It never proves that execution stopped.
//
// A nil error from any method never means that a member exited.
type WorkerBackend interface {
	CreateWorker(context.Context, WorkerCreate) (WorkerCreated, error)
	ReleaseWorker(context.Context, WorkerReference) (WorkerReleased, error)
	ObserveWorker(context.Context, WorkerReference) (WorkerObservation, error)
	CancelWorker(context.Context, WorkerReference) (WorkerCanceled, error)
	WorkerLogs(context.Context, WorkerReference, int64, int) (LogChunk, error)
	RemoveWorker(context.Context, WorkerReference) (WorkerRemoval, error)
}

// WorkerResource is an original Create-return identity. Zero is unknown; a
// later lookup by name cannot fill a missing UID.
type WorkerResource struct{ Name, UID string }

// WorkerGroup is the immutable identity shared by every member call of one
// group attempt. Execution holds the group's job ID, accepted task and target
// binding, and an empty Instance: a group has no single instance, and no
// Status.Instance holds a serialized group. TaskDigest is the SHA-256 of
// TaskBytes(Execution.Task). Spec is normalized, with Count 2..MaxWorkers.
// The task has no retry policy, workspace, setup, outputs or data mounts.
type WorkerGroup struct {
	Execution  Reference
	AttemptID  string
	TaskDigest [32]byte
	Spec       WorkerSpec
}

// WorkerCreate creates rank Rank of Group.
type WorkerCreate struct {
	Group WorkerGroup
	Rank  uint8
}

// WorkerCreated preserves original receipts even alongside an error. Pod is
// the member's gated Pod. Service is nonzero only for rank 0 of a WorkerTCP
// group, which receives a Pod receipt only after a Service receipt. The
// bounded partial results are: nothing; for TCP rank 0, the Service alone;
// and the complete result. A partial result does not mean that a missing
// resource was not created: each resource whose Create was attempted and is
// not in the result remains unresolved under its creating intent and blocks
// group cleanup completion.
type WorkerCreated struct {
	Pod, Service WorkerResource
}

// WorkerReference addresses one member. Pod is the member's original
// Create-return identity, and Service is the group's rendezvous Service from
// rank 0's original Create, or zero while unknown and in a WorkerIndependent
// group. Pod is zero only in a cleanup reference to a rank 0 Service whose Pod
// was never created.
type WorkerReference struct {
	Group        WorkerGroup
	Rank         uint8
	Pod, Service WorkerResource
}

// WorkerReleased is the original acknowledgement of a gate removal. Revision
// is the resource revision the update returned. Acknowledged means the update
// was accepted, not that the member was scheduled or started.
type WorkerReleased struct {
	Pod          WorkerResource
	Acknowledged bool
	Revision     string
}

// WorkerObservation is trusted in-process output about one original member.
// Status.Instance is empty or Pod.UID. Gated reports that the skygo gate is
// still present, and Node is the assigned node, if any; neither is readiness
// or rendezvous membership. Start is Running-only; Exit is a native
// termination receipt, required for StoppedWithExitCode.
type WorkerObservation struct {
	Pod    WorkerResource
	Status Status
	Gated  bool
	Node   string
	Start  *ContainerStart
	Exit   *ContainerExit
}

// WorkerCanceled is the original acknowledgement of a termination request.
// Acknowledged is delivery, never termination or removal.
type WorkerCanceled struct {
	Pod          WorkerResource
	Acknowledged bool
}

// WorkerRemoval echoes each recorded original identity independently. Each
// Removed flag requires actual UID-qualified retirement after its ownership
// prerequisites; only rank 0 of a WorkerTCP group owns the Service.
//
// PodDispatched and ServiceDispatched attest dispatch of the same original
// UID-qualified removal request for their respective resource. A durable intent,
// entering a client call, or a timeout is not dispatch evidence. Dispatch alone
// proves neither application nor retirement. Unqualified producers leave these
// fields false.
type WorkerRemoval struct {
	Pod, Service                     WorkerResource
	PodRemoved, ServiceRemoved       bool
	PodDispatched, ServiceDispatched bool
}

// ValidateWorkerCreated validates original receipt shape. allowPartial is only
// for preserving a failed original call's partial receipts; it grants no
// release or launch authority.
func ValidateWorkerCreated(c WorkerCreate, got WorkerCreated, allowPartial bool) error {
	if err := validateWorkerMember(c.Group, c.Rank); err != nil {
		return err
	}
	if !workerServiceOwner(c.Group, c.Rank) && got.Service != (WorkerResource{}) {
		return errors.New("unexpected worker service receipt")
	}
	if allowPartial && got == (WorkerCreated{}) {
		return nil
	}
	return ValidateWorkerReference(WorkerReference{Group: c.Group, Rank: c.Rank, Pod: got.Pod, Service: got.Service}, allowPartial)
}

// ValidateWorkerReference validates a member reference. cleanup admits the
// rank 0 Service-only reference that RemoveWorker alone accepts.
func ValidateWorkerReference(r WorkerReference, cleanup bool) error {
	if err := validateWorkerMember(r.Group, r.Rank); err != nil {
		return err
	}
	if !workerResource(r.Pod) || !workerResource(r.Service) {
		return errors.New("invalid worker identity")
	}
	none := WorkerResource{}
	if r.Group.Spec.Mode == WorkerIndependent && r.Service != none {
		return errors.New("unexpected worker service")
	}
	if r.Pod != none && r.Pod.UID == r.Service.UID {
		return errors.New("worker resources share a uid")
	}
	owner := workerServiceOwner(r.Group, r.Rank)
	if owner && r.Pod != none && r.Service == none {
		return errors.New("worker pod lacks its service")
	}
	if r.Pod == none && !(cleanup && owner && r.Service != none) {
		return errors.New("missing worker pod")
	}
	return nil
}

// ValidateWorkerRelease validates a ReleaseWorker reference: a complete
// member, and in a WorkerTCP group the known rendezvous Service.
func ValidateWorkerRelease(r WorkerReference) error {
	if err := ValidateWorkerReference(r, false); err != nil {
		return err
	}
	if r.Group.Spec.Mode == WorkerTCP && r.Service == (WorkerResource{}) {
		return errors.New("worker release requires the rendezvous service")
	}
	return nil
}

// ValidateWorkerRemoval checks exact original echoes and independent flags. It
// does not prove UID-qualified deletion, execution stop or original joins.
func ValidateWorkerRemoval(r WorkerReference, got WorkerRemoval) error {
	if err := ValidateWorkerReference(r, true); err != nil {
		return err
	}
	var service WorkerResource
	if workerServiceOwner(r.Group, r.Rank) {
		service = r.Service
	}
	if got.Pod != r.Pod || got.Service != service {
		return errors.New("worker removal identity mismatch")
	}
	if got.PodRemoved && r.Pod == (WorkerResource{}) || got.ServiceRemoved && service == (WorkerResource{}) {
		return errors.New("worker removal acknowledges unknown resource")
	}
	if got.PodDispatched && r.Pod == (WorkerResource{}) || got.ServiceDispatched && service == (WorkerResource{}) {
		return errors.New("worker removal dispatches unknown resource")
	}
	return nil
}

// validateWorkerMember validates g and that rank is one of its ranks.
func validateWorkerMember(g WorkerGroup, rank uint8) error {
	if !dataID(g.Execution.ID) || !dataID(g.AttemptID) || !dataText(g.Execution.Binding, 256) || g.Execution.Instance != "" {
		return errors.New("invalid worker group identity")
	}
	n, err := NormalizeWorkers(&g.Spec)
	if err != nil {
		return err
	}
	if n == nil {
		return errors.New("worker group requires at least 2 workers")
	}
	if rank >= n.Count {
		return errors.New("worker rank out of range")
	}
	t := g.Execution.Task
	if t.Retry != nil || t.Workdir != nil || len(t.Setup) != 0 || len(t.Outputs) != 0 || len(t.DataMounts) != 0 {
		return errors.New("worker tasks do not support retry, workspaces or data")
	}
	raw, err := TaskBytes(t)
	if err != nil {
		return err
	}
	if sha256.Sum256(raw) != g.TaskDigest {
		return errors.New("worker task digest mismatch")
	}
	return nil
}

func workerServiceOwner(g WorkerGroup, rank uint8) bool {
	return g.Spec.Mode == WorkerTCP && rank == 0
}

func workerResource(r WorkerResource) bool {
	return dataResource(DataResource(r))
}
