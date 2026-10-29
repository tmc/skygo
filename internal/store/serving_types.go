package store

import (
	"errors"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
)

var (
	// ErrServiceStale reports a mutation or commit whose expected record
	// revision is not the service's current revision. Nothing was written.
	ErrServiceStale = errors.New("service record revision changed")

	// ErrServiceDeleted reports a mutation of a service whose deletion is
	// latched. Nothing was written.
	ErrServiceDeleted = errors.New("service is deleted")

	// ErrServiceNameInUse reports a name held by another of the principal's
	// services. A name is held until its service is retired, not merely
	// deleted.
	ErrServiceNameInUse = errors.New("service name is in use")

	// ErrServiceHeld reports a retirement refused because the service's
	// deletion is not yet established: its intent is not Absent, an operation
	// is in flight, its reservation is held or absence was never observed.
	// Nothing was written.
	ErrServiceHeld = errors.New("service custody is held")
)

// ServiceQuota names one service admission reservation. The values match
// the protocol's ServiceQuotaDomain.
type ServiceQuota uint8

const (
	ServicePrincipalQuota    ServiceQuota = iota + 1 // unretired services of one principal
	ServiceInstallationQuota                         // unretired services of the installation
	ServiceReplicasQuota                             // reserved replicas on one binding
	ServiceCPUQuota                                  // reserved CPU millis on one binding
	ServiceMemoryQuota                               // reserved memory bytes on one binding
	ServiceRevisionsQuota                            // service metadata bytes
	ServiceRequestsQuota                             // request witnesses and tombstones
)

// ServiceCapacityError reports a refused admission reservation. Nothing was
// written. It is a reservation, not a claim about enforced target capacity.
type ServiceCapacityError struct {
	Domain                 ServiceQuota
	Used, Requested, Limit uint64
}

func (e *ServiceCapacityError) Error() string { return "service capacity reached" }

// ServiceLimits are the operator's admission bounds for one target binding.
// Zero PrincipalServices, InstallationServices and Replicas select 4, 32 and
// 64. CPUMillis and MemoryBytes are required. InstallationServices counts
// every unretired service of the installation, on any binding.
type ServiceLimits struct {
	PrincipalServices    uint64
	InstallationServices uint64
	Replicas             uint64
	CPUMillis            uint64
	MemoryBytes          uint64
}

// normalize applies defaults and refuses limits outside the hard bounds.
func (l ServiceLimits) normalize() (ServiceLimits, error) {
	if l.PrincipalServices == 0 {
		l.PrincipalServices = 4
	}
	if l.InstallationServices == 0 {
		l.InstallationServices = 32
	}
	if l.Replicas == 0 {
		l.Replicas = 64
	}
	switch {
	case l.PrincipalServices > maxServingRecords, l.InstallationServices > maxServingRecords,
		l.PrincipalServices > l.InstallationServices, l.Replicas > 8*maxServingRecords:
		return l, errors.New("service limits outside hard bounds")
	case l.CPUMillis == 0 || l.MemoryBytes == 0:
		return l, errors.New("service limits require cpu and memory budgets")
	}
	return l, nil
}

// Validate reports whether l, with defaults applied, is usable.
func (l ServiceLimits) Validate() error {
	_, err := l.normalize()
	return err
}

// ServiceRequest is one service acceptance. Target is the configured public
// target name and Binding its server-side binding. Authority is the actor's
// fresh delegation; the Store persists it unchanged.
type ServiceRequest struct {
	Domain    RequestDomain
	RequestID string
	Name      string
	Target    string
	Binding   string
	Authority auth.Delegation
	Spec      serving.Spec
}

// ServiceMutation identifies one mutation of service ID, expected at record
// revision Revision, under a request ID in a request domain.
type ServiceMutation struct {
	Domain    RequestDomain
	RequestID string
	ID        string
	Revision  uint64
}

// ServiceOperationKind is the kind of a native operation in flight. Zero is
// invalid.
type ServiceOperationKind uint8

const (
	ServiceApply ServiceOperationKind = iota + 1
	ServiceDelete
)

// ServiceOperation is the identity of the one native operation that a
// reconciler committed before its first runtime call and has not yet
// integrated. It stays fixed until a commit clears it. Every native create
// of the operation is declared in the committed Native before it is sent; a
// declaration recorded before a restart counts as possibly sent.
type ServiceOperation struct {
	ID         string               `json:"id"`
	Kind       ServiceOperationKind `json:"kind"`
	Generation uint64               `json:"generation"`
	Revision   uint64               `json:"revision"`
	Started    time.Time            `json:"started"`
}

// ServiceConditionCode is a bounded diagnostic. The values match the
// protocol's ServiceConditionCode. Zero is invalid.
type ServiceConditionCode uint8

const (
	ServiceOperationUncertain ServiceConditionCode = iota + 1
	ServiceTargetUnavailable
	ServiceAuthorityUnavailable
	ServiceAuthorityRevoked
	ServiceForwardDisabled
	ServiceCleanupPending
)

// ServiceCondition is one recorded condition, at most one per code.
type ServiceCondition struct {
	Code       ServiceConditionCode `json:"code"`
	Generation uint64               `json:"generation"`
	Since      time.Time            `json:"since"`
}

// ServiceReservation is a service's admission charge on its binding: the
// current revision's replicas at its CPU and memory requests. The fixed
// renderer of this format rolls out with no surge, so no surge replica is
// charged. The charge is held until the service's absence is observed with
// no operation in flight, whatever its phase.
type ServiceReservation struct {
	Replicas    uint64 `json:"replicas"`
	CPUMillis   uint64 `json:"cpu_millis"`
	MemoryBytes uint64 `json:"memory_bytes"`
}

// Service is one accepted service. ID, Installation, Nonce, Principal,
// Domain, RequestID, Name, Target, Binding and Authority are fixed at
// acceptance. Revision increases with every committed change. Native,
// Binding, Nonce, Installation and Authority are server-only. A retired
// service has only its identity, Desired Absent and a zero History.
type Service struct {
	ID           string
	Installation string
	Nonce        string
	Principal    string
	Domain       RequestDomain
	RequestID    string
	Name         string
	Target       string
	Binding      string
	Authority    auth.Delegation
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Revision     uint64
	Desired      serving.Desired
	History      serving.History
	Native       serving.Native
	Observation  *serving.Observation
	ObservedAt   time.Time
	Operation    *ServiceOperation
	Conditions   []ServiceCondition
	Reserved     ServiceReservation
	Retired      bool
}

// Ref returns the service's native reference.
func (s Service) Ref() serving.Ref {
	return serving.Ref{Service: s.ID, Owner: s.Installation, Nonce: s.Nonce, Binding: s.Binding, Native: s.Native}
}

// Phase derives the service's reported phase. A retired service is
// Deleted. A service with an operation in flight is never Deleted: its
// recorded absence may predate a create that can still land.
func (s Service) Phase() serving.Phase {
	if s.Retired {
		return serving.Deleted
	}
	p := serving.PhaseOf(s.Desired, s.History, s.Observation)
	if p == serving.Deleted && (s.Operation != nil || s.Reserved != (ServiceReservation{})) {
		return serving.Deleting
	}
	return p
}

// ServicePage is one listing page. After is a listing ordinal.
type ServicePage struct {
	Services []Service
	After    uint64
	More     bool
}

// ServiceCommit is the next runtime-derived state of a service. It replaces
// Native, Operation, History, Observation, ObservedAt and Conditions; the
// reservation is derived by the Store. See CommitService.
type ServiceCommit struct {
	Native      serving.Native
	Operation   *ServiceOperation
	History     serving.History
	Observation *serving.Observation
	ObservedAt  time.Time
	Conditions  []ServiceCondition
}

// Commit returns the commit that keeps every runtime-derived field of s.
// Callers change the fields they intend to.
func (s Service) Commit() ServiceCommit {
	c := ServiceCommit{
		Native:     s.Native,
		History:    s.History,
		ObservedAt: s.ObservedAt,
		Conditions: append([]ServiceCondition(nil), s.Conditions...),
	}
	if s.Operation != nil {
		op := *s.Operation
		c.Operation = &op
	}
	if s.Observation != nil {
		o := *s.Observation
		c.Observation = &o
	}
	return c
}
