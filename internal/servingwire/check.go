package servingwire

import (
	"errors"
	"fmt"
	"regexp"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// History bounds, as the server retains them.
const (
	maxRevisions  = 8
	maxRollouts   = 32
	maxConditions = 16
	maxReplicas   = 1 << 10
	// The portable fixed policy permits 1..8 replicas. Diagnostic counts
	// above remain bounded separately, including for held native state.
	maxFixedReplicas = 8
)

// name is a service name and a configured target name: a DNS label.
var name = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)

// CheckService validates a service returned by a server, before a client
// prints or uses it. It refuses unknown fields at any depth; an invalid ID,
// name or target; an undeclared desired state, phase, reason, kind or
// outcome; a REAPPLY rollout or a create that is not generation 1; a
// history that is unordered, over its bounds, or whose revisions' digests
// are not their specs'; a status whose generations, counts or conditions
// are inconsistent with each other or with the history; any scaling state;
// a ready status whose updated, ready and available replica counts do not
// match its current revision, or that has no ready endpoint; a deleted
// status without observed absence; and a retired view that claims anything
// beyond deletion.
// Condition codes this client does not know are accepted and printed as
// unknown.
func CheckService(m *v1.Service) error {
	if m == nil {
		return errors.New("missing service")
	}
	if err := Unknown(m, m.History, m.CreatedAt, m.UpdatedAt); err != nil {
		return err
	}
	if err := checkHead(m.Id, m.Name, m.Target, m.RecordRevision, m.Desired, m.CreatedAt, m.UpdatedAt); err != nil {
		return err
	}
	revisions, rollouts := m.GetHistory().GetRevisions(), m.GetHistory().GetRollouts()
	if m.Retired {
		if m.Desired != v1.ServiceDesired_SERVICE_DESIRED_ABSENT || m.Status.GetPhase() != v1.ServicePhase_SERVICE_PHASE_DELETED {
			return errors.New("retired service is not deleted")
		}
		if len(revisions) != 0 || len(rollouts) != 0 {
			return errors.New("retired service has a history")
		}
		return checkStatus(m.Status, m.Desired, m.Status.GetDesiredGeneration(), true)
	}
	if err := checkHistory(revisions, rollouts); err != nil {
		return err
	}
	current := rollouts[len(rollouts)-1]
	if m.Status.GetDesiredGeneration() != current.Generation {
		return errors.New("service status is not of its current rollout")
	}
	if err := checkStatus(m.Status, m.Desired, current.Generation, false); err != nil {
		return err
	}
	if m.Status.Phase == v1.ServicePhase_SERVICE_PHASE_READY {
		for _, r := range revisions {
			if r.Number == current.Revision && m.Status.Observation.DesiredReplicas != r.Spec.Scaling.GetReplicas() {
				return errors.New("ready service replica count is not its revision's")
			}
		}
	}
	return nil
}

// CheckSummary validates a listed service as CheckService does, without
// the history the list omits: its current rollout must be well formed and
// be the status's, and its digest well formed. Ready counts must be mutually
// consistent and in 1..8; a summary cannot bind them to its omitted spec.
func CheckSummary(m *v1.ServiceSummary) error {
	if m == nil {
		return errors.New("missing service")
	}
	if err := Unknown(m, m.Current, m.CreatedAt, m.UpdatedAt); err != nil {
		return err
	}
	if err := checkHead(m.Id, m.Name, m.Target, m.RecordRevision, m.Desired, m.CreatedAt, m.UpdatedAt); err != nil {
		return err
	}
	if m.Current == nil {
		return errors.New("service summary has no current rollout")
	}
	if err := checkRollout(m.Current, true); err != nil {
		return err
	}
	if !lowerHex(m.Sha256, 64) {
		return errors.New("invalid service revision digest")
	}
	if m.Status.GetDesiredGeneration() != m.Current.Generation {
		return errors.New("service status is not of its current rollout")
	}
	return checkStatus(m.Status, m.Desired, m.Current.Generation, false)
}

func checkHead(id, n, target string, record uint64, desired v1.ServiceDesired, created, updated *timestamppb.Timestamp) error {
	switch {
	case !ValidID(id):
		return errors.New("invalid service id")
	case !name.MatchString(n):
		return errors.New("invalid service name")
	case !name.MatchString(target):
		return errors.New("invalid service target")
	case record == 0:
		return errors.New("invalid service record revision")
	case desired != v1.ServiceDesired_SERVICE_DESIRED_PRESENT && desired != v1.ServiceDesired_SERVICE_DESIRED_ABSENT:
		return errors.New("invalid service desired state")
	}
	for _, ts := range []*timestamppb.Timestamp{created, updated} {
		if ts != nil && ts.CheckValid() != nil {
			return errors.New("invalid service timestamp")
		}
	}
	if created != nil && updated != nil && updated.AsTime().Before(created.AsTime()) {
		return errors.New("service updated before it was created")
	}
	return nil
}

func checkHistory(revisions []*v1.ServiceRevision, rollouts []*v1.ServiceRollout) error {
	if len(revisions) == 0 || len(revisions) > maxRevisions || len(rollouts) == 0 || len(rollouts) > maxRollouts {
		return errors.New("service history out of bounds")
	}
	retained := map[uint64]bool{}
	var last uint64
	for _, r := range revisions {
		if r == nil {
			return errors.New("missing service revision")
		}
		if err := Unknown(r); err != nil {
			return err
		}
		if r.Number <= last {
			return errors.New("service revisions out of order")
		}
		last = r.Number
		spec, err := SpecFromProto(r.Spec)
		if err != nil {
			return fmt.Errorf("service revision %d: %w", r.Number, err)
		}
		if sum, err := spec.Digest(); err != nil || sum != r.Sha256 {
			return fmt.Errorf("service revision %d digest is not its spec's", r.Number)
		}
		retained[r.Number] = true
	}
	for i, r := range rollouts {
		if r == nil {
			return errors.New("missing service rollout")
		}
		if err := checkRollout(r, i == len(rollouts)-1); err != nil {
			return err
		}
		if i > 0 && r.Generation != rollouts[i-1].Generation+1 {
			return errors.New("service rollouts out of order")
		}
		if r.Revision > last {
			return errors.New("service rollout of an unknown revision")
		}
	}
	if !retained[rollouts[len(rollouts)-1].Revision] {
		return errors.New("service current revision is not retained")
	}
	return nil
}

// checkRollout checks r; only the current rollout may be unfinished. The
// first generation, and only it, is a create. REAPPLY, which no server
// sends, is refused like any unknown kind.
func checkRollout(r *v1.ServiceRollout, current bool) error {
	if err := Unknown(r); err != nil {
		return err
	}
	switch {
	case r.Generation == 0 || r.Revision == 0:
		return errors.New("invalid service rollout")
	case r.Kind < v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_CREATE || r.Kind > v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_ROLLBACK:
		return errors.New("invalid service rollout kind")
	case (r.Kind == v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_CREATE) != (r.Generation == 1):
		return errors.New("service rollout kind is not its generation's")
	case r.Outcome < v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING || r.Outcome > v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_SUPERSEDED:
		return errors.New("invalid service rollout outcome")
	case !current && (r.Outcome == v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING || r.Outcome == v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_ROLLING):
		return errors.New("earlier service rollout is unfinished")
	}
	return nil
}

// checkStatus checks s against desired and the current generation. A
// retired view is deleted with no observation; any other deleted status
// must have observed absence.
func checkStatus(s *v1.ServiceStatus, desired v1.ServiceDesired, generation uint64, retired bool) error {
	if s == nil {
		return errors.New("missing service status")
	}
	o := s.Observation
	ms := []proto.Message{s, o, s.ObservedAt}
	for _, c := range s.Conditions {
		if c == nil {
			return errors.New("missing service condition")
		}
		ms = append(ms, c, c.Since)
	}
	if err := Unknown(ms...); err != nil {
		return err
	}
	p := s.Phase
	switch {
	case p < v1.ServicePhase_SERVICE_PHASE_CREATING || p > v1.ServicePhase_SERVICE_PHASE_HELD:
		return errors.New("invalid service phase")
	case p == v1.ServicePhase_SERVICE_PHASE_DELETED && desired != v1.ServiceDesired_SERVICE_DESIRED_ABSENT:
		return errors.New("deleted service is not desired absent")
	case p == v1.ServicePhase_SERVICE_PHASE_DELETING && desired != v1.ServiceDesired_SERVICE_DESIRED_ABSENT:
		return errors.New("deleting service is not desired absent")
	case s.ObservedAt != nil && s.ObservedAt.CheckValid() != nil:
		return errors.New("invalid service timestamp")
	case len(s.Conditions) > maxConditions:
		return errors.New("too many service conditions")
	}
	codes := map[v1.ServiceConditionCode]bool{}
	for _, c := range s.Conditions {
		if c.Code == v1.ServiceConditionCode_SERVICE_CONDITION_CODE_UNSPECIFIED || codes[c.Code] || c.Generation > generation ||
			c.Since != nil && c.Since.CheckValid() != nil {
			return errors.New("invalid service condition")
		}
		codes[c.Code] = true
	}
	if desired == v1.ServiceDesired_SERVICE_DESIRED_ABSENT &&
		p != v1.ServicePhase_SERVICE_PHASE_DELETING && p != v1.ServicePhase_SERVICE_PHASE_DELETED && p != v1.ServicePhase_SERVICE_PHASE_HELD {
		return errors.New("service desired absent is not deleting")
	}
	if o == nil {
		switch p {
		case v1.ServicePhase_SERVICE_PHASE_READY:
			return errors.New("ready service has no observation")
		case v1.ServicePhase_SERVICE_PHASE_DELETED:
			if !retired {
				return errors.New("deleted service has no observation of absence")
			}
		}
		return nil
	}
	if retired {
		return errors.New("retired service has an observation")
	}
	for _, n := range []int32{o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas, o.ReadyEndpoints} {
		if n < 0 || n > maxReplicas {
			return errors.New("invalid service replica count")
		}
	}
	switch {
	case o.Reason < v1.ServiceReason_SERVICE_REASON_UNSPECIFIED || o.Reason > v1.ServiceReason_SERVICE_REASON_IDENTITY_CONFLICT:
		return errors.New("invalid service reason")
	case o.Scaling != v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_UNSPECIFIED:
		// Only fixed revisions are served; any scaling state is foreign.
		return errors.New("service observation reports scaling")
	case o.ReadyGeneration > o.ObservedGeneration || o.ObservedGeneration > generation:
		return errors.New("service observation generations are inconsistent")
	case o.AvailableReplicas > o.ReadyReplicas:
		return errors.New("service available replicas exceed ready replicas")
	case o.Absent && (o.ObservedGeneration != 0 || o.ReadyGeneration != 0 || o.DesiredReplicas != 0 || o.UpdatedReplicas != 0 ||
		o.ReadyReplicas != 0 || o.AvailableReplicas != 0 || o.ReadyEndpoints != 0 || o.Reason != v1.ServiceReason_SERVICE_REASON_UNSPECIFIED):
		return errors.New("absent service observation claims state")
	case o.Absent && desired == v1.ServiceDesired_SERVICE_DESIRED_PRESENT && p != v1.ServicePhase_SERVICE_PHASE_HELD:
		return errors.New("service observed absent is not held")
	case p == v1.ServicePhase_SERVICE_PHASE_DELETED && !o.Absent:
		return errors.New("deleted service is not observed absent")
	case p == v1.ServicePhase_SERVICE_PHASE_READY && !readyShape(o, desired, generation):
		return errors.New("ready service is not observed ready")
	}
	return nil
}

// readyShape checks a ready fixed service's observation: desired present,
// the current generation observed and ready, 1..8 replicas updated, ready
// and available, and between one and that count's ready endpoints. A full
// service must also bind the count to its current revision.
func readyShape(o *v1.ServiceObservation, desired v1.ServiceDesired, generation uint64) bool {
	return desired == v1.ServiceDesired_SERVICE_DESIRED_PRESENT && !o.Absent &&
		o.ObservedGeneration == generation && o.ReadyGeneration == generation &&
		o.DesiredReplicas >= 1 && o.DesiredReplicas <= maxFixedReplicas &&
		o.UpdatedReplicas == o.DesiredReplicas && o.ReadyReplicas == o.DesiredReplicas && o.AvailableReplicas == o.DesiredReplicas &&
		o.ReadyEndpoints >= 1 && o.ReadyEndpoints <= o.ReadyReplicas &&
		o.Reason == v1.ServiceReason_SERVICE_REASON_UNSPECIFIED
}

func lowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
