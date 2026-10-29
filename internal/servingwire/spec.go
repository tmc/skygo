package servingwire

import (
	"errors"
	"fmt"
	"slices"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/serving"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrUnsupported reports a spec feature that this endpoint does not accept.
// It is refused before acceptance and before any effect.
var ErrUnsupported = errors.New("unsupported service feature")

// ErrUnknownFields reports a message with fields this endpoint does not
// declare.
var ErrUnknownFields = errors.New("message has unknown fields")

// Unknown reports ErrUnknownFields if any of ms, which may be nil, carries
// unknown fields. It checks only the messages given, not their children.
func Unknown(ms ...proto.Message) error {
	for _, m := range ms {
		if m == nil {
			continue
		}
		if r := m.ProtoReflect(); r.IsValid() && len(r.GetUnknown()) != 0 {
			return fmt.Errorf("%w: %s", ErrUnknownFields, r.Descriptor().Name())
		}
	}
	return nil
}

// SpecFromProto returns the normalized portable spec of m. It refuses a nil
// spec, unknown fields at any depth, and every feature this endpoint does
// not run: mounts, workdir, setup, CPU scaling and a replica count other
// than one.
func SpecFromProto(m *v1.ServiceSpec) (serving.Spec, error) {
	if m == nil {
		return serving.Spec{}, errors.New("missing service spec")
	}
	if err := specUnknown(m); err != nil {
		return serving.Spec{}, err
	}
	switch {
	case len(m.Mounts) != 0:
		return serving.Spec{}, fmt.Errorf("%w: mounts", ErrUnsupported)
	case m.Workdir != nil:
		return serving.Spec{}, fmt.Errorf("%w: workdir", ErrUnsupported)
	case len(m.Setup) != 0:
		return serving.Spec{}, fmt.Errorf("%w: setup", ErrUnsupported)
	case m.Scaling.GetCpu() != nil:
		return serving.Spec{}, fmt.Errorf("%w: cpu scaling", ErrUnsupported)
	case m.Scaling.GetReplicas() != 1:
		return serving.Spec{}, fmt.Errorf("%w: replicas other than 1", ErrUnsupported)
	}
	s := serving.Spec{
		Image: m.Image,
		Argv:  slices.Clone(m.Argv),
		Port:  m.Port,
		Readiness: serving.Readiness{
			Path:                m.Readiness.GetPath(),
			PeriodSeconds:       m.Readiness.GetPeriodSeconds(),
			TimeoutSeconds:      m.Readiness.GetTimeoutSeconds(),
			FailureThreshold:    m.Readiness.GetFailureThreshold(),
			InitialDelaySeconds: m.Readiness.GetInitialDelaySeconds(),
		},
		Resources: serving.Resources{
			CPUMillis:        m.Resources.GetCpuMillis(),
			MemoryBytes:      m.Resources.GetMemoryBytes(),
			CPULimitMillis:   m.Resources.GetCpuLimitMillis(),
			MemoryLimitBytes: m.Resources.GetMemoryLimitBytes(),
		},
		Rollout: serving.RolloutPolicy{
			ProgressDeadlineSeconds: m.Rollout.GetProgressDeadlineSeconds(),
			TerminationGraceSeconds: m.Rollout.GetTerminationGraceSeconds(),
		},
		Scaling: serving.Scaling{Replicas: m.Scaling.GetReplicas()},
	}
	for _, e := range m.Env {
		if e == nil {
			return serving.Spec{}, errors.New("missing env entry")
		}
		s.Env = append(s.Env, serving.EnvVar{Name: e.Name, Value: e.Value})
	}
	for _, l := range m.NodeSelector {
		if l == nil {
			return serving.Spec{}, errors.New("missing node selector entry")
		}
		s.NodeSelector = append(s.NodeSelector, serving.Label{Key: l.Key, Value: l.Value})
	}
	return s.Normalize()
}

// specUnknown refuses unknown fields in m and every message it holds.
func specUnknown(m *v1.ServiceSpec) error {
	ms := []proto.Message{m, m.Readiness, m.Resources, m.Rollout, m.Scaling, m.Scaling.GetCpu(), m.Workdir}
	for _, e := range m.Env {
		ms = append(ms, e)
	}
	for _, l := range m.NodeSelector {
		ms = append(ms, l)
	}
	for _, x := range m.Mounts {
		ms = append(ms, x)
	}
	return Unknown(ms...)
}

// SpecToProto returns the message of s. A spec this endpoint accepted has no
// mounts, workdir or setup.
func SpecToProto(s serving.Spec) *v1.ServiceSpec {
	m := &v1.ServiceSpec{
		Image: s.Image,
		Argv:  slices.Clone(s.Argv),
		Port:  s.Port,
		Readiness: &v1.ServiceReadiness{
			Path:                s.Readiness.Path,
			PeriodSeconds:       s.Readiness.PeriodSeconds,
			TimeoutSeconds:      s.Readiness.TimeoutSeconds,
			FailureThreshold:    s.Readiness.FailureThreshold,
			InitialDelaySeconds: s.Readiness.InitialDelaySeconds,
		},
		Resources: &v1.ServiceResources{
			CpuMillis:        s.Resources.CPUMillis,
			MemoryBytes:      s.Resources.MemoryBytes,
			CpuLimitMillis:   s.Resources.CPULimitMillis,
			MemoryLimitBytes: s.Resources.MemoryLimitBytes,
		},
		Rollout: &v1.ServiceRolloutPolicy{
			ProgressDeadlineSeconds: s.Rollout.ProgressDeadlineSeconds,
			TerminationGraceSeconds: s.Rollout.TerminationGraceSeconds,
		},
		Scaling: &v1.ServiceScaling{Replicas: s.Scaling.Replicas},
	}
	for _, e := range s.Env {
		m.Env = append(m.Env, &v1.ServiceEnvVar{Name: e.Name, Value: e.Value})
	}
	for _, l := range s.NodeSelector {
		m.NodeSelector = append(m.NodeSelector, &v1.ServiceLabel{Key: l.Key, Value: l.Value})
	}
	return m
}

// HistoryToProto returns the message of h.
func HistoryToProto(h serving.History) *v1.ServiceHistory {
	m := &v1.ServiceHistory{}
	for _, r := range h.Revisions {
		m.Revisions = append(m.Revisions, &v1.ServiceRevision{Number: r.Number, Spec: SpecToProto(r.Spec), Sha256: r.SHA256})
	}
	for _, r := range h.Rollouts {
		m.Rollouts = append(m.Rollouts, RolloutToProto(r))
	}
	return m
}

// RolloutToProto returns the message of r.
func RolloutToProto(r serving.Rollout) *v1.ServiceRollout {
	return &v1.ServiceRollout{
		Generation: r.Generation,
		Revision:   r.Revision,
		Kind:       v1.ServiceRolloutKind(r.Kind),
		Outcome:    v1.ServiceRolloutOutcome(r.Outcome),
	}
}

// ObservationToProto returns the message of o, or nil.
func ObservationToProto(o *serving.Observation) *v1.ServiceObservation {
	if o == nil {
		return nil
	}
	return &v1.ServiceObservation{
		ObservedGeneration: o.ObservedGeneration,
		ReadyGeneration:    o.ReadyGeneration,
		DesiredReplicas:    o.DesiredReplicas,
		UpdatedReplicas:    o.UpdatedReplicas,
		ReadyReplicas:      o.ReadyReplicas,
		AvailableReplicas:  o.AvailableReplicas,
		ReadyEndpoints:     o.ReadyEndpoints,
		Reason:             v1.ServiceReason(o.Reason),
		Absent:             o.Absent,
	}
}

// DesiredToProto returns the message value of d.
func DesiredToProto(d serving.Desired) v1.ServiceDesired {
	return v1.ServiceDesired(d)
}

// PhaseToProto returns the message value of p. The protocol declares the
// phases in the order of serving.Phase.
func PhaseToProto(p serving.Phase) v1.ServicePhase {
	return v1.ServicePhase(p)
}

// Timestamp returns t, or nil for the zero time.
func Timestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// ValidID reports whether s is a service ID: 32 lowercase hexadecimal
// digits.
func ValidID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
