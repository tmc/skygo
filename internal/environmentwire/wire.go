package environmentwire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/environment"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxSessions bounds open_sessions and unknown_sessions together.
const maxSessions = 8

// SpecFromProto returns the normalized spec m names. It refuses a nil spec or
// mount and unknown fields or enum values anywhere in m.
func SpecFromProto(m *v1.EnvironmentSpec) (environment.Spec, error) {
	if m == nil {
		return environment.Spec{}, errors.New("missing environment spec")
	}
	for _, a := range m.Artifacts {
		if a == nil {
			return environment.Spec{}, errors.New("missing artifact mount")
		}
	}
	if undeclared(m.ProtoReflect()) {
		return environment.Spec{}, errors.New("environment spec has unknown fields or enum values")
	}
	s := environment.Spec{
		Name:               m.Name,
		Target:             m.Target,
		Image:              m.Image,
		CPUMillis:          m.CpuMillis,
		MemoryBytes:        m.MemoryBytes,
		CPULimitMillis:     m.CpuLimitMillis,
		MemoryLimitBytes:   m.MemoryLimitBytes,
		Accelerator:        m.Accelerator,
		AcceleratorCount:   m.AcceleratorCount,
		NodeSelector:       maps.Clone(m.NodeSelector),
		StorageAlias:       m.StorageAlias,
		Port:               m.Port,
		IdleTimeoutSeconds: m.IdleTimeoutSeconds,
	}
	for _, a := range m.Artifacts {
		s.Artifacts = append(s.Artifacts, environment.ArtifactMount{ID: a.Id, SHA256: a.Sha256, Size: a.Size, MountPath: a.MountPath})
	}
	return environment.Normalize(s)
}

// SpecToProto returns the wire form of s. A spec without artifacts has no
// artifacts field, so its encoding is that of a version 1 spec.
func SpecToProto(s environment.Spec) *v1.EnvironmentSpec {
	m := &v1.EnvironmentSpec{
		Name:               s.Name,
		Target:             s.Target,
		Image:              s.Image,
		CpuMillis:          s.CPUMillis,
		MemoryBytes:        s.MemoryBytes,
		CpuLimitMillis:     s.CPULimitMillis,
		MemoryLimitBytes:   s.MemoryLimitBytes,
		Accelerator:        s.Accelerator,
		AcceleratorCount:   s.AcceleratorCount,
		NodeSelector:       maps.Clone(s.NodeSelector),
		StorageAlias:       s.StorageAlias,
		Port:               s.Port,
		IdleTimeoutSeconds: s.IdleTimeoutSeconds,
	}
	for _, a := range s.Artifacts {
		m.Artifacts = append(m.Artifacts, &v1.EnvironmentArtifactMount{Id: a.ID, Sha256: a.SHA256, Size: a.Size, MountPath: a.MountPath})
	}
	return m
}

// SpecSHA256 returns the lowercase hexadecimal SHA-256 of the canonical
// encoding of s: the spec_sha256 a server records for s.
func SpecSHA256(s environment.Spec) (string, error) {
	raw, err := environment.Canonical(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// CheckEnvironment checks an environment a server returned: its identifier,
// spec and digest, a valid desired state and phase, and its session counts.
func CheckEnvironment(e *v1.Environment) error {
	if e == nil {
		return errors.New("server returned no environment")
	}
	if !environment.ValidID(e.Id) {
		return errors.New("server returned an invalid environment ID")
	}
	if err := checkSpec(e.Spec, e.SpecSha256); err != nil {
		return err
	}
	if undeclared(e.ProtoReflect()) {
		return errors.New("server returned an environment with unknown fields or enum values")
	}
	st := environment.State{Desired: environment.Desired(e.Desired), Phase: environment.Phase(e.Phase)}
	if !st.Valid() {
		return fmt.Errorf("server returned an invalid environment state %v/%v", e.Desired, e.Phase)
	}
	if e.OpenSessions > maxSessions || e.UnknownSessions > maxSessions-e.OpenSessions {
		return errors.New("server returned more than 8 sessions")
	}
	return nil
}

// CheckPlan checks a plan a server returned for spec: the returned spec is
// spec, support is SUPPORTED or UNSUPPORTED, and the artifacts fact is
// UNSPECIFIED exactly when spec has no artifacts. It never reads VERIFIED
// from an absent fact.
func CheckPlan(r *v1.PlanEnvironmentResponse, spec environment.Spec) error {
	if r == nil {
		return errors.New("server returned no plan")
	}
	want, err := SpecSHA256(spec)
	if err != nil {
		return err
	}
	if err := checkSpec(r.Spec, r.SpecSha256); err != nil {
		return err
	}
	if r.SpecSha256 != want {
		return errors.New("server planned a different spec")
	}
	if undeclared(r.ProtoReflect()) {
		return errors.New("server returned a plan with unknown fields or enum values")
	}
	switch r.Support {
	case v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_SUPPORTED, v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_UNSUPPORTED:
	default:
		return errors.New("server returned no plan support")
	}
	if (r.Artifacts == v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNSPECIFIED) != (len(spec.Artifacts) == 0) {
		return errors.New("server returned an inconsistent artifacts fact")
	}
	return nil
}

// checkSpec checks that m is a valid spec whose digest is sum.
func checkSpec(m *v1.EnvironmentSpec, sum string) error {
	s, err := SpecFromProto(m)
	if err != nil {
		return fmt.Errorf("server returned an invalid spec: %w", err)
	}
	got, err := SpecSHA256(s)
	if err != nil {
		return fmt.Errorf("server returned an invalid spec: %w", err)
	}
	if got != sum {
		return errors.New("server returned a spec_sha256 that does not match its spec")
	}
	return nil
}

// undeclared reports whether m, or any message it holds, has unknown fields
// or an enum value its enum does not declare. Environment messages have no
// message- or enum-valued maps.
func undeclared(m protoreflect.Message) bool {
	if !m.IsValid() {
		return false
	}
	if len(m.GetUnknown()) != 0 {
		return true
	}
	found := false
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
		case fd.IsList():
			l := v.List()
			for i := range l.Len() {
				if fd.Message() != nil && undeclared(l.Get(i).Message()) ||
					fd.Enum() != nil && fd.Enum().Values().ByNumber(l.Get(i).Enum()) == nil {
					found = true
				}
			}
		case fd.Message() != nil:
			found = undeclared(v.Message())
		case fd.Enum() != nil:
			found = fd.Enum().Values().ByNumber(v.Enum()) == nil
		}
		return !found
	})
	return found
}
