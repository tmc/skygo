package environmentwire

import (
	"strings"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/environment"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func wireSpec() *v1.EnvironmentSpec {
	return &v1.EnvironmentSpec{Name: "analysis", Target: "kube", Image: "task:v1", CpuMillis: 500, MemoryBytes: 1 << 30, NodeSelector: map[string]string{"pool": "cpu"}}
}

func mount(id byte, path string) *v1.EnvironmentArtifactMount {
	return &v1.EnvironmentArtifactMount{Id: strings.Repeat(string(id), 32), Sha256: strings.Repeat(string(id), 64), Size: 1, MountPath: path}
}

// unknown returns m with an unknown varint field 99 appended.
func unknown[M proto.Message](m M) M {
	m.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	return m
}

func TestSpecFromProto(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1.EnvironmentSpec)
		want string
	}{
		{"v1", func(*v1.EnvironmentSpec) {}, ""},
		{"empty_artifacts", func(m *v1.EnvironmentSpec) { m.Artifacts = []*v1.EnvironmentArtifactMount{} }, ""},
		{"v2_ordered", func(m *v1.EnvironmentSpec) {
			m.Artifacts = []*v1.EnvironmentArtifactMount{mount('b', "/skygo/artifacts/b"), mount('a', "/skygo/artifacts/a")}
		}, ""},
		{"nil_mount", func(m *v1.EnvironmentSpec) { m.Artifacts = []*v1.EnvironmentArtifactMount{nil} }, "missing artifact mount"},
		{"unknown_spec", func(m *v1.EnvironmentSpec) { unknown(m) }, "unknown fields"},
		{"unknown_mount", func(m *v1.EnvironmentSpec) {
			m.Artifacts = []*v1.EnvironmentArtifactMount{unknown(mount('a', "/skygo/artifacts/a"))}
		}, "unknown fields"},
		{"invalid", func(m *v1.EnvironmentSpec) { m.Port = 22 }, "port must be"},
		{"overlap", func(m *v1.EnvironmentSpec) {
			m.Artifacts = []*v1.EnvironmentArtifactMount{mount('a', "/skygo/artifacts/a"), mount('b', "/skygo/artifacts/a/b")}
		}, "overlap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := wireSpec()
			tc.edit(m)
			s, err := SpecFromProto(m)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("SpecFromProto error = %v, want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			back := SpecToProto(s)
			if !proto.Equal(back, m) {
				t.Fatalf("round trip = %v, want %v", back, m)
			}
			// The result shares no memory with m.
			m.NodeSelector["pool"] = "gpu"
			if len(m.Artifacts) != 0 {
				m.Artifacts[0].MountPath = "/skygo/artifacts/z"
			}
			if s.NodeSelector["pool"] != "cpu" || len(s.Artifacts) != 0 && s.Artifacts[0].MountPath == "/skygo/artifacts/z" {
				t.Fatal("spec aliases its message")
			}
		})
	}
	if _, err := SpecFromProto(nil); err == nil {
		t.Fatal("SpecFromProto(nil) succeeded")
	}
}

// TestSpecVersions checks that a spec without artifacts keeps its version 1
// encoding and digest, and that artifacts select version 2.
func TestSpecVersions(t *testing.T) {
	v1Spec, err := SpecFromProto(wireSpec())
	if err != nil {
		t.Fatal(err)
	}
	m := wireSpec()
	m.Artifacts = []*v1.EnvironmentArtifactMount{}
	empty, err := SpecFromProto(m)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := proto.Marshal(SpecToProto(v1Spec))
	b, _ := proto.Marshal(SpecToProto(empty))
	if string(a) != string(b) {
		t.Fatal("empty artifacts changed the wire encoding")
	}
	sa, _ := SpecSHA256(v1Spec)
	sb, _ := SpecSHA256(empty)
	if sa != sb {
		t.Fatal("empty artifacts changed the digest")
	}
	m.Artifacts = []*v1.EnvironmentArtifactMount{mount('a', "/skygo/artifacts/a")}
	v2Spec, err := SpecFromProto(m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := environment.Canonical(v2Spec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(raw), `{"version":2,`) {
		t.Fatalf("canonical = %s, want version 2", raw)
	}
}

func checkedEnvironment(t *testing.T) *v1.Environment {
	t.Helper()
	s, err := SpecFromProto(wireSpec())
	if err != nil {
		t.Fatal(err)
	}
	sum, err := SpecSHA256(s)
	if err != nil {
		t.Fatal(err)
	}
	return &v1.Environment{Id: strings.Repeat("1", 32), Spec: SpecToProto(s), SpecSha256: sum, Desired: v1.EnvironmentDesired_ENVIRONMENT_DESIRED_PRESENT, Phase: v1.EnvironmentPhase_ENVIRONMENT_PHASE_READY}
}

func TestCheckEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1.Environment)
		want string
	}{
		{"ok", func(*v1.Environment) {}, ""},
		{"deleted", func(e *v1.Environment) {
			e.Desired, e.Phase = v1.EnvironmentDesired_ENVIRONMENT_DESIRED_ABSENT, v1.EnvironmentPhase_ENVIRONMENT_PHASE_DELETED
		}, ""},
		{"bad_id", func(e *v1.Environment) { e.Id = "x" }, "invalid environment ID"},
		{"no_spec", func(e *v1.Environment) { e.Spec = nil }, "missing environment spec"},
		{"foreign_digest", func(e *v1.Environment) { e.SpecSha256 = strings.Repeat("0", 64) }, "does not match"},
		{"unknown_field", func(e *v1.Environment) { unknown(e) }, "unknown fields"},
		{"unknown_nested", func(e *v1.Environment) { unknown(e.Spec) }, "unknown fields"},
		{"unknown_phase", func(e *v1.Environment) { e.Phase = 9 }, "unknown fields or enum values"},
		{"unspecified_desired", func(e *v1.Environment) { e.Desired = 0 }, "invalid environment state"},
		{"present_deleted", func(e *v1.Environment) { e.Phase = v1.EnvironmentPhase_ENVIRONMENT_PHASE_DELETED }, "invalid environment state"},
		{"sessions", func(e *v1.Environment) { e.OpenSessions, e.UnknownSessions = 5, 4 }, "more than 8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := checkedEnvironment(t)
			tc.edit(e)
			err := CheckEnvironment(e)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckEnvironment error = %v, want %q", err, tc.want)
			}
		})
	}
	if err := CheckEnvironment(nil); err == nil {
		t.Fatal("CheckEnvironment(nil) succeeded")
	}
}

func TestCheckPlan(t *testing.T) {
	const (
		supported   = v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_SUPPORTED
		unsupported = v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_UNSUPPORTED
		none        = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNSPECIFIED
		unknownFact = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_UNKNOWN
		verified    = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_VERIFIED
		refused     = v1.EnvironmentPlanFact_ENVIRONMENT_PLAN_FACT_REFUSED
	)
	plain, err := SpecFromProto(wireSpec())
	if err != nil {
		t.Fatal(err)
	}
	m := wireSpec()
	m.Artifacts = []*v1.EnvironmentArtifactMount{mount('a', "/skygo/artifacts/a")}
	mounted, err := SpecFromProto(m)
	if err != nil {
		t.Fatal(err)
	}
	other := plain.Clone()
	other.Name = "other"
	for _, tc := range []struct {
		name     string
		spec     environment.Spec
		returned environment.Spec
		support  v1.EnvironmentPlanSupport
		fact     v1.EnvironmentPlanFact
		want     string
	}{
		{"supported", plain, plain, supported, none, ""},
		{"unsupported", plain, plain, unsupported, none, ""},
		{"artifacts_unknown", mounted, mounted, supported, unknownFact, ""},
		{"artifacts_verified", mounted, mounted, supported, verified, ""},
		{"artifacts_refused", mounted, mounted, unsupported, refused, ""},
		{"no_support", plain, plain, 0, none, "no plan support"},
		{"unknown_support", plain, plain, 7, none, "unknown fields or enum values"},
		{"unknown_fact", mounted, mounted, supported, 9, "unknown fields or enum values"},
		// An absent fact is never read as verified.
		{"artifacts_absent_fact", mounted, mounted, supported, none, "inconsistent artifacts fact"},
		{"fact_without_artifacts", plain, plain, supported, verified, "inconsistent artifacts fact"},
		{"different_spec", plain, other, supported, none, "different spec"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sum, err := SpecSHA256(tc.returned)
			if err != nil {
				t.Fatal(err)
			}
			r := &v1.PlanEnvironmentResponse{Spec: SpecToProto(tc.returned), SpecSha256: sum, Support: tc.support, Artifacts: tc.fact}
			err = CheckPlan(r, tc.spec)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CheckPlan error = %v, want %q", err, tc.want)
			}
		})
	}
	if err := CheckPlan(nil, plain); err == nil {
		t.Fatal("CheckPlan(nil) succeeded")
	}
}
