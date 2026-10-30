package servingwire

import (
	"errors"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const testImage = "registry.example.com/team/web@sha256:" +
	"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testSpec() *v1.ServiceSpec {
	return &v1.ServiceSpec{
		Image:     testImage,
		Argv:      []string{"/bin/web", "-addr=:8080"},
		Env:       []*v1.ServiceEnvVar{{Name: "B", Value: "2"}, {Name: "A", Value: "1"}},
		Port:      8080,
		Readiness: &v1.ServiceReadiness{Path: "/healthz"},
		Resources: &v1.ServiceResources{CpuMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   &v1.ServiceScaling{Replicas: 1},
	}
}

func withUnknown(m proto.Message) {
	var b []byte
	b = protowire.AppendTag(b, 999, protowire.VarintType)
	b = protowire.AppendVarint(b, 1)
	m.ProtoReflect().SetUnknown(b)
}

func TestSpecFromProto(t *testing.T) {
	s, err := SpecFromProto(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	if s.Env[0].Name != "A" || s.Readiness.PeriodSeconds != 10 || s.Rollout.ProgressDeadlineSeconds != 600 {
		t.Fatalf("not normalized: %+v", s)
	}
	back, err := SpecFromProto(SpecToProto(s))
	if err != nil {
		t.Fatal(err)
	}
	d1, _ := s.Digest()
	d2, _ := back.Digest()
	if d1 != d2 {
		t.Fatalf("round trip digest %s != %s", d2, d1)
	}
}

func TestSpecFromProtoRefuses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1.ServiceSpec)
		want   error
	}{
		{"mounts", func(m *v1.ServiceSpec) {
			m.Mounts = []*v1.ServiceMount{{Reference: "data", Destination: "/mnt/skygo/data"}}
		}, ErrUnsupported},
		{"workdir", func(m *v1.ServiceSpec) { m.Workdir = &v1.ArtifactRef{} }, ErrUnsupported},
		{"setup", func(m *v1.ServiceSpec) { m.Setup = []string{"make"} }, ErrUnsupported},
		{"cpu scaling", func(m *v1.ServiceSpec) {
			m.Scaling.Cpu = &v1.ServiceCpuScaling{MinReplicas: 1, MaxReplicas: 2}
		}, ErrUnsupported},
		{"nine replicas", func(m *v1.ServiceSpec) { m.Scaling.Replicas = 9 }, nil},
		{"zero replicas", func(m *v1.ServiceSpec) { m.Scaling = nil }, nil},
		{"unknown top", func(m *v1.ServiceSpec) { withUnknown(m) }, ErrUnknownFields},
		{"unknown readiness", func(m *v1.ServiceSpec) { withUnknown(m.Readiness) }, ErrUnknownFields},
		{"unknown env", func(m *v1.ServiceSpec) { withUnknown(m.Env[0]) }, ErrUnknownFields},
		{"unknown mount", func(m *v1.ServiceSpec) {
			m.Mounts = []*v1.ServiceMount{{}}
			withUnknown(m.Mounts[0])
		}, ErrUnknownFields},
		{"tagged image", func(m *v1.ServiceSpec) { m.Image = "registry.example.com/web:latest" }, nil},
		{"nil env", func(m *v1.ServiceSpec) { m.Env = append(m.Env, nil) }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testSpec()
			tt.mutate(m)
			_, err := SpecFromProto(m)
			if err == nil {
				t.Fatal("accepted")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
	if _, err := SpecFromProto(nil); err == nil {
		t.Fatal("nil spec accepted")
	}
}

const testServiceID = "0123456789abcdef0123456789abcdef"

// readyService returns a valid ready service of one revision and one
// complete create, as a server reports it.
func readyService(t *testing.T) *v1.Service {
	t.Helper()
	s, err := SpecFromProto(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	sum, err := s.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return &v1.Service{
		Id:             testServiceID,
		Name:           "web",
		Target:         "kube",
		RecordRevision: 3,
		Desired:        v1.ServiceDesired_SERVICE_DESIRED_PRESENT,
		History: &v1.ServiceHistory{
			Revisions: []*v1.ServiceRevision{{Number: 1, Spec: SpecToProto(s), Sha256: sum}},
			Rollouts: []*v1.ServiceRollout{{
				Generation: 1, Revision: 1,
				Kind:    v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_CREATE,
				Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_COMPLETE,
			}},
		},
		Status: &v1.ServiceStatus{
			Phase:             v1.ServicePhase_SERVICE_PHASE_READY,
			DesiredGeneration: 1,
			Observation:       readyObservation(1),
		},
	}
}

func readyObservation(generation uint64) *v1.ServiceObservation {
	return &v1.ServiceObservation{
		ObservedGeneration: generation, ReadyGeneration: generation,
		DesiredReplicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, ReadyEndpoints: 1,
	}
}

func retiredService() *v1.Service {
	return &v1.Service{
		Id:             testServiceID,
		Name:           "web",
		Target:         "kube",
		RecordRevision: 9,
		Desired:        v1.ServiceDesired_SERVICE_DESIRED_ABSENT,
		History:        &v1.ServiceHistory{},
		Status:         &v1.ServiceStatus{Phase: v1.ServicePhase_SERVICE_PHASE_DELETED},
		Retired:        true,
	}
}

// statusMutations are the status shapes no server reports, shared by the
// Service and Summary checks. Each applies to a ready status of
// generation 1.
var statusMutations = []struct {
	name   string
	mutate func(*v1.ServiceStatus)
}{
	{"no status phase", func(s *v1.ServiceStatus) { s.Phase = 0 }},
	{"unknown phase", func(s *v1.ServiceStatus) { s.Phase = 10 }},
	{"unknown status field", func(s *v1.ServiceStatus) { withUnknown(s) }},
	{"unknown observation field", func(s *v1.ServiceStatus) { withUnknown(s.Observation) }},
	{"ready no observation", func(s *v1.ServiceStatus) { s.Observation = nil }},
	{"ready all zero", func(s *v1.ServiceStatus) { s.Observation = &v1.ServiceObservation{} }},
	{"ready zero endpoints", func(s *v1.ServiceStatus) { s.Observation.ReadyEndpoints = 0 }},
	{"ready endpoints over ready", func(s *v1.ServiceStatus) { s.Observation.ReadyEndpoints = 2 }},
	{"ready nine replicas", func(s *v1.ServiceStatus) {
		o := s.Observation
		o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 9, 9, 9, 9
	}},
	{"ready not updated", func(s *v1.ServiceStatus) { s.Observation.UpdatedReplicas = 0 }},
	{"ready not available", func(s *v1.ServiceStatus) { s.Observation.AvailableReplicas = 0 }},
	{"available over ready", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_PROGRESSING
		s.Observation.ReadyReplicas, s.Observation.ReadyEndpoints = 0, 0
	}},
	{"ready generation behind", func(s *v1.ServiceStatus) { s.Observation.ReadyGeneration = 0 }},
	{"observed generation ahead", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.ObservedGeneration = 2
	}},
	{"ready with reason", func(s *v1.ServiceStatus) { s.Observation.Reason = v1.ServiceReason_SERVICE_REASON_CRASH_LOOP }},
	{"ready absent", func(s *v1.ServiceStatus) { s.Observation = &v1.ServiceObservation{Absent: true} }},
	{"absent with state", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.Absent = true
	}},
	{"absent present not held", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_UNAVAILABLE
		s.Observation = &v1.ServiceObservation{Absent: true}
	}},
	{"healthy scaling", func(s *v1.ServiceStatus) {
		s.Observation.Scaling = v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_HEALTHY
	}},
	{"held scaling", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.Scaling = v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_CONFLICT
	}},
	{"unknown scaling", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.Scaling = 99
	}},
	{"unknown reason", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.Reason = 99
	}},
	{"negative count", func(s *v1.ServiceStatus) {
		s.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
		s.Observation.ReadyEndpoints = -1
	}},
	{"deleted present", func(s *v1.ServiceStatus) { s.Phase = v1.ServicePhase_SERVICE_PHASE_DELETED }},
	{"deleting present", func(s *v1.ServiceStatus) { s.Phase = v1.ServicePhase_SERVICE_PHASE_DELETING }},
	{"duplicate condition", func(s *v1.ServiceStatus) {
		c := &v1.ServiceCondition{Code: v1.ServiceConditionCode_SERVICE_CONDITION_CODE_TARGET_UNAVAILABLE, Generation: 1}
		s.Conditions = []*v1.ServiceCondition{c, c}
	}},
	{"condition of a later generation", func(s *v1.ServiceStatus) {
		s.Conditions = []*v1.ServiceCondition{{Code: v1.ServiceConditionCode_SERVICE_CONDITION_CODE_TARGET_UNAVAILABLE, Generation: 2}}
	}},
}

func TestCheckService(t *testing.T) {
	if err := CheckService(readyService(t)); err != nil {
		t.Fatalf("ready service: %v", err)
	}
	if err := CheckService(retiredService()); err != nil {
		t.Fatalf("retired service: %v", err)
	}
	valid := []struct {
		name   string
		mutate func(*v1.Service)
	}{
		{"degraded", func(m *v1.Service) {
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_DEGRADED
		}},
		{"creating", func(m *v1.Service) {
			m.History.Rollouts[0].Outcome = v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING
			m.Status.Phase, m.Status.Observation = v1.ServicePhase_SERVICE_PHASE_CREATING, nil
		}},
		{"held identity conflict", func(m *v1.Service) {
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
			m.Status.Observation = &v1.ServiceObservation{Reason: v1.ServiceReason_SERVICE_REASON_IDENTITY_CONFLICT}
		}},
		{"held absent", func(m *v1.Service) {
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
			m.Status.Observation = &v1.ServiceObservation{Absent: true}
		}},
		{"deleting", func(m *v1.Service) {
			m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_DELETING
		}},
		{"deleting held", func(m *v1.Service) {
			m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_HELD
			m.Status.Observation = &v1.ServiceObservation{Reason: v1.ServiceReason_SERVICE_REASON_IDENTITY_CONFLICT}
		}},
		{"deleted unretired", func(m *v1.Service) {
			m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_DELETED
			m.Status.Observation = &v1.ServiceObservation{Absent: true}
		}},
		{"rollback", func(m *v1.Service) {
			m.History.Rollouts = append(m.History.Rollouts, &v1.ServiceRollout{
				Generation: 2, Revision: 1,
				Kind:    v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_ROLLBACK,
				Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING,
			})
			m.Status.DesiredGeneration = 2
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_PROGRESSING
		}},
	}
	for _, tt := range valid {
		t.Run("valid "+tt.name, func(t *testing.T) {
			m := readyService(t)
			tt.mutate(m)
			if err := CheckService(m); err != nil {
				t.Fatalf("refused: %v", err)
			}
		})
	}
	tests := []struct {
		name   string
		mutate func(*v1.Service)
	}{
		{"status ready two replicas", func(m *v1.Service) {
			o := m.Status.Observation
			o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 2, 2, 2, 2
		}},
		{"bad id", func(m *v1.Service) { m.Id = "x" }},
		{"bad name", func(m *v1.Service) { m.Name = "Web" }},
		{"no record revision", func(m *v1.Service) { m.RecordRevision = 0 }},
		{"no desired", func(m *v1.Service) { m.Desired = 0 }},
		{"no status", func(m *v1.Service) { m.Status = nil }},
		{"no history", func(m *v1.Service) { m.History = nil }},
		{"retired present", func(m *v1.Service) { m.Retired = true }},
		{"reapply", func(m *v1.Service) {
			m.History.Rollouts[0].Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_REAPPLY
		}},
		{"reapply later", func(m *v1.Service) {
			m.History.Rollouts = append(m.History.Rollouts, &v1.ServiceRollout{
				Generation: 2, Revision: 1,
				Kind:    v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_REAPPLY,
				Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING,
			})
			m.Status.DesiredGeneration = 2
			m.Status.Phase, m.Status.Observation = v1.ServicePhase_SERVICE_PHASE_HELD, nil
		}},
		{"unknown kind", func(m *v1.Service) { m.History.Rollouts[0].Kind = 9 }},
		{"first rollout an update", func(m *v1.Service) {
			m.History.Rollouts[0].Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_UPDATE
		}},
		{"later create", func(m *v1.Service) {
			m.History.Rollouts = append(m.History.Rollouts, &v1.ServiceRollout{
				Generation: 2, Revision: 1,
				Kind:    v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_CREATE,
				Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_PENDING,
			})
			m.Status.DesiredGeneration = 2
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_PROGRESSING
		}},
		{"bad digest", func(m *v1.Service) { m.History.Revisions[0].Sha256 = testServiceID + testServiceID }},
		{"status of another rollout", func(m *v1.Service) { m.Status.DesiredGeneration = 2 }},
		{"absent desired ready", func(m *v1.Service) { m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT }},
		{"absent desired progressing", func(m *v1.Service) {
			m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			m.Status.Phase = v1.ServicePhase_SERVICE_PHASE_PROGRESSING
		}},
		{"deleted not observed absent", func(m *v1.Service) {
			m.Desired = v1.ServiceDesired_SERVICE_DESIRED_ABSENT
			m.Status.Phase, m.Status.Observation = v1.ServicePhase_SERVICE_PHASE_DELETED, nil
		}},
		{"retired with observation", func(m *v1.Service) {
			*m = *retiredService()
			m.Status.Observation = &v1.ServiceObservation{Absent: true}
		}},
		{"retired with history", func(m *v1.Service) {
			h := m.History
			*m = *retiredService()
			m.History = h
		}},
	}
	for _, tt := range statusMutations {
		tests = append(tests, struct {
			name   string
			mutate func(*v1.Service)
		}{"status " + tt.name, func(m *v1.Service) { tt.mutate(m.Status) }})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := readyService(t)
			tt.mutate(m)
			if CheckService(m) == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func readySummary(t *testing.T) *v1.ServiceSummary {
	m := readyService(t)
	return &v1.ServiceSummary{
		Id:             m.Id,
		Name:           m.Name,
		Target:         m.Target,
		RecordRevision: m.RecordRevision,
		Desired:        m.Desired,
		Current:        m.History.Rollouts[0],
		Sha256:         m.History.Revisions[0].Sha256,
		Status:         m.Status,
	}
}

func TestCheckSummary(t *testing.T) {
	if err := CheckSummary(readySummary(t)); err != nil {
		t.Fatalf("ready summary: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*v1.ServiceSummary)
	}{
		{"no current", func(m *v1.ServiceSummary) { m.Current = nil }},
		{"bad digest", func(m *v1.ServiceSummary) { m.Sha256 = "x" }},
		{"reapply", func(m *v1.ServiceSummary) { m.Current.Kind = v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_REAPPLY }},
		{"create of generation 2", func(m *v1.ServiceSummary) {
			m.Current.Generation, m.Status.DesiredGeneration = 2, 2
			m.Status.Observation = readyObservation(2)
		}},
	}
	for _, tt := range statusMutations {
		tests = append(tests, struct {
			name   string
			mutate func(*v1.ServiceSummary)
		}{"status " + tt.name, func(m *v1.ServiceSummary) { tt.mutate(m.Status) }})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := readySummary(t)
			tt.mutate(m)
			if CheckSummary(m) == nil {
				t.Fatal("accepted")
			}
		})
	}
}
