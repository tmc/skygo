package servingwire

import (
	"errors"
	"fmt"
	"testing"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
)

func TestFixedReplicaSpecRoundTrip(t *testing.T) {
	for _, count := range []int32{1, 2, 8} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			m := testSpec()
			m.Scaling.Replicas = count
			s, err := SpecFromProto(m)
			if err != nil {
				t.Fatal(err)
			}
			if s.Scaling.Replicas != count {
				t.Fatalf("replicas = %d, want %d", s.Scaling.Replicas, count)
			}
			wire := SpecToProto(s)
			back, err := SpecFromProto(wire)
			if err != nil {
				t.Fatal(err)
			}
			beforeDigest, _ := s.Digest()
			afterDigest, _ := back.Digest()
			if back.Scaling.Replicas != count || beforeDigest != afterDigest {
				t.Fatalf("round trip changed count or spec: %d, %s != %s", back.Scaling.Replicas, afterDigest, beforeDigest)
			}
			wire.Scaling.Replicas = 1
			if s.Scaling.Replicas != count || back.Scaling.Replicas != count {
				t.Fatal("wire result aliases portable spec")
			}
		})
	}
}

func TestFixedReplicaSpecRefuses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1.ServiceSpec)
		want   error
	}{
		{"negative", func(m *v1.ServiceSpec) { m.Scaling.Replicas = -1 }, nil},
		{"zero", func(m *v1.ServiceSpec) { m.Scaling.Replicas = 0 }, nil},
		{"nine", func(m *v1.ServiceSpec) { m.Scaling.Replicas = 9 }, nil},
		{"missing scaling", func(m *v1.ServiceSpec) { m.Scaling = nil }, nil},
		{"cpu", func(m *v1.ServiceSpec) { m.Scaling.Cpu = &v1.ServiceCpuScaling{} }, ErrUnsupported},
		{"mount", func(m *v1.ServiceSpec) {
			m.Mounts = []*v1.ServiceMount{{Reference: "data", Destination: "/mnt/skygo/data"}}
		}, ErrUnsupported},
		{"workdir", func(m *v1.ServiceSpec) { m.Workdir = &v1.ArtifactRef{} }, ErrUnsupported},
		{"setup", func(m *v1.ServiceSpec) { m.Setup = []string{"/bin/setup"} }, ErrUnsupported},
		{"unknown scaling", func(m *v1.ServiceSpec) { withUnknown(m.Scaling) }, ErrUnknownFields},
		{"unknown cpu", func(m *v1.ServiceSpec) {
			m.Scaling.Cpu = &v1.ServiceCpuScaling{}
			withUnknown(m.Scaling.Cpu)
		}, ErrUnknownFields},
		{"unknown workdir", func(m *v1.ServiceSpec) {
			m.Workdir = &v1.ArtifactRef{}
			withUnknown(m.Workdir)
		}, ErrUnknownFields},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := testSpec()
			m.Scaling.Replicas = 2
			tt.mutate(m)
			if _, err := SpecFromProto(m); err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("SpecFromProto error = %v, want refusal %v", err, tt.want)
			}
		})
	}
}

func fixedReplicaService(t *testing.T, count int32) *v1.Service {
	t.Helper()
	m := readyService(t)
	p := testSpec()
	p.Scaling.Replicas = count
	s, err := SpecFromProto(p)
	if err != nil {
		t.Fatal(err)
	}
	sum, err := s.Digest()
	if err != nil {
		t.Fatal(err)
	}
	m.History.Revisions[0].Spec = SpecToProto(s)
	m.History.Revisions[0].Sha256 = sum
	o := m.Status.Observation
	o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas, o.ReadyEndpoints = count, count, count, count, count
	return m
}

func fixedReplicaSummary(m *v1.Service) *v1.ServiceSummary {
	return &v1.ServiceSummary{
		Id: m.Id, Name: m.Name, Target: m.Target,
		RecordRevision: m.RecordRevision, Desired: m.Desired,
		Current: m.History.Rollouts[len(m.History.Rollouts)-1],
		Sha256:  m.History.Revisions[0].Sha256,
		Status:  m.Status,
	}
}

func TestFixedReplicaReadyResponses(t *testing.T) {
	for _, count := range []int32{1, 2, 8} {
		counts := []int32{1}
		if count > 1 {
			counts = append(counts, count)
		}
		for _, endpoints := range counts {
			t.Run(fmt.Sprintf("%d/%d endpoints", count, endpoints), func(t *testing.T) {
				m := fixedReplicaService(t, count)
				m.Status.Observation.ReadyEndpoints = endpoints
				if err := CheckService(m); err != nil {
					t.Fatalf("CheckService: %v", err)
				}
				if err := CheckSummary(fixedReplicaSummary(m)); err != nil {
					t.Fatalf("CheckSummary: %v", err)
				}
			})
		}
	}
}

func TestFixedReplicaReadyBindsCurrentRevision(t *testing.T) {
	m := fixedReplicaService(t, 2)
	other := fixedReplicaService(t, 8).History.Revisions[0]
	other.Number = 2
	m.History.Revisions = append(m.History.Revisions, other)
	m.History.Rollouts = append(m.History.Rollouts,
		&v1.ServiceRollout{Generation: 2, Revision: 2, Kind: v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_UPDATE, Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_COMPLETE},
		&v1.ServiceRollout{Generation: 3, Revision: 1, Kind: v1.ServiceRolloutKind_SERVICE_ROLLOUT_KIND_ROLLBACK, Outcome: v1.ServiceRolloutOutcome_SERVICE_ROLLOUT_OUTCOME_COMPLETE})
	m.Status.DesiredGeneration = 3
	m.Status.Observation.ObservedGeneration, m.Status.Observation.ReadyGeneration = 3, 3
	if err := CheckService(m); err != nil {
		t.Fatalf("current revision with two replicas: %v", err)
	}
	o := m.Status.Observation
	o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas, o.ReadyEndpoints = 8, 8, 8, 8, 8
	if CheckService(m) == nil {
		t.Fatal("accepted count of newest retained revision instead of current rollout")
	}
}

func TestFixedReplicaReadyRefuses(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1.Service)
	}{
		{"two for spec one", func(m *v1.Service) {
			o := m.Status.Observation
			o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 2, 2, 2, 2
		}},
		{"eight for spec two", func(m *v1.Service) {
			o := m.Status.Observation
			o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 8, 8, 8, 8
		}},
		{"partially ready", func(m *v1.Service) { m.Status.Observation.ReadyReplicas = 1 }},
		{"partially updated", func(m *v1.Service) { m.Status.Observation.UpdatedReplicas = 1 }},
		{"partially available", func(m *v1.Service) { m.Status.Observation.AvailableReplicas = 1 }},
		{"zero endpoints", func(m *v1.Service) { m.Status.Observation.ReadyEndpoints = 0 }},
		{"too many endpoints", func(m *v1.Service) { m.Status.Observation.ReadyEndpoints = 3 }},
		{"stale observed generation", func(m *v1.Service) { m.Status.Observation.ObservedGeneration = 0 }},
		{"stale ready generation", func(m *v1.Service) { m.Status.Observation.ReadyGeneration = 0 }},
		{"scaling", func(m *v1.Service) {
			m.Status.Observation.Scaling = v1.ServiceScalingCondition_SERVICE_SCALING_CONDITION_HEALTHY
		}},
		{"unknown observation", func(m *v1.Service) { withUnknown(m.Status.Observation) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count := int32(2)
			if tt.name == "two for spec one" {
				count = 1
			}
			m := fixedReplicaService(t, count)
			tt.mutate(m)
			if CheckService(m) == nil {
				t.Fatal("accepted invalid ready response")
			}
		})
	}
}

func TestFixedReplicaSummaryHasNoSpec(t *testing.T) {
	m := readySummary(t)
	o := m.Status.Observation
	o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = 2, 2, 2, 2
	if err := CheckSummary(m); err != nil {
		t.Fatalf("summary cannot infer a count from its opaque digest: %v", err)
	}
	for _, count := range []int32{0, 9} {
		bad := proto.Clone(m).(*v1.ServiceSummary)
		o := bad.Status.Observation
		o.DesiredReplicas, o.UpdatedReplicas, o.ReadyReplicas, o.AvailableReplicas = count, count, count, count
		if CheckSummary(bad) == nil {
			t.Fatalf("accepted ready summary count %d", count)
		}
	}
}
