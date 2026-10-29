package servingwire_test

import (
	"fmt"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/servingwire"
)

func ExampleSpecFromProto() {
	s, err := servingwire.SpecFromProto(&v1.ServiceSpec{
		Image:     "registry.example.com/web@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Argv:      []string{"/bin/web"},
		Port:      8080,
		Readiness: &v1.ServiceReadiness{Path: "/healthz"},
		Resources: &v1.ServiceResources{CpuMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   &v1.ServiceScaling{Replicas: 1},
	})
	fmt.Println(s.Readiness.PeriodSeconds, err)

	_, err = servingwire.SpecFromProto(&v1.ServiceSpec{Setup: []string{"make"}})
	fmt.Println(err)
	// Output:
	// 10 <nil>
	// unsupported service feature: setup
}
