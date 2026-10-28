package environmentwire_test

import (
	"fmt"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/environmentwire"
)

func Example() {
	spec, err := environmentwire.SpecFromProto(&v1.EnvironmentSpec{
		Name:        "analysis",
		Target:      "kube",
		Image:       "registry.example/task:v1",
		CpuMillis:   500,
		MemoryBytes: 1 << 30,
		Port:        8080,
		Artifacts:   []*v1.EnvironmentArtifactMount{},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	sum, err := environmentwire.SpecSHA256(spec)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(spec.Artifacts == nil, sum)
	// Output: true ce4fe85a00389a5f91b9d477a27c9549b43e9b937c962953b02db3d116bfbc1c
}

func ExampleCheckPlan() {
	spec, err := environmentwire.SpecFromProto(&v1.EnvironmentSpec{Name: "analysis", Target: "kube", Image: "task:v1", CpuMillis: 500, MemoryBytes: 1 << 30})
	if err != nil {
		fmt.Println(err)
		return
	}
	sum, err := environmentwire.SpecSHA256(spec)
	if err != nil {
		fmt.Println(err)
		return
	}
	plan := &v1.PlanEnvironmentResponse{Spec: environmentwire.SpecToProto(spec), SpecSha256: sum}
	fmt.Println(environmentwire.CheckPlan(plan, spec))
	plan.Support = v1.EnvironmentPlanSupport_ENVIRONMENT_PLAN_SUPPORT_SUPPORTED
	fmt.Println(environmentwire.CheckPlan(plan, spec))
	// Output:
	// server returned no plan support
	// <nil>
}
