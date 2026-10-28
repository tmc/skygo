package environment_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/environment"
)

func Example() {
	s := environment.Spec{
		Name:         "analysis",
		Target:       "kube",
		Image:        "registry.example/task:v1",
		CPUMillis:    500,
		MemoryBytes:  1 << 30,
		NodeSelector: map[string]string{},
		Port:         environment.Port,
	}
	data, err := environment.Canonical(s)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	// Output:
	// {"version":1,"name":"analysis","target":"kube","image":"registry.example/task:v1","cpu_millis":500,"memory_bytes":1073741824,"port":8080}
}

func ExampleNormalize() {
	selector := map[string]string{"pool": "gpu"}
	s, err := environment.Normalize(environment.Spec{Name: "train", Target: "kube", Image: "task:v1", CPUMillis: 1000, MemoryBytes: 1 << 30, NodeSelector: selector})
	if err != nil {
		fmt.Println(err)
		return
	}
	selector["pool"] = "cpu"
	fmt.Println(s.NodeSelector["pool"])
	// Output: gpu
}

func ExampleParse() {
	_, err := environment.Parse([]byte(`{"version":3,"name":"analysis"}`))
	fmt.Println(err)
	// Output: unsupported spec version 3
}

func ExampleValidName() {
	fmt.Println(environment.ValidName("analysis-2"))
	fmt.Println(environment.ValidName("Analysis"))
	fmt.Println(environment.ValidName("analysis-"))
	// Output:
	// true
	// false
	// false
}

func ExampleValidTarget() {
	fmt.Println(environment.ValidTarget("kube east"))
	fmt.Println(environment.ValidTarget("kube\n"))
	fmt.Println(environment.ValidTarget(""))
	// Output:
	// true
	// false
	// false
}

func ExampleState_Next() {
	deleting := environment.State{Desired: environment.Absent, Phase: environment.Deleting}
	fmt.Println(deleting.Next(environment.State{Desired: environment.Present, Phase: environment.Ready}))
	fmt.Println(deleting.Next(environment.State{Desired: environment.Absent, Phase: environment.Deleted}))
	// Output:
	// absent environment cannot become present
	// <nil>
}
