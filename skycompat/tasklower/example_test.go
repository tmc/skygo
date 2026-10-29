package tasklower_test

import (
	"fmt"
	"strings"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
)

func ExampleLower() {
	src := "run: echo hello\nresources:\n  cpus: 0.5\n  memory: 512MB\n"
	d, err := taskyaml.Parse(strings.NewReader(src), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	task, notes, err := tasklower.Lower(d, tasklower.Options{Shell: "/bin/sh"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(task.Argv, task.CpuMillis, task.MemoryBytes)
	for _, n := range notes {
		fmt.Println(n.Path, n.Status, n.Detail)
	}
	// Output:
	// [/bin/sh -c echo hello] 500 536870912
	// run lowered argv is shell -c text
	// resources default fields noted separately
	// resources.cpus lowered cpu_millis
	// resources.memory lowered memory_bytes
}

func ExampleLower_runtimeEnvironment() {
	d, err := taskyaml.Parse(strings.NewReader("run: echo $SKYPILOT_NODE_RANK\n"), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	task, _, err := tasklower.Lower(d, tasklower.Options{Shell: "/bin/sh"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(task.Env["SKYPILOT_NODE_RANK"], task.Env["SKYPILOT_NUM_NODES"], task.Env["SKYPILOT_NUM_GPUS_PER_NODE"])
	// Output: 0 1 0
}

func ExampleLower_refused() {
	d, err := taskyaml.Parse(strings.NewReader("run: echo hello\nnum_nodes: 2\n"), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, _, err = tasklower.Lower(d, tasklower.Options{Shell: "/bin/sh"})
	fmt.Println(err)
	// Output: task yaml: unsupported num_nodes at 2:12
}

func ExampleLower_choice() {
	src := "run: nvidia-smi\nresources:\n  accelerators: {L4:1, A10G:1}\n"
	d, err := taskyaml.Parse(strings.NewReader(src), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	_, _, err = tasklower.Lower(d, tasklower.Options{Shell: "/bin/sh"})
	fmt.Println(err)
	// Output: task yaml: unsupported resources.accelerators at 3:17
}

func ExampleMarshal() {
	b, err := tasklower.Marshal(&skyv1.Task{
		Argv:    []string{"/bin/sh", "-c", "make test\n"},
		Env:     map[string]string{"MODE": "ci"},
		Workdir: &skyv1.ArtifactRef{Id: "00112233445566778899aabbccddeeff", Sha256: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", Size: 4096},
		Outputs: []string{"out"},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Print(string(b))
	// Output:
	// {
	//   "argv": ["/bin/sh", "-c", "make test\n"],
	//   "env": {
	//     "MODE": "ci"
	//   },
	//   "workdir": {
	//     "id": "00112233445566778899aabbccddeeff",
	//     "sha256": "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
	//     "size": 4096
	//   },
	//   "outputs": ["out"]
	// }
}
