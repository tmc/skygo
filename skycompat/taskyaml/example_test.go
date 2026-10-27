package taskyaml_test

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/skycompat/taskyaml"
)

func Example() {
	d, err := taskyaml.Parse(strings.NewReader("run: echo hello\nenvs: {COUNT: 2}\n"), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(*d.Task.Run, d.Task.Envs["COUNT"])
	// Output: echo hello 2
}

func ExampleParse() {
	d, err := taskyaml.Parse(strings.NewReader("envs: {TOKEN: null}\n"), taskyaml.Options{EnvOverrides: map[string]string{"TOKEN": "explicit"}})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(d.Task.Envs["TOKEN"])
	// Output: explicit
}

func ExampleParseDAG() {
	d, err := taskyaml.ParseDAG(strings.NewReader("name: example\n---\nrun: echo hello\n"), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(*d.DAGName, *d.Task.Run)
	// Output: example echo hello
}

func ExampleError() {
	_, err := taskyaml.Parse(strings.NewReader("envs: {TOKEN: null}\n"), taskyaml.Options{})
	fmt.Println(err)
	// Output: task yaml: invalid envs at 1:15
}

func ExampleResources() {
	src := "run: nvidia-smi\nresources:\n  accelerators: {L4:1, A10G:1}\n  disk_size: 128\n  infra: ssh/dgx\n  autostop: {idle_minutes: 30, wait_for: jobs_and_ssh}\n  job_recovery: {strategy: FAILOVER, recover_on_exit_codes: [33]}\n"
	d, err := taskyaml.Parse(strings.NewReader(src), taskyaml.Options{})
	if err != nil {
		fmt.Println(err)
		return
	}
	r := d.Task.Resources
	for _, a := range r.Accelerators {
		fmt.Println(a.Name, a.Count.Value)
	}
	fmt.Println(r.DiskSize.Value, *r.Infra)
	fmt.Println(r.Autostop.Enabled, *r.Autostop.IdleMinutes, *r.Autostop.WaitFor)
	fmt.Println(*r.JobRecovery.Strategy, r.JobRecovery.RecoverOnExitCodes)
	// Output:
	// L4 1
	// A10G 1
	// 128 ssh/dgx
	// true 30 jobs_and_ssh
	// FAILOVER [33]
}
