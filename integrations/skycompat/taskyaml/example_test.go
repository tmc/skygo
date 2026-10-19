package taskyaml_test

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
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
