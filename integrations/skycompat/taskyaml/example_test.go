package taskyaml_test

import (
	"fmt"
	"strings"

	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
)

func Example() {
	d, err := taskyaml.Parse(strings.NewReader("run: echo hello\n"), taskyaml.Options{})
	fmt.Println(*d.Task.Run, err)
	// Output: echo hello <nil>
}

func ExampleParse() {
	d, err := taskyaml.Parse(strings.NewReader("resources: {autostop: 10m}\n"), taskyaml.Options{})
	fmt.Println(*d.Task.Resources.Autostop.IdleMinutes, err)
	// Output: 10 <nil>
}

func ExampleParseDAG() {
	d, err := taskyaml.ParseDAG(strings.NewReader("name: example\n---\nrun: echo hello\n"), taskyaml.Options{})
	fmt.Println(*d.DAGName, *d.Task.Run, err)
	// Output: example echo hello <nil>
}
