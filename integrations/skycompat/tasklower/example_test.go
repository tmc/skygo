package tasklower_test

import (
	"fmt"
	"strings"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/skycompat/tasklower"
	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
)

func ExampleLower() {
	d, _ := taskyaml.Parse(strings.NewReader("run: echo hello\n"), taskyaml.Options{})
	task, _, err := tasklower.Lower(d, tasklower.Options{Shell: "/bin/sh"})
	fmt.Println(task.Argv, err)
	// Output: [/bin/sh -c echo hello] <nil>
}

func ExampleMarshal() {
	data, err := tasklower.Marshal(&skyv1.Task{Argv: []string{"echo", "hello"}})
	fmt.Println(len(data) > 0, err)
	// Output: true <nil>
}
