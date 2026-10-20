package control_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

func ExampleValidate_workspace() {
	task := backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{
		ID:     "0123456789abcdef0123456789abcdef",
		SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Size:   1024,
	}, Setup: []string{"./prepare"}, Outputs: []string{"result"}}
	fmt.Println(control.Validate(task))
	// Output: <nil>
}
