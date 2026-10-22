package backend_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleValidateWorkspaceTemplate() {
	task := backend.Task{Argv: []string{"./run"}, Setup: []string{"./prepare"}, Outputs: []string{"result"}}
	fmt.Println(backend.ValidateWorkspaceTemplate(task))
	fmt.Println(backend.ValidateWorkspace(task))
	// Output:
	// <nil>
	// setup and outputs require workdir
}
