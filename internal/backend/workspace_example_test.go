package backend_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func ExampleValidateWorkspace() {
	task := backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{
		ID:     "0123456789abcdef0123456789abcdef",
		SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Size:   1024,
	}, Setup: []string{"./prepare"}, Outputs: []string{"result"}}
	fmt.Println(backend.ValidateWorkspace(task))
	// Output: <nil>
}

func ExampleCloneTask() {
	task := backend.Task{Argv: []string{"run"}, Env: map[string]string{"MODE": "original"}}
	copy := backend.CloneTask(task)
	copy.Env["MODE"] = "changed"
	fmt.Println(task.Env["MODE"])
	// Output: original
}
