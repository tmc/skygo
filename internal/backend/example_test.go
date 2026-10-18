package backend_test

import (
	"fmt"

	"github.com/tmc/skygo/internal/backend"
)

func Example() {
	task := backend.Task{Argv: []string{"trainer", "--steps", "10"}, Image: "trainer:v1", CPUMillis: 500}
	ref := backend.Reference{ID: "0123456789abcdef0123456789abcdef", Task: task}
	job := backend.Job{ID: ref.ID, Task: ref.Task, Target: "kubernetes", Status: backend.Status{State: backend.Queued}}
	fmt.Println(job.Target, job.Status.State, job.Task.CPUMillis)
	// Output: kubernetes queued 500
}

func ExampleState_Terminal() {
	fmt.Println(backend.Running.Terminal())
	// Unknown stops reconciliation; it does not establish process termination.
	fmt.Println(backend.Unknown.Terminal())
	// Output:
	// false
	// true
}

func ExampleLogChunk() {
	chunk := backend.LogChunk{Data: []byte("hello"), NextOffset: 5, Complete: true}
	fmt.Println(string(chunk.Data), chunk.NextOffset, chunk.Complete)
	fmt.Println(backend.MaxChunkBytes <= backend.MaxLogBytes)
	// Output:
	// hello 5 true
	// true
}
