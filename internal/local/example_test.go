package local_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
)

const exampleID = "0123456789abcdef0123456789abcdef"

func exampleLocal() (*local.Backend, func()) {
	dir, err := os.MkdirTemp("", "skygo-local-example-")
	if err != nil {
		panic(err)
	}
	b, err := local.New(dir)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	return b, func() {
		if err := b.Close(); err != nil {
			panic(err)
		}
		os.RemoveAll(dir)
	}
}

// Use the compiled Go test helper as a workload, without depending on host tools.
func exampleTask(mode string) backend.Task {
	return backend.Task{Argv: []string{os.Args[0], "-test.run=^TestProgram$"}, Env: map[string]string{"SKYGO_TEST_PROGRAM": mode}}
}

func exampleWait(b *local.Backend, ref backend.Reference) backend.Status {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			panic(err)
		}
		status, err := b.Observe(ctx, ref)
		if err != nil {
			panic(err)
		}
		if status.State.Terminal() {
			return status
		}
		time.Sleep(time.Millisecond)
	}
}

func Example() {
	b, close := exampleLocal()
	defer close()
	task := exampleTask("success")
	fmt.Println(b.Check(task))
	fmt.Println(b.Start(context.Background(), exampleID, task))
	fmt.Println(exampleWait(b, backend.Reference{ID: exampleID, Task: task}).State)
	// Output:
	// <nil>
	// <nil>
	// succeeded
}

func ExampleBackend_Logs() {
	b, close := exampleLocal()
	defer close()
	ref := backend.Reference{ID: exampleID, Task: exampleTask("success")}
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		panic(err)
	}
	exampleWait(b, ref)
	chunk, err := b.Logs(context.Background(), ref, 0, backend.MaxChunkBytes)
	fmt.Println(string(chunk.Data), chunk.Complete, err)
	// Output: hello world true <nil>
}

func ExampleBackend_Cancel() {
	b, close := exampleLocal()
	defer close()
	ref := backend.Reference{ID: exampleID, Task: exampleTask("sleep")}
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		panic(err)
	}
	fmt.Println(b.Cancel(context.Background(), ref))
	fmt.Println(exampleWait(b, ref).State)
	// Output:
	// <nil>
	// canceled
}

func ExampleBackend_SupportsRetry() {
	b, close := exampleLocal()
	defer close()
	task := exampleTask("fail")
	if b.SupportsRetry() {
		task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}
	}
	fmt.Println(b.Check(task))
	// Output: <nil>
}
