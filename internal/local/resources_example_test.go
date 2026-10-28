package local_test

import (
	"errors"
	"fmt"
	"runtime"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
)

func ExampleBackend_Check_resources() {
	b, done := exampleLocal()
	defer done()
	image := backend.Task{Argv: []string{"true"}, Image: "cuda:12"}
	negative := backend.Task{Argv: []string{"true"}, CPUMillis: -1}
	// One CPU more than this host has can never be admitted.
	tooMany := backend.Task{Argv: []string{"true"}, CPUMillis: (int64(runtime.NumCPU()) + 1) * 1000}
	fmt.Println(errors.Is(b.Check(image), local.ErrUnsupported))
	fmt.Println(errors.Is(b.Check(negative), local.ErrInvalidResources))
	fmt.Println(errors.Is(b.Check(tooMany), local.ErrUnsatisfiable))
	// Output:
	// true
	// true
	// true
}
