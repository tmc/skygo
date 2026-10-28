package local_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/local"
)

func ExampleBackend_Check_resources() {
	b, done := exampleLocal()
	defer done()
	image := backend.Task{Argv: []string{"true"}, Image: "cuda:12"}
	negative := backend.Task{Argv: []string{"true"}, CPUMillis: -1}
	fmt.Println(errors.Is(b.Check(image), local.ErrUnsupported))
	fmt.Println(errors.Is(b.Check(negative), local.ErrInvalidResources))
	// Output:
	// true
	// true
}
