package local

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/backend"
)

func ExampleBackend_AcceleratorNames() {
	dir, err := os.MkdirTemp("", "skygo-local-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	b, err := New(dir)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	// Inject the inventory nvidia-smi would report, so the output is fixed.
	b.resources.probe.gpus = func(context.Context) ([]hostGPU, error) {
		return []hostGPU{
			{uuid: "GPU-00000000-0000-0000-0000-000000000002", name: "NVIDIA GB10"},
			{uuid: "GPU-00000000-0000-0000-0000-000000000001", name: "NVIDIA GB10"},
		}, nil
	}
	names, err := b.AcceleratorNames(context.Background())
	fmt.Println(names, err)
	// Output: [GB10] <nil>
}

func ExampleErrInventory() {
	dir, err := os.MkdirTemp("", "skygo-local-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	b, err := New(dir)
	if err != nil {
		panic(err)
	}
	defer b.Close()
	// MODELED: a failing GPU query stands in for nvidia-smi, which is not run.
	errDriver := errors.New("modeled driver failure")
	b.resources.probe.gpus = func(context.Context) ([]hostGPU, error) { return nil, errDriver }

	err = b.Check(backend.Task{Argv: []string{"true"}, Accelerator: "GB10", AcceleratorCount: 1})
	fmt.Println(errors.Is(err, ErrInventory), errors.Is(err, errDriver))
	fmt.Println(err)
	// Output:
	// true true
	// local resource inventory is unavailable: gpus: modeled driver failure
}
