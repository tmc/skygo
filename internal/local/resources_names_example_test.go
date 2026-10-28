package local

import (
	"context"
	"fmt"
	"os"
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
