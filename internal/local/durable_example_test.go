//go:build linux || darwin

package local_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/tmc/skygo/internal/local"
)

func ExampleBackend_EnableDurable() {
	dir, err := os.MkdirTemp("", "skygo-durable-example-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer b.Close()
	exe, err := os.Executable()
	if err != nil {
		fmt.Println(err)
		return
	}
	// Executable must dispatch SupervisorCommand before normal CLI setup.
	err = b.EnableDurable(local.DurableConfig{State: filepath.Join(dir, "state"), Executable: exe})
	fmt.Println(err)
	fmt.Println(b.Detaches(strings.Repeat("a", 32))) // no execution was started
	// Output:
	// <nil>
	// false
}

func ExampleRunSupervisor() {
	// A missing job directory is rejected before any process or file is opened.
	fmt.Println(local.RunSupervisor(nil))
	// Output: 2
}
