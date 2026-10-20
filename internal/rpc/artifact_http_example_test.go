package rpc_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

func ExampleHTTPHandler_WaitArtifactHTTP() {
	// The qualification caller supplies a project-scoped TMPDIR.
	dir, err := os.MkdirTemp("", "skygo-http-owner-example-")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer os.RemoveAll(dir)
	s, err := store.Open(dir)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer s.Close()
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer b.Close()
	engine := auth.NewLegacy("")
	c, err := control.New(s, []backend.Target{{Name: "local", Kind: "local", Binding: "test-local", Backend: b}}, engine)
	if err != nil {
		fmt.Println(err)
		return
	}
	h := rpc.NewHandler(c, engine)
	// No requests were admitted. This stops future artifact HTTP admission and
	// joins this handler's empty owner set; it is not a whole-service drain.
	h.WaitArtifactHTTP()
	fmt.Println("artifact HTTP admission stopped")
	// Output: artifact HTTP admission stopped
}
