package server_test

import (
	"context"
	"fmt"
	"os"

	"github.com/tmc/skygo/internal/server"
)

func Example() {
	dir, err := os.MkdirTemp("", "skygo-server-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Port zero asks the OS for a free loopback port. Cancellation drains Run.
	err = server.Run(ctx, server.Config{DataDir: dir, Listen: "127.0.0.1:0"})
	fmt.Println(err)
	// Output: <nil>
}
