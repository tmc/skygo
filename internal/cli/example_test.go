package cli_test

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/cli"
)

func ExampleNew() {
	root := cli.New(nil)
	fmt.Println(root.Name())
	// Output: skygo
}

func ExampleExitCode() {
	fmt.Println(cli.ExitCode(nil), cli.ExitCode(errors.New("unavailable")))
	// Output: 0 1
}

func ExampleServerOptions() {
	options := cli.ServerOptions{DataDir: "state", Listen: "127.0.0.1:8080"}
	fmt.Println(options.Listen)
	// Output: 127.0.0.1:8080
}
