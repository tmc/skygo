// Skygo-data stages accepted input, replaces itself with a workload, or collects
// a private result candidate under the configured data-helper profile.
package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/tmc/skygo/internal/remotehelper"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := remotehelper.Run(ctx, os.Args[1:], os.Environ(), os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
