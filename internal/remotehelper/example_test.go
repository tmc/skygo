package remotehelper_test

import (
	"context"
	"os"

	"github.com/tmc/skygo/internal/remotehelper"
)

func ExampleRun() {
	remotehelper.Run(context.Background(), []string{"--help"}, nil, os.Stdout, os.Stderr)
	// Output: usage: skygo-data stage | exec -- argv... | collect -- root...
}
