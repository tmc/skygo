package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cmd := cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		return server.Run(ctx, server.Config{DisableLocal: o.DisableLocal, DataDir: o.DataDir, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile})
	})
	err := cmd.ExecuteContext(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(cli.ExitCode(err))
}
