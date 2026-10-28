package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/server"
)

func main() {
	// A durable local job's supervisor is this binary re-executed by the
	// server. It must not install the CLI's signal handling.
	if len(os.Args) > 1 && os.Args[1] == local.SupervisorCommand {
		os.Exit(local.RunSupervisor(os.Args[2:]))
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cmd := command(server.Run)
	err := cmd.ExecuteContext(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(cli.ExitCode(err))
}

func command(run func(context.Context, server.Config) error) *cobra.Command {
	return cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		exe, err := os.Executable()
		if err != nil {
			return fmt.Errorf("locate supervisor executable: %w", err)
		}
		return run(ctx, server.Config{DurableLocal: true, LocalSupervisor: exe, DisableLocal: o.DisableLocal, DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, TargetFacts: o.TargetFacts})
	})
}
