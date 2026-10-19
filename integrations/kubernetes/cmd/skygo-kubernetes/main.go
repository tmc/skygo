// Skygo-kubernetes serves finite jobs through native Kubernetes Jobs and the
// shared skygo Connect API. Client commands work with either server executable.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/integrations/kubernetes/internal/kube"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
)

type targetFlags struct {
	file, kubeconfig, context, namespace string
}

func command(run func(context.Context, server.Config) error) *cobra.Command {
	var flags targetFlags
	var serverCommand *cobra.Command
	root := cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		var config kube.Config
		named := serverCommand.Flags().Changed("targets")
		if named {
			for _, name := range []string{"kubeconfig", "context", "namespace"} {
				if serverCommand.Flags().Changed(name) {
					return errors.New("--targets conflicts with --kubeconfig, --context or --namespace")
				}
			}
			var err error
			config, err = kube.ReadConfig(flags.file, !o.DisableLocal)
			if err != nil {
				return err
			}
		} else if flags.namespace == "" {
			return errors.New("namespace is required for a Kubernetes target")
		}
		return run(ctx, server.Config{
			DataDir: o.DataDir, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, DisableLocal: o.DisableLocal,
			Targets: func(owner string) ([]backend.Target, error) {
				if named {
					var targets []backend.Target
					for _, target := range config.Targets() {
						configured, err := kube.FromTarget(target, owner)
						if err != nil {
							return nil, err
						}
						targets = append(targets, configured)
					}
					return targets, nil
				}
				target, err := kube.FromConfig(flags.kubeconfig, flags.context, flags.namespace, owner)
				if err != nil {
					return nil, err
				}
				return []backend.Target{target}, nil
			},
		})
	})
	root.Use = "skygo-kubernetes"
	// The shared tree always contains the server command.
	for _, cmd := range root.Commands() {
		if cmd.Name() == "server" {
			serverCommand = cmd
			cmd.Flags().StringVar(&flags.file, "targets", "", "Named Kubernetes targets configuration file")
			cmd.Flags().StringVar(&flags.kubeconfig, "kubeconfig", "", "Kubernetes configuration file")
			cmd.Flags().StringVar(&flags.context, "context", "", "Kubernetes context")
			cmd.Flags().StringVar(&flags.namespace, "namespace", "", "Kubernetes namespace (required without --targets)")
		}
	}
	return root
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := command(server.Run).ExecuteContext(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(cli.ExitCode(err))
}
