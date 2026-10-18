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

func command(run func(context.Context, server.Config) error) *cobra.Command {
	var kubeconfig, contextName, namespace string
	root := cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		if namespace == "" {
			return errors.New("namespace is required for a Kubernetes target")
		}
		return run(ctx, server.Config{
			DataDir: o.DataDir, Listen: o.Listen, Token: o.Token, DisableLocal: o.DisableLocal,
			Targets: func(owner string) ([]backend.Target, error) {
				k, err := kube.FromConfig(kubeconfig, contextName, namespace, owner)
				if err != nil {
					return nil, err
				}
				return []backend.Target{{Name: "kubernetes", Kind: "kubernetes", Backend: k}}, nil
			},
		})
	})
	root.Use = "skygo-kubernetes"
	// The shared tree always contains the server command.
	for _, cmd := range root.Commands() {
		if cmd.Name() == "server" {
			cmd.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Kubernetes configuration file")
			cmd.Flags().StringVar(&contextName, "context", "", "Kubernetes context")
			cmd.Flags().StringVar(&namespace, "namespace", "", "Kubernetes namespace (required)")
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
