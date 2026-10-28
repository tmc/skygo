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
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

type targetFlags struct {
	file, kubeconfig, context, namespace, environments, state string
}

func command(run func(context.Context, server.Config) error) *cobra.Command {
	var flags targetFlags
	var serverCommand *cobra.Command
	root := cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		var config kube.Config
		named := serverCommand.Flags().Changed("targets")
		// An explicit state directory, even an empty one, selects receipts;
		// it never falls back to targets without them.
		state := serverCommand.Flags().Changed("execution-state")
		if state && !named {
			return errors.New("--execution-state requires --targets")
		}
		if state && serverCommand.Flags().Changed("environment-profiles") {
			return errors.New("--execution-state conflicts with --environment-profiles")
		}
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
		if serverCommand.Flags().Changed("environment-profiles") {
			if !named {
				return errors.New("--environment-profiles requires --targets")
			}
			if o.AuthFile == "" {
				return errors.New("--environment-profiles requires --auth-file")
			}
			profiles, err := kube.ReadEnvironmentConfig(flags.environments)
			if err != nil {
				return err
			}
			// The integration constructs and owns every configured target.
			return run(ctx, server.Config{
				DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
				Install: func(s *store.Store, engine *auth.Engine) (server.Integration, error) {
					return kube.NewEnvironmentIntegration(ctx, s, engine, config, profiles)
				},
			})
		}
		return run(ctx, server.Config{
			DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
			Targets: func(owner string) ([]backend.Target, error) {
				if named {
					var targets []backend.Target
					for _, target := range config.Targets() {
						// The receipt constructor validates the state
						// directory and refuses data targets before any
						// request; it keeps one scope per owner, namespace
						// and binding.
						var configured backend.Target
						var err error
						if state {
							configured, err = kube.FromTargetWithState(ctx, target, owner, flags.state)
						} else {
							configured, err = kube.FromTarget(ctx, target, owner)
						}
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
			cmd.Flags().StringVar(&flags.environments, "environment-profiles", "", "Private environment profiles file for one of --targets (requires --auth-file)")
			cmd.Flags().StringVar(&flags.state, "execution-state", "", "Existing private 0700 directory for durable execution receipts of --targets")
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
