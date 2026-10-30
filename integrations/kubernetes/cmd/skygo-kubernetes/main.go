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
	"time"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/integrations/kubernetes/internal/kube"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

type targetFlags struct {
	file, kubeconfig, context, namespace, environments, services, state string
	workers                                                             string
	draState                                                            string
	draRetention                                                        time.Duration
}

// usesDRA reports whether any target maps an accelerator to DRA.
func usesDRA(config kube.Config) bool {
	for _, t := range config.Targets() {
		for _, a := range t.Resources.Accelerators {
			if a.DRA != nil {
				return true
			}
		}
	}
	return false
}

func command(run func(context.Context, server.Config) error) *cobra.Command {
	var flags targetFlags
	var serverCommand *cobra.Command
	root := cli.New(func(ctx context.Context, o cli.ServerOptions) error {
		var config kube.Config
		named := serverCommand.Flags().Changed("targets")
		withWorkers := serverCommand.Flags().Changed("worker-profile")
		var workers kube.WorkerConfig
		if withWorkers {
			for _, name := range []string{"service-profiles", "environment-profiles", "execution-state", "dra-state", "dra-log-retention", "kubeconfig", "context", "namespace"} {
				if serverCommand.Flags().Changed(name) {
					return errors.New("--worker-profile conflicts with service, environment, execution-state, dra and legacy target flags")
				}
			}
			if !named {
				return errors.New("--worker-profile requires --targets")
			}
			if o.AuthFile == "" {
				return errors.New("--worker-profile requires --auth-file")
			}
			if !o.DisableLocal {
				return errors.New("--worker-profile requires --disable-local")
			}
			if flags.workers == "" {
				return errors.New("--worker-profile requires a profile file")
			}
			var err error
			workers, err = kube.ReadWorkerConfig(flags.workers)
			if err != nil {
				return err
			}
		}
		// An explicit state directory, even an empty one, selects receipts;
		// it never falls back to targets without them.
		state := serverCommand.Flags().Changed("execution-state")
		if state && serverCommand.Flags().Changed("service-profiles") {
			return errors.New("--execution-state conflicts with --service-profiles")
		}
		if state && !named {
			return errors.New("--execution-state requires --targets")
		}
		if state && serverCommand.Flags().Changed("environment-profiles") {
			return errors.New("--execution-state conflicts with --environment-profiles")
		}
		// DRA flags select a separate integration and must not select services.
		dra := serverCommand.Flags().Changed("dra-state")
		if dra && serverCommand.Flags().Changed("service-profiles") {
			return errors.New("--dra-state conflicts with --service-profiles")
		}
		if dra && !state {
			return errors.New("--dra-state requires --targets and --execution-state")
		}
		if serverCommand.Flags().Changed("dra-log-retention") && !dra {
			return errors.New("--dra-log-retention requires --dra-state")
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
		if withWorkers {
			targets := config.Targets()
			if len(targets) != 1 || targets[0].Name != workers.Target() {
				return errors.New("worker profile requires exactly its configured target")
			}
			if targets[0].Data != nil || len(targets[0].Resources.Accelerators) != 0 {
				return errors.New("worker profile conflicts with data and accelerator targets")
			}
			return run(ctx, server.Config{
				DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, OIDCFile: o.OIDCFile, OIDCJWKSURL: o.OIDCJWKSURL, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
				Install: func(s *store.Store, engine *auth.Engine) (server.Integration, error) {
					return kube.NewWorkerIntegration(ctx, s, engine, config, workers)
				},
			})
		}
		if usesDRA(config) && serverCommand.Flags().Changed("service-profiles") {
			return errors.New("dra accelerators conflict with --service-profiles")
		}
		if dra {
			// The integration constructs and owns every configured target;
			// no Targets callback runs.
			return run(ctx, server.Config{
				DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, OIDCFile: o.OIDCFile, OIDCJWKSURL: o.OIDCJWKSURL, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
				Install: func(s *store.Store, _ *auth.Engine) (server.Integration, error) {
					return kube.NewDRAIntegration(ctx, config, s.Owner(), kube.DRAOptions{
						ExecutionState: flags.state, State: flags.draState, LogRetention: flags.draRetention,
					})
				},
			})
		}
		if usesDRA(config) {
			return errors.New("dra accelerators require --dra-state")
		}
		withEnvironments := serverCommand.Flags().Changed("environment-profiles")
		withServices := serverCommand.Flags().Changed("service-profiles")
		for _, f := range []struct {
			name string
			set  bool
		}{{"--environment-profiles", withEnvironments}, {"--service-profiles", withServices}} {
			if !f.set {
				continue
			}
			if !named {
				return fmt.Errorf("%s requires --targets", f.name)
			}
			if o.AuthFile == "" {
				return fmt.Errorf("%s requires --auth-file", f.name)
			}
		}
		if withEnvironments || withServices {
			var profiles kube.EnvironmentConfig
			var services kube.ServiceConfig
			var err error
			if withEnvironments {
				if profiles, err = kube.ReadEnvironmentConfig(flags.environments); err != nil {
					return err
				}
			}
			if withServices {
				if services, err = kube.ReadServiceConfig(flags.services); err != nil {
					return err
				}
			}
			// The integration constructs and owns every configured target.
			return run(ctx, server.Config{
				DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, OIDCFile: o.OIDCFile, OIDCJWKSURL: o.OIDCJWKSURL, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
				Install: func(s *store.Store, engine *auth.Engine) (server.Integration, error) {
					if !withServices {
						return kube.NewEnvironmentIntegration(ctx, s, engine, config, profiles)
					}
					return kube.NewIntegration(ctx, s, engine, config, profiles, services)
				},
			})
		}
		return run(ctx, server.Config{
			DataDir: o.DataDir, ArtifactDir: o.ArtifactDir, RecoverStoppedArtifactReaders: o.RecoverStoppedArtifactReaders, Listen: o.Listen, Token: o.Token, AuthFile: o.AuthFile, OIDCFile: o.OIDCFile, OIDCJWKSURL: o.OIDCJWKSURL, TargetFacts: o.TargetFacts, DisableLocal: o.DisableLocal,
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
			cmd.Flags().StringVar(&flags.draState, "dra-state", "", "Separate private 0700 directory for experimental DRA records (requires --execution-state)")
			cmd.Flags().DurationVar(&flags.draRetention, "dra-log-retention", 0, "Keep succeeded DRA Pods this long before API cleanup; 0 keeps them")
			cmd.Flags().StringVar(&flags.services, "service-profiles", "", "Private service profiles file for one of --targets (requires --auth-file)")
			cmd.Flags().StringVar(&flags.workers, "worker-profile", "", "Private CPU worker profile for exactly one of --targets (requires --auth-file and --disable-local)")
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
