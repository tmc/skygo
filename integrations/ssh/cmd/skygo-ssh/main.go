package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/integrations/ssh/internal/sshconfig"
	"github.com/tmc/skygo/integrations/ssh/sshagent"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/server"
)

const (
	agentName   = agentsession.AgentName
	defaultPort = agentsession.DefaultPort
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == local.SupervisorCommand {
		os.Exit(local.RunSupervisor(os.Args[2:]))
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	err := run(ctx, os.Args[1:], os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(cli.ExitCode(err))
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "agent" {
		signal.Ignore(syscall.SIGHUP)
		return runAgent(ctx, args[1:], stderr, server.Run)
	}
	f, rest, err := parseFlags(args, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}
	return runClient(ctx, f, rest, stderr)
}

// flags are skygo-ssh's own flags, which precede the skygo arguments.
type flags struct {
	host           string // OpenSSH host alias
	knownHosts     string // dedicated known_hosts file
	fingerprint    string // pinned client key, SHA256:...
	keyFile        string // unencrypted private key instead of the SSH agent
	source         string // explicit skygo checkout
	prebuilt       agentbinary.Spec
	target         string // name of the remote built-in target
	port           int    // remote loopback port of the agent
	restartStopped bool

	// installation is an operator-initialized remote installation for the
	// agent to serve, and scopedToken the local private file holding a
	// credential its auth file issued. Both are set or neither.
	installation bootstrap.Installation
	scopedToken  string
}

func parseFlags(args []string, stderr io.Writer) (flags, []string, error) {
	var f flags
	fs := flag.NewFlagSet("skygo-ssh", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: skygo-ssh --host ALIAS --known-hosts FILE [flags] SKYGO-ARGS...")
		fs.PrintDefaults()
	}
	fs.StringVar(&f.host, "host", "", "OpenSSH host `alias`, resolved with ssh -G (required)")
	fs.StringVar(&f.knownHosts, "known-hosts", "", "known_hosts `file` dedicated to the host, literal names only (required)")
	fs.StringVar(&f.fingerprint, "identity-fingerprint", "", "SHA256 `fingerprint` of the SSH agent key to use (default: the one IdentityFile.pub)")
	fs.StringVar(&f.keyFile, "key-file", "", "unencrypted private key `file` to use instead of the SSH agent")
	fs.StringVar(&f.source, "source", "", "skygo checkout to build the agent from, uncommitted changes included (default: the checkout holding the current directory, which must be clean)")
	fs.StringVar(&f.prebuilt.Path, "agent-binary", "", "read-only Linux arm64 agent executable `file` (with --agent-commit and --agent-digest)")
	fs.StringVar(&f.prebuilt.Commit, "agent-commit", "", "asserted clean source `commit` of --agent-binary")
	fs.StringVar(&f.prebuilt.Digest, "agent-digest", "", "SHA-256 `digest` of --agent-binary")
	fs.StringVar(&f.target, "target-name", "", "`name` of the remote target (default: the host alias)")
	fs.IntVar(&f.port, "agent-port", defaultPort, "remote loopback `port` of the agent")
	fs.BoolVar(&f.restartStopped, "restart-stopped", false, "start a new agent process if the recorded one has exited")
	fs.StringVar(&f.installation.DataDir, "remote-data-dir", "", "existing remote state `directory` of an operator-initialized installation, with scoped auth and artifacts initialized")
	fs.StringVar(&f.installation.AuthFile, "remote-auth-file", "", "the installation's remote scoped auth `file`")
	fs.StringVar(&f.installation.ArtifactDir, "remote-artifact-dir", "", "the installation's remote initialized artifact `directory`")
	fs.StringVar(&f.scopedToken, "scoped-token-file", "", "local private `file` holding a token issued by the installation's auth file")
	if err := fs.Parse(args); err != nil {
		return flags{}, nil, err
	}
	var prebuiltSet, sourceSet bool
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "agent-binary", "agent-commit", "agent-digest":
			prebuiltSet = true
		case "source":
			sourceSet = true
		}
	})
	if prebuiltSet {
		if sourceSet {
			return flags{}, nil, errors.New("skygo-ssh: --source and --agent-binary are exclusive")
		}
		if countSet(f.prebuilt.Path, f.prebuilt.Commit, f.prebuilt.Digest) != 3 {
			return flags{}, nil, errors.New("skygo-ssh: --agent-binary, --agent-commit and --agent-digest go together")
		}
		var err error
		f.prebuilt.Path, err = filepath.Abs(f.prebuilt.Path)
		if err != nil {
			return flags{}, nil, err
		}
		if err := f.prebuilt.Check(); err != nil {
			return flags{}, nil, err
		}
	}
	in := f.installation
	if set := countSet(in.DataDir, in.AuthFile, in.ArtifactDir, f.scopedToken); set != 0 && set != 4 {
		return flags{}, nil, errors.New("skygo-ssh: --remote-data-dir, --remote-auth-file, --remote-artifact-dir and --scoped-token-file go together")
	}
	if !in.IsZero() && (!bootstrap.ValidPath(in.DataDir) || !bootstrap.ValidPath(in.AuthFile) || !bootstrap.ValidPath(in.ArtifactDir)) {
		return flags{}, nil, errors.New("skygo-ssh: remote installation paths must be clean absolute paths of at most 200 letters, digits, '.', '_', '-' and '/'")
	}
	if f.host == "" || f.knownHosts == "" {
		fs.Usage()
		return flags{}, nil, errors.New("skygo-ssh: --host and --known-hosts are required")
	}
	if f.target == "" {
		f.target = f.host
	}
	if !agentsession.ValidTarget(f.target) {
		return flags{}, nil, fmt.Errorf("skygo-ssh: target name %q must be a lowercase DNS name; pass --target-name", f.target)
	}
	if f.port < 1024 || f.port > 65535 {
		return flags{}, nil, errors.New("skygo-ssh: --agent-port must be in 1024..65535")
	}
	if f.keyFile != "" && f.fingerprint != "" {
		return flags{}, nil, errors.New("skygo-ssh: --key-file and --identity-fingerprint are exclusive")
	}
	return f, fs.Args(), nil
}

func countSet(values ...string) int {
	n := 0
	for _, v := range values {
		if v != "" {
			n++
		}
	}
	return n
}

// runClient runs the skygo command in args against the agent on f.host,
// first building, installing and starting the agent as needed.
func runClient(ctx context.Context, f flags, args []string, stderr io.Writer) error {
	needed, err := sshagent.CheckCommand(args)
	if err != nil {
		return err
	}
	command := sshagent.Command{Args: args, Host: f.host, Stderr: stderr}
	if !needed {
		return sshagent.RunCommand(ctx, sshagent.Config{}, command)
	}
	cfg, err := clientConfig(ctx, f, stderr)
	if err != nil {
		return err
	}
	return sshagent.RunCommand(ctx, cfg, command)
}

// clientConfig preserves the CLI resolution and refusal ordering:
// the agent is built from the local checkout, installed and started on the
// host if needed, and answers as itself. With an installation, the agent
// serves it and the session uses the scoped token, which is read before
// anything else happens.
func clientConfig(ctx context.Context, f flags, stderr io.Writer) (sshagent.Config, error) {
	var cfg sshagent.Config
	cfg.Target, cfg.Port, cfg.RestartStopped, cfg.Log = f.target, f.port, f.restartStopped, stderr
	cfg.Installation = sshagent.Installation(f.installation)
	var err error
	// Preserve the CLI's token/prebuilt refusal-before-ssh-G ordering. The
	// public entrypoint revalidates these exact selected inputs before Open.
	if !f.installation.IsZero() {
		if cfg.ScopedTokenFile, err = filepath.Abs(f.scopedToken); err != nil {
			return cfg, err
		}
		if _, err = agentidentity.ReadToken(cfg.ScopedTokenFile); err != nil {
			return cfg, fmt.Errorf("skygo-ssh: scoped token: %w", err)
		}
	}
	if cfg.IdentityDir, cfg.CacheDir, err = localDirs(f.host); err != nil {
		return cfg, err
	}
	if f.prebuilt != (agentbinary.Spec{}) {
		if f.source != "" {
			return cfg, errors.New("skygo-ssh: prebuilt binary and source build options are exclusive")
		}
		if _, err = agentbinary.Prepare(ctx, cfg.CacheDir, f.prebuilt); err != nil {
			return cfg, err
		}
		cfg.Prebuilt = sshagent.Binary(f.prebuilt)
	} else {
		cfg.Source = f.source
		cfg.AllowDirty = f.source != ""
		if cfg.Source == "" {
			cfg.Source, err = os.Getwd()
		} else {
			cfg.Source, err = filepath.Abs(cfg.Source)
		}
		if err != nil {
			return cfg, err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return cfg, err
	}
	host, err := sshconfig.Resolve(ctx, f.host, home, nil)
	if err != nil {
		return cfg, err
	}
	cfg.Address, cfg.User = host.Address, host.User
	if cfg.KnownHostsFile, err = filepath.Abs(f.knownHosts); err != nil {
		return cfg, err
	}
	if f.keyFile != "" {
		cfg.KeyFile, err = filepath.Abs(f.keyFile)
		if err != nil {
			return cfg, err
		}
	} else {
		cfg.Fingerprint = f.fingerprint
		if cfg.Fingerprint == "" {
			cfg.Fingerprint, err = publicFingerprint(host.IdentityFiles)
			if err != nil {
				return cfg, fmt.Errorf("skygo-ssh: %s: %w", f.host, err)
			}
		}
		cfg.AgentSocket = os.Getenv("SSH_AUTH_SOCK")
		switch host.IdentityAgent {
		case "":
		case "none":
			return cfg, fmt.Errorf("skygo-ssh: %s: identityagent is none; pass --key-file", f.host)
		default:
			cfg.AgentSocket = host.IdentityAgent
		}
		if cfg.AgentSocket != "" {
			cfg.AgentSocket, err = filepath.Abs(cfg.AgentSocket)
			if err != nil {
				return cfg, err
			}
		}
	}
	return cfg, nil
}
