package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/integrations/ssh/internal/sshconfig"
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
func runClient(ctx context.Context, f flags, args []string, stderr io.Writer) (err error) {
	needed, err := checkArgs(cli.NewWithClient(nil, nil, cli.ClientOptions{}), args)
	if err != nil {
		return err
	}
	if !needed {
		root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
		root.SetArgs(args)
		return root.ExecuteContext(ctx)
	}
	s, err := startAgent(ctx, f, stderr)
	if err != nil {
		return err
	}
	defer func() { err = closeSession(s, err) }()
	root, err := agentCommand(s.HTTPClient(), f.host, f.target, s.TokenFile(), args)
	if err != nil {
		return err
	}
	return root.ExecuteContext(ctx)
}

// closeSession closes s once. If that Close fails, it returns err joined
// with a *closeError that keeps s; otherwise it returns err.
func closeSession(s io.Closer, err error) error {
	if cerr := s.Close(); cerr != nil {
		return errors.Join(err, &closeError{owner: s, err: cerr})
	}
	return err
}

// A closeError reports a failed first Close of a session's SSH agent
// connection, whose state is then unknown. It keeps the session, which
// keeps the connection; the Close is never retried. Its text is fixed;
// Unwrap returns the original Close error.
type closeError struct {
	owner io.Closer // the *agentsession.Session
	err   error
}

func (e *closeError) Error() string {
	return "skygo-ssh: closing the ssh agent connection failed; its state is unknown"
}

func (e *closeError) Unwrap() error { return e.err }

// agentCommand returns the skygo command for args, reaching the agent on
// host through hc with the credential in tokenFile.
func agentCommand(hc *http.Client, host, target, tokenFile string, args []string) (*cobra.Command, error) {
	root := cli.NewWithClient(nil, hc, cli.ClientOptions{Infra: "ssh/" + host, Target: target})
	root.SetArgs(args)
	if err := setEndpoint(root, agentssh.Target{AgentName: agentName}.Endpoint(), tokenFile); err != nil {
		return nil, err
	}
	return root, nil
}

// checkArgs refuses skygo arguments that would bypass the agent, and
// reports whether the command needs it. Help needs no agent.
func checkArgs(root *cobra.Command, args []string) (bool, error) {
	for _, a := range args {
		if a == "--" {
			break
		}
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if strings.HasPrefix(a, "-") && (name == "endpoint" || name == "token" || name == "token-file") {
			return false, fmt.Errorf("skygo-ssh: --%s is set by skygo-ssh", name)
		}
		if a == "-h" || a == "--help" {
			return false, nil
		}
	}
	if os.Getenv("SKYGO_TOKEN") != "" {
		return false, errors.New("skygo-ssh: unset SKYGO_TOKEN; skygo-ssh supplies the agent token")
	}
	root.InitDefaultHelpCmd()
	cmd, _, err := root.Find(args)
	if err != nil {
		return false, err
	}
	top := cmd
	for top.Parent() != nil && top.Parent() != root {
		top = top.Parent()
	}
	switch top.Name() {
	case "server", "auth", "state":
		return false, fmt.Errorf("skygo-ssh: %q works on local files; run it on the remote host", top.Name())
	case "dashboard":
		return false, errors.New("skygo-ssh: the dashboard is not supported over SSH")
	case "help":
		return false, nil
	}
	return cmd.Runnable(), nil
}

// setEndpoint points root at the agent with its private token file.
func setEndpoint(root *cobra.Command, endpoint, tokenFile string) error {
	if err := root.PersistentFlags().Set("endpoint", endpoint); err != nil {
		return err
	}
	return root.PersistentFlags().Set("token-file", tokenFile)
}

// startAgent resolves f.host with ssh -G and opens an agentsession for it:
// the agent is built from the local checkout, installed and started on the
// host if needed, and answers as itself. With an installation, the agent
// serves it and the session uses the scoped token, which is read before
// anything else happens.
func startAgent(ctx context.Context, f flags, stderr io.Writer) (*agentsession.Session, error) {
	o := agentsession.Options{
		Name:           f.host,
		Source:         f.source,
		AllowDirty:     f.source != "",
		Target:         f.target,
		Port:           f.port,
		RestartStopped: f.restartStopped,
		Installation:   f.installation,
		Log:            stderr,
	}
	var err error
	if !f.installation.IsZero() {
		if o.TokenFile, err = filepath.Abs(f.scopedToken); err != nil {
			return nil, err
		}
		if o.Bearer, err = agentidentity.ReadToken(o.TokenFile); err != nil {
			return nil, fmt.Errorf("skygo-ssh: scoped token: %w", err)
		}
	}
	if f.prebuilt != (agentbinary.Spec{}) {
		if f.source != "" {
			return nil, errors.New("skygo-ssh: prebuilt binary and source build options are exclusive")
		}
		if o.IdentityDir, o.CacheDir, err = localDirs(f.host); err != nil {
			return nil, err
		}
		if o.Prebuilt, err = agentbinary.Prepare(ctx, o.CacheDir, f.prebuilt); err != nil {
			return nil, err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	host, err := sshconfig.Resolve(ctx, f.host, home, nil)
	if err != nil {
		return nil, err
	}
	o.SSHAddress, o.User = host.Address, host.User
	if o.KnownHostsFile, err = filepath.Abs(f.knownHosts); err != nil {
		return nil, err
	}
	if f.keyFile != "" {
		if o.KeyFile, err = filepath.Abs(f.keyFile); err != nil {
			return nil, err
		}
	} else {
		if o.SignerKey = f.fingerprint; o.SignerKey == "" {
			if o.SignerKey, err = publicFingerprint(host.IdentityFiles); err != nil {
				return nil, fmt.Errorf("skygo-ssh: %s: %w", f.host, err)
			}
		}
		o.AgentSocket = os.Getenv("SSH_AUTH_SOCK")
		switch host.IdentityAgent {
		case "":
		case "none":
			return nil, fmt.Errorf("skygo-ssh: %s: identityagent is none; pass --key-file", f.host)
		default:
			o.AgentSocket = host.IdentityAgent
		}
	}
	if o.Prebuilt.IsZero() {
		if o.IdentityDir, o.CacheDir, err = localDirs(f.host); err != nil {
			return nil, err
		}
	}
	return agentsession.Open(ctx, o)
}
