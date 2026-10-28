package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/integrations/ssh/internal/sshconfig"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/server"
)

const (
	// agentName is the DNS name in every agent's TLS certificate. Each
	// agent directory has its own self-signed certificate for it.
	agentName   = "agent.skygo.invalid"
	defaultPort = 47080

	readyTimeout = time.Minute
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
	target         string // name of the remote built-in target
	port           int    // remote loopback port of the agent
	restartStopped bool
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
	fs.StringVar(&f.target, "target-name", "", "`name` of the remote target (default: the host alias)")
	fs.IntVar(&f.port, "agent-port", defaultPort, "remote loopback `port` of the agent")
	fs.BoolVar(&f.restartStopped, "restart-stopped", false, "start a new agent process if the recorded one has exited")
	if err := fs.Parse(args); err != nil {
		return flags{}, nil, err
	}
	if f.host == "" || f.knownHosts == "" {
		fs.Usage()
		return flags{}, nil, errors.New("skygo-ssh: --host and --known-hosts are required")
	}
	if f.target == "" {
		f.target = f.host
	}
	if !validTargetName(f.target) {
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

func validTargetName(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for _, label := range strings.Split(s, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' ||
			strings.Trim(label, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return false
		}
	}
	return true
}

// runClient runs the skygo command in args against the agent on f.host,
// first building, installing and starting the agent as needed.
func runClient(ctx context.Context, f flags, args []string, stderr io.Writer) error {
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
	defer s.close()
	root := cli.NewWithClient(nil, s.http, cli.ClientOptions{Infra: "ssh/" + f.host, Target: f.target})
	root.SetArgs(args)
	if err := setEndpoint(root, agentssh.Target{AgentName: agentName}.Endpoint(), s.id.token); err != nil {
		return err
	}
	return root.ExecuteContext(ctx)
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

// A session is a ready agent and the HTTP client that reaches it.
type session struct {
	http   *http.Client
	id     identity
	closer io.Closer // SSH agent connection, if any
}

func (s *session) close() {
	if s.closer != nil {
		s.closer.Close()
	}
}

// startAgent builds the agent from the local checkout, installs and starts
// it on the host if needed, and waits until it answers as itself.
func startAgent(ctx context.Context, f flags, stderr io.Writer) (_ *session, err error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	host, err := sshconfig.Resolve(ctx, f.host, home, nil)
	if err != nil {
		return nil, err
	}
	knownHosts, err := filepath.Abs(f.knownHosts)
	if err != nil {
		return nil, err
	}
	s := &session{}
	defer func() {
		if err != nil {
			s.close()
		}
	}()
	target := agentssh.Target{
		SSHAddress:     host.Address,
		User:           host.User,
		KnownHostsFile: knownHosts,
		AgentAddress:   net.JoinHostPort("127.0.0.1", strconv.Itoa(f.port)),
		AgentName:      agentName,
	}
	if f.keyFile != "" {
		if target.KeyFile, err = filepath.Abs(f.keyFile); err != nil {
			return nil, err
		}
	} else {
		if target.SignerKey = f.fingerprint; target.SignerKey == "" {
			if target.SignerKey, err = publicFingerprint(host.IdentityFiles); err != nil {
				return nil, fmt.Errorf("skygo-ssh: %s: %w", f.host, err)
			}
		}
		socket := os.Getenv("SSH_AUTH_SOCK")
		switch host.IdentityAgent {
		case "":
		case "none":
			return nil, fmt.Errorf("skygo-ssh: %s: identityagent is none; pass --key-file", f.host)
		default:
			socket = host.IdentityAgent
		}
		if target.Signer, s.closer, err = agentSigner(socket, target.SignerKey); err != nil {
			return nil, fmt.Errorf("skygo-ssh: %w", err)
		}
	}

	build, err := buildAgent(ctx, f.source, stderr)
	if err != nil {
		return nil, err
	}
	config, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	s.id, err = loadIdentity(filepath.Join(config, "skygo-ssh", "agents", f.host, build.Commit, build.Digest), agentName)
	if err != nil {
		return nil, fmt.Errorf("skygo-ssh: %w", err)
	}
	target.AgentCAFile = s.id.cert
	a := bootstrap.Agent{
		Arch:   bootstrap.Arch,
		Commit: build.Commit,
		Digest: build.Digest,
		Port:   f.port,
		Target: f.target,
		Local:  map[string]string{"skygo-ssh": build.Path, "cert.pem": s.id.cert, "key.pem": s.id.key, "token": s.id.token},
	}

	sc, err := agentssh.Dial(ctx, target)
	if err != nil {
		return nil, err
	}
	defer sc.Close()
	r := bootstrap.SSHRunner{Client: sc}
	if _, err := bootstrap.Install(ctx, r, a, stderr); err != nil {
		return nil, err
	}
	pid, err := bootstrap.Start(ctx, r, a, f.restartStopped)
	if err != nil {
		return nil, err
	}
	if s.http, err = agentssh.NewClient(target); err != nil {
		return nil, err
	}
	s.http.Transport = identityTransport{s.http.Transport}
	status := func(ctx context.Context) (bootstrap.State, error) { return bootstrap.Status(ctx, r, a) }
	if err := waitReady(ctx, s.http, target.Endpoint(), s.id.bearer, f.target, status); err != nil {
		if log, lerr := bootstrap.Log(ctx, r, a); lerr == nil && log != "" {
			fmt.Fprintf(stderr, "skygo-ssh: end of %s/agent.log:\n%s\n", a.Dir(), log)
		}
		return nil, err
	}
	fmt.Fprintf(stderr, "skygo-ssh: agent ready on %s: target %s, pid %d, %s, legacy bearer token\n", f.host, f.target, pid, a.Dir())
	return s, nil
}

// buildAgent builds the agent from the checkout named by source, or the
// clean checkout holding the current directory.
func buildAgent(ctx context.Context, source string, stderr io.Writer) (agentbuild.Build, error) {
	dir := source
	if dir == "" {
		wd, err := os.Getwd()
		if err != nil {
			return agentbuild.Build{}, err
		}
		dir = wd
	}
	root, err := agentbuild.FindRoot(dir)
	if err != nil {
		return agentbuild.Build{}, err
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return agentbuild.Build{}, err
	}
	fmt.Fprintf(stderr, "skygo-ssh: building linux/arm64 agent from %s\n", root)
	b, err := new(agentbuild.Builder).Build(ctx, root, filepath.Join(cache, "skygo-ssh", "build"), source != "")
	if err != nil {
		return agentbuild.Build{}, err
	}
	dirty := ""
	if b.Dirty {
		dirty = " with uncommitted changes"
	}
	fmt.Fprintf(stderr, "skygo-ssh: agent %s%s, sha256 %s\n", b.Commit, dirty, b.Digest)
	return b, nil
}

// waitReady waits until the agent at endpoint answers an authorized
// GetServerInfo naming target as a local target. A response proves the
// agent holds the key of its pinned certificate and accepts the token.
// Between attempts it checks that the recorded process has not stopped.
func waitReady(ctx context.Context, hc *http.Client, endpoint, token, target string, status func(context.Context) (bootstrap.State, error)) error {
	cc := skyv1connect.NewControlServiceClient(hc, endpoint)
	deadline := time.Now().Add(readyTimeout)
	delay := 100 * time.Millisecond
	for {
		err := checkReady(ctx, cc, token, target)
		if err == nil || errors.Is(err, errWrongAgent) {
			return err
		}
		st, serr := status(ctx)
		if serr != nil {
			return serr
		}
		if st.Agent != "running" && st.Agent != "unknown" {
			return fmt.Errorf("skygo-ssh: agent %s before it was ready: %w", st.Agent, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("skygo-ssh: agent not ready after %v: %w", readyTimeout, err)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(delay):
		}
		delay = min(2*delay, time.Second)
	}
}

var errWrongAgent = errors.New("skygo-ssh: agent does not serve the expected target")

func checkReady(ctx context.Context, cc skyv1connect.ControlServiceClient, token, target string) error {
	req := connect.NewRequest(&v1.GetServerInfoRequest{})
	req.Header().Set("Authorization", "Bearer "+token)
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := cc.GetServerInfo(ctx, req)
	if err != nil {
		return err
	}
	for _, t := range resp.Msg.Targets {
		if t.Name == target && t.Kind == "local" {
			return nil
		}
	}
	return fmt.Errorf("%w %s", errWrongAgent, target)
}
