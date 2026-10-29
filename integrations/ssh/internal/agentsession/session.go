// Package agentsession opens a ready skygo agent on an SSH host. It builds
// the agent from a skygo checkout, installs and starts it with the
// bootstrap script, and waits until it answers as itself. It is the one
// implementation behind the skygo-ssh command and package sshagent.
package agentsession

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"golang.org/x/crypto/ssh"
)

const (
	// AgentName is the DNS name in every agent's TLS certificate. Each
	// agent directory has its own self-signed certificate for it.
	AgentName = "agent.skygo.invalid"

	// DefaultPort is the agent's default remote loopback port.
	DefaultPort = 47080
)

// ValidTarget reports whether s can name the agent's target: a lowercase
// DNS name of at most 63 bytes.
func ValidTarget(s string) bool {
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

// Options configure Open. The packages Open calls validate them before
// they act on them.
type Options struct {
	Name string // the host in diagnostics, such as its SSH alias

	SSHAddress     string // SSH server as canonical host:port
	User           string // SSH user
	KnownHostsFile string // absolute known_hosts file dedicated to SSHAddress

	// KeyFile, if set, is an absolute unencrypted private key. Otherwise
	// the SSH agent on AgentSocket signs with the key whose SHA256
	// fingerprint is SignerKey.
	KeyFile     string
	AgentSocket string
	SignerKey   string

	// Source is the skygo checkout to build, "" for the one holding the
	// current directory; AllowDirty builds its uncommitted changes too.
	Source      string
	AllowDirty  bool
	CacheDir    string // build cache
	IdentityDir string // local agent identities, in COMMIT/DIGEST below it

	Target         string // name of the remote built-in target
	Port           int    // remote loopback port of the agent
	RestartStopped bool   // start a new agent if the recorded one has exited

	// Installation, if not zero, is the initialized remote installation
	// the agent serves. TokenFile is then the local private file holding
	// a token its auth file issued, and Bearer that token, which the
	// caller reads before anything else happens.
	Installation      bootstrap.Installation
	TokenFile, Bearer string

	Log io.Writer // progress and diagnostics; nil discards them

	// signer and build, if set, replace AgentSigner and buildAgent.
	signer func(socket, fingerprint string) (ssh.Signer, io.Closer, error)
	build  func(ctx context.Context, source, cache string, allowDirty bool, log io.Writer) (agentbuild.Build, error)
}

// A Binding records what Open verified and started. It holds no
// credential.
type Binding struct {
	Name           string
	SSHAddress     string
	User           string
	KnownHostsFile string
	KeyFile        string // "" with an SSH agent
	SignerKey      string // pinned SSH agent key, "" with a key file
	AgentAddress   string // the agent as seen from the SSH server
	Target         string
	Commit, Digest string
	Dirty          bool
	AgentDir       string // relative to the remote home directory
	Installation   bootstrap.Installation
	PID            int // the recorded process when Open checked it
}

// A Session is a ready agent and the HTTP client that reaches it.
type Session struct {
	http    *http.Client
	binding Binding
	signer  io.Closer // SSH agent connection, nil with a key file

	// tokenFile is the private file holding the credential the agent
	// accepts, bearer: the agent's legacy token or the scoped token.
	tokenFile, bearer string

	once     sync.Once
	closeErr error
}

// HTTPClient returns the client whose requests reach the agent. Each
// request uses its own SSH connection; the client never retries.
func (s *Session) HTTPClient() *http.Client { return s.http }

// Endpoint returns the agent's base URL.
func (s *Session) Endpoint() string { return agentssh.Target{AgentName: AgentName}.Endpoint() }

// TokenFile returns the private file holding the token the agent accepts.
func (s *Session) TokenFile() string { return s.tokenFile }

// Bearer returns the token the agent accepts.
func (s *Session) Bearer() string { return s.bearer }

// Binding returns what Open verified and started.
func (s *Session) Binding() Binding { return s.binding }

// Close closes the SSH agent connection that signs the session's SSH
// authentications, once. Every call waits for that first Close and returns
// its result. A failed Close is never retried, and s keeps referencing the
// connection. Close does not join requests in flight, which own their
// connections, and never stops the remote agent or its jobs.
func (s *Session) Close() error {
	s.once.Do(func() {
		if s.signer != nil {
			s.closeErr = s.signer.Close()
		}
	})
	return s.closeErr
}

// An UnsettledError reports a failed Open whose local cleanup did not
// settle: the first Close of a connection Open acquired failed, or a
// readiness request did not show that its connections were closed, so
// their state is unknown. The error references each such connection, and
// Err any *agentssh.DialError or *ProbeError and what it retains, for as
// long as the caller holds it; none is closed again. Holding the error
// joins nothing that Open had not already joined, and once it is dropped
// the runtime may finalize the connections.
type UnsettledError struct {
	Err         error // why Open failed
	SignerClose error // first Close result of the SSH agent connection
	SSHClose    error // first Close result of the bootstrap SSH connection
	SSHWait     error // that connection's Wait result: its shutdown cause

	signer io.Closer
	client *ssh.Client
}

func (e *UnsettledError) Error() string {
	s := e.Err.Error()
	if e.SignerClose != nil {
		s += " (close ssh agent connection: " + e.SignerClose.Error() + ")"
	}
	if e.SSHClose != nil {
		s += " (close bootstrap ssh connection: " + e.SSHClose.Error() + ")"
	}
	return s
}

func (e *UnsettledError) Unwrap() []error {
	var errs []error
	for _, err := range []error{e.Err, e.SignerClose, e.SSHClose} {
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// Summary names, in fixed words, each part of Open's cleanup that did not
// settle, such as "ssh agent connection". It holds no cause, path or
// credential.
func (e *UnsettledError) Summary() string { return strings.Join(e.stages(), ", ") }

func (e *UnsettledError) stages() []string {
	var s []string
	if e.SignerClose != nil {
		s = append(s, "ssh agent connection")
	}
	if e.SSHClose != nil {
		s = append(s, "bootstrap ssh connection")
	}
	var (
		inner *UnsettledError
		probe *ProbeError
		dial  *agentssh.DialError
	)
	switch {
	case errors.As(e.Err, &inner):
		s = append(s, inner.stages()...)
	case errors.As(e.Err, &probe):
		s = append(s, "readiness request")
	case errors.As(e.Err, &dial) && dial.CloseErr() != nil:
		s = append(s, "ssh connection")
	case agentssh.OwnershipUnsettled(e.Err):
		s = append(s, "connection")
	}
	return s
}

// Open builds the agent, installs and starts it on the host if needed, and
// waits until it answers as itself. The SSH connection it uses to run the
// bootstrap commands is closed, and its Wait joined, before Open returns.
//
// If Open fails, it closes every connection it acquired once. It returns
// an ordinary error only if each of those Closes succeeded, each Dial it
// made closed its own connection, and each readiness request closed its
// response body with a nil result; otherwise it returns an
// *UnsettledError. A readiness request that did not settle stops Open
// before any further request or bootstrap command.
func Open(ctx context.Context, o Options) (*Session, error) {
	s := new(Session)
	u := new(UnsettledError)
	err := s.open(ctx, o, u)
	if err == nil {
		return s, nil
	}
	u.Err = err
	if s.signer != nil {
		u.SignerClose, u.signer = s.Close(), s.signer
	}
	if u.SignerClose != nil || u.SSHClose != nil ||
		agentssh.OwnershipUnsettled(err) ||
		errors.As(err, new(*ProbeError)) ||
		errors.As(err, new(*UnsettledError)) {
		return nil, u
	}
	return nil, err
}

func (s *Session) open(ctx context.Context, o Options, u *UnsettledError) error {
	log := o.Log
	if log == nil {
		log = io.Discard
	}
	target := agentssh.Target{
		SSHAddress:     o.SSHAddress,
		User:           o.User,
		KeyFile:        o.KeyFile,
		KnownHostsFile: o.KnownHostsFile,
		AgentAddress:   net.JoinHostPort("127.0.0.1", strconv.Itoa(o.Port)),
		AgentName:      AgentName,
	}
	if o.KeyFile == "" {
		sign := o.signer
		if sign == nil {
			sign = AgentSigner
		}
		target.SignerKey = o.SignerKey
		var err error
		if target.Signer, s.signer, err = sign(o.AgentSocket, o.SignerKey); err != nil {
			return fmt.Errorf("skygo-ssh: %w", err)
		}
	}

	buildFunc := o.build
	if buildFunc == nil {
		buildFunc = buildAgent
	}
	build, err := buildFunc(ctx, o.Source, o.CacheDir, o.AllowDirty, log)
	if err != nil {
		return err
	}
	id, err := agentidentity.Load(filepath.Join(o.IdentityDir, build.Commit, build.Digest), AgentName)
	if err != nil {
		return fmt.Errorf("skygo-ssh: %w", err)
	}
	target.AgentCAFile = id.Cert
	a := bootstrap.Agent{
		Arch:         bootstrap.Arch,
		Commit:       build.Commit,
		Digest:       build.Digest,
		Port:         o.Port,
		Target:       o.Target,
		Installation: o.Installation,
		Local:        map[string]string{"skygo-ssh": build.Path, "cert.pem": id.Cert, "key.pem": id.Key},
	}
	s.tokenFile, s.bearer = o.TokenFile, o.Bearer
	if a.Installation.IsZero() {
		a.Local["token"] = id.Token
		s.tokenFile, s.bearer = id.Token, id.Bearer
	}

	sc, err := agentssh.Dial(ctx, target)
	if err != nil {
		return err
	}
	pid, err := s.start(ctx, bootstrap.SSHRunner{Client: sc}, a, target, o.RestartStopped, log)
	// The bootstrap connection serves only Open. If a canceled command
	// closed it first, this Close returns that first result.
	u.SSHClose = sc.Close()
	u.SSHWait = sc.Wait()
	if u.SSHClose != nil {
		u.client = sc
		if err == nil {
			err = errors.New("skygo-ssh: agent ready, but the bootstrap ssh connection did not close")
		}
	}
	if err != nil {
		return err
	}
	s.binding = Binding{
		Name:           o.Name,
		SSHAddress:     target.SSHAddress,
		User:           target.User,
		KnownHostsFile: target.KnownHostsFile,
		KeyFile:        target.KeyFile,
		SignerKey:      target.SignerKey,
		AgentAddress:   target.AgentAddress,
		Target:         a.Target,
		Commit:         build.Commit,
		Digest:         build.Digest,
		Dirty:          build.Dirty,
		AgentDir:       a.Dir(),
		Installation:   a.Installation,
		PID:            pid,
	}
	if a.Installation.IsZero() {
		fmt.Fprintf(log, "skygo-ssh: agent ready on %s: target %s, pid %d, %s, legacy bearer token\n", o.Name, a.Target, pid, a.Dir())
	} else {
		fmt.Fprintf(log, "skygo-ssh: agent ready on %s: target %s, pid %d, %s, installation %s, scoped token\n", o.Name, a.Target, pid, a.Dir(), a.Installation.DataDir)
	}
	return nil
}

// start installs and starts a through r, and waits until the agent answers
// as itself through a new HTTP client for target.
func (s *Session) start(ctx context.Context, r bootstrap.Runner, a bootstrap.Agent, target agentssh.Target, restart bool, log io.Writer) (int, error) {
	if _, err := bootstrap.Install(ctx, r, a, log); err != nil {
		return 0, err
	}
	pid, err := bootstrap.Start(ctx, r, a, restart)
	if err != nil {
		return 0, err
	}
	hc, err := agentssh.NewClient(target)
	if err != nil {
		return 0, err
	}
	hc.Transport = Transport{hc.Transport}
	status := func(ctx context.Context) (bootstrap.State, error) { return bootstrap.Status(ctx, r, a) }
	if err := WaitReady(ctx, hc, target.Endpoint(), s.bearer, a.Target, !a.Installation.IsZero(), status); err != nil {
		if errors.As(err, new(*ProbeError)) {
			// A readiness request may still hold its connections: run
			// nothing more on the host.
			return 0, err
		}
		if tail, lerr := bootstrap.Log(ctx, r, a); lerr == nil && tail != "" {
			fmt.Fprintf(log, "skygo-ssh: end of %s/agent.log:\n%s\n", a.Dir(), tail)
		}
		return 0, err
	}
	s.http = hc
	return pid, nil
}

// buildAgent builds the agent from the checkout named by source, or the
// checkout holding the current directory, into cache.
func buildAgent(ctx context.Context, source, cache string, allowDirty bool, log io.Writer) (agentbuild.Build, error) {
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
	fmt.Fprintf(log, "skygo-ssh: building linux/arm64 agent from %s\n", root)
	b, err := new(agentbuild.Builder).Build(ctx, root, cache, allowDirty)
	if err != nil {
		return agentbuild.Build{}, err
	}
	dirty := ""
	if b.Dirty {
		dirty = " with uncommitted changes"
	}
	fmt.Fprintf(log, "skygo-ssh: agent %s%s, sha256 %s\n", b.Commit, dirty, b.Digest)
	return b, nil
}
