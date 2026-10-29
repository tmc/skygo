package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbuild"
	"golang.org/x/crypto/ssh"
)

// Arch is the remote machine architecture, as reported by uname -m, of
// the linux/arm64 agent.
const Arch = "aarch64"

// Files lists the files the agent directory holds, in transfer order.
var Files = []string{"skygo-ssh", "cert.pem", "key.pem", "token"}

const (
	maxStdout = 64 << 10
	maxStderr = 16 << 10
)

// A Runner runs one remote command line with stdin, which may be nil, and
// returns its bounded standard output and error.
type Runner interface {
	Run(ctx context.Context, command string, stdin io.Reader) (stdout, stderr []byte, err error)
}

// An SSHClient is the SSH connection an SSHRunner uses. *ssh.Client
// implements it. The client agentssh.Dial returns closes its connection
// once: every Close returns the first result, so the Close of a canceled
// Run and its owner's later Close share one result.
type SSHClient interface {
	NewSession() (*ssh.Session, error)
	Close() error
	Wait() error
}

// An SSHRunner runs each command in its own session on Client. Sessions
// request no terminal, environment or agent forwarding.
type SSHRunner struct{ Client SSHClient }

// Run implements Runner. Canceling ctx closes Client, including a pending
// session open, and returns only after that Close, Client's Wait and the
// command's session have returned; a failed Close is joined to the error.
// Wait's result is the connection's shutdown cause, not a failure. A
// canceled client cannot be reused. Run closes each session once. If Run
// fails after the command was sent, the command may have run.
//
// The error of a canceled Run is a *canceledError, which also keeps the
// results of Client's Wait and of a command its cancellation cut short.
func (r SSHRunner) Run(ctx context.Context, command string, stdin io.Reader) ([]byte, []byte, error) {
	return r.runThen(ctx, command, stdin, nil)
}

// runThen implements Run. If returned is not nil, runThen calls it with
// runSession's error after runSession returns and before stopping the
// cancellation callback.
func (r SSHRunner) runThen(ctx context.Context, command string, stdin io.Reader, returned func(error)) ([]byte, []byte, error) {
	if r.Client == nil {
		return nil, nil, errors.New("bootstrap: no SSH client")
	}
	var closeErr, waitErr error
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(closed)
		closeErr = r.Client.Close()
		waitErr = r.Client.Wait()
	})
	stdout, stderr, run, err := r.runSession(ctx, command, stdin)
	if returned != nil {
		returned(err)
	}
	if !stop() {
		<-closed
		// ctx ended, but runSession may have returned, even successfully,
		// before it did.
		if cause := context.Cause(ctx); !errors.Is(err, cause) {
			err = errors.Join(err, cause)
		}
		err = &canceledError{err: errors.Join(err, closeErr), run: run, wait: waitErr}
	}
	return stdout, stderr, err
}

// A canceledError reports a Run that ctx canceled. Its text and Unwrap
// are those of err; run and wait are kept as receipts, not causes.
type canceledError struct {
	err  error // the cancellation cause and each failed Close
	run  error // result of the session's Run that the cancellation cut short
	wait error // Client's Wait result: the connection's shutdown cause
}

func (e *canceledError) Error() string { return e.err.Error() }

func (e *canceledError) Unwrap() error { return e.err }

// runSession runs command in a new session. If ctx ends while the command
// runs, it returns the command's own result as run.
func (r SSHRunner) runSession(ctx context.Context, command string, stdin io.Reader) (stdout, stderr []byte, run, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, context.Cause(ctx)
	}
	s, err := r.Client.NewSession()
	if err != nil {
		return nil, nil, nil, errors.Join(err, context.Cause(ctx))
	}
	out := &limitedBuffer{n: maxStdout}
	errOut := &limitedBuffer{n: maxStderr}
	s.Stdout, s.Stderr = out, errOut
	if stdin != nil {
		s.Stdin = stdin
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(command) }()
	var cerr error
	select {
	case err = <-done:
		cerr = s.Close()
	case <-ctx.Done():
		cerr = s.Close()
		run = <-done
		err = context.Cause(ctx)
	}
	if errors.Is(cerr, io.EOF) {
		// x/crypto returns EOF once the channel's close is latched
		// locally, as it is after the server closed the channel when the
		// command exited. EOF reports that state, not that a close
		// message was delivered. The session is a channel on Client, not
		// a connection of its own; Client's owner settles the connection.
		cerr = nil
	}
	return out.buf.Bytes(), errOut.buf.Bytes(), run, errors.Join(err, cerr, context.Cause(ctx))
}

// A limitedBuffer keeps the first n bytes written and discards the rest.
type limitedBuffer struct {
	buf bytes.Buffer
	n   int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if room := b.n - b.buf.Len(); room > 0 {
		b.buf.Write(p[:min(len(p), room)])
	}
	return len(p), nil
}

// An Agent describes one agent directory and the local files that fill it.
type Agent struct {
	Arch   string // required remote architecture, such as Arch
	Commit string // full hex commit of the agent's source
	Digest string // hex SHA-256 of the agent binary
	Port   int    // loopback port the agent listens on
	Target string // name of the agent's built-in local target

	// Installation, if not zero, is the operator-initialized installation
	// the agent serves instead of its own data directory and token.
	Installation Installation

	Local map[string]string // local file for each name in Files, less token for an installation
}

// An Installation names an operator-initialized skygo installation on the
// remote host: an existing state directory with scoped authentication
// active, its auth file, and its initialized artifact directory. The
// bootstrap never creates or changes any of them.
type Installation struct {
	DataDir, AuthFile, ArtifactDir string
}

// IsZero reports whether i names no installation.
func (i Installation) IsZero() bool { return i == Installation{} }

func (i Installation) validate() error {
	for _, p := range []string{i.DataDir, i.AuthFile, i.ArtifactDir} {
		if !ValidPath(p) {
			return errors.New("bootstrap: installation paths must be clean absolute paths")
		}
	}
	return nil
}

// maxPath bounds each installation path, so that a configuration record
// stays well below the bytes the script reads of it.
const maxPath = 200

// ValidPath reports whether p is a clean absolute path other than "/", of
// at most 200 bytes of ASCII letters, digits, '.', '_', '-' and '/'.
func ValidPath(p string) bool {
	if len(p) < 2 || len(p) > maxPath || p[0] != '/' || path.Clean(p) != p {
		return false
	}
	return strings.Trim(p, "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._/-") == ""
}

// files returns the names of the files a's directory holds. An agent
// serving an installation holds no legacy token.
func (a Agent) files() []string {
	if a.Installation.IsZero() {
		return Files
	}
	return []string{"skygo-ssh", "cert.pem", "key.pem"}
}

func (a Agent) validate() error {
	switch {
	case a.Arch == "":
		return errors.New("bootstrap: no architecture")
	case !agentbuild.ValidCommit(a.Commit):
		return errors.New("bootstrap: invalid commit")
	case !agentbuild.ValidDigest(a.Digest):
		return errors.New("bootstrap: invalid digest")
	case a.Port < 1024 || a.Port > 65535:
		return errors.New("bootstrap: agent port must be in 1024..65535")
	case !safeWord(a.Target):
		return errors.New("bootstrap: invalid target name")
	}
	if !a.Installation.IsZero() {
		if err := a.Installation.validate(); err != nil {
			return err
		}
		if a.Local["token"] != "" {
			return errors.New("bootstrap: an installation agent takes no legacy token")
		}
	}
	for _, name := range a.files() {
		if a.Local[name] == "" {
			return fmt.Errorf("bootstrap: no local %s", name)
		}
	}
	return nil
}

// Dir returns the agent directory relative to the remote home directory.
func (a Agent) Dir() string { return "~/skygo-test/" + a.Commit + "/" + a.Digest }

// A State is what the remote host reports about an agent directory.
type State struct {
	Arch  string
	Files map[string]string // name to hex SHA-256, "missing" or "unsafe"
	Agent string            // "none", "running", "stopped", "unknown" or "unsafe"
	PID   int               // recorded process, if any

	// Config is the form of the configuration record: "none", "legacy",
	// "installation" or "unsafe". The remote host reads it with the same
	// exact parser as Start, and reports a record of any other form, or
	// one that is not a regular file of at most 1024 bytes, as unsafe.
	Config string

	// ConfigSame reports whether a legacy or installation record is
	// exactly the one a's first start would write: a's port, target and
	// installation, or none. The record itself is never reported.
	ConfigSame bool
}

// Status reports the state of a's directory. It changes nothing and
// creates nothing.
func Status(ctx context.Context, r Runner, a Agent) (State, error) {
	if err := a.validate(); err != nil {
		return State{}, err
	}
	out, err := run(ctx, r, nil, append([]string{"status", a.Commit, a.Digest}, a.configOperands()...)...)
	if err != nil {
		return State{}, err
	}
	return parseStatus(out)
}

// configOperands returns the operands naming a's configuration: PORT
// TARGET, followed for an installation by DATA AUTH ARTIFACTS.
func (a Agent) configOperands() []string {
	ops := []string{strconv.Itoa(a.Port), a.Target}
	if i := a.Installation; !i.IsZero() {
		ops = append(ops, i.DataDir, i.AuthFile, i.ArtifactDir)
	}
	return ops
}

func parseStatus(out []byte) (State, error) {
	st := State{Files: map[string]string{}}
	bad := func(line string) error { return fmt.Errorf("bootstrap: unexpected status line %q", line) }
	s := bufio.NewScanner(bytes.NewReader(out))
	for s.Scan() {
		f := strings.Fields(s.Text())
		switch {
		case len(f) == 2 && f[0] == "arch":
			st.Arch = f[1]
		case len(f) == 3 && f[0] == "file":
			if f[2] != "missing" && f[2] != "unsafe" && !agentbuild.ValidDigest(f[2]) {
				return State{}, bad(s.Text())
			}
			st.Files[f[1]] = f[2]
		case (len(f) == 2 || len(f) == 3) && f[0] == "agent":
			st.Agent = f[1]
			if len(f) == 3 {
				pid, err := strconv.Atoi(f[2])
				if err != nil || pid <= 0 {
					return State{}, bad(s.Text())
				}
				st.PID = pid
			}
		case len(f) == 2 && f[0] == "config" && (f[1] == "none" || f[1] == "unsafe"):
			st.Config, st.ConfigSame = f[1], false
		case len(f) == 3 && f[0] == "config" && (f[1] == "legacy" || f[1] == "installation") && (f[2] == "same" || f[2] == "other"):
			st.Config, st.ConfigSame = f[1], f[2] == "same"
		default:
			return State{}, bad(s.Text())
		}
	}
	switch st.Agent {
	case "none", "running", "stopped", "unknown", "unsafe":
	default:
		return State{}, fmt.Errorf("bootstrap: unexpected agent state %q", st.Agent)
	}
	switch st.Config {
	case "none", "legacy", "installation", "unsafe":
	default:
		return State{}, fmt.Errorf("bootstrap: unexpected configuration form %q", st.Config)
	}
	for _, name := range Files {
		if st.Files[name] == "" {
			return State{}, fmt.Errorf("bootstrap: status omits %s", name)
		}
	}
	if st.Arch == "" {
		return State{}, errors.New("bootstrap: status omits arch")
	}
	return st, nil
}

// Install stores every file of a that the remote directory lacks. Before
// sending anything it refuses a remote architecture other than a.Arch and
// any existing remote file that is not the local one. It refuses, in
// either mode, a directory whose configuration record is unsafe. It
// refuses a record of the other form, whether or not a token is present,
// and a record of the same form naming another port, target or
// installation, wrapping fs.ErrExist. It also refuses a directory holding
// a legacy token for an installation agent. Progress, without file
// contents, goes to w.
func Install(ctx context.Context, r Runner, a Agent, w io.Writer) (State, error) {
	st, err := Status(ctx, r, a)
	if err != nil {
		return State{}, err
	}
	if st.Arch != a.Arch {
		return State{}, fmt.Errorf("bootstrap: remote architecture %q is not %s", st.Arch, a.Arch)
	}
	switch {
	case st.Config == "unsafe":
		return State{}, fmt.Errorf("bootstrap: remote %s/agent.config is unverifiable; refusing to install into it", a.Dir())
	case a.Installation.IsZero() && st.Config == "installation":
		return State{}, fmt.Errorf("bootstrap: remote %s serves an installation; refusing to install a legacy token: %w", a.Dir(), fs.ErrExist)
	case !a.Installation.IsZero() && st.Config == "legacy":
		return State{}, fmt.Errorf("bootstrap: remote %s is recorded for a legacy agent; refusing to serve an installation from it: %w", a.Dir(), fs.ErrExist)
	case st.Config != "none" && !st.ConfigSame:
		if !a.Installation.IsZero() {
			return State{}, fmt.Errorf("bootstrap: remote %s is configured for a different port, target or installation; refusing to install into it: %w", a.Dir(), fs.ErrExist)
		}
		return State{}, fmt.Errorf("bootstrap: remote %s is configured for a different port or target; refusing to install into it: %w", a.Dir(), fs.ErrExist)
	case !a.Installation.IsZero() && st.Files["token"] != "missing":
		return State{}, fmt.Errorf("bootstrap: remote %s holds a legacy token; refusing to serve an installation from it", a.Dir())
	}
	sums := map[string]string{}
	files := a.files()
	for _, name := range files {
		sum, err := fileSum(a.Local[name])
		if err != nil {
			return State{}, err
		}
		if name == "skygo-ssh" && sum != a.Digest {
			return State{}, fmt.Errorf("bootstrap: local agent binary is not digest %s", a.Digest)
		}
		switch got := st.Files[name]; got {
		case sum, "missing":
		case "unsafe":
			return State{}, fmt.Errorf("bootstrap: remote %s/%s is not a regular file; refusing to use it", a.Dir(), name)
		default:
			return State{}, fmt.Errorf("bootstrap: remote %s/%s differs from the local file; refusing to replace it", a.Dir(), name)
		}
		sums[name] = sum
	}
	for _, name := range files {
		if st.Files[name] != "missing" {
			continue
		}
		if err := put(ctx, r, a, name, sums[name], w); err != nil {
			return State{}, err
		}
		st.Files[name] = sums[name]
	}
	return st, nil
}

func put(ctx context.Context, r Runner, a Agent, name, sum string, w io.Writer) error {
	f, err := os.Open(a.Local[name])
	if err != nil {
		return fmt.Errorf("bootstrap: %w", err)
	}
	defer f.Close()
	if info, err := f.Stat(); err == nil {
		fmt.Fprintf(w, "skygo-ssh: copying %s (%d bytes) to %s\n", name, info.Size(), a.Dir())
	}
	_, err = run(ctx, r, f, "put", a.Commit, a.Digest, name, sum)
	return err
}

// configMismatch is the exact output of a start refused because the
// directory's configuration record names another port, target or
// installation.
const configMismatch = "agent configuration-mismatch\n"

// Start starts the agent in a's directory unless a running agent is
// recorded there, and returns the recorded process. The first start
// records a.Port, a.Target and a.Installation in the directory, and the
// record never changes: if it names another port, target or installation,
// or none where a names one or the reverse, Start changes nothing and
// returns an error wrapping fs.ErrExist. A stopped agent is restarted
// only if restart is set; an unverifiable process or configuration record
// is refused. An installation must already exist on the host; Start
// refuses one that does not before recording or starting anything.
func Start(ctx context.Context, r Runner, a Agent, restart bool) (int, error) {
	if err := a.validate(); err != nil {
		return 0, err
	}
	flag := "0"
	if restart {
		flag = "1"
	}
	operands := []string{"start", a.Commit, a.Digest, strconv.Itoa(a.Port), a.Target, flag}
	if i := a.Installation; !i.IsZero() {
		operands = append(operands, i.DataDir, i.AuthFile, i.ArtifactDir)
	}
	out, err := run(ctx, r, nil, operands...)
	if err != nil {
		return 0, err
	}
	if string(out) == configMismatch {
		if !a.Installation.IsZero() {
			return 0, fmt.Errorf("bootstrap: agent already configured for a different port, target or installation: %w", fs.ErrExist)
		}
		return 0, fmt.Errorf("bootstrap: agent already configured for a different port or target: %w", fs.ErrExist)
	}
	f := strings.Fields(string(out))
	if len(f) != 3 || f[0] != "agent" || f[1] != "started" && f[1] != "running" {
		return 0, fmt.Errorf("bootstrap: unexpected start output %q", out)
	}
	pid, err := strconv.Atoi(f[2])
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("bootstrap: unexpected start output %q", out)
	}
	return pid, nil
}

// Log returns the end of the agent's log.
func Log(ctx context.Context, r Runner, a Agent) (string, error) {
	if err := a.validate(); err != nil {
		return "", err
	}
	out, err := run(ctx, r, nil, "log", a.Commit, a.Digest)
	return string(out), err
}

// run runs script in mode with operands. A failure carries the script's
// diagnostic, which holds no file contents.
func run(ctx context.Context, r Runner, stdin io.Reader, operands ...string) ([]byte, error) {
	cmd, err := command(operands...)
	if err != nil {
		return nil, err
	}
	stdout, stderr, err := r.Run(ctx, cmd, stdin)
	if err != nil {
		if msg := printable(stderr); msg != "" {
			return nil, &scriptError{msg: "bootstrap: " + operands[0] + ": " + msg, err: err}
		}
		return nil, fmt.Errorf("bootstrap: %s: %w", operands[0], err)
	}
	return stdout, nil
}

// A scriptError reports a failed command by the script's diagnostic.
// Unwrap returns the Runner's original error.
type scriptError struct {
	msg string
	err error
}

func (e *scriptError) Error() string { return e.msg }

func (e *scriptError) Unwrap() error { return e.err }

// printable returns the last line of b with control bytes removed.
func printable(b []byte) string {
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	return strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f {
			return -1
		}
		return r
	}, lines[len(lines)-1])
}

func fileSum(name string) (string, error) {
	f, err := os.Open(name)
	if err != nil {
		return "", fmt.Errorf("bootstrap: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("bootstrap: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
