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

// An SSHRunner runs each command in its own session on Client. Sessions
// request no terminal, environment or agent forwarding.
type SSHRunner struct{ Client *ssh.Client }

// Run implements Runner. Canceling ctx closes Client, including a pending
// session open, and waits for the original operation. A canceled client
// cannot be reused.
func (r SSHRunner) Run(ctx context.Context, command string, stdin io.Reader) ([]byte, []byte, error) {
	if r.Client == nil {
		return nil, nil, errors.New("bootstrap: no SSH client")
	}
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(closed)
		r.Client.Close()
	})
	defer func() {
		if !stop() {
			<-closed
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, nil, context.Cause(ctx)
	}
	s, err := r.Client.NewSession()
	if err != nil {
		return nil, nil, errors.Join(err, context.Cause(ctx))
	}
	defer s.Close()
	stdout := &limitedBuffer{n: maxStdout}
	stderr := &limitedBuffer{n: maxStderr}
	s.Stdout, s.Stderr = stdout, stderr
	if stdin != nil {
		s.Stdin = stdin
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(command) }()
	select {
	case err = <-done:
	case <-ctx.Done():
		s.Close()
		<-done
		err = context.Cause(ctx)
	}
	return stdout.buf.Bytes(), stderr.buf.Bytes(), errors.Join(err, context.Cause(ctx))
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
	Arch   string            // required remote architecture, such as Arch
	Commit string            // full hex commit of the agent's source
	Digest string            // hex SHA-256 of the agent binary
	Port   int               // loopback port the agent listens on
	Target string            // name of the agent's built-in local target
	Local  map[string]string // local file for each name in Files
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
	for _, name := range Files {
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
}

// Status reports the state of a's directory. It changes nothing.
func Status(ctx context.Context, r Runner, a Agent) (State, error) {
	if err := a.validate(); err != nil {
		return State{}, err
	}
	out, err := run(ctx, r, nil, "status", a.Commit, a.Digest)
	if err != nil {
		return State{}, err
	}
	return parseStatus(out)
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
		default:
			return State{}, bad(s.Text())
		}
	}
	switch st.Agent {
	case "none", "running", "stopped", "unknown", "unsafe":
	default:
		return State{}, fmt.Errorf("bootstrap: unexpected agent state %q", st.Agent)
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
// any existing remote file that is not the local one. Progress, without
// file contents, goes to w.
func Install(ctx context.Context, r Runner, a Agent, w io.Writer) (State, error) {
	st, err := Status(ctx, r, a)
	if err != nil {
		return State{}, err
	}
	if st.Arch != a.Arch {
		return State{}, fmt.Errorf("bootstrap: remote architecture %q is not %s", st.Arch, a.Arch)
	}
	sums := map[string]string{}
	for _, name := range Files {
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
	for _, name := range Files {
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
// directory's configuration record names another port or target.
const configMismatch = "agent configuration-mismatch\n"

// Start starts the agent in a's directory unless a running agent is
// recorded there, and returns the recorded process. The first start
// records a.Port and a.Target in the directory, and the record never
// changes: if it names another port or target, Start changes nothing and
// returns an error wrapping fs.ErrExist. A stopped agent is restarted
// only if restart is set; an unverifiable process or configuration record
// is refused.
func Start(ctx context.Context, r Runner, a Agent, restart bool) (int, error) {
	if err := a.validate(); err != nil {
		return 0, err
	}
	flag := "0"
	if restart {
		flag = "1"
	}
	out, err := run(ctx, r, nil, "start", a.Commit, a.Digest, strconv.Itoa(a.Port), a.Target, flag)
	if err != nil {
		return 0, err
	}
	if string(out) == configMismatch {
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
			return nil, fmt.Errorf("bootstrap: %s: %s", operands[0], msg)
		}
		return nil, fmt.Errorf("bootstrap: %s: %w", operands[0], err)
	}
	return stdout, nil
}

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
