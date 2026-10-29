package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

const testCommit = "0123456789abcdef0123456789abcdef01234567"

// An execServer is an in-process SSH server that runs each exec request
// with the local /bin/sh and HOME set to a private directory, as sshd
// runs it with the user's login shell.
//
// The execServer owns every process it starts and signals none. Each
// command runs behind fixturePrelude, so the shell running the script
// reports its exit status on a result pipe, which ends the session, and
// then waits for the agent it started before it exits. Agents built from
// sleeper run until end of file on the stop pipe, inherited as descriptor
// 4, which only release closes. Cleanup shuts the server down, releases
// the agents, joins every original command and fixture goroutine, and
// only then removes HOME; if it cannot, it holds them all and says so.
//
// Fixture goroutines may outlive the test, so they never use its
// testing.T: they record failures with diag, and only settle, called by
// the test or its cleanup, reports them.
type execServer struct {
	home   string
	ln     net.Listener
	client *ssh.Client
	wg     sync.WaitGroup // serve, connection, session and request goroutines

	stop  *os.File      // read end of the stop pipe, every command's descriptor 4
	stopW *os.File      // write end, never inherited, closed by release
	done  chan struct{} // closed by shutdown

	served      chan struct{} // closed once wg is done after shutdown
	shutOnce    sync.Once
	releaseOnce sync.Once

	mu             sync.Mutex
	closed         bool // set by shutdown: no command starts after it
	retired        bool
	execs          []execRecord
	launches       []*launch
	diags          []string // fixture failures, in the order recorded
	reported       int      // how many of diags settle has reported
	holdSessions   chan struct{}
	sessionArrived chan struct{}
}

type execRecord struct {
	command string
	stdin   []byte
	stdout  []byte
	launch  *launch // nil if the shell did not start
}

// A launch is one original command: the /bin/sh the execServer started,
// which becomes the shell running the script.
type launch struct {
	command  string
	pid      int // data only: never signaled or looked up
	cmd      *exec.Cmd
	life     *os.File      // read end of a pipe every descendant inherits as descriptor 5
	done     chan struct{} // closed when cmd.Wait returns
	err      error         // cmd.Wait's result, set before done is closed
	orphaned bool          // guarded by execServer.mu
}

// holdBound bounds how long the fixture waits for a join it expects
// before it holds the namespace. A command whose agents were released
// is joined as soon as they read end of file; the bound is reached only
// when something was not.
const holdBound = 10 * time.Second

// held keeps every execServer whose cleanup could not join what it
// started, with its namespace, process handles and diagnostics, until the
// test binary exits.
var held struct {
	sync.Mutex
	servers []*execServer
}

// fixturePrelude precedes each command an execServer runs. It defines sh
// so that the one command line every mode sends, "sh -c SCRIPT
// skygo-bootstrap OPERANDS", replaces the login shell with a /bin/sh
// running the exact script text between fixtureEnter and fixtureLeave,
// with $0 skygo-bootstrap and the same operands. It refuses any other
// invocation of sh rather than run other text.
var fixturePrelude = "fixture_script=" + quote(script) + "\n" +
	"fixture_text=" + quote(fixtureEnter+script+fixtureLeave) + "\n" + `sh() {
	if [ "$#" -lt 3 ] || [ "$1" != -c ] || [ "$2" != "$fixture_script" ] || [ "$3" != skygo-bootstrap ]; then
		printf "execServer: refusing an unexpected sh invocation\n" >&2
		exit 125
	fi
	shift 2
	exec /bin/sh -c "$fixture_text" "$@"
}
`

// fixtureEnter precedes the script. The shell reports its exit status
// once, on the result pipe, its descriptor 3: from the EXIT trap, or from
// fixtureLeave if the script ends without exiting. Reporting first closes
// the shell's standard input, output and error, so the session can end
// with all of its output, and afterwards the shell waits for its
// background jobs, the agent it started, before it exits.
const fixtureEnter = `fixture_reported=
fixture_report() {
	if [ -z "$fixture_reported" ]; then
		fixture_reported=1
		exec 0<&- 1>&- 2>&-
		printf "%s\n" "$1" >&3 || :
		exec 3>&-
	fi
}
fixture_exit() {
	fixture_status=$?
	fixture_report "$fixture_status"
	wait
	exit "$fixture_status"
}
trap fixture_exit EXIT
`

// fixtureLeave follows the script. When record has run it has replaced
// the EXIT trap with clean, which the script runs as it exits, before the
// client sees the status, so fixtureLeave runs clean before reporting. A
// script that exits after record has run is not reported; the execServer
// then takes the status from the shell, which waited for nothing.
const fixtureLeave = `fixture_status=$?
trap - EXIT
if [ "$mode" = start ] && [ -n "$want" ]; then clean; fi
fixture_report "$fixture_status"
wait
exit "$fixture_status"
`

// fixtureShape reports an error if script no longer has the shape
// fixtureEnter and fixtureLeave rely on.
func fixtureShape() error {
	switch {
	case strings.Count(script, "trap ") != 1 || !strings.Contains(script, "\twant=$t\n\ttrap clean EXIT\n"):
		return errors.New("execServer: script must trap EXIT only in record, after setting want")
	case !strings.Contains(script, "\nmode=${1-} ") || !strings.Contains(script, "\nwant=\n"):
		return errors.New("execServer: script must set mode and want before any mode runs")
	case strings.Contains(script, "fixture_") || strings.Contains(script, "wait") || strings.Contains(script, "exec "):
		return errors.New("execServer: script uses a fixture name, wait or exec")
	case !strings.HasSuffix(script, "\n"):
		return errors.New("execServer: script does not end with a newline")
	}
	for _, fd := range []string{"3", "4", "5"} {
		if strings.Contains(script, fd+">") || strings.Contains(script, fd+"<") || strings.Contains(script, "&"+fd) {
			return fmt.Errorf("execServer: script uses descriptor %s", fd)
		}
	}
	return nil
}

func newExecServer(t *testing.T) *execServer {
	t.Helper()
	if err := fixtureShape(); err != nil {
		t.Fatal(err)
	}
	home, err := os.MkdirTemp("", "skygo-bootstrap-home-")
	if err != nil {
		t.Fatal(err)
	}
	s := &execServer{
		home:           home,
		done:           make(chan struct{}),
		served:         make(chan struct{}),
		sessionArrived: make(chan struct{}, 1),
	}
	t.Cleanup(func() { s.cleanup(t) })
	if err := os.Chmod(home, 0o700); err != nil {
		t.Fatal(err)
	}
	s.stop, s.stopW, err = os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostKey, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{NoClientAuth: true}
	config.AddHostKey(hostKey)
	s.ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.wg.Add(1)
	go s.serve(config)
	s.client, err = ssh.Dial("tcp", s.ln.Addr().String(), &ssh.ClientConfig{
		User:            "skygo",
		HostKeyCallback: ssh.FixedHostKey(hostKey.PublicKey()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// cleanup shuts the server down, releases the agents and, once every
// original command and fixture goroutine is joined, removes HOME. If one
// is not, it keeps HOME, the server and its process handles, and fails
// the test with a HOLD. Either way it fails the test with every fixture
// diagnostic recorded so far.
func (s *execServer) cleanup(t *testing.T) {
	s.shutdown()
	s.release()
	s.settle(holdBound, t.Errorf)
}

// settle retires s within bound and passes report every diagnostic not
// reported before. If s cannot retire, settle also reports a HOLD and
// keeps s in held; a diagnostic recorded afterwards stays in s, unreported
// unless a later settle retires s.
func (s *execServer) settle(bound time.Duration, report func(format string, args ...any)) error {
	err := s.retire(bound)
	s.mu.Lock()
	diags := slices.Clone(s.diags[s.reported:])
	s.reported = len(s.diags)
	s.mu.Unlock()
	for _, d := range diags {
		report("%s", d)
	}
	held.Lock()
	defer held.Unlock()
	if err != nil {
		if !slices.Contains(held.servers, s) {
			held.servers = append(held.servers, s)
		}
		report("HOLD: %v; keeping %s, the processes started in it and their diagnostics", err, s.home)
		return err
	}
	held.servers = slices.DeleteFunc(held.servers, func(h *execServer) bool { return h == s })
	return nil
}

// diag records a fixture failure for settle to report.
func (s *execServer) diag(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.diags = append(s.diags, fmt.Sprintf(format, args...))
}

// diagnostics returns the fixture failures recorded so far.
func (s *execServer) diagnostics() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.diags)
}

// shutdown closes admission, the client and the listener.
func (s *execServer) shutdown() {
	s.shutOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
		if s.client != nil {
			s.client.Close()
		}
		if s.ln != nil {
			s.ln.Close()
		}
		go func() {
			s.wg.Wait()
			close(s.served)
		}()
	})
}

// release closes the stop pipe, so every agent the execServer started
// reads end of file and exits. Afterwards only commands that start no
// agent may run.
func (s *execServer) release() {
	s.releaseOnce.Do(func() {
		if s.stopW != nil {
			s.stopW.Close()
		}
	})
}

// retire shuts the server down and removes HOME once every fixture
// goroutine and every original command is joined and nothing started by
// one still holds its lifetime pipe. It waits at most bound; otherwise
// it removes nothing and reports what it could not join.
func (s *execServer) retire(bound time.Duration) error {
	s.shutdown()
	if s.retired {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	select {
	case <-s.served:
	case <-ctx.Done():
		return errors.New("execServer goroutines not joined")
	}
	// Every session has ended, so every launch is registered.
	s.mu.Lock()
	launches := append([]*launch(nil), s.launches...)
	s.mu.Unlock()
	deadline, _ := ctx.Deadline()
	for _, l := range launches {
		select {
		case <-l.done:
		case <-ctx.Done():
			return fmt.Errorf("original command (pid %d) not joined", l.pid)
		}
		s.mu.Lock()
		orphaned := l.orphaned
		s.mu.Unlock()
		if orphaned {
			return fmt.Errorf("original command (pid %d) exited before a process it started", l.pid)
		}
		if l.life != nil {
			l.life.SetReadDeadline(deadline)
			if _, err := io.Copy(io.Discard, l.life); err != nil {
				return fmt.Errorf("a process started by original command (pid %d) still runs: %v", l.pid, err)
			}
		}
	}
	// Remove HOME before closing the lifetime pipes, so that if removal
	// fails the held server still has them, and a later retire reads
	// them again rather than failing on a closed file.
	if err := os.RemoveAll(s.home); err != nil {
		return fmt.Errorf("removing namespace: %w", err)
	}
	for _, l := range launches {
		if l.life != nil {
			l.life.Close()
		}
	}
	if s.stop != nil {
		s.stop.Close()
	}
	s.retired = true
	return nil
}

func (s *execServer) serve(config *ssh.ServerConfig) {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer c.Close()
			conn, chans, reqs, err := ssh.NewServerConn(c, config)
			if err != nil {
				return
			}
			defer conn.Close()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				ssh.DiscardRequests(reqs)
			}()
			for nc := range chans {
				if nc.ChannelType() != "session" {
					nc.Reject(ssh.UnknownChannelType, "refused")
					continue
				}
				s.mu.Lock()
				hold := s.holdSessions
				s.mu.Unlock()
				if hold != nil {
					select {
					case s.sessionArrived <- struct{}{}:
					case <-s.done:
					}
					select {
					case <-hold:
					case <-s.done:
						nc.Reject(ssh.ConnectionFailed, "shutting down")
						continue
					}
				}
				ch, requests, err := nc.Accept()
				if err != nil {
					return
				}
				s.wg.Add(1)
				go s.session(ch, requests)
			}
		}()
	}
}

func (s *execServer) session(ch ssh.Channel, requests <-chan *ssh.Request) {
	defer s.wg.Done()
	defer ch.Close()
	for req := range requests {
		if req.Type != "exec" {
			// No pty, env or agent forwarding is ever requested.
			s.diag("execServer: unexpected session request %q", req.Type)
			req.Reply(false, nil)
			continue
		}
		// Requests after the exec are refused until the channel closes.
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			ssh.DiscardRequests(requests)
		}()
		var payload struct{ Command string }
		if err := ssh.Unmarshal(req.Payload, &payload); err != nil {
			req.Reply(false, nil)
			return
		}
		s.mu.Lock()
		closed := s.closed
		s.mu.Unlock()
		if closed {
			req.Reply(false, nil)
			return
		}
		req.Reply(true, nil)
		r, code := s.exec(ch, payload.Command)
		s.mu.Lock()
		s.execs = append(s.execs, r)
		s.mu.Unlock()
		ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(code)}))
		return
	}
}

// exec runs command behind fixturePrelude and returns its record and its
// logical exit status: the one the script reported, or the shell's own
// if it reported none. It returns once the command's input and output are
// finished, which may be long before the original command is.
func (s *execServer) exec(ch ssh.Channel, command string) (execRecord, int) {
	r := execRecord{command: command}
	ps, err := pipes(5)
	if err != nil {
		s.diag("execServer: %v", err)
		return r, 255
	}
	in, out, errp, result, life := ps[0], ps[1], ps[2], ps[3], ps[4]
	cmd := exec.Command("/bin/sh", "-c", fixturePrelude+command)
	cmd.Env = []string{"HOME=" + s.home, "PATH=" + os.Getenv("PATH")}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = in[0], out[1], errp[1]
	cmd.ExtraFiles = []*os.File{result[1], s.stop, life[1]}
	err = cmd.Start()
	// The child has its own copies of these ends, or none.
	for _, f := range []*os.File{in[0], out[1], errp[1], result[1], life[1]} {
		f.Close()
	}
	if err != nil {
		for _, f := range []*os.File{in[1], out[0], errp[0], result[0], life[0]} {
			f.Close()
		}
		return r, 255
	}
	r.launch = s.track(command, cmd, life[0])

	var stdin, stdout bytes.Buffer
	var copies sync.WaitGroup
	copies.Add(3)
	go func() {
		defer copies.Done()
		io.Copy(in[1], io.TeeReader(ch, &stdin))
		in[1].Close()
	}()
	go func() {
		defer copies.Done()
		drain(ch, io.TeeReader(out[0], &stdout))
		out[0].Close()
	}()
	go func() {
		defer copies.Done()
		drain(ch.Stderr(), errp[0])
		errp[0].Close()
	}()
	code := s.result(r.launch, result[0])
	copies.Wait()
	r.stdin, r.stdout = stdin.Bytes(), stdout.Bytes()
	return r, code
}

// track registers cmd, just started, as an original command and waits
// for it in a goroutine joined through done.
func (s *execServer) track(command string, cmd *exec.Cmd, life *os.File) *launch {
	l := &launch{command: command, pid: cmd.Process.Pid, cmd: cmd, life: life, done: make(chan struct{})}
	s.mu.Lock()
	s.launches = append(s.launches, l)
	s.mu.Unlock()
	go func() {
		l.err = cmd.Wait()
		close(l.done)
	}()
	return l
}

// result returns the exit status reported on r. If every holder closed r
// without a report, the fixture's code did not run, as for a refused sh
// invocation or a script that exits after record replaced the EXIT trap;
// the shell has then exited, and result takes its status, after checking
// that nothing it started outlived it.
func (s *execServer) result(l *launch, r *os.File) int {
	defer r.Close()
	line, err := bufio.NewReader(r).ReadString('\n')
	if err == nil {
		code, err := strconv.Atoi(strings.TrimSuffix(line, "\n"))
		if err != nil || code < 0 || code > 255 {
			s.diag("execServer: command %q reported unexpected result %q", l.command, line)
			return 255
		}
		return code
	}
	<-l.done
	l.life.SetReadDeadline(time.Now().Add(holdBound))
	if _, err := io.Copy(io.Discard, l.life); err != nil {
		s.mu.Lock()
		l.orphaned = true
		s.mu.Unlock()
		s.diag("execServer: command %q left a process running after its shell exited", l.command)
	}
	code := 0
	if err := l.err; err != nil {
		code = 255
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			code = exit.ExitCode()
		}
	}
	return code
}

// pipes returns n pipes as read and write ends, or closes them all.
func pipes(n int) ([][2]*os.File, error) {
	var ps [][2]*os.File
	for range n {
		r, w, err := os.Pipe()
		if err != nil {
			for _, p := range ps {
				p[0].Close()
				p[1].Close()
			}
			return nil, err
		}
		ps = append(ps, [2]*os.File{r, w})
	}
	return ps, nil
}

// drain copies r to w and, if w fails, discards the rest of r, so the
// writer of r never blocks.
func drain(w io.Writer, r io.Reader) {
	if _, err := io.Copy(w, r); err != nil {
		io.Copy(io.Discard, r)
	}
}

// starts returns the original commands that reported starting an agent.
func (s *execServer) starts() []*launch {
	var ls []*launch
	for _, r := range s.records() {
		if strings.HasPrefix(string(r.stdout), "agent started ") {
			ls = append(ls, r.launch)
		}
	}
	return ls
}

// joined reports whether l's original command has been waited for.
func joined(l *launch) bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// waitJoined waits for l's original command, which must be joinable.
func waitJoined(t *testing.T, l *launch) {
	t.Helper()
	select {
	case <-l.done:
	case <-time.After(holdBound):
		t.Fatalf("original command (pid %d) not joined", l.pid)
	}
}

func (s *execServer) records() []execRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]execRecord(nil), s.execs...)
}

// puts returns the file names transferred so far.
func (s *execServer) puts() []string {
	var names []string
	for _, r := range s.records() {
		if f := strings.Fields(r.command); f[len(f)-5] == "'put'" {
			names = append(names, strings.Trim(f[len(f)-2], "'"))
		}
	}
	return names
}

func (s *execServer) runner() Runner { return SSHRunner{Client: s.client} }

func TestCancelPendingSessionOpen(t *testing.T) {
	s := newExecServer(t)
	hold := make(chan struct{})
	s.mu.Lock()
	s.holdSessions = hold
	s.mu.Unlock()
	t.Cleanup(func() { close(hold) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := s.runner().Run(ctx, "true", nil)
		done <- err
	}()
	select {
	case <-s.sessionArrived:
	case <-time.After(3 * time.Second):
		t.Fatal("session open did not arrive")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending session open = %v; want canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled session open did not return")
	}
}

// remote returns the path of name in a's remote directory.
func (s *execServer) remote(a Agent, name string) string {
	return filepath.Join(s.home, "skygo-test", a.Commit, a.Digest, name)
}

// newAgent writes local agent files under a new directory. The binary is
// the given content.
func newAgent(t *testing.T, binary []byte) Agent {
	t.Helper()
	dir := t.TempDir()
	sum := sha256.Sum256(binary)
	arch, err := exec.Command("uname", "-m").Output()
	if err != nil {
		t.Fatal(err)
	}
	a := Agent{Arch: strings.TrimSpace(string(arch)), Commit: testCommit, Digest: hex.EncodeToString(sum[:]), Port: 47080, Target: "dgx", Local: map[string]string{}}
	contents := map[string][]byte{
		"skygo-ssh": binary,
		"cert.pem":  []byte("-----BEGIN CERTIFICATE-----\ntest certificate\n-----END CERTIFICATE-----\n"),
		"key.pem":   []byte("-----BEGIN EC PRIVATE KEY-----\nsecret-key-" + randomHex(t) + "\n-----END EC PRIVATE KEY-----\n"),
		"token":     []byte("secret-token-" + randomHex(t) + "\n"),
	}
	for name, data := range contents {
		a.Local[name] = filepath.Join(dir, name)
		if err := os.WriteFile(a.Local[name], data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func randomHex(t *testing.T) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// sleeper is an agent binary that runs until the execServer releases it.
// It closes the result pipe of the command that started it, descriptor 3,
// and reads the stop pipe, descriptor 4, with the shell's builtin read,
// which returns at end of file. Started anywhere else, it exits at once.
var sleeper = []byte("#!/bin/sh\nexec 3>&-\nread -r line <&4 || :\nexit 0\n")

// shellArgv runs line with a POSIX shell in which sh is a function that
// prints each argument it receives followed by a NUL byte, and returns
// those bytes. It shows the argument vector a command line passes to sh.
func shellArgv(t *testing.T, line string) []byte {
	t.Helper()
	const intercept = `sh() { for a in "$@"; do printf '%s\0' "$a"; done; }` + "\n"
	out, err := exec.Command("/bin/sh", "-c", intercept+line).Output()
	if err != nil {
		var stderr []byte
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
			stderr = ee.Stderr
		}
		t.Fatalf("/bin/sh: %v\n%s", err, stderr)
	}
	return out
}

// nulJoin returns args each followed by a NUL byte.
func nulJoin(args ...string) []byte {
	var b bytes.Buffer
	for _, a := range args {
		b.WriteString(a)
		b.WriteByte(0)
	}
	return b.Bytes()
}

func TestScript(t *testing.T) {
	if out, err := exec.Command("/bin/sh", "-n", "-c", script).CombinedOutput(); err != nil {
		t.Fatalf("sh -n: %v\n%s", err, out)
	}

	// The command line must hand sh exactly -c, the script, the program
	// name and the operands, whatever the script contains.
	digest := strings.Repeat("a", 64)
	for _, operands := range [][]string{
		{"status", testCommit, digest},
		{"status", testCommit, digest, "8022", "web"},
		{"status", testCommit, digest, "8022", "web", "/var/lib/skygo", "/etc/skygo/auth_token.json", "/srv/skygo-artifacts/v1.2"},
		{"start", testCommit, digest, "8022", "web", "true"},
		{"log", testCommit, digest},
	} {
		cmd, err := command(operands...)
		if err != nil {
			t.Fatalf("command(%q): %v", operands, err)
		}
		want := nulJoin(append([]string{"-c", script, "skygo-bootstrap"}, operands...)...)
		if got := shellArgv(t, cmd); !bytes.Equal(got, want) {
			t.Errorf("command(%q) passes sh %q, want %q", operands, got, want)
		}
	}

	// quote must round-trip any text, apostrophes included.
	for _, s := range []string{"", "'", "''", "head's bytes", "a'b'c", "'\\''", "it's\n\"$x\" `id` $(id) \\", script + "'", "'" + script} {
		if got, want := shellArgv(t, "sh "+quote(s)), nulJoin(s); !bytes.Equal(got, want) {
			t.Errorf("quote(%q) passes sh %q, want %q", s, got, want)
		}
	}

	for _, bad := range []string{"", "a b", "$(id)", "x'y", "UPPER", "a;b", "../x", "a\nb", "`id`", strings.Repeat("a", 129),
		"/", "/a/", "/a/../b", "/a//b", "rel/path", "/a b", "/a'b", "/$(id)", "/" + strings.Repeat("a", 200)} {
		if _, err := command("status", bad); err == nil {
			t.Errorf("command accepted operand %q", bad)
		}
	}
}

func TestInstall(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, []byte("agent binary"))
	ctx := context.Background()
	var progress bytes.Buffer
	st, err := Install(ctx, s.runner(), a, &progress)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.puts(), " "); got != "skygo-ssh cert.pem key.pem token" {
		t.Fatalf("transferred %q", got)
	}
	if st.Agent != "none" {
		t.Fatalf("agent state = %q, want none", st.Agent)
	}
	for _, name := range Files {
		want, _ := os.ReadFile(a.Local[name])
		info, err := os.Lstat(s.remote(a, name))
		if err != nil {
			t.Fatal(err)
		}
		mode := fs.FileMode(0o400)
		if name == "skygo-ssh" {
			mode = 0o500
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != mode {
			t.Errorf("%s mode = %v, want %v", name, info.Mode(), mode)
		}
		if got, _ := os.ReadFile(s.remote(a, name)); !bytes.Equal(got, want) {
			t.Errorf("%s content differs", name)
		}
	}
	// Only the namespace exists, every directory is private and no
	// temporary file remains.
	err = filepath.WalkDir(s.home, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == s.home {
			return err
		}
		rel, _ := filepath.Rel(s.home, path)
		if !strings.HasPrefix(rel+"/", "skygo-test/") {
			t.Errorf("wrote %s outside skygo-test", rel)
		}
		if strings.HasPrefix(d.Name(), ".put-") {
			t.Errorf("temporary file %s remains", rel)
		}
		if info, _ := d.Info(); d.IsDir() && info.Mode().Perm() != 0o700 {
			t.Errorf("%s mode %v, want 0700", rel, info.Mode().Perm())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	checkNoSecrets(t, s, a, progress.String())

	// Installing again transfers nothing.
	before := len(s.records())
	if _, err := Install(ctx, s.runner(), a, io.Discard); err != nil {
		t.Fatal(err)
	}
	if n := len(s.records()) - before; n != 1 {
		t.Fatalf("second install ran %d commands, want one status", n)
	}
}

// checkNoSecrets fails if a command line or progress text carries the key
// or token, or if they were sent anywhere but their own transfer.
func checkNoSecrets(t *testing.T, s *execServer, a Agent, progress string) {
	t.Helper()
	for _, name := range []string{"key.pem", "token"} {
		secret, _ := os.ReadFile(a.Local[name])
		secret = bytes.TrimSpace(secret)
		if strings.Contains(progress, string(secret)) {
			t.Errorf("progress output contains %s", name)
		}
		for _, r := range s.records() {
			if strings.Contains(r.command, string(secret)) {
				t.Errorf("a command line contains %s", name)
			}
			if bytes.Contains(r.stdin, secret) && !strings.Contains(r.command, "'put'") {
				t.Errorf("%s was sent to a command other than its transfer", name)
			}
			if bytes.Contains(r.stdin, secret) && !strings.Contains(r.command, "'"+name+"'") {
				t.Errorf("%s was sent with another file's transfer", name)
			}
		}
	}
}

func TestInstallRefusesMismatch(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, []byte("agent binary"))
	dir := filepath.Dir(s.remote(a, "cert.pem"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.remote(a, "cert.pem"), []byte("another identity\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.remote(a, "skygo-ssh"), []byte("agent binary"), 0o500); err != nil {
		t.Fatal(err)
	}
	_, err := Install(context.Background(), s.runner(), a, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cert.pem differs from the local file") {
		t.Fatalf("Install = %v, want mismatch refusal", err)
	}
	if got, _ := os.ReadFile(s.remote(a, "cert.pem")); string(got) != "another identity\n" {
		t.Fatalf("mismatched file was replaced with %q", got)
	}
	// The matching binary was kept and nothing after the mismatch was sent.
	if got := s.puts(); len(got) != 0 {
		t.Fatalf("transferred %q", got)
	}
}

func TestInstallRefusesSymlinks(t *testing.T) {
	tests := []struct {
		name string
		link func(s *execServer, a Agent, outside string) error
		err  string
	}{
		{"file", func(s *execServer, a Agent, outside string) error {
			if err := os.MkdirAll(filepath.Dir(s.remote(a, "token")), 0o700); err != nil {
				return err
			}
			return os.Symlink(outside, s.remote(a, "token"))
		}, "token is not a regular file"},
		{"commit directory", func(s *execServer, a Agent, outside string) error {
			if err := os.MkdirAll(filepath.Join(s.home, "skygo-test"), 0o700); err != nil {
				return err
			}
			return os.Symlink(filepath.Dir(outside), filepath.Join(s.home, "skygo-test", a.Commit))
		}, "is a symlink"},
		{"root", func(s *execServer, a Agent, outside string) error {
			return os.Symlink(filepath.Dir(outside), filepath.Join(s.home, "skygo-test"))
		}, "is a symlink"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExecServer(t)
			a := newAgent(t, []byte("agent binary"))
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte("unrelated\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := tt.link(s, a, outside); err != nil {
				t.Fatal(err)
			}
			_, err := Install(context.Background(), s.runner(), a, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("Install = %v, want %q", err, tt.err)
			}
			if got, _ := os.ReadFile(outside); string(got) != "unrelated\n" {
				t.Fatalf("symlink target changed to %q", got)
			}
			entries, _ := os.ReadDir(filepath.Dir(outside))
			if len(entries) != 1 {
				t.Fatalf("wrote through the symlink: %v", entries)
			}
		})
	}
}

// A fakeRunner answers commands with fixed output and records them.
type fakeRunner struct {
	out  map[string]string // mode to stdout
	cmds []string
}

func (f *fakeRunner) Run(_ context.Context, command string, _ io.Reader) ([]byte, []byte, error) {
	f.cmds = append(f.cmds, command)
	for mode, out := range f.out {
		if strings.Contains(command, " skygo-bootstrap '"+mode+"'") {
			return []byte(out), nil, nil
		}
	}
	return nil, []byte("skygo-bootstrap: refused\n"), errors.New("exit status 1")
}

func status(arch, agent string, files map[string]string) string {
	var b strings.Builder
	b.WriteString("arch " + arch + "\n")
	for _, name := range Files {
		v := files[name]
		if v == "" {
			v = "missing"
		}
		b.WriteString("file " + name + " " + v + "\n")
	}
	b.WriteString("agent " + agent + "\n")
	b.WriteString("config none\n")
	return b.String()
}

func TestInstallRefusesArch(t *testing.T) {
	a := newAgent(t, []byte("agent binary"))
	a.Arch = Arch
	r := &fakeRunner{out: map[string]string{"status": status("x86_64", "none", nil)}}
	if _, err := Install(context.Background(), r, a, io.Discard); err == nil || !strings.Contains(err.Error(), "architecture") {
		t.Fatalf("Install = %v, want architecture refusal", err)
	}
	if len(r.cmds) != 1 {
		t.Fatalf("ran %d commands, want only status", len(r.cmds))
	}
}

func TestParseStatus(t *testing.T) {
	sum := strings.Repeat("b", 64)
	good := status("aarch64", "running 42", map[string]string{"skygo-ssh": sum, "cert.pem": "unsafe"})
	st, err := parseStatus([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if st.Arch != "aarch64" || st.Agent != "running" || st.PID != 42 || st.Files["skygo-ssh"] != sum || st.Files["cert.pem"] != "unsafe" || st.Files["token"] != "missing" || st.Config != "none" {
		t.Fatalf("parseStatus = %+v", st)
	}
	for _, tt := range []struct {
		line string
		form string
		same bool
	}{
		{"config unsafe", "unsafe", false},
		{"config legacy same", "legacy", true},
		{"config legacy other", "legacy", false},
		{"config installation same", "installation", true},
		{"config installation other", "installation", false},
	} {
		st, err := parseStatus([]byte(strings.Replace(good, "config none", tt.line, 1)))
		if err != nil || st.Config != tt.form || st.ConfigSame != tt.same {
			t.Errorf("parseStatus(%q) = %q same=%v, %v; want %q same=%v", tt.line, st.Config, st.ConfigSame, err, tt.form, tt.same)
		}
	}
	for _, bad := range []string{
		"",
		strings.Replace(good, "config none\n", "", 1),
		strings.Replace(good, "config none", "config other", 1),
		strings.Replace(good, "config none", "config legacy", 1),
		strings.Replace(good, "config none", "config installation", 1),
		strings.Replace(good, "config none", "config none same", 1),
		strings.Replace(good, "config none", "config unsafe other", 1),
		strings.Replace(good, "config none", "config legacy maybe", 1),
		strings.Replace(good, "config none", "config legacy same port", 1),
		strings.Replace(good, "agent running 42", "agent started", 1),
		strings.Replace(good, "running 42", "running -1", 1),
		strings.Replace(good, "file token missing", "file token SECRET", 1),
		strings.Replace(good, "file token missing\n", "", 1),
		good + "extra line\n",
		strings.Replace(good, "arch aarch64\n", "", 1),
	} {
		if st, err := parseStatus([]byte(bad)); err == nil {
			t.Errorf("parseStatus(%q) = %+v, want error", bad, st)
		}
	}
}

// install places a's files remotely without a status check.
func install(t *testing.T, s *execServer, a Agent) {
	t.Helper()
	for _, name := range Files {
		sum, err := fileSum(a.Local[name])
		if err != nil {
			t.Fatal(err)
		}
		if err := put(context.Background(), s.runner(), a, name, sum, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPutRefused(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, []byte("agent binary"))
	install(t, s, a)
	ctx := context.Background()
	if err := put(ctx, s.runner(), a, "token", strings.Repeat("c", 64), io.Discard); err == nil || !strings.Contains(err.Error(), "token already exists") {
		t.Fatalf("put over an existing file = %v", err)
	}
	b := newAgent(t, []byte("agent binary"))
	b.Digest = strings.Repeat("d", 64)
	if err := put(ctx, s.runner(), b, "skygo-ssh", strings.Repeat("e", 64), io.Discard); err == nil || !strings.Contains(err.Error(), "not the digest") {
		t.Fatalf("put of a binary that is not the digest = %v", err)
	}
	if err := put(ctx, s.runner(), b, "skygo-ssh", b.Digest, io.Discard); err == nil || !strings.Contains(err.Error(), "differs after transfer") {
		t.Fatalf("put of corrupted content = %v", err)
	}
	if _, err := os.Lstat(s.remote(b, "skygo-ssh")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupted binary was stored: %v", err)
	}
}

func TestStartNoReplay(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, sleeper)
	install(t, s, a)
	ctx := context.Background()
	pidfile := s.remote(a, "agent.pid")

	// A start that was lost before recording its process is never replayed.
	if err := os.WriteFile(pidfile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, restart := range []bool{false, true} {
		if _, err := Start(ctx, s.runner(), a, restart); err == nil || !strings.Contains(err.Error(), "agent state unknown") {
			t.Fatalf("Start(restart=%v) after a lost start = %v", restart, err)
		}
	}
	if data, _ := os.ReadFile(pidfile); len(data) != 0 {
		t.Fatalf("pid record changed to %q", data)
	}
	if _, err := os.Stat(s.remote(a, "agent.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused start launched the agent")
	}
	record := s.remote(a, "agent.config")
	if _, err := os.Lstat(record); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused start recorded a configuration")
	}

	// A recorded process that has exited is restarted only on request.
	exited := exec.Command("/bin/sh", "-c", "exit 0")
	if err := exited.Run(); err != nil {
		t.Fatal(err)
	}
	dead := strconv.Itoa(exited.ProcessState.Pid())
	if err := os.WriteFile(pidfile, []byte(dead+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := Status(ctx, s.runner(), a)
	if err != nil || st.Agent != "stopped" || strconv.Itoa(st.PID) != dead {
		t.Fatalf("Status = %+v, %v; want stopped %s", st, err, dead)
	}
	if _, err := Start(ctx, s.runner(), a, false); err == nil || !strings.Contains(err.Error(), "--restart-stopped") {
		t.Fatalf("Start of a stopped agent = %v, want refusal", err)
	}
	if _, err := os.Lstat(record); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused restart recorded a configuration")
	}
	if n := len(s.starts()); n != 0 {
		t.Fatalf("refused starts started %d agents", n)
	}
	pid, err := Start(ctx, s.runner(), a, true)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(s.starts()); n != 1 {
		t.Fatalf("restart started %d agents, want one", n)
	}
	if data, _ := os.ReadFile(pidfile); string(data) != strconv.Itoa(pid)+"\n" {
		t.Fatalf("pid record = %q, want %d", data, pid)
	}
	if data, _ := os.ReadFile(pidfile + ".stopped"); string(data) != dead+"\n" {
		t.Fatalf("stopped record = %q, want %s", data, dead)
	}
	// The process stopped without a configuration record, so the restart
	// records one.
	if data, _ := os.ReadFile(record); string(data) != configRecord(a) {
		t.Fatalf("configuration record = %q, want %q", data, configRecord(a))
	}
}

// configRecord returns the configuration record a's first start writes.
func configRecord(a Agent) string {
	return "port " + strconv.Itoa(a.Port) + "\ntarget " + a.Target + "\n"
}

// exitedPID returns the ID of a process that has exited.
func exitedPID(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return strconv.Itoa(cmd.ProcessState.Pid())
}

// snapshot describes every entry in a's remote directory, with file
// contents as checksums, so a test can check that a start changed nothing.
func snapshot(t *testing.T, s *execServer, a Agent) map[string]string {
	t.Helper()
	dir := filepath.Dir(s.remote(a, "agent.pid"))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]string{}
	for _, e := range entries {
		p := filepath.Join(dir, e.Name())
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, _ := os.Readlink(p)
			m[e.Name()] = "symlink " + target
		case info.IsDir():
			m[e.Name()] = "directory"
		default:
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			m[e.Name()] = info.Mode().String() + " " + hex.EncodeToString(sum[:])
		}
	}
	return m
}

func writeConfig(t *testing.T, s *execServer, a Agent, data string) {
	t.Helper()
	if err := os.WriteFile(s.remote(a, "agent.config"), []byte(data), 0o400); err != nil {
		t.Fatal(err)
	}
}

// TestStartConfigProtocol rejects near-matches instead of classifying
// arbitrary successful output as a configuration conflict.
func TestStartConfigProtocol(t *testing.T) {
	a := newAgent(t, sleeper)
	tests := []struct {
		name     string
		out      string
		conflict bool
	}{
		{"exact", configMismatch, true},
		{"no newline", strings.TrimSuffix(configMismatch, "\n"), false},
		{"extra line", configMismatch + "agent running 42\n", false},
		{"prefix", "unexpected " + configMismatch, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &fakeRunner{out: map[string]string{"start": tt.out}}
			pid, err := Start(context.Background(), r, a, false)
			if err == nil || pid != 0 || errors.Is(err, fs.ErrExist) != tt.conflict {
				t.Fatalf("Start = %d, %v; want conflict=%v", pid, err, tt.conflict)
			}
		})
	}
}

func TestStartRecordsConfig(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, sleeper)
	install(t, s, a)
	pid, err := Start(context.Background(), s.runner(), a, false)
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(s.remote(a, "agent.pid")); string(data) != strconv.Itoa(pid)+"\n" {
		t.Fatalf("pid record = %q, want %d", data, pid)
	}
	record := s.remote(a, "agent.config")
	info, err := os.Lstat(record)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
		t.Errorf("configuration record mode = %v, want 0400", info.Mode())
	}
	if data, _ := os.ReadFile(record); string(data) != configRecord(a) {
		t.Errorf("configuration record = %q, want %q", data, configRecord(a))
	}
	for name := range snapshot(t, s, a) {
		if strings.HasPrefix(name, ".config-") {
			t.Errorf("temporary file %s remains", name)
		}
	}
}

func TestStartSameConfig(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		t.Run("stopped="+strconv.FormatBool(stopped), func(t *testing.T) {
			s := newExecServer(t)
			a := newAgent(t, sleeper)
			install(t, s, a)
			writeConfig(t, s, a, configRecord(a))
			record := s.remote(a, "agent.config")
			before, err := os.Lstat(record)
			if err != nil {
				t.Fatal(err)
			}
			pidfile := s.remote(a, "agent.pid")
			dead := ""
			if stopped {
				dead = exitedPID(t)
				if err := os.WriteFile(pidfile, []byte(dead+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			pid, err := Start(context.Background(), s.runner(), a, stopped)
			if err != nil {
				t.Fatal(err)
			}
			if data, _ := os.ReadFile(pidfile); string(data) != strconv.Itoa(pid)+"\n" {
				t.Fatalf("pid record = %q, want %d", data, pid)
			}
			if stopped {
				if data, _ := os.ReadFile(pidfile + ".stopped"); string(data) != dead+"\n" {
					t.Fatalf("stopped record = %q, want %s", data, dead)
				}
			}
			after, err := os.Lstat(record)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("configuration record was replaced: %v", err)
			}
			if data, _ := os.ReadFile(record); string(data) != configRecord(a) {
				t.Fatalf("configuration record = %q", data)
			}
		})
	}
}

func TestStartRefusesChangedConfig(t *testing.T) {
	changes := []struct {
		name   string
		change func(*Agent)
	}{
		{"port", func(a *Agent) { a.Port++ }},
		{"target", func(a *Agent) { a.Target = "other" }},
	}
	for _, c := range changes {
		for _, stopped := range []bool{false, true} {
			for _, restart := range []bool{false, true} {
				name := c.name + "/stopped=" + strconv.FormatBool(stopped) + "/restart=" + strconv.FormatBool(restart)
				t.Run(name, func(t *testing.T) {
					s := newExecServer(t)
					a := newAgent(t, sleeper)
					install(t, s, a)
					writeConfig(t, s, a, configRecord(a))
					if stopped {
						if err := os.WriteFile(s.remote(a, "agent.pid"), []byte(exitedPID(t)+"\n"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					before := snapshot(t, s, a)
					b := a
					c.change(&b)
					pid, err := Start(context.Background(), s.runner(), b, restart)
					if !errors.Is(err, fs.ErrExist) || pid != 0 {
						t.Fatalf("Start = %d, %v; want an error wrapping fs.ErrExist", pid, err)
					}
					if after := snapshot(t, s, a); !maps.Equal(before, after) {
						t.Fatalf("refused start changed the directory from\n%v\nto\n%v", before, after)
					}
				})
			}
		}
	}
}

func TestStartRefusesUnverifiableConfig(t *testing.T) {
	a := newAgent(t, sleeper)
	rec := configRecord(a)
	tests := []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"no final newline", strings.TrimSuffix(rec, "\n")},
		{"extra newline", rec + "\n"},
		{"trailing byte", rec + "x"},
		{"nul", strings.Replace(rec, "\n", "\x00\n", 1)},
		{"reordered", "target " + a.Target + "\nport " + strconv.Itoa(a.Port) + "\n"},
		{"leading zero", "port 0" + strconv.Itoa(a.Port) + "\ntarget " + a.Target + "\n"},
		{"invalid target", "port " + strconv.Itoa(a.Port) + "\ntarget DGX\n"},
		{"oversized", rec + strings.Repeat("#", 1<<20)},
		{"oversized other", "port 1024\ntarget " + a.Target + "\n" + strings.Repeat("#", 300)},
		{"symlink", ""},
		{"directory", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExecServer(t)
			install(t, s, a)
			record := s.remote(a, "agent.config")
			outside := filepath.Join(t.TempDir(), "outside")
			if err := os.WriteFile(outside, []byte(rec), 0o600); err != nil {
				t.Fatal(err)
			}
			var err error
			switch tt.name {
			case "symlink":
				// Even a link to the expected record is refused.
				err = os.Symlink(outside, record)
			case "directory":
				err = os.Mkdir(record, 0o700)
			default:
				err = os.WriteFile(record, []byte(tt.data), 0o400)
			}
			if err != nil {
				t.Fatal(err)
			}
			check := func(restart bool) {
				t.Helper()
				before := snapshot(t, s, a)
				pid, err := Start(context.Background(), s.runner(), a, restart)
				if err == nil || errors.Is(err, fs.ErrExist) || !strings.Contains(err.Error(), "unverifiable") || pid != 0 {
					t.Fatalf("Start(restart=%v) = %d, %v; want unverifiable configuration", restart, pid, err)
				}
				if after := snapshot(t, s, a); !maps.Equal(before, after) {
					t.Fatalf("refused start changed the directory from\n%v\nto\n%v", before, after)
				}
			}
			check(false)
			if err := os.WriteFile(s.remote(a, "agent.pid"), []byte(exitedPID(t)+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			check(true)
			if got, _ := os.ReadFile(outside); string(got) != rec {
				t.Fatalf("symlink target changed to %q", got)
			}
			if entries, _ := os.ReadDir(filepath.Dir(outside)); len(entries) != 1 {
				t.Fatalf("wrote outside the agent directory: %v", entries)
			}
		})
	}
}

// TestStartConfigRace makes another start create the configuration record
// between the check and the link, by running a start whose ln writes the
// racing record before linking.
func TestStartConfigRace(t *testing.T) {
	a := newAgent(t, sleeper)
	tests := []struct {
		name     string
		racer    string
		mismatch bool
	}{
		{"same", configRecord(a), false},
		{"different", "port 1024\ntarget " + a.Target + "\n", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newExecServer(t)
			install(t, s, a)
			// The agent inherits PATH, so the directory is in the
			// namespace, removed only after the agent is joined.
			bin := filepath.Join(s.home, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			racer := filepath.Join(bin, "racer")
			if err := os.WriteFile(racer, []byte(tt.racer), 0o600); err != nil {
				t.Fatal(err)
			}
			ln := "#!/bin/sh\n/bin/cp '" + racer + "' \"$2\"\nexec /bin/ln \"$@\"\n"
			if err := os.WriteFile(filepath.Join(bin, "ln"), []byte(ln), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
			pid, err := Start(context.Background(), s.runner(), a, false)
			if tt.mismatch {
				if !errors.Is(err, fs.ErrExist) {
					t.Fatalf("Start = %d, %v; want an error wrapping fs.ErrExist", pid, err)
				}
				if _, err := os.Lstat(s.remote(a, "agent.pid")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("a refused start recorded a process")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if data, _ := os.ReadFile(s.remote(a, "agent.config")); string(data) != tt.racer {
				t.Fatalf("configuration record = %q, want the racing record %q", data, tt.racer)
			}
			for name := range snapshot(t, s, a) {
				if strings.HasPrefix(name, ".config-") {
					t.Errorf("temporary file %s remains", name)
				}
			}
		})
	}
}

func TestStartRefusesChangedBinary(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, sleeper)
	install(t, s, a)
	bin := s.remote(a, "skygo-ssh")
	if err := os.Chmod(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho replaced\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(context.Background(), s.runner(), a, false); err == nil || !strings.Contains(err.Error(), "refusing to run it") {
		t.Fatalf("Start = %v, want refusal of a changed binary", err)
	}
	if _, err := os.Stat(s.remote(a, "agent.pid")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused start recorded a process")
	}
}

// TestStartReuse needs /proc to identify the running agent.
func TestStartReuse(t *testing.T) {
	if _, err := os.Readlink("/proc/self/exe"); err != nil {
		t.Skip("no /proc process identity")
	}
	// The agent binary is a copy of the shell, which runs ./agent from
	// the agent directory, so the process image is the agent binary.
	shell, err := os.ReadFile("/bin/sh")
	if err != nil {
		t.Fatal(err)
	}
	s := newExecServer(t)
	a := newAgent(t, shell)
	install(t, s, a)
	// Like sleeper, ./agent waits with builtin read for end of file on the
	// stop pipe, starting no other program, so the process image stays
	// the agent binary.
	if err := os.WriteFile(s.remote(a, "agent"), []byte("exec 3>&-\nread -r line <&4 || :\nexit 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	pid, err := Start(ctx, s.runner(), a, false)
	if err != nil {
		t.Fatal(err)
	}
	starts := s.starts()
	if len(starts) != 1 {
		t.Fatalf("Start started %d agents, want one", len(starts))
	}
	started := starts[0]
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, err := Status(ctx, s.runner(), a)
		if err != nil {
			t.Fatal(err)
		}
		if st.Agent == "running" && st.PID == pid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Status = %+v, want running %d", st, pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
	again, err := Start(ctx, s.runner(), a, false)
	if err != nil || again != pid {
		t.Fatalf("second Start = %d, %v; want reuse of %d", again, err, pid)
	}

	// The running agent cannot be reused with another port or target.
	pidfile := s.remote(a, "agent.pid")
	before := snapshot(t, s, a)
	other := a
	other.Port++
	renamed := a
	renamed.Target = "other"
	for _, b := range []Agent{other, renamed} {
		if got, err := Start(ctx, s.runner(), b, true); !errors.Is(err, fs.ErrExist) {
			t.Fatalf("Start with port %d, target %s = %d, %v; want an error wrapping fs.ErrExist", b.Port, b.Target, got, err)
		}
	}
	if after := snapshot(t, s, a); !maps.Equal(before, after) {
		t.Fatalf("refused start changed the directory from\n%v\nto\n%v", before, after)
	}

	// A running agent without a configuration record is refused, and no
	// record is made for it.
	record := s.remote(a, "agent.config")
	if err := os.Rename(record, record+".saved"); err != nil {
		t.Fatal(err)
	}
	if got, err := Start(ctx, s.runner(), a, true); err == nil || !strings.Contains(err.Error(), "no configuration record") {
		t.Fatalf("Start without a configuration record = %d, %v; want refusal", got, err)
	}
	if _, err := os.Lstat(record); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a refused start recorded a configuration")
	}
	if data, _ := os.ReadFile(pidfile); string(data) != strconv.Itoa(pid)+"\n" {
		t.Fatalf("pid record = %q, want %d", data, pid)
	}
	if err := os.Rename(record+".saved", record); err != nil {
		t.Fatal(err)
	}
	if again, err := Start(ctx, s.runner(), a, false); err != nil || again != pid {
		t.Fatalf("Start with the record restored = %d, %v; want reuse of %d", again, err, pid)
	}
	// Reuse started nothing, and the shell that started the agent is
	// still waiting for it.
	if n := len(s.starts()); n != 1 {
		t.Fatalf("starts and reuses started %d agents, want one", n)
	}
	if joined(started) {
		t.Fatal("the shell that started the agent exited while the agent ran")
	}

	// Released, the agent exits and its shell, having waited for it,
	// exits too; from then on the agent is stopped.
	s.release()
	waitJoined(t, started)
	if started.err != nil {
		t.Fatalf("original start command: %v", started.err)
	}
	st, err := Status(ctx, s.runner(), a)
	if err != nil || st.Agent != "stopped" || st.PID != pid {
		t.Fatalf("Status = %+v, %v after release; want stopped %d", st, err, pid)
	}
}
