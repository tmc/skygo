package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"google.golang.org/protobuf/proto"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// TestRetryWorkload uses files owned by one script to expose actual process
// execution. Neither the script nor this helper writes the daemon's database.
func TestRetryWorkload(t *testing.T) {
	mode := os.Getenv("SKYGO_RETRY_WORKLOAD")
	if mode == "" {
		return
	}
	dir := os.Getenv("SKYGO_RETRY_DIR")
	first, err := os.OpenFile(filepath.Join(dir, "first"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		first.Close()
		fmt.Println("attempt one")
		if mode == "hold" {
			// A script that owns custody receives this process's own pidfd,
			// so it can prove this exit after the daemon is gone.
			if _, err := os.Stat(filepath.Join(dir, retryCustodyDir)); err == nil {
				if err := sendRetryCustody(dir); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(99)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(99)
			}
		}
		if mode == "hold" || mode == "follow" {
			// Release markers synchronize product behavior. This independent
			// bound contains a helper whose release failed; it is never
			// evidence that the helper exited.
			deadline := time.Now().Add(2 * time.Minute)
			for {
				if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
					break
				} else if !errors.Is(err, os.ErrNotExist) {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(99)
				}
				if time.Now().After(deadline) {
					fmt.Fprintln(os.Stderr, "retry workload release marker timed out")
					os.Exit(99)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
		os.Exit(7)
	}
	if !errors.Is(err, os.ErrExist) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	second, err := os.OpenFile(filepath.Join(dir, "second"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(99)
	}
	second.Close()
	fmt.Println("attempt two")
	os.Exit(0)
}

// TestRetryScripts runs the production CLI and daemon. Linux is currently the
// only qualified local retry backend; validation and old-server tests below
// remain enabled on other platforms.
func TestRetryScripts(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("local retry requires qualified Linux nonreaping wait")
	}
	exe := retryScriptBinary(t)
	for _, name := range []string{"success", "follow", "cancel_backoff", "restart_canceled", "restart_unknown", "lost_response", "scoped", "original_request"} {
		t.Run(name, func(t *testing.T) { runRetryScript(t, exe, exe, "testdata/retry/"+name+".txt") })
	}
}

func TestRetryValidationScripts(t *testing.T) {
	exe := retryScriptBinary(t)
	runRetryScript(t, exe, exe, "testdata/retry/validation.txt")
}

// TestRetryOldServerScripts requires a source-archived S04 binary. Its SHA is
// provided by the caller and checked before execution; no old daemon is rebuilt
// from the current protobuf or used against a new-format database.
func TestRetryOldServerScripts(t *testing.T) {
	old, want := os.Getenv("SKYGO_S04_BINARY"), os.Getenv("SKYGO_S04_BINARY_SHA256")
	if old == "" || want == "" {
		t.Skip("set SKYGO_S04_BINARY and SKYGO_S04_BINARY_SHA256 to the archived S04 daemon")
	}
	old, err := filepath.Abs(old)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(old)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("S04 binary SHA256 = %s; want %s", got, want)
	}
	t.Logf("archived S04 binary SHA256 %s", want)
	exe := retryScriptBinary(t)
	runRetryScript(t, exe, old, "testdata/retry/old_server.txt")
}

func retryScriptBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs production CLI and daemon")
	}
	// A parent-qualified cross-compiled executable lets the same scripts run
	// on a host without a Go installation. It is an explicit test fixture,
	// never selected from PATH or silently substituted for a local build.
	fixture, want := os.Getenv("SKYGO_RETRY_BINARY"), os.Getenv("SKYGO_RETRY_BINARY_SHA256")
	if fixture != "" || want != "" {
		if fixture == "" || want == "" {
			t.Fatal("set both SKYGO_RETRY_BINARY and SKYGO_RETRY_BINARY_SHA256")
		}
		var err error
		fixture, err = filepath.Abs(fixture)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Fatalf("retry CLI fixture SHA256 = %s; want %s", got, want)
		}
		t.Logf("provided retry CLI fixture SHA256 %s (coverage depends on fixture build)", want)
		return fixture
	}
	exe := filepath.Join(t.TempDir(), "skygo")
	args := []string{"build", "-o", exe}
	if mode := testing.CoverMode(); mode != "" {
		args = append(args, "-cover", "-covermode="+mode, "-coverpkg=github.com/tmc/skygo/...")
	}
	cmd := exec.Command("go", append(args, "../../cmd/skygo")...)
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build retry CLI: %v\n%s", err, out)
	}
	return exe
}

func runRetryScript(t *testing.T, exe, daemon, file string) {
	t.Helper()
	var env []string
	if dir := coverDir(t); dir != "" {
		env = append(env, "GOCOVERDIR="+dir)
	}
	address := freeAddress(t)
	// The server's data directory is not a TempDir: it is removed only after
	// the server's original Wait result, and kept while workload custody is
	// held. This cleanup runs after custody settles.
	data, err := os.MkdirTemp("", "skygo-server-")
	if err != nil {
		t.Fatal(err)
	}
	process := &serverProcess{exe: daemon, address: address, data: data, ownData: true, env: env}
	var custody *retryCustody
	t.Cleanup(func() {
		if err := finishRetryServer(process, custody); err != nil {
			t.Errorf("server: %v", err)
		}
	})
	// Registered before any script runs: if the script fails after starting
	// a held workload, this still releases that same workload and requires
	// its pidfd to report exit. Without that proof the owner stays retained.
	custody = newRetryCustody(os.TempDir(), defaultRetryCustodyOps())
	t.Cleanup(func() {
		if err := custody.settle(30 * time.Second); err != nil {
			t.Errorf("retry workload custody: %v", err)
		}
	})
	authFile := filepath.Join(t.TempDir(), "auth.json")
	privateDir := func(label, dir string) error {
		info, err := os.Stat(dir)
		if err != nil {
			return err
		}
		t.Logf("scoped %s parent mode before: %04o", label, info.Mode().Perm())
		if err := os.Chmod(dir, 0700); err != nil {
			return err
		}
		info, err = os.Stat(dir)
		if err != nil {
			return err
		}
		t.Logf("scoped %s parent mode after: %04o", label, info.Mode().Perm())
		if !info.IsDir() || info.Mode().Perm() != 0700 {
			return fmt.Errorf("scoped %s parent is not private", label)
		}
		return nil
	}
	if filepath.Base(file) == "scoped.txt" {
		// Go's numbered TempDir children use 0777 before the process umask.
		// Authentication fixtures require private parents even with umask 0002.
		for label, dir := range map[string]string{"data": process.data, "auth": filepath.Dir(authFile)} {
			if err := privateDir(label, dir); err != nil {
				t.Fatal(err)
			}
		}
		process.args = []string{"--auth-file", authFile}
	}
	target := &url.URL{Scheme: "http", Host: process.address}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	calls := make(map[string]int)
	var submitted []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls[r.URL.Path]++
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/SubmitRetryJob") {
			data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, "read submission", 500)
				return
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(data))
			mu.Lock()
			submitted = append([]byte(nil), data...)
			mu.Unlock()
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	fault := &faultProxy{target: target, drop: make(map[string]int)}
	dropServer := httptest.NewServer(fault)
	t.Cleanup(dropServer.Close)
	e := executableScriptEngine(exe, nil, env)
	bg := &background{}
	e.Cmds["skygo"] = bg.command(exe, env)
	e.Cmds["await-stdout"] = bg.awaitStdout()
	e.Cmds["server"] = process.command()
	if filepath.Base(file) == "scoped.txt" {
		e.Cmds["private-workdir"] = script.Command(script.CmdUsage{Summary: "record and secure this script's token-output directory"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
			if len(args) != 0 {
				return nil, script.ErrUsage
			}
			return nil, privateDir("work", s.Getwd())
		})
	}
	e.Cmds["job-id"] = jobID()
	e.Cmds["await-file"] = awaitFile()
	e.Cmds["drop-response"] = fault.command()
	e.Cmds["retry-task"] = retryTaskfile(t)
	e.Cmds["custody"] = custody.command()
	e.Cmds["await-state"] = retryAwaitState(exe, env)
	e.Cmds["history"] = retryHistory()
	e.Cmds["original-request"] = script.Command(script.CmdUsage{Summary: "check original task bytes forwarded through the retry method"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 0 {
			return nil, script.ErrUsage
		}
		mu.Lock()
		data := append([]byte(nil), submitted...)
		mu.Unlock()
		var body skyv1.SubmitJobRequest
		if err := proto.Unmarshal(data, &body); err != nil {
			return nil, err
		}
		task := body.GetTask()
		policy := task.GetRetry()
		codes := policy.GetExitCodes()
		if task.GetTarget() != "" || policy.GetDelaySeconds() != 0 || len(codes) != 3 || codes[0] != 7 || codes[1] != 3 || codes[2] != 7 {
			return nil, errors.New("CLI rewrote original retry task")
		}
		return nil, nil
	})
	e.Cmds["total-calls"] = script.Command(script.CmdUsage{Summary: "check all forwarded daemon calls", Args: "count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := 0
		for _, n := range calls {
			got += n
		}
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("total forwarded calls = %d; want %d", got, want)
		}
		return nil, nil
	})
	e.Cmds["calls"] = script.Command(script.CmdUsage{Summary: "check actual daemon RPC calls", Args: "method count"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		want, err := strconv.Atoi(args[1])
		if err != nil {
			return nil, err
		}
		mu.Lock()
		got := calls["/skygo.v1.ControlService/"+args[0]]
		mu.Unlock()
		if got != want {
			return nil, fmt.Errorf("%s calls = %d; want %d", args[0], got, want)
		}
		return nil, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, e, []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=coverage-token", "PROXY=" + dropServer.URL, "DATA=" + process.data, "AUTH=" + authFile}, file)
}

const (
	retryCustodyDir    = "custody"
	retryCustodyNonce  = "nonce"
	retryCustodyAddr   = "addr"
	retryCustodyProbe  = "probe"
	retryCustodySocket = "sock"
	retryCustodyAck    = "ok\n"

	// retryCustodyPathMax is the longest socket path Linux accepts:
	// sockaddr_un.sun_path holds 108 bytes including the terminating NUL.
	retryCustodyPathMax = 107
	// retryCustodyHeldMax bounds the owners retained by this test process.
	retryCustodyHeldMax = 8
)

func retryCustodyPath(dir, name string) string { return filepath.Join(dir, retryCustodyDir, name) }

// retryCustodySocketPath returns the longest socket path that MkdirTemp(tmp,
// "custody-") can produce, or an error if it could exceed the Linux limit.
// It depends only on tmp, never on the test name or workdir.
func retryCustodySocketPath(tmp string) (string, error) {
	if !filepath.IsAbs(tmp) {
		return "", fmt.Errorf("custody TMPDIR %q is not absolute", tmp)
	}
	// MkdirTemp appends a decimal uint32, at most 10 digits.
	longest := filepath.Join(tmp, "custody-4294967295", retryCustodySocket)
	if len(longest) > retryCustodyPathMax {
		return "", fmt.Errorf("custody socket path can reach %d bytes under TMPDIR %q; Linux allows %d", len(longest), tmp, retryCustodyPathMax)
	}
	return longest, nil
}

// retryCustodyOps are the operations a custody owner performs on its
// descriptors. Tests replace them on one owner only, to model failures; no
// global is changed.
type retryCustodyOps struct {
	receive       func(conn *net.UnixConn, nonce []byte) (fd, pid int, err error)
	awaitExit     func(fd int, timeout time.Duration) error
	closeFD       func(fd int) error
	closeListener func(ln *net.UnixListener) error
}

func defaultRetryCustodyOps() retryCustodyOps {
	return retryCustodyOps{
		receive:       receiveRetryCustody,
		awaitExit:     awaitRetryExit,
		closeFD:       closeRetryPidfd,
		closeListener: (*net.UnixListener).Close,
	}
}

// closeOnce records the single Close attempt of one descriptor. An attempted
// Close with an error is uncertain: the descriptor number may already name
// something else, so it is never used or closed again.
type closeOnce struct {
	attempted bool
	err       error
}

func (c *closeOnce) close(f func() error) error {
	if !c.attempted {
		c.attempted = true
		c.err = f()
	}
	return c.err
}

func (c *closeOnce) done() bool { return c.attempted && c.err == nil }

// retryCustodyHeld is the strong root of every custody owner from admission
// until its positive release. An owner whose release is pending or uncertain
// stays here, with its descriptors, causes and both namespaces, for the rest
// of the test process. Admission precedes any resource; when the table is
// full, listen refuses instead of dropping an owner.
var retryCustodyHeld struct {
	mu     sync.Mutex
	owners []*retryCustody
}

func (c *retryCustody) admit() error {
	retryCustodyHeld.mu.Lock()
	defer retryCustodyHeld.mu.Unlock()
	if len(retryCustodyHeld.owners) >= retryCustodyHeldMax {
		return fmt.Errorf("%d retained custody owners; refusing another", len(retryCustodyHeld.owners))
	}
	retryCustodyHeld.owners = append(retryCustodyHeld.owners, c)
	return nil
}

func (c *retryCustody) retire() {
	retryCustodyHeld.mu.Lock()
	defer retryCustodyHeld.mu.Unlock()
	if i := slices.Index(retryCustodyHeld.owners, c); i >= 0 {
		retryCustodyHeld.owners = slices.Delete(retryCustodyHeld.owners, i, i+1)
	}
}

func retryCustodyRetained(c *retryCustody) bool {
	retryCustodyHeld.mu.Lock()
	defer retryCustodyHeld.mu.Unlock()
	return slices.Contains(retryCustodyHeld.owners, c)
}

// retryCustody owns one held retry workload across an abrupt daemon death.
// It has two namespaces: the guarded custody directory in the script workdir,
// which also holds the workload's markers, and a shallow private socket
// directory under TMPDIR, so the socket path does not depend on the test
// name. The workload hands over its own pidfd on that socket.
//
// Only kernel exit of that pidfd followed by a positive Close of the pidfd,
// the listener and the accepted connection, and a join of the accept
// goroutine, releases the guard and removes either directory. Anything less
// keeps the owner retained, with both namespaces. Elapsed time never counts
// as exit.
type retryCustody struct {
	ops     retryCustodyOps
	tmp     string // parent of the socket directory
	dir     string // the script workdir, which is also the workload's
	sockDir string // the shallow socket directory

	admitted bool
	ln       *net.UnixListener
	accepted chan struct{} // closed when the single accept attempt returns
	lnClose  closeOnce
	joined   bool

	// Written by the accept goroutine before accepted is closed.
	fd, pid   int           // the received pidfd and its process
	err       error         // the handoff's result
	conn      *net.UnixConn // the accepted connection, kept after its Close
	connClose closeOnce     // that connection's single Close
	exited    bool          // the pidfd reported exit
	fdClose   closeOnce
	released  bool // both namespaces were removed after every positive Close
}

func newRetryCustody(tmp string, ops retryCustodyOps) *retryCustody {
	return &retryCustody{ops: ops, tmp: tmp, fd: -1}
}

func (c *retryCustody) command() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "take, check or release custody of the held workload", Args: "listen|acquire|release"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 1 {
			return nil, script.ErrUsage
		}
		switch args[0] {
		case "listen":
			return nil, c.listen(s.Getwd())
		case "acquire":
			return nil, c.acquire(10 * time.Second)
		case "release":
			return nil, c.release(10 * time.Second)
		}
		return nil, script.ErrUsage
	})
}

// listen checks the socket path and admits the owner before any effect. It
// then creates the custody directory with the nonce and socket address, the
// shallow socket directory and listener, accepts one handoff in the
// background, and guards the custody directory against removal.
func (c *retryCustody) listen(dir string) error {
	if c.admitted {
		return errors.New("custody already listening")
	}
	if _, err := retryCustodySocketPath(c.tmp); err != nil {
		return err
	}
	if err := c.admit(); err != nil {
		return err
	}
	c.admitted, c.dir = true, dir
	nonce, err := c.prepare()
	if err != nil {
		// Nothing is listening and no workload knows the namespace.
		c.admitted = false
		c.retire()
		return err
	}
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(c.sockDir, retryCustodySocket), Net: "unix"})
	if err != nil {
		c.admitted = false
		c.retire()
		return errors.Join(err, os.Remove(c.sockDir))
	}
	// Closing the listener would otherwise unlink the socket before its
	// Close returns; only unguard removes it, after every positive Close.
	ln.SetUnlinkOnClose(false)
	// From here the owner stays retained until a positive release.
	c.ln, c.accepted = ln, make(chan struct{})
	go func() {
		defer close(c.accepted)
		conn, err := ln.AcceptUnix()
		if err != nil {
			c.err = err
			return
		}
		c.conn = conn
		err = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if err == nil {
			c.fd, c.pid, err = c.ops.receive(conn, nonce)
		}
		if err == nil {
			_, err = conn.Write([]byte(retryCustodyAck))
		}
		c.err = err
		c.connClose.close(conn.Close)
	}()
	return c.guard()
}

// prepare writes the fixture-owned nonce and socket address, then creates the
// socket directory, and returns the nonce. Both directories are private.
func (c *retryCustody) prepare() ([]byte, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	nonce := []byte(hex.EncodeToString(b[:]))
	if err := os.Mkdir(filepath.Join(c.dir, retryCustodyDir), 0700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(retryCustodyPath(c.dir, retryCustodyNonce), nonce, 0600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(retryCustodyPath(c.dir, retryCustodyProbe), nil, 0600); err != nil {
		return nil, err
	}
	sockDir, err := os.MkdirTemp(c.tmp, "custody-")
	if err != nil {
		return nil, err
	}
	c.sockDir = sockDir
	addr := filepath.Join(sockDir, retryCustodySocket)
	if len(addr) > retryCustodyPathMax {
		return nil, errors.Join(fmt.Errorf("custody socket path has %d bytes", len(addr)), os.Remove(sockDir))
	}
	if err := os.WriteFile(retryCustodyPath(c.dir, retryCustodyAddr), []byte(addr), 0600); err != nil {
		return nil, errors.Join(err, os.Remove(sockDir))
	}
	return nonce, nil
}

// guard makes the custody directory unwritable and proves it: removing the
// probe must be refused. A process that can bypass directory permissions
// gets no guard, so the workload is never started.
func (c *retryCustody) guard() error {
	cdir := filepath.Join(c.dir, retryCustodyDir)
	if err := os.Chmod(cdir, 0500); err != nil {
		return err
	}
	err := os.Remove(retryCustodyPath(c.dir, retryCustodyProbe))
	if err == nil {
		return errors.New("custody guard ineffective: probe removed from a 0500 directory")
	}
	if !errors.Is(err, fs.ErrPermission) {
		return fmt.Errorf("custody guard probe: %w", err)
	}
	return nil
}

// acquire waits for the handoff, so the test holds the workload's own pidfd
// before the daemon is killed.
func (c *retryCustody) acquire(timeout time.Duration) error {
	if c.ln == nil {
		return errors.New("custody not listening")
	}
	select {
	case <-c.accepted:
	case <-time.After(timeout):
		return fmt.Errorf("no workload custody handoff within %v", timeout)
	}
	return c.err
}

// settled reports whether no workload custody remains: none was taken, or it
// was released after every positive proof.
func (c *retryCustody) settled() bool { return !c.admitted || c.released }

// finishRetryServer finishes the retry harness's server, keeping its data
// directory while workload custody is unsettled.
func finishRetryServer(p *serverProcess, custody *retryCustody) error {
	return p.finish(custody != nil && !custody.settled())
}

func (c *retryCustody) handedOver() bool {
	if c.ln == nil {
		return false
	}
	select {
	case <-c.accepted:
		return c.err == nil && c.fd >= 0
	default:
		return false
	}
}

// release writes the workload's release marker and requires the same pidfd
// to report exit, then finishes custody.
func (c *retryCustody) release(timeout time.Duration) error {
	if !c.handedOver() {
		return fmt.Errorf("workload custody not acquired: %v", c.err)
	}
	if err := c.writeRelease(); err != nil {
		return err
	}
	return c.finish(timeout)
}

// finish proves exit on the original pidfd, closes it, the listener and the
// accepted connection, joins the accept goroutine, and only then removes the
// guard and both directories. Each Close is attempted once. Any failure
// returns with the owner retained and nothing removed.
func (c *retryCustody) finish(timeout time.Duration) error {
	if c.released {
		return nil
	}
	if !c.exited {
		if err := c.ops.awaitExit(c.fd, timeout); err != nil {
			return fmt.Errorf("process %d: %w; pidfd %d retained", c.pid, err, c.fd)
		}
		c.exited = true
	}
	if err := c.fdClose.close(func() error { return c.ops.closeFD(c.fd) }); err != nil {
		return fmt.Errorf("close pidfd %d: %w; close uncertain, guard retained", c.fd, err)
	}
	if err := c.lnClose.close(func() error { return c.ops.closeListener(c.ln) }); err != nil {
		return fmt.Errorf("close custody listener: %w; close uncertain, guard retained", err)
	}
	if !c.joined {
		select {
		case <-c.accepted:
			c.joined = true
		case <-time.After(timeout):
			return errors.New("custody accept goroutine not joined; guard retained")
		}
	}
	if c.conn != nil && !c.connClose.done() {
		return fmt.Errorf("close custody connection: %w; close uncertain, guard retained", c.connClose.err)
	}
	return c.unguard()
}

func (c *retryCustody) writeRelease() error {
	f, err := os.OpenFile(filepath.Join(c.dir, "release"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return f.Close()
}

// unguard removes both namespaces. Callers have proven exit and every Close.
func (c *retryCustody) unguard() error {
	cdir := filepath.Join(c.dir, retryCustodyDir)
	if err := os.Chmod(cdir, 0700); err != nil {
		return err
	}
	for _, name := range []string{retryCustodyNonce, retryCustodyAddr, retryCustodyProbe} {
		if err := os.Remove(retryCustodyPath(c.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Remove(filepath.Join(c.sockDir, retryCustodySocket)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := errors.Join(os.Remove(cdir), os.Remove(c.sockDir)); err != nil {
		return err
	}
	c.released = true
	c.retire()
	return nil
}

// settle runs after the script's subtest. If the script stopped before
// release, that subtest's TempDir removal could not delete the guarded
// directory; settle releases the same workload and finishes custody, then
// removes what the TempDir removal left. Without positive proof it returns
// the cause and the owner stays retained.
func (c *retryCustody) settle(timeout time.Duration) error {
	if !c.admitted || c.released {
		return nil
	}
	if c.ln == nil {
		return fmt.Errorf("custody in %s has no listener; retained", c.dir)
	}
	if !c.handedOver() {
		err := c.writeRelease()
		return fmt.Errorf("held workload in %s was never handed over (%v); exit unproven, owner retained (release: %v)", c.dir, c.err, err)
	}
	if err := c.writeRelease(); err != nil {
		return fmt.Errorf("release held workload in %s: %w; owner retained", c.dir, err)
	}
	if err := c.finish(timeout); err != nil {
		return fmt.Errorf("held workload in %s: %w", c.dir, err)
	}
	if _, err := os.Stat(c.dir); err == nil {
		return errors.Join(os.RemoveAll(c.dir), os.Remove(filepath.Dir(c.dir)))
	}
	return nil
}

func retryTaskfile(t *testing.T) script.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cover := coverDir(t)
	return script.Command(script.CmdUsage{Summary: "write task using the real retry workload helper", Args: "file mode delay"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 3 {
			return nil, script.ErrUsage
		}
		delay, err := strconv.Atoi(args[2])
		if err != nil {
			return nil, err
		}
		env := map[string]string{"SKYGO_RETRY_WORKLOAD": args[1], "SKYGO_RETRY_DIR": s.Getwd()}
		if cover != "" {
			env["GOCOVERDIR"] = cover
		}
		task := map[string]any{"argv": []string{exe, "-test.run=^TestRetryWorkload$"}, "env": env}
		if args[1] != "default" {
			codes := []int{7}
			if args[1] == "original" {
				codes = []int{7, 3, 7}
			}
			task["retry"] = map[string]any{"max_attempts": 2, "delay_seconds": delay, "exit_codes": codes}
		}
		data, err := json.Marshal(task)
		if err != nil {
			return nil, err
		}
		return nil, os.WriteFile(s.Path(args[0]), data, 0600)
	})
}

type retryScriptJob struct {
	ID       string `json:"id"`
	State    string `json:"state"`
	Attempts []struct {
		ID       string `json:"id"`
		Ordinal  uint32 `json:"ordinal"`
		State    string `json:"state"`
		ExitCode *int32 `json:"exit_code"`
	} `json:"attempts"`
	NextAttemptAt string `json:"next_attempt_at"`
}

// Await-state polls the public CLI; it never reads or manufactures durable rows.
func retryAwaitState(exe string, env []string) script.Cmd {
	return script.Command(script.CmdUsage{Summary: "wait for an actual CLI job state", Args: "job state"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		if len(args) != 2 {
			return nil, script.ErrUsage
		}
		ctx, cancel := context.WithTimeout(s.Context(), 15*time.Second)
		defer cancel()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		// The last successfully decoded state survives a probe the deadline killed.
		last := "none observed"
		for {
			cmd := exec.CommandContext(ctx, exe, "--json", "jobs", "get", args[0])
			cmd.Env = append(s.Environ(), env...)
			out, err := cmd.Output()
			if err != nil {
				if ctx.Err() != nil {
					return nil, fmt.Errorf("await %s: last state %s: probe %v: %w", args[1], last, err, context.Cause(ctx))
				}
				return nil, err
			}
			var j retryScriptJob
			if err := json.Unmarshal(out, &j); err != nil {
				return nil, err
			}
			last = j.State
			if j.State == "JOB_STATE_"+strings.ToUpper(args[1]) {
				return func(*script.State) (string, string, error) { return string(out), "", nil }, nil
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("await %s: last state %s: %w", args[1], last, context.Cause(ctx))
			case <-tick.C:
			}
		}
	})
}

func retryHistory() script.Cmd {
	return script.Command(script.CmdUsage{Summary: "check bounded immutable attempt history from previous JSON", Args: "state..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		var j retryScriptJob
		if err := json.Unmarshal([]byte(s.Stdout()), &j); err != nil {
			return nil, err
		}
		if len(j.Attempts) != len(args) {
			return nil, fmt.Errorf("history has %d attempts; want %d", len(j.Attempts), len(args))
		}
		seen := make(map[string]bool)
		for i, a := range j.Attempts {
			if a.ID == "" || (i == 0 && a.ID != j.ID) || (i > 0 && a.ID == j.ID) || seen[a.ID] || a.Ordinal != uint32(i+1) || a.State != "JOB_STATE_"+strings.ToUpper(args[i]) {
				return nil, fmt.Errorf("attempt %d has invalid identity/ordinal/state: %+v", i+1, a)
			}
			seen[a.ID] = true
			if args[i] == "failed" && (a.ExitCode == nil || *a.ExitCode != 7) {
				return nil, fmt.Errorf("failed attempt %d lacks exit 7", i+1)
			}
			if args[i] == "succeeded" && (a.ExitCode == nil || *a.ExitCode != 0) {
				return nil, fmt.Errorf("successful attempt %d lacks exit 0", i+1)
			}
		}
		if j.State != "JOB_STATE_RETRYING" && j.NextAttemptAt != "" {
			return nil, errors.New("non-retrying job retains retry deadline")
		}
		if j.State == "JOB_STATE_RETRYING" && j.NextAttemptAt == "" {
			return nil, errors.New("retrying job lacks retry deadline")
		}
		return nil, nil
	})
}

// The custody policy tests below model every descriptor operation of one
// owner. The pidfd is a fixture number that never reaches the kernel, and
// each failure is an injected error, not a kernel Close failure. The
// listener, the handoff connection and both directories are real.
const (
	modeledCustodyFD  = 1 << 20
	modeledCustodyPID = 4242
)

type modeledCustody struct {
	awaitErr, fdErr, lnErr error
	awaits, fdCloses       []int
	lnCloses               int
	sockAfterClose         error // Lstat of the socket after the real listener Close
}

func (m *modeledCustody) ops() retryCustodyOps {
	return retryCustodyOps{
		receive: func(conn *net.UnixConn, nonce []byte) (int, int, error) {
			buf := make([]byte, len(nonce)+1)
			n, err := io.ReadAtLeast(conn, buf, len(nonce))
			if err != nil {
				return -1, 0, err
			}
			if !bytes.Equal(buf[:n], nonce) {
				return -1, 0, errors.New("custody nonce mismatch")
			}
			return modeledCustodyFD, modeledCustodyPID, nil
		},
		awaitExit: func(fd int, _ time.Duration) error {
			m.awaits = append(m.awaits, fd)
			return m.awaitErr
		},
		closeFD: func(fd int) error {
			m.fdCloses = append(m.fdCloses, fd)
			return m.fdErr
		},
		closeListener: func(ln *net.UnixListener) error {
			m.lnCloses++
			if m.lnErr != nil {
				return m.lnErr
			}
			err := ln.Close()
			_, m.sockAfterClose = os.Lstat(ln.Addr().String())
			return err
		},
	}
}

// startModeledCustody listens for work under a fresh socket parent, hands
// over through the real socket as the workload does, and acquires. A
// retained owner is discarded when the test ends: no real descriptor was
// handed over, so only the test's own listener and directories remain.
func startModeledCustody(t *testing.T, m *modeledCustody, work string) *retryCustody {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("custody relies on Unix directory permissions")
	}
	tmp, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	c := newRetryCustody(tmp, m.ops())
	t.Cleanup(func() {
		if !c.released && c.ln != nil {
			if !c.lnClose.done() {
				c.ln.Close()
			}
			<-c.accepted
			c.retire()
			cdir := filepath.Join(c.dir, retryCustodyDir)
			if err := errors.Join(os.Chmod(cdir, 0700), os.RemoveAll(cdir), os.RemoveAll(c.sockDir)); err != nil {
				t.Errorf("discard modeled custody: %v", err)
			}
		}
		if err := os.Remove(tmp); err != nil {
			t.Errorf("remove socket parent: %v", err)
		}
	})
	if err := c.listen(work); err != nil {
		t.Fatal(err)
	}
	nonce, err := os.ReadFile(retryCustodyPath(work, retryCustodyNonce))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := os.ReadFile(retryCustodyPath(work, retryCustodyAddr))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: string(addr), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(nonce); err != nil {
		t.Fatal(err)
	}
	ack := make([]byte, len(retryCustodyAck))
	if _, err := io.ReadFull(conn, ack); err != nil || string(ack) != retryCustodyAck {
		t.Fatalf("custody acknowledgement %q: %v", ack, err)
	}
	if err := c.acquire(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	return c
}

// checkRetained requires the owner, its original pidfd number, accepted
// connection and both namespaces to remain, with the guard still unwritable.
func checkRetained(t *testing.T, c *retryCustody) {
	t.Helper()
	if !retryCustodyRetained(c) || c.released {
		t.Error("owner was not retained")
	}
	if c.fd != modeledCustodyFD || c.pid != modeledCustodyPID {
		t.Errorf("frame changed: fd %d pid %d", c.fd, c.pid)
	}
	if c.conn == nil {
		t.Error("accepted connection not kept by the owner")
	}
	info, err := os.Stat(filepath.Join(c.dir, retryCustodyDir))
	if err != nil || info.Mode().Perm() != 0500 {
		t.Errorf("guard: %v %v; want mode 0500", info, err)
	}
	for _, name := range []string{retryCustodyPath(c.dir, retryCustodyNonce), retryCustodyPath(c.dir, retryCustodyAddr), filepath.Join(c.sockDir, retryCustodySocket)} {
		if _, err := os.Lstat(name); err != nil {
			t.Errorf("namespace entry removed: %v", err)
		}
	}
}

func TestRetryCustodyPidfdCloseFailure(t *testing.T) {
	m := &modeledCustody{fdErr: errors.New("modeled pidfd close failure")}
	c := startModeledCustody(t, m, t.TempDir())
	for i, err := range []error{c.release(time.Second), c.release(time.Second), c.settle(time.Second)} {
		if !errors.Is(err, m.fdErr) {
			t.Errorf("attempt %d: %v; want the cached close cause", i, err)
		}
	}
	if len(m.awaits) != 1 || len(m.fdCloses) != 1 || m.fdCloses[0] != modeledCustodyFD {
		t.Errorf("awaits %v pidfd closes %v; want one of each on the original pidfd", m.awaits, m.fdCloses)
	}
	if m.lnCloses != 0 {
		t.Errorf("listener closed %d times after an uncertain pidfd close", m.lnCloses)
	}
	if !c.fdClose.attempted || !errors.Is(c.fdClose.err, m.fdErr) {
		t.Errorf("close state %+v", c.fdClose)
	}
	checkRetained(t, c)
}

func TestRetryCustodyListenerCloseFailure(t *testing.T) {
	m := &modeledCustody{lnErr: errors.New("modeled listener close failure")}
	c := startModeledCustody(t, m, t.TempDir())
	for i, err := range []error{c.release(time.Second), c.settle(time.Second)} {
		if !errors.Is(err, m.lnErr) {
			t.Errorf("attempt %d: %v; want the cached close cause", i, err)
		}
	}
	if len(m.fdCloses) != 1 || m.lnCloses != 1 {
		t.Errorf("pidfd closes %v listener closes %d; want one each", m.fdCloses, m.lnCloses)
	}
	checkRetained(t, c)
}

func TestRetryCustodyExitTimeout(t *testing.T) {
	m := &modeledCustody{awaitErr: errors.New("modeled exit not observed")}
	c := startModeledCustody(t, m, t.TempDir())
	for i, err := range []error{c.release(time.Second), c.settle(time.Second)} {
		if !errors.Is(err, m.awaitErr) {
			t.Errorf("attempt %d: %v; want the exit failure", i, err)
		}
	}
	if len(m.awaits) != 2 || m.awaits[0] != modeledCustodyFD || m.awaits[1] != modeledCustodyFD {
		t.Errorf("awaits %v; want two on the original pidfd", m.awaits)
	}
	if len(m.fdCloses) != 0 || m.lnCloses != 0 || c.exited {
		t.Errorf("pidfd closes %v listener closes %d exited %v without exit", m.fdCloses, m.lnCloses, c.exited)
	}
	checkRetained(t, c)
}

// TestRetryCustodyRelease is the positive control: the same setup without a
// fault releases everything once.
func TestRetryCustodyRelease(t *testing.T) {
	m := &modeledCustody{}
	c := startModeledCustody(t, m, t.TempDir())
	if err := c.release(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.settle(time.Second); err != nil {
		t.Fatal(err)
	}
	if len(m.awaits) != 1 || len(m.fdCloses) != 1 || m.fdCloses[0] != modeledCustodyFD || m.lnCloses != 1 {
		t.Errorf("awaits %v pidfd closes %v listener closes %d; want one each", m.awaits, m.fdCloses, m.lnCloses)
	}
	if m.sockAfterClose != nil {
		t.Errorf("socket after listener Close: %v; only unguard may remove it", m.sockAfterClose)
	}
	if c.conn == nil || !c.connClose.done() {
		t.Errorf("accepted connection %v close %+v; want the original kept and closed once", c.conn, c.connClose)
	}
	if retryCustodyRetained(c) || !c.released {
		t.Error("owner retained after positive release")
	}
	for _, name := range []string{filepath.Join(c.dir, retryCustodyDir), c.sockDir} {
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s remains: %v", name, err)
		}
	}
}

// TestRetryCustodyLongWorkdir shows the socket path depends on TMPDIR only:
// a workdir too deep to hold a socket still releases through its own fresh
// shallow socket directory.
func TestRetryCustodyLongWorkdir(t *testing.T) {
	work := filepath.Join(t.TempDir(), strings.Repeat("w", 60), strings.Repeat("x", 60))
	if err := os.MkdirAll(work, 0700); err != nil {
		t.Fatal(err)
	}
	if n := len(retryCustodyPath(work, retryCustodySocket)); n <= retryCustodyPathMax {
		t.Fatalf("workdir socket path has only %d bytes", n)
	}
	m := &modeledCustody{}
	c := startModeledCustody(t, m, work)
	addr, err := os.ReadFile(retryCustodyPath(work, retryCustodyAddr))
	if err != nil {
		t.Fatal(err)
	}
	if string(addr) != filepath.Join(c.sockDir, retryCustodySocket) || filepath.Dir(c.sockDir) != c.tmp || strings.HasPrefix(c.sockDir, work) || len(addr) > retryCustodyPathMax {
		t.Errorf("socket %s in %s; want a fresh directory directly under %s", addr, c.sockDir, c.tmp)
	}
	if err := c.release(time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestRetryCustodySocketPath(t *testing.T) {
	for _, tt := range []struct {
		tmp  string
		want int // 0 refuses
	}{
		{"/home/tmc/skygo-test/s07-3ef8f8c3/t", 59}, // root's task TMPDIR
		{"/" + strings.Repeat("a", 82), 107},
		{"/" + strings.Repeat("a", 83), 0},
		{"t", 0},
	} {
		path, err := retryCustodySocketPath(tt.tmp)
		if tt.want == 0 {
			if err == nil {
				t.Errorf("%q: accepted %s", tt.tmp, path)
			}
			continue
		}
		if err != nil || len(path) != tt.want {
			t.Errorf("%q: %d bytes, %v; want %d", tt.tmp, len(path), err, tt.want)
		}
	}
}

// TestRetryCustodyRefusals checks that an overlong TMPDIR and a full owner
// table are refused before any effect.
func TestRetryCustodyRefusals(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("custody relies on Unix directory permissions")
	}
	long := filepath.Join(t.TempDir(), strings.Repeat("d", 100))
	m := &modeledCustody{}
	work := t.TempDir()
	c := newRetryCustody(long, m.ops())
	if err := c.listen(work); err == nil {
		t.Fatal("overlong TMPDIR accepted")
	}
	if retryCustodyRetained(c) || c.admitted {
		t.Error("refused owner admitted")
	}
	for _, name := range []string{long, filepath.Join(work, retryCustodyDir)} {
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s created: %v", name, err)
		}
	}

	var fill []*retryCustody
	defer func() {
		for _, f := range fill {
			f.retire()
		}
	}()
	for {
		f := &retryCustody{}
		if f.admit() != nil {
			break
		}
		fill = append(fill, f)
	}
	tmp, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp)
	c = newRetryCustody(tmp, m.ops())
	if err := c.listen(work); err == nil {
		t.Fatal("full owner table accepted another owner")
	}
	if _, err := os.Lstat(filepath.Join(work, retryCustodyDir)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("custody created while the table was full: %v", err)
	}
	for _, f := range fill {
		if !retryCustodyRetained(f) {
			t.Error("a retained owner was dropped")
		}
	}
}

// TestServerProcessRetryCustodyData requires the retry harness to keep its
// server's data directory while workload custody is unsettled. No server
// process is started; the test removes what it made.
func TestServerProcessRetryCustodyData(t *testing.T) {
	for _, tt := range []struct {
		name    string
		custody *retryCustody
		keep    bool
	}{
		{"none", nil, false},
		{"unused", &retryCustody{}, false},
		{"held", &retryCustody{admitted: true}, true},
		{"released", &retryCustody{admitted: true, released: true}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.MkdirTemp("", "skygo-server-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { os.RemoveAll(data) })
			err = finishRetryServer(&serverProcess{data: data, ownData: true}, tt.custody)
			_, statErr := os.Stat(data)
			if kept := statErr == nil; kept != tt.keep || errors.Is(err, errServerDataKept) != tt.keep {
				t.Errorf("finish: %v, data directory: %v; want kept %v", err, statErr, tt.keep)
			}
		})
	}
}
