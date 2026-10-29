//go:build unix

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/gcp"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// NOT_RUN by author.

// TestScript runs testdata/*.txt. The skygo-gce command runs run in
// process. Every case either fails before credentials or holds on an
// expired token before any request, so no case reaches the network.
func TestScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo-gce"] = script.Command(script.CmdUsage{Summary: "run skygo-gce in process", Args: "args..."}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		for i, a := range args {
			if a == "-config" || a == "-state" || a == "-credentials" {
				if i+1 < len(args) {
					args[i+1] = s.Path(args[i+1])
				}
			}
		}
		var out, diag bytes.Buffer
		code := newCommand().run(s.Context(), args, &out, &diag) // every case ends with no step running
		var err error
		if code != 0 {
			err = fmt.Errorf("exit status %d", code)
		}
		return func(*script.State) (string, string, error) {
			fmt.Fprintf(&out, "exit=%d\n", code)
			return out.String(), diag.String(), err
		}, nil
	})
	scripttest.Test(t, ctx, e, nil, "testdata/*.txt")
}

func TestParseFlags(t *testing.T) {
	tests := []struct {
		cmd  string
		args []string
		ok   bool
	}{
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t"}, true},
		{"start", []string{"-config", "c", "-state", "s"}, false},
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t", "-steps", "0"}, false},
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t", "-interval", "0s"}, false},
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t", "-timeout", "3h"}, false},
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t", "extra"}, false},
		{"start", []string{"-config", "c", "-state", "s", "-credentials", "t", "-refresh"}, false},
		{"status", []string{"-config", "c", "-state", "s"}, true},
		{"status", []string{"-config", "c", "-state", "s", "-credentials", "t"}, false},
		{"status", []string{"-config", "c", "-state", "s", "-refresh"}, false},
		{"status", []string{"-config", "c", "-state", "s", "-refresh", "-credentials", "t"}, true},
	}
	for _, tt := range tests {
		_, err := parseFlags(tt.cmd, tt.args, new(bytes.Buffer))
		if (err == nil) != tt.ok {
			t.Errorf("parseFlags(%s, %q) = %v, want ok=%v", tt.cmd, tt.args, err, tt.ok)
		}
	}
	if _, err := parseFlags("start", []string{"-h"}, new(bytes.Buffer)); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("help = %v", err)
	}
}

// escape returns the JSON \uXXXX escape of r.
func escape(r rune) string {
	return fmt.Sprintf("%cu%04x", '\\', r)
}

func TestReadToken(t *testing.T) {
	const good = `{"access_token":"ya29.secret","token_type":"Bearer","expiry":"2099-01-01T00:00:00Z"}`
	tests := []struct {
		name string
		in   string
		ok   bool
	}{
		{"valid", good, true},
		{"one escaped key", strings.Replace(good, `"expiry"`, `"expir`+escape('y')+`"`, 1), true},
		{"duplicate token", strings.Replace(good, `{`, `{"access_token":"ya29.other",`, 1), false},
		{"escaped duplicate token", strings.Replace(good, `{`, `{"access_`+escape('t')+`oken":"ya29.other",`, 1), false},
		{"duplicate type", strings.Replace(good, `{`, `{"token_type":"MAC",`, 1), false},
		{"escaped duplicate type", good[:len(good)-1] + `,"token_typ` + escape('e') + `":"MAC"}`, false},
		{"duplicate expiry", good[:len(good)-1] + `,"expiry":"2000-01-01T00:00:00Z"}`, false},
		{"escaped duplicate expiry", good[:len(good)-1] + `,"` + escape('e') + `xpiry":"2000-01-01T00:00:00Z"}`, false},
		{"case alias", strings.Replace(good, `"access_token"`, `"Access_Token"`, 1), false},
		{"refresh token", good[:len(good)-1] + `,"refresh_token":"r"}`, false},
		{"non-string value", strings.Replace(good, `"Bearer"`, `["Bearer"]`, 1), false},
		{"trailing", good + `{}`, false},
		{"not an object", `["ya29.secret"]`, false},
		{"missing expiry", `{"access_token":"ya29.secret","token_type":"Bearer"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "token.json")
			if err := os.WriteFile(name, []byte(tt.in), 0o600); err != nil {
				t.Fatal(err)
			}
			tok, err := readToken(name)
			if (err == nil) != tt.ok {
				t.Fatalf("readToken = %v, want ok=%v", err, tt.ok)
			}
			if err != nil && strings.Contains(err.Error(), "ya29") {
				t.Errorf("error carries the token: %v", err)
			}
			if tt.ok && tok.AccessToken != "ya29.secret" {
				t.Errorf("token = %q", tok.AccessToken)
			}
		})
	}
}

// An input file whose first Close fails is not used: the error keeps the
// file and its original cause, and run stops before any state exists.
func TestInputCloseFailure(t *testing.T) {
	errClose := errors.New("injected input close failure")
	t.Cleanup(func() { closeInput = (*os.File).Close })
	closeInput = func(f *os.File) error {
		f.Close()
		return errClose
	}
	dir := t.TempDir()
	name := filepath.Join(dir, "token.json")
	os.WriteFile(name, []byte(`{"access_token":"a","token_type":"Bearer","expiry":"2099-01-01T00:00:00Z"}`), 0o600)
	_, err := readPrivate(name)
	var ce *inputCloseError
	if !errors.Is(err, errClose) || !errors.As(err, &ce) || ce.f == nil {
		t.Fatalf("readPrivate = %v; want the close failure keeping its file", err)
	}
	var stderr bytes.Buffer
	c := newCommand()
	code := c.run(context.Background(), []string{"start", "-config", name, "-state", filepath.Join(dir, "st"), "-credentials", name}, new(bytes.Buffer), &stderr)
	if code != 1 || !strings.Contains(stderr.String(), errClose.Error()) {
		t.Errorf("run = %d, %s", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "st")); err == nil {
		t.Error("state created after a failed input close")
	}
	var kept *inputCloseError
	if len(c.errs) != 1 || !errors.As(c.errs[0], &kept) || kept.f == nil {
		t.Errorf("the command does not keep the input error: %v", c.errs)
	}
}

const cmdConfig = `{"project":"skygo-test","compute_project_id":"123456789012","zone":"us-central1-a","machine_type":"zones/us-central1-a/machineTypes/t2a-standard-1","subnet":"projects/skygo-net/regions/us-central1/subnetworks/default","image":{"project":"debian-cloud","name":"debian-12-arm64-v20261001","id":"4242"},"disk_type":"zones/us-central1-a/diskTypes/pd-balanced","disk_gib":20,"ssh_user":"skygo","authorized_key":"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl"}`

// stepClock is a command clock under the test's control: the test sets
// now, every observation waits on cleanup, which the test closes to pass
// the cleanup deadline, and poll fires at once. No real time is waited.
type stepClock struct {
	mu        sync.Mutex
	t         time.Time
	cleanup   chan time.Time
	once      sync.Once
	deadlines []time.Time // every deadline a timer was set for
}

func (k *stepClock) install(c *command) {
	c.now = func() time.Time {
		k.mu.Lock()
		defer k.mu.Unlock()
		return k.t
	}
	c.timer = func(d time.Time) <-chan time.Time {
		k.mu.Lock()
		defer k.mu.Unlock()
		k.deadlines = append(k.deadlines, d)
		return k.cleanup
	}
	c.poll = func() <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}
		return ch
	}
}

func (k *stepClock) advance(d time.Duration) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.t = k.t.Add(d)
}

// expire passes the cleanup deadline.
func (k *stepClock) expire() { k.once.Do(func() { close(k.cleanup) }) }

// A cmdFixture runs one command with the real steps on a real state
// directory, except that inputs come from memory and the token has
// expired, so no step sends a request. Each step is logged, an overlap of
// two steps is counted, and the step named held waits for release.
type cmdFixture struct {
	c       *command
	k       *stepClock
	dir     string
	held    string
	entered chan struct{}
	hold    chan struct{}
	once    sync.Once
	ran     chan struct{}
	code    int
	stderr  bytes.Buffer
	mu      sync.Mutex
	log     []string
	running atomic.Int32
	overlap atomic.Int32
}

// retained keeps every fixture whose Machine did not close, with its
// directory, until the test process exits.
var (
	retainedMu sync.Mutex
	retained   []*cmdFixture
)

// newCmdFixture returns a fixture holding the step named held. Its
// cleanup, registered before anything can fail, releases the step, waits
// for run and for any step still running, then closes the actual Machine,
// and removes the directory only if that Close succeeds.
func newCmdFixture(t *testing.T, held string) *cmdFixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "skygo-gce-test-")
	if err != nil {
		t.Fatal(err)
	}
	f := &cmdFixture{c: newCommand(), dir: dir, held: held, entered: make(chan struct{}, 1), hold: make(chan struct{})}
	f.k = &stepClock{t: time.Now(), cleanup: make(chan time.Time)}
	f.k.install(f.c)
	f.c.step = f.stepsFor()
	t.Cleanup(func() {
		f.release()
		if f.ran != nil {
			<-f.ran
		}
		if j := f.c.active; j != nil {
			<-j.done
		}
		if m := f.c.m; m != nil {
			if err := m.Close(); err != nil {
				t.Logf("kept %s: machine not closed: %v", dir, err)
				retainedMu.Lock()
				retained = append(retained, f)
				retainedMu.Unlock()
				return
			}
		}
		os.RemoveAll(dir)
	})
	return f
}

func (f *cmdFixture) release() { f.once.Do(func() { close(f.hold) }) }

func (f *cmdFixture) record(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, s)
}

func (f *cmdFixture) logged() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

// enter logs the start of step what and, if it is the held step, waits
// for release. The returned func logs its end.
func (f *cmdFixture) enter(what string) func() {
	if f.running.Add(1) > 1 {
		f.overlap.Add(1)
	}
	f.record(what)
	if what == f.held {
		f.entered <- struct{}{}
		<-f.hold
	}
	return func() {
		f.record("end " + what)
		f.running.Add(-1)
	}
}

func (f *cmdFixture) stepsFor() steps {
	s := realSteps
	s.readConfig = func(string) (gce.Config, error) {
		defer f.enter("read config")()
		return gce.ParseConfig(strings.NewReader(cmdConfig))
	}
	s.readToken = func(string) (oauthToken, error) {
		defer f.enter("read credentials")()
		return oauthToken{AccessToken: "ya29.expired", TokenType: "Bearer", Expiry: time.Now().Add(-time.Hour)}, nil
	}
	s.create = func(dir string, cfg gce.Config) (*gce.Machine, error) {
		defer f.enter("create state")()
		return realSteps.create(dir, cfg)
	}
	s.open = func(dir string, cfg gce.Config) (*gce.Machine, error) {
		defer f.enter("open state")()
		return realSteps.open(dir, cfg)
	}
	s.setGoal = func(m *gce.Machine, g gce.Goal) error {
		defer f.enter("set goal")()
		return realSteps.setGoal(m, g)
	}
	s.drive = func(m *gce.Machine, ctx context.Context, tok oauthToken, o gce.DriveOptions) (gce.Status, error) {
		defer f.enter("drive")()
		return realSteps.drive(m, ctx, tok, o)
	}
	s.refresh = func(m *gce.Machine, ctx context.Context, tok oauthToken) (gce.Status, error) {
		defer f.enter("refresh")()
		return realSteps.refresh(m, ctx, tok)
	}
	s.close = func(m *gce.Machine) error {
		defer f.enter("close")()
		return realSteps.close(m)
	}
	return s
}

// start runs cmd in its own goroutine, as main would.
func (f *cmdFixture) start(cmd string) {
	args := []string{cmd, "-config", "config.json", "-state", filepath.Join(f.dir, "state"), "-credentials", "token.json"}
	if cmd == "status" {
		args = append(args, "-refresh")
	}
	f.ran = make(chan struct{})
	go func() {
		defer close(f.ran)
		f.code = f.c.run(context.Background(), args, new(bytes.Buffer), &f.stderr)
	}()
}

func index(log []string, s string) int {
	for i, x := range log {
		if x == s {
			return i
		}
	}
	return -1
}

func count(log []string, s string) int {
	n := 0
	for _, x := range log {
		if x == s {
			n++
		}
	}
	return n
}

// Each step of a command is held past the operation deadline. If it
// returns before the cleanup deadline, nothing more starts but Close, and
// Close starts only after it returned. If the cleanup deadline passes
// first, the command reports a final HOLD, never starts Close, and keeps
// the step, its Machine and lock; the step's later return changes
// nothing. No two steps ever overlap.
func TestCommandHeldStep(t *testing.T) {
	cases := []struct {
		cmd, step string
		lateCode  int // exit status when the step returns late
	}{
		{"start", "read config", 1},
		{"start", "read credentials", 1},
		{"start", "create state", 1},
		{"start", "set goal", 1},
		{"start", "drive", 3}, // drive's own result: the token expired
		{"start", "close", 3},
		{"status", "refresh", 0},
	}
	for _, tt := range cases {
		for _, late := range []bool{true, false} {
			name := tt.cmd + "/" + tt.step + "/expired"
			if late {
				name = tt.cmd + "/" + tt.step + "/late"
			}
			t.Run(name, func(t *testing.T) {
				if tt.cmd == "status" {
					pre := newCmdFixture(t, "")
					pre.start("start")
					<-pre.ran
					f := newCmdFixture(t, tt.step)
					os.Remove(f.dir) // empty: f uses pre's state, which pre's cleanup removes after f's
					f.dir = pre.dir
					runHeld(t, f, tt.cmd, tt.step, late, tt.lateCode)
					return
				}
				runHeld(t, newCmdFixture(t, tt.step), tt.cmd, tt.step, late, tt.lateCode)
			})
		}
	}
}

func runHeld(t *testing.T, f *cmdFixture, cmd, step string, late bool, lateCode int) {
	t.Helper()
	f.start(cmd)
	<-f.entered
	f.k.advance(11 * time.Minute) // past the 10m operation deadline, before cleanup
	if late {
		f.release()
		<-f.ran
		log := f.logged()
		if f.code != lateCode || f.c.held != nil || strings.Contains(f.stderr.String(), "HOLD") {
			t.Fatalf("code %d, %s", f.code, f.stderr.String())
		}
		if f.c.m != nil && step != "close" {
			if i, j := index(log, "end "+step), index(log, "close"); j < 0 || j < i {
				t.Errorf("close not after the step returned: %v", log)
			}
		}
	} else {
		f.k.expire()
		<-f.ran
		if f.code != 1 || f.c.held == nil || !strings.Contains(f.stderr.String(), "HOLD") {
			t.Fatalf("code %d, %s", f.code, f.stderr.String())
		}
		j := f.c.active
		if j == nil || j.what != step {
			t.Fatalf("the command does not keep the running step: %+v", j)
		}
		held := f.c.held
		f.release()
		<-j.done // the late return
		if f.code != 1 || f.c.finish() != held {
			t.Error("a late return changed the HOLD")
		}
		log := f.logged()
		if want := 0; step == "close" {
			if count(log, "close") != 1 {
				t.Errorf("close ran again after the HOLD: %v", log)
			}
		} else if count(log, "close") != want {
			t.Errorf("close started after a HOLD: %v", log)
		}
	}
	if n := f.overlap.Load(); n != 0 {
		t.Errorf("%d overlapping steps", n)
	}
}

// Both deadlines are fixed when the command begins: every observation
// waits for the one cleanup deadline, 90s after the operation deadline.
func TestCommandDeadlinesFixed(t *testing.T) {
	f := newCmdFixture(t, "")
	start := f.k.t
	f.start("start")
	<-f.ran
	c := f.c
	if !c.expiry.Equal(start.Add(10*time.Minute)) || !c.cleanup.Equal(c.expiry.Add(90*time.Second)) {
		t.Errorf("expiry %v, cleanup %v", c.expiry, c.cleanup)
	}
	for _, d := range f.k.deadlines {
		if !d.Equal(c.cleanup) {
			t.Errorf("observed deadline %v, want only %v", d, c.cleanup)
		}
	}
	if f.code != 3 || f.c.active != nil || index(f.logged(), "end close") < 0 {
		t.Errorf("code %d, %v", f.code, f.logged())
	}
	if closeBudget != 90*time.Second {
		t.Errorf("closeBudget = %v, documented as 90s", closeBudget)
	}
}

// A Close that fails is called again, one at a time, until it succeeds;
// one that never succeeds ends with the machine still owned and no
// successful close reported.
func TestCommandCloseRetries(t *testing.T) {
	errPendingClose := errors.New("an original request is still pending")
	t.Run("settles", func(t *testing.T) {
		f := newCmdFixture(t, "")
		n := 0
		f.c.step.close = func(m *gce.Machine) error {
			defer f.enter("close")()
			if n++; n < 3 {
				return errPendingClose
			}
			return realSteps.close(m)
		}
		f.start("start")
		<-f.ran
		if f.code != 3 || count(f.logged(), "close") != 3 || f.overlap.Load() != 0 {
			t.Errorf("code %d, %v", f.code, f.logged())
		}
	})
	t.Run("never", func(t *testing.T) {
		f := newCmdFixture(t, "")
		n := 0
		f.c.step.close = func(m *gce.Machine) error {
			defer f.enter("close")()
			if n++; n == 3 {
				f.k.expire()
			}
			return errPendingClose
		}
		f.start("start")
		<-f.ran
		out := f.stderr.String()
		if f.code != 1 || !strings.Contains(out, "HOLD") && !strings.Contains(out, "not closed") {
			t.Errorf("code %d, %s", f.code, out)
		}
		if f.overlap.Load() != 0 {
			t.Error("overlapping closes")
		}
	})
}

// A constructor error is kept by the command, with whatever file or lock
// it keeps, after run returns.
func TestCommandKeepsConstructorError(t *testing.T) {
	f := newCmdFixture(t, "")
	errCtor := errors.New("injected constructor failure")
	f.c.step.create = func(string, gce.Config) (*gce.Machine, error) {
		defer f.enter("create state")()
		return nil, errCtor
	}
	f.start("start")
	<-f.ran
	if f.code != 1 || len(f.c.errs) != 1 || f.c.errs[0] != errCtor || index(f.logged(), "set goal") >= 0 {
		t.Errorf("code %d, errs %v, steps %v", f.code, f.c.errs, f.logged())
	}
}
