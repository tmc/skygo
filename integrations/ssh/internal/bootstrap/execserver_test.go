package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// The tests in this file check the execServer fixture itself: that the
// script keeps its logical result behind fixturePrelude, and that the
// execServer owns what the script starts until it releases and joins it.

func TestFixtureText(t *testing.T) {
	if err := fixtureShape(); err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"prelude": fixturePrelude,
		"script":  fixtureEnter + script + fixtureLeave,
	} {
		if out, err := exec.Command("/bin/sh", "-n", "-c", text).CombinedOutput(); err != nil {
			t.Errorf("sh -n %s: %v\n%s", name, err, out)
		}
	}
}

// exitStatus returns the remote exit status carried by err, or -1.
func exitStatus(err error) int {
	var exit *ssh.ExitError
	if errors.As(err, &exit) {
		return exit.ExitStatus()
	}
	return -1
}

// TestExecServerRunsScript checks that commands keep their exit status,
// output, error and input, that the script receives its operands, and
// that the record holds the command as sent.
func TestExecServerRunsScript(t *testing.T) {
	s := newExecServer(t)
	var binary []byte
	for i := range 1024 {
		binary = append(binary, byte(i))
	}
	a := newAgent(t, binary)
	ctx := context.Background()
	r := s.runner()

	tests := []struct {
		name   string
		cmd    string
		stdin  string
		status int
		stdout string
		stderr string
	}{
		{"mode", mustCommand(t, "bogus", a.Commit, a.Digest), "", 1, "", "skygo-bootstrap: invalid mode\n"},
		{"commit", mustCommand(t, "status", "abc", a.Digest), "", 1, "", "skygo-bootstrap: invalid commit\n"},
		// A command that is not the script runs in the login shell
		// itself, which reports nothing on the result pipe.
		{"shell", "cat; printf err >&2; exit 3", "in\x00put", 3, "in\x00put", "err"},
		{"other text", "sh -c 'echo replaced' skygo-bootstrap 'status'", "", 125, "", "execServer: refusing an unexpected sh invocation\n"},
		{"other name", "sh -c " + quote(script) + " other 'status'", "", 125, "", "execServer: refusing an unexpected sh invocation\n"},
	}
	for _, tt := range tests {
		var stdin io.Reader
		if tt.stdin != "" {
			stdin = strings.NewReader(tt.stdin)
		}
		stdout, stderr, err := r.Run(ctx, tt.cmd, stdin)
		if exitStatus(err) != tt.status || string(stdout) != tt.stdout || string(stderr) != tt.stderr {
			t.Errorf("%s: Run = %q, %q, %v; want %q, %q, status %d", tt.name, stdout, stderr, err, tt.stdout, tt.stderr, tt.status)
		}
		recs := s.records()
		last := recs[len(recs)-1]
		if last.command != tt.cmd || string(last.stdin) != tt.stdin {
			t.Errorf("%s: record = %q with input %q", tt.name, last.command, last.stdin)
		}
		waitJoined(t, last.launch)
	}

	// Binary input reaches the script whole, and the script ends without
	// exiting, so fixtureLeave reports.
	sum, err := fileSum(a.Local["skygo-ssh"])
	if err != nil {
		t.Fatal(err)
	}
	if err := put(ctx, r, a, "skygo-ssh", sum, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(s.remote(a, "skygo-ssh")); !bytes.Equal(got, binary) {
		t.Fatal("stored binary differs")
	}
	recs := s.records()
	last := recs[len(recs)-1]
	if !bytes.Equal(last.stdin, binary) || string(last.stdout) != "stored skygo-ssh\n" {
		t.Fatalf("put record = input %d bytes, output %q", len(last.stdin), last.stdout)
	}
	st, err := Status(ctx, r, a)
	if err != nil || st.Agent != "none" || st.Files["skygo-ssh"] != sum {
		t.Fatalf("Status = %+v, %v", st, err)
	}
	for _, rec := range s.records() {
		waitJoined(t, rec.launch)
	}
	if n := len(s.starts()); n != 0 {
		t.Fatalf("started %d agents", n)
	}
	if d := s.diagnostics(); len(d) != 0 {
		t.Fatalf("fixture diagnostics = %q", d)
	}
}

func mustCommand(t *testing.T, operands ...string) string {
	t.Helper()
	cmd, err := command(operands...)
	if err != nil {
		t.Fatal(err)
	}
	return cmd
}

// TestExecServerHoldsAgent starts an agent with the real script, through
// setsid and nohup where the host has setsid, and checks that Start
// returns while the shell that started the agent still waits for it, that
// the agent runs on the stop pipe it inherited through them, and that
// release alone ends it and then its shell.
func TestExecServerHoldsAgent(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, sleeper)
	install(t, s, a)
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
	if joined(started) {
		t.Fatal("the shell that started the agent exited before the agent was released")
	}

	// The sleeper is a script, so the script's state cannot match its
	// process image to the binary; it reports a live process as unknown.
	st, err := Status(ctx, s.runner(), a)
	if err != nil || st.Agent != "unknown" || st.PID != pid {
		t.Fatalf("Status = %+v, %v; want unknown %d", st, err, pid)
	}
	recs := s.records()
	waitJoined(t, recs[len(recs)-1].launch)
	if joined(started) {
		t.Fatal("the shell that started the agent exited before the agent was released")
	}

	s.release()
	waitJoined(t, started)
	if started.err != nil {
		t.Fatalf("original start command: %v", started.err)
	}
	st, err = Status(ctx, s.runner(), a)
	if err != nil || st.Agent != "stopped" || st.PID != pid {
		t.Fatalf("Status = %+v, %v after release; want stopped %d", st, err, pid)
	}
}

// TestRetireHoldsOriginal checks that the namespace outlives a held agent
// and the shell that started it, and is removed once both are joined.
func TestRetireHoldsOriginal(t *testing.T) {
	s := newExecServer(t)
	a := newAgent(t, sleeper)
	install(t, s, a)
	if _, err := Start(context.Background(), s.runner(), a, false); err != nil {
		t.Fatal(err)
	}
	started := s.starts()[0]
	err := s.retire(time.Second)
	if err == nil || !strings.Contains(err.Error(), "original command") {
		t.Fatalf("retire with the agent held = %v, want a hold on the original command", err)
	}
	if _, err := os.Stat(s.remote(a, "agent.pid")); err != nil {
		t.Fatalf("retire removed the namespace of a held agent: %v", err)
	}
	if joined(started) {
		t.Fatal("the shell that started the agent exited before the agent was released")
	}

	s.release()
	if err := s.retire(holdBound); err != nil {
		t.Fatal(err)
	}
	if !joined(started) || started.err != nil {
		t.Fatalf("retired without joining the original start command: %v", started.err)
	}
	if _, err := os.Stat(s.home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retire kept the namespace: %v", err)
	}
}

// TestRetireHoldsUnknownJoin checks that retire removes nothing while an
// original command's join is unknown. The launch is a TEST-FIXTURE MODEL:
// it has no process, and its done channel stands for a cmd.Wait that has
// not returned.
func TestRetireHoldsUnknownJoin(t *testing.T) {
	s := newExecServer(t)
	model := &launch{command: "model", done: make(chan struct{})}
	s.mu.Lock()
	s.launches = append(s.launches, model)
	s.mu.Unlock()
	if err := s.retire(time.Second); err == nil {
		t.Fatal("retire succeeded with an unjoined original command")
	}
	if _, err := os.Stat(s.home); err != nil {
		t.Fatalf("retire removed the namespace: %v", err)
	}
	close(model.done)
	if err := s.retire(holdBound); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retire kept the namespace: %v", err)
	}
}

// TestSettleKeepsLateDiagnostics checks that settle reports a HOLD with
// the diagnostics recorded before it, that a diagnostic a fixture
// goroutine records after the HOLD is kept with the held server and not
// reported to the cleanup that has returned, and that it is reported once
// the server retires. The launch is a TEST-FIXTURE MODEL, as in
// TestRetireHoldsUnknownJoin, and report stands for the test's Errorf.
func TestSettleKeepsLateDiagnostics(t *testing.T) {
	s := newExecServer(t)
	model := &launch{command: "model", done: make(chan struct{})}
	s.mu.Lock()
	s.launches = append(s.launches, model)
	s.mu.Unlock()
	s.diag("early")

	var reports []string
	report := func(format string, args ...any) {
		reports = append(reports, fmt.Sprintf(format, args...))
	}
	if err := s.settle(time.Second, report); err == nil {
		t.Fatal("settled with an unjoined original command")
	}
	if len(reports) != 2 || reports[0] != "early" || !strings.HasPrefix(reports[1], "HOLD: ") {
		t.Fatalf("reports at the HOLD = %q, want early and a HOLD", reports)
	}
	if !isHeld(s) {
		t.Fatal("the HOLD did not keep the server")
	}
	if _, err := os.Stat(s.home); err != nil {
		t.Fatalf("the HOLD removed the namespace: %v", err)
	}

	// A fixture goroutine that outlives the HOLD records into the server.
	recorded := make(chan struct{})
	go func() {
		defer close(recorded)
		s.diag("late")
	}()
	<-recorded
	if len(reports) != 2 {
		t.Fatalf("a diagnostic after the HOLD was reported: %q", reports[2:])
	}
	if d := s.diagnostics(); !slices.Equal(d, []string{"early", "late"}) {
		t.Fatalf("held diagnostics = %q, want early and late", d)
	}

	close(model.done)
	reports = nil
	if err := s.settle(holdBound, report); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reports, []string{"late"}) {
		t.Fatalf("reports at retirement = %q, want late alone", reports)
	}
	if isHeld(s) {
		t.Fatal("a retired server is still held")
	}
	if _, err := os.Stat(s.home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("settle kept the namespace: %v", err)
	}
}

func isHeld(s *execServer) bool {
	held.Lock()
	defer held.Unlock()
	return slices.Contains(held.servers, s)
}
