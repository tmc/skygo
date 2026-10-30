package sshagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/cli"
	corecontrol "github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

type commandRoundTrip func(*http.Request) (*http.Response, error)

func (f commandRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func commandCall(t *testing.T, next http.RoundTripper) (*ledger, *call, *commandTransport) {
	t.Helper()
	l := newLedger(nil)
	c, err := l.admit(false)
	if err != nil {
		t.Fatal(err)
	}
	c.command = true
	return l, c, &commandTransport{call: c, token: "session-token", recorder: recorder{l: l, next: next}}
}
func commandRequest(t *testing.T) *http.Request {
	t.Helper()
	r, err := http.NewRequest("GET", testEndpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func commandCount(c *call) int {
	c.l.mu.Lock()
	defer c.l.mu.Unlock()
	return len(c.bodies) + len(c.roundTrips) + c.reserved
}

func TestCommandClosedBodyReclamation(t *testing.T) {
	var reads, closes, dispatches atomic.Int32
	next := commandRoundTrip(func(req *http.Request) (*http.Response, error) {
		dispatches.Add(1)
		if req.Header.Get("Authorization") != "Bearer session-token" || len(req.Header.Values("Authorization")) != 1 || len(req.Header["authorization"]) != 0 {
			t.Error("caller authorization not replaced")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &commandBodyCounter{reads: &reads, closes: &closes}}, nil
	})
	l, c, tr := commandCall(t, next)
	for i := 0; i < 10000; i++ {
		req := commandRequest(t)
		req.Header.Add("Authorization", "Bearer caller-file-token")
		req.Header.Add("Authorization", "Bearer other")
		req.Header["authorization"] = []string{"Bearer lower-case"}
		resp, err := tr.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		b := resp.Body.(*body)
		if _, err := io.ReadAll(b); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if commandCount(c) != 0 || !b.retired || b.rc != nil {
			t.Fatal("clean body retained")
		}
		before := reads.Load()
		if _, err := b.Read(make([]byte, 1)); err == nil {
			t.Fatal("late Read accepted")
		}
		if reads.Load() != before {
			t.Fatal("late underlying Read")
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if dispatches.Load() != 10000 || closes.Load() != 10000 {
		t.Fatal("duplicated or missing effects")
	}
	if _, err := c.settle(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.close(); err != nil {
		t.Fatal(err)
	}
	before := dispatches.Load()
	if _, err := tr.RoundTrip(commandRequest(t)); err == nil || dispatches.Load() != before {
		t.Fatal("post-command dispatch")
	}
}

type commandBodyCounter struct{ reads, closes *atomic.Int32 }

func (b *commandBodyCounter) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (b *commandBodyCounter) Close() error             { b.closes.Add(1); return nil }

func TestCommandRecordLimit(t *testing.T) {
	for _, frontier := range []int{62, 63} {
		t.Run(fmt.Sprint(frontier), func(t *testing.T) {
			var dispatch atomic.Int32
			l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) {
				dispatch.Add(1)
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
			}))
			for i := 0; i < frontier; i++ {
				if _, err := tr.RoundTrip(commandRequest(t)); err != nil {
					t.Fatal(err)
				}
			}
			_, err := tr.RoundTrip(commandRequest(t))
			if frontier == 62 {
				if err != nil || dispatch.Load() != 63 || commandCount(c) != 63 {
					t.Fatal("62+2 must admit atomically")
				}
				_, err = tr.RoundTrip(commandRequest(t))
			}
			if err == nil || dispatch.Load() != 63 {
				t.Fatal("63+2 must refuse before dispatch")
			}
			if !l.faulted || !c.commandRefused {
				t.Fatal("first local refusal did not fence")
			}
			if _, err := c.settle(nil, nil); err == nil {
				t.Fatal("settlement erased refusal fence")
			}
			if err := l.close(); err == nil {
				t.Fatal("refused command reported clean")
			}
		})
	}
}

func TestCommandReservedLimit(t *testing.T) {
	gate := make(chan struct{})
	entered := make(chan struct{}, 32)
	var once sync.Once
	var wg sync.WaitGroup
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { release(); wg.Wait() })
	next := commandRoundTrip(func(*http.Request) (*http.Response, error) {
		entered <- struct{}{}
		<-gate
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	l, c, tr := commandCall(t, next)
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		req := commandRequest(t)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := tr.RoundTrip(req)
			if err == nil {
				err = r.Body.Close()
			}
			errs <- err
		}()
	}
	for i := 0; i < 32; i++ {
		<-entered
	}
	if commandCount(c) != 64 {
		t.Fatal("reserved frontier")
	}
	if _, err := tr.RoundTrip(commandRequest(t)); err == nil {
		t.Fatal("overcap dispatch")
	}
	release()
	wg.Wait()
	for i := 0; i < 32; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if commandCount(c) != 0 {
		t.Fatal("reservation leak")
	}
	if _, err := c.settle(nil, nil); err == nil {
		t.Fatal(err)
	}
	if err := l.close(); err == nil {
		t.Fatal(err)
	}
}

func TestCommandPoisonAndFirstClose(t *testing.T) {
	raw := errors.New("modeled raw private transport/read cause")
	for _, tc := range []struct {
		name            string
		read, close, rt error
	}{
		{"read", raw, nil, nil}, {"joinedEOF", errors.Join(io.EOF, raw), nil, nil}, {"close", nil, raw, nil}, {"transport", nil, nil, raw},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newModelBody(nil)
			b.readErr = tc.read
			b.closeErr = tc.close
			var sends atomic.Int32
			next := commandRoundTrip(func(*http.Request) (*http.Response, error) {
				sends.Add(1)
				return &http.Response{StatusCode: 200, Body: b}, tc.rt
			})
			signer := &modelSigner{}
			l, c, tr := commandCall(t, next)
			l.signer = signer
			resp, err := tr.RoundTrip(commandRequest(t))
			if tc.rt == nil {
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			if tc.read != nil && !errors.Is(err, raw) {
				t.Fatal("read cause lost")
			}
			if !l.faulted || commandCount(c) == 0 {
				t.Fatal("poison not latched before original return")
			}
			if _, err := tr.RoundTrip(commandRequest(t)); err == nil || sends.Load() != 1 {
				t.Fatal("dispatch after poison")
			}
			_, err = c.settle(nil, err)
			if !errors.Is(err, raw) {
				t.Fatal("original cause lost at settlement")
			}
			cerr := l.close()
			if !errors.Is(cerr, raw) || signer.count() != 0 {
				t.Fatal("failed owner dropped or signer closed")
			}
			if b.closeCount() != 1 {
				t.Fatal("first Close not once")
			}
		})
	}
}

func TestCommandCanonicalJob(t *testing.T) {
	// Only enrollment/SSH is modeled: CLI, Connect, RPC, authentication,
	// Store, Control and Local child execution are actual.
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	engine := auth.NewLegacy("captured-token")
	ctl, err := corecontrol.New(s, []backend.Target{{Name: "local", Kind: "local", Binding: "command-test", Backend: b}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	done := make(chan error, 1)
	go func() { done <- ctl.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	srv := httptest.NewServer(rpc.NewHandler(ctl, engine))
	t.Cleanup(srv.Close)
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("caller-file-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	closeCause := &secretError{"modeled original signer Close failure", secret}
	for _, tc := range []struct {
		name     string
		status   int
		closeErr error
	}{{"success", 0, nil}, {"command", 7, nil}, {"close", 0, closeCause}, {"both", 7, closeCause}} {
		t.Run(tc.name, func(t *testing.T) {
			want := 0
			if tc.status != 0 {
				want = 2
			} else if tc.closeErr != nil {
				want = 1
			}
			status := tc.status
			task := fmt.Sprintf(`{"argv":["/bin/sh","-c","printf command-log; exit %d"]}`, status)
			signer := &modelSigner{err: tc.closeErr}
			var out, errout bytes.Buffer
			hc := *srv.Client()
			base := hc.Transport
			var mu sync.Mutex
			var actualCall *call
			hc.Transport = commandRoundTrip(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				actualCall, _ = req.Context().Value(callKey{}).(*call)
				mu.Unlock()
				return base.RoundTrip(req)
			})
			agent := commandAgent{client: &hc, owner: signer, endpoint: srv.URL, tokenFile: token, bearer: "captured-token", host: "alias", target: "local"}
			err := runCommand(ctx, agent, Command{Args: []string{"run", "-f", "-", "--wait", "--json"}, Stdin: strings.NewReader(task), Stdout: &out, Stderr: &errout})
			if cli.ExitCode(err) != want || signer.count() != 1 {
				t.Fatalf("code=%d want%d err=%v", cli.ExitCode(err), want, err)
			}
			mu.Lock()
			observed := actualCall
			mu.Unlock()
			if observed == nil {
				t.Fatal("canonical command bypassed ledger")
			}
			observed.l.mu.Lock()
			canonicalErr := observed.result
			observed.l.mu.Unlock()
			if tc.closeErr == nil {
				if err != canonicalErr {
					t.Fatal("clean closure changed canonical error identity")
				}
			} else {
				if !errors.Is(err, tc.closeErr) || (canonicalErr != nil && !errors.Is(err, canonicalErr)) {
					t.Fatal("production runCommand dropped a canonical or signer Close cause")
				}
				ce := asCallError(t, err, "")
				l, ok := ce.owner.(*ledger)
				if !ok || l.signer != io.Closer(signer) {
					t.Fatal("original signer not retained")
				}
				if strings.Contains(err.Error(), secret) {
					t.Fatal("signer error leaked")
				}
			}
			observed.l.close()
			if signer.count() != 1 {
				t.Fatal("reported first signer Close retried")
			}
			if !strings.Contains(out.String(), "JOB_STATE_") {
				t.Fatal("missing canonical job")
			}
			var job struct{ ID string }
			if err := json.Unmarshal(out.Bytes(), &job); err != nil || job.ID == "" {
				t.Fatalf("job decode: %v", err)
			}
			out.Reset()
			agent.owner = &modelSigner{}
			if err := runCommand(ctx, agent, Command{Args: []string{"jobs", "logs", job.ID, "--follow"}, Stdout: &out, Stderr: &errout}); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "command-log") {
				t.Fatal("canonical logs not transported")
			}
		})
	}
}

func ExampleCommand() {
	c := Command{Args: []string{"jobs", "list"}}
	fmt.Println(c.Args[0])
	// Output: jobs
}
func ExampleCheckCommand() {
	needed, err := CheckCommand([]string{"--help"})
	fmt.Println(needed, err)
	// Output: false <nil>
}
func ExampleRunCommand() {
	var out bytes.Buffer
	err := RunCommand(context.Background(), Config{}, Command{Args: []string{"--help"}, Stdout: &out})
	fmt.Println(err, out.Len() > 0)
	// Output: <nil> true
}

func TestCommandLateReadPoison(t *testing.T) {
	for _, finishFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(finishFirst), func(t *testing.T) {
			raw := errors.New("late original read cause")
			b := newModelBody(nil)
			b.readErr = errors.Join(io.EOF, raw)
			b.readGate = make(chan struct{})
			var release sync.Once
			done := make(chan struct{})
			var readErr error
			started := false
			t.Cleanup(func() {
				release.Do(func() { close(b.readGate) })
				if started {
					<-done
				}
			})
			l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: b}, nil }))
			signer := &modelSigner{}
			l.signer = signer
			resp, err := tr.RoundTrip(commandRequest(t))
			if err != nil {
				t.Fatal(err)
			}
			started = true
			go func() { _, readErr = resp.Body.Read(make([]byte, 1)); close(done) }()
			<-b.readIn
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if commandCount(c) != 1 {
				t.Fatal("active original read pruned at successful Close")
			}
			if finishFirst {
				if _, err := c.settle(nil, nil); err == nil {
					t.Fatal("command finished with original Read active")
				}
			}
			release.Do(func() { close(b.readGate) })
			<-done
			err = readErr
			if !errors.Is(err, raw) || !l.faulted {
				t.Fatal("late poison not latched before Read returned")
			}
			if !finishFirst {
				if _, err := c.settle(nil, nil); !errors.Is(err, raw) {
					t.Fatal("settlement erased late poison")
				}
			}
			if err := l.close(); !errors.Is(err, raw) || signer.count() != 0 {
				t.Fatal("raw cause or original signer lost")
			}
			if b.closeCount() != 1 || commandCount(c) != 1 {
				t.Fatal("failed original reclaimed/reclosed")
			}
		})
	}
}

func TestCommandDualReplyLimit(t *testing.T) {
	raw := errors.New("original RoundTrip error")
	var last bool
	next := commandRoundTrip(func(*http.Request) (*http.Response, error) {
		var err error
		if last {
			err = raw
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, err
	})
	l, c, tr := commandCall(t, next)
	for i := 0; i < 62; i++ {
		if _, err := tr.RoundTrip(commandRequest(t)); err != nil {
			t.Fatal(err)
		}
	}
	last = true
	if _, err := tr.RoundTrip(commandRequest(t)); !errors.Is(err, raw) {
		t.Fatal("dual response/error cause lost")
	}
	if commandCount(c) != 64 || c.reserved != 0 {
		t.Fatal("dual original exceeded/dropped record reservation")
	}
	if _, err := tr.RoundTrip(commandRequest(t)); err == nil {
		t.Fatal("post-error dispatch")
	}
	if _, err := c.settle(nil, nil); !errors.Is(err, raw) {
		t.Fatal("raw error forgotten")
	}
	if err := l.close(); !errors.Is(err, raw) {
		t.Fatal("failed command not retained")
	}
}

func TestCommandHeldClose(t *testing.T) {
	raw := errors.New("modeled held first Close failure")
	b := newModelBody(nil)
	b.gate = make(chan struct{})
	b.closeErr = raw
	var release sync.Once
	done := make(chan struct{})
	var closeErr error
	started := false
	t.Cleanup(func() {
		release.Do(func() { close(b.gate) })
		if started {
			<-done
		}
	})
	l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: b}, nil }))
	signer := &modelSigner{}
	l.signer = signer
	resp, err := tr.RoundTrip(commandRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	started = true
	go func() { closeErr = resp.Body.Close(); close(done) }()
	<-b.entered
	if _, err := resp.Body.Read(make([]byte, 1)); err == nil || b.readCount() != 0 {
		t.Fatal("underlying Read admitted after first Close started")
	}
	if commandCount(c) != 1 {
		t.Fatal("held Close body pruned")
	}
	if _, err := c.settle(nil, nil); err == nil {
		t.Fatal("command settled while original Close held")
	}
	release.Do(func() { close(b.gate) })
	<-done
	if !errors.Is(closeErr, raw) {
		t.Fatal("first Close cause lost")
	}
	if err := l.close(); !errors.Is(err, raw) || signer.count() != 0 || b.closeCount() != 1 {
		t.Fatal("held/failed Close owner not retained")
	}
}

type commandJobAnnouncement struct {
	once  sync.Once
	ready chan string
}

func (w *commandJobAnnouncement) Write(b []byte) (int, error) {
	if s := strings.Fields(string(b)); len(s) == 2 && s[0] == "job:" {
		w.once.Do(func() { w.ready <- s[1] })
	}
	return len(b), nil
}

type commandLogAnnouncement struct {
	mu    sync.Mutex
	data  bytes.Buffer
	once  sync.Once
	ready chan struct{}
}

func (w *commandLogAnnouncement) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.data.Write(b)
	if strings.Contains(w.data.String(), "ready") {
		w.once.Do(func() { close(w.ready) })
	}
	return len(b), nil
}

func TestCommandCanonicalCancel(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	engine := auth.NewLegacy("captured-token")
	ctl, err := corecontrol.New(s, []backend.Target{{Name: "local", Kind: "local", Binding: "command-cancel", Backend: b}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	controlDone := make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		if err := <-controlDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	go func() { controlDone <- ctl.Run(ctx) }()
	srv := httptest.NewServer(rpc.NewHandler(ctl, engine))
	t.Cleanup(srv.Close)
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("caller-token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	agent := commandAgent{client: srv.Client(), endpoint: srv.URL, tokenFile: token, bearer: "captured-token", host: "alias", target: "local"}
	submitSigner := &modelSigner{}
	agent.owner = submitSigner
	announced := &commandJobAnnouncement{ready: make(chan string, 1)}
	submitDone := make(chan struct{})
	var submitErr error
	t.Cleanup(func() { cancel(); <-submitDone })
	go func() {
		submitErr = runCommand(ctx, agent, Command{Args: []string{"run", "-f", "-", "--wait"}, Stdin: strings.NewReader(`{"argv":["/bin/sh","-c","printf ready; exec /bin/sleep 120"]}`), Stdout: io.Discard, Stderr: announced})
		close(submitDone)
	}()
	var id string
	select {
	case id = <-announced.ready:
	case <-submitDone:
		t.Fatalf("run ended: %v", submitErr)
	case <-ctx.Done():
		t.Fatal("job not accepted")
	}
	logSigner := &modelSigner{}
	logAgent := agent
	logAgent.owner = logSigner
	logOutput := &commandLogAnnouncement{ready: make(chan struct{})}
	logDone := make(chan struct{})
	var logErr error
	t.Cleanup(func() { cancel(); <-logDone })
	go func() {
		logErr = runCommand(ctx, logAgent, Command{Args: []string{"jobs", "logs", id, "--follow"}, Stdout: logOutput, Stderr: io.Discard})
		close(logDone)
	}()
	select {
	case <-logOutput.ready:
	case <-logDone:
		t.Fatalf("logs ended before original job ran: %v", logErr)
	case <-ctx.Done():
		t.Fatal("job did not run")
	}
	cancelSigner := &modelSigner{}
	cancelAgent := agent
	cancelAgent.owner = cancelSigner
	if err := runCommand(ctx, cancelAgent, Command{Args: []string{"jobs", "cancel", id, "--yes"}, Stdout: io.Discard, Stderr: io.Discard}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-submitDone:
	case <-ctx.Done():
		t.Fatal("canceled job did not settle")
	}
	select {
	case <-logDone:
	case <-ctx.Done():
		t.Fatal("original follow did not settle")
	}
	if cli.ExitCode(submitErr) != 2 || logErr != nil || submitSigner.count() != 1 || logSigner.count() != 1 || cancelSigner.count() != 1 {
		t.Fatalf("canonical cancel: run=%v logs=%v", submitErr, logErr)
	}
}

func TestCommandAllRefusalOwners(t *testing.T) {
	l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
	}))
	signer := &modelSigner{}
	l.signer = signer
	for i := 0; i < 63; i++ {
		if _, err := tr.RoundTrip(commandRequest(t)); err != nil {
			t.Fatal(err)
		}
	}
	cancelled := make(chan struct{})
	var cancels atomic.Int32
	c.cancel = func() {
		if cancels.Add(1) == 1 {
			close(cancelled)
		}
	}
	gate := make(chan struct{})
	var release sync.Once
	var wg sync.WaitGroup
	t.Cleanup(func() { release.Do(func() { close(gate) }); wg.Wait() })
	raw := []error{errors.New("first refused original Close"), errors.New("second refused original Close")}
	bodies := []*modelBody{newModelBody(nil), newModelBody(nil)}
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := range bodies {
		bodies[i].gate = gate
		bodies[i].closeErr = raw[i]
		req := commandRequest(t)
		req.Body = bodies[i]
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, err := tr.RoundTrip(req); errs <- err }()
	}
	close(start)
	// Cancel is observed while BOTH original Close calls are still blocked.
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("first refusal did not cancel before Close")
	}
	for _, b := range bodies {
		select {
		case <-b.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close original not registered")
		}
	}
	l.mu.Lock()
	pending, retained, high := c.refusalPending, len(c.refusals), c.refusalHighWater
	owners := append([]*refusedRequest(nil), c.refusals...)
	l.mu.Unlock()
	if pending != 2 || retained != 2 || high != 2 || cancels.Load() != 1 || signer.count() != 0 {
		t.Fatal("second pending original lost")
	}
	seen := make(map[io.ReadCloser]bool)
	for _, r := range owners {
		seen[r.original] = true
		if r.request.Body != r.original {
			t.Fatal("actual refused request/body identity replaced")
		}
	}
	if !seen[bodies[0]] || !seen[bodies[1]] {
		t.Fatal("second original body owner not retained")
	}
	for _, r := range owners {
		if r.request == nil || r.original == nil || r.closed {
			t.Fatal("pending owner replaced or invented closed")
		}
	}
	release.Do(func() { close(gate) })
	wg.Wait()
	for i := 0; i < 2; i++ {
		if err := <-errs; err == nil {
			t.Fatal("refusal disappeared")
		}
	}
	if _, err := c.settle(nil, nil); !errors.Is(err, raw[0]) || !errors.Is(err, raw[1]) {
		t.Fatal("settlement lost a simultaneous original cause")
	}
	err := l.close()
	if !errors.Is(err, raw[0]) || !errors.Is(err, raw[1]) || signer.count() != 0 {
		t.Fatal("final owner/cause missing")
	}
	l.mu.Lock()
	pending, retained = c.refusalPending, len(c.refusals)
	l.mu.Unlock()
	if pending != 0 || retained != 2 {
		t.Fatal("failed originals pruned")
	}
	for _, r := range owners {
		r.Close()
	}
	for _, b := range bodies {
		if b.closeCount() != 1 {
			t.Fatal("original Close repeated")
		}
	}
}

func TestCommandCleanRefusalRetires(t *testing.T) {
	l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) {
		t.Error("dispatch after fence")
		return nil, errors.New("unexpected dispatch")
	}))
	l.fenced = true
	var cancels atomic.Int32
	c.cancel = func() { cancels.Add(1) }
	for i := 0; i < 10000; i++ {
		req := commandRequest(t)
		b := newModelBody(nil)
		req.Body = b
		if _, err := tr.RoundTrip(req); err == nil {
			t.Fatal("closed command admitted")
		}
		l.mu.Lock()
		n, pending := len(c.refusals), c.refusalPending
		l.mu.Unlock()
		if n != 0 || pending != 0 || b.closeCount() != 1 {
			t.Fatal("positive refusal Close bookkeeping leaked")
		}
	}
	if cancels.Load() != 1 || !l.faulted || !c.commandRefused {
		t.Fatal("local refusal poison not permanent")
	}
}

func TestCommandNoBodyRefusalCancels(t *testing.T) {
	for _, noBody := range []bool{false, true} {
		t.Run(fmt.Sprint(noBody), func(t *testing.T) {
			l, c, tr := commandCall(t, commandRoundTrip(func(*http.Request) (*http.Response, error) { t.Error("dispatch after local refusal"); return nil, nil }))
			l.fenced = true
			var cancels atomic.Int32
			c.cancel = func() { cancels.Add(1) }
			req := commandRequest(t)
			if noBody {
				req.Body = http.NoBody
			}
			if _, err := tr.RoundTrip(req); err == nil || !l.faulted || !c.commandRefused || cancels.Load() != 1 {
				t.Fatal("bodyless local refusal did not stop command")
			}
			if c.refusalPending != 0 || len(c.refusals) != 0 {
				t.Fatal("invented body Close pending")
			}
		})
	}
}
