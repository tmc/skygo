package kube

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/rest"
)

const successStatus = `{"metadata":{},"status":"Success"}`

func exitStatus(code string) string {
	return `{"metadata":{},"status":"Failure","message":"command terminated","reason":"NonZeroExitCode",` +
		`"details":{"causes":[{"reason":"ExitCode","message":"` + code + `"}]}}`
}

// execScript is what the in-process server does for one exec, in the
// kubelet's order: read stdin to its end, write and close stdout and stderr,
// then write the status and close the error stream.
type execScript struct {
	stdin          bool
	stdout, stderr []byte
	status         string
	hold           chan struct{} // if set, the server waits for it before writing output
}

type execSeen struct {
	types []string
	stdin []byte
	err   error
}

// serveExec runs s on the next exec's streams in a goroutine.
func (a *fakeAPI) serveExec(s execScript) <-chan execSeen {
	c := make(chan execSeen, 1)
	n := 3
	if s.stdin {
		n = 4
	}
	go func() {
		var seen execSeen
		defer func() { c <- seen }()
		byType := map[string]httpstream.Stream{}
		timeout := time.After(testWait)
		for len(seen.types) < n {
			select {
			case st := <-a.streams:
				typ := st.Headers().Get(corev1.StreamType)
				seen.types = append(seen.types, typ)
				byType[typ] = st
			case <-timeout:
				seen.err = errors.New("streams did not arrive")
				return
			}
		}
		if in := byType[corev1.StreamTypeStdin]; in != nil {
			seen.stdin, seen.err = io.ReadAll(in)
		}
		if s.hold != nil {
			<-s.hold
		}
		byType[corev1.StreamTypeStdout].Write(s.stdout)
		byType[corev1.StreamTypeStdout].Close()
		byType[corev1.StreamTypeStderr].Write(s.stderr)
		byType[corev1.StreamTypeStderr].Close()
		if s.status != "" {
			byType[corev1.StreamTypeError].Write([]byte(s.status))
		}
		byType[corev1.StreamTypeError].Close()
	}()
	return c
}

func startTestExec(t *testing.T, a *fakeAPI, tr *envTransport, r execRequest) *execFrame {
	t.Helper()
	x, err := tr.startExec(r)
	if err != nil {
		t.Fatal(err)
	}
	a.keep(x.envFrame)
	return x
}

func waitExec(t *testing.T, x *execFrame) execResult {
	t.Helper()
	waitSettled(t, x.envFrame)
	return x.result()
}

func TestExecRun(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	seen := a.serveExec(execScript{stdin: true, stdout: []byte("out"), stderr: []byte("err"), status: successStatus})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"sh", "-c", "echo $HOME; cat"}, Stdin: []byte("input")})
	r := waitExec(t, x)
	s := <-seen
	if s.err != nil {
		t.Fatal(s.err)
	}
	if r.Err != nil || !r.Settled {
		t.Fatalf("result = %+v", r)
	}
	if !r.Reported || r.Code != 0 {
		t.Fatalf("exit = %v %d, want reported 0", r.Reported, r.Code)
	}
	if string(r.Stdout) != "out" || string(r.Stderr) != "err" || r.Truncated {
		t.Fatalf("output = %q %q %v", r.Stdout, r.Stderr, r.Truncated)
	}
	if string(s.stdin) != "input" {
		t.Fatalf("server stdin = %q", s.stdin)
	}
	if want := []string{"error", "stdin", "stdout", "stderr"}; !slices.Equal(s.types, want) {
		t.Fatalf("stream order = %q, want %q", s.types, want)
	}
	req := a.requestList()[0]
	if got, want := req.URL.Path, "/api/v1/namespaces/workloads/pods/env-0/exec"; got != want {
		t.Errorf("path = %s", got)
	}
	q := req.URL.Query()
	if got := q["command"]; !slices.Equal(got, []string{"sh", "-c", "echo $HOME; cat"}) {
		t.Errorf("command = %q", got)
	}
	if q.Get("container") != "main" || q.Get("stdin") != "true" || q.Get("stdout") != "true" || q.Get("stderr") != "true" || q.Has("tty") {
		t.Errorf("query = %v", q)
	}
}

func TestExecWithoutStdin(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	seen := a.serveExec(execScript{status: exitStatus("3")})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"false"}})
	r := waitExec(t, x)
	s := <-seen
	if s.err != nil {
		t.Fatal(s.err)
	}
	if want := []string{"error", "stdout", "stderr"}; !slices.Equal(s.types, want) {
		t.Fatalf("stream order = %q", s.types)
	}
	if r.Err != nil || !r.Reported || r.Code != 3 {
		t.Fatalf("result = %+v", r)
	}
	if a.requestList()[0].URL.Query().Has("stdin") {
		t.Fatal("stdin requested")
	}
}

// Statuses that are lost, malformed or oversized are Unknown, not exit zero.
func TestExecStatusUnknown(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   error // if set, the specific error
	}{
		{"lost", "", nil},
		{"malformed", "{", nil},
		{"oversized", `{"status":"Success","message":"` + strings.Repeat("x", maxExecStatus) + `"}`, errExecStatusLimit},
		{"failure", `{"status":"Failure","message":"container not found"}`, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			tr := a.transport(t)
			seen := a.serveExec(execScript{stdout: []byte("partial"), status: tt.status})
			x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
			r := waitExec(t, x)
			<-seen
			if r.Reported {
				t.Fatalf("exit reported: %d", r.Code)
			}
			if r.Err == nil {
				t.Fatal("unknown status has no error")
			}
			if tt.want != nil && !errors.Is(r.Err, tt.want) {
				t.Fatalf("err = %v, want %v", r.Err, tt.want)
			}
		})
	}
}

func TestExecStatus(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want int
		ok   bool
	}{
		{"success", successStatus, 0, true},
		{"exit", exitStatus("42"), 42, true},
		{"exit255", exitStatus("255"), 255, true},
		{"exit256", exitStatus("256"), 0, false},
		{"negative", exitStatus("-1"), 0, false},
		{"nodetails", `{"status":"Failure","reason":"NonZeroExitCode"}`, 0, false},
		{"nocause", `{"status":"Failure","reason":"NonZeroExitCode","details":{"causes":[{"reason":"Other","message":"1"}]}}`, 0, false},
		{"failure", `{"status":"Failure","message":"m"}`, 0, false},
		{"unknown", `{"status":"Pending"}`, 0, false},
		{"empty", "", 0, false},
		{"malformed", "not json", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := execStatus([]byte(tt.in))
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("execStatus = %d, %v; want %d, ok %v", got, err, tt.want, tt.ok)
			}
		})
	}
}

func TestExecOutputBounded(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	out := bytes.Repeat([]byte("o"), backend.MaxLogBytes-10)
	seen := a.serveExec(execScript{stdout: out, stderr: []byte(strings.Repeat("e", 100)), status: successStatus})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"yes"}})
	r := waitExec(t, x)
	<-seen
	if r.Err != nil || !r.Reported {
		t.Fatalf("result err = %v reported = %v", r.Err, r.Reported)
	}
	if !r.Truncated || len(r.Stdout)+len(r.Stderr) != backend.MaxLogBytes {
		t.Fatalf("retained %d+%d truncated %v", len(r.Stdout), len(r.Stderr), r.Truncated)
	}
}

func TestExecRequestValidation(t *testing.T) {
	tr, err := newEnvTransport(&rest.Config{Host: "https://127.0.0.1:6443"}, "workloads")
	if err != nil {
		t.Fatal(err)
	}
	ok := execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}}
	tests := []struct {
		name string
		edit func(*execRequest)
	}{
		{"pod", func(r *execRequest) { r.Pod = "Env" }},
		{"container", func(r *execRequest) { r.Container = "" }},
		{"argv", func(r *execRequest) { r.Argv = nil }},
		{"nul", func(r *execRequest) { r.Argv = []string{"a\x00b"} }},
		{"argvsize", func(r *execRequest) { r.Argv = []string{strings.Repeat("a", maxExecArgv), "b"} }},
		{"stdinsize", func(r *execRequest) { r.Stdin = make([]byte, maxExecStdin+1) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ok
			tt.edit(&r)
			if x, err := tr.startExec(r); err == nil {
				x.cancel()
				t.Fatal("startExec succeeded")
			}
		})
	}
	if _, err := tr.execURL(ok); err != nil {
		t.Fatal(err)
	}
}

// FIN of stdin and the later Reset are separate operations, each made once.
func TestExecStdinFinThenReset(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	var streams []*faultStream
	fc := &faultConn{stream: func(s httpstream.Stream) httpstream.Stream {
		fs := &faultStream{Stream: s}
		streams = append(streams, fs)
		return fs
	}}
	tr.wrap = wrapWith(fc)
	seen := a.serveExec(execScript{stdin: true, status: successStatus})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"cat"}, Stdin: []byte("x")})
	r := waitExec(t, x)
	<-seen
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if len(streams) != 4 {
		t.Fatalf("%d streams", len(streams))
	}
	for i, s := range streams {
		fins, resets := s.counts()
		wantFins := 0
		if i == 1 {
			wantFins = 1 // stdin
		}
		if fins != wantFins || resets != 1 {
			t.Errorf("stream %d: %d FIN, %d Reset; want %d, 1", i, fins, resets, wantFins)
		}
	}
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("connection closed %d times", closes)
	}
}

func TestExecStdinWriteFailure(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	fc := &faultConn{stream: func(s httpstream.Stream) httpstream.Stream {
		if s.Headers().Get(corev1.StreamType) == corev1.StreamTypeStdin {
			return &faultStream{Stream: s, writeErr: errSynthetic}
		}
		return s
	}}
	tr.wrap = wrapWith(fc)
	hold := make(chan struct{})
	defer close(hold)
	seen := a.serveExec(execScript{stdin: true, status: successStatus, hold: hold})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"cat"}, Stdin: []byte("x")})
	r := waitExec(t, x)
	_ = seen
	if !errors.Is(r.Err, errSynthetic) {
		t.Fatalf("err = %v", r.Err)
	}
	if r.Reported {
		t.Fatal("exit reported after stdin failure closed the connection")
	}
	if closes, _ := fc.counts(); closes != 1 {
		t.Fatalf("connection closed %d times", closes)
	}
}

// A failed CreateStream after earlier streams were created closes the whole
// connection once and starts no worker.
func TestExecPartialCreate(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	fc := &faultConn{failCreate: 3}
	tr.wrap = wrapWith(fc)
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"cat"}, Stdin: []byte("x")})
	r := waitExec(t, x)
	if !errors.Is(r.Err, errSynthetic) || r.Reported {
		t.Fatalf("result = %+v", r)
	}
	closes, creates := fc.counts()
	if closes != 1 || creates != 3 {
		t.Fatalf("closes = %d creates = %d; want 1, 3", closes, creates)
	}
	x.mu.Lock()
	n := len(x.streams)
	x.mu.Unlock()
	if n != 2 {
		t.Fatalf("frame holds %d streams, want the 2 created", n)
	}
}

// A blocked CreateStream keeps the frame unsettled after cancellation. The
// connection's single Close waits for that call, so its GOAWAY never precedes
// the stream request; no further stream is requested, and the frame settles
// when the call returns.
func TestExecBlockedCreate(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	gate := make(chan struct{})
	fc := &faultConn{createGate: gate}
	tr.wrap = wrapWith(fc)
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
	eventually(t, "CreateStream", func() bool { _, n := fc.counts(); return n == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	r := x.wait(ctx)
	if r.Settled || r.Reported {
		t.Fatalf("result = %+v", r)
	}
	// wait canceled the frame synchronously; a Close it started would be
	// recorded as started before cancel returned.
	x.mu.Lock()
	started := x.connErr.started
	x.mu.Unlock()
	if closes, _ := fc.counts(); started || closes != 0 {
		t.Fatalf("Close started while CreateStream is in flight (started %v, %d calls)", started, closes)
	}
	if x.result().Settled {
		t.Fatal("settled while CreateStream is blocked")
	}
	close(gate)
	r = waitExec(t, x)
	if !errors.Is(r.Err, errEnvCanceled) {
		t.Fatalf("err = %v", r.Err)
	}
	if closes, creates := fc.counts(); closes != 1 || creates != 1 {
		t.Fatalf("closes = %d creates = %d; want 1, 1", closes, creates)
	}
}

// Cancellation closes the transport; the original call stays on the frame,
// whose exit stays Unknown.
func TestExecCancel(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	hold := make(chan struct{})
	defer close(hold)
	a.serveExec(execScript{status: successStatus, hold: hold})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"sleep", "inf"}})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	x.wait(ctx)
	r := waitExec(t, x)
	if r.Reported {
		t.Fatal("canceled exec reported an exit")
	}
	if !errors.Is(r.Err, errEnvCanceled) {
		t.Fatalf("err = %v", r.Err)
	}
	if n := len(a.requestList()); n != 1 {
		t.Fatalf("server saw %d exec requests", n)
	}
}

func TestExecWrongProtocolServer(t *testing.T) {
	a := newFakeAPI(t, "v5.channel.k8s.io")
	tr := a.transport(t)
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
	r := waitExec(t, x)
	if r.Err == nil || r.Reported {
		t.Fatalf("result = %+v", r)
	}
	if got := fmt.Sprint(a.requestList()[0].Header.Values(httpstream.HeaderProtocolVersion)); got != "["+execProtocol+"]" {
		t.Fatalf("offered %s", got)
	}
	if req := a.requestList()[0]; req.Method != http.MethodPost {
		t.Fatalf("method %s", req.Method)
	}
}

// Waiting on a settled exec with an ended context never cancels it.
func TestExecWaitAfterSettle(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	tr := a.transport(t)
	a.serveExec(execScript{status: successStatus})
	x := startTestExec(t, a, tr, execRequest{Pod: "env-0", Container: "main", Argv: []string{"true"}})
	if r := waitExec(t, x); r.Err != nil || !r.Reported {
		t.Fatalf("result = %+v", r)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 64 {
		if r := x.wait(ctx); r.Err != nil || !r.Settled {
			t.Fatalf("wait after settle = %+v", r)
		}
	}
}
