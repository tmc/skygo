//go:build linux

package ec2job

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
)

// The examples of a successful job run it on the real canonical server:
// an installation initialized with the canonical CLI and served by
// server.Run with its local target "ec2vm", which runs the task.
//
// MODEL portions: the VM is testVM's identity, never verified with EC2,
// and the session's control client is loopback HTTP with the operator's
// scoped bearer, as sshagent's Session.Control sends it; it does not
// cross SSH. Each example removes its directory only after its Job closed
// and server.Run returned nil; otherwise it keeps the directory and error.

// An exampleServer is one example's installation, server and job.
type exampleServer struct {
	dir     string
	cfg     *Config
	vm      VM
	address string
	token   string
	cancel  context.CancelFunc
	done    chan error // run's one result
	ran     bool       // run's result was received, as runErr
	runErr  error
	jobs    []*Job
	held    error // every error that may hold an original below dir
}

// startExample initializes and serves an installation in a new temporary
// directory. The caller calls stop, also when startExample fails.
func startExample() (*exampleServer, error) {
	dir, err := os.MkdirTemp("", "ec2job-example-")
	if err != nil {
		return nil, err
	}
	e := &exampleServer{dir: dir}
	return e, e.keep(e.start())
}

func (e *exampleServer) start() error {
	b, err := json.Marshal(testWire(e.dir, testInstanceID))
	if err != nil {
		return err
	}
	if e.cfg, err = ParseConfig(bytes.NewReader(b)); err != nil {
		return err
	}
	e.vm = testVM(e.cfg)
	in := e.cfg.Installation
	for _, d := range []string{filepath.Dir(in.DataDir), in.DataDir, in.ArtifactDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			return err
		}
	}
	admin := filepath.Join(filepath.Dir(in.DataDir), "admin.token")
	for _, args := range [][]string{
		{"auth", "init", "--auth-file", in.AuthFile, "--data-dir", in.DataDir, "--token-output", admin},
		{"auth", "principal", "add", "bob", "--role", "operator", "--auth-file", in.AuthFile},
		{"auth", "token", "issue", "bob", "--output", e.cfg.ScopedTokenFile, "--auth-file", in.AuthFile},
		{"state", "artifacts", "init", "--data-dir", in.DataDir, "--artifact-dir", in.ArtifactDir, "--ack-writers-stopped"},
	} {
		root := cli.NewWithClient(nil, nil, cli.ClientOptions{})
		root.SetArgs(args)
		root.SetOut(io.Discard)
		root.SetErr(io.Discard)
		if err := root.ExecuteContext(context.Background()); err != nil {
			return fmt.Errorf("skygo %s %s: %w", args[0], args[1], err)
		}
	}
	token, err := os.ReadFile(e.cfg.ScopedTokenFile)
	if err != nil {
		return err
	}
	e.token = strings.TrimSpace(string(token))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	// The address is reserved and released, so another listener may take
	// it before server.Run does: the server takes no listener. Then the
	// wait below fails, bounded, and stop still joins server.Run.
	e.address = ln.Addr().String()
	if err := ln.Close(); err != nil {
		return err
	}
	return e.serve(func(ctx context.Context) error {
		return server.Run(ctx, server.Config{
			DataDir:              in.DataDir,
			AuthFile:             in.AuthFile,
			ArtifactDir:          in.ArtifactDir,
			ExistingInstallation: true,
			Listen:               e.address,
			LocalTargetName:      e.cfg.Target,
		})
	}, startTimeout)
}

// startTimeout bounds every request of an example server's startup.
const startTimeout = 30 * time.Second

// errReturned reports a server whose run returned before it answered.
// Its result is halt's.
var errReturned = errors.New("server returned before it was ready")

// serve runs run in a goroutine that halt joins, and waits until the
// server at e.address answers GetServerInfo. One startup context bounds
// every request: it ends after timeout, or as soon as run returns, whose
// result serve then keeps for halt. Neither the deadline nor any error of
// serve shows that run returned; only halt does.
func (e *exampleServer) serve(run func(context.Context) error, timeout time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	start, returned := context.WithCancel(context.Background())
	defer returned()
	e.cancel, e.done = cancel, make(chan error, 1)
	go func() {
		e.done <- run(ctx)
		returned()
	}()
	start, stop := context.WithTimeout(start, timeout)
	defer stop()
	c := e.control()
	for {
		_, err := c.GetServerInfo(start, connect.NewRequest(&v1.GetServerInfoRequest{}))
		if err == nil {
			return nil
		}
		select {
		case rerr := <-e.done:
			e.ran, e.runErr = true, rerr
			return errReturned
		default:
		}
		if start.Err() != nil {
			return fmt.Errorf("server not ready: %w", errors.Join(start.Err(), err))
		}
		t := time.NewTimer(100 * time.Millisecond)
		select {
		case <-start.Done():
		case <-t.C:
		}
		t.Stop()
	}
}

// halt stops the server serve started and waits for its run to return.
// It receives run's result once and returns it to every call, also when
// serve already received it.
func (e *exampleServer) halt() error {
	if e.cancel == nil {
		return nil
	}
	e.cancel()
	if !e.ran {
		e.ran, e.runErr = true, <-e.done
	}
	return e.runErr
}

// A bearer sends each request with the scoped token.
type bearer struct {
	token string
	base  http.RoundTripper
}

func (b bearer) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(req)
}

func (e *exampleServer) control() skyv1connect.ControlServiceClient {
	return skyv1connect.NewControlServiceClient(&http.Client{Transport: bearer{e.token, http.DefaultTransport}}, "http://"+e.address)
}

// newJob creates, binds and attaches a job running testTaskJSON on a
// one-file workdir.
func (e *exampleServer) newJob(name string) (*Job, error) {
	src := filepath.Join(e.dir, name+"-workdir")
	if err := os.Mkdir(src, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(src, "input.txt"), []byte("input\n"), 0o600); err != nil {
		return nil, err
	}
	archive, digest, err := Snapshot(src)
	if err != nil {
		return nil, e.keep(err)
	}
	task, err := e.cfg.Task(TaskInput{JSON: []byte(testTaskJSON)}, digest, int64(len(archive)), time.Now())
	if err != nil {
		return nil, err
	}
	j, err := Create(filepath.Join(e.dir, name), e.cfg, e.vm, task, archive)
	if err != nil {
		return nil, e.keep(err)
	}
	e.jobs = append(e.jobs, j)
	if err := j.Bind(e.cfg, e.vm); err != nil {
		return nil, e.keep(err)
	}
	return j, e.keep(j.Attach(peerSession{e.control(), sessionFor(e.cfg, e.vm)}))
}

// keep records err, if any, as one that may hold an original, and
// returns it.
func (e *exampleServer) keep(err error) error {
	if err != nil {
		e.held = errors.Join(e.held, err)
	}
	return err
}

// stop closes every job once, stops the server and waits for server.Run
// to return, and then removes the directory only if all of that, and
// every error kept before, settled.
func (e *exampleServer) stop() {
	if e == nil {
		return
	}
	for _, j := range e.jobs {
		e.keep(j.Close())
	}
	e.keep(e.halt())
	removeExample(e.dir, e.held)
}

// keptExamples keeps every error that may hold an original reachable
// until the process exits.
var keptExamples []error

func removeExample(dir string, err error) {
	if err != nil {
		keptExamples = append(keptExamples, err)
		fmt.Printf("keeping %s: %v\n", dir, err)
		return
	}
	if err := os.RemoveAll(dir); err != nil {
		fmt.Println(err)
	}
}

var exampleWait = Wait{Poll: 100 * time.Millisecond}

func ExampleConfig_Session() {
	b, err := json.Marshal(testWire("/srv/example", testInstanceID))
	if err != nil {
		fmt.Println(err)
		return
	}
	c, err := ParseConfig(bytes.NewReader(b))
	if err != nil {
		fmt.Println(err)
		return
	}
	s := c.Session(c.IP, nil)
	fmt.Println(s.Address, s.User, s.Target)
	fmt.Println(s.KnownHostsFile, s.KeyFile)
	// Output:
	// 10.0.0.5:22 ec2-user ec2vm
	// /srv/example/known_hosts /srv/example/id_ed25519
}

func ExampleJob_Start() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	st, err := j.Start(ctx, exampleWait)
	e.keep(err)
	fmt.Println(st.State, st.Output, st.Succeeded(), err)
	// Output:
	// SUCCEEDED saved true <nil>
}

func ExampleJob_Resume() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Without a Wait, Start returns after its one observation.
	_, err = j.Start(ctx, Wait{})
	fmt.Println(e.keep(err), j.Known())

	// Resume observes the known job until it ends; it never submits again.
	st, err := j.Resume(ctx, exampleWait)
	e.keep(err)
	fmt.Println(st.State, st.Succeeded(), err)
	// Output:
	// <nil> true
	// SUCCEEDED true <nil>
}

func ExampleJob_Refresh() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	st, err := j.Refresh(ctx)
	e.keep(err)
	fmt.Println(st.State, err)
	// Output:
	// SUCCEEDED <nil>
}

func ExampleJob_Known() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	fmt.Println(j.Known())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	fmt.Println(j.Known())
	// Output:
	// false
	// true
}

func ExampleJob_NeedsRemote() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	// The job was seen to end and its result download was closed: a
	// cancel needs no remote call, and Cancel makes none.
	if err := e.keep(j.RequestCancel()); err != nil {
		return
	}
	fmt.Println(j.NeedsRemote())
	st, err := j.Cancel(ctx, exampleWait)
	e.keep(err)
	fmt.Println(st.State, st.Desired, err)
	// Output:
	// false
	// SUCCEEDED cancel <nil>
}

func ExampleJob_Status() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	st := j.Status()
	fmt.Println(st.Method, st.Upload, st.Submit, st.State, st.Output, st.Download, st.Durable)
	fmt.Println(len(st.JobID), st.OutputFile == filepath.Join(e.dir, "job", "output.tar"))
	// Output:
	// SubmitWorkspaceJob finalized accepted SUCCEEDED saved closed true
	// 32 true
}

func ExampleJob_Logs() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	var out bytes.Buffer
	page, err := j.Logs(ctx, 0, &out)
	e.keep(err)
	fmt.Println(strings.Contains(out.String(), "hello"), page.NextOffset == int64(out.Len()), err)
	// Output:
	// true true <nil>
}

func ExampleJob_CommitLogs() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	j, err := e.newJob("job")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := j.Start(ctx, exampleWait); e.keep(err) != nil {
		return
	}
	var out bytes.Buffer
	page, err := j.NextLogs(ctx, &out)
	if e.keep(err) != nil {
		return
	}
	// Once the copy is delivered, commit it: the next copy starts there.
	err = j.CommitLogs(page)
	e.keep(err)
	fmt.Println(err, j.Status().LogOffset == page.NextOffset, page.NextOffset > 0)
	// Output:
	// <nil> true true
}

func ExampleStatus_Ended() {
	for _, st := range []Status{
		{State: "RUNNING"},
		{State: "FAILED"},
		{State: "NEVER_SUBMITTED"},
	} {
		fmt.Println(st.State, st.Ended())
	}
	// Output:
	// RUNNING false
	// FAILED true
	// NEVER_SUBMITTED true
}

func ExampleStatus_Succeeded() {
	for _, st := range []Status{
		{State: "SUCCEEDED", Output: "saved"},
		{State: "SUCCEEDED", Output: "not-used"},
		{State: "SUCCEEDED"}, // its outputs are not yet saved
		{State: "FAILED", Output: "saved"},
	} {
		fmt.Println(st.State, st.Output, st.Succeeded())
	}
	// Output:
	// SUCCEEDED saved true
	// SUCCEEDED not-used true
	// SUCCEEDED  false
	// FAILED saved false
}

// exampleRemoteError starts a job whose control client reaches no server
// (MODEL: a loopback address nothing listens on) and returns Start's
// error. Nothing was stored for the refused request.
func exampleRemoteError(e *exampleServer) (*RemoteError, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	e.address = ln.Addr().String()
	if err := ln.Close(); err != nil {
		return nil, err
	}
	j, err := e.newJob("job")
	if err != nil {
		return nil, err
	}
	_, err = j.Start(context.Background(), Wait{})
	var re *RemoteError
	if !errors.As(err, &re) {
		return nil, err
	}
	return re, nil
}

func ExampleRemoteError_Error() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	re, err := exampleRemoteError(e)
	if e.keep(err) != nil {
		return
	}
	fmt.Println(re.Error())
	// Output:
	// ec2job: GetServerInfo: unavailable
}

func ExampleRemoteError_Unwrap() {
	e, err := startExample()
	defer e.stop()
	if err != nil {
		return
	}
	re, err := exampleRemoteError(e)
	if e.keep(err) != nil {
		return
	}
	// Unwrap returns the original error, whose message the text omits.
	fmt.Println(re.Method, connect.CodeOf(re.Unwrap()) == re.Code)
	// Output:
	// GetServerInfo true
}

// The tests below check the examples' serve and halt with MODEL servers in
// place of server.Run: a listener that never answers, a run that returns
// at once, and a minimal ready handler. Each bound is far above what the
// helper needs and far below a hung request.

var errModelRun = errors.New("model: the server failed to serve")

// stalled listens on a loopback address and accepts connections it never
// reads or answers (MODEL: a listener that took the released address, or
// a server whose handlers hang). Its cleanup closes the listener and every
// accepted connection, and joins its accept loop.
func stalled(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
		wg    sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// modelServer returns an exampleServer for address whose server halt
// joins at the test's end, also when the test fails first.
func modelServer(t *testing.T, address string) *exampleServer {
	e := &exampleServer{dir: t.TempDir(), address: address, token: "model"}
	t.Cleanup(func() { e.halt() })
	return e
}

// TestServeStalledHeaders: the server accepts the request and never
// answers. The startup deadline bounds that request, and serve fails;
// run, which has not returned, is joined only by halt.
func TestServeStalledHeaders(t *testing.T) {
	e := modelServer(t, stalled(t))
	begin := time.Now()
	err := e.serve(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}, 500*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errReturned) {
		t.Fatalf("serve = %v; want the startup deadline", err)
	}
	if d := time.Since(begin); d > 20*time.Second {
		t.Fatalf("serve took %v with a 500ms startup bound", d)
	}
	if e.ran {
		t.Fatal("serve received a result from a run that had not returned")
	}
	if err := e.halt(); err != nil || !e.ran {
		t.Fatalf("halt = %v, joined %v", err, e.ran)
	}
}

// TestServeRunReturns: run returns while a request stalls. serve sees it
// at once, long before its deadline, and keeps run's result, which every
// halt returns, a nil result included; serve's error stays errReturned.
func TestServeRunReturns(t *testing.T) {
	for _, want := range []error{errModelRun, nil} {
		e := modelServer(t, stalled(t))
		begin := time.Now()
		err := e.serve(func(context.Context) error { return want }, 10*time.Minute)
		if !errors.Is(err, errReturned) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("serve = %v; want errReturned", err)
		}
		if d := time.Since(begin); d > 20*time.Second {
			t.Fatalf("serve took %v to see run return", d)
		}
		if !e.ran || e.runErr != want {
			t.Fatalf("serve kept %v, %v; want %v", e.ran, e.runErr, want)
		}
		for i := range 2 {
			if err := e.halt(); err != want {
				t.Fatalf("halt %d = %v, want %v", i, err, want)
			}
		}
	}
}

type readyServer struct {
	skyv1connect.UnimplementedControlServiceHandler
}

func (readyServer) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	return connect.NewResponse(&v1.GetServerInfoResponse{}), nil
}

// TestServeReady: the twin of the cases above. A server that answers is
// ready: serve returns nil without having received run's result, and halt
// stops and joins run, whose clean result it returns.
func TestServeReady(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	e := modelServer(t, ln.Addr().String())
	mux := http.NewServeMux()
	mux.Handle(skyv1connect.NewControlServiceHandler(readyServer{}))
	err = e.serve(func(ctx context.Context) error {
		srv := &http.Server{Handler: mux}
		served := make(chan error, 1)
		go func() { served <- srv.Serve(ln) }()
		<-ctx.Done()
		err := srv.Close()
		if serr := <-served; !errors.Is(serr, http.ErrServerClosed) {
			err = errors.Join(err, serr)
		}
		return err
	}, 20*time.Second)
	if err != nil || e.ran {
		t.Fatalf("serve = %v, received run's result %v", err, e.ran)
	}
	if err := e.halt(); err != nil || !e.ran {
		t.Fatalf("halt = %v, joined %v", err, e.ran)
	}
}
