//go:build darwin || linux

package remotehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// These tests run the actual helper stage and collect paths over real TLS
// against rpc.NewHandler with the original Control, Store, Files, auth engine
// and Run. The scoped tokens are the plaintext the original data worker handed
// to StartData. Only the native creation and stopped-main observations below
// are MODELED; no Pod, Secret, collector or removal receipt is implied.
//
// Each Busy is legitimate: a concurrent generated call with the SAME scoped
// token holds its original response at the final Flush, so Control refuses the
// helper's call on the occupied input reader or output upload. Releasing that
// same owner, and only that, makes the helper's unchanged retry succeed.
type busyBackend struct {
	collect  bool
	starts   atomic.Int32
	removes  atomic.Int32
	started  chan busyStart
	observed chan struct{}
	mu       sync.Mutex
	began    time.Time
}

type busyStart struct {
	key           store.RemoteKey
	input, output string
}

func (*busyBackend) Check(backend.Task) error { return nil }
func (*busyBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected start")
}
func (*busyBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*busyBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (*busyBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func (b *busyBackend) StartData(_ context.Context, start backend.DataStart) (backend.DataCreated, error) {
	b.starts.Add(1)
	b.mu.Lock()
	b.began = time.Now().UTC()
	b.mu.Unlock()
	// Copy before return: Control clears the original slices after integration.
	b.started <- busyStart{store.RemoteKey{JobID: start.JobID, AttemptID: start.Execution.ID}, string(start.InputToken), string(start.OutputToken)}
	id := start.Execution.ID
	created := backend.DataCreated{Instance: "modeled-pod-" + id, Pod: backend.DataResource{Name: "modeled-pod", UID: "modeled-pod-" + id}, InputSecret: backend.DataResource{Name: "modeled-input", UID: "modeled-input-" + id}}
	if len(start.OutputToken) != 0 {
		created.OutputSecret = backend.DataResource{Name: "modeled-output", UID: "modeled-output-" + id}
	}
	return created, nil
}

func (b *busyBackend) ObserveData(_ context.Context, ref backend.Reference) (backend.DataObservation, error) {
	select {
	case b.observed <- struct{}{}:
	default:
	}
	if !b.collect {
		return backend.DataObservation{Status: backend.Status{State: backend.Starting, Instance: ref.Instance}, Phase: backend.DataStage}, nil
	}
	b.mu.Lock()
	began := b.began
	b.mu.Unlock()
	zero := int32(0)
	stop := &backend.DataTaskStop{
		Stage: &backend.ContainerExit{ContainerID: "modeled-stage", StartedAt: began, FinishedAt: began},
		Task:  backend.ContainerExit{ContainerID: "modeled-main", StartedAt: began, FinishedAt: began},
	}
	return backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: ref.Instance, ExitCode: &zero}, TaskStop: stop, Phase: backend.DataCollect}, nil
}

func (b *busyBackend) RemoveData(context.Context, backend.DataResources) (backend.DataRemoval, error) {
	b.removes.Add(1)
	return backend.DataRemoval{}, errors.New("unexpected native removal")
}

// busyBarrier holds one original response at its final Flush, after the
// generated service returned and before Control retires the call.
type busyBarrier struct {
	http.ResponseWriter
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *busyBarrier) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *busyBarrier) FlushError() error {
	w.once.Do(func() { close(w.entered); <-w.release })
	return http.NewResponseController(w.ResponseWriter).Flush()
}

type busyFixture struct {
	t        *testing.T
	paths    paths
	backend  *busyBackend
	store    *store.Store
	files    *artifact.Files
	root     *os.Root
	control  *control.Control
	server   *httptest.Server
	origin   string
	ca       string
	key      store.RemoteKey
	input    string
	output   string
	ref      artifact.Ref
	archive  []byte
	parkPath string
	park     atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	unblock  func()

	// Every original the fixture starts is recorded here BEFORE it is
	// launched, so each exit path, including an early Fatal, can retire it.
	runCancel, authCancel context.CancelFunc
	runDone               chan error
	authDone              chan struct{}
	owners                []*busyOwner
	permits               []*control.ArtifactCall
	clients               []*client
	observers             []*busyTransport
	kept                  []busyKept
}

// busyKept is a test-local original whose Close was not positively proven.
type busyKept struct {
	original any
	err      error
}

// busyOwner is one original goroutine started by a test: its own cancel and
// result channel.
type busyOwner struct {
	name   string
	cancel context.CancelFunc
	done   chan error
	joined bool
	err    error
}

// heldBusyFixtures keeps every fixture whose originals were not positively
// retired reachable for the life of the test process, with nothing closed.
var heldBusyFixtures struct {
	sync.Mutex
	fixtures []*busyFixture
}

// newBusyFixture publishes a real input through the Control RPCs, submits a
// real data job and waits for the original Run to start it. With outputs, the
// modeled backend reports a successful stopped main, so the actual Store
// observation releases the output scope for collection.
func newBusyFixture(t *testing.T, outputs bool, parkPath string) *busyFixture {
	t.Helper()
	f := &busyFixture{t: t, paths: fixturePaths(t), parkPath: parkPath, entered: make(chan struct{}), release: make(chan struct{})}
	f.unblock = sync.OnceFunc(func() { close(f.release) })
	f.backend = &busyBackend{collect: outputs, started: make(chan busyStart, 1), observed: make(chan struct{}, 1)}
	dir := filepath.Join(filepath.Dir(f.paths.workspace), "service")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	// Registered after fixturePaths, so this runs first and the case directory
	// is removed only after every original owner was positively retired.
	t.Cleanup(f.settle)
	if err := os.Mkdir(filepath.Join(dir, "artifacts"), 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(filepath.Join(dir, "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	f.root = root
	info, err := root.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("actual root identity unavailable")
	}
	identity := artifact.FileID{Device: uint64(physical.Dev), Inode: uint64(physical.Ino)}
	facts, err := artifact.Initialize(root, identity, strings.Repeat("a", 32))
	if err != nil {
		t.Fatal(err)
	}
	if f.files, err = artifact.Open(root, identity); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	if _, err := store.InstallArtifacts(state, store.ArtifactInstallation{Enabled: true, ID: facts.ID, Unit: facts.Unit, Volume: facts.Volume}); err != nil {
		t.Fatal(err)
	}
	if f.store, err = store.Open(state); err != nil {
		t.Fatal(err)
	}
	authFile := filepath.Join(dir, "auth.json")
	if _, err := auth.Initialize(authFile, filepath.Join(dir, "legacy.token"), f.store.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(authFile, "alice", auth.Operator); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.Issue(authFile, "alice", filepath.Join(dir, "alice.token"), time.Hour); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "alice.token"))
	if err != nil {
		t.Fatal(err)
	}
	alice := strings.TrimSuffix(string(data), "\n")
	if err := f.store.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	engine, err := auth.NewFile(authFile, f.store.Owner())
	if err != nil {
		t.Fatal(err)
	}
	authCtx, stopAuth := context.WithCancel(context.Background())
	f.authCancel, f.authDone = stopAuth, make(chan struct{})
	go func() { defer close(f.authDone); engine.Run(authCtx) }()
	policy := backend.DefaultDataPolicy()
	target := backend.Target{Name: "test", Binding: "test-binding", Kind: "kubernetes", Backend: f.backend, OutputStorage: backend.RemoteOutput, DataPolicy: &policy, SupportsRemoteData: true}
	if f.control, err = control.NewWithConfig(f.store, []backend.Target{target}, engine, control.Config{Artifacts: f.files}); err != nil {
		t.Fatal(err)
	}
	original := rpc.NewHandler(f.control, engine)
	f.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == f.parkPath && f.park.CompareAndSwap(true, false) {
			w = &busyBarrier{ResponseWriter: w, entered: f.entered, release: f.release}
		}
		original.ServeHTTP(w, r)
	}))
	f.ca = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.server.Certificate().Raw}))
	// The httptest certificate covers example.com; dialing stays on this server.
	f.origin = "https://example.com:" + strings.TrimPrefix(f.server.URL, "https://127.0.0.1:")
	runCtx, stopRun := context.WithCancel(context.Background())
	f.runCancel, f.runDone = stopRun, make(chan error, 1)
	go func() { f.runDone <- f.control.Run(runCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cc := skyv1connect.NewControlServiceClient(f.server.Client(), f.server.URL)
	f.archive = testArchive(t)
	digest := sha256.Sum256(f.archive)
	begun, err := busyAdmitted(ctx, func() (*connect.Response[skyv1.BeginArtifactUploadResponse], error) {
		return cc.BeginArtifactUpload(ctx, busyAuthorized(alice, &skyv1.BeginArtifactUploadRequest{Sha256: hex.EncodeToString(digest[:]), Size: int64(len(f.archive))}))
	})
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(f.archive); {
		end := min(offset+(64<<10), len(f.archive))
		if _, err := busyAdmitted(ctx, func() (*connect.Response[skyv1.WriteArtifactChunkResponse], error) {
			return cc.WriteArtifactChunk(ctx, busyAuthorized(alice, &skyv1.WriteArtifactChunkRequest{Id: begun.Msg.Upload.Id, Offset: int64(offset), Data: f.archive[offset:end]}))
		}); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	finalized, err := busyAdmitted(ctx, func() (*connect.Response[skyv1.FinalizeArtifactUploadResponse], error) {
		return cc.FinalizeArtifactUpload(ctx, busyAuthorized(alice, &skyv1.FinalizeArtifactUploadRequest{Id: begun.Msg.Upload.Id}))
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := finalized.Msg.Reference
	f.ref = artifact.Ref{ID: ref.Id, SHA256: ref.Sha256, Size: ref.Size}
	task := &skyv1.Task{Argv: []string{"reader"}, Image: "modeled-image", Target: "test", Workdir: ref}
	if outputs {
		task.Outputs = []string{"result"}
	}
	accepted, err := busyAdmitted(ctx, func() (*connect.Response[skyv1.SubmitDataJobResponse], error) {
		return cc.SubmitDataJob(ctx, busyAuthorized(alice, &skyv1.SubmitDataJobRequest{RequestId: "helper-busy", Task: task}))
	})
	if err != nil {
		t.Fatal(err)
	}
	var start busyStart
	select {
	case start = <-f.backend.started:
	case <-ctx.Done():
		t.Fatal("original Run did not start the accepted data job")
	}
	if start.key.JobID != accepted.Msg.Job.Id || len(start.input) != 114 || outputs != (len(start.output) == 114) {
		t.Fatal("StartData received a different job or noncanonical tokens")
	}
	// The second call proves the first observation was integrated.
	for range 2 {
		select {
		case <-f.backend.observed:
		case <-ctx.Done():
			t.Fatal("original Run did not record the modeled creation")
		}
	}
	f.key, f.input, f.output = start.key, start.input, start.output
	return f
}

// busyAdmitted repeats a setup call only while it is refused for capacity:
// before any handler ran (429/503) or by the physical growth guard. The
// caller's deadline bounds it; the original sampler is slower under load.
func busyAdmitted[T any](ctx context.Context, call func() (*connect.Response[T], error)) (*connect.Response[T], error) {
	for {
		out, err := call()
		code := connect.CodeOf(err)
		if code != connect.CodeResourceExhausted && code != connect.CodeUnavailable {
			return out, err
		}
		if err := pause(ctx, 250*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func busyAuthorized[T any](token string, msg *T) *connect.Request[T] {
	r := connect.NewRequest(msg)
	r.Header().Set("Authorization", "Bearer "+token)
	return r
}

// client returns a production helper client for a scoped token. Only DNS
// resolution is mapped to the owned TLS server; verification is unchanged.
func (f *busyFixture) client(token string) *client {
	f.t.Helper()
	c, err := newClient(f.origin, f.ca, token)
	if err != nil {
		f.t.Fatal(err)
	}
	c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, network, f.server.Listener.Addr().String())
	}
	f.clients = append(f.clients, c)
	f.t.Cleanup(c.Close)
	return c
}

// launch records an original owner on the fixture, then starts it.
func (f *busyFixture) launch(name string, cancel context.CancelFunc, run func() error) *busyOwner {
	o := &busyOwner{name: name, cancel: cancel, done: make(chan error, 1)}
	f.owners = append(f.owners, o)
	go func() { o.done <- run() }()
	return o
}

// wait reports whether the owner returned within d, recording its original
// result. A deadline is never a join.
func (o *busyOwner) wait(d time.Duration) bool {
	if o.joined {
		return true
	}
	select {
	case o.err = <-o.done:
		o.joined = true
	case <-time.After(d):
	}
	return o.joined
}

// hold starts the controlled owner: one generated call that parks at its final
// Flush. It returns after the owner is parked, holding its original call.
func (f *busyFixture) hold(call func(context.Context) error) *busyOwner {
	f.t.Helper()
	f.park.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	o := f.launch("controlled owner", cancel, func() error {
		defer cancel()
		return call(ctx)
	})
	select {
	case <-f.entered:
	case <-time.After(10 * time.Second):
		f.t.Fatal("controlled owner did not reach its final Flush")
	}
	return o
}

// join releases the parked controlled owner and returns its original result.
func (f *busyFixture) join(o *busyOwner) error {
	f.t.Helper()
	f.unblock()
	return f.wait(o)
}

// wait returns an owner's original result. On timeout the owner stays on the
// fixture, which settle then retains.
func (f *busyFixture) wait(o *busyOwner) error {
	f.t.Helper()
	if !o.wait(60 * time.Second) {
		f.t.Fatalf("%s did not return", o.name)
	}
	return o.err
}

// holdCalls acquires common artifact permits until n are held. The original
// Run's sampler takes short owners from the same four, so a refusal waits.
// Each permit is recorded on the fixture as soon as it is held.
func (f *busyFixture) holdCalls(n int) {
	f.t.Helper()
	for attempt := 0; len(f.permits) < n; attempt++ {
		call, err := f.control.TryArtifactCall()
		if err == nil {
			f.permits = append(f.permits, call)
			continue
		}
		if !errors.Is(err, control.ErrArtifactBusy) || attempt == 80 {
			f.t.Fatalf("hold %d common permits: %v", n, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// releaseCalls returns every permit held by holdCalls.
func (f *busyFixture) releaseCalls() {
	for _, call := range f.permits {
		call.Done()
	}
	f.permits = nil
}

// idle waits until all four common permits are free: no earlier call remains.
func (f *busyFixture) idle() {
	f.t.Helper()
	f.holdCalls(4)
	f.releaseCalls()
}

func (f *busyFixture) attempt() store.RemoteAttempt {
	f.t.Helper()
	row, err := f.store.GetRemoteAttempt(f.key)
	if err != nil {
		f.t.Fatal(err)
	}
	if row.Active == nil || row.Active.Input == nil {
		f.t.Fatal("remote attempt lost its active scope")
	}
	return row
}

func (f *busyFixture) upload() store.ArtifactUpload {
	f.t.Helper()
	row := f.attempt()
	if row.Active.Output == nil {
		f.t.Fatal("remote attempt has no output scope")
	}
	grant, err := f.store.CheckRemoteGrant(f.key.AttemptID, store.RemoteOutput, row.Active.Output.Hash, time.Now().UTC())
	if err != nil {
		f.t.Fatal(err)
	}
	u, _, _, err := f.store.GetRemoteOutput(grant, time.Now().UTC())
	if err != nil {
		f.t.Fatal(err)
	}
	return u
}

// settle retires the fixture in dependency order: the owners it launched,
// the production shutdown, the server, Run, the auth engine, then Files, root
// and Store. Each step runs only after every earlier one was proven. On the
// first failure or timeout the fixture is retained, with its namespace, and
// nothing further is closed.
func (f *busyFixture) settle() {
	if err := f.retire(); err != nil {
		heldBusyFixtures.Lock()
		heldBusyFixtures.fixtures = append(heldBusyFixtures.fixtures, f)
		heldBusyFixtures.Unlock()
		f.t.Errorf("retaining busy fixture: %v", err)
		return
	}
	if f.backend.starts.Load() > 1 || f.backend.removes.Load() != 0 {
		f.t.Errorf("native calls starts=%d removes=%d", f.backend.starts.Load(), f.backend.removes.Load())
	}
}

func (f *busyFixture) retire() error {
	f.unblock()
	f.releaseCalls()
	for _, o := range f.owners {
		o.cancel()
	}
	for _, o := range f.owners {
		if !o.wait(10 * time.Second) {
			return fmt.Errorf("%s did not return", o.name)
		}
	}
	if len(f.kept) != 0 {
		return f.kept[0].err
	}
	if err := f.receipts(); err != nil {
		return err
	}
	if f.control != nil {
		if err := f.control.BeginShutdown(); err != nil {
			return fmt.Errorf("begin shutdown: %w", err)
		}
		prepared, stop := context.WithTimeout(context.Background(), 15*time.Second)
		err := f.control.WaitShutdownPrepared(prepared)
		stop()
		if err != nil {
			return fmt.Errorf("wait shutdown prepared: %w", err)
		}
		drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
		err = f.control.DrainArtifacts(drain)
		stop()
		if err != nil {
			return fmt.Errorf("drain artifacts: %w", err)
		}
	}
	if f.server != nil && !returns(10*time.Second, f.server.Close) {
		return errors.New("server Close did not return")
	}
	if f.runCancel != nil {
		f.runCancel()
		select {
		case err := <-f.runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("original Run: %w", err)
			}
		case <-time.After(10 * time.Second):
			return errors.New("original Run did not return")
		}
		join, stop := context.WithTimeout(context.Background(), 10*time.Second)
		err := f.control.Join(join)
		stop()
		if err != nil {
			return fmt.Errorf("join: %w", err)
		}
	}
	if f.authCancel != nil {
		f.authCancel()
		select {
		case <-f.authDone:
		case <-time.After(10 * time.Second):
			return errors.New("auth engine did not return")
		}
	}
	if f.files != nil {
		if err := f.files.Close(); err != nil {
			return fmt.Errorf("close files: %w", err)
		}
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			return fmt.Errorf("close artifact root: %w", err)
		}
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			return fmt.Errorf("close store: %w", err)
		}
	}
	return nil
}

// receipts proves, after every owner returned, that each observed Busy body's
// original was closed exactly once without error and that no helper client
// holds a sticky error, except a vetoed client holding its labeled oracle.
func (f *busyFixture) receipts() error {
	vetoes := map[*client]error{}
	for _, b := range f.observers {
		b.mu.Lock()
		bodies := b.bodies
		b.mu.Unlock()
		for _, body := range bodies {
			if calls, err := body.receipt(); calls != 1 || err != nil {
				return fmt.Errorf("busy body original Close calls=%d: %v", calls, err)
			}
		}
		if b.veto != nil {
			vetoes[b.client] = b.veto
		}
	}
	for _, c := range f.clients {
		err := c.takeError()
		var retained *bodyCloseError
		if veto := vetoes[c]; veto != nil && errors.As(err, &retained) && errors.Is(err, veto) {
			continue
		}
		if err != nil {
			return fmt.Errorf("helper client retains: %w", err)
		}
	}
	return nil
}

// returns reports whether fn returned within d. On timeout the original call
// keeps running; it is not repeated.
func returns(d time.Duration, fn func()) bool {
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// busyTransport observes the helper's own requests on the unchanged
// production transport. Each Busy response body records its real original
// Close receipt and reports it on closed; with a veto, the first one then also
// reports the labeled policy fault. The veto is a policy oracle injected AFTER
// the real original Close, not a native fault claim.
type busyTransport struct {
	next   http.RoundTripper
	client *client
	veto   error
	busy   chan string
	closed chan string
	mu     sync.Mutex
	calls  []busyCall
	bodies []*busyBody
	body   *busyBody // the vetoed body
}

// heldBusyTransports keeps each observed client, with every response and body
// it acquires, reachable for the life of the test process. A frame is entered
// before its first request, and released only when its case passed with no
// veto and every original Close succeeded. The keeper is bounded; a case
// refuses to start rather than grow it further.
var heldBusyTransports struct {
	sync.Mutex
	transports []*busyTransport
}

type busyCall struct {
	path   string
	status int
	wire   bool // a Connect error body rather than a pre-handler refusal
}

func (b *busyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := b.next.RoundTrip(r)
	if err != nil {
		return response, err
	}
	busy := response.StatusCode == http.StatusTooManyRequests
	b.mu.Lock()
	b.calls = append(b.calls, busyCall{r.URL.Path, response.StatusCode, response.Header.Get("Content-Type") == "application/json"})
	if busy {
		body := &busyBody{response: response, original: response.Body, path: r.URL.Path, closed: b.closed}
		if b.veto != nil && b.body == nil {
			body.veto = b.veto
			b.body = body
		}
		b.bodies = append(b.bodies, body)
		response.Body = body
	}
	b.mu.Unlock()
	if busy {
		select {
		case b.busy <- r.URL.Path:
		default:
		}
	}
	return response, nil
}

func (b *busyTransport) log(path string) []busyCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []busyCall
	for _, c := range b.calls {
		if c.path == path {
			out = append(out, c)
		}
	}
	return out
}

// clean reports whether no body was vetoed and every Busy body's original
// was closed exactly once without error, leaving the client no sticky error.
func (b *busyTransport) clean() bool {
	b.mu.Lock()
	bodies := b.bodies
	b.mu.Unlock()
	for _, body := range bodies {
		if calls, err := body.receipt(); body.veto != nil || calls != 1 || err != nil {
			return false
		}
	}
	return b.client.takeError() == nil
}

type busyBody struct {
	response *http.Response
	original io.ReadCloser
	path     string
	veto     error
	closed   chan string

	mu            sync.Mutex
	closes        int
	originalCalls int
	originalErr   error // the real original Close receipt
	returned      error
}

func (b *busyBody) Read(p []byte) (int, error) { return b.original.Read(p) }
func (b *busyBody) Close() error {
	b.mu.Lock()
	b.closes++
	if b.originalCalls == 0 {
		b.originalCalls++
		b.originalErr = b.original.Close()
		b.returned = errors.Join(b.originalErr, b.veto)
	}
	err := b.returned
	b.mu.Unlock()
	select {
	case b.closed <- b.path:
	default:
	}
	return err
}

// receipt returns the real original Close calls and result.
func (b *busyBody) receipt() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.originalCalls, b.originalErr
}

// vetoed checks the vetoed body's frame: one wrapper Close, one clean real
// original Close, and the retained production body and cause.
func (b *busyTransport) vetoed(t *testing.T, err error) {
	t.Helper()
	b.mu.Lock()
	body := b.body
	b.mu.Unlock()
	if body == nil {
		t.Fatal("no vetoed Busy body")
	}
	body.mu.Lock()
	closes, originalCalls, originalErr := body.closes, body.originalCalls, body.originalErr
	body.mu.Unlock()
	t.Logf("original Close receipt: calls=%d err=%v", originalCalls, originalErr)
	if closes != 1 || originalCalls != 1 || originalErr != nil {
		t.Fatalf("vetoed Busy body Close: wrapper=%d original=%d err=%v", closes, originalCalls, originalErr)
	}
	var retained *bodyCloseError
	production, ok := body.response.Body.(*transportBody)
	if !errors.As(err, &retained) || !ok || retained.body != production.original || err != b.client.takeError() || !errors.Is(err, b.veto) {
		t.Fatalf("Close veto frame not retained: %v", err)
	}
}

func (f *busyFixture) observe(c *client, veto error) *busyTransport {
	f.t.Helper()
	b := &busyTransport{next: c.transport, client: c, veto: veto, busy: make(chan string, 64), closed: make(chan string, 64)}
	heldBusyTransports.Lock()
	if len(heldBusyTransports.transports) >= 16 {
		heldBusyTransports.Unlock()
		f.t.Fatal("busy transport keeper full")
	}
	heldBusyTransports.transports = append(heldBusyTransports.transports, b)
	heldBusyTransports.Unlock()
	f.observers = append(f.observers, b)
	f.t.Cleanup(func() {
		if f.t.Failed() || !b.clean() {
			return
		}
		heldBusyTransports.Lock()
		defer heldBusyTransports.Unlock()
		held := heldBusyTransports.transports
		for i := range held {
			if held[i] == b {
				heldBusyTransports.transports = append(held[:i], held[i+1:]...)
				break
			}
		}
	})
	c.httpClient.Transport = b
	return b
}

// waitBusy waits for a Busy on path. A Busy on another route, such as a
// pre-handler refusal of PollRelease while the sampler holds a permit, is
// skipped; the call logs still record it.
func waitBusy(t *testing.T, b *busyTransport, path string) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case got := <-b.busy:
			if got == path {
				return
			}
		case <-timeout:
			b.mu.Lock()
			defer b.mu.Unlock()
			t.Fatalf("helper saw no Busy on %s: %+v", path, b.calls)
		}
	}
}

// cancelInPause cancels only after the helper's client closed the decoded Busy
// response, so the helper is past that call and in (or entering) its pause.
// It returns the helper's result and how long it took to stop.
func cancelInPause(t *testing.T, b *busyTransport, path string, helper *busyOwner) (time.Duration, error) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for closed := false; !closed; {
		select {
		case got := <-b.closed:
			closed = got == path
		case <-timeout:
			t.Fatalf("helper did not close the Busy response on %s", path)
		}
	}
	start := time.Now()
	helper.cancel()
	if !helper.wait(10 * time.Second) {
		t.Fatal("canceled helper did not return")
	}
	return time.Since(start), helper.err
}

// checkBusy reports whether every logged call is a server-sent Busy.
func checkBusy(calls []busyCall, n int) bool {
	if len(calls) != n {
		return false
	}
	for _, c := range calls {
		if c.status != http.StatusTooManyRequests || !c.wire {
			return false
		}
	}
	return true
}

func TestStageBusyInputReader(t *testing.T) {
	read := skyv1connect.RemoteDataServiceReadInputProcedure
	for _, mode := range []string{"release", "persistent", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			f := newBusyFixture(t, false, read)
			owner := f.client(f.input)
			done := f.hold(func(ctx context.Context) error {
				_, err := owner.remote.ReadInput(ctx, connect.NewRequest(&skyv1.ReadInputRequest{Offset: 0, Limit: 64 << 10}))
				return err
			})
			c := f.client(f.input)
			var veto error
			if mode == "close" {
				veto = errors.New("labeled policy fault after real response Close")
			}
			observed := f.observe(c, veto)
			self, err := os.ReadFile(f.paths.self)
			if err != nil {
				t.Fatal(err)
			}
			selfSHA := sha256.Sum256(self)
			env := map[string]string{"SKYGO_DATA_INPUT_ID": f.ref.ID, "SKYGO_DATA_INPUT_SHA256": f.ref.SHA256, "SKYGO_DATA_INPUT_SIZE": fmt.Sprint(f.ref.Size), "SKYGO_DATA_HELPER_SHA256": hex.EncodeToString(selfSHA[:])}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			helper := f.launch("stage", cancel, func() error { return stage(ctx, c, env, f.paths) })
			waitBusy(t, observed, read)
			switch mode {
			case "release":
				if err := f.join(done); err != nil {
					t.Fatalf("controlled owner: %v", err)
				}
			}
			var stopped time.Duration
			if mode == "cancel" {
				stopped, err = cancelInPause(t, observed, read, helper)
			} else {
				err = f.wait(helper)
			}
			calls := observed.log(read)
			switch mode {
			case "release":
				if err != nil {
					t.Fatal(err)
				}
				got, err := os.ReadFile(filepath.Join(f.paths.workspace, "work", "input.txt"))
				if err != nil || !bytes.Equal(got, bytes.Repeat([]byte("content"), 12000)) {
					t.Fatalf("materialized bytes: %v", err)
				}
				if len(calls) != 3 || !checkBusy(calls[:1], 1) || calls[1].status != http.StatusOK || calls[2].status != http.StatusOK {
					t.Fatalf("helper reads: %+v", calls)
				}
			case "persistent":
				if connect.CodeOf(err) != connect.CodeResourceExhausted || !connect.IsWireError(err) || !checkBusy(calls, 3) {
					t.Fatalf("persistent Busy not bounded: %v %+v", err, calls)
				}
			case "cancel":
				// The cancellable 2s pause stops at once; no second attempt.
				if !errors.Is(err, context.Canceled) || stopped >= time.Second || !checkBusy(calls, 1) {
					t.Fatalf("canceled Busy wait: %v after %v %+v", err, stopped, calls)
				}
			case "close":
				observed.vetoed(t, err)
				if !checkBusy(calls, 1) {
					t.Fatalf("Close veto did not stop before retry: %v %+v", err, calls)
				}
			}
			if mode != "release" {
				if _, err := os.Lstat(filepath.Join(f.paths.workspace, "work")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("published input after a stopped stage")
				}
				if err := f.join(done); err != nil {
					t.Fatalf("controlled owner: %v", err)
				}
			}
		})
	}
}

func TestCollectBusyOutputUpload(t *testing.T) {
	poll := skyv1connect.RemoteDataServicePollReleaseProcedure
	begin := skyv1connect.RemoteDataServiceBeginOutputProcedure
	for _, mode := range []string{"release", "persistent", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			f := newBusyFixture(t, true, begin)
			work := filepath.Join(f.paths.workspace, "work")
			if err := os.MkdirAll(filepath.Join(work, "result"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(work, "result", "output.txt"), bytes.Repeat([]byte("actual scoped output\n"), 3500), 0600); err != nil {
				t.Fatal(err)
			}
			// The helper's own archive identity, computed the same way collect does.
			root, err := os.OpenRoot(work)
			if err != nil {
				t.Fatal(err)
			}
			var archive boundedArchive
			err = artifact.Collect(root, []string{"result"}, &archive)
			if closeErr := root.Close(); closeErr != nil {
				f.kept = append(f.kept, busyKept{root, fmt.Errorf("close collection root: %w", closeErr)})
				t.Fatal(closeErr)
			}
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(archive.Bytes())
			sha, size := hex.EncodeToString(digest[:]), int64(archive.Len())

			// The controlled owner creates the original part, then parks an
			// identical Begin replay that holds the selected upload.
			owner := f.client(f.output)
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			if _, err := busyAdmitted(ctx, func() (*connect.Response[skyv1.BeginOutputResponse], error) {
				return owner.remote.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: sha, Size: size}))
			}); err != nil {
				t.Fatal(err)
			}
			f.idle()
			done := f.hold(func(ctx context.Context) error {
				_, err := owner.remote.BeginOutput(ctx, connect.NewRequest(&skyv1.BeginOutputRequest{Sha256: sha, Size: size}))
				return err
			})
			if mode == "release" {
				// The parked owner has one common permit; hold the other three.
				f.holdCalls(3)
			}
			c := f.client(f.output)
			var veto error
			if mode == "close" {
				veto = errors.New("labeled policy fault after real response Close")
			}
			observed := f.observe(c, veto)
			helper := f.launch("collect", cancel, func() error { return collect(ctx, c, []string{"result"}, f.paths) })
			if mode == "release" {
				waitBusy(t, observed, poll)
				f.releaseCalls()
			}
			waitBusy(t, observed, begin)
			switch mode {
			case "release":
				if err := f.join(done); err != nil {
					t.Fatalf("controlled owner: %v", err)
				}
			}
			var stopped time.Duration
			if mode == "cancel" {
				stopped, err = cancelInPause(t, observed, begin, helper)
			} else {
				err = f.wait(helper)
			}
			calls := observed.log(begin)
			switch mode {
			case "release":
				if err != nil {
					t.Fatal(err)
				}
				polls := observed.log(poll)
				if len(polls) != 2 || polls[0].status != http.StatusTooManyRequests || polls[0].wire || polls[1].status != http.StatusOK {
					t.Fatalf("helper polls: %+v", polls)
				}
				if len(calls) != 2 || !checkBusy(calls[:1], 1) || calls[1].status != http.StatusOK {
					t.Fatalf("helper begins: %+v", calls)
				}
				u := f.upload()
				if u.State != "complete" || u.Reference.SHA256 != sha || u.Reference.Size != uint64(size) || u.Acknowledged != uint64(size) {
					t.Fatalf("sealed output: %+v", u)
				}
			case "persistent":
				if connect.CodeOf(err) != connect.CodeResourceExhausted || !connect.IsWireError(err) || !checkBusy(calls, 3) {
					t.Fatalf("persistent Busy not bounded: %v %+v", err, calls)
				}
			case "cancel":
				// The cancellable 2s pause stops at once; no second attempt.
				if !errors.Is(err, context.Canceled) || stopped >= time.Second || !checkBusy(calls, 1) {
					t.Fatalf("canceled Busy wait: %v after %v %+v", err, stopped, calls)
				}
			case "close":
				observed.vetoed(t, err)
				if !checkBusy(calls, 1) {
					t.Fatalf("Close veto did not stop before retry: %v %+v", err, calls)
				}
			}
			if mode != "release" {
				if err := f.join(done); err != nil {
					t.Fatalf("controlled owner: %v", err)
				}
				if u := f.upload(); u.State != "writable" || u.Acknowledged != 0 {
					t.Fatalf("stopped collect wrote or sealed output: %+v", u)
				}
				if n := len(observed.log(skyv1connect.RemoteDataServiceWriteOutputProcedure)) + len(observed.log(skyv1connect.RemoteDataServiceFinishOutputProcedure)); n != 0 {
					t.Fatalf("stopped collect made %d write/finish calls", n)
				}
			}
		})
	}
}
