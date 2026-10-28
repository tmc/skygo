package kube

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Source-only; NOT_RUN. These tests close the integration that
// NewEnvironmentIntegration built, on its own Controller and runtime, while
// an exec or forward session of that Controller holds its original native
// connection Close. Startup reads go to profileAPI; after construction the
// runtime's typed client and Pod writer are pointed at environmentAPIModel,
// which MODELS the API server, and its transport at fakeAPI, which runs the
// real SPDY upgrade and streams over loopback. The kubelet's Running and
// Ready status is an explicit test edit. Nothing here contacts a cluster or
// qualifies native Kubernetes behavior.

// closeHarness is one Ready environment of an integration built by
// NewEnvironmentIntegration, with its original Controller and runtime.
type closeHarness struct {
	o     *closeOwner
	s     *store.Store
	token string
	actor auth.Actor
	in    *environmentIntegration
	ctl   *environmentcontrol.Controller
	model *environmentAPIModel
	api   *fakeAPI
	fc    *faultConn
	id    string
	pod   string

	// gate holds the original connection's Close; hold holds the modeled
	// server side. Each is closed once.
	gate, hold         chan struct{}
	gateOnce, holdOnce sync.Once
}

func (h *closeHarness) releaseGate() { h.gateOnce.Do(func() { close(h.gate) }) }
func (h *closeHarness) releaseHold() { h.holdOnce.Do(func() { close(h.hold) }) }

// release closes both; cleanup calls it unconditionally.
func (h *closeHarness) release() {
	h.releaseGate()
	h.releaseHold()
}

// closeBorrowed names the borrowed objects: the model's resource and name
// and the profileAPI path the integration read at startup.
var closeBorrowed = []struct{ resource, name, path string }{
	{"namespaces", "workloads", "/api/v1/namespaces/workloads"},
	{"serviceaccounts", "default", "/api/v1/namespaces/workloads/serviceaccounts/default"},
	{"persistentvolumeclaims", "alice-home", "/api/v1/namespaces/workloads/persistentvolumeclaims/alice-home"},
	{"persistentvolumes", "pv-alice-home", "/api/v1/persistentvolumes/pv-alice-home"},
}

// closeOwner owns, from the moment each is acquired, the private directory,
// the Store, the auth Engine and the integration of one close test. Its
// cleanup retires the Engine, the Store and the directory, in that order,
// only after the original integration's Close returned nil. Any other
// outcome of that Close, or an error closing the Store, is latched in held:
// nothing further is retired or retried, and the owner, with every original
// reference and the directory, is kept until the test process exits.
type closeOwner struct {
	dir     string
	s       *store.Store // nil while closed offline during setup
	engine  *auth.Engine
	stop    context.CancelFunc // stops engine; done closes when its Run returns
	done    chan struct{}
	in      *environmentIntegration // as returned by the factory, even with an error
	harness *closeHarness

	// The retirement operations. closeIn is in.Close; tests may wrap them
	// to observe or, for a known test sentinel only, to model a held Close.
	closeIn    func(context.Context) error
	closeStore func(*store.Store) error
	remove     func(string) error

	held       error // sticky: never cleared or replaced by fixture code
	joined     bool  // closeIn, or a test's own Close of in, returned nil
	retired    bool
	retainOnce sync.Once
}

// closeOwnerDirectory returns a new private directory under a persistent
// task-scoped root. Only its closeOwner removes it; testing.TempDir would
// remove it even while an original owner still holds the Store inside.
func closeOwnerDirectory(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "tmp", "skypilot", "20261028-s10-entrypoint-c3", "kube-close-fixtures")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(base)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatalf("%s must be a real directory with mode 0700", base)
	}
	dir, err := os.MkdirTemp(base, "integration-close-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// newCloseOwner acquires the private directory and registers the owner's
// cleanup before anything is created inside it.
func newCloseOwner(t *testing.T) *closeOwner {
	t.Helper()
	o := &closeOwner{dir: closeOwnerDirectory(t), closeStore: (*store.Store).Close, remove: os.RemoveAll}
	t.Cleanup(func() { o.cleanup(t) })
	return o
}

// open opens the Store at state and records it before returning. An Open
// error may carry a failed close of the database it opened, so it is held.
func (o *closeOwner) open(t *testing.T, state string) *store.Store {
	t.Helper()
	s, err := store.Open(state)
	if err != nil {
		o.held = fmt.Errorf("open Store: %w", err)
		t.Fatal(o.held)
	}
	o.s = s
	return s
}

// closeIntegration makes the one cleanup attempt to close the original
// integration, if there is one and it is not already joined. Only a nil
// result joins it; an error is held and never retried.
func (o *closeOwner) closeIntegration() error {
	if o.held != nil {
		return o.held
	}
	if o.in == nil || o.joined {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := o.closeIn(ctx); err != nil {
		o.held = fmt.Errorf("original integration Close: %w", err)
		return o.held
	}
	o.joined = true
	return nil
}

// retireJoined is the decision cleanup makes. It retires the Engine, the
// Store and the directory only after a positive join of the integration,
// and returns the held error, unchanged, otherwise.
func (o *closeOwner) retireJoined() error {
	if o.retired {
		return nil
	}
	if err := o.closeIntegration(); err != nil {
		return err
	}
	if o.stop != nil {
		o.stop()
		<-o.done
	}
	if o.s != nil {
		if err := o.closeStore(o.s); err != nil {
			o.held = fmt.Errorf("close Store: %w", err)
			return o.held
		}
	}
	if err := o.remove(o.dir); err != nil {
		o.held = fmt.Errorf("remove %s: %w", o.dir, err)
		return o.held
	}
	o.retired = true
	return nil
}

func (o *closeOwner) cleanup(t *testing.T) {
	t.Helper()
	if err := o.retireJoined(); err != nil {
		o.retain(t)
	}
}

// retain reports the held error and keeps this owner reachable until the
// test process exits, by one goroutine waiting on a channel that is never
// closed. It replaces no operation and records no settlement.
func (o *closeOwner) retain(t *testing.T) {
	t.Helper()
	o.retainOnce.Do(func() {
		t.Errorf("retaining original integration fixture at %s: %v", o.dir, o.held)
		processLifetime := make(chan struct{})
		go func(original *closeOwner) {
			<-processLifetime
			stdruntime.KeepAlive(original)
		}(o)
	})
}

// closeStore returns the owner of an open Store with scoped authentication,
// enabled retention and operator alice and of its running file Engine, and
// alice's token.
func closeStore(t *testing.T) (*closeOwner, string) {
	t.Helper()
	o := newCloseOwner(t)
	dir := o.dir
	state, config := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	s := o.open(t, state)
	if _, err := auth.Initialize(config, filepath.Join(dir, "admin.token"), s.Owner()); err != nil {
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(config, "alice", auth.Operator); err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(dir, "alice.token")
	if _, err := auth.Issue(config, "alice", tokenFile, time.Hour); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		o.held = fmt.Errorf("offline Store Close: %w", err)
		t.Fatal(o.held)
	}
	o.s = nil
	policy := store.DefaultRetentionPolicy()
	policy.LocalOutputEnabled = false
	// EnableRetention opens and closes its own Store; its error may be
	// that Close.
	if _, err := store.EnableRetention(state, policy); err != nil {
		o.held = fmt.Errorf("enable retention: %w", err)
		t.Fatal(o.held)
	}
	s = o.open(t, state)
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); engine.Run(ctx) }()
	o.engine, o.stop, o.done = engine, stop, done
	return o, strings.TrimSuffix(string(data), "\n")
}

// newCloseHarness builds the integration with the actual factory, points
// its original runtime's client, Pod writer and transport at the model and
// at a fakeAPI speaking protocol, and drives one environment of alice to
// Ready through the original Controller.
func newCloseHarness(t *testing.T, protocol string) *closeHarness {
	t.Helper()
	o, token := closeStore(t)
	actor, err := o.engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	profiles := newProfileAPI(t)
	api := newFakeAPI(t, protocol)
	h := &closeHarness{o: o, s: o.s, token: token, actor: actor, api: api,
		gate: make(chan struct{}), hold: make(chan struct{})}
	o.harness = h
	targets := profiles.targets(t)
	// Registered before the factory runs, so it covers an Integration the
	// factory returns with an error, and runs before the API servers'
	// cleanups and the owner's: it releases the modeled gates and then
	// makes the owner's one attempt to close the original integration.
	t.Cleanup(func() {
		h.release()
		o.closeIntegration()
	})
	got, err := NewEnvironmentIntegration(context.Background(), o.s, o.engine, targets, environmentTestConfig())
	if got != nil {
		in, ok := got.(*environmentIntegration)
		if !ok {
			o.held = fmt.Errorf("factory returned %T", got)
			t.Fatal(o.held)
		}
		o.in, o.closeIn = in, in.Close
	}
	if err != nil {
		t.Fatal(err)
	}
	if o.in == nil {
		t.Fatal("factory returned no Integration")
	}
	in := o.in
	h.in, h.ctl = in, in.EnvironmentController()
	if h.ctl == nil {
		t.Fatal("no Controller")
	}

	// The model starts with typed copies of the borrowed objects the
	// factory verified.
	h.model = newEnvironmentAPIModel(t)
	profiles.mu.Lock()
	for _, b := range closeBorrowed {
		obj := profiles.objs[b.path].(runtime.Object).DeepCopyObject()
		m, err := meta.Accessor(obj)
		if err != nil {
			profiles.mu.Unlock()
			t.Fatal(err)
		}
		h.model.mu.Lock()
		h.model.assign(m)
		h.model.objs[environmentAPIKey{b.resource, b.name}] = obj
		h.model.mu.Unlock()
	}
	profiles.mu.Unlock()

	pods := h.model.client.CoreV1().Pods("workloads")
	in.rt.b.client = h.model.client
	in.rt.pods = &funcPods{
		createFunc: func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			return pods.Create(ctx, pod, metav1.CreateOptions{})
		},
		updateFunc: func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			return pods.Update(ctx, pod, metav1.UpdateOptions{})
		},
		removeFunc: func(ctx context.Context, name string, opts metav1.DeleteOptions) error {
			return pods.Delete(ctx, name, opts)
		},
	}
	tr := api.transport(t)
	h.fc = &faultConn{closeGate: h.gate}
	tr.wrap = wrapWith(h.fc)
	// serve was bound to the original transport at construction.
	in.rt.port, in.rt.serve = tr, tr.servePort

	info, err := h.s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	v, err := h.ctl.Create(actor, info.Domain, "close-1", envRTSpec())
	if err != nil {
		t.Fatal(err)
	}
	h.id, h.pod = v.ID, "skygo-env-"+v.ID
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	var rerr error
	for range 4 {
		if h.model.get("pods", h.pod) != nil {
			break
		}
		rerr = h.ctl.Reconcile(ctx)
	}
	if h.model.get("pods", h.pod) == nil {
		t.Fatalf("no Pod created by Reconcile: %v", rerr)
	}
	// MODELED kubelet: the Pod is bound and its container running and ready.
	h.model.edit("pods", h.pod, func(o runtime.Object) {
		p := o.(*corev1.Pod)
		p.Spec.NodeName = "node1"
		p.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: environmentContainer, Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
	})
	for range 4 {
		rerr = h.ctl.Reconcile(ctx)
		if v, err = h.ctl.Get(actor, h.id); err != nil {
			t.Fatal(err)
		}
		if v.State.Phase == environment.Ready {
			return h
		}
	}
	t.Fatalf("environment phase = %v, want Ready (%v)", v.State.Phase, rerr)
	return nil
}

// borrowed returns copies of the model's borrowed objects.
func (h *closeHarness) borrowed(t *testing.T) []runtime.Object {
	t.Helper()
	var out []runtime.Object
	for _, b := range closeBorrowed {
		o := h.model.get(b.resource, b.name)
		if o == nil {
			t.Fatalf("borrowed %s %s missing", b.resource, b.name)
		}
		out = append(out, o)
	}
	return out
}

func (h *closeHarness) callCount() int {
	h.model.mu.Lock()
	defer h.model.mu.Unlock()
	return len(h.model.calls)
}

// writesSince returns the model calls after the first n that were not
// reads.
func (h *closeHarness) writesSince(n int) []environmentAPICall {
	h.model.mu.Lock()
	defer h.model.mu.Unlock()
	var out []environmentAPICall
	for _, c := range h.model.calls[n:] {
		if c.verb != "get" && c.verb != "list" {
			out = append(out, c)
		}
	}
	return out
}

// closeHeld closes the integration while the session's original native
// Close is held, then releases it. It checks that Close fails while held and
// succeeds once released, that the Store stays usable throughout, that
// nothing owned or borrowed was written, and that the stopped Controller
// refuses new sessions with no provider request.
func (h *closeHarness) closeHeld(t *testing.T) {
	t.Helper()
	before := h.borrowed(t)
	pod := h.model.get("pods", h.pod).(*corev1.Pod)
	calls := h.callCount()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := h.in.Close(ctx)
	cancel()
	if err == nil {
		// A nil Close while the original Close is known to be held
		// contradicts it, so it is no join: cleanup releases the gates
		// but neither retries Close nor retires anything.
		h.o.held = errors.New("integration Close = nil while the original connection Close is held")
		t.Fatal("integration Close = nil while the original connection Close is held")
	}
	eventually(t, "the held connection Close", func() bool { c, _ := h.fc.counts(); return c == 1 })
	if n := h.in.rt.activeFrames(); n != 1 {
		t.Fatalf("%d runtime frames while held, want the original 1", n)
	}
	if _, err := h.ctl.Get(h.actor, h.id); err != nil {
		t.Fatalf("Store unusable while held: %v", err)
	}
	if _, err := h.s.RetentionInfo(h.actor); err != nil {
		t.Fatalf("Store unusable while held: %v", err)
	}

	// Release only the original Close, and wait until the original SPDY
	// connection reports itself closed (its spdystream read loop ended)
	// before the modeled server may write anything more, so that no exit
	// status or response byte can arrive ahead of the shutdown.
	h.releaseGate()
	receive(t, "original connection shutdown", h.fc.Connection.CloseChan())
	h.releaseHold()
	ctx, cancel = context.WithTimeout(context.Background(), testWait)
	err = h.in.Close(ctx)
	cancel()
	if err != nil {
		// Held before Fatal: cleanup neither retries this Close nor
		// retires what the original integration may still use.
		h.o.held = fmt.Errorf("integration Close after release: %w", err)
		t.Fatalf("integration Close after release = %v", err)
	}
	// A nil Close joins only if the original runtime frame and connection
	// Close agree; otherwise it is held before the Fatal.
	if n := h.in.rt.activeFrames(); n != 0 {
		h.o.held = fmt.Errorf("integration Close = nil with %d runtime frames", n)
		t.Fatalf("%d runtime frames after Close", n)
	}
	if c, _ := h.fc.counts(); c != 1 {
		h.o.held = fmt.Errorf("integration Close = nil after %d connection Closes, want 1", c)
		t.Fatalf("connection Close called %d times, want 1", c)
	}
	h.o.joined = true

	if w := h.writesSince(calls); len(w) != 0 {
		t.Fatalf("writes during Close: %+v", w)
	}
	after := h.borrowed(t)
	for i := range before {
		if !equality.Semantic.DeepEqual(before[i], after[i]) {
			t.Errorf("borrowed %s %s changed", closeBorrowed[i].resource, closeBorrowed[i].name)
		}
	}
	got, _ := h.model.get("pods", h.pod).(*corev1.Pod)
	if got == nil || got.UID != pod.UID || got.DeletionTimestamp != nil {
		t.Fatal("Close removed or replaced the environment's Pod")
	}

	// A stopped Controller refuses at entry: no model call, no transport
	// request and nothing written to the client.
	calls, requests := h.callCount(), len(h.api.requestList())
	if _, err := h.ctl.Exec(context.Background(), h.actor, h.id, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); err == nil {
		t.Fatal("Exec after Close succeeded")
	}
	rec := httptest.NewRecorder()
	if _, err := h.ctl.Forward(rec, httptest.NewRequest(http.MethodGet, "/", nil), h.actor, h.id); err == nil {
		t.Fatal("Forward after Close succeeded")
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("refused Forward wrote %q", rec.Body.String())
	}
	if n := h.callCount(); n != calls {
		t.Fatalf("%d model calls after Close", n-calls)
	}
	if n := len(h.api.requestList()); n != requests {
		t.Fatalf("%d transport requests after Close", n-requests)
	}
	if _, err := h.ctl.Get(h.actor, h.id); err != nil {
		t.Fatalf("Store unusable after Close: %v", err)
	}
}

// TestEnvironmentIntegrationCloseHeldExec checks that the integration's
// Close does not return nil while an exec of its Controller holds the
// original exec connection's Close, and that once that Close returns the
// session is settled durably as unknown, since the command reported no exit.
func TestEnvironmentIntegrationCloseHeldExec(t *testing.T) {
	h := newCloseHarness(t, execProtocol)
	h.api.serveExec(execScript{status: successStatus, hold: h.hold})
	type execOut struct {
		res environmentcontrol.ExecResult
		err error
	}
	done := make(chan execOut, 1)
	go func() {
		res, err := h.ctl.Exec(context.Background(), h.actor, h.id, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
		done <- execOut{res, err}
	}()
	// The error, stdout and stderr streams of the original connection.
	eventually(t, "exec streams", func() bool { _, n := h.fc.counts(); return n == 3 })
	if n := len(h.api.requestList()); n != 1 {
		t.Fatalf("%d exec requests, want 1", n)
	}

	h.closeHeld(t)

	out := receive(t, "Exec", done)
	if out.err != nil {
		t.Fatalf("Exec = %v; a started command is not an error", out.err)
	}
	if out.res.Exited {
		t.Fatalf("canceled exec reported exit %d", out.res.Code)
	}
	v, err := h.ctl.Get(h.actor, h.id)
	if err != nil {
		t.Fatal(err)
	}
	if v.Sessions != 0 || v.Unknown != 1 {
		t.Fatalf("sessions = %d open, %d unknown; want 0, 1", v.Sessions, v.Unknown)
	}
}

// TestEnvironmentIntegrationCloseHeldForward checks the same for a
// streaming forward served by the runtime's actual port gateway: Close does
// not return nil while the exchange's original port-forward connection Close
// is held, and once it returns the session is released.
func TestEnvironmentIntegrationCloseHeldForward(t *testing.T) {
	h := newCloseHarness(t, portProtocol)
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-h.hold
		io.WriteString(w, "late")
	})
	h.api.servePortForward(app.Listener.Addr(), "")
	type forwardOut struct {
		res environmentcontrol.ForwardResult
		err error
	}
	forwarded := make(chan forwardOut, 1)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res, err := h.ctl.Forward(w, r, h.actor, h.id)
		forwarded <- forwardOut{res, err}
	}))
	t.Cleanup(front.Close)
	// Registered after the app and gateway, so their Close does not wait
	// on a held handler.
	t.Cleanup(h.release)

	status := make(chan int, 1)
	body := make(chan struct{})
	go func() {
		defer close(body)
		resp, err := testClient().Get(front.URL + "/events")
		if err != nil {
			status <- 0
			return
		}
		status <- resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	if code := receive(t, "forward response header", status); code != http.StatusOK {
		t.Fatalf("forward status = %d, want 200 streaming", code)
	}
	if _, n := h.fc.counts(); n != 2 {
		t.Fatalf("%d port-forward streams, want the error and data pair", n)
	}

	h.closeHeld(t)

	out := receive(t, "Forward", forwarded)
	if out.err != nil {
		t.Fatalf("Forward = %v; an admitted exchange is not an error", out.err)
	}
	receive(t, "forward client", body)
	v, err := h.ctl.Get(h.actor, h.id)
	if err != nil {
		t.Fatal(err)
	}
	if v.Sessions != 0 || v.Unknown != 0 {
		t.Fatalf("sessions = %d open, %d unknown; want none", v.Sessions, v.Unknown)
	}
}

// TestEnvironmentIntegrationCloseOwnerHeldRetainsDependencies checks the
// owner's cleanup decision when its final Close of the original integration
// does not return nil. The sentinel MODELS only that: while it stands in for
// in.Close, no Close of the integration runs, so the integration really
// remains open. It is not a native or syscall failure. The Engine, the Store
// and the directory must not be retired, every original reference must be
// kept, and a repeated decision must neither retry nor replace the error.
func TestEnvironmentIntegrationCloseOwnerHeldRetainsDependencies(t *testing.T) {
	h := newCloseHarness(t, execProtocol)
	o := h.o
	failure := errors.New("modeled test sentinel: final integration Close remains held")
	var closes, stops, storeCloses, removes int
	realClose, realStop, realStoreClose, realRemove := o.closeIn, o.stop, o.closeStore, o.remove
	o.closeIn = func(context.Context) error { closes++; return failure }
	o.stop = func() { stops++; realStop() }
	o.closeStore = func(s *store.Store) error { storeCloses++; return realStoreClose(s) }
	o.remove = func(path string) error { removes++; return realRemove(path) }
	in, s, engine, done, dir := o.in, o.s, o.engine, o.done, o.dir

	if err := o.retireJoined(); !errors.Is(err, failure) {
		t.Fatalf("held integration Close became a join: %v", err)
	}
	saved := o.held
	if err := o.retireJoined(); err != saved || o.held != saved {
		t.Fatal("repeated cleanup replaced or cleared the held error")
	}
	if closes != 1 {
		t.Fatalf("final integration Close attempted %d times, want 1", closes)
	}
	if stops != 0 || storeCloses != 0 || removes != 0 || o.joined || o.retired {
		t.Fatalf("held Close retired dependencies: stop=%d store=%d remove=%d", stops, storeCloses, removes)
	}
	if o.in != in || o.s != s || o.engine != engine || o.done != done || o.harness != h || o.dir != dir {
		t.Fatal("held owner lost an original reference")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("held namespace removed: %v", err)
	}
	select {
	case <-done:
		t.Fatal("auth Engine stopped while held")
	default:
	}
	if _, err := engine.Authenticate("Bearer " + h.token); err != nil {
		t.Fatalf("auth Engine unusable while held: %v", err)
	}
	if _, err := s.RetentionInfo(h.actor); err != nil {
		t.Fatalf("Store unusable while held: %v", err)
	}
	if _, err := h.ctl.Get(h.actor, h.id); err != nil {
		t.Fatalf("Controller unusable while held: %v", err)
	}

	// The sentinel owns no uncertain operation. Clear only it, and the
	// same decision closes the actual integration, which positively joins,
	// before it retires anything. Fixture code never clears a real held error.
	if !errors.Is(o.held, failure) {
		t.Fatalf("held error = %v, want the sentinel", o.held)
	}
	o.held, o.closeIn = nil, realClose
	o.cleanup(t)
	if !o.joined || !o.retired || stops != 1 || storeCloses != 1 || removes != 1 {
		t.Fatalf("positive cleanup: joined=%v retired=%v stop=%d store=%d remove=%d",
			o.joined, o.retired, stops, storeCloses, removes)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("joined namespace not removed: %v", err)
	}
}

// TestEnvironmentIntegrationCloseOwnerRetiresAfterJoin checks the order of a
// positive retirement: the actual Close of the original integration returns
// nil, then the Engine is stopped and its Run returns, then the Store is
// closed, and only then is the directory removed.
func TestEnvironmentIntegrationCloseOwnerRetiresAfterJoin(t *testing.T) {
	h := newCloseHarness(t, execProtocol)
	o := h.o
	var order []string
	realClose, realStop, realStoreClose, realRemove := o.closeIn, o.stop, o.closeStore, o.remove
	s, done, dir := o.s, o.done, o.dir
	o.closeIn = func(ctx context.Context) error {
		err := realClose(ctx)
		order = append(order, "close")
		return err
	}
	o.stop = func() { order = append(order, "stop"); realStop() }
	o.closeStore = func(s *store.Store) error {
		select {
		case <-done:
		default:
			t.Error("Store closed before the auth Engine's Run returned")
		}
		order = append(order, "store")
		return realStoreClose(s)
	}
	o.remove = func(path string) error { order = append(order, "remove"); return realRemove(path) }

	o.cleanup(t)
	if got := strings.Join(order, " "); got != "close stop store remove" {
		t.Fatalf("retirement order = %q, want %q", got, "close stop store remove")
	}
	if !o.joined || !o.retired || o.held != nil {
		t.Fatalf("joined=%v retired=%v held=%v", o.joined, o.retired, o.held)
	}
	if n := h.in.rt.activeFrames(); n != 0 {
		t.Fatalf("%d runtime frames after the joined Close", n)
	}
	if _, err := s.RetentionInfo(h.actor); err == nil {
		t.Fatal("Store still open after retirement")
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("namespace not removed: %v", err)
	}
}

// TestEnvironmentIntegrationCloseOwnerSetupFailure checks the owner that a
// setup failure right after the first Store Open leaves: the Store recorded
// as acquired, and no Engine or Integration. A MODELED sentinel error from
// the Store's Close, which does not close it, is held with nothing removed
// or retried. Once only that sentinel is cleared, cleanup closes the actual
// Store once and then removes the directory.
func TestEnvironmentIntegrationCloseOwnerSetupFailure(t *testing.T) {
	o := newCloseOwner(t)
	s := o.open(t, filepath.Join(o.dir, "state"))
	if o.s != s || o.in != nil || o.engine != nil || o.stop != nil {
		t.Fatal("Store not recorded as acquired, or more than the Store owned")
	}
	failure := errors.New("modeled test sentinel: Store Close outcome unknown")
	var closes, removes int
	realStoreClose, realRemove := o.closeStore, o.remove
	o.closeStore = func(*store.Store) error { closes++; return failure }
	o.remove = func(path string) error { removes++; return realRemove(path) }

	if err := o.retireJoined(); !errors.Is(err, failure) {
		t.Fatalf("held Store Close became a retirement: %v", err)
	}
	saved := o.held
	if err := o.retireJoined(); err != saved || o.held != saved {
		t.Fatal("repeated cleanup replaced or cleared the held error")
	}
	if closes != 1 || removes != 0 || o.retired || o.s != s {
		t.Fatalf("held Store: close=%d remove=%d retired=%v", closes, removes, o.retired)
	}
	if _, err := os.Stat(o.dir); err != nil {
		t.Fatalf("held namespace removed: %v", err)
	}
	if _, _, err := s.RetentionPolicy(); err != nil {
		t.Fatalf("held Store unusable: %v", err)
	}

	if !errors.Is(o.held, failure) {
		t.Fatalf("held error = %v, want the sentinel", o.held)
	}
	o.held = nil
	o.closeStore = func(s *store.Store) error { closes++; return realStoreClose(s) }
	o.cleanup(t)
	if closes != 2 || removes != 1 || !o.retired {
		t.Fatalf("positive cleanup: close=%d remove=%d retired=%v", closes, removes, o.retired)
	}
	if _, _, err := s.RetentionPolicy(); err == nil {
		t.Fatal("Store still open after retirement")
	}
	if _, err := os.Stat(o.dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("namespace not removed: %v", err)
	}
}
