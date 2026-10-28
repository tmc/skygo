package kube

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	"github.com/tmc/skygo/internal/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// Source-only; NOT_RUN. profileAPI MODELS the four API reads that
// NewEnvironmentIntegration makes with an HTTP fake; it is not an API
// server. The stop tests reuse the runtime tests' model (environmentAPIModel and
// fakeAPI); none observes a real cluster.

// profileAPI answers GETs of the objects it holds, by URL path, and records
// every request.
type profileAPI struct {
	server *httptest.Server
	mu     sync.Mutex
	objs   map[string]any
	seen   []string // method and path of every request
}

func newProfileAPI(t *testing.T) *profileAPI {
	t.Helper()
	filesystem := corev1.PersistentVolumeFilesystem
	a := &profileAPI{objs: map[string]any{
		"/api/v1/namespaces/workloads": &corev1.Namespace{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "namespace-uid-1"},
			Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
		},
		"/api/v1/namespaces/workloads/serviceaccounts/default": &corev1.ServiceAccount{
			TypeMeta:         metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta:       metav1.ObjectMeta{Name: "default", Namespace: "workloads", UID: "service-account-uid-1"},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "registry-b"}, {Name: "registry-a"}},
		},
		"/api/v1/namespaces/workloads/persistentvolumeclaims/alice-home": &corev1.PersistentVolumeClaim{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
			ObjectMeta: metav1.ObjectMeta{Name: "alice-home", Namespace: "workloads", UID: "claim-uid-1"},
			Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeName: "pv-alice-home", VolumeMode: &filesystem},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
		"/api/v1/persistentvolumes/pv-alice-home": &corev1.PersistentVolume{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "PersistentVolume"},
			ObjectMeta: metav1.ObjectMeta{Name: "pv-alice-home", UID: "pv-uid-1"},
			Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
				Namespace: "workloads", Name: "alice-home", UID: "claim-uid-1"}},
			Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
		},
	}}
	a.server = httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(a.server.Close)
	return a
}

func (a *profileAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.seen = append(a.seen, r.Method+" "+r.URL.Path)
	obj, ok := a.objs[r.URL.Path]
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet || !ok {
		code := http.StatusNotFound
		reason := metav1.StatusReasonNotFound
		if r.Method != http.MethodGet {
			code, reason = http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
			Status: metav1.StatusFailure, Reason: reason, Code: int32(code)})
		return
	}
	json.NewEncoder(w).Encode(obj)
}

// edit applies f to the object at path, or removes it when f is nil.
func (a *profileAPI) edit(path string, f func(any)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if f == nil {
		delete(a.objs, path)
		return
	}
	f(a.objs[path])
}

// writes returns every request that was not a GET.
func (a *profileAPI) writes() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, s := range a.seen {
		if !strings.HasPrefix(s, http.MethodGet+" ") {
			out = append(out, s)
		}
	}
	return out
}

func (a *profileAPI) requests() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.seen)
}

// targets returns two targets on a's server: kube, which the profiles
// name, and other.
func (a *profileAPI) targets(t *testing.T) Config {
	t.Helper()
	path := writeKubeconfig(t, a.server.URL)
	return Config{targets: []TargetConfig{
		{Name: "kube", Namespace: "workloads", Kubeconfig: path, Context: "test", Resources: envRTPolicy()},
		{Name: "other", Namespace: "workloads", Kubeconfig: path, Context: "test"},
	}}
}

// environmentFileProfile is environmentTestProfile as the profile file
// requires it, with the bound volume's UID.
func environmentFileProfile() environmentPodProfile {
	p := environmentTestProfile()
	p.volumeUID = "pv-uid-1"
	return p
}

func environmentTestConfig() EnvironmentConfig {
	return EnvironmentConfig{target: "kube", profiles: []environmentPodProfile{environmentFileProfile()}}
}

// environmentStore returns an open Store, closed at cleanup, and an Engine.
// With scoped, it activates file authentication offline, as auth init does;
// with retention, it enables retention, which requires scoped. Without
// scoped the Engine is the legacy one.
func environmentStore(t *testing.T, scoped, retention bool) (*store.Store, *auth.Engine) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	state, config := filepath.Join(dir, "state"), filepath.Join(dir, "auth.json")
	s, err := store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	if scoped {
		if _, err := auth.Initialize(config, filepath.Join(dir, "admin.token"), s.Owner()); err != nil {
			t.Fatal(err)
		}
		if err := s.ActivateScoped(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if retention {
		policy := store.DefaultRetentionPolicy()
		policy.LocalOutputEnabled = false
		if _, err := store.EnableRetention(state, policy); err != nil {
			t.Fatal(err)
		}
	}
	s, err = store.Open(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if !scoped {
		return s, auth.NewLegacy("legacy-token")
	}
	engine, err := auth.NewFile(config, s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	return s, engine
}

// closeIntegration closes in with a bounded context and fails on error.
func closeIntegration(t *testing.T, in interface{ Close(context.Context) error }) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := in.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestNewEnvironmentIntegration checks that the factory owns every
// configured target, marks only the profiles' target, builds the runtime on
// that target's own Backend and frozen configuration, and only reads the
// namespace, service account, claim and volume.
func TestNewEnvironmentIntegration(t *testing.T) {
	api := newProfileAPI(t)
	s, engine := environmentStore(t, true, true)
	got, err := NewEnvironmentIntegration(context.Background(), s, engine, api.targets(t), environmentTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	in := got.(*environmentIntegration)
	defer closeIntegration(t, in)
	targets := in.Targets()
	if len(targets) != 2 || targets[0].Name != "kube" || targets[1].Name != "other" {
		t.Fatalf("targets = %+v", targets)
	}
	if !targets[0].SupportsPersistentEnvironments || targets[0].SupportsEnvironmentArtifacts ||
		targets[1].SupportsPersistentEnvironments || targets[1].SupportsEnvironmentArtifacts {
		t.Fatalf("environment support = %v/%v, %v/%v; want kube only, without artifacts",
			targets[0].SupportsPersistentEnvironments, targets[0].SupportsEnvironmentArtifacts,
			targets[1].SupportsPersistentEnvironments, targets[1].SupportsEnvironmentArtifacts)
	}
	if in.rt.b != targets[0].Backend.(*Backend) || in.rt.target != "kube" {
		t.Fatal("environment runtime is not on the selected target's Backend")
	}
	if host, err := canonicalServer(api.server.URL); err != nil || in.rt.port.config.Host != host {
		t.Fatalf("runtime host = %q, want the target's frozen %q (%v)", in.rt.port.config.Host, host, err)
	}
	if in.EnvironmentController() == nil {
		t.Fatal("no Controller")
	}
	if w := api.writes(); len(w) != 0 {
		t.Fatalf("writes = %q", w)
	}
	if n := api.requests(); n != 4 {
		t.Fatalf("%d requests, want the 4 profile reads", n)
	}
	// A Spec of the other target is refused before any effect.
	other := envRTSpec()
	other.Target = "other"
	if _, _, err := in.rt.Plan("alice", other, envRTDigest(t, other)); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("Plan on the other target = %v", err)
	}
	in.StopEffects()
	if n := api.requests(); n != 4 {
		t.Fatal("StopEffects made a request")
	}
}

// TestNewEnvironmentIntegrationProfileMismatch checks that a namespace,
// service account, claim or volume that differs from its profile fails the
// factory with the integration returned, closable, and nothing written.
func TestNewEnvironmentIntegrationProfileMismatch(t *testing.T) {
	const (
		ns  = "/api/v1/namespaces/workloads"
		sa  = ns + "/serviceaccounts/default"
		pvc = ns + "/persistentvolumeclaims/alice-home"
		pv  = "/api/v1/persistentvolumes/pv-alice-home"
	)
	for _, tt := range []struct {
		name string
		path string
		edit func(any) // nil removes the object
		want error
	}{
		{"namespace replaced", ns, func(o any) { o.(*corev1.Namespace).UID = "namespace-uid-2" }, backend.ErrIdentityMismatch},
		{"namespace terminating", ns, func(o any) { o.(*corev1.Namespace).Status.Phase = corev1.NamespaceTerminating }, errEnvironmentObjectUnavailable},
		{"namespace missing", ns, nil, errEnvironmentObjectMissing},
		{"service account replaced", sa, func(o any) { o.(*corev1.ServiceAccount).UID = "service-account-uid-2" }, backend.ErrIdentityMismatch},
		{"pull secrets reordered", sa, func(o any) {
			o.(*corev1.ServiceAccount).ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry-a"}, {Name: "registry-b"}}
		}, backend.ErrIdentityMismatch},
		{"claim replaced", pvc, func(o any) { o.(*corev1.PersistentVolumeClaim).UID = "claim-uid-2" }, backend.ErrIdentityMismatch},
		{"claim other volume", pvc, func(o any) { o.(*corev1.PersistentVolumeClaim).Spec.VolumeName = "pv-other" }, backend.ErrIdentityMismatch},
		{"claim unbound", pvc, func(o any) {
			c := o.(*corev1.PersistentVolumeClaim)
			c.Spec.VolumeName, c.Status.Phase = "", corev1.ClaimPending
		}, backend.ErrIdentityMismatch},
		{"claim missing", pvc, nil, errEnvironmentObjectMissing},
		{"volume bound elsewhere", pv, func(o any) { o.(*corev1.PersistentVolume).Spec.ClaimRef.UID = "claim-uid-2" }, backend.ErrIdentityMismatch},
		// Same name, same claimRef, bound: only the profile's volume_uid
		// tells the replacement apart.
		{"volume replaced", pv, func(o any) { o.(*corev1.PersistentVolume).UID = "pv-uid-2" }, backend.ErrIdentityMismatch},
		{"volume released", pv, func(o any) { o.(*corev1.PersistentVolume).Status.Phase = corev1.VolumeReleased }, errEnvironmentObjectUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api := newProfileAPI(t)
			api.edit(tt.path, tt.edit)
			s, engine := environmentStore(t, true, true)
			in, err := NewEnvironmentIntegration(context.Background(), s, engine, api.targets(t), environmentTestConfig())
			if err == nil {
				t.Fatal("factory accepted a differing profile object")
			}
			// An unbound claim has no volume name: it differs from the
			// profile before its phase is read.
			if !errors.Is(err, tt.want) {
				t.Fatalf("factory = %v, want %v", err, tt.want)
			}
			if in == nil {
				t.Fatal("factory dropped the integration that owns its runtime")
			}
			if in.(*environmentIntegration).EnvironmentController() != nil {
				t.Fatal("a Controller was built over a differing profile")
			}
			closeIntegration(t, in)
			if w := api.writes(); len(w) != 0 {
				t.Fatalf("writes = %q", w)
			}
		})
	}
}

// TestNewEnvironmentIntegrationRefuses checks the prerequisites that fail
// before any target is built or any request is made.
func TestNewEnvironmentIntegrationRefuses(t *testing.T) {
	for _, tt := range []struct {
		name              string
		scoped, retention bool
		profiles          EnvironmentConfig
		want              string
	}{
		{"unscoped", false, false, environmentTestConfig(), "scoped authentication"},
		{"retention disabled", true, false, environmentTestConfig(), "enabled retention"},
		{"no profiles", true, true, EnvironmentConfig{}, "require profiles"},
		{"unconfigured target", true, true, EnvironmentConfig{target: "missing", profiles: []environmentPodProfile{environmentFileProfile()}}, "unconfigured target"},
		{"artifacts without helper", true, true, EnvironmentConfig{target: "kube", artifacts: true, profiles: []environmentPodProfile{environmentFileProfile()}}, "data helper image"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api := newProfileAPI(t)
			s, engine := environmentStore(t, tt.scoped, tt.retention)
			in, err := NewEnvironmentIntegration(context.Background(), s, engine, api.targets(t), tt.profiles)
			if err == nil || !strings.Contains(err.Error(), tt.want) || in != nil {
				t.Fatalf("factory = %v, %v; want nil and %q", in, err, tt.want)
			}
			if n := api.requests(); n != 0 {
				t.Fatalf("%d requests before the prerequisites passed", n)
			}
		})
	}
	if _, err := NewEnvironmentIntegration(context.Background(), nil, nil, Config{}, environmentTestConfig()); err == nil {
		t.Fatal("factory accepted no Store")
	}
}

// TestEnvironmentRuntimeTargetGuard checks that a runtime refuses a Spec of
// another target in Plan and every Ref-taking method before any request,
// reservation or send.
func TestEnvironmentRuntimeTargetGuard(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	other := envRTSpec()
	other.Target = "other"
	digest := envRTDigest(t, other)
	if _, _, err := e.rt.Plan("alice", other, digest); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("Plan = %v, want ErrUnsupported", err)
	}
	ref := e.ref
	ref.Spec, ref.SpecDigest = other, digest
	ctx := context.Background()
	if _, err := e.rt.Ensure(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Ensure = %v", err)
	}
	if _, err := e.rt.Delete(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Delete = %v", err)
	}
	if _, err := e.rt.Exec(ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Exec = %v", err)
	}
	if res := e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), ref).Result(); !errors.Is(res.Err, backend.ErrIdentityMismatch) {
		t.Fatalf("Forward = %+v", res)
	}
	if _, err := e.rt.Materialize(ref, "incarnation-1", nil, strings.NewReader("")); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Materialize = %v", err)
	}
	if _, err := e.rt.Bootstrap(ref, "incarnation-1"); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Bootstrap = %v", err)
	}
	e.f.mu.Lock()
	calls := len(e.f.calls)
	e.f.mu.Unlock()
	if calls != 0 || e.rt.activeFrames() != 0 || e.rt.createFrame(envRTID, 1) != nil {
		t.Fatalf("%d calls, %d frames after refusals", calls, e.rt.activeFrames())
	}
}

// TestEnvironmentRuntimeProfileVolumeUID checks the profile file's volume
// UID: Plan records it as the accepted UID, a same-name replacement with the
// same claimRef is refused before any Create, the original volume is
// accepted, and a profile whose volume UID differs refuses the environment
// rather than adopting the other volume.
func TestEnvironmentRuntimeProfileVolumeUID(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.rt = e.runtime(environmentFileProfile())
	binding, native, err := e.rt.Plan("alice", envRTSpec(), envRTDigest(t, envRTSpec()))
	if err != nil {
		t.Fatal(err)
	}
	e.ref.Binding, e.ref.Native = binding, native
	if n := e.native(); n.VolumeUID != "pv-uid-1" || n.Sends != 0 {
		t.Fatalf("planned native = %+v, want the profile's volume UID", n)
	}
	e.f.remove("persistentvolumes", "pv-alice-home")
	e.store("persistentvolumes", envRTVolume("pv-uid-2"))
	o, err := e.rt.Ensure(context.Background(), e.ref)
	e.want(o, err, environment.Unknown, backend.ErrIdentityMismatch)
	e.commit(o)
	if n := e.native(); n.Sends != 0 || n.Write != nil {
		t.Fatalf("native after a replaced volume = %+v, want nothing declared", n)
	}
	if len(e.calls("create", "pods")) != 0 {
		t.Fatal("a Create on a replaced volume")
	}
	e.f.remove("persistentvolumes", "pv-alice-home")
	e.store("persistentvolumes", envRTVolume("pv-uid-1"))
	e.created()
	if n := e.native(); n.VolumeUID != "pv-uid-1" {
		t.Fatalf("native = %+v", n)
	}
	changed := environmentFileProfile()
	changed.volumeUID = "pv-uid-2"
	if _, err := e.runtime(changed).Ensure(context.Background(), e.ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("Ensure under a profile with another volume UID = %v", err)
	}
	e.borrowedIntact()
}

// heldCreate starts environment e's Create send, whose model is create, and
// returns once the Controller-facing Ensure returned with the send in
// flight, as in TestEnvironmentRuntimeLateResult.
func heldCreate(t *testing.T, e *envRT, entered <-chan struct{}) *environmentCreate {
	t.Helper()
	o, err := e.rt.Ensure(context.Background(), e.ref)
	e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
	e.commit(o)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-entered; cancel() }()
	o, err = e.rt.Ensure(ctx, e.ref)
	e.want(o, err, environment.Creating, context.Canceled)
	e.commit(o)
	c := e.rt.createFrame(envRTID, 1)
	if c == nil {
		t.Fatal("create frame not kept")
	}
	return c
}

// shortJoin calls rt.join with a short deadline.
func shortJoin(rt *environmentRuntime) error {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	return rt.join(ctx)
}

func longJoin(t *testing.T, rt *environmentRuntime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := rt.join(ctx); err != nil {
		t.Fatalf("join = %v, want nil once everything settled", err)
	}
}

// TestEnvironmentRuntimeStopCreate checks that join waits for a Create send
// in flight: one that ignores cancellation holds join until it returns, and
// one that honors it is canceled by stop. Neither is deleted; the late
// result may still land, as Ensure's contract allows.
func TestEnvironmentRuntimeStopCreate(t *testing.T) {
	t.Run("held", func(t *testing.T) {
		e := newEnvRT(t, &rest.Config{Host: envRTHost})
		entered, release := make(chan struct{}), make(chan struct{})
		e.rt.pods.(*funcPods).createFunc = func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			close(entered)
			<-release // ignores ctx, as an unbounded transport would
			return e.podCreate(context.Background(), pod)
		}
		c := heldCreate(t, e, entered)
		if err := shortJoin(e.rt); err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "1 create sends") {
			t.Fatalf("join with a held send = %v", err)
		}
		if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentStopped) {
			t.Fatalf("Exec after stop = %v", err)
		}
		close(release)
		receive(t, "create frame", c.done)
		longJoin(t, e.rt)
		if len(e.calls("delete", "pods")) != 0 || len(e.calls("update", "pods")) != 0 {
			t.Fatal("stop wrote to the Pod")
		}
		e.borrowedIntact()
	})
	t.Run("canceled", func(t *testing.T) {
		e := newEnvRT(t, &rest.Config{Host: envRTHost})
		entered := make(chan struct{})
		e.rt.pods.(*funcPods).createFunc = func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		c := heldCreate(t, e, entered)
		longJoin(t, e.rt)
		receive(t, "create frame", c.done)
		if !errors.Is(c.err, context.Canceled) {
			t.Fatalf("create send = %v, want canceled by stop", c.err)
		}
		if e.pod() != nil || len(e.calls("delete", "pods")) != 0 {
			t.Fatal("stop created or deleted a Pod")
		}
		e.borrowedIntact()
	})
}

// TestEnvironmentRuntimeStopExec checks that join cancels a running exec,
// which reports no exit, waits for its frame, and refuses later execs
// without a request.
func TestEnvironmentRuntimeStopExec(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvRT(t, a.config())
	e.created()
	e.run("node-1")
	hold := make(chan struct{})
	defer close(hold)
	a.serveExec(execScript{status: successStatus, hold: hold})
	x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "exec request", func() bool { return len(a.requestList()) == 1 })
	longJoin(t, e.rt)
	select {
	case <-x.Done():
	default:
		t.Fatal("join returned before the exec frame settled")
	}
	if res := x.Result(); !res.Settled || res.Exited || res.Err == nil {
		t.Fatalf("stopped exec = %+v", res)
	}
	if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentStopped) {
		t.Fatalf("Exec after join = %v", err)
	}
	if len(a.requestList()) != 1 {
		t.Fatal("an exec after stop reached the server")
	}
	if len(e.calls("update", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("stop wrote to the Pod")
	}
	e.borrowedIntact()
}

// TestEnvironmentRuntimeStopForward checks that stop cancels a streaming
// exchange through its request context and that join waits for an exchange
// whose frame has not settled.
func TestEnvironmentRuntimeStopForward(t *testing.T) {
	t.Run("streaming", func(t *testing.T) {
		e := newEnvRT(t, &rest.Config{Host: envRTHost})
		e.created()
		e.run("node-1")
		entered := make(chan struct{})
		e.rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
			close(entered)
			<-r.Context().Done()
			return settledPortFrame()
		}
		done := make(chan environmentcontrol.Forward, 1)
		go func() {
			done <- e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/events", nil), e.ref)
		}()
		receive(t, "serve", entered)
		longJoin(t, e.rt)
		if res := receive(t, "Forward", done).Result(); !res.Settled {
			t.Fatalf("Forward = %+v", res)
		}
		if len(e.calls("update", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
			t.Fatal("stop wrote to the Pod")
		}
	})
	t.Run("unsettled", func(t *testing.T) {
		e := newEnvRT(t, &rest.Config{Host: envRTHost})
		e.created()
		e.run("node-1")
		p := &portFrame{envFrame: newEnvFrame()}
		p.enter() // an owner that ignores cancellation
		e.rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
			w.WriteHeader(http.StatusNoContent)
			return p
		}
		f := e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), e.ref)
		if err := shortJoin(e.rt); err == nil || !strings.Contains(err.Error(), "1 kubernetes environment frames") {
			t.Fatalf("join with an unsettled exchange = %v", err)
		}
		if res := e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), e.ref).Result(); !errors.Is(res.Err, errEnvironmentStopped) {
			t.Fatalf("Forward after stop = %+v", res)
		}
		p.leave(nil)
		receive(t, "forward", f.Done())
		longJoin(t, e.rt)
	})
}

// TestEnvironmentRuntimeStopReserved checks a slot reserved before stop and
// attached after it: its cancel function is called at attach, and join
// waits for its release.
func TestEnvironmentRuntimeStopReserved(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	key, err := e.rt.reserve(envRTID)
	if err != nil {
		t.Fatal(err)
	}
	e.rt.stop()
	if _, err := e.rt.reserve(envRTID); !errors.Is(err, errEnvironmentStopped) {
		t.Fatalf("reserve after stop = %v", err)
	}
	canceled := false
	e.rt.attach(envRTID, key, func() { canceled = true })
	if !canceled {
		t.Fatal("a frame attached after stop was not canceled")
	}
	if err := shortJoin(e.rt); err == nil {
		t.Fatal("join returned with a reserved slot")
	}
	e.rt.release(envRTID, key)
	longJoin(t, e.rt)
}
