package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

// The environment runtime tests use environmentAPIModel, which MODELS the API server
// (see its comment), plus namespace, claim and volume objects stored
// directly. The kubelet's status writes, an activeDeadlineSeconds kill and
// a Create delivered late are explicit test edits, not observed behavior.
// The tests play the Controller: they commit each returned Native before
// the next call. Native qualification must confirm each rule.

const (
	envRTID    = "0123456789abcdef0123456789abcdef"
	envRTPod   = "skygo-env-" + envRTID
	envRTNonce = "00112233445566778899aabbccddeeff"
	envRTToken = "fedcba9876543210fedcba9876543210"
	envRTHost  = "https://127.0.0.1:6443"
)

func envRTPolicy() ResourcePolicy {
	return ResourcePolicy{
		MaxCPUMillis: 4000, MaxMemoryBytes: 8 << 30, MaxAccelerators: 2,
		NodeSelectors: map[string][]string{"pool": {"cpu", "gpu"}},
		Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu",
			RequiredNodeSelector: map[string]string{"pool": "gpu"}}},
	}
}

func envRTSpec() environment.Spec {
	return environment.Spec{Name: "dev", Target: "kube", Image: environmentTestImage,
		CPUMillis: 500, MemoryBytes: 256 << 20, CPULimitMillis: 1000, MemoryLimitBytes: 512 << 20,
		StorageAlias: "home", Port: environment.Port}
}

func envRTDigest(t *testing.T, s environment.Spec) [32]byte {
	t.Helper()
	raw, err := environment.Canonical(s)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(raw)
}

// envRT is one environment on a modeled API server.
type envRT struct {
	t      *testing.T
	f      *environmentAPIModel
	config *rest.Config
	target backend.Target
	rt     *environmentRuntime
	ref    environmentcontrol.Ref
}

func newEnvRT(t *testing.T, config *rest.Config) *envRT {
	t.Helper()
	f := newEnvironmentAPIModel(t)
	target, err := fromREST(TargetConfig{Name: "kube", Namespace: "workloads", Resources: envRTPolicy()}, "owner", config)
	if err != nil {
		t.Fatal(err)
	}
	target.Backend.(*Backend).client = f.client
	e := &envRT{t: t, f: f, config: config, target: target}
	e.rt = e.runtime(environmentTestProfile())
	filesystem := corev1.PersistentVolumeFilesystem
	e.store("namespaces", &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "namespace-uid-1"},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	})
	e.store("persistentvolumeclaims", &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "alice-home", Namespace: "workloads", UID: "claim-uid-1"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			VolumeName:  "pv-alice-home", VolumeMode: &filesystem,
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	})
	e.store("persistentvolumes", envRTVolume("pv-uid-1"))
	digest := envRTDigest(t, envRTSpec())
	binding, native, err := e.rt.Plan("alice", envRTSpec(), digest)
	if err != nil {
		t.Fatal(err)
	}
	e.ref = environmentcontrol.Ref{Identity: environment.Identity{Installation: "install-1", ID: envRTID, Generation: 1},
		Principal: "alice", Spec: envRTSpec(), SpecDigest: digest, Binding: binding, Nonce: envRTNonce, Native: native}
	return e
}

func envRTVolume(uid string) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-alice-home", UID: types.UID(uid)},
		Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
			Namespace: "workloads", Name: "alice-home", UID: "claim-uid-1"}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
}

// runtime returns a new runtime on the same API, as after a restart. Its
// Create sends go to the model.
func (e *envRT) runtime(profiles ...environmentPodProfile) *environmentRuntime {
	e.t.Helper()
	rt, err := newEnvironmentRuntime(e.target, e.config, profiles)
	if err != nil {
		e.t.Fatal(err)
	}
	rt.pods = &funcPods{
		createFunc: e.podCreate,
		updateFunc: func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
			return e.f.client.CoreV1().Pods("workloads").Update(ctx, pod, metav1.UpdateOptions{})
		},
		removeFunc: func(ctx context.Context, name string, opts metav1.DeleteOptions) error {
			return e.f.client.CoreV1().Pods("workloads").Delete(ctx, name, opts)
		},
	}
	return rt
}

func (e *envRT) podCreate(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	return e.f.client.CoreV1().Pods("workloads").Create(ctx, pod, metav1.CreateOptions{})
}

// funcPods is an environmentPodWriter made of functions. The runtime tests
// send its writes to the model through the typed fake client;
// TestEnvironmentPodWriteOneSend tests the real writer.
type funcPods struct {
	createFunc func(context.Context, *corev1.Pod) (*corev1.Pod, error)
	updateFunc func(context.Context, *corev1.Pod) (*corev1.Pod, error)
	removeFunc func(context.Context, string, metav1.DeleteOptions) error
}

func (p *funcPods) create(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	return p.createFunc(ctx, pod)
}

func (p *funcPods) update(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	return p.updateFunc(ctx, pod)
}

func (p *funcPods) remove(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return p.removeFunc(ctx, name, opts)
}

// store stores obj directly, keeping a set UID.
func (e *envRT) store(resource string, obj runtime.Object) {
	e.f.mu.Lock()
	defer e.f.mu.Unlock()
	m, _ := meta.Accessor(obj)
	e.f.assign(m)
	e.f.objs[environmentAPIKey{resource, m.GetName()}] = obj
}

func (e *envRT) pod() *corev1.Pod {
	o := e.f.get("pods", envRTPod)
	if o == nil {
		return nil
	}
	return o.(*corev1.Pod)
}

func (e *envRT) native() environmentNative {
	e.t.Helper()
	n, err := decodeEnvironmentNative(e.ref.Native)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *envRT) calls(verb, resource string) []environmentAPICall {
	e.f.mu.Lock()
	defer e.f.mu.Unlock()
	var out []environmentAPICall
	for _, c := range e.f.calls {
		if c.verb == verb && c.resource == resource {
			out = append(out, c)
		}
	}
	return out
}

// commit commits o's Native, as the Controller does before any later call.
func (e *envRT) commit(o environmentcontrol.Observation) {
	if o.Native != nil {
		e.ref.Native = o.Native
	}
}

// ensure calls Ensure and commits its Native until no write is pending.
func (e *envRT) ensure(rt *environmentRuntime) (environmentcontrol.Observation, error) {
	e.t.Helper()
	for range 4 {
		o, err := rt.Ensure(context.Background(), e.ref)
		e.commit(o)
		if !errors.Is(err, environmentcontrol.ErrWritePending) {
			return o, err
		}
	}
	e.t.Fatal("Ensure still pending")
	return environmentcontrol.Observation{}, nil
}

// delete is ensure for Delete.
func (e *envRT) delete(rt *environmentRuntime) (environmentcontrol.Observation, error) {
	e.t.Helper()
	for range 6 {
		o, err := rt.Delete(context.Background(), e.ref)
		e.commit(o)
		if !errors.Is(err, environmentcontrol.ErrWritePending) {
			return o, err
		}
	}
	e.t.Fatal("Delete still pending")
	return environmentcontrol.Observation{}, nil
}

// want fails unless o has phase and err matches target, or is nil when
// target is nil.
func (e *envRT) want(o environmentcontrol.Observation, err error, phase environment.Phase, target error) {
	e.t.Helper()
	if o.Phase != phase || target == nil && err != nil || target != nil && !errors.Is(err, target) {
		e.t.Fatalf("observation = %v, %v; want %v, %v", o.Phase, err, phase, target)
	}
}

// run models the scheduler binding the Pod to node and the kubelet
// reporting a running, ready workspace container.
func (e *envRT) run(node string) {
	e.f.edit("pods", envRTPod, func(o runtime.Object) {
		p := o.(*corev1.Pod)
		p.Spec.NodeName = node
		p.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: environmentContainer, Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
	})
}

// kill models the kubelet enforcing activeDeadlineSeconds.
func (e *envRT) kill() {
	e.f.edit("pods", envRTPod, func(o runtime.Object) {
		p := o.(*corev1.Pod)
		p.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "DeadlineExceeded",
			ContainerStatuses: []corev1.ContainerStatus{{Name: environmentContainer,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}}}}}
	})
}

// created creates and records the environment's Pod.
func (e *envRT) created() {
	e.t.Helper()
	if _, err := e.ensure(e.rt); err != nil {
		e.t.Fatal(err)
	}
	if e.native().Pod == nil {
		e.t.Fatal("no Pod recorded")
	}
}

// borrowedIntact fails unless the claim and volume are stored and nothing
// but Gets reached them.
func (e *envRT) borrowedIntact() {
	e.t.Helper()
	for _, res := range []string{"persistentvolumeclaims", "persistentvolumes", "namespaces"} {
		for _, verb := range []string{"create", "update", "patch", "delete"} {
			if c := e.calls(verb, res); len(c) != 0 {
				e.t.Errorf("%d %s calls on %s", len(c), verb, res)
			}
		}
	}
	if e.f.get("persistentvolumeclaims", "alice-home") == nil || e.f.get("persistentvolumes", "pv-alice-home") == nil {
		e.t.Error("borrowed claim or volume removed")
	}
}

func (rt *environmentRuntime) activeFrames() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.active
}

// attachedFrames counts the frames of id whose cancel function is attached.
func (rt *environmentRuntime) attachedFrames(id string) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	n := 0
	if f := rt.frames[id]; f != nil {
		for _, c := range f.held {
			if c != nil {
				n++
			}
		}
	}
	return n
}

// receive returns the value from c, failing after testWait.
func receive[T any](t *testing.T, what string, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(testWait):
		t.Fatalf("%s did not return", what)
	}
	panic("unreachable")
}

func lostCreate(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
	return nil, apierrors.NewTimeoutError("response lost", 0)
}

func TestEnvironmentRuntimeEnsure(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Creating, nil)
	if c := e.calls("create", "pods"); len(c) != 1 {
		t.Fatalf("%d pod creates, want 1", len(c))
	}
	pod := e.pod()
	n := e.native()
	if n.Pod == nil || n.Pod.UID != pod.UID || n.Pod.Altered || n.Sends != 1 || n.Resolved != 1 ||
		n.Write != nil || n.VolumeUID != "pv-uid-1" {
		t.Fatalf("native = %+v", n)
	}
	sum := environmentSpecHash(e.ref, environmentTestProfile())
	if got := pod.Annotations[environmentSpecAnnotation]; got != hex.EncodeToString(sum[:]) {
		t.Fatalf("spec annotation = %q", got)
	}
	for k, v := range pod.Annotations {
		if strings.Contains(v, envRTNonce) || strings.Contains(k, envRTNonce) {
			t.Fatalf("nonce in annotation %s", k)
		}
	}
	e.run("node-1")
	o, err = e.ensure(e.rt)
	e.want(o, err, environment.Ready, nil)

	// A restarted runtime writes nothing for a recorded Pod.
	before := e.ref.Native
	o, err = e.ensure(e.runtime(environmentTestProfile()))
	e.want(o, err, environment.Ready, nil)
	if !bytes.Equal(e.ref.Native, before) || len(e.calls("create", "pods")) != 1 {
		t.Fatal("Ensure of a recorded Pod changed something")
	}
	e.borrowedIntact()
}

// TestEnvironmentRuntimeDeclaresFirst checks that a send is declared in a
// returned Native before it is sent, and sent only by the declaring
// process in the call that passes that declaration back.
func TestEnvironmentRuntimeDeclaresFirst(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	accepted := e.ref
	o, err := e.rt.Ensure(context.Background(), accepted)
	e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
	e.commit(o)
	if n := e.native(); n.Sends != 1 || n.Resolved != 0 || n.Write == nil || n.Write.Op != environmentWriteCreate ||
		n.VolumeUID != "pv-uid-1" || n.Pod != nil {
		t.Fatalf("native = %+v", n)
	}
	if c := e.calls("create", "pods"); len(c) != 0 {
		t.Fatal("a pod create before the declaration was committed")
	}

	// The declaration's commit failed: a call with the committed Native
	// sends nothing and declares again.
	again, err := e.rt.Ensure(context.Background(), accepted)
	e.want(again, err, environment.Creating, environmentcontrol.ErrWritePending)
	if bytes.Equal(again.Native, o.Native) || len(e.calls("create", "pods")) != 0 {
		t.Fatal("an uncommitted declaration was sent or reused")
	}

	// Only the latest declaration is sent: the first may have been
	// committed after all, and a call with it treats it as possibly sent.
	stale, err := e.rt.Ensure(context.Background(), e.ref)
	e.want(stale, err, environment.Unknown, errEnvironmentUnresolved)
	if c := e.calls("create", "pods"); len(c) != 0 {
		t.Fatalf("%d pod creates, want 0", len(c))
	}

	// Another process may not send a declaration either.
	e.ref.Native = again.Native
	other, err := e.runtime(environmentTestProfile()).Ensure(context.Background(), e.ref)
	e.want(other, err, environment.Unknown, errEnvironmentUnresolved)
	e.commit(other)
	if n := e.native(); n.Write != nil || n.Sends != 1 || n.Resolved != 0 {
		t.Fatalf("native after another process = %+v", n)
	}
	if c := e.calls("create", "pods"); len(c) != 0 {
		t.Fatalf("%d pod creates, want 0", len(c))
	}
}

// TestEnvironmentRuntimeRestartUnresolved: after a restart a declared send
// may have been sent, and an empty name proves nothing. The environment
// stays Unknown, and Deleting through Delete, with no send.
func TestEnvironmentRuntimeRestartUnresolved(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	o, err := e.rt.Ensure(context.Background(), e.ref)
	e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
	e.commit(o)
	rt := e.runtime(environmentTestProfile())
	for range 3 {
		o, err := e.ensure(rt)
		e.want(o, err, environment.Unknown, errEnvironmentUnresolved)
	}
	if n := e.native(); n.Sends != 1 || n.Resolved != 0 || n.Write != nil || n.Pod != nil {
		t.Fatalf("native = %+v", n)
	}
	for range 3 {
		o, err := e.delete(rt)
		e.want(o, err, environment.Deleting, errEnvironmentUnresolved)
	}
	if len(e.calls("create", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("a write after a restart with an unresolved send")
	}
	e.borrowedIntact()
}

// TestEnvironmentRuntimeDelayedCreate: the first Create's response is lost
// and it lands only after empty reads and a Delete. It is never resent, and
// Delete stops and deletes the Pod it made, by UID, before Deleted.
func TestEnvironmentRuntimeDelayedCreate(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	var held ktesting.Action
	e.f.on("create", "pods", 1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
		held = a // delivered later
		return lostCreate(a, next)
	})
	o, err := e.ensure(e.rt)
	if o.Phase != environment.Unknown || !apierrors.IsTimeout(err) {
		t.Fatalf("Ensure = %v, %v", o.Phase, err)
	}
	for range 2 {
		o, err := e.ensure(e.rt)
		e.want(o, err, environment.Unknown, errEnvironmentUnresolved)
	}
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleting, errEnvironmentUnresolved)
	if e.pod() != nil || len(e.calls("create", "pods")) != 1 {
		t.Fatal("the lost Create was resent")
	}

	if _, err := e.f.replay(held); err != nil {
		t.Fatal(err)
	}
	late := e.pod().UID
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	dels := e.calls("delete", "pods")
	if len(dels) != 1 || *dels[0].del.Preconditions.UID != late || e.pod() != nil {
		t.Fatalf("pod deletes = %+v", dels)
	}
	if n := e.native(); n.Sends != 1 || n.Resolved != 1 || n.Pod != nil {
		t.Fatalf("native = %+v", n)
	}
	if len(e.calls("create", "pods")) != 1 {
		t.Fatal("a second Create")
	}
	e.borrowedIntact()
}

func TestEnvironmentRuntimeNoUncertainResend(t *testing.T) {
	for _, tt := range []struct {
		name    string
		applied bool
	}{
		{"applied", true},
		{"not applied", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			e.f.on("create", "pods", 1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
				if tt.applied {
					next()
				}
				return lostCreate(a, next)
			})
			if _, err := e.ensure(e.rt); !apierrors.IsTimeout(err) {
				t.Fatalf("Ensure = %v, want the lost response", err)
			}
			for range 3 {
				o, err := e.ensure(e.rt)
				if tt.applied {
					e.want(o, err, environment.Creating, nil)
				} else {
					e.want(o, err, environment.Unknown, errEnvironmentUnresolved)
				}
			}
			if c := e.calls("create", "pods"); len(c) != 1 {
				t.Fatalf("%d pod creates, want 1", len(c))
			}
			n := e.native()
			if tt.applied && (n.Pod == nil || n.Pod.UID != e.pod().UID || n.Resolved != 1) ||
				!tt.applied && (n.Pod != nil || n.Resolved != 0) {
				t.Fatalf("native = %+v", n)
			}
		})
	}
}

// TestEnvironmentRuntimeLateResult: a send's result arrives after its call
// returned, while Delete has started. Delete stays Deleting while the send
// is in flight, then recovers the UID from the late response.
func TestEnvironmentRuntimeLateResult(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ensure bool // the late result is recorded by Ensure, not Delete
	}{
		{"delete", false},
		{"ensure", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			entered, release := make(chan struct{}), make(chan struct{})
			e.rt.pods.(*funcPods).createFunc = func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
				close(entered)
				<-release
				return e.podCreate(ctx, pod)
			}
			o, err := e.rt.Ensure(context.Background(), e.ref)
			e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
			e.commit(o)
			ctx, cancel := context.WithCancel(context.Background())
			go func() { <-entered; cancel() }()
			o, err = e.rt.Ensure(ctx, e.ref)
			e.want(o, err, environment.Creating, context.Canceled)
			e.commit(o)
			if n := e.native(); n.Write != nil || n.Sends != 1 || n.Resolved != 0 {
				t.Fatalf("native = %+v", n)
			}
			c := e.rt.createFrame(envRTID, 1)
			if c == nil {
				t.Fatal("create frame not kept")
			}
			if tt.ensure {
				o, err = e.ensure(e.rt)
				e.want(o, err, environment.Creating, nil)
			} else {
				o, err = e.delete(e.rt)
				e.want(o, err, environment.Deleting, errEnvironmentUnresolved)
			}
			close(release)
			receive(t, "create frame", c.done)
			created := e.pod()
			if created == nil {
				t.Fatal("late Create not applied")
			}
			if tt.ensure {
				o, err = e.ensure(e.rt)
				e.want(o, err, environment.Creating, nil)
				if n := e.native(); n.Pod == nil || n.Pod.UID != created.UID || n.Resolved != 1 {
					t.Fatalf("native = %+v", n)
				}
			}
			o, err = e.delete(e.rt)
			e.want(o, err, environment.Deleted, nil)
			if dels := e.calls("delete", "pods"); len(dels) != 1 || *dels[0].del.Preconditions.UID != created.UID {
				t.Fatalf("pod deletes = %+v", dels)
			}
			if len(e.calls("create", "pods")) != 1 {
				t.Fatal("a second Create")
			}
		})
	}
}

// TestEnvironmentRuntimeResultCommitLost: the Native that records a Pod's
// UID was never committed. A later call with the declaration recovers the
// same Pod by its nonce and never sends again.
func TestEnvironmentRuntimeResultCommitLost(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint("restart=", restart), func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			o, err := e.rt.Ensure(context.Background(), e.ref)
			e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
			e.commit(o)
			o, err = e.rt.Ensure(context.Background(), e.ref)
			e.want(o, err, environment.Creating, nil)
			// o is not committed.
			rt := e.rt
			if restart {
				rt = e.runtime(environmentTestProfile())
			}
			o, err = e.ensure(rt)
			e.want(o, err, environment.Creating, nil)
			if n := e.native(); n.Pod == nil || n.Pod.UID != e.pod().UID || n.Sends != 1 || n.Resolved != 1 {
				t.Fatalf("native = %+v", n)
			}
			if c := e.calls("create", "pods"); len(c) != 1 {
				t.Fatalf("%d pod creates, want 1", len(c))
			}
		})
	}
}

// TestEnvironmentRuntimeCreateRacesDelete: Delete passes a declared send
// that was never sent. The declaring process knows it was not sent; any
// other process must keep it unresolved.
func TestEnvironmentRuntimeCreateRacesDelete(t *testing.T) {
	for _, tt := range []struct {
		name  string
		other bool
		phase environment.Phase
		err   error
	}{
		{"declaring process", false, environment.Deleted, nil},
		{"other process", true, environment.Deleting, errEnvironmentUnresolved},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			o, err := e.rt.Ensure(context.Background(), e.ref)
			e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
			e.commit(o)
			rt := e.rt
			if tt.other {
				rt = e.runtime(environmentTestProfile())
			}
			for range 2 {
				o, err := e.delete(rt)
				e.want(o, err, tt.phase, tt.err)
			}
			if !e.native().Closing {
				t.Fatal("Delete not recorded")
			}
			// The declaration is gone: nothing sends it.
			o, err = e.rt.Ensure(context.Background(), e.ref)
			e.want(o, err, 0, errEnvironmentDeleting)
			if len(e.calls("create", "pods")) != 0 {
				t.Fatal("a Create after Delete")
			}
		})
	}
}

// TestEnvironmentPodWriteOneSend checks that each Pod write makes one
// request, with the typed client's body, even when the response invites
// client-go to resend it.
func TestEnvironmentPodWriteOneSend(t *testing.T) {
	status := func(code int) func(w http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status: metav1.StatusFailure, Code: int32(code)})
		}
	}
	firsts := []struct {
		name  string
		first func(w http.ResponseWriter) // nil succeeds
	}{
		{"succeeded", nil},
		{"throttled", status(http.StatusTooManyRequests)},
		{"unavailable", status(http.StatusServiceUnavailable)},
		{"timeout", status(http.StatusGatewayTimeout)},
		{"dropped", func(w http.ResponseWriter) {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				conn.Close()
			}
		}},
	}
	uid, rv := types.UID("pod-uid-1"), "7"
	deadline := environmentStopDeadline
	stopped := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envRTPod, Namespace: "workloads", UID: uid, ResourceVersion: rv},
		Spec: corev1.PodSpec{ActiveDeadlineSeconds: &deadline}}
	writes := []struct {
		method, path string
		code         int // of a successful response
		write        func(environmentPodClient) error
		body         func(t *testing.T, raw []byte) // checks the request body
	}{
		{"POST", "/api/v1/namespaces/workloads/pods", http.StatusCreated, func(c environmentPodClient) error {
			pod, err := c.create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envRTPod}})
			if err == nil && pod.UID != uid {
				return fmt.Errorf("created uid %q", pod.UID)
			}
			return err
		}, func(t *testing.T, raw []byte) {}},
		{"PUT", "/api/v1/namespaces/workloads/pods/" + envRTPod, http.StatusOK, func(c environmentPodClient) error {
			_, err := c.update(context.Background(), stopped.DeepCopy())
			return err
		}, func(t *testing.T, raw []byte) {
			var got corev1.Pod
			if err := json.Unmarshal(raw, &got); err != nil || got.UID != uid || got.ResourceVersion != rv ||
				got.Spec.ActiveDeadlineSeconds == nil || *got.Spec.ActiveDeadlineSeconds != deadline {
				t.Errorf("PUT body = %s, %v", raw, err)
			}
		}},
		{"DELETE", "/api/v1/namespaces/workloads/pods/" + envRTPod, http.StatusOK, func(c environmentPodClient) error {
			return c.remove(context.Background(), envRTPod, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		}, func(t *testing.T, raw []byte) {
			var got metav1.DeleteOptions
			if err := json.Unmarshal(raw, &got); err != nil || got.Preconditions == nil ||
				got.Preconditions.UID == nil || *got.Preconditions.UID != uid ||
				got.Preconditions.ResourceVersion == nil || *got.Preconditions.ResourceVersion != rv {
				t.Errorf("DELETE body = %s, %v", raw, err)
			}
		}},
	}
	for _, w := range writes {
		for _, tt := range firsts {
			t.Run(w.method+"/"+tt.name, func(t *testing.T) {
				var mu sync.Mutex
				var requests []string
				server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					mu.Lock()
					requests = append(requests, r.Method+" "+r.URL.Path)
					n := len(requests)
					mu.Unlock()
					w.body(t, raw)
					if n == 1 && tt.first != nil {
						tt.first(rw)
						return
					}
					rw.Header().Set("Content-Type", "application/json")
					rw.WriteHeader(w.code)
					if w.method == "DELETE" {
						json.NewEncoder(rw).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
							Status: metav1.StatusSuccess})
						return
					}
					json.NewEncoder(rw).Encode(&corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
						ObjectMeta: metav1.ObjectMeta{Name: envRTPod, Namespace: "workloads", UID: uid}})
				}))
				defer server.Close()
				c, err := newEnvironmentPodClient(&rest.Config{Host: server.URL}, "workloads")
				if err != nil {
					t.Fatal(err)
				}
				err = w.write(c)
				mu.Lock()
				defer mu.Unlock()
				// The count comes first: a resend that succeeds also
				// changes the outcome.
				if !slices.Equal(requests, []string{w.method + " " + w.path}) {
					t.Fatalf("requests = %q, want one %s %s", requests, w.method, w.path)
				}
				if (err == nil) != (tt.first == nil) {
					t.Fatalf("%s = %v", w.method, err)
				}
			})
		}
	}
}

func TestEnvironmentRuntimeCreateRefused(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.f.on("create", "pods", 1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
		return nil, apierrors.NewForbidden(corev1.Resource("pods"), envRTPod, errors.New("quota"))
	})
	o, err := e.ensure(e.rt)
	if o.Phase != environment.Creating || !apierrors.IsForbidden(err) {
		t.Fatalf("Ensure = %v, %v", o.Phase, err)
	}
	if n := e.native(); n.Sends != 1 || n.Resolved != 1 || n.Pod != nil {
		t.Fatalf("native = %+v", n)
	}
	// A refused send is resolved; the next call declares another.
	o, err = e.ensure(e.rt)
	e.want(o, err, environment.Creating, nil)
	if n := e.native(); n.Sends != 2 || n.Resolved != 2 || n.Pod == nil || len(e.calls("create", "pods")) != 2 {
		t.Fatalf("native = %+v", n)
	}
}

// TestEnvironmentRuntimeCreateOutcome checks which Create responses
// resolve a send as not applied.
func TestEnvironmentRuntimeCreateOutcome(t *testing.T) {
	gr := corev1.Resource("pods")
	for _, tt := range []struct {
		name     string
		err      error
		resolved bool
	}{
		{"already exists", apierrors.NewAlreadyExists(gr, envRTPod), true},
		{"forbidden", apierrors.NewForbidden(gr, envRTPod, errors.New("quota")), true},
		{"invalid", apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("Pod").GroupKind(), envRTPod, nil), true},
		{"bad request", apierrors.NewBadRequest("proxy"), false},
		{"not found", apierrors.NewNotFound(gr, envRTPod), false},
		{"unauthorized", apierrors.NewUnauthorized("proxy"), false},
		{"conflict", apierrors.NewConflict(gr, envRTPod, errors.New("conflict")), false},
		{"throttled", apierrors.NewTooManyRequests("busy", 1), false},
		{"internal", apierrors.NewInternalError(errors.New("etcd")), false},
		{"timeout", apierrors.NewTimeoutError("lost", 1), false},
		{"dial", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			e.rt.pods.(*funcPods).createFunc = func(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
				return &corev1.Pod{}, tt.err
			}
			if _, err := e.ensure(e.rt); !errors.Is(err, tt.err) {
				t.Fatalf("Ensure = %v", err)
			}
			if n := e.native(); n.Sends != 1 || n.unresolved() == tt.resolved || n.Pod != nil {
				t.Fatalf("native = %+v, want resolved %v", n, tt.resolved)
			}
		})
	}
}

func TestEnvironmentRuntimeCreateLimit(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.f.on("create", "pods", -1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
		return nil, apierrors.NewForbidden(corev1.Resource("pods"), envRTPod, errors.New("quota"))
	})
	for range maxEnvironmentCreates {
		if _, err := e.ensure(e.rt); !apierrors.IsForbidden(err) {
			t.Fatalf("Ensure = %v", err)
		}
	}
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Unknown, errEnvironmentCreateLimit)
	if c := e.calls("create", "pods"); len(c) != maxEnvironmentCreates {
		t.Fatalf("%d pod creates", len(c))
	}
}

// TestEnvironmentRuntimeCreateCollision: another object takes the name
// between the declaration and the send.
func TestEnvironmentRuntimeCreateCollision(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	o, err := e.rt.Ensure(context.Background(), e.ref)
	e.want(o, err, environment.Creating, environmentcontrol.ErrWritePending)
	e.commit(o)
	e.store("pods", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envRTPod, Namespace: "workloads"}})
	o, err = e.ensure(e.rt)
	if o.Phase != environment.Creating || !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Ensure = %v, %v", o.Phase, err)
	}
	o, err = e.ensure(e.rt)
	e.want(o, err, environment.Unknown, backend.ErrIdentityMismatch)
	if n := e.native(); n.Sends != 1 || n.Resolved != 1 || n.Pod != nil || len(e.calls("create", "pods")) != 1 {
		t.Fatalf("native = %+v", n)
	}
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	if len(e.calls("delete", "pods")) != 0 || e.pod() == nil {
		t.Fatal("Delete touched the other object")
	}
}

func TestEnvironmentRuntimeForeignName(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.store("pods", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: envRTPod, Namespace: "workloads",
		Labels: map[string]string{ownerLabel: "owner", idLabel: envRTID},
		Annotations: map[string]string{bindingAnnotation: e.target.Binding,
			environmentSpecAnnotation: strings.Repeat("ab", 32)}}})
	before := e.ref.Native
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Unknown, backend.ErrIdentityMismatch)
	if !bytes.Equal(e.ref.Native, before) || len(e.calls("create", "pods")) != 0 {
		t.Fatal("Ensure declared or sent with a foreign Pod at the name")
	}
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	for _, verb := range []string{"update", "delete"} {
		if c := e.calls(verb, "pods"); len(c) != 0 {
			t.Fatalf("%d %s calls on the foreign Pod", len(c), verb)
		}
	}
	if e.pod() == nil {
		t.Fatal("foreign Pod removed")
	}
	e.borrowedIntact()
}

func TestEnvironmentRuntimeCreateAltered(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.f.on("create", "pods", 1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
		a.(ktesting.CreateActionImpl).Object.(*corev1.Pod).Spec.Containers[0].ImagePullPolicy = corev1.PullAlways
		return next()
	})
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Unknown, backend.ErrIdentityMismatch)
	n := e.native()
	if n.Pod == nil || !n.Pod.Altered || n.Pod.UID != e.pod().UID {
		t.Fatalf("native = %+v", n)
	}
	o, err = e.ensure(e.rt)
	e.want(o, err, environment.Unknown, backend.ErrIdentityMismatch)
	if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentNotReady) {
		t.Fatalf("Exec = %v", err)
	}
	// Ours by our own Create response: Delete still removes it.
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	if e.pod() != nil {
		t.Fatal("altered Pod kept")
	}
}

func TestEnvironmentRuntimeObserve(t *testing.T) {
	for _, tt := range []struct {
		name  string
		edit  func(e *envRT)
		phase environment.Phase
	}{
		{"ready", func(e *envRT) {}, environment.Ready},
		{"pod replaced", func(e *envRT) {
			p := e.pod()
			e.f.remove("pods", envRTPod)
			p.UID = "foreign-uid"
			e.store("pods", p)
		}, environment.Unknown},
		{"pod missing", func(e *envRT) { e.f.remove("pods", envRTPod) }, environment.Unknown},
		{"pod image edited", func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) {
				o.(*corev1.Pod).Spec.Containers[0].Image = "registry.example.com/other@sha256:" + strings.Repeat("f", 64)
			})
		}, environment.Unknown},
		{"pod deleting", func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) { now := metav1.Now(); o.(*corev1.Pod).DeletionTimestamp = &now })
		}, environment.Unknown},
		{"pod stopped", func(e *envRT) { e.kill() }, environment.Unknown},
		{"pod not ready", func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) { o.(*corev1.Pod).Status.ContainerStatuses[0].Ready = false })
		}, environment.Creating},
		{"volume replaced", func(e *envRT) {
			e.f.remove("persistentvolumes", "pv-alice-home")
			e.store("persistentvolumes", envRTVolume("pv-uid-2"))
		}, environment.Unknown},
		{"volume bound elsewhere", func(e *envRT) {
			e.f.edit("persistentvolumes", "pv-alice-home", func(o runtime.Object) {
				o.(*corev1.PersistentVolume).Spec.ClaimRef.UID = "claim-uid-2"
			})
		}, environment.Unknown},
		{"volume missing", func(e *envRT) { e.f.remove("persistentvolumes", "pv-alice-home") }, environment.Unknown},
		{"volume released", func(e *envRT) {
			e.f.edit("persistentvolumes", "pv-alice-home", func(o runtime.Object) {
				o.(*corev1.PersistentVolume).Status.Phase = corev1.VolumeReleased
			})
		}, environment.Unknown},
		{"claim deleting", func(e *envRT) {
			e.f.edit("persistentvolumeclaims", "alice-home", func(o runtime.Object) {
				now := metav1.Now()
				o.(*corev1.PersistentVolumeClaim).DeletionTimestamp = &now
			})
		}, environment.Unknown},
		{"namespace replaced", func(e *envRT) {
			e.f.edit("namespaces", "workloads", func(o runtime.Object) { o.(*corev1.Namespace).UID = "namespace-uid-2" })
		}, environment.Unknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			e.created()
			e.run("node-1")
			tt.edit(e)
			before := e.ref.Native
			o, _ := e.ensure(e.rt)
			if o.Phase != tt.phase {
				t.Fatalf("phase = %v, want %v", o.Phase, tt.phase)
			}
			if tt.phase != environment.Ready {
				if _, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentNotReady) {
					t.Fatalf("Exec = %v", err)
				}
			}
			if !bytes.Equal(e.ref.Native, before) || len(e.calls("update", "pods")) != 0 || len(e.calls("create", "pods")) != 1 {
				t.Fatal("observing a recorded Pod wrote")
			}
		})
	}
}

func TestEnvironmentRuntimeResolveRefuses(t *testing.T) {
	for _, tt := range []struct {
		name string
		edit func(e *envRT, ref *environmentcontrol.Ref)
	}{
		{"binding", func(e *envRT, ref *environmentcontrol.Ref) { ref.Binding = "other" }},
		{"generation", func(e *envRT, ref *environmentcontrol.Ref) { ref.Identity.Generation = 2 }},
		{"installation", func(e *envRT, ref *environmentcontrol.Ref) { ref.Identity.Installation = "" }},
		{"nonce", func(e *envRT, ref *environmentcontrol.Ref) { ref.Nonce = "0011" }},
		{"digest", func(e *envRT, ref *environmentcontrol.Ref) { ref.SpecDigest[0] ^= 1 }},
		{"principal", func(e *envRT, ref *environmentcontrol.Ref) { ref.Principal = "bob" }},
		{"storage alias", func(e *envRT, ref *environmentcontrol.Ref) {
			ref.Spec.StorageAlias = "scratch"
			ref.SpecDigest = envRTDigest(e.t, ref.Spec)
		}},
		{"no limits", func(e *envRT, ref *environmentcontrol.Ref) {
			ref.Spec.CPULimitMillis = 0
			ref.SpecDigest = envRTDigest(e.t, ref.Spec)
		}},
		{"native version", func(e *envRT, ref *environmentcontrol.Ref) {
			ref.Native = bytes.Replace(ref.Native, []byte(`"version":1`), []byte(`"version":2`), 1)
		}},
		{"no native", func(e *envRT, ref *environmentcontrol.Ref) { ref.Native = nil }},
		{"profile revision", func(e *envRT, ref *environmentcontrol.Ref) {
			p := environmentTestProfile()
			p.revision = "profile-2"
			e.rt = e.runtime(p)
		}},
		{"profile claim", func(e *envRT, ref *environmentcontrol.Ref) {
			p := environmentTestProfile()
			p.pvcUID = "claim-uid-2"
			e.rt = e.runtime(p)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnvRT(t, &rest.Config{Host: envRTHost})
			ref := e.ref
			tt.edit(e, &ref)
			if o, err := e.rt.Ensure(context.Background(), ref); err == nil || errors.Is(err, environmentcontrol.ErrWritePending) || o.Native != nil {
				t.Fatalf("Ensure = %+v, %v", o, err)
			}
			if o, err := e.rt.Delete(context.Background(), ref); err == nil || o.Native != nil {
				t.Fatalf("Delete = %+v, %v", o, err)
			}
			if _, err := e.rt.Exec(ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); err == nil {
				t.Fatal("Exec succeeded")
			}
			w := httptest.NewRecorder()
			f := e.rt.Forward(w, httptest.NewRequest(http.MethodGet, "/", nil), ref)
			if res := f.Result(); !res.Settled || res.Err == nil || w.Code != http.StatusServiceUnavailable {
				t.Fatalf("Forward = %+v, %d", res, w.Code)
			}
			if len(e.f.calls) != 0 {
				t.Fatalf("%d API calls after a refused reference", len(e.f.calls))
			}
		})
	}
}

// TestEnvironmentRuntimeDeleteStops follows a running Pod through its
// declared stop and stop evidence to Deleted.
func TestEnvironmentRuntimeDeleteStops(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	e.run("node-1")
	rv := e.pod().ResourceVersion
	o, err := e.rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
	e.commit(o)
	if n := e.native(); !n.Closing || n.Write == nil || n.Write.Op != environmentWriteStop || n.Write.ResourceVersion != rv {
		t.Fatalf("native = %+v", n)
	}
	if len(e.calls("update", "pods")) != 0 {
		t.Fatal("stop sent before its declaration was committed")
	}
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleting, nil)
	ups := e.calls("update", "pods")
	if len(ups) != 1 {
		t.Fatalf("%d pod updates, want 1", len(ups))
	}
	body := ups[0].obj.(*corev1.Pod)
	if d := body.Spec.ActiveDeadlineSeconds; d == nil || *d != environmentStopDeadline || body.ResourceVersion != rv || body.UID != e.native().Pod.UID {
		t.Fatalf("stop update = deadline %v, rv %q, uid %q", d, body.ResourceVersion, body.UID)
	}
	if e.native().Stopped {
		t.Fatal("stop recorded before evidence")
	}
	// A deadline that is set but not yet enforced is not resent.
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleting, nil)
	if len(e.calls("update", "pods")) != 1 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("Delete resent the stop or deleted without evidence")
	}
	o, err = e.ensure(e.rt)
	e.want(o, err, 0, errEnvironmentDeleting)

	e.kill()
	killed := e.pod()
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	dels := e.calls("delete", "pods")
	if len(dels) != 1 || *dels[0].del.Preconditions.UID != killed.UID || *dels[0].del.Preconditions.ResourceVersion != killed.ResourceVersion {
		t.Fatalf("pod deletes = %+v", dels)
	}
	if n := e.native(); n.Pod != nil || n.Write != nil {
		t.Fatalf("native = %+v", n)
	}
	o, err = e.delete(e.rt)
	e.want(o, err, environment.Deleted, nil)
	e.borrowedIntact()
}

// TestEnvironmentRuntimeDeleteDeclaredElsewhere: a stop declared by
// another process is not sent; it is declared again and sent once.
func TestEnvironmentRuntimeDeleteDeclaredElsewhere(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	e.run("node-1")
	o, err := e.rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
	e.commit(o)
	rt := e.runtime(environmentTestProfile())
	o, err = rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
	if len(e.calls("update", "pods")) != 0 {
		t.Fatal("a stop sent by a process that did not declare it")
	}
	e.commit(o)
	o, err = e.delete(rt)
	e.want(o, err, environment.Deleting, nil)
	if len(e.calls("update", "pods")) != 1 {
		t.Fatal("stop not sent once")
	}
}

func TestEnvironmentRuntimeDeleteUnscheduled(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	// The resourceVersion moves between the declaration and the DELETE.
	e.f.on("delete", "pods", 1, func(a ktesting.Action, next func() (runtime.Object, error)) (runtime.Object, error) {
		return nil, apierrors.NewConflict(corev1.Resource("pods"), envRTPod, errors.New("precondition failed"))
	})
	for range 2 {
		o, err := e.rt.Delete(context.Background(), e.ref)
		e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
		e.commit(o)
		if n := e.native(); n.Stopped || n.Write == nil || n.Write.Op != environmentWriteRemoveUnbound || e.pod() == nil {
			t.Fatalf("native = %+v", n)
		}
	}
	o, err := e.rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleted, nil)
	if len(e.calls("delete", "pods")) != 2 || len(e.calls("update", "pods")) != 0 || e.pod() != nil {
		t.Fatal("unscheduled Pod not deleted directly")
	}
}

// TestEnvironmentRuntimeDeleteUnproven: a Pod removed by another writer
// before any stop evidence is Unknown, never Deleted.
func TestEnvironmentRuntimeDeleteUnproven(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	e.run("node-1")
	e.f.remove("pods", envRTPod)
	for range 3 {
		o, err := e.delete(e.rt)
		e.want(o, err, environment.Unknown, errEnvironmentStopUnknown)
	}
	o, err := e.delete(e.runtime(environmentTestProfile()))
	e.want(o, err, environment.Unknown, errEnvironmentStopUnknown)
	e.borrowedIntact()
}

func TestEnvironmentRuntimeExec(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvRT(t, a.config())
	e.created()
	e.run("node-1")
	seen := a.serveExec(execScript{stdin: true, stdout: []byte("out"), stderr: []byte("err"), status: exitStatus("3")})
	x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/cat", "-"}, Stdin: []byte("in")})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, "exec", x.Done())
	res := x.Result()
	if !res.Settled || !res.Exited || res.Refused || res.Code != 3 || string(res.Stdout) != "out" || string(res.Stderr) != "err" {
		t.Fatalf("result = %+v", res)
	}
	if s := receive(t, "exec server", seen); s.err != nil || string(s.stdin) != "in" {
		t.Fatalf("server saw %+v", s)
	}
	reqs := a.requestList()
	if len(reqs) != 1 {
		t.Fatalf("%d requests", len(reqs))
	}
	q := reqs[0].URL.Query()
	if reqs[0].URL.Path != "/api/v1/namespaces/workloads/pods/"+envRTPod+"/exec" ||
		!slices.Equal(q["container"], []string{environmentContainer}) || !slices.Equal(q["command"], []string{"/bin/cat", "-"}) {
		t.Fatalf("exec request = %v", reqs[0].URL)
	}
	if e.rt.activeFrames() != 0 {
		t.Fatal("settled exec kept its slot")
	}
}

func TestEnvironmentRuntimeExecRefused(t *testing.T) {
	for _, tt := range []struct {
		name    string
		serve   func(w http.ResponseWriter, r *http.Request) bool
		refused bool
	}{
		{"forbidden", func(w http.ResponseWriter, r *http.Request) bool {
			http.Error(w, "forbidden", http.StatusForbidden)
			return true
		}, true},
		{"redirect", func(w http.ResponseWriter, r *http.Request) bool {
			http.Redirect(w, r, "https://elsewhere.example.com/", http.StatusFound)
			return true
		}, true},
		{"dropped after the request", func(w http.ResponseWriter, r *http.Request) bool {
			if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
				conn.Close()
			}
			return true
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			e := newEnvRT(t, a.config())
			e.created()
			e.run("node-1")
			a.mu.Lock()
			a.override = tt.serve
			a.mu.Unlock()
			x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}})
			if err != nil {
				t.Fatal(err)
			}
			receive(t, "exec", x.Done())
			res := x.Result()
			if !res.Settled || res.Exited || res.Refused != tt.refused || res.Err == nil {
				t.Fatalf("result = %+v", res)
			}
		})
	}
}

func TestExecRefused(t *testing.T) {
	for _, tt := range []struct {
		name   string
		dialed bool
		err    error
		want   bool
	}{
		{"never dialed", false, errEnvCanceled, true},
		{"refused", true, fmt.Errorf("upgrade: %w", errEnvRefused), true},
		{"redirect", true, errors.Join(errEnvRedirect, errEnvRefused), true},
		{"protocol", true, errEnvProtocol, true},
		{"proxy refused", true, errEnvProxyRefused, true},
		{"dial failed", true, fmt.Errorf("dial kubernetes environment connection: %w",
			&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}), true},
		{"read failed", true, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}, false},
		{"canceled after the request", true, errEnvCanceled, false},
		{"lost connection", true, io.ErrUnexpectedEOF, false},
		{"no error", true, nil, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			x := &execFrame{envFrame: newEnvFrame()}
			x.dialed = tt.dialed
			if got := execRefused(x, tt.err); got != tt.want {
				t.Fatalf("execRefused = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnvironmentRuntimeExecPodChanged(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvRT(t, a.config())
	e.created()
	e.run("node-1")
	hold := make(chan struct{})
	a.serveExec(execScript{status: successStatus, hold: hold})
	x, err := e.rt.Exec(e.ref, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "exec request", func() bool { return len(a.requestList()) == 1 })
	p := e.pod()
	e.f.remove("pods", envRTPod)
	p.UID = "replacement-uid"
	e.store("pods", p)
	close(hold)
	receive(t, "exec", x.Done())
	if res := x.Result(); !res.Settled || res.Exited || res.Refused || !errors.Is(res.Err, errEnvironmentPodChanged) {
		t.Fatalf("result = %+v", res)
	}
	if e.rt.activeFrames() != 0 {
		t.Fatal("settled exec kept its slot")
	}
}

// TestEnvironmentRuntimeDeleteClosesFrames: Delete cancels a running exec,
// which reports no exit, and refuses new frames before its stop is even
// committed.
func TestEnvironmentRuntimeDeleteClosesFrames(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvRT(t, a.config())
	e.created()
	e.run("node-1")
	hold := make(chan struct{})
	defer close(hold)
	a.serveExec(execScript{status: successStatus, hold: hold})
	ready := e.ref
	x, err := e.rt.Exec(ready, environmentcontrol.ExecRequest{Argv: []string{"/bin/sleep", "1000"}})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, "exec request", func() bool { return len(a.requestList()) == 1 })
	o, err := e.rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
	receive(t, "exec", x.Done())
	if res := x.Result(); res.Exited || res.Refused || !errors.Is(res.Err, errEnvCanceled) {
		t.Fatalf("canceled exec = %+v", res)
	}
	if _, err := e.rt.Exec(ready, environmentcontrol.ExecRequest{Argv: []string{"/bin/true"}}); !errors.Is(err, errEnvironmentDeleting) {
		t.Fatalf("Exec after Delete = %v", err)
	}
	if len(a.requestList()) != 1 {
		t.Fatal("an exec after Delete reached the server")
	}
	eventually(t, "slot release", func() bool { return e.rt.activeFrames() == 0 })
}

func TestEnvironmentRuntimeExecCancel(t *testing.T) {
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
	x.Cancel()
	receive(t, "exec", x.Done())
	if res := x.Result(); res.Exited || res.Refused || res.Err == nil {
		t.Fatalf("canceled exec = %+v", res)
	}
	// The environment and its other work are untouched.
	if len(e.calls("update", "pods")) != 0 || len(e.calls("delete", "pods")) != 0 {
		t.Fatal("exec cancel wrote to the Pod")
	}
	if e.rt.activeFrames() != 0 {
		t.Fatal("settled exec kept its slot")
	}
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Ready, nil)
}

func settledPortFrame() *portFrame {
	p := &portFrame{envFrame: newEnvFrame()}
	p.enter()
	p.leave(nil)
	return p
}

func TestEnvironmentRuntimeForward(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	var served []portTarget
	e.rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		served = append(served, target)
		w.WriteHeader(http.StatusNoContent)
		return settledPortFrame()
	}
	forward := func(ref environmentcontrol.Ref) (int, environmentcontrol.ForwardResult) {
		w := httptest.NewRecorder()
		f := e.rt.Forward(w, httptest.NewRequest(http.MethodGet, "/events", nil), ref)
		receive(t, "forward", f.Done())
		return w.Code, f.Result()
	}
	if code, res := forward(e.ref); code != http.StatusServiceUnavailable || res.Err == nil || res.Status != code {
		t.Fatalf("Forward before ready = %d, %+v", code, res)
	}
	e.run("node-1")
	noPort := e.ref
	noPort.Spec.Port = 0
	noPort.SpecDigest = envRTDigest(t, noPort.Spec)
	if code, res := forward(noPort); code != http.StatusServiceUnavailable || res.Err == nil {
		t.Fatalf("Forward without a port = %d, %+v", code, res)
	}
	if code, res := forward(e.ref); code != http.StatusNoContent || !res.Settled || res.Err != nil {
		t.Fatalf("Forward = %d, %+v", code, res)
	}
	if !slices.Equal(served, []portTarget{{Pod: envRTPod, Port: environment.Port}}) {
		t.Fatalf("served %v", served)
	}
	eventually(t, "slot release", func() bool { return e.rt.activeFrames() == 0 })
}

// TestEnvironmentRuntimeForwardUnsettled: an exchange whose frame has not
// settled keeps its slot after Forward returns.
func TestEnvironmentRuntimeForwardUnsettled(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	e.run("node-1")
	p := &portFrame{envFrame: newEnvFrame()}
	p.enter() // an owner that has not returned
	e.rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		w.WriteHeader(http.StatusNoContent)
		return p
	}
	f := e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), e.ref)
	if res := f.Result(); res.Settled {
		t.Fatalf("Forward = %+v", res)
	}
	if e.rt.activeFrames() != 1 {
		t.Fatal("unsettled exchange released its slot")
	}
	p.leave(nil)
	receive(t, "forward", f.Done())
	eventually(t, "slot release", func() bool { return e.rt.activeFrames() == 0 })
}

func TestEnvironmentRuntimeDeleteCancelsForward(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	e.created()
	e.run("node-1")
	entered := make(chan struct{})
	e.rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		close(entered)
		<-r.Context().Done() // a streaming exchange
		return settledPortFrame()
	}
	done := make(chan environmentcontrol.Forward, 1)
	go func() {
		done <- e.rt.Forward(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/events", nil), e.ref)
	}()
	receive(t, "serve", entered)
	eventually(t, "frame attached", func() bool { return e.rt.attachedFrames(envRTID) == 1 })
	o, err := e.rt.Delete(context.Background(), e.ref)
	e.want(o, err, environment.Deleting, environmentcontrol.ErrWritePending)
	f := receive(t, "Forward", done)
	if res := f.Result(); !res.Settled || res.Err != nil {
		t.Fatalf("Forward = %+v", res)
	}
	eventually(t, "slot release", func() bool { return e.rt.activeFrames() == 0 })
}

func TestEnvironmentRuntimeFrameLimits(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	rt := e.rt
	var keys []uint64
	for range maxFramesPerEnv {
		k, err := rt.reserve(envRTID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	if _, err := rt.reserve(envRTID); err == nil {
		t.Fatal("reserve beyond the per-environment limit")
	}
	rt.release(envRTID, keys[0])
	if _, err := rt.reserve(envRTID); err != nil {
		t.Fatal(err)
	}
	for i := 1; rt.activeFrames() < maxEnvironmentFrames; i++ {
		if _, err := rt.reserve(fmt.Sprintf("%032x", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := rt.reserve(strings.Repeat("f", 32)); err == nil {
		t.Fatal("reserve beyond the runtime limit")
	}
}

func TestEnvironmentPodStopped(t *testing.T) {
	running := corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}
	terminated := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137}}
	for _, tt := range []struct {
		name  string
		phase corev1.PodPhase
		state corev1.ContainerState
		want  bool
	}{
		{"failed terminated", corev1.PodFailed, terminated, true},
		{"succeeded terminated", corev1.PodSucceeded, terminated, true},
		{"failed never started", corev1.PodFailed, corev1.ContainerState{}, true},
		{"failed still running", corev1.PodFailed, running, false},
		{"running", corev1.PodRunning, running, false},
		{"running terminated", corev1.PodRunning, terminated, false},
		{"pending", corev1.PodPending, corev1.ContainerState{}, false},
		{"unknown", corev1.PodUnknown, terminated, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{Status: corev1.PodStatus{Phase: tt.phase,
				ContainerStatuses: []corev1.ContainerStatus{{Name: environmentContainer, State: tt.state}}}}
			if got := environmentPodStopped(pod); got != tt.want {
				t.Fatalf("environmentPodStopped = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnvironmentNativeCodec(t *testing.T) {
	good := environmentNative{Version: environmentNativeVersion, Profile: "profile-1", Alias: "home",
		Namespace: "namespace-uid-1", Claim: "claim-uid-1", Volume: "pv-alice-home"}
	if n, err := decodeEnvironmentNative(good.encode()); err != nil || n.Profile != "profile-1" {
		t.Fatalf("round trip = %+v, %v", n, err)
	}
	sent := good
	sent.VolumeUID, sent.Sends, sent.Resolved = "pv", 1, 1
	pod := &environmentNativePod{UID: "pod"}
	for _, tt := range []struct {
		name string
		edit func(*environmentNative)
	}{
		{"version", func(n *environmentNative) { n.Version = 2 }},
		{"bad alias", func(n *environmentNative) { n.Alias = "Home" }},
		{"no profile", func(n *environmentNative) { n.Profile = "" }},
		{"send without volume uid", func(n *environmentNative) { n.Sends = 1 }},
		{"resolved over sends", func(n *environmentNative) { n.VolumeUID, n.Sends, n.Resolved = "pv", 1, 2 }},
		{"two unresolved", func(n *environmentNative) { n.VolumeUID, n.Sends = "pv", 2 }},
		{"too many sends", func(n *environmentNative) {
			n.VolumeUID, n.Sends, n.Resolved = "pv", maxEnvironmentCreates+1, maxEnvironmentCreates+1
		}},
		{"pod without send", func(n *environmentNative) { n.Pod = pod }},
		{"pod while unresolved", func(n *environmentNative) { *n = sent; n.Sends, n.Pod = 2, pod }},
		{"pod without uid", func(n *environmentNative) { *n = sent; n.Pod = &environmentNativePod{} }},
		{"negative generation", func(n *environmentNative) { *n = sent; n.Pod = &environmentNativePod{UID: "pod", Generation: -1} }},
		{"stopped without pod", func(n *environmentNative) { *n = sent; n.Closing, n.Stopped = true, true }},
		{"stopped while open", func(n *environmentNative) { *n = sent; n.Pod, n.Stopped = pod, true }},
		{"create resolved", func(n *environmentNative) {
			*n = sent
			n.Write = &environmentNativeWrite{Op: environmentWriteCreate, Token: envRTToken}
		}},
		{"create with pod", func(n *environmentNative) {
			*n = sent
			n.Sends, n.Pod, n.Write = 2, pod, &environmentNativeWrite{Op: environmentWriteCreate, Token: envRTToken}
		}},
		{"stop while open", func(n *environmentNative) {
			*n = sent
			n.Pod, n.Write = pod, &environmentNativeWrite{Op: environmentWriteStop, Token: envRTToken, ResourceVersion: "7"}
		}},
		{"stop without version", func(n *environmentNative) {
			*n = sent
			n.Pod, n.Closing, n.Write = pod, true, &environmentNativeWrite{Op: environmentWriteStop, Token: envRTToken}
		}},
		{"remove unstopped", func(n *environmentNative) {
			*n = sent
			n.Pod, n.Closing, n.Write = pod, true, &environmentNativeWrite{Op: environmentWriteRemove, Token: envRTToken, ResourceVersion: "7"}
		}},
		{"no token", func(n *environmentNative) {
			*n = sent
			n.Pod, n.Closing, n.Write = pod, true, &environmentNativeWrite{Op: environmentWriteStop, ResourceVersion: "7"}
		}},
		{"unknown write", func(n *environmentNative) {
			*n = sent
			n.Pod, n.Closing, n.Write = pod, true, &environmentNativeWrite{Op: "patch", Token: envRTToken, ResourceVersion: "7"}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := good
			tt.edit(&n)
			if _, err := decodeEnvironmentNative(n.encode()); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("decode = %v", err)
			}
		})
	}
	stopping := sent
	stopping.Pod, stopping.Closing = pod, true
	stopping.Write = &environmentNativeWrite{Op: environmentWriteStop, Token: envRTToken, ResourceVersion: "7"}
	if _, err := decodeEnvironmentNative(stopping.encode()); err != nil {
		t.Fatalf("decode of a declared stop = %v", err)
	}
	raw := good.encode()
	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{"empty", nil},
		{"trailing", append(slices.Clone(raw), ' ')},
		{"second value", append(slices.Clone(raw), "{}"...)},
		{"unknown field", []byte(`{"x":1,` + string(raw[1:]))},
		{"reordered", []byte(`{"profile":"profile-1","version":1,"alias":"home","namespace_uid":"namespace-uid-1","claim_uid":"claim-uid-1","volume":"pv-alice-home"}`)},
		{"too large", append(slices.Clone(raw), bytes.Repeat([]byte(" "), environmentcontrol.MaxNativeBytes)...)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeEnvironmentNative(tt.raw); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("decode = %v", err)
			}
		})
	}
}

func TestEnvironmentRuntimePlan(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	if e.rt.Kind() != environmentKind || e.ref.Binding != e.target.Binding {
		t.Fatalf("kind %q, binding %q", e.rt.Kind(), e.ref.Binding)
	}
	if n := e.native(); n.Profile != "profile-1" || n.Claim != "claim-uid-1" || n.Volume != "pv-alice-home" || n.Sends != 0 {
		t.Fatalf("planned native = %+v", n)
	}
	for _, tt := range []struct {
		name        string
		principal   string
		edit        func(*environment.Spec)
		unsupported bool
	}{
		{"no alias", "alice", func(s *environment.Spec) { s.StorageAlias = "" }, true},
		{"unknown alias", "alice", func(s *environment.Spec) { s.StorageAlias = "scratch" }, true},
		{"other principal", "bob", func(s *environment.Spec) {}, true},
		{"no cpu limit", "alice", func(s *environment.Spec) { s.CPULimitMillis = 0 }, true},
		{"no memory limit", "alice", func(s *environment.Spec) { s.MemoryLimitBytes = 0 }, true},
		{"other image", "alice", func(s *environment.Spec) { s.Image = "registry.example.com/env/other:latest" }, true},
		{"over policy", "alice", func(s *environment.Spec) { s.CPUMillis, s.CPULimitMillis = 8000, 8000 }, true},
		{"invalid spec", "alice", func(s *environment.Spec) { s.Name = "" }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := envRTSpec()
			tt.edit(&s)
			var digest [32]byte
			if raw, err := environment.Canonical(s); err == nil {
				digest = sha256.Sum256(raw)
			}
			_, _, err := e.rt.Plan(tt.principal, s, digest)
			if err == nil || errors.Is(err, environmentcontrol.ErrUnsupported) != tt.unsupported {
				t.Fatalf("Plan = %v", err)
			}
		})
	}
	if _, _, err := e.rt.Plan("alice", envRTSpec(), [32]byte{}); err == nil {
		t.Fatal("Plan accepted another digest")
	}
	if len(e.f.calls) != 0 {
		t.Fatal("Plan made a request")
	}
}

func TestNewEnvironmentRuntime(t *testing.T) {
	e := newEnvRT(t, &rest.Config{Host: envRTHost})
	dup := []environmentPodProfile{environmentTestProfile(), environmentTestProfile()}
	unpinned := environmentTestProfile()
	unpinned.image = "registry.example.com/env/idle:latest"
	for _, tt := range []struct {
		name     string
		target   backend.Target
		config   *rest.Config
		profiles []environmentPodProfile
	}{
		{"unbound", backend.Target{Backend: e.target.Backend, Labels: e.target.Labels}, &rest.Config{Host: envRTHost}, nil},
		{"no config", e.target, nil, nil},
		{"ca file", e.target, &rest.Config{Host: envRTHost, TLSClientConfig: rest.TLSClientConfig{CAFile: "/ca.pem"}}, nil},
		{"other server", e.target, &rest.Config{Host: "https://127.0.0.2:6443"}, nil},
		{"duplicate profile", e.target, &rest.Config{Host: envRTHost}, dup},
		{"invalid profile", e.target, &rest.Config{Host: envRTHost}, []environmentPodProfile{unpinned}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := newEnvironmentRuntime(tt.target, tt.config, tt.profiles); err == nil {
				t.Fatal("newEnvironmentRuntime succeeded")
			}
		})
	}
}
