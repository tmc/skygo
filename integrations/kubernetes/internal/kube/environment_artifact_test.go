package kube

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// MODELED. These tests run the environment runtime against environmentAPIModel, which
// models the API server, and fakeAPI, which models the pods/exec upgrade and
// plays the helper's side of each exec from a script. The kubelet's init
// container status, the downward API uid file, the helper's exit codes and
// the read-only subPath mounts are test edits and scripts, not observed
// behavior; native qualification must confirm each rule (PROPOSAL-1 §7).

func envArtifactBody(s string) (string, uint64) {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:]), uint64(len(s))
}

// The artifact IDs of envArtifactSpec.
const (
	envArtifactID1 = "11111111111111111111111111111111"
	envArtifactID2 = "22222222222222222222222222222222"
)

// envArtifactSpec is envRTSpec with reference envArtifactID1 at two paths
// and envArtifactID2 at one.
func envArtifactSpec() environment.Spec {
	s := envRTSpec()
	sum1, size1 := envArtifactBody("one")
	sum2, size2 := envArtifactBody("second")
	s.Artifacts = []environment.ArtifactMount{
		{ID: envArtifactID1, SHA256: sum1, Size: size1, MountPath: "/skygo/artifacts/app/b.conf"},
		{ID: envArtifactID2, SHA256: sum2, Size: size2, MountPath: "/skygo/artifacts/two"},
		{ID: envArtifactID1, SHA256: sum1, Size: size1, MountPath: "/skygo/artifacts/app/a.conf"},
	}
	return s
}

// newEnvArtifactRT is newEnvRT for envArtifactSpec on a target with a data
// helper image.
func newEnvArtifactRT(t *testing.T, a *fakeAPI) *envRT {
	t.Helper()
	e := newEnvRTData(t, a.config())
	spec := envArtifactSpec()
	digest := envRTDigest(t, spec)
	binding, native, err := e.rt.Plan("alice", spec, digest)
	if err != nil {
		t.Fatal(err)
	}
	e.ref.Spec, e.ref.SpecDigest, e.ref.Binding, e.ref.Native = spec, digest, binding, native
	return e
}

// newEnvRTData is newEnvRT on a target whose configuration has the data
// profile of testDataConfig, so its Pods may have a stage container.
func newEnvRTData(t *testing.T, config *rest.Config) *envRT {
	t.Helper()
	f := newEnvironmentAPIModel(t)
	target, err := fromREST(TargetConfig{Name: "kube", Namespace: "workloads", Resources: envRTPolicy(),
		Data: testDataConfig()}, "owner", config)
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

// staging models the kubelet running the stage init container.
func (e *envRT) staging(node string) {
	e.f.edit("pods", envRTPod, func(o runtime.Object) {
		p := o.(*corev1.Pod)
		p.Spec.NodeName = node
		p.Status = corev1.PodStatus{Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{{Name: environmentStageContainer,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			ContainerStatuses: []corev1.ContainerStatus{{Name: environmentContainer,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}}
	})
}

func r1Mounts(e *envRT) []environment.ArtifactMount {
	var m []environment.ArtifactMount
	for _, a := range e.ref.Spec.Artifacts {
		if a.ID == envArtifactID1 {
			m = append(m, a)
		}
	}
	return m
}

// countingBody records every Read and whether one happened after done.
type countingBody struct {
	r     io.Reader
	mu    sync.Mutex
	reads int
	late  bool
	done  <-chan struct{}
}

func (b *countingBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reads++
	if b.done != nil {
		select {
		case <-b.done:
			b.late = true
		default:
		}
	}
	return b.r.Read(p)
}

func TestEnvironmentArtifacts(t *testing.T) {
	sum, size := envArtifactBody("one")
	other, _ := envArtifactBody("two")
	m := func(id, p string) environment.ArtifactMount {
		return environment.ArtifactMount{ID: id, SHA256: sum, Size: size, MountPath: p}
	}
	arts, err := environmentArtifacts([]environment.ArtifactMount{m("r1", "/b"), m("r2", "/c"), m("r1", "/a")})
	if err != nil {
		t.Fatal(err)
	}
	if len(arts) != 2 || arts[0].id != "r1" || !slices.Equal(arts[0].paths, []string{"/a", "/b"}) ||
		arts[1].id != "r2" || !slices.Equal(arts[1].paths, []string{"/c"}) {
		t.Fatalf("groups = %+v", arts)
	}
	many := func(n int) []environment.ArtifactMount {
		var ms []environment.ArtifactMount
		for i := range n {
			ms = append(ms, m("r"+strings.Repeat("x", i+1), "/m"+strings.Repeat("x", i+1)))
		}
		return ms
	}
	if _, err := environmentArtifacts(many(maxEnvironmentArtifacts)); err != nil {
		t.Errorf("%d references: %v", maxEnvironmentArtifacts, err)
	}
	for _, tt := range []struct {
		name   string
		mounts []environment.ArtifactMount
	}{
		{"relative", []environment.ArtifactMount{m("r1", "etc/a")}},
		{"unclean", []environment.ArtifactMount{m("r1", "/etc//a")}},
		{"root", []environment.ArtifactMount{m("r1", "/")}},
		{"in workspace claim", []environment.ArtifactMount{m("r1", "/workspace/a")}},
		{"workspace itself", []environment.ArtifactMount{m("r1", "/workspace")}},
		{"over helper", []environment.ArtifactMount{m("r1", "/opt")}},
		{"in credential scope", []environment.ArtifactMount{m("r1", "/run/skygo/artifacts/a0")}},
		{"in secrets", []environment.ArtifactMount{m("r1", "/var/run/secrets/x")}},
		{"duplicate path", []environment.ArtifactMount{m("r1", "/a"), m("r2", "/a")}},
		{"nested path", []environment.ArtifactMount{m("r1", "/a"), m("r2", "/a/b")}},
		{"digest differs by id", []environment.ArtifactMount{m("r1", "/a"),
			{ID: "r1", SHA256: other, Size: size, MountPath: "/b"}}},
		{"size differs by id", []environment.ArtifactMount{m("r1", "/a"),
			{ID: "r1", SHA256: sum, Size: size + 1, MountPath: "/b"}}},
		{"uppercase digest", []environment.ArtifactMount{{ID: "r1", SHA256: strings.ToUpper(sum), Size: size, MountPath: "/a"}}},
		{"zero size", []environment.ArtifactMount{{ID: "r1", SHA256: sum, Size: 0, MountPath: "/a"}}},
		{"too large", []environment.ArtifactMount{{ID: "r1", SHA256: sum, Size: maxEnvironmentArtifactBytes + 1, MountPath: "/a"}}},
		{"too many references", many(maxEnvironmentArtifacts + 1)},
	} {
		if _, err := environmentArtifacts(tt.mounts); err == nil {
			t.Errorf("%s: accepted", tt.name)
		}
	}
}

func TestEnvironmentPodArtifacts(t *testing.T) {
	b := newEnvRTData(t, newFakeAPI(t, execProtocol).config()).rt.b
	p := environmentTestProfile()
	in := environmentTestInput()
	plain, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain.Spec.InitContainers) != 0 || len(plain.Spec.Volumes) != 1 || len(plain.Spec.Containers[0].VolumeMounts) != 1 {
		t.Fatalf("pod without artifacts changed: %+v", plain.Spec)
	}

	arts, err := environmentArtifacts(envArtifactSpec().Artifacts)
	if err != nil {
		t.Fatal(err)
	}
	in.artifacts = arts
	pod, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.InitContainers) != 1 {
		t.Fatalf("init containers = %d", len(pod.Spec.InitContainers))
	}
	stage := pod.Spec.InitContainers[0]
	sum1, _ := envArtifactBody("one")
	sum2, _ := envArtifactBody("second")
	if want := []string{helperEntry, environmentWaitVerb, "--", "a0:" + sum1 + ":3", "a1:" + sum2 + ":6"}; !slices.Equal(stage.Command, want) {
		t.Errorf("stage command = %q, want %q", stage.Command, want)
	}
	if stage.Name != environmentStageContainer || stage.Image != b.data.helperImage || stage.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("stage = %+v", stage)
	}
	if _, ok := stage.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Error("stage requests an accelerator")
	}
	var mounts []string
	for _, m := range pod.Spec.Containers[0].VolumeMounts[1:] {
		if m.Name != environmentArtifactVolume || !m.ReadOnly {
			t.Errorf("artifact mount = %+v", m)
		}
		mounts = append(mounts, m.MountPath+"="+m.SubPath)
	}
	if want := []string{"/skygo/artifacts/app/a.conf=a0", "/skygo/artifacts/app/b.conf=a0", "/skygo/artifacts/two=a1"}; !slices.Equal(mounts, want) {
		t.Errorf("artifact mounts = %q, want %q", mounts, want)
	}
	var instance *corev1.DownwardAPIVolumeSource
	for _, v := range pod.Spec.Volumes {
		if v.Name == environmentInstanceVolume {
			instance = v.DownwardAPI
		}
	}
	if instance == nil || len(instance.Items) != 1 || instance.Items[0].Path != "uid" ||
		instance.Items[0].FieldRef == nil || instance.Items[0].FieldRef.FieldPath != "metadata.uid" ||
		instance.Items[0].FieldRef.APIVersion != "v1" || instance.DefaultMode == nil || *instance.DefaultMode != 0o444 {
		t.Errorf("instance volume = %+v", instance)
	}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == environmentInstanceVolume {
			t.Error("workspace container mounts the instance volume")
		}
	}

	// A read-back with the API server's defaults for these fields matches.
	got := pod.DeepCopy()
	got.Spec.InitContainers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	got.Spec.InitContainers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	if !fixedPodSpecMatches(&pod.Spec, &got.Spec) {
		t.Error("defaulted read-back does not match")
	}
	got.Spec.Containers[0].VolumeMounts[1].ReadOnly = false
	if fixedPodSpecMatches(&pod.Spec, &got.Spec) {
		t.Error("writable artifact mount matches")
	}

	b.data = nil
	if _, err := b.environmentPod(in, p); err == nil {
		t.Error("artifacts accepted without a data helper image")
	}
}

// TestEnvironmentRuntimePlanArtifacts is the opt-in that replaces the
// refusal of environment_artifact_guard_test.go: a runtime that implements
// environmentcontrol.Materializer, on a target with a data helper image,
// plans a spec with artifacts and creates its staging Pod. Without the
// helper image the same spec is still unsupported.
func TestEnvironmentRuntimePlanArtifacts(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	var rt environmentcontrol.Runtime = e.rt
	if _, ok := rt.(environmentcontrol.Materializer); !ok {
		t.Fatal("runtime is not a Materializer")
	}
	e.created()
	if p := e.pod(); len(p.Spec.InitContainers) != 1 || p.Spec.InitContainers[0].Name != environmentStageContainer {
		t.Fatalf("created pod init containers = %+v", p.Spec.InitContainers)
	}
	plain := newEnvRT(t, a.config())
	spec := envArtifactSpec()
	if _, _, err := plain.rt.Plan("alice", spec, envRTDigest(t, spec)); !errors.Is(err, environmentcontrol.ErrUnsupported) {
		t.Fatalf("Plan without a data helper = %v; want ErrUnsupported", err)
	}
}

func TestEnvironmentRuntimeIncarnation(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvRT(t, a.config())
	e.created()
	o, err := e.ensure(e.rt)
	e.want(o, err, environment.Creating, nil)
	if o.Incarnation == "" || o.Incarnation != string(e.pod().UID) {
		t.Fatalf("incarnation = %q, pod uid %q", o.Incarnation, e.pod().UID)
	}
	e.run("node-1")
	o, err = e.ensure(e.rt)
	e.want(o, err, environment.Ready, nil)
	if o.Incarnation != string(e.pod().UID) {
		t.Fatalf("ready incarnation = %q", o.Incarnation)
	}
	// A Pod at the name that is not the recorded one is never an incarnation.
	e.f.edit("pods", envRTPod, func(o runtime.Object) { o.(*corev1.Pod).UID = "other-uid" })
	o, _ = e.ensure(e.rt)
	if o.Incarnation != "" {
		t.Fatalf("replaced pod incarnation = %q", o.Incarnation)
	}
}

func TestEnvironmentRuntimeMaterialize(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	uid := string(e.pod().UID)
	seen := a.serveExec(execScript{stdin: true, status: successStatus})
	body := &countingBody{r: strings.NewReader("one")}
	m, err := e.rt.Materialize(e.ref, uid, r1Mounts(e), body)
	if err != nil {
		t.Fatal(err)
	}
	body.mu.Lock()
	body.done = m.Done()
	body.mu.Unlock()
	receive(t, "materialize", m.Done())
	if res := m.Result(); !res.Settled || res.Err != nil {
		t.Fatalf("result = %+v", res)
	}
	if s := receive(t, "exec server", seen); s.err != nil || string(s.stdin) != "one" {
		t.Fatalf("server saw %+v", s)
	}
	reqs := a.requestList()
	if len(reqs) != 1 {
		t.Fatalf("%d exec requests", len(reqs))
	}
	q := reqs[0].URL.Query()
	sum, _ := envArtifactBody("one")
	if reqs[0].URL.Path != "/api/v1/namespaces/workloads/pods/"+envRTPod+"/exec" ||
		!slices.Equal(q["container"], []string{environmentStageContainer}) ||
		!slices.Equal(q["command"], []string{helperEntry, environmentPutVerb, uid, "a0", sum, "3"}) ||
		q.Get("stdin") != "true" {
		t.Fatalf("exec request = %v", reqs[0].URL)
	}
	body.mu.Lock()
	late := body.late
	body.mu.Unlock()
	if late {
		t.Fatal("body read after Done")
	}
	if e.rt.activeFrames() != 0 {
		t.Fatal("settled materialization kept its slot")
	}
}

func TestEnvironmentRuntimeMaterializeOutcome(t *testing.T) {
	for _, tt := range []struct {
		name    string
		script  execScript
		body    string
		replace bool // the Pod at the name is replaced during the exec
		refused bool // Err wraps errEnvironmentInstanceRefused
		sealed  bool // Err wraps errEnvironmentSealed
	}{
		{name: "helper refused identity", script: execScript{stdin: true, status: exitStatus("3")}, body: "one", refused: true},
		{name: "helper refused released", script: execScript{stdin: true, status: exitStatus("4")}, body: "one", sealed: true},
		{name: "helper failed after effects", script: execScript{stdin: true, status: exitStatus("1")}, body: "one"},
		{name: "no status", script: execScript{stdin: true}, body: "one"},
		{name: "short body", script: execScript{stdin: true, status: successStatus}, body: "on"},
		{name: "pod replaced", script: execScript{stdin: true, status: successStatus}, body: "one", replace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			e := newEnvArtifactRT(t, a)
			e.created()
			e.staging("node-1")
			uid := string(e.pod().UID)
			hold := make(chan struct{})
			tt.script.hold = hold
			a.serveExec(tt.script)
			m, err := e.rt.Materialize(e.ref, uid, r1Mounts(e), strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			if tt.replace {
				e.f.edit("pods", envRTPod, func(o runtime.Object) { o.(*corev1.Pod).UID = "replacement-uid" })
			}
			close(hold)
			receive(t, "materialize", m.Done())
			res := m.Result()
			if !res.Settled || res.Err == nil {
				t.Fatalf("result = %+v, want a settled failure", res)
			}
			if got := errors.Is(res.Err, errEnvironmentInstanceRefused); got != tt.refused {
				t.Fatalf("refused = %v, want %v (%v)", got, tt.refused, res.Err)
			}
			if got := errors.Is(res.Err, errEnvironmentSealed); got != tt.sealed {
				t.Fatalf("sealed = %v, want %v (%v)", got, tt.sealed, res.Err)
			}
			// ErrIncarnation means nothing was started; a started frame
			// never reports it.
			if errors.Is(res.Err, environmentcontrol.ErrIncarnation) {
				t.Fatalf("started frame reports ErrIncarnation: %v", res.Err)
			}
		})
	}
}

func TestEnvironmentRuntimeMaterializeDropped(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	a.mu.Lock()
	a.override = func(w http.ResponseWriter, r *http.Request) bool {
		if conn, _, err := http.NewResponseController(w).Hijack(); err == nil {
			conn.Close()
		}
		return true
	}
	a.mu.Unlock()
	m, err := e.rt.Materialize(e.ref, string(e.pod().UID), r1Mounts(e), strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	receive(t, "materialize", m.Done())
	if res := m.Result(); !res.Settled || res.Err == nil || errors.Is(res.Err, environmentcontrol.ErrIncarnation) {
		t.Fatalf("result = %+v, want unknown", res)
	}
}

func TestEnvironmentRuntimeMaterializeRefused(t *testing.T) {
	for _, tt := range []struct {
		name        string
		edit        func(e *envRT)
		incarnation func(e *envRT) string
		mounts      func(e *envRT) []environment.ArtifactMount
		want        error
	}{
		{name: "other incarnation", incarnation: func(*envRT) string { return "other-uid" }, want: environmentcontrol.ErrIncarnation},
		{name: "bootstrapped", edit: func(e *envRT) { e.run("node-1") }, want: environmentcontrol.ErrIncarnation},
		{name: "stage exited", edit: func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) {
				o.(*corev1.Pod).Status.InitContainerStatuses[0].State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}
			})
		}, want: environmentcontrol.ErrIncarnation},
		{name: "pod replaced", edit: func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) { o.(*corev1.Pod).UID = "replacement-uid" })
		}, want: environmentcontrol.ErrIncarnation},
		{name: "one mount of two", mounts: func(e *envRT) []environment.ArtifactMount { return r1Mounts(e)[:1] }},
		{name: "not in spec order", mounts: func(e *envRT) []environment.ArtifactMount {
			m := r1Mounts(e)
			return []environment.ArtifactMount{m[1], m[0]}
		}},
		{name: "two references", mounts: func(e *envRT) []environment.ArtifactMount { return e.ref.Spec.Artifacts }},
		{name: "not in spec", mounts: func(e *envRT) []environment.ArtifactMount {
			sum, size := envArtifactBody("x")
			return []environment.ArtifactMount{{ID: strings.Repeat("9", 32), SHA256: sum, Size: size, MountPath: "/skygo/artifacts/x"}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			e := newEnvArtifactRT(t, a)
			e.created()
			e.staging("node-1")
			incarnation := string(e.pod().UID)
			if tt.incarnation != nil {
				incarnation = tt.incarnation(e)
			}
			mounts := r1Mounts(e)
			if tt.mounts != nil {
				mounts = tt.mounts(e)
			}
			if tt.edit != nil {
				tt.edit(e)
			}
			body := &countingBody{r: strings.NewReader("one")}
			m, err := e.rt.Materialize(e.ref, incarnation, mounts, body)
			if err == nil || m != nil {
				t.Fatalf("Materialize = %v, %v; want refusal", m, err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if body.reads != 0 || len(a.requestList()) != 0 || e.rt.activeFrames() != 0 {
				t.Fatalf("refusal read %d times, sent %d requests, kept %d slots", body.reads, len(a.requestList()), e.rt.activeFrames())
			}
		})
	}
}

func TestEnvironmentRuntimeMaterializeBusy(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	uid := string(e.pod().UID)
	hold := make(chan struct{})
	a.serveExec(execScript{stdin: true, status: successStatus, hold: hold})
	m, err := e.rt.Materialize(e.ref, uid, r1Mounts(e), strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.rt.Materialize(e.ref, uid, r1Mounts(e), bytes.NewReader([]byte("one"))); !errors.Is(err, errMaterializeBusy) {
		t.Fatalf("second Materialize = %v, want errMaterializeBusy", err)
	}
	close(hold)
	receive(t, "materialize", m.Done())
	if len(a.requestList()) != 1 {
		t.Fatalf("%d exec requests", len(a.requestList()))
	}
}

func TestEnvironmentRuntimeMaterializeDelete(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	hold := make(chan struct{})
	defer close(hold)
	a.serveExec(execScript{stdin: true, status: successStatus, hold: hold})
	m, err := e.rt.Materialize(e.ref, string(e.pod().UID), r1Mounts(e), strings.NewReader("one"))
	if err != nil {
		t.Fatal(err)
	}
	e.delete(e.rt) // cancels the frame; the result is not awaited here
	receive(t, "materialize", m.Done())
	if res := m.Result(); !res.Settled || res.Err == nil {
		t.Fatalf("canceled result = %+v", res)
	}
}

// TestEnvironmentRuntimeMaterializeNoRelease is the barrier's client half:
// a successful Materialize of every reference runs only env-put, never
// env-release, so the stage container keeps waiting until Bootstrap.
func TestEnvironmentRuntimeMaterializeNoRelease(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	uid := string(e.pod().UID)
	for _, body := range []string{"one", "second"} {
		a.serveExec(execScript{stdin: true, status: successStatus})
		var mounts []environment.ArtifactMount
		for _, m := range e.ref.Spec.Artifacts {
			if sum, _ := envArtifactBody(body); m.SHA256 == sum {
				mounts = append(mounts, m)
			}
		}
		m, err := e.rt.Materialize(e.ref, uid, mounts, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		receive(t, "materialize", m.Done())
		if res := m.Result(); !res.Settled || res.Err != nil {
			t.Fatalf("result = %+v", res)
		}
	}
	for _, r := range a.requestList() {
		if q := r.URL.Query()["command"]; len(q) < 2 || q[1] != environmentPutVerb {
			t.Fatalf("Materialize ran %q", q)
		}
	}
}

func TestEnvironmentRuntimeBootstrap(t *testing.T) {
	a := newFakeAPI(t, execProtocol)
	e := newEnvArtifactRT(t, a)
	e.created()
	e.staging("node-1")
	uid := string(e.pod().UID)
	a.serveExec(execScript{status: successStatus})
	m, err := e.rt.Bootstrap(e.ref, uid)
	if err != nil {
		t.Fatal(err)
	}
	receive(t, "bootstrap", m.Done())
	if res := m.Result(); !res.Settled || res.Err != nil {
		t.Fatalf("result = %+v", res)
	}
	reqs := a.requestList()
	if len(reqs) != 1 {
		t.Fatalf("%d exec requests", len(reqs))
	}
	q := reqs[0].URL.Query()
	sum1, _ := envArtifactBody("one")
	sum2, _ := envArtifactBody("second")
	want := []string{helperEntry, environmentReleaseVerb, uid, "--", "a0:" + sum1 + ":3", "a1:" + sum2 + ":6"}
	if !slices.Equal(q["container"], []string{environmentStageContainer}) || !slices.Equal(q["command"], want) ||
		q.Get("stdin") == "true" {
		t.Fatalf("exec request = %v", reqs[0].URL)
	}
	// The manifest is the stage container's, so the helper compares the
	// release with what env-wait recorded.
	if stage := e.pod().Spec.InitContainers[0]; !slices.Equal(stage.Command[3:], want[4:]) {
		t.Fatalf("stage manifest %q, release manifest %q", stage.Command[3:], want[4:])
	}
	if e.rt.activeFrames() != 0 {
		t.Fatal("settled bootstrap kept its slot")
	}
}

func TestEnvironmentRuntimeBootstrapOutcome(t *testing.T) {
	for _, tt := range []struct {
		name    string
		script  execScript
		replace bool
		refused bool
	}{
		{name: "helper refused identity", script: execScript{status: exitStatus("3")}, refused: true},
		{name: "helper failed", script: execScript{status: exitStatus("1")}},
		{name: "no status", script: execScript{}},
		{name: "pod replaced", script: execScript{status: successStatus}, replace: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			e := newEnvArtifactRT(t, a)
			e.created()
			e.staging("node-1")
			hold := make(chan struct{})
			tt.script.hold = hold
			a.serveExec(tt.script)
			m, err := e.rt.Bootstrap(e.ref, string(e.pod().UID))
			if err != nil {
				t.Fatal(err)
			}
			if tt.replace {
				e.f.edit("pods", envRTPod, func(o runtime.Object) { o.(*corev1.Pod).UID = "replacement-uid" })
			}
			close(hold)
			receive(t, "bootstrap", m.Done())
			res := m.Result()
			if !res.Settled || res.Err == nil {
				t.Fatalf("result = %+v, want a settled failure", res)
			}
			if got := errors.Is(res.Err, errEnvironmentInstanceRefused); got != tt.refused {
				t.Fatalf("refused = %v, want %v (%v)", got, tt.refused, res.Err)
			}
			if errors.Is(res.Err, environmentcontrol.ErrIncarnation) {
				t.Fatalf("started frame reports ErrIncarnation: %v", res.Err)
			}
		})
	}
}

func TestEnvironmentRuntimeBootstrapRefused(t *testing.T) {
	for _, tt := range []struct {
		name        string
		plain       bool // a spec without artifacts
		edit        func(e *envRT)
		incarnation string
		want        error
	}{
		{name: "other incarnation", incarnation: "other-uid", want: environmentcontrol.ErrIncarnation},
		{name: "stage exited", edit: func(e *envRT) {
			e.f.edit("pods", envRTPod, func(o runtime.Object) {
				o.(*corev1.Pod).Status.InitContainerStatuses[0].State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}
			})
		}, want: environmentcontrol.ErrIncarnation},
		{name: "bootstrapped", edit: func(e *envRT) { e.run("node-1") }, want: environmentcontrol.ErrIncarnation},
		{name: "no artifacts", plain: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newFakeAPI(t, execProtocol)
			var e *envRT
			if tt.plain {
				e = newEnvRTData(t, a.config())
				e.created()
			} else {
				e = newEnvArtifactRT(t, a)
				e.created()
				e.staging("node-1")
			}
			incarnation := string(e.pod().UID)
			if tt.incarnation != "" {
				incarnation = tt.incarnation
			}
			if tt.edit != nil {
				tt.edit(e)
			}
			m, err := e.rt.Bootstrap(e.ref, incarnation)
			if err == nil || m != nil {
				t.Fatalf("Bootstrap = %v, %v; want refusal", m, err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if len(a.requestList()) != 0 || e.rt.activeFrames() != 0 {
				t.Fatalf("refusal sent %d requests, kept %d slots", len(a.requestList()), e.rt.activeFrames())
			}
		})
	}
}
