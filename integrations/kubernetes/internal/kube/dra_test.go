package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
)

// draAPI is a loopback API server for one namespace "workloads": discovery of
// resource.k8s.io/v1, the namespace, DeviceClasses, ResourceClaims, Pods and
// Pod logs. It runs no controllers, scheduler or kubelet: every status,
// reservation, finalizer and deletion progress is MODELED by the tests.
//
// Pod creation applies the v1.35.9 defaults and admission outputs of the
// independent source fixture (evidence K8S-SOURCE-FIXTURE.json), written here
// field by field rather than by calling product canonicalization.
type draAPI struct {
	mu        sync.Mutex
	nsUID     string
	classes   map[string]*resourcev1.DeviceClass
	claims    map[string]*resourcev1.ResourceClaim
	pods      map[string]*corev1.Pod
	requests  []string
	posts     map[string]int // by resource, creates received
	deletes   []metav1.DeleteOptions
	formats   []string // Content-Type of each decoded deletion
	badBodies []error  // decode errors of refused deletion bodies
	nextUID   int
	logs      string
	discovery bool // serve resource.k8s.io/v1

	// Create faults by resource ("resourceclaims" or "pods").
	failCode   map[string]int // reply this status (with reason) and store nothing
	failReason map[string]metav1.StatusReason
	// storeFail stores the object, as an independently admitted create,
	// then replies a complete Status of this code with failReason.
	storeFail map[string]int
	// cut stores the object and replies 201 headers with only part of the
	// declared body, then ends the connection: an incomplete reply.
	cut        map[string]bool
	drop       map[string]bool // store, then drop the connection: a lost reply
	notCreated map[string]bool // store, reply 200
	redirect   map[string]bool // store nothing, reply a complete 307
	// reply rewrites the JSON reply of a create as a generic map, after the
	// object is stored; for example an unknown field or another nonce.
	reply map[string]func(map[string]any)
	// admit changes a created object before it is stored and replied.
	admit map[string]func(any)
	// keepDeleted keeps a deleted Pod or claim with a deletion timestamp
	// instead of removing it, until the test removes it.
	keepDeleted map[string]bool
	// stall, when set, holds a discovery request without any response
	// headers until it is closed or the request is canceled; stalled
	// counts those held.
	stall   chan struct{}
	stalled chan struct{}

	srv *httptest.Server
}

const (
	draClaimsPath  = "/apis/resource.k8s.io/v1/namespaces/workloads/resourceclaims"
	draClassesPath = "/apis/resource.k8s.io/v1/deviceclasses"
)

// newDRAAPI starts the API. Its caller owns its server: draFixture closes
// it only after every owner that uses it closed (draFixture.cleanup).
func newDRAAPI() *draAPI {
	a := &draAPI{nsUID: "ns-uid", classes: map[string]*resourcev1.DeviceClass{}, claims: map[string]*resourcev1.ResourceClaim{},
		pods: map[string]*corev1.Pod{}, posts: map[string]int{}, logs: "output\n", discovery: true,
		failCode: map[string]int{}, failReason: map[string]metav1.StatusReason{}, drop: map[string]bool{},
		storeFail: map[string]int{}, cut: map[string]bool{}, stalled: make(chan struct{}, 16),
		notCreated: map[string]bool{}, redirect: map[string]bool{}, reply: map[string]func(map[string]any){},
		admit: map[string]func(any){}, keepDeleted: map[string]bool{}}
	a.classes["gpu.example.com"] = &resourcev1.DeviceClass{
		TypeMeta:   metav1.TypeMeta{APIVersion: "resource.k8s.io/v1", Kind: "DeviceClass"},
		ObjectMeta: metav1.ObjectMeta{Name: "gpu.example.com", UID: "class-uid", Generation: 1},
		Spec: resourcev1.DeviceClassSpec{Selectors: []resourcev1.DeviceSelector{{CEL: &resourcev1.CELDeviceSelector{
			Expression: `device.driver == "gpu.example.com"`}}}},
	}
	a.srv = httptest.NewServer(http.HandlerFunc(a.serve))
	return a
}

func (a *draAPI) uid() types.UID {
	a.nextUID++
	return types.UID(fmt.Sprintf("uid-%d", a.nextUID))
}

func draWrite(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func draStatus(w http.ResponseWriter, code int, reason metav1.StatusReason) {
	draWrite(w, code, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: reason})
}

func (a *draAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	stall := a.stall
	a.mu.Unlock()
	if stall != nil && r.URL.Path == "/apis/resource.k8s.io/v1" {
		// Accepted, but no headers are sent: only the client's own
		// cancellation or the test's release ends it.
		a.stalled <- struct{}{}
		select {
		case <-stall:
		case <-r.Context().Done():
			return
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, r.Method+" "+r.URL.Path)
	p := r.URL.Path
	switch {
	case r.Method == "GET" && p == "/apis/resource.k8s.io/v1":
		if !a.discovery {
			draStatus(w, 404, metav1.StatusReasonNotFound)
			return
		}
		draWrite(w, 200, metav1.APIResourceList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "APIResourceList"}, GroupVersion: "resource.k8s.io/v1",
			APIResources: []metav1.APIResource{
				{Name: "resourceclaims", Namespaced: true, Kind: "ResourceClaim", Verbs: []string{"create", "delete", "get", "list"}},
				{Name: "deviceclasses", Kind: "DeviceClass", Verbs: []string{"get", "list"}},
			}})
	case r.Method == "GET" && p == "/api/v1/namespaces/workloads":
		draWrite(w, 200, corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: types.UID(a.nsUID)}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}})
	case r.Method == "GET" && strings.HasPrefix(p, draClassesPath+"/"):
		c := a.classes[strings.TrimPrefix(p, draClassesPath+"/")]
		if c == nil {
			draStatus(w, 404, metav1.StatusReasonNotFound)
			return
		}
		draWrite(w, 200, c)
	case r.Method == "POST" && (p == draClaimsPath || p == podsPath):
		a.create(w, r, p == podsPath)
	case strings.HasSuffix(p, "/log") && strings.HasPrefix(p, podsPath+"/"):
		if a.pods[strings.TrimSuffix(strings.TrimPrefix(p, podsPath+"/"), "/log")] == nil {
			draStatus(w, 404, metav1.StatusReasonNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, a.logs)
	case strings.HasPrefix(p, draClaimsPath+"/") || strings.HasPrefix(p, podsPath+"/"):
		pod := strings.HasPrefix(p, podsPath+"/")
		name := strings.TrimPrefix(strings.TrimPrefix(p, draClaimsPath+"/"), podsPath+"/")
		a.object(w, r, pod, name)
	default:
		draStatus(w, 404, metav1.StatusReasonNotFound)
	}
}

func (a *draAPI) create(w http.ResponseWriter, r *http.Request, pod bool) {
	resource := "resourceclaims"
	if pod {
		resource = "pods"
	}
	a.posts[resource]++
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		draStatus(w, 400, metav1.StatusReasonBadRequest)
		return
	}
	if code := a.failCode[resource]; code != 0 {
		draStatus(w, code, a.failReason[resource])
		return
	}
	if a.redirect[resource] {
		w.Header().Set("Location", "http://127.0.0.1:1/elsewhere")
		draStatus(w, http.StatusTemporaryRedirect, "")
		return
	}
	var obj any
	var name string
	if pod {
		p := new(corev1.Pod)
		if json.Unmarshal(raw, p) != nil {
			draStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		draAdmitPod(p)
		p.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		obj, name = p, p.Name
	} else {
		c := new(resourcev1.ResourceClaim)
		if json.Unmarshal(raw, c) != nil {
			draStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		c.TypeMeta = metav1.TypeMeta{APIVersion: "resource.k8s.io/v1", Kind: "ResourceClaim"}
		obj, name = c, c.Name
	}
	if (pod && a.pods[name] != nil) || (!pod && a.claims[name] != nil) {
		draStatus(w, 409, metav1.StatusReasonAlreadyExists)
		return
	}
	if f := a.admit[resource]; f != nil {
		f(obj)
	}
	m := draMeta(obj.(runtime.Object))
	m.UID, m.ResourceVersion, m.CreationTimestamp = a.uid(), "1", metav1.Now()
	if pod {
		a.pods[name] = obj.(*corev1.Pod)
	} else {
		a.claims[name] = obj.(*resourcev1.ResourceClaim)
	}
	if a.drop[resource] {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	if code := a.storeFail[resource]; code != 0 {
		draStatus(w, code, a.failReason[resource])
		return
	}
	data, _ := json.Marshal(obj)
	if a.cut[resource] {
		// net/http ends a connection whose handler wrote less than its
		// declared Content-Length; the client reads io.ErrUnexpectedEOF.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(http.StatusCreated)
		w.Write(data[:len(data)/2])
		return
	}
	if f := a.reply[resource]; f != nil {
		var generic map[string]any
		json.Unmarshal(data, &generic)
		f(generic)
		data, _ = json.Marshal(generic)
	}
	code := http.StatusCreated
	if a.notCreated[resource] {
		code = http.StatusOK
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(data)
}

func (a *draAPI) object(w http.ResponseWriter, r *http.Request, pod bool, name string) {
	var obj runtime.Object
	if pod {
		if p := a.pods[name]; p != nil {
			obj = p
		}
	} else if c := a.claims[name]; c != nil {
		obj = c
	}
	if obj == nil {
		draStatus(w, 404, metav1.StatusReasonNotFound)
		return
	}
	switch r.Method {
	case "GET":
		draWrite(w, 200, obj)
	case "DELETE":
		opts, ct, err := decodeDeleteOptions(r)
		if err != nil {
			a.badBodies = append(a.badBodies, err)
			draStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		a.deletes = append(a.deletes, *opts)
		a.formats = append(a.formats, ct)
		m := draMeta(obj)
		if opts.Preconditions == nil || opts.Preconditions.UID == nil || *opts.Preconditions.UID != m.UID {
			draStatus(w, 409, metav1.StatusReasonConflict)
			return
		}
		key := "resourceclaims"
		if pod {
			key = "pods"
		}
		if a.keepDeleted[key] || len(m.Finalizers) != 0 {
			now := metav1.Now()
			m.DeletionTimestamp = &now
		} else if pod {
			delete(a.pods, name)
		} else {
			delete(a.claims, name)
		}
		draWrite(w, 200, obj)
	default:
		draStatus(w, 405, metav1.StatusReasonMethodNotAllowed)
	}
}

// decodeDeleteOptions decodes a deletion body as the API server does, by its
// Content-Type, with the selected SDK's real serializers: JSON, or the
// Kubernetes protobuf envelope that typed clients prefer (gentype/type.go,
// rest/request.go). Any other type, or a body that does not decode, is an
// error; the fixture never assumes a format.
func decodeDeleteOptions(r *http.Request) (*metav1.DeleteOptions, string, error) {
	ct, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, "", fmt.Errorf("deletion content type: %w", err)
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, ct, err
	}
	opts := new(metav1.DeleteOptions)
	switch ct {
	case runtime.ContentTypeJSON:
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(opts); err != nil {
			return nil, ct, fmt.Errorf("json deletion options: %w", err)
		}
	case runtime.ContentTypeProtobuf:
		s := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
		if _, _, err := s.Decode(raw, nil, opts); err != nil {
			return nil, ct, fmt.Errorf("protobuf deletion options: %w", err)
		}
	default:
		return nil, ct, fmt.Errorf("deletion content type %q", ct)
	}
	return opts, ct, nil
}

// draAdmitPod applies the fixture's v1.35.9 defaults and default admission
// outputs to a created Pod, independently of product canonicalization.
func draAdmitPod(p *corev1.Pod) {
	s := &p.Spec
	if s.DNSPolicy == "" {
		s.DNSPolicy = "ClusterFirst"
	}
	if s.SecurityContext == nil {
		s.SecurityContext = &corev1.PodSecurityContext{}
	}
	if s.TerminationGracePeriodSeconds == nil {
		v := int64(30)
		s.TerminationGracePeriodSeconds = &v
	}
	if s.SchedulerName == "" {
		s.SchedulerName = "default-scheduler"
	}
	if s.EnableServiceLinks == nil {
		v := true
		s.EnableServiceLinks = &v
	}
	if s.ServiceAccountName == "" {
		s.ServiceAccountName, s.DeprecatedServiceAccount = "default", "default"
	}
	if s.Priority == nil {
		v := int32(0)
		s.Priority = &v
	}
	if s.PreemptionPolicy == nil {
		v := corev1.PreemptionPolicy("PreemptLowerPriority")
		s.PreemptionPolicy = &v
	}
	if len(s.Tolerations) == 0 {
		v := int64(300)
		s.Tolerations = []corev1.Toleration{
			{Key: "node.kubernetes.io/not-ready", Operator: "Exists", Effect: "NoExecute", TolerationSeconds: &v},
			{Key: "node.kubernetes.io/unreachable", Operator: "Exists", Effect: "NoExecute", TolerationSeconds: &v},
		}
	}
	for i := range s.Containers {
		c := &s.Containers[i]
		if c.ImagePullPolicy == "" {
			c.ImagePullPolicy = "IfNotPresent"
		}
		if c.TerminationMessagePath == "" {
			c.TerminationMessagePath = "/dev/termination-log"
		}
		if c.TerminationMessagePolicy == "" {
			c.TerminationMessagePolicy = "File"
		}
		for name, q := range c.Resources.Limits {
			if _, ok := c.Resources.Requests[name]; !ok {
				if c.Resources.Requests == nil {
					c.Resources.Requests = corev1.ResourceList{}
				}
				c.Resources.Requests[name] = q
			}
		}
	}
}

// set runs f, which changes the API's faults or objects, under its lock.
func (a *draAPI) set(f func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f()
}

// log returns a copy of the requests received so far.
func (a *draAPI) log() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

// deleted returns a copy of the options of every deletion received.
func (a *draAPI) deleted() []metav1.DeleteOptions {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.deletes)
}

// count returns the creates received for resource.
func (a *draAPI) count(resource string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.posts[resource]
}

// pod and claim return the stored object of attempt id for MODELED changes
// by f under the server lock.
func (a *draAPI) pod(id string, f func(*corev1.Pod)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p := a.pods["skygo-"+id]; p != nil {
		f(p)
	}
}

func (a *draAPI) claim(id string, f func(*resourcev1.ResourceClaim)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for name, c := range a.claims {
		if strings.HasPrefix(name, "skygo-"+id+"-") {
			f(c)
		}
	}
}

// succeed MODELS the kubelet publishing an ordinary success of the Pod of
// attempt id: per the traced v1.35.9 kubelet ordering, only after its
// containers stopped and its claims were unprepared. It is not a kubelet.
func (a *draAPI) succeed(id string) {
	a.pod(id, func(p *corev1.Pod) {
		now := metav1.Now()
		p.Status = corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "task", ContainerID: "containerd://abc",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, ContainerID: "containerd://abc",
				StartedAt: metav1.NewTime(now.Add(-time.Second)), FinishedAt: now}},
		}}}
	})
}

// draFixture is a loopback API, two separate private state roots and a
// kubeconfig for target "test" in namespace workloads with DRA accelerator
// "h100" of class gpu.example.com and ordinary accelerator "nvidia".
type draFixture struct {
	api        *draAPI
	exec, root string
	dir        string // holds the kubeconfig
	config     Config
	now        time.Time

	// The owner custody of the test's originals over exec, root and dir.
	roots   []*receiptFixture // of exec, root and dir
	mu      sync.Mutex
	gates   []func()
	cancels []context.CancelFunc
	owned   []*draOwner
	ops     []*receiptOp
}

// draOwner is one integration in a draFixture's custody, with the outcome
// its test declared for its Close.
type draOwner struct {
	in   *draIntegration
	want error // the expected Close failure (expect); nil expects a clean Close
	held bool  // known never to close (retain): not closed, kept
}

// retainedDRAFixtures keeps every fixture whose cleanup did not positively
// close all of its owners strongly reachable, with its integrations, their
// stores, clients and frames, its API server and its roots, for the test
// binary's lifetime: no finalizer may close a permit file of a HOLD.
var retainedDRAFixtures []*draFixture

func newDRAFixture(t *testing.T) *draFixture {
	t.Helper()
	// Each directory is retired by its own cleanup, registered first so that
	// it runs last; f.cleanup keeps them all if any owner is not closed.
	exec, root, dir := newReceiptFixture(t), newReceiptFixture(t), newReceiptFixture(t)
	f := &draFixture{exec: exec.dir, root: root.dir, dir: dir.dir, roots: []*receiptFixture{exec, root, dir},
		now: time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)}
	// Registered after the directories' retirement, so it runs before it, on
	// every exit of the test, a Fatal before any gate release included, and
	// before the API exists: only this cleanup closes the API.
	t.Cleanup(func() { f.cleanup(t) })
	f.api = newDRAAPI()
	kubeconfig := filepath.Join(f.dir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: %q}}]
contexts: [{name: test, context: {cluster: c, user: u}}]
users: [{name: u, user: {}}]
current-context: test
`, f.api.srv.URL)), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := readConfig([]byte(draFixtureTargets), f.dir, false)
	if err != nil {
		t.Fatal(err)
	}
	f.config = config
	return f
}

const draFixtureTargets = `version: 1
targets:
- name: test
  kubeconfig: kubeconfig
  context: test
  namespace: workloads
  resources:
    max_accelerators: 8
    accelerators:
      nvidia:
        resource_name: nvidia.com/gpu
      h100:
        dra:
          device_class: gpu.example.com
          selectors:
          - 'device.attributes["gpu.example.com"].model == "H100"'
`

// integration constructs the integration as the server's Install does. Each
// call models a new process over the same roots.
func (f *draFixture) integration(t *testing.T) (*draIntegration, *Backend) {
	t.Helper()
	return f.integrationWith(t, nil)
}

// integrationWith is integration with the receipt client's base transport
// wrapped by wrap, as for a draWire.
func (f *draFixture) integrationWith(t *testing.T, wrap func(http.RoundTripper) http.RoundTripper) (*draIntegration, *Backend) {
	t.Helper()
	in, err := newDRAIntegration(context.Background(), f.config, "owner", DRAOptions{ExecutionState: f.exec, State: f.root, LogRetention: time.Hour}, wrap)
	f.own(in) // a partial integration included
	if err != nil {
		t.Fatal(err)
	}
	b := in.backends[0]
	b.dra.now = func() time.Time { return f.now }
	return in, b
}

// own puts in, if not nil, in the fixture's custody: its cleanup closes it
// before the API and roots it depends on may go, and expects that Close to
// return nil. Call it as soon as in is returned, before any check that may
// end the test.
func (f *draFixture) own(in *draIntegration) {
	if in == nil {
		return
	}
	f.owner(in)
}

// owner returns the custody of in, adding it if absent.
func (f *draFixture) owner(in *draIntegration) *draOwner {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.owned {
		if o.in == in {
			return o
		}
	}
	o := &draOwner{in: in}
	f.owned = append(f.owned, o)
	return o
}

// expect declares that the cleanup's Close of in must fail with want
// (errors.Is): it then keeps in, the API and the roots without failing the
// test. Any other outcome, a clean Close included, fails the test.
func (f *draFixture) expect(in *draIntegration, want error) {
	o := f.owner(in)
	f.mu.Lock()
	o.want = want
	f.mu.Unlock()
}

// wire returns a draWire of faults whose gates the fixture's cleanup
// releases, once each, if the test has not.
func (f *draFixture) wire(faults ...*draFault) *draWire {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range faults {
		f.gates = append(f.gates, x.release)
	}
	return &draWire{faults: faults}
}

// arm adds faults to w, matched from now on, whose gates the fixture's
// cleanup releases, once each, if the test has not.
func (f *draFixture) arm(w *draWire, faults ...*draFault) {
	f.mu.Lock()
	for _, x := range faults {
		f.gates = append(f.gates, x.release)
	}
	f.mu.Unlock()
	w.mu.Lock()
	w.faults = append(w.faults, faults...)
	w.mu.Unlock()
}

// context returns a context the fixture's cleanup cancels.
func (f *draFixture) context() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels = append(f.cancels, cancel)
	return ctx, cancel
}

// run starts op as an original operation of the fixture and of every root,
// all registered before it is spawned: the cleanup joins it, within the
// bound, before the API or any root may go.
func (f *draFixture) run(name string, op func() error) *receiptOp {
	o := &receiptOp{name: name, done: make(chan struct{})}
	f.mu.Lock()
	f.ops = append(f.ops, o)
	f.mu.Unlock()
	for _, r := range f.roots {
		r.mu.Lock()
		r.ops = append(r.ops, o)
		r.mu.Unlock()
	}
	go func() {
		defer close(o.done)
		o.err = op()
	}()
	return o
}

// retain declares in known never to close, such as one whose original kept
// its frame: the cleanup does not close it and keeps it, the API and the
// roots, without failing the test.
func (f *draFixture) retain(in *draIntegration) {
	o := f.owner(in)
	f.mu.Lock()
	o.held = true
	f.mu.Unlock()
}

// hold transfers the whole fixture into a process-lifetime HOLD: f stays
// strongly reachable, with every owner, the API server and the roots, which
// are kept, never removed.
func (f *draFixture) hold() {
	if !slices.Contains(retainedDRAFixtures, f) {
		retainedDRAFixtures = append(retainedDRAFixtures, f)
	}
	for _, r := range f.roots {
		r.keep()
	}
}

// cleanup releases every gate of the test, once, and cancels every context
// it registered; only then does it close each owned integration within the
// join bound, which joins its Run, operations and the original Close of
// every native response, and join each original of run. It continues past
// any failure. A Close whose outcome differs from the declared one (own,
// expect) fails the test. Only if every owner closed with nil and every
// original returned does it close the API, and do the roots' retirements
// remove them; otherwise the fixture, with everything its owners use, is
// held (hold) for the test binary's lifetime.
func (f *draFixture) cleanup(t *testing.T) {
	f.mu.Lock()
	gates, cancels, owned, ops := slices.Clone(f.gates), slices.Clone(f.cancels), slices.Clone(f.owned), slices.Clone(f.ops)
	f.mu.Unlock()
	for _, release := range gates {
		release()
	}
	for _, cancel := range cancels {
		cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
	defer cancel()
	closed := true
	for _, o := range owned {
		f.mu.Lock()
		want, held := o.want, o.held
		f.mu.Unlock()
		if held {
			t.Log("MODELED HOLD: integration known never to close; it, the API and the roots retained")
			closed = false
			continue
		}
		err := o.in.Close(ctx)
		switch {
		case err == nil && want == nil:
		case err == nil:
			t.Errorf("integration closed, but its expected Close failure (%v) did not happen", want)
		case want != nil && errors.Is(err, want):
			t.Logf("MODELED HOLD: integration not closed, as expected (%v); it, the API and the roots retained", err)
			closed = false
		default:
			t.Errorf("integration not closed: %v; it, the API and the roots retained", err)
			closed = false
		}
	}
	join, cancelJoin := context.WithTimeout(context.Background(), receiptJoinTimeout)
	defer cancelJoin()
	for _, o := range ops {
		select {
		case <-o.done:
		case <-join.Done():
			t.Errorf("%s did not return; it, the API and the roots retained", o.name)
			closed = false
		}
	}
	if !closed {
		f.hold()
		return
	}
	f.api.srv.Close()
}

// draCheckKept checks f after its cleanup ran. Kept, f is held with its
// owner in, whose Close still fails with want, its same API still serves
// requests, and the kubeconfig and roots are present. Released, the API is
// closed and they are removed.
func draCheckKept(t *testing.T, f *draFixture, in *draIntegration, kept bool, want error) {
	t.Helper()
	client := &http.Client{Timeout: receiptJoinTimeout}
	resp, err := client.Get(f.api.srv.URL + "/api/v1/namespaces/workloads")
	if err == nil {
		resp.Body.Close()
	}
	served := err == nil && resp.StatusCode == http.StatusOK
	f.mu.Lock()
	owned := slices.ContainsFunc(f.owned, func(o *draOwner) bool { return o.in == in })
	f.mu.Unlock()
	held := owned && slices.Contains(retainedDRAFixtures, f)
	if served != kept || held != kept {
		t.Fatalf("API served %v (%v), fixture and owner held %v; want %v", served, err, held, kept)
	}
	for _, path := range []string{f.exec, f.root, f.dir, filepath.Join(f.dir, "kubeconfig")} {
		_, err := os.Lstat(path)
		if present := err == nil; present != kept {
			t.Fatalf("%s present %v, want %v (%v)", path, present, kept, err)
		}
	}
	if !kept {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), receiptJoinTimeout)
	defer cancel()
	if err := in.Close(ctx); !errors.Is(err, want) {
		t.Fatalf("retained owner's Close = %v, want %v", err, want)
	}
}

// formats returns the Content-Type of every decoded deletion, and the decode
// errors of every refused one.
func (a *draAPI) deleteFormats() ([]string, []error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.formats), slices.Clone(a.badBodies)
}

// draTask is a DRA task of count devices.
func draTask(count int32) backend.Task {
	return backend.Task{Argv: []string{"/task"}, Image: "example.invalid/task@sha256:" + strings.Repeat("1", 64),
		Accelerator: "h100", AcceleratorCount: count, CPUMillis: 1000, CPULimitMillis: 1000}
}

func draRef(b *Backend, id string, t backend.Task) backend.Reference {
	return backend.Reference{ID: id, Task: t, Binding: b.binding}
}
