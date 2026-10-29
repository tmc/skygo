package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/serving"
	"github.com/tmc/skygo/internal/servingcontrol"
	"github.com/tmc/skygo/internal/servingwire"
	"github.com/tmc/skygo/internal/store"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

// AUTHORED_NOT_RUN. These tests send requests through the whole serving
// vertical of an integration NewIntegration built from a kubeconfig file:
// the typed ServingService client and the production RPC handler with the
// same file auth Engine and Store, the integration's own serving Controller
// and Kubernetes service runtime, the authenticated route
// /services/<id>/forward/, and a real HTTP application server.
//
// The configured path is the factory's own: the kubeconfig names one TLS
// endpoint (fakeAPI) with its CA, the operator's bearer and impersonation,
// and loadTarget, newServiceRuntime, newServiceHTTP and newEnvTransport
// build the target's typed client, service writer and port transport from
// it. The test replaces none of them. It wraps loadIntegrationTarget only
// to count loads, and the runtime's serve only to observe each frame the
// original servePort returns; the original transport's existing wrapRaw
// hook holds an original connection's Close. The kubeconfig file and the
// target input are changed after construction to show the runtime frozen.
//
// What is MODELED: the API server behind that endpoint — its object store
// (objectAPI over client-go's fake clientset), the scheduler, kubelet and
// endpoint controller (addReadyAt), and the kubelet's port-forward relay
// (fakeAPI.servePortForward) — and every injected fault. Nothing here
// contacts a cluster.

const operatorToken = "operator-token"

// operatorImpersonation is the operator's configured impersonation. Every
// value differs from the caller's.
var operatorImpersonation = rest.ImpersonationConfig{
	UserName: "skygo-operator",
	UID:      "operator-uid-1",
	Groups:   []string{"skygo:operators", "skygo:serving"},
	Extra:    map[string][]string{"scope": {"serving"}},
}

// mutatedToken and mutatedUser replace the operator's values in the
// kubeconfig file after construction; no request may carry them.
const (
	mutatedToken = "mutated-token"
	mutatedUser  = "mutated-operator"
)

// serviceVertical is one service of alice in an integration built by
// NewIntegration with services only, served by the production handler.
type serviceVertical struct {
	o       *closeOwner
	token   string
	actor   auth.Actor
	in      *environmentIntegration
	ctl     *servingcontrol.Controller
	client  *fake.Clientset // the MODELED API server's object store
	objects *objectAPI
	api     *fakeAPI
	loads   func() map[string]int
	front   *frontClose
	rpc     skyv1connect.ServingServiceClient
	domain  *skyv1.RequestDomain
	spec    *skyv1.ServiceSpec
	id      string

	// routes receives the escaped path of each /services/ request once the
	// production handler returned; gateway receives each port frame once
	// the gateway's servePort returned it.
	routes  chan string
	gateway chan *portFrame
}

// bearer returns m as a request carrying token.
func bearer[T any](token string, m *T) *connect.Request[T] {
	r := connect.NewRequest(m)
	r.Header().Set("Authorization", "Bearer "+token)
	return r
}

// objectAPI is the MODELED object store of the API server behind fakeAPI's
// TLS endpoint. It answers every request that is not an upgrade from a fake
// clientset: the borrowed namespace, service account and pull secret
// reads, the owned Deployment and Service creates, reads and UID-
// preconditioned deletes, and the ReplicaSet, Pod and EndpointSlice lists.
// The upgrade is left to fakeAPI's SPDY server.
type objectAPI struct {
	client *fake.Clientset
	ns     string

	mu      sync.Mutex
	creates map[string]int
	deletes map[string]int
	// ack, if set, changes the next created object of ackKind, or of any
	// kind if ackKind is empty, before it is stored and answered 201: a
	// complete known-bad acknowledgment (MODELED).
	ack     func(obj any)
	ackKind string
	// fail, if set, sees every request that is not an upgrade before it is
	// answered; an error it returns is answered 500 (MODELED).
	fail func(r *http.Request) error
}

// newObjectAPI installs an objectAPI over client as a's object routes, with
// the profile's borrowed objects.
func newObjectAPI(t *testing.T, a *fakeAPI, client *fake.Clientset) *objectAPI {
	t.Helper()
	ctx := context.Background()
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "namespace-uid-1"},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ServiceAccounts("workloads").Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "workloads", UID: "service-account-uid-1"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Secrets("workloads").Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "registry-a", Namespace: "workloads", UID: "secret-uid-1"},
		Type:       corev1.SecretTypeDockerConfigJson,
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	m := &objectAPI{client: client, ns: "workloads", creates: map[string]int{}, deletes: map[string]int{}}
	a.mu.Lock()
	a.override = m.serve
	a.mu.Unlock()
	return m
}

func (m *objectAPI) sends(kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.creates[kind]
}

func (m *objectAPI) deleted(kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.deletes[kind]
}

// serve answers r unless it is an upgrade.
func (m *objectAPI) serve(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Upgrade") != "" {
		return false
	}
	m.mu.Lock()
	fail := m.fail
	m.mu.Unlock()
	var obj runtime.Object
	var gvk schema.GroupVersionKind
	var code int
	var err error
	if fail != nil {
		err = fail(r)
	}
	if err == nil {
		obj, gvk, code, err = m.answer(r)
	}
	if err != nil {
		var s apierrors.APIStatus
		if !errors.As(err, &s) {
			s = apierrors.NewInternalError(err)
		}
		st := s.Status()
		obj, gvk, code = &st, schema.GroupVersionKind{Version: "v1", Kind: "Status"}, int(st.Code)
	}
	obj.GetObjectKind().SetGroupVersionKind(gvk)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(obj)
	return true
}

// answer returns the object, its kind and status for r.
func (m *objectAPI) answer(r *http.Request) (runtime.Object, schema.GroupVersionKind, int, error) {
	ctx, c := r.Context(), m.client
	core := func(kind string) schema.GroupVersionKind { return schema.GroupVersionKind{Version: "v1", Kind: kind} }
	apps := func(kind string) schema.GroupVersionKind {
		return schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: kind}
	}
	list := metav1.ListOptions{LabelSelector: r.URL.Query().Get("labelSelector")}
	get := metav1.GetOptions{}
	p := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	var res, name string
	switch {
	case len(p) == 4 && p[0] == "api" && p[1] == "v1" && p[2] == "namespaces" && p[3] == m.ns:
		res, name = "namespaces", m.ns
	case len(p) >= 5 && p[0] == "api" && p[1] == "v1" && p[2] == "namespaces" && p[3] == m.ns:
		res = p[4]
		if len(p) == 6 {
			name = p[5]
		}
	case len(p) >= 6 && p[0] == "apis" && p[3] == "namespaces" && p[4] == m.ns:
		res = p[5]
		if len(p) == 7 {
			name = p[6]
		}
	default:
		return nil, schema.GroupVersionKind{}, 0, apierrors.NewNotFound(schema.GroupResource{}, r.URL.Path)
	}
	key := r.Method + " " + res
	if name == "" {
		key += " list"
	}
	switch key {
	case "GET namespaces":
		o, err := c.CoreV1().Namespaces().Get(ctx, name, get)
		return o, core("Namespace"), http.StatusOK, err
	case "GET serviceaccounts":
		o, err := c.CoreV1().ServiceAccounts(m.ns).Get(ctx, name, get)
		return o, core("ServiceAccount"), http.StatusOK, err
	case "GET secrets":
		o, err := c.CoreV1().Secrets(m.ns).Get(ctx, name, get)
		return o, core("Secret"), http.StatusOK, err
	case "GET deployments":
		o, err := c.AppsV1().Deployments(m.ns).Get(ctx, name, get)
		return o, apps("Deployment"), http.StatusOK, err
	case "GET services":
		o, err := c.CoreV1().Services(m.ns).Get(ctx, name, get)
		return o, core("Service"), http.StatusOK, err
	case "GET replicasets list":
		o, err := c.AppsV1().ReplicaSets(m.ns).List(ctx, list)
		return o, apps("ReplicaSetList"), http.StatusOK, err
	case "GET pods list":
		o, err := c.CoreV1().Pods(m.ns).List(ctx, list)
		return o, core("PodList"), http.StatusOK, err
	case "GET endpointslices list":
		o, err := c.DiscoveryV1().EndpointSlices(m.ns).List(ctx, list)
		return o, schema.GroupVersionKind{Group: "discovery.k8s.io", Version: "v1", Kind: "EndpointSliceList"}, http.StatusOK, err
	case "POST deployments list":
		d := &appsv1.Deployment{}
		if err := json.NewDecoder(r.Body).Decode(d); err != nil {
			return nil, schema.GroupVersionKind{}, 0, apierrors.NewBadRequest(err.Error())
		}
		d.UID = types.UID("deployment-" + d.Annotations[serviceDeclaration])
		m.created("deployment", d)
		o, err := c.AppsV1().Deployments(m.ns).Create(ctx, d, metav1.CreateOptions{})
		return o, apps("Deployment"), http.StatusCreated, err
	case "POST services list":
		s := &corev1.Service{}
		if err := json.NewDecoder(r.Body).Decode(s); err != nil {
			return nil, schema.GroupVersionKind{}, 0, apierrors.NewBadRequest(err.Error())
		}
		s.UID = types.UID("service-" + s.Annotations[serviceDeclaration])
		m.created("service", s)
		o, err := c.CoreV1().Services(m.ns).Create(ctx, s, metav1.CreateOptions{})
		return o, core("Service"), http.StatusCreated, err
	case "DELETE deployments", "DELETE services":
		return m.remove(r, res, name)
	}
	return nil, schema.GroupVersionKind{}, 0, apierrors.NewMethodNotSupported(schema.GroupResource{Resource: res}, r.Method)
}

// created counts one create of kind and applies a pending ack to obj.
func (m *objectAPI) created(kind string, obj any) {
	m.mu.Lock()
	m.creates[kind]++
	var ack func(any)
	if m.ackKind == "" || m.ackKind == kind {
		ack, m.ack = m.ack, nil
	}
	m.mu.Unlock()
	if ack != nil {
		ack(obj)
	}
}

// remove deletes the object at name only if it has the precondition's UID.
func (m *objectAPI) remove(r *http.Request, res, name string) (runtime.Object, schema.GroupVersionKind, int, error) {
	ctx, c := r.Context(), m.client
	var opts metav1.DeleteOptions
	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil {
		return nil, schema.GroupVersionKind{}, 0, apierrors.NewBadRequest("delete without a UID precondition")
	}
	kind := "service"
	if res == "deployments" {
		kind = "deployment"
	}
	m.mu.Lock()
	m.deletes[kind]++
	m.mu.Unlock()
	var cur types.UID
	var err error
	if res == "deployments" {
		var d *appsv1.Deployment
		if d, err = c.AppsV1().Deployments(m.ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			cur = d.UID
		}
	} else {
		var s *corev1.Service
		if s, err = c.CoreV1().Services(m.ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			cur = s.UID
		}
	}
	if err != nil {
		return nil, schema.GroupVersionKind{}, 0, err
	}
	if cur != *opts.Preconditions.UID {
		return nil, schema.GroupVersionKind{}, 0, apierrors.NewConflict(schema.GroupResource{Resource: res}, name, errors.New("precondition failed"))
	}
	if res == "deployments" {
		err = c.AppsV1().Deployments(m.ns).Delete(ctx, name, metav1.DeleteOptions{})
	} else {
		err = c.CoreV1().Services(m.ns).Delete(ctx, name, metav1.DeleteOptions{})
	}
	return &metav1.Status{Status: metav1.StatusSuccess}, schema.GroupVersionKind{Version: "v1", Kind: "Status"}, http.StatusOK, err
}

// upgrades returns the upgrade requests a received, and every request.
func upgrades(a *fakeAPI) (up, all []*http.Request) {
	all = a.requestList()
	for _, r := range all {
		if r.Header.Get("Upgrade") != "" {
			up = append(up, r)
		}
	}
	return up, all
}

// checkOperatorOnly fails unless every request the API server received
// carries exactly the operator's bearer and impersonation of the original
// kubeconfig, and none of the caller's credentials or the values the file
// was changed to after construction.
func checkOperatorOnly(t *testing.T, a *fakeAPI, token string) {
	t.Helper()
	_, all := upgrades(a)
	if len(all) == 0 {
		t.Fatal("api server saw no request")
	}
	want := map[string][]string{
		"Authorization":           {"Bearer " + operatorToken},
		"Impersonate-User":        {operatorImpersonation.UserName},
		"Impersonate-Uid":         {operatorImpersonation.UID},
		"Impersonate-Group":       operatorImpersonation.Groups,
		"Impersonate-Extra-Scope": operatorImpersonation.Extra["scope"],
	}
	for _, r := range all {
		for k, vs := range want {
			if got := r.Header.Values(k); strings.Join(got, ",") != strings.Join(vs, ",") {
				t.Errorf("%s %s: %s = %q, want %q", r.Method, r.URL.Path, k, got, vs)
			}
		}
		// Every other header: any other credential or impersonation header,
		// and any caller or changed value, is a leak.
		other := http.Header{}
		for k, vs := range r.Header {
			if want[k] == nil {
				other[k] = vs
			}
		}
		if l := leaks(other, append(callerSecrets(token), mutatedToken, mutatedUser)...); len(l) != 0 {
			t.Errorf("%s %s carries %q", r.Method, r.URL.Path, l)
		}
	}
}

// writeOperatorKubeconfig writes operatorKubeconfig to a new file and
// returns its path.
func writeOperatorKubeconfig(t *testing.T, a *fakeAPI, token string, as rest.ImpersonationConfig) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(path, operatorKubeconfig(t, a, token, as), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

// operatorKubeconfig returns a kubeconfig naming a's TLS endpoint, with its
// CA, and the user token and impersonation as.
func operatorKubeconfig(t *testing.T, a *fakeAPI, token string, as rest.ImpersonationConfig) []byte {
	t.Helper()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.server.Certificate().Raw})
	data := map[string]any{
		"apiVersion": "v1", "kind": "Config", "current-context": "test",
		"clusters": []any{map[string]any{"name": "test", "cluster": map[string]any{
			"server": a.server.URL, "certificate-authority-data": base64.StdEncoding.EncodeToString(ca)}}},
		"contexts": []any{map[string]any{"name": "test", "context": map[string]any{"cluster": "test", "user": "test"}}},
		"users": []any{map[string]any{"name": "test", "user": map[string]any{
			"token": token, "as": as.UserName, "as-uid": as.UID, "as-groups": as.Groups, "as-user-extra": as.Extra}}},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// latchHeld records cause as o's held error, before anything reports or
// retains on its account, unless o already holds one: a held error is never
// replaced. o then retires nothing.
func latchHeld(o *closeOwner, cause error) {
	if o.held == nil {
		o.held = cause
	}
}

// A frontClose is a front server whose original Close is started at most
// once, in its own goroutine; done is closed when that Close returns, after
// every handler did.
type frontClose struct {
	*httptest.Server
	once sync.Once
	done chan struct{}
}

func newFrontClose(s *httptest.Server) *frontClose {
	return &frontClose{Server: s, done: make(chan struct{})}
}

// start starts f's one original Close.
func (f *frontClose) start() {
	f.once.Do(func() {
		go func() {
			f.Server.Close()
			close(f.done)
		}()
	})
}

// errFrontUnjoined is the held cause of a front server whose Close did not
// return within its bound.
var errFrontUnjoined = errors.New("front server handlers did not return")

// joinFront starts f's one Close and waits up to bound for it to return. If
// it does not, it first latches o's held error, so that no later clean
// integration Close lets cleanup retire the Engine, Store or directory, and
// then reports false. Close is never called again: a later join waits on
// the same original.
func joinFront(o *closeOwner, f *frontClose, bound time.Duration) bool {
	f.start()
	select {
	case <-f.done:
		return true
	case <-time.After(bound):
		latchHeld(o, fmt.Errorf("%w within %v", errFrontUnjoined, bound))
		return false
	}
}

// newIntegrationAt builds an integration with the actual factory on o's
// Store and Engine, from the kubeconfig at path, and the production handler
// over it. own takes the integration as soon as the factory returns one,
// before any failure is reported, and must arrange its Close. The front
// server's cleanup, which runs before that Close, joins its handlers or
// latches o's held error before any report.
func newIntegrationAt(t *testing.T, o *closeOwner, path string, routes chan<- string, own func(*environmentIntegration)) (*environmentIntegration, *frontClose) {
	t.Helper()
	targets := Config{targets: []TargetConfig{
		{Name: "kube", Namespace: "workloads", Kubeconfig: path, Context: "test", Resources: envRTPolicy()},
	}}
	got, err := NewIntegration(context.Background(), o.s, o.engine, targets, EnvironmentConfig{}, serviceTestConfig())
	var in *environmentIntegration
	if got != nil {
		var ok bool
		if in, ok = got.(*environmentIntegration); !ok {
			latchHeld(o, fmt.Errorf("factory returned %T", got))
			t.Fatal(o.held)
		}
		own(in)
	}
	// The input is changed after construction; the runtime keeps its own.
	targets.targets[0].Kubeconfig = filepath.Join(t.TempDir(), "absent")
	targets.targets[0].Namespace = "elsewhere"
	if err != nil {
		t.Fatal(err)
	}
	if in == nil || in.ServingController() == nil || in.svc == nil || in.svc.port == nil {
		t.Fatal("factory returned no serving integration")
	}
	c, err := control.New(o.s, in.Targets(), o.engine)
	if err != nil {
		t.Fatal(err)
	}
	if err := in.BindControl(c); err != nil {
		t.Fatal(err)
	}
	handler := rpc.NewIntegratedHandler(c, o.engine, nil, in.ServingController())
	front := newFrontClose(httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		if routes != nil && strings.HasPrefix(r.URL.Path, "/services/") {
			routes <- r.URL.EscapedPath()
		}
	})))
	// Runs after every test cleanup registered later, which opens the
	// gates and joins the callers. Close waits for the handlers; one that
	// does not return in time latches the owner's held error first, so that
	// no later integration Close waives it, and is then reported and the
	// server retained. A Close a test already started is the one joined.
	t.Cleanup(func() {
		if !joinFront(o, front, testWait) {
			t.Errorf("front server handlers did not return; fixture retained: %v", o.held)
			retainCall(o, front)
		}
	})
	return in, front
}

// newServiceVertical builds the integration with the actual factory from a
// kubeconfig naming the MODELED API server with the operator's bearer and
// impersonation, and drives one service of alice to Ready through the typed
// client and the integration's own Controller.
func newServiceVertical(t *testing.T) *serviceVertical {
	t.Helper()
	v := newServiceVerticalCreated(t)
	v.driveReady(t, v.id)
	if d, s := v.objects.sends("deployment"), v.objects.sends("service"); d != 1 || s != 1 {
		t.Fatalf("creates sent: deployment %d, service %d; want 1 each", d, s)
	}
	return v
}

// newTappedVertical is newServiceVerticalCreated with a writerTap on the
// integration's own service writer, installed before its first write, and
// the owner's originals join set to the tap's. If ready, the service is
// then driven to Ready.
func newTappedVertical(t *testing.T, ready bool) (*serviceVertical, *writerTap) {
	t.Helper()
	return newTappedVerticalProtocol(t, ready, false)
}

func newTappedVerticalProtocol(t *testing.T, ready, h2 bool) (*serviceVertical, *writerTap) {
	t.Helper()
	v := newServiceVerticalCreatedProtocol(t, h2)
	p := tapWriter(t, v.writer(t))
	v.o.originals = p.joined
	if ready {
		v.driveReady(t, v.id)
	}
	return v, p
}

// writer returns the service writer the factory built for the runtime.
func (v *serviceVertical) writer(t *testing.T) *serviceHTTP {
	t.Helper()
	w, ok := v.in.svc.writer.(*serviceHTTP)
	if !ok || w.do == nil {
		t.Fatalf("service writer %T has no do hook", v.in.svc.writer)
	}
	return w
}

// budgets sets the writer's caller waits: the private short ones, 25ms and
// 10ms, or the production defaults. No write may be in progress.
func (v *serviceVertical) budgets(t *testing.T, short bool) {
	t.Helper()
	w := v.writer(t)
	if short {
		w.answerWait, w.settleWait = 25*time.Millisecond, 10*time.Millisecond
	} else {
		w.answerWait, w.settleWait = serviceAnswerWait, serviceSettleWait
	}
}

// pass runs one Reconcile of the integration's own Controller. A pass that
// does not return within testWait latches the owner's held error, is
// retained and fails the test; cancellation is never taken for its return.
func (v *serviceVertical) pass(t *testing.T) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*testWait)
	return v.joinPass(t, v.startPass(ctx), cancel)
}

// startPass starts one Reconcile under ctx and returns its result channel.
func (v *serviceVertical) startPass(ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- v.ctl.Reconcile(ctx) }()
	return done
}

func (v *serviceVertical) joinPass(t *testing.T, done <-chan error, cancel context.CancelFunc) error {
	t.Helper()
	select {
	case err := <-done:
		cancel()
		return err
	case <-time.After(testWait):
		latchHeld(v.o, errors.New("reconcile pass did not return"))
		retainCall(v, cancel)
		t.Fatal(v.o.held)
		return nil
	}
}

// closeIn calls the integration's Close once, bounded by d.
func (v *serviceVertical) closeIn(d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return v.in.Close(ctx)
}

// hasObjects reports whether both objects of service id exist.
func (v *serviceVertical) hasObjects(id string) bool {
	ctx := context.Background()
	_, derr := v.client.AppsV1().Deployments("workloads").Get(ctx, serviceObjectName(id), metav1.GetOptions{})
	_, serr := v.client.CoreV1().Services("workloads").Get(ctx, serviceObjectName(id), metav1.GetOptions{})
	return derr == nil && serr == nil
}

// driveReady drives service id to Ready through the integration's own
// Controller: passes until both objects are created, the MODELED
// scheduler, kubelet and endpoint controller, then passes until the typed
// client reads it Ready.
func (v *serviceVertical) driveReady(t *testing.T, id string) {
	t.Helper()
	var rerr error
	for range 4 {
		if v.hasObjects(id) {
			break
		}
		rerr = v.pass(t)
	}
	if !v.hasObjects(id) {
		t.Fatalf("objects of %s not created (%v)", id, rerr)
	}
	addReadyAt(t, v.client, id, nil)
	for range 4 {
		rerr = v.pass(t)
		if v.getOf(t, id).Status.GetPhase() == skyv1.ServicePhase_SERVICE_PHASE_READY {
			return
		}
	}
	t.Fatalf("service %s phase = %v, want READY (%v)", id, v.getOf(t, id).Status.GetPhase(), rerr)
}

// createService creates another service of alice, named name, through the
// typed client, and returns its id.
func (v *serviceVertical) createService(t *testing.T, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	r, err := v.rpc.CreateService(ctx, bearer(v.token, &skyv1.CreateServiceRequest{
		RequestId: "create-" + name, Name: name, Target: "kube", Spec: v.spec, Domain: v.domain,
	}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Msg.Service.Id
}

// deleteService deletes service id through the typed client.
func (v *serviceVertical) deleteService(t *testing.T, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	cur := v.getOf(t, id)
	d, err := v.rpc.DeleteService(ctx, bearer(v.token, &skyv1.DeleteServiceRequest{
		RequestId: "delete-" + id, Id: id, ExpectedRecordRevision: cur.RecordRevision, Domain: v.domain,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if d.Msg.Service.Desired != skyv1.ServiceDesired_SERVICE_DESIRED_ABSENT {
		t.Fatalf("delete = %v", d.Msg.Service)
	}
}

// record reads service id's record from the Store.
func (v *serviceVertical) record(t *testing.T, id string) store.Service {
	t.Helper()
	s, err := v.o.s.ServiceFor(v.actor, id)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// native decodes the Native of service id's record.
func (v *serviceVertical) native(t *testing.T, id string) serviceNative {
	t.Helper()
	n, err := decodeServiceNative(v.record(t, id).Native)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// ownership returns, under the runtime's lock, the number of service id's
// owned exchanges and the first one's outstanding offer and delivered facts.
func (v *serviceVertical) ownership(id string) (n int, offer *serviceOffer, delivered exchangeFacts, x *serviceExchange) {
	rt := v.in.svc
	rt.mu.Lock()
	defer rt.mu.Unlock()
	owners := rt.owners[id]
	if len(owners) == 0 {
		return 0, nil, exchangeFacts{}, nil
	}
	return len(owners), owners[0].offer, owners[0].delivered, owners[0].x
}

// lastToken returns the runtime's last offer token.
func (v *serviceVertical) lastToken() uint64 {
	rt := v.in.svc
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.token
}

// joinExchange waits up to testWait for every original of x. One that does
// not return latches the owner's held error and fails the test.
func (v *serviceVertical) joinExchange(t *testing.T, x *serviceExchange) {
	t.Helper()
	for _, c := range []<-chan struct{}{x.done, x.requestClosed()} {
		select {
		case <-c:
		case <-time.After(testWait):
			latchHeld(v.o, errors.New("service exchange original did not return"))
			t.Fatal(v.o.held)
		}
	}
	select {
	case <-x.closeDone():
	case <-time.After(testWait):
		latchHeld(v.o, errors.New("service exchange response Close did not return"))
		t.Fatal(v.o.held)
	}
}

// expectRetention records that the test causes the owner's retention on
// purpose, for cause: cleanup then accepts it only as expectedRetention
// does.
func (v *serviceVertical) expectRetention(cause error) { v.o.expect = cause }

// closeHeld closes the integration once, after the test released every
// original it holds, and requires the error to carry want. The error is
// latched as the owner's held error, so cleanup retains the owner.
func (v *serviceVertical) closeHeld(t *testing.T, want error) {
	t.Helper()
	err := v.closeIn(testWait)
	if err == nil {
		v.o.joined = true
		t.Fatalf("integration Close = nil, want %v", want)
	}
	latchHeld(v.o, fmt.Errorf("original integration Close: %w", err))
	if !errors.Is(err, want) {
		t.Fatalf("integration Close = %v, want %v", err, want)
	}
}

// closeClean closes the integration once and requires nil: the owner is
// joined.
func (v *serviceVertical) closeClean(t *testing.T) {
	t.Helper()
	if err := v.closeIn(testWait); err != nil {
		latchHeld(v.o, fmt.Errorf("original integration Close: %w", err))
		t.Fatal(v.o.held)
	}
	v.o.joined = true
}

// getOf reads service id through the typed client and the client's check.
func (v *serviceVertical) getOf(t *testing.T, id string) *skyv1.Service {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	r, err := v.rpc.GetService(ctx, bearer(v.token, &skyv1.GetServiceRequest{Id: id}))
	if err != nil {
		t.Fatal(err)
	}
	if err := servingwire.CheckService(r.Msg.Service); err != nil {
		t.Fatalf("service fails the client's check: %v", err)
	}
	return r.Msg.Service
}

// newServiceVerticalCreated is newServiceVertical up to the accepted
// create: no Reconcile has run.
func newServiceVerticalCreated(t *testing.T) *serviceVertical {
	t.Helper()
	return newServiceVerticalCreatedProtocol(t, false)
}

func newServiceVerticalCreatedProtocol(t *testing.T, h2 bool) *serviceVertical {
	t.Helper()
	o, token := closeStore(t)
	actor, err := o.engine.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	api := newFakeAPIProtocol(t, portProtocol, nil, h2)
	v := &serviceVertical{o: o, token: token, actor: actor, api: api,
		routes: make(chan string, 8), gateway: make(chan *portFrame, 8)}
	v.client = fake.NewSimpleClientset()
	v.objects = newObjectAPI(t, api, v.client)
	v.loads = countLoads(t)
	path := writeOperatorKubeconfig(t, api, operatorToken, operatorImpersonation)
	// Registered after the API servers and before the front server, so it
	// runs after the front server's handlers are joined and before the API
	// servers close: the owner's one attempt to close the integration. A
	// held error is latched and the owner retains everything.
	t.Cleanup(func() { o.closeIntegration() })
	in, front := newIntegrationAt(t, o, path, v.routes, func(in *environmentIntegration) {
		o.in, o.closeIn = in, in.Close
	})
	v.in, v.ctl, v.front = in, in.ServingController(), front
	if n := v.loads()["kube"]; n != 1 {
		t.Fatalf("target loaded %d times, want once", n)
	}
	// The original kubeconfig is changed after construction: a frozen
	// runtime never sends these values.
	mutated := operatorKubeconfig(t, api, mutatedToken, rest.ImpersonationConfig{UserName: mutatedUser})
	if err := os.WriteFile(path, mutated, 0600); err != nil {
		t.Fatal(err)
	}

	// A transparent observer: the original servePort, unchanged, and the
	// frame it returned.
	serve := v.in.svc.serve
	v.in.svc.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		p := serve(w, r, target)
		api.keep(p.envFrame)
		v.gateway <- p
		return p
	}
	v.rpc = skyv1connect.NewServingServiceClient(testClient(), v.front.URL)

	info, err := o.s.RetentionInfo(actor)
	if err != nil {
		t.Fatal(err)
	}
	v.domain = &skyv1.RequestDomain{Lineage: info.Domain.Lineage, Epoch: info.Domain.Epoch}
	spec, err := serving.Spec{
		Image:     testServiceImage,
		Argv:      []string{"/bin/web"},
		Port:      testPort,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   serving.Scaling{Replicas: 1},
	}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	v.spec = servingwire.SpecToProto(spec)

	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	r, err := v.rpc.CreateService(ctx, bearer(token, &skyv1.CreateServiceRequest{
		RequestId: "create-web", Name: "web", Target: "kube", Spec: v.spec, Domain: v.domain,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := servingwire.CheckService(r.Msg.Service); err != nil {
		t.Fatalf("created service fails the client's check: %v", err)
	}
	v.id = r.Msg.Service.Id
	return v
}

// get reads the service through the typed client and the client's check.
func (v *serviceVertical) get(t *testing.T) *skyv1.Service {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	r, err := v.rpc.GetService(ctx, bearer(v.token, &skyv1.GetServiceRequest{Id: v.id}))
	if err != nil {
		t.Fatal(err)
	}
	if err := servingwire.CheckService(r.Msg.Service); err != nil {
		t.Fatalf("service fails the client's check: %v", err)
	}
	return r.Msg.Service
}

// pod is the name of the ready Pod addReadyAt added.
func (v *serviceVertical) pod() string { return serviceObjectName(v.id) + "-abc123-x" }

// forwardCall is one original client exchange with the forward route. Its
// fields are stored before done closes, so the test and the cleanup each
// read them after done without competing receives.
type forwardCall struct {
	done   chan struct{}
	status int
	header http.Header
	body   []byte
	err    error
}

// errCallerUnjoined is the held cause of a forward caller whose original
// did not return within its bound.
var errCallerUnjoined = errors.New("forward caller did not return")

// joinCaller waits up to bound for c's original to return. If it does not,
// it first latches the vertical owner's held error, so that no later clean
// integration Close lets cleanup retire the Engine, Store or directory,
// and then reports false; c and the vertical are retained. Cancellation is
// never taken for its return.
func (v *serviceVertical) joinCaller(c *forwardCall, bound time.Duration, cancel context.CancelFunc) bool {
	select {
	case <-c.done:
		cancel()
		return true
	case <-time.After(bound):
		latchHeld(v.o, fmt.Errorf("%w within %v", errCallerUnjoined, bound))
		retainCall(c, v, cancel)
		return false
	}
}

// startForward sends one POST of body to the route at path with header,
// bounded by a test-owned context that outlives portJoinBudget. Its cleanup,
// registered before the call starts, runs release, which must open every
// gate the test holds, and then joins the call; a call that does not return
// within testWait latches the owner's held error and is then reported and
// retained with the vertical.
func (v *serviceVertical) startForward(t *testing.T, path, body string, header http.Header, release func()) *forwardCall {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*testWait)
	c := &forwardCall{done: make(chan struct{})}
	t.Cleanup(func() {
		release()
		if !v.joinCaller(c, testWait, cancel) {
			t.Errorf("forward caller did not return; caller and vertical retained: %v", v.o.held)
		}
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.front.URL+path, strings.NewReader(body))
	if err != nil {
		close(c.done)
		t.Fatal(err)
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	client := &http.Client{
		Transport:     &http.Transport{DisableKeepAlives: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	go func() {
		defer close(c.done)
		resp, err := client.Do(req)
		if err != nil {
			c.err = err
			return
		}
		c.status, c.header = resp.StatusCode, resp.Header
		c.body, c.err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}()
	return c
}

// controlHeaders are the caller's control-plane credentials, proofs and
// impersonation, each of which must reach neither the API server nor the
// workload. X-Skygo-Session and X-Skygo-Login (dashboard), Skygo-Audit
// (server) and Skygo-Artifact-Begin-Not-Started (artifact responses) are
// defined headers; X-Skygo-Proof-Canary is a MODELED member of the family.
func controlHeaders(token string) http.Header {
	h := http.Header{}
	for k, v := range map[string]string{
		"Authorization":                    "Bearer " + token,
		"Proxy-Authorization":              "Basic cHJveHk=",
		"Cookie":                           "skygo_session=caller-cookie",
		"Impersonate-User":                 "caller-admin",
		"Impersonate-Uid":                  "caller-uid",
		"Impersonate-Extra-Scopes":         "caller-all",
		"X-Skygo-Session":                  "caller-session-proof",
		"X-Skygo-Login":                    "caller-login-proof",
		"X-Skygo-Proof-Canary":             "caller-canary",
		"Skygo-Audit":                      "caller-audit",
		"Skygo-Artifact-Begin-Not-Started": "caller-artifact",
	} {
		h.Set(k, v)
	}
	h.Add("Impersonate-Group", "system:masters")
	h.Add("Impersonate-Group", "caller-group")
	return h
}

// callerSecrets are the caller values that no forwarded request may carry,
// under any header.
func callerSecrets(token string) []string {
	return []string{token, "cHJveHk=", "caller-cookie", "caller-admin", "caller-uid", "caller-all",
		"caller-session-proof", "caller-login-proof", "caller-canary", "caller-audit", "caller-artifact",
		"system:masters", "caller-group"}
}

// leaks returns each header of h in a control-plane family, and each value
// of h containing one of secrets.
func leaks(h http.Header, secrets ...string) []string {
	var out []string
	for k, vs := range h {
		lk := strings.ToLower(k)
		switch {
		case lk == "authorization", lk == "proxy-authorization", lk == "cookie",
			strings.HasPrefix(lk, "impersonate-"), strings.HasPrefix(lk, "x-skygo-"), strings.HasPrefix(lk, "skygo-"):
			out = append(out, k)
			continue
		}
		for _, v := range vs {
			for _, s := range secrets {
				if strings.Contains(v, s) {
					out = append(out, k+": "+v)
				}
			}
		}
	}
	return out
}

// TestServiceForwardLeakCheckDetects is the control for leaks: a header
// filter that let any one caller header through, or the operator's bearer
// into the workload request, is reported.
func TestServiceForwardLeakCheckDetects(t *testing.T) {
	secrets := append(callerSecrets("alice-token"), operatorToken)
	for k := range controlHeaders("alice-token") {
		h := http.Header{"X-Custom": {"keep"}}
		h[k] = controlHeaders("alice-token")[k]
		if len(leaks(h, secrets...)) == 0 {
			t.Errorf("leaked %s not reported", k)
		}
	}
	if len(leaks(http.Header{"X-Forwarded-For": {"Bearer " + operatorToken}}, secrets...)) == 0 {
		t.Error("operator bearer under another name not reported")
	}
	if l := leaks(http.Header{"X-Custom": {"keep"}, "Accept": {"text/event-stream"}}, secrets...); len(l) != 0 {
		t.Errorf("ordinary headers reported: %q", l)
	}
}

// TestServiceForwardComposedCredentials sends one request through the whole
// vertical, built by the factory from the operator's kubeconfig, and
// captures both requests it causes: the upgrade to the API server carries
// only the operator's bearer and impersonation, and the workload's request
// carries neither credential family, with its custom header, raw path,
// query and body intact and its streamed response relayed. Every API
// request carries the original kubeconfig's values, never those the file
// was changed to after construction, and the target was loaded once.
func TestServiceForwardComposedCredentials(t *testing.T) {
	v := newServiceVertical(t)
	bodies := make(chan string, 1)
	const events = "event: token\ndata: one\n\ndata: two\n\n"
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies <- string(b)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-App", "yes")
		io.WriteString(w, events[:len("event: token\ndata: one\n\n")])
		w.(http.Flusher).Flush()
		io.WriteString(w, events[len("event: token\ndata: one\n\n"):])
	})
	seen := v.api.servePortForward(app.Listener.Addr(), "")

	header := controlHeaders(v.token)
	header.Set("X-Custom", "keep")
	header.Set("Accept", "text/event-stream")
	const path = "/v1/predict%2Fx?q=1&r=%2F"
	call := v.startForward(t, "/services/"+v.id+"/forward"+path, "prompt=hello", header, func() {})
	receive(t, "the forward caller", call.done)
	if call.err != nil {
		t.Fatal(call.err)
	}
	if call.status != http.StatusOK || string(call.body) != events ||
		call.header.Get("Content-Type") != "text/event-stream" || call.header.Get("X-App") != "yes" {
		t.Fatalf("response %d %q %v", call.status, call.body, call.header)
	}

	// Joins: the production handler, the gateway's frame and the peer.
	if got := receive(t, "the forward route handler", v.routes); got != "/services/"+v.id+"/forward/v1/predict%2Fx" {
		t.Fatalf("route handler path %q", got)
	}
	f := receive(t, "the gateway's return", v.gateway)
	if s := waitSettled(t, f.envFrame); s.Err != nil {
		t.Fatalf("frame = %v", s.Err)
	}
	if pair := receive(t, "the port-forward peer", seen); pair.err != nil {
		t.Fatal(pair.err)
	}
	if got := receive(t, "the workload body", bodies); got != "prompt=hello" {
		t.Fatalf("workload body %q", got)
	}

	// Every request to the API server — the startup reads, the creates,
	// reads and lists, and the upgrade — was sent by the factory's frozen
	// configuration: the operator's bearer and impersonation only.
	checkOperatorOnly(t, v.api, v.token)
	if n := v.loads()["kube"]; n != 1 {
		t.Errorf("target loaded %d times, want once", n)
	}

	// The outer request: the upgrade to the API server.
	reqs, _ := upgrades(v.api)
	if len(reqs) != 1 {
		t.Fatalf("api server saw %d upgrades, want 1", len(reqs))
	}
	outer := reqs[0]
	if outer.URL.Path != "/api/v1/namespaces/workloads/pods/"+v.pod()+"/portforward" {
		t.Errorf("upgrade path %s", outer.URL.Path)
	}
	want := map[string][]string{
		"Authorization":           {"Bearer " + operatorToken},
		"Impersonate-User":        {operatorImpersonation.UserName},
		"Impersonate-Uid":         {operatorImpersonation.UID},
		"Impersonate-Group":       operatorImpersonation.Groups,
		"Impersonate-Extra-Scope": operatorImpersonation.Extra["scope"],
	}
	for k, vs := range want {
		if got := outer.Header.Values(k); strings.Join(got, ",") != strings.Join(vs, ",") {
			t.Errorf("upgrade %s = %q, want %q", k, got, vs)
		}
	}
	for k := range outer.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "impersonate-") && want[k] == nil {
			t.Errorf("upgrade carries %s", k)
		}
	}
	operator := http.Header{}
	for k, vs := range outer.Header {
		if want[k] == nil {
			operator[k] = vs
		}
	}
	if l := leaks(operator, callerSecrets(v.token)...); len(l) != 0 {
		t.Errorf("upgrade carries caller credentials: %q", l)
	}
	if outer.Header.Get("X-Custom") != "" {
		t.Error("upgrade carries the caller's X-Custom")
	}

	// The inner request: the workload's.
	ar := app.requestList()
	if len(ar) != 1 {
		t.Fatalf("workload saw %d requests, want 1", len(ar))
	}
	inner := ar[0]
	if l := leaks(inner.Header, append(callerSecrets(v.token), operatorToken, operatorImpersonation.UserName, operatorImpersonation.UID)...); len(l) != 0 {
		t.Errorf("workload request carries credentials: %q", l)
	}
	if inner.Method != http.MethodPost || inner.RequestURI != path || inner.Host != "localhost:8080" {
		t.Errorf("workload request %s %s host %s", inner.Method, inner.RequestURI, inner.Host)
	}
	if inner.Header.Get("X-Custom") != "keep" || inner.Header.Get("Accept") != "text/event-stream" {
		t.Errorf("workload headers %v", inner.Header)
	}
}

// TestServiceForwardHeldOriginalClose holds the original TCP connection's
// Close of a forward's exchange past portJoinBudget. The actual gateway
// returns with the frame unsettled; the runtime's Forward, the Controller's
// admission and the route handler stay held. While held, deleting the
// service and reconciling neither deletes, retires nor releases its name or
// reservation, and the integration's Close does not return nil. Once
// released, the original caller, frame and peer are joined, and only then
// does cleanup proceed.
func TestServiceForwardHeldOriginalClose(t *testing.T) {
	for _, mode := range []string{"delete", "close"} {
		t.Run(mode, func(t *testing.T) {
			v := newServiceVertical(t)
			app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Length", "2")
				io.WriteString(w, "ok")
			})
			seen := v.api.servePortForward(app.Listener.Addr(), "")
			// After the vertical and app: the release runs at cleanup
			// before their fixtures, which require every frame to settle.
			rg, release := holdRaw(t)
			// The factory's own transport: its existing hook wraps each
			// original connection; its configuration is unchanged.
			v.in.svc.port.wrapRaw = rg.wrap
			// The owner's actual bearer: the integrated handler
			// authenticates it before the forward route.
			call := v.startForward(t, "/services/"+v.id+"/forward/v1/x", "",
				http.Header{"Authorization": {"Bearer " + v.token}}, release)

			// The framed response reaches the caller before teardown.
			receive(t, "the forward caller", call.done)
			if call.err != nil || call.status != http.StatusOK || string(call.body) != "ok" {
				t.Fatalf("response %d %q, %v", call.status, call.body, call.err)
			}
			// The actual gateway returns after portJoinBudget with the
			// frame unsettled.
			f := receive(t, "the gateway's return", v.gateway)
			raw := originalRaw(t, f.envFrame)
			checkRawHeld(t, f.envFrame, raw, rg)
			routeHeld := func() {
				t.Helper()
				select {
				case p := <-v.routes:
					t.Fatalf("route handler %s returned while the original Close is held", p)
				case <-time.After(50 * time.Millisecond):
				}
			}
			routeHeld()

			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			switch mode {
			case "delete":
				cur := v.get(t)
				d, err := v.rpc.DeleteService(ctx, bearer(v.token, &skyv1.DeleteServiceRequest{
					RequestId: "delete-web", Id: v.id, ExpectedRecordRevision: cur.RecordRevision, Domain: v.domain,
				}))
				if err != nil {
					t.Fatal(err)
				}
				if err := servingwire.CheckService(d.Msg.Service); err != nil || d.Msg.Service.Desired != skyv1.ServiceDesired_SERVICE_DESIRED_ABSENT {
					t.Fatalf("delete = %v, %v", d.Msg.Service, err)
				}
				for range 2 {
					if err := v.ctl.Reconcile(ctx); err == nil || !strings.Contains(err.Error(), "service forwards still admitted") {
						t.Fatalf("Reconcile with a held forward = %v", err)
					}
				}
				if dd, ds := v.objects.deleted("deployment"), v.objects.deleted("service"); dd != 0 || ds != 0 {
					t.Fatalf("deletes sent while held: deployment %d, service %d", dd, ds)
				}
				s, err := v.ctl.Get(v.actor, v.id)
				if err != nil {
					t.Fatalf("service retired while its forward is held: %v", err)
				}
				if s.Reserved == (store.ServiceReservation{}) {
					t.Fatal("reservation released while the forward is held")
				}
				if _, err := v.rpc.CreateService(ctx, bearer(v.token, &skyv1.CreateServiceRequest{
					RequestId: "create-web-again", Name: "web", Target: "kube", Spec: v.spec, Domain: v.domain,
				})); err == nil {
					t.Fatal("name released while the forward is held")
				}
			case "close":
				short, stop := context.WithTimeout(context.Background(), 200*time.Millisecond)
				err := v.in.Close(short)
				stop()
				if err == nil {
					latchHeld(v.o, errors.New("integration Close = nil while the original connection Close is held"))
					t.Fatal(v.o.held)
				}
				if _, err := v.ctl.Get(v.actor, v.id); !errors.Is(err, servingcontrol.ErrClosed) {
					t.Fatalf("serving Controller admits after Close: %v", err)
				}
				if _, err := v.in.svc.begin(); !errors.Is(err, errServiceStopped) {
					t.Fatalf("service runtime admits after Close: %v", err)
				}
				if _, err := v.o.s.RetentionInfo(v.actor); err != nil {
					t.Fatalf("Store unusable while held: %v", err)
				}
				if _, err := v.o.engine.Authenticate("Bearer " + v.token); err != nil {
					t.Fatalf("auth Engine unusable while held: %v", err)
				}
				if dd, ds := v.objects.deleted("deployment"), v.objects.deleted("service"); dd != 0 || ds != 0 {
					t.Fatalf("Close deleted objects: deployment %d, service %d", dd, ds)
				}
			}
			checkRawHeld(t, f.envFrame, raw, rg)
			routeHeld()

			release()
			receive(t, "the forward route handler", v.routes)
			checkRawJoined(t, f.envFrame, raw, rg)
			if pair := receive(t, "the port-forward peer", seen); pair.err != nil {
				t.Fatal(pair.err)
			}

			switch mode {
			case "delete":
				for range 4 {
					if _, err := v.ctl.Get(v.actor, v.id); errors.Is(err, store.ErrNotFound) {
						break
					}
					v.ctl.Reconcile(ctx)
				}
				if _, err := v.ctl.Get(v.actor, v.id); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("service not retired once its forward returned: %v", err)
				}
				if dd, ds := v.objects.deleted("deployment"), v.objects.deleted("service"); dd != 1 || ds != 1 {
					t.Fatalf("deletes: deployment %d, service %d; want 1 each", dd, ds)
				}
			case "close":
				err := v.in.Close(ctx)
				if err != nil {
					latchHeld(v.o, fmt.Errorf("integration Close after release: %w", err))
					t.Fatal(v.o.held)
				}
				v.o.joined = true
				// Close deletes nothing and keeps the record.
				getDeploymentOf(t, v.client, v.id)
				getServiceOf(t, v.client, v.id)
				if s, err := v.o.s.ServiceFor(v.actor, v.id); err != nil || s.Desired != serving.Present {
					t.Fatalf("service after Close = %+v, %v", s, err)
				}
			}
		})
	}
}

// TestServiceForwardMissingBearerRefused is the negative of the held-Close
// positive: the same request without the owner's bearer is refused 401 by
// the integrated handler, and reaches neither the gateway, nor the API
// server, nor a workload.
func TestServiceForwardMissingBearerRefused(t *testing.T) {
	v := newServiceVertical(t)
	_, before := upgrades(v.api)
	call := v.startForward(t, "/services/"+v.id+"/forward/v1/x", "", nil, func() {})
	receive(t, "the forward caller", call.done)
	if call.err != nil || call.status != http.StatusUnauthorized {
		t.Fatalf("response %d %q, %v; want 401", call.status, call.body, call.err)
	}
	receive(t, "the forward route handler", v.routes)
	select {
	case p := <-v.gateway:
		t.Fatalf("unauthenticated forward reached the gateway: %v", p)
	default:
	}
	if up, all := upgrades(v.api); len(up) != 0 || len(all) != len(before) {
		t.Fatalf("unauthenticated forward: %d upgrades, api requests %d -> %d", len(up), len(before), len(all))
	}
}

// TestServiceVerticalBadAckHeld drives the integration's own Controller,
// over the factory's frozen writer, to a complete 201 whose object is not
// the rendering (a MODELED known-bad acknowledgment of the API server), and
// reads the service through the actual typed GetService of the production
// handler, after repeated passes and through a second integration that the
// factory reconstructs on the same Store and Engine. Each read shows Held
// with target-unavailable; the record keeps its name, nonce, operation,
// spec, charges and the refusal witness, never a UID from the refused
// answer. No create is sent again, nothing is deleted, and an object later
// read back exactly as rendered (MODELED) is not adopted.
func TestServiceVerticalBadAckHeld(t *testing.T) {
	v := newServiceVerticalCreated(t)
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	created, err := v.o.s.ServiceFor(v.actor, v.id)
	if err != nil {
		t.Fatal(err)
	}
	v.objects.mu.Lock()
	v.objects.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true }
	v.objects.mu.Unlock()
	if err := v.ctl.Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("reconcile = %v, want identity conflict", err)
	}
	first, err := v.o.s.ServiceFor(v.actor, v.id)
	if err != nil {
		t.Fatal(err)
	}
	n0, err := decodeServiceNative(first.Native)
	if err != nil || n0.Deployment.Refusal == nil {
		t.Fatalf("native after the bad acknowledgment %+v: %v", n0, err)
	}
	check := func(when string, c skyv1connect.ServingServiceClient) {
		t.Helper()
		r, err := c.GetService(ctx, bearer(v.token, &skyv1.GetServiceRequest{Id: v.id}))
		if err != nil {
			t.Fatalf("%s: typed get: %v", when, err)
		}
		svc := r.Msg.Service
		if err := servingwire.CheckService(svc); err != nil {
			t.Fatalf("%s: held service fails the client's check: %v", when, err)
		}
		if svc.Status.GetPhase() != skyv1.ServicePhase_SERVICE_PHASE_HELD || svc.Retired ||
			svc.Desired != skyv1.ServiceDesired_SERVICE_DESIRED_PRESENT || svc.Name != "web" || svc.Id != v.id {
			t.Fatalf("%s: typed get = %v", when, svc)
		}
		unavailable := false
		for _, cond := range svc.Status.GetConditions() {
			unavailable = unavailable || cond.Code == skyv1.ServiceConditionCode_SERVICE_CONDITION_CODE_TARGET_UNAVAILABLE
		}
		if !unavailable {
			t.Fatalf("%s: conditions %v, want target unavailable", when, svc.Status.GetConditions())
		}
		s, err := v.o.s.ServiceFor(v.actor, v.id)
		if err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		n, err := decodeServiceNative(s.Native)
		d := n.Deployment
		if err != nil || !d.Sent || !d.Refused || d.UID != "" || d.Refusal == nil ||
			d.Refusal.Status != http.StatusCreated || !d.Refusal.Decoded || *d.Refusal != *n0.Deployment.Refusal ||
			d.Declaration != n0.Deployment.Declaration || d.Spec != n0.Deployment.Spec || n.Service.Declaration != "" {
			t.Fatalf("%s: native %+v: %v", when, n, err)
		}
		if s.Name != created.Name || s.Nonce != created.Nonce || s.Operation == nil || *s.Operation != *created.Operation ||
			s.Reserved != created.Reserved || s.Reserved == (store.ServiceReservation{}) ||
			s.History.Current() != created.History.Current() {
			t.Fatalf("%s: record %+v, created %+v", when, s, created)
		}
		if v.objects.sends("deployment") != 1 || v.objects.sends("service") != 0 ||
			v.objects.deleted("deployment") != 0 || v.objects.deleted("service") != 0 {
			t.Fatalf("%s: creates %d+%d, deletes %d+%d; want 1+0, 0+0", when, v.objects.sends("deployment"),
				v.objects.sends("service"), v.objects.deleted("deployment"), v.objects.deleted("service"))
		}
	}
	check("first pass", v.rpc)
	for range 3 {
		if err := v.ctl.Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
			t.Fatalf("repeated reconcile = %v", err)
		}
	}
	check("repeated passes", v.rpc)

	// MODELED: the stored object is later read back exactly as rendered,
	// carrying its declaration. It is still not the service's.
	d := getDeploymentOf(t, v.client, v.id)
	d.Spec.Template.Spec.HostNetwork = false
	if _, err := v.client.AppsV1().Deployments("workloads").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := v.ctl.Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("reconcile after the exact read = %v", err)
	}
	check("exact object later", v.rpc)
	if _, err := v.rpc.CreateService(ctx, bearer(v.token, &skyv1.CreateServiceRequest{
		RequestId: "create-web-again", Name: "web", Target: "kube", Spec: v.spec, Domain: v.domain,
	})); err == nil {
		t.Fatal("name of a held service released")
	}

	// Reconstruction (V10, D1): only one live integration per Store. The
	// original reaches terminal clean native custody, with no original
	// pending or failed, and is positively closed and joined; only then
	// does the factory build a second integration on the same, still open,
	// Store and Engine. The closed original is not queried again.
	v.reconstructAfterClose(t)
	path := writeOperatorKubeconfig(t, v.api, operatorToken, operatorImpersonation)
	in2, front2 := newIntegrationAt(t, v.o, path, nil, func(in *environmentIntegration) {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			if err := in.Close(ctx); err != nil {
				latchHeld(v.o, fmt.Errorf("reconstructed integration Close: %w", err))
				t.Errorf("close reconstructed integration: %v", err)
			}
		})
	})
	rpc2 := skyv1connect.NewServingServiceClient(testClient(), front2.URL)
	check("reconstructed", rpc2)
	for range 2 {
		if err := in2.ServingController().Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
			t.Fatalf("reconstructed reconcile = %v", err)
		}
	}
	check("reconstructed passes", rpc2)
	checkOperatorOnly(t, v.api, v.token)
}

// reconstructAfterClose requires the original integration's service
// runtime to hold no owned exchange, then closes the original integration
// once and requires nil, before any reconstruction on the same Store. A
// held original fails the test, with the owner retaining everything.
func (v *serviceVertical) reconstructAfterClose(t *testing.T) {
	t.Helper()
	if err := v.in.svc.closeFailures(); err != nil {
		t.Fatalf("original integration not terminally clean before reconstruction: %v", err)
	}
	if err := v.o.closeIntegration(); err != nil || !v.o.joined {
		t.Fatalf("original integration Close before reconstruction = %v", err)
	}
	if _, err := v.o.s.Services(0, 1); err != nil {
		t.Fatalf("Store unusable after the original Close: %v", err)
	}
}

// TestServiceVerticalUnjoinedCallerRetainsOwner: a forward caller whose
// original has not returned when its bound ends latches the owner's held
// error before anything is reported or retained. The owner's cleanup
// decision then neither closes the integration nor retires the Engine, the
// Store or the directory, and repeating it changes nothing. The short bound
// is the MODELED early failure: the application, which the test owns,
// holds the caller behind a gate whose release is registered before any
// Fatal. Once the gate opens, the actual original caller returns and is
// joined, with its route handler, frame and peer; only then is the bound's
// cause cleared, and the ordinary decision closes and retires everything.
func TestServiceVerticalUnjoinedCallerRetainsOwner(t *testing.T) {
	v := newServiceVertical(t)
	gate, entered := make(chan struct{}), make(chan struct{})
	release := releaser(gate)
	var once sync.Once
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		once.Do(func() { close(entered) })
		<-gate
		io.WriteString(w, "ok")
	})
	t.Cleanup(release) // before the application's cleanup, which joins its handler
	seen := v.api.servePortForward(app.Listener.Addr(), "")
	call := v.startForward(t, "/services/"+v.id+"/forward/v1/x", "",
		http.Header{"Authorization": {"Bearer " + v.token}}, release)
	receive(t, "the application request", entered)

	o := v.o
	var closes, stops, storeCloses, removes int
	realClose, realStop, realStoreClose, realRemove := o.closeIn, o.stop, o.closeStore, o.remove
	o.closeIn = func(ctx context.Context) error { closes++; return realClose(ctx) }
	o.stop = func() { stops++; realStop() }
	o.closeStore = func(s *store.Store) error { storeCloses++; return realStoreClose(s) }
	o.remove = func(path string) error { removes++; return realRemove(path) }
	s, engine, done, dir := o.s, o.engine, o.done, o.dir

	if v.joinCaller(call, 100*time.Millisecond, func() {}) {
		t.Fatal("caller returned while its application is held")
	}
	if !errors.Is(o.held, errCallerUnjoined) {
		t.Fatalf("held = %v, want the unjoined caller latched", o.held)
	}
	held := o.held
	for range 2 {
		if err := o.retireJoined(); err != held || o.held != held {
			t.Fatalf("cleanup decision = %v; want the latched cause unchanged", err)
		}
	}
	if closes != 0 || stops != 0 || storeCloses != 0 || removes != 0 || o.joined || o.retired {
		t.Fatalf("unjoined caller: close=%d stop=%d store=%d remove=%d joined=%v retired=%v",
			closes, stops, storeCloses, removes, o.joined, o.retired)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("directory removed with a caller unjoined: %v", err)
	}
	select {
	case <-done:
		t.Fatal("auth Engine stopped with a caller unjoined")
	default:
	}
	if _, err := engine.Authenticate("Bearer " + v.token); err != nil {
		t.Fatalf("auth Engine unusable with a caller unjoined: %v", err)
	}
	if _, err := s.RetentionInfo(v.actor); err != nil {
		t.Fatalf("Store unusable with a caller unjoined: %v", err)
	}

	// The actual originals return and are joined.
	release()
	receive(t, "the forward caller", call.done)
	if call.err != nil || call.status != http.StatusOK || string(call.body) != "ok" {
		t.Fatalf("response %d %q, %v", call.status, call.body, call.err)
	}
	receive(t, "the forward route handler", v.routes)
	f := receive(t, "the gateway's return", v.gateway)
	if st := waitSettled(t, f.envFrame); st.Err != nil {
		t.Fatalf("frame = %v", st.Err)
	}
	if pair := receive(t, "the port-forward peer", seen); pair.err != nil {
		t.Fatal(pair.err)
	}
	// Only the bound's cause, whose original has now returned, is cleared.
	if o.held != held {
		t.Fatalf("held = %v, want only the bound's cause", o.held)
	}
	o.held = nil
	o.cleanup(t)
	if !o.joined || !o.retired || closes != 1 || stops != 1 || storeCloses != 1 || removes != 1 {
		t.Fatalf("positive cleanup: joined=%v retired=%v close=%d stop=%d store=%d remove=%d",
			o.joined, o.retired, closes, stops, storeCloses, removes)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("joined directory not removed: %v", err)
	}
}

// The composed matrix tests below run the integration NewIntegration built,
// with the typed client, the production handler, the file auth Engine and
// Store, the integration's own Controller and service runtime, and the
// factory's own writer. Their one fault seam is the writer's private do
// hook (writerTap): it calls the configured client's Do exactly once and
// holds or replaces only what it is told, ignoring the caller's context
// (MODELED). The API server behind the TLS endpoint is the MODELED
// objectAPI. fakeAPI speaks HTTP/1.1 only, where the transport flushes a
// request only after its body's first Close returned: a request Close held
// keeps the request from the server, and the request's context, which ends
// when the runtime call returns, then fails it once released.

// TestServiceVerticalWriterOwners (V01, V05): services A and B are created
// through the same typed client, Controller, Store, Engine and target. The
// tap selects only A's Deployment create, by method, collection and object
// name. Its exact 201 is read whole and its response body's first Close
// holds (MODELED): A's accepted UID is committed in the same pass, the
// fence holds A's next Apply, and B, in that pass and later ones, is
// created, observed and made Ready. Released: a nil Close lets A finish
// with the same UID and no create replayed; a failed Close (C) keeps A
// held for good, with that UID, through repeated passes and Close, and the
// owner retains everything as expected. The clean row selects A with no
// hold. Every request carries only the operator's credentials (U03).
func TestServiceVerticalWriterOwners(t *testing.T) {
	for _, tt := range []struct {
		name     string
		hold     bool
		closeErr error
	}{
		{"clean", false, nil},
		{"held then nil", true, nil},
		{"held then failed", true, errModeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			a := v.id
			b := v.createService(t, "web-b")
			f := p.fault(http.MethodPost, "deployments", a)
			if tt.hold {
				f.respClose = newServiceGate()
			}
			f.closeErr = tt.closeErr
			if tt.closeErr != nil {
				v.expectRetention(errServiceCloseFailed)
			}

			err := v.pass(t)
			c := called(t, f)
			if !v.hasObjects(b) {
				t.Fatalf("B not created in the pass that held A: %v", err)
			}
			na := v.native(t, a)
			uid := getDeploymentOf(t, v.client, a).UID
			if na.Deployment.UID != uid {
				t.Fatalf("A's accepted UID %q, object %q", na.Deployment.UID, uid)
			}
			if tt.hold {
				if !errors.Is(err, errServiceClosePending) {
					t.Fatalf("first pass = %v, want A's Close pending", err)
				}
				if na.Service.Declaration != "" {
					t.Fatal("A advanced past its held Close")
				}
				if n, _, _, _ := v.ownership(a); n != 1 {
					t.Fatalf("A has %d owned exchanges, want 1", n)
				}
				if err := v.in.svc.closeFence(b); err != nil {
					t.Fatalf("B fenced by A's original: %v", err)
				}
				held := v.record(t, a).Native
				v.driveReady(t, b) // A stays held meanwhile
				if err := v.pass(t); !errors.Is(err, errServiceClosePending) {
					t.Fatalf("held pass = %v", err)
				}
				if v.record(t, a).Native != held {
					t.Fatal("A's Native changed while held")
				}
				f.respClose.open()
				_, _, _, x := v.ownership(a)
				v.joinExchange(t, x)
			}
			if c.underRespCloses.Load() > 1 {
				t.Fatalf("A's response body closed %d times", c.underRespCloses.Load())
			}
			if tt.closeErr != nil {
				for range 2 {
					if err := v.pass(t); !errors.Is(err, errModeledClose) || !errors.Is(err, errServiceCloseFailed) {
						t.Fatalf("pass after the failed Close = %v", err)
					}
				}
				if n := v.native(t, a); n.Deployment.UID != uid || n.Service.Declaration != "" {
					t.Fatalf("A after the failed Close: %+v", n)
				}
				if v.getOf(t, b).Status.GetPhase() != skyv1.ServicePhase_SERVICE_PHASE_READY {
					t.Fatal("B not Ready beside A's failed Close")
				}
				if p.count(http.MethodPost, "deployments", a) != 1 || p.count(http.MethodPost, "services", a) != 0 {
					t.Fatal("A's create replayed or advanced")
				}
				checkOperatorOnly(t, v.api, v.token)
				if err := p.join(testWait); err != nil {
					t.Fatal(err)
				}
				v.closeHeld(t, errModeledClose)
				return
			}
			v.driveReady(t, a)
			if b2 := v.getOf(t, b).Status.GetPhase(); tt.hold && b2 != skyv1.ServicePhase_SERVICE_PHASE_READY {
				t.Fatalf("B phase %v", b2)
			}
			if n := v.native(t, a); n.Deployment.UID != uid {
				t.Fatalf("A's UID changed: %q, want %q", n.Deployment.UID, uid)
			}
			for _, id := range []string{a, b} {
				if p.count(http.MethodPost, "deployments", id) != 1 || p.count(http.MethodPost, "services", id) != 1 {
					t.Fatalf("service %s creates replayed or missing", id)
				}
			}
			checkOperatorOnly(t, v.api, v.token)
		})
	}
}

// TestServiceVerticalDoReturnHeld (V02) holds the return of the actual
// configured Do after its successful response (MODELED). The caller's live
// budget or cancellation returns without joining that original. Draining
// after release records only what the actual response read established;
// this case claims neither buffering after cancellation nor Ready.
func TestServiceVerticalDoReturnHeld(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled %v", canceled), func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			v.writer(t).answerWait = time.Second
			f := p.fault(http.MethodPost, "deployments", v.id)
			f.doReturn = newServiceGate()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := v.startPass(ctx)
			c := called(t, f)
			within(t, "the actual Do's successful response", c.atDo)
			if c.err != nil || c.resp == nil || c.resp.StatusCode != http.StatusCreated || v.objects.sends("deployment") != 1 {
				t.Fatalf("actual Do = %v, %v", c.resp, c.err)
			}
			if canceled {
				cancel()
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(testWait):
				latchHeld(v.o, errors.New("reconcile pass did not return"))
				retainCall(v, cancel)
				t.Fatal(v.o.held)
			}
			if !canceled && ctx.Err() != nil {
				t.Fatalf("live caller context ended: %v", ctx.Err())
			}
			cancel()
			if !errors.Is(err, errServiceAnswerPending) {
				t.Fatalf("pass = %v, want the answer pending", err)
			}
			n, _, delivered, x := v.ownership(v.id)
			if n != 1 || delivered.answered || isClosed(x.done) {
				t.Fatalf("owned %d, delivered %+v", n, delivered)
			}
			if err := v.pass(t); !errors.Is(err, errServiceAnswerPending) || p.count(http.MethodPost, "deployments", v.id) != 1 {
				t.Fatalf("held pass = %v", err)
			}
			before := v.record(t, v.id)
			f.doReturn.open()
			v.joinExchange(t, x)
			calls := p.total()
			v.closeClean(t)
			after := v.record(t, v.id)
			if p.total() != calls || before.Operation == nil || (after.Operation == nil || *before.Operation != *after.Operation) {
				t.Fatal("pure drain sent a request or changed the original operation")
			}
			if p.count(http.MethodPost, "deployments", v.id) != 1 || v.objects.sends("deployment") != 1 {
				t.Fatal("create resent")
			}
		})
	}
}

// A context ended before admission starts no write, unlike V02's already
// successful actual Do whose return remains held.
func TestServiceVerticalPreCanceledNoWrite(t *testing.T) {
	v, p := newTappedVertical(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := v.ctl.Reconcile(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile = %v", err)
	}
	if p.total() != 0 || v.objects.sends("deployment") != 0 || v.objects.sends("service") != 0 {
		t.Fatal("pre-canceled pass sent a write")
	}
}

// TestServiceVerticalReadHeld (V03): the tap's response body delegates
// every Read to the original and holds the end of the read (MODELED). The
// pass returns at the 25ms budget with the 201's bytes read and the read
// pending; the same read stays pending through another pass. Released with
// io.EOF, the drained answer's UID is committed; released with the named
// read error R, the answer is incomplete, R is reported by the draining
// pass, and only a read of the exact object resolves the UID. No create is
// resent.
func TestServiceVerticalReadHeld(t *testing.T) {
	for _, readErr := range []error{nil, errModeledRead} {
		t.Run(fmt.Sprintf("read %v", readErr), func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			f := p.fault(http.MethodPost, "deployments", v.id)
			f.read, f.readErr = newServiceGate(), readErr
			ctx, cancel := context.WithCancel(context.Background())
			done := v.startPass(ctx)
			c := called(t, f)
			within(t, "the read's end", c.atRead)
			cancel()
			if err := v.joinPass(t, done, cancel); !errors.Is(err, errServiceAnswerPending) {
				t.Fatalf("pass = %v, want the answer pending", err)
			}
			if c.resp.StatusCode != http.StatusCreated || len(c.readBytes()) == 0 {
				t.Fatalf("original answer %d, %d bytes", c.resp.StatusCode, len(c.readBytes()))
			}
			_, _, _, x := v.ownership(v.id)
			v.budgets(t, true)
			if err := v.pass(t); !errors.Is(err, errServiceAnswerPending) || isClosed(x.done) {
				t.Fatalf("held pass = %v", err)
			}
			read := c.readBytes()

			f.read.open()
			v.joinExchange(t, x)
			v.budgets(t, false)
			err := v.pass(t)
			if readErr != nil && !errors.Is(err, readErr) {
				t.Fatalf("draining pass = %v, want R", err)
			}
			v.driveReady(t, v.id)
			if got, want := v.native(t, v.id).Deployment.UID, getDeploymentOf(t, v.client, v.id).UID; got != want {
				t.Fatalf("UID %q, object %q", got, want)
			}
			if string(c.readBytes()) != string(read) || p.count(http.MethodPost, "deployments", v.id) != 1 {
				t.Fatal("original read changed or create resent")
			}
		})
	}
}

// TestServiceVerticalRequestCloseHeld (V04) uses HTTP/2 to obtain an
// actual response while the same underlying request body's first Close is
// still pending. The response is read completely before cancellation;
// neither response availability nor cancellation joins the request Close.
func TestServiceVerticalRequestCloseHeld(t *testing.T) {
	for _, held := range []bool{true, false} {
		t.Run(fmt.Sprintf("held %v", held), func(t *testing.T) {
			v, p := newTappedVerticalProtocol(t, false, true)
			f := p.fault(http.MethodPost, "deployments", v.id)
			f.reqClose = newServiceGate()
			if !held {
				f.reqClose.open()
				v.driveReady(t, v.id)
				if c := called(t, f); c.underReqCloses.Load() != 1 || v.objects.sends("deployment") != 1 {
					t.Fatalf("unheld: %d original Closes, %d creates", c.underReqCloses.Load(), v.objects.sends("deployment"))
				}
				return
			}
			f.respClose = newServiceGate()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := v.startPass(ctx)
			c := called(t, f)
			within(t, "the request body's first Close", c.atReqClose)
			within(t, "the actual response read and first Close", c.atRespClose)
			if err := c.actualReadError(); err != io.EOF {
				t.Fatalf("actual response Read = %v, want EOF before cancellation", err)
			}
			if c.resp == nil || c.resp.ProtoMajor != 2 || c.resp.StatusCode != http.StatusCreated {
				t.Fatalf("response = %v, want HTTP/2 201", c.resp)
			}
			cancel()
			if err := v.joinPass(t, done, cancel); !errors.Is(err, errServiceRequestClosePending) {
				t.Fatalf("pass = %v", err)
			}
			_, _, delivered, x := v.ownership(v.id)
			fs := x.facts()
			if !fs.answered || delivered.requestClosed || fs.requestClosed || isClosed(x.body.closed) || c.underReqCloses.Load() != 0 {
				t.Fatalf("facts %+v, delivered %+v", fs, delivered)
			}
			if v.objects.sends("deployment") != 1 {
				t.Fatal("actual response did not come from the configured server")
			}
			if err := v.pass(t); !errors.Is(err, errServiceRequestClosePending) || p.count(http.MethodPost, "deployments", v.id) != 1 {
				t.Fatalf("held pass = %v", err)
			}
			f.reqClose.open()
			f.respClose.open()
			v.joinExchange(t, x)
			if c.underReqCloses.Load() != 1 {
				t.Fatalf("%d original request Closes", c.underReqCloses.Load())
			}
			v.driveReady(t, v.id)
			if p.count(http.MethodPost, "deployments", v.id) != 1 || v.objects.sends("deployment") != 1 {
				t.Fatal("create resent")
			}
		})
	}
}

// TestServiceVerticalReadAndClose (V06): the named read error R and the
// response body's first Close error C on the same 201 body, through the
// actual writer. The first pass commits the sent create and reports R and
// C; every later pass, a typed delete and its pass, and Close report both,
// with no request sent again and nothing deleted. With a nil Close the
// object is resolved by a read and the service becomes Ready. Held then C:
// the first pass reports R with the Close pending, later ones C.
func TestServiceVerticalReadAndClose(t *testing.T) {
	for _, tt := range []struct {
		name     string
		hold     bool
		closeErr error
	}{
		{"R and C", false, errModeledClose},
		{"R and nil C", false, nil},
		{"R and held then C", true, errModeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			f := p.fault(http.MethodPost, "deployments", v.id)
			f.readErr, f.closeErr = errModeledRead, tt.closeErr
			if tt.hold {
				f.respClose = newServiceGate()
			}
			if tt.closeErr != nil {
				v.expectRetention(errServiceCloseFailed)
			}
			err := v.pass(t)
			// With a nil Close the same pass resolves the UID by a read, and
			// that later Apply's result replaces R's: R is not reported.
			if (tt.closeErr != nil) != errors.Is(err, errModeledRead) {
				t.Fatalf("first pass = %v; R reported %v", err, errors.Is(err, errModeledRead))
			}
			if got := errors.Is(err, errModeledClose); got != (tt.closeErr != nil && !tt.hold) {
				t.Fatalf("first pass = %v; C reported %v", err, got)
			}
			if tt.hold && !errors.Is(err, errServiceClosePending) {
				t.Fatalf("first pass = %v, want the Close pending", err)
			}
			if n := v.native(t, v.id); !n.Deployment.Sent || n.Deployment.Refused || (tt.closeErr != nil) != (n.Deployment.UID == "") {
				t.Fatalf("native %+v", n)
			}
			c := called(t, f)
			if tt.hold {
				f.respClose.open()
				_, _, _, x := v.ownership(v.id)
				v.joinExchange(t, x)
			}
			if tt.closeErr == nil {
				v.driveReady(t, v.id)
				if p.count(http.MethodPost, "deployments", v.id) != 1 {
					t.Fatal("create resent")
				}
				return
			}
			for range 2 {
				if err := v.pass(t); !errors.Is(err, errModeledRead) || !errors.Is(err, errModeledClose) {
					t.Fatalf("repeated pass = %v, want R and C", err)
				}
			}
			v.deleteService(t, v.id)
			if err := v.pass(t); !errors.Is(err, errModeledClose) {
				t.Fatalf("delete pass = %v", err)
			}
			if p.count(http.MethodDelete, "deployments", v.id) != 0 || v.objects.deleted("deployment") != 0 ||
				p.count(http.MethodPost, "deployments", v.id) != 1 || c.underRespCloses.Load() != 1 {
				t.Fatal("request sent again, object deleted or original Close replayed")
			}
			if err := p.join(testWait); err != nil {
				t.Fatal(err)
			}
			v.closeHeld(t, errModeledClose)
			if !errors.Is(v.o.held, errModeledRead) {
				t.Fatalf("Close = %v, want R kept", v.o.held)
			}
		})
	}
}

// TestServiceVerticalLateCreate (V07): the actual read's return is held
// (MODELED) after the same actual response body reached EOF; then the pass's
// context is canceled, or the integration's
// Close begins, before the writer's answer. The original Apply operation,
// Ref and Native recorded before the create survive. Released, the next
// Close drains the late answer with no runtime call and no request: an
// exact 201's UID is committed under the original operation and
// acknowledged, a complete bad 201 is committed as a refusal, and a failed
// response Close keeps the committed UID with the owner retained.
func TestServiceVerticalLateCreate(t *testing.T) {
	for _, tt := range []struct {
		name     string
		close    bool // Close begins instead of a cancel
		bad      bool
		closeErr error
	}{
		{"cancel valid", false, false, nil},
		{"cancel bad201", false, true, nil},
		{"close valid", true, false, nil},
		{"close bad201", true, true, nil},
		{"close failed", true, false, errModeledClose},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			if tt.bad {
				v.objects.mu.Lock()
				v.objects.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true }
				v.objects.ackKind = "deployment"
				v.objects.mu.Unlock()
			}
			if tt.closeErr != nil {
				v.expectRetention(errServiceCloseFailed)
			}
			f := p.fault(http.MethodPost, "deployments", v.id)
			f.read, f.closeErr = newServiceGate(), tt.closeErr
			ctx, cancel := context.WithCancel(context.Background())
			done := v.startPass(ctx)
			c := called(t, f)
			within(t, "the actual response body reaching EOF", c.atRead)
			if err := c.actualReadError(); err != io.EOF {
				t.Fatalf("actual response Read = %v, want EOF before cancellation", err)
			}
			if tt.close {
				if err := v.closeIn(time.Second); err == nil {
					v.o.joined = true
					t.Fatal("integration Close = nil with the create's Do held")
				}
			} else {
				cancel()
			}
			if err := v.joinPass(t, done, cancel); !errors.Is(err, errServiceAnswerPending) {
				t.Fatalf("pass = %v", err)
			}
			before := v.record(t, v.id)
			if n, err := decodeServiceNative(before.Native); err != nil || !n.Deployment.Sent || n.Deployment.UID != "" {
				t.Fatalf("native before the drain %+v: %v", n, err)
			}
			_, _, _, x := v.ownership(v.id)
			f.read.open()
			v.joinExchange(t, x)
			calls := p.total()

			err := v.closeIn(testWait)
			after := v.record(t, v.id)
			if p.total() != calls {
				t.Fatal("Close sent a request while draining")
			}
			if after.Operation == nil || *after.Operation != *before.Operation {
				t.Fatalf("operation %+v, want the original %+v", after.Operation, before.Operation)
			}
			n, derr := decodeServiceNative(after.Native)
			if derr != nil {
				t.Fatal(derr)
			}
			switch {
			case tt.bad:
				if err != nil {
					latchHeld(v.o, err)
					t.Fatal(err)
				}
				v.o.joined = true
				if !n.Deployment.Refused || n.Deployment.UID != "" || n.Deployment.Refusal == nil || n.Deployment.Refusal.Status != http.StatusCreated {
					t.Fatalf("late bad 201 committed as %+v", n.Deployment)
				}
			default:
				if n.Deployment.UID != getDeploymentOf(t, v.client, v.id).UID {
					t.Fatalf("late valid UID not committed: %+v", n.Deployment)
				}
				if tt.closeErr == nil {
					if err != nil {
						latchHeld(v.o, err)
						t.Fatal(err)
					}
					v.o.joined = true
					break
				}
				latchHeld(v.o, fmt.Errorf("original integration Close: %w", err))
				if !errors.Is(err, errModeledClose) {
					t.Fatalf("Close = %v, want C", err)
				}
			}
			if p.count(http.MethodPost, "deployments", v.id) != 1 || p.count(http.MethodPost, "services", v.id) != 0 {
				t.Fatal("create resent or advanced")
			}
		})
	}
}

// deleteReady is a Ready service with its creating original drained, then
// deleted through the typed client: a Present record with no operation,
// now Absent.
func deleteReady(t *testing.T) (*serviceVertical, *writerTap) {
	t.Helper()
	v, p := newTappedVertical(t, true)
	if err := v.pass(t); err != nil { // drains the Apply original, if kept
		t.Fatal(err)
	}
	if r := v.record(t, v.id); r.Operation != nil {
		t.Fatalf("ready service has operation %+v", r.Operation)
	}
	v.deleteService(t, v.id)
	return v, p
}

// startDelete starts the first delete pass of v with the default budgets
// and, once the tap's DELETE of the Deployment had its actual Do return,
// held at doReturn, ends it with stop. It returns the owned exchange and
// the pass's error.
func startDelete(t *testing.T, v *serviceVertical, f *tapFault, stop func(cancel context.CancelFunc)) (*serviceExchange, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := v.startPass(ctx)
	within(t, "the actual DELETE's return", called(t, f).atDo)
	stop(cancel)
	err := v.joinPass(t, done, cancel)
	n, _, delivered, x := v.ownership(v.id)
	if n != 1 || delivered.answered || !delivered.requestClosed {
		t.Fatalf("owned %d, delivered %+v", n, delivered)
	}
	return x, err
}

// TestServiceVerticalDeleteTokenRace (V08) drains three immutable versions
// of the same Native. HTTP/2 returns the actual DELETE response while its
// original request Close is held. The same real response read reaches EOF
// before cancellation; its gated return yields fact1/T1. Request Close
// yields fact2/T2, then the MODELED wrapper's first-Close return yields
// fact3/T3; its actual underlying Close has already returned. Every commit and Ack
// is performed by the actual Controller, including after Stop.
func TestServiceVerticalDeleteTokenRace(t *testing.T) {
	for _, mode := range []string{"pass", "close", "failed"} {
		t.Run(mode, func(t *testing.T) {
			v, p := newTappedVerticalProtocol(t, true, true)
			if err := v.pass(t); err != nil {
				t.Fatal(err)
			}
			v.deleteService(t, v.id)
			f := p.fault(http.MethodDelete, "deployments", v.id)
			f.reqClose, f.read, f.respClose = newServiceGate(), newServiceGate(), newServiceGate()
			if mode == "failed" {
				f.closeErr = errModeledClose
				v.expectRetention(errServiceCloseFailed)
			}
			ready := v.record(t, v.id).Native
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := v.startPass(ctx)
			c := called(t, f)
			within(t, "the original request Close", c.atReqClose)
			within(t, "the actual response body's EOF", c.atRead)
			if err := c.actualReadError(); err != io.EOF {
				t.Fatalf("actual response Read = %v, want EOF before cancellation", err)
			}
			if c.resp == nil || c.resp.ProtoMajor != 2 || c.err != nil || c.underReqCloses.Load() != 0 {
				t.Fatalf("actual HTTP/2 response %v, %v; request Closes %d", c.resp, c.err, c.underReqCloses.Load())
			}
			cancel()
			if err := v.joinPass(t, done, cancel); !errors.Is(err, errServiceAnswerPending) {
				t.Fatalf("delete pass = %v", err)
			}
			rec := v.record(t, v.id)
			if rec.Operation == nil || rec.Operation.Kind != store.ServiceDelete || rec.Native != ready {
				t.Fatalf("record %+v", rec)
			}
			_, _, _, x := v.ownership(v.id)
			f.read.open()
			within(t, "the same response's first Close", c.atRespClose)
			within(t, "the original answer", &event{c: x.done})
			ref := rec.Ref()
			native, t1, err := v.in.svc.Drain(ref)
			if t1 == 0 || native != ready || !errors.Is(err, errServiceRequestClosePending) || isClosed(x.body.closed) {
				t.Fatalf("T1 = %d, %v", t1, err)
			}
			f.reqClose.open()
			within(t, "the same original request Close returning", c.reqClosed)
			calls := p.total()
			if mode == "close" {
				err = v.closeIn(testWait)
			} else {
				err = v.pass(t)
			}
			if !errors.Is(err, errServiceClosePending) || v.lastToken() != t1+1 || p.total() != calls {
				t.Fatalf("T2 drain = %v, token %d, calls %d/%d", err, v.lastToken(), p.total(), calls)
			}
			if r := v.record(t, v.id); r.Operation == nil || *r.Operation != *rec.Operation || r.Native != ready || r.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("T2 changed lineage or released the reservation: %+v", r)
			}
			for _, token := range []uint64{t1, t1 + 1} {
				if err := v.in.svc.Ack(ref, token); err == nil {
					t.Fatalf("stale token %d acknowledged", token)
				}
			}
			f.respClose.open()
			within(t, "the same response Close returning", c.respClosed)
			v.joinExchange(t, x)
			switch mode {
			case "close":
				v.closeClean(t)
				if p.total() != calls {
					t.Fatal("Close sent a request")
				}
			default:
				err = v.pass(t)
				if mode == "failed" && !errors.Is(err, errModeledClose) {
					t.Fatalf("T3 = %v, want C", err)
				}
			}
			if v.lastToken() != t1+2 || c.underReqCloses.Load() != 1 || c.underRespCloses.Load() != 1 {
				t.Fatalf("T3 token %d; original request/response Closes %d/%d", v.lastToken(), c.underReqCloses.Load(), c.underRespCloses.Load())
			}
			if err := v.in.svc.Ack(ref, t1+2); err == nil {
				t.Fatal("T3 acknowledged twice")
			}
			if mode == "failed" {
				if err := p.join(testWait); err != nil {
					t.Fatal(err)
				}
				v.closeHeld(t, errModeledClose)
			} else if mode == "pass" {
				for range 4 {
					if _, err := v.o.s.ServiceFor(v.actor, v.id); errors.Is(err, store.ErrNotFound) {
						break
					}
					v.pass(t)
				}
				if _, err := v.o.s.ServiceFor(v.actor, v.id); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("service not retired: %v", err)
				}
			}
		})
	}
}

// TestServiceVerticalPriorUIDThenBadService201 (V09, V10): the Deployment's
// exact 201 grants its UID; the Service's complete 201 is of another
// object (MODELED). Through the typed client the service is Held with
// target-unavailable after the first and repeated passes, and its record
// keeps the prior Deployment's UID, declaration and spec, the refusal
// witness, its operation, nonce, charges and history; nothing is resent or
// deleted. Then, with no original pending or failed, the original
// integration is closed and joined, and only then does the factory build a
// second on the same Store and Engine, whose typed client reads the same
// retained truth. The closed original is not queried.
func TestServiceVerticalPriorUIDThenBadService201(t *testing.T) {
	v := newServiceVerticalCreated(t)
	created := v.record(t, v.id)
	v.objects.mu.Lock()
	v.objects.ack = func(obj any) {
		s := obj.(*corev1.Service)
		if s.Labels == nil {
			s.Labels = map[string]string{}
		}
		s.Labels["modeled-bad-ack"] = "true"
	}
	v.objects.ackKind = "service"
	v.objects.mu.Unlock()
	if err := v.pass(t); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("pass = %v, want identity conflict", err)
	}
	first := v.native(t, v.id)
	uid := getDeploymentOf(t, v.client, v.id).UID
	if first.Deployment.UID != uid || first.Deployment.Refused || !first.Service.Refused || first.Service.Refusal == nil ||
		first.Service.Refusal.Status != http.StatusCreated || !first.Service.Refusal.Decoded || first.Service.UID != "" {
		t.Fatalf("native after the bad Service 201: %+v", first)
	}
	check := func(when string, c skyv1connect.ServingServiceClient) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), testWait)
		defer cancel()
		r, err := c.GetService(ctx, bearer(v.token, &skyv1.GetServiceRequest{Id: v.id}))
		if err != nil {
			t.Fatalf("%s: typed get: %v", when, err)
		}
		svc := r.Msg.Service
		if err := servingwire.CheckService(svc); err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		unavailable := false
		for _, cond := range svc.Status.GetConditions() {
			unavailable = unavailable || cond.Code == skyv1.ServiceConditionCode_SERVICE_CONDITION_CODE_TARGET_UNAVAILABLE
		}
		if svc.Status.GetPhase() != skyv1.ServicePhase_SERVICE_PHASE_HELD || !unavailable || svc.Retired {
			t.Fatalf("%s: typed get = %v", when, svc)
		}
		s := v.record(t, v.id)
		n, err := decodeServiceNative(s.Native)
		if err != nil || n.Deployment != first.Deployment || n.Service.Declaration != first.Service.Declaration ||
			n.Service.Spec != first.Service.Spec || n.Service.Refusal == nil || *n.Service.Refusal != *first.Service.Refusal {
			t.Fatalf("%s: native %+v: %v", when, n, err)
		}
		if s.Name != created.Name || s.Nonce != created.Nonce || s.Operation == nil || *s.Operation != *created.Operation ||
			s.Reserved != created.Reserved || s.History.Current() != created.History.Current() {
			t.Fatalf("%s: record %+v, created %+v", when, s, created)
		}
		if v.objects.sends("deployment") != 1 || v.objects.sends("service") != 1 ||
			v.objects.deleted("deployment") != 0 || v.objects.deleted("service") != 0 {
			t.Fatalf("%s: creates or deletes sent", when)
		}
	}
	check("first pass", v.rpc)
	for range 2 {
		if err := v.pass(t); !errors.Is(err, serving.ErrIdentityConflict) {
			t.Fatalf("repeated pass = %v", err)
		}
	}
	check("repeated passes", v.rpc)

	v.reconstructAfterClose(t)
	path := writeOperatorKubeconfig(t, v.api, operatorToken, operatorImpersonation)
	in2, front2 := newIntegrationAt(t, v.o, path, nil, func(in *environmentIntegration) {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), testWait)
			defer cancel()
			if err := in.Close(ctx); err != nil {
				latchHeld(v.o, fmt.Errorf("reconstructed integration Close: %w", err))
				t.Errorf("close reconstructed integration: %v", err)
			}
		})
	})
	rpc2 := skyv1connect.NewServingServiceClient(testClient(), front2.URL)
	check("reconstructed", rpc2)
	ctx, cancel := context.WithTimeout(context.Background(), testWait)
	defer cancel()
	if err := in2.ServingController().Reconcile(ctx); !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("reconstructed pass = %v", err)
	}
	check("reconstructed pass", rpc2)
}

// TestServiceVerticalFrontCloseLatch (V11): the front server's one
// original Close is started while the forward route's handler is held by
// the transport's own wrapRaw Close gate. A short bound returns false only
// after the owner's held error is latched; the cleanup decision then
// closes and retires nothing. Released, the same Close returns, the
// originals are joined, the bound's cause alone is cleared, and cleanup,
// which joins that same Close rather than calling it again, retires
// everything.
func TestServiceVerticalFrontCloseLatch(t *testing.T) {
	v := newServiceVertical(t)
	app := newApp(t, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", "2")
		io.WriteString(w, "ok")
	})
	seen := v.api.servePortForward(app.Listener.Addr(), "")
	rg, release := holdRaw(t)
	v.in.svc.port.wrapRaw = rg.wrap
	call := v.startForward(t, "/services/"+v.id+"/forward/v1/x", "",
		http.Header{"Authorization": {"Bearer " + v.token}}, release)
	receive(t, "the forward caller", call.done)
	fr := receive(t, "the gateway's return", v.gateway)
	raw := originalRaw(t, fr.envFrame)
	checkRawHeld(t, fr.envFrame, raw, rg)

	o := v.o
	var closes int
	realClose := o.closeIn
	o.closeIn = func(ctx context.Context) error { closes++; return realClose(ctx) }
	if joinFront(o, v.front, 100*time.Millisecond) {
		t.Fatal("front Close returned with its handler held")
	}
	if !errors.Is(o.held, errFrontUnjoined) {
		t.Fatalf("held = %v, want the front latched", o.held)
	}
	held := o.held
	if err := o.retireJoined(); err != held || closes != 0 || o.retired {
		t.Fatalf("cleanup decision = %v; closes %d", err, closes)
	}

	release()
	receive(t, "the front server's Close", v.front.done)
	receive(t, "the forward route handler", v.routes)
	checkRawJoined(t, fr.envFrame, raw, rg)
	if pair := receive(t, "the port-forward peer", seen); pair.err != nil {
		t.Fatal(pair.err)
	}
	if o.held != held {
		t.Fatalf("held = %v, want only the bound's cause", o.held)
	}
	o.held = nil
}

// TestServiceVerticalFirstDeleteThenClose (V12): a Ready service with no
// operation and no original is deleted. Its first native delete, under the
// Delete operation committed and recorded as its original before the
// send, has its actual Do return held, and the integration's Close begins
// before another pass: that Close holds. Released, the same original
// drains: clean, a second Close commits and acknowledges it with no
// request and returns nil; a failed response Close keeps the owner
// retained; an operation replaced meanwhile (MODELED direct commits) is
// never given the late facts, which stay unacknowledged.
func TestServiceVerticalFirstDeleteThenClose(t *testing.T) {
	for _, mode := range []string{"clean", "failed", "another op"} {
		t.Run(mode, func(t *testing.T) {
			v, p := deleteReady(t)
			f := p.fault(http.MethodDelete, "deployments", v.id)
			f.doReturn = newServiceGate()
			switch mode {
			case "failed":
				f.closeErr = errModeledClose
				v.expectRetention(errServiceCloseFailed)
			case "another op":
				v.expectRetention(errServiceFactsUnacknowledged)
			}
			ready := v.record(t, v.id).Native
			var cerr error
			x, _ := startDelete(t, v, f, func(context.CancelFunc) { cerr = v.closeIn(time.Second) })
			if cerr == nil || !errors.Is(cerr, errServiceAnswerPending) {
				t.Fatalf("first Close = %v, want the delete held", cerr)
			}
			rec := v.record(t, v.id)
			if rec.Operation == nil || rec.Operation.Kind != store.ServiceDelete || rec.Native != ready ||
				rec.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("record %+v", rec)
			}
			if p.count(http.MethodDelete, "deployments", v.id) != 1 || p.count(http.MethodDelete, "services", v.id) != 0 {
				t.Fatal("deletes sent")
			}
			if mode == "another op" {
				cm := rec.Commit()
				cm.Operation = nil
				cm.Observation = &serving.Observation{ObservedGeneration: 1}
				cm.ObservedAt = time.Now().UTC()
				cur, err := v.o.s.CommitService(v.id, rec.Revision, cm)
				if err != nil {
					t.Fatal(err)
				}
				cm = cur.Commit()
				cm.Operation = &store.ServiceOperation{ID: "0123456789abcdef0123456789abcdef", Kind: store.ServiceDelete,
					Generation: cur.History.Current().Generation, Revision: cur.History.Current().Revision, Started: time.Now().UTC()}
				if _, err := v.o.s.CommitService(v.id, cur.Revision, cm); err != nil {
					t.Fatal(err)
				}
			}
			f.doReturn.open()
			v.joinExchange(t, x)
			calls := p.total()
			switch mode {
			case "clean":
				v.closeClean(t)
				if r := v.record(t, v.id); r.Operation == nil || *r.Operation != *rec.Operation || r.Native != ready {
					t.Fatalf("record after Close %+v", r)
				}
			case "failed":
				v.closeHeld(t, errModeledClose)
			case "another op":
				v.closeHeld(t, errServiceFactsUnacknowledged)
				if n, offer, _, _ := v.ownership(v.id); n != 1 || offer == nil {
					t.Fatalf("late facts acknowledged under another operation: %d, %v", n, offer)
				}
			}
			if p.total() != calls {
				t.Fatal("Close sent a request")
			}
		})
	}
}

// TestServiceVerticalPartialApplyCancel (V13): healthy, the Deployment's
// exact 201 grants its UID while its response Close holds, so the Service
// is never declared; the service is then deleted with its real Apply
// operation in flight. Once the Close returns, the original drains first;
// then that same Apply operation, never replaced, is the cleanup
// authority: the server sees the DELETE of the Deployment under it, over
// the Native that recorded the UID, the Service is not deleted, and the
// service retires. With a bad 201 (MODELED) the create is refused: the
// cleanup deletes nothing, observes the conflict and keeps the record,
// its operation and its charges.
func TestServiceVerticalPartialApplyCancel(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprintf("bad201 %v", bad), func(t *testing.T) {
			v, p := newTappedVertical(t, false)
			f := p.fault(http.MethodPost, "deployments", v.id)
			if bad {
				v.objects.mu.Lock()
				v.objects.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true }
				v.objects.ackKind = "deployment"
				v.objects.mu.Unlock()
			} else {
				f.respClose = newServiceGate()
			}
			err := v.pass(t)
			applied := v.record(t, v.id)
			n, derr := decodeServiceNative(applied.Native)
			if derr != nil || applied.Operation == nil || applied.Operation.Kind != store.ServiceApply || n.Service.Declaration != "" {
				t.Fatalf("after the first pass %+v: %v (%v)", applied, derr, err)
			}
			if bad != n.Deployment.Refused || bad == (n.Deployment.UID != "") {
				t.Fatalf("deployment %+v", n.Deployment)
			}
			v.deleteService(t, v.id)

			type seen struct {
				op     *store.ServiceOperation
				native serving.Native
			}
			deletes := make(chan seen, 4)
			v.objects.mu.Lock()
			v.objects.fail = func(r *http.Request) error {
				if r.Method == http.MethodDelete {
					s, err := v.o.s.ServiceFor(v.actor, v.id)
					if err == nil {
						select {
						case deletes <- seen{s.Operation, s.Native}:
						default: // counted by the tap; never blocks the handler
						}
					}
				}
				return nil
			}
			v.objects.mu.Unlock()
			if !bad {
				f.respClose.open()
				_, _, _, x := v.ownership(v.id)
				v.joinExchange(t, x)
				for range 4 {
					if _, err := v.o.s.ServiceFor(v.actor, v.id); errors.Is(err, store.ErrNotFound) {
						break
					}
					v.pass(t)
				}
				if _, err := v.o.s.ServiceFor(v.actor, v.id); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("service not retired: %v", err)
				}
				d := receive(t, "the Deployment's DELETE", deletes)
				if d.op == nil || *d.op != *applied.Operation || d.native != applied.Native {
					t.Fatalf("DELETE under %+v over %q, want the Apply operation %+v", d.op, d.native, applied.Operation)
				}
				if p.count(http.MethodDelete, "deployments", v.id) != 1 || p.count(http.MethodDelete, "services", v.id) != 0 ||
					p.count(http.MethodPost, "services", v.id) != 0 {
					t.Fatal("requests other than the one DELETE")
				}
				return
			}
			for range 2 {
				v.pass(t)
			}
			r := v.record(t, v.id)
			if r.Operation == nil || *r.Operation != *applied.Operation || r.Native != applied.Native ||
				r.Reserved == (store.ServiceReservation{}) || r.Observation == nil || r.Observation.Reason != serving.IdentityConflict {
				t.Fatalf("refused cleanup record %+v", r)
			}
			if p.count(http.MethodDelete, "deployments", v.id) != 0 || p.count(http.MethodDelete, "services", v.id) != 0 {
				t.Fatal("refused create's object deleted")
			}
		})
	}
}

// TestServiceVerticalReleasedReservation (V14): a Ready service whose
// objects are removed (MODELED) is observed absent while present and then
// deleted: released, with its reservation still held. Its cleanup only
// reads: an absence observed afresh is committed, the record reread and
// retired with the reservation released. A read error or an object at its
// name that is not its own holds the record, operation-less, with its
// reservation. No DELETE or create is sent and no operation is made.
func TestServiceVerticalReleasedReservation(t *testing.T) {
	for _, mode := range []string{"fresh absence", "read error", "not absent"} {
		t.Run(mode, func(t *testing.T) {
			v, p := newTappedVertical(t, true)
			ctx := context.Background()
			name := serviceObjectName(v.id)
			if err := v.client.AppsV1().Deployments("workloads").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := v.client.CoreV1().Services("workloads").Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			v.pass(t)
			v.deleteService(t, v.id)
			rec := v.record(t, v.id)
			if rec.Operation != nil || rec.Observation == nil || !rec.Observation.Absent || rec.Reserved == (store.ServiceReservation{}) {
				t.Fatalf("not released with its reservation held: %+v", rec)
			}
			calls := p.total()
			switch mode {
			case "read error":
				v.objects.mu.Lock()
				v.objects.fail = func(r *http.Request) error {
					if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/"+name) {
						return errors.New("modeled read failure")
					}
					return nil
				}
				v.objects.mu.Unlock()
			case "not absent":
				if _, err := v.client.AppsV1().Deployments("workloads").Create(ctx, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "workloads", UID: "another-object"},
				}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				err := v.pass(t)
				if mode == "fresh absence" {
					if err != nil {
						t.Fatal(err)
					}
					break
				}
				if err == nil {
					t.Fatal("released record not held")
				}
			}
			if p.total() != calls || v.objects.deleted("deployment") != 0 || v.objects.deleted("service") != 0 {
				t.Fatalf("cleanup sent %d writes", p.total()-calls)
			}
			_, err := v.o.s.ServiceFor(v.actor, v.id)
			if mode == "fresh absence" {
				if !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("released service not retired: %v", err)
				}
				return
			}
			r := v.record(t, v.id)
			if r.Operation != nil || r.Native != rec.Native || r.Reserved != rec.Reserved || r.Revision != rec.Revision {
				t.Fatalf("held record committed or released: %+v", r)
			}
		})
	}
}
