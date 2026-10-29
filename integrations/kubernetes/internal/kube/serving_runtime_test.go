package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

// AUTHORED_NOT_RUN. These tests drive the service runtime against
// client-go's fake clientset, with a model writer in place of the HTTP
// writer: it applies each create and delete to the fake clientset and
// answers as the API server would. Lost answers, known-bad
// acknowledgments, and objects changed between calls are MODELED faults,
// not observed API server behavior.

const (
	testServiceID    = "0123456789abcdef0123456789abcdef"
	testServiceNonce = "fedcba9876543210fedcba9876543210"
	testServiceOwner = "00112233445566778899aabbccddeeff"
	testServiceImage = "registry.example.com/team/web@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

// modelWriter applies writes to a fake clientset and counts them.
type modelWriter struct {
	client *fake.Clientset
	ns     string

	mu      sync.Mutex
	creates map[string]int // kind: sends
	deletes map[string]int
	uids    map[string]types.UID // kind: UID of each delete's precondition, last
	// lose makes the next write apply and then report no answer.
	lose bool
	// status, if set, makes the next create apply nothing and answer it.
	status int
	// ack, if set, changes the next created object before it is stored and
	// acknowledged: a known-bad acknowledgment.
	ack func(obj any)
	// answerAs, if set, makes the next create apply exactly and then answer
	// with this status: the stored object as the body of a 2xx, a Status
	// otherwise.
	answerAs int
	// close, if set, is the next answer's original response body Close
	// (MODELED); otherwise an answer has none.
	close *bodyClose
	// readErr, if set, makes the next answer's body incomplete with this
	// read error (MODELED).
	readErr error
}

// closeFunc is a MODELED response body whose Close calls the function.
type closeFunc func() error

func (f closeFunc) Close() error { return f() }

// modeledClose returns an original Close (MODELED) that returns err once
// hold, if not nil, is closed.
func modeledClose(err error, hold chan struct{}) *bodyClose {
	return startClose(closeFunc(func() error {
		if hold != nil {
			<-hold
		}
		return err
	}))
}

var errModeledRead = errors.New("modeled response body read error")

// attach gives a the next MODELED Close and read fault.
func (w *modelWriter) attach(a serviceAnswer) serviceAnswer {
	w.mu.Lock()
	defer w.mu.Unlock()
	a.Close, w.close = w.close, nil
	if w.readErr != nil && a.Status != 0 {
		a.Complete, a.Err = false, w.readErr
		w.readErr = nil
	}
	return a
}

func newModelWriter(client *fake.Clientset) *modelWriter {
	return &modelWriter{client: client, ns: "workloads", creates: map[string]int{}, deletes: map[string]int{}, uids: map[string]types.UID{}}
}

func kindName(k serviceKind) string {
	if k == deploymentObject {
		return "deployment"
	}
	return "service"
}

func (w *modelWriter) take(m map[string]int, k serviceKind) (lose bool, status int, ack func(any)) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m[kindName(k)]++
	lose, status, ack = w.lose, w.status, w.ack
	w.lose, w.status, w.ack = false, 0, nil
	return
}

func answer(status int, v any) serviceAnswer {
	raw, _ := json.Marshal(v)
	return serviceAnswer{Status: status, Body: raw, Complete: true}
}

func statusAnswer(err error) serviceAnswer {
	var s apierrors.APIStatus
	if !errors.As(err, &s) {
		return serviceAnswer{Err: err}
	}
	st := s.Status()
	st.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}
	return answer(int(st.Code), st)
}

func (w *modelWriter) create(ctx context.Context, k serviceKind, obj any) serviceAnswer {
	return w.attach(w.createAnswer(ctx, k, obj))
}

func (w *modelWriter) delete(ctx context.Context, k serviceKind, name string, uid types.UID) serviceAnswer {
	return w.attach(w.deleteAnswer(ctx, k, name, uid))
}

func (w *modelWriter) createAnswer(ctx context.Context, k serviceKind, obj any) serviceAnswer {
	lose, status, ack := w.take(w.creates, k)
	w.mu.Lock()
	as := w.answerAs
	w.answerAs = 0
	w.mu.Unlock()
	if status != 0 {
		return answer(status, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: int32(status)})
	}
	var (
		got any
		err error
	)
	switch o := obj.(type) {
	case *appsv1.Deployment:
		o = o.DeepCopy()
		o.UID = types.UID("deployment-" + o.Annotations[serviceDeclaration])
		if ack != nil {
			ack(o)
		}
		var d *appsv1.Deployment
		if d, err = w.client.AppsV1().Deployments(w.ns).Create(ctx, o, metav1.CreateOptions{}); err == nil {
			d = d.DeepCopy()
			d.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
			got = d
		}
	case *corev1.Service:
		o = o.DeepCopy()
		o.UID = types.UID("service-" + o.Annotations[serviceDeclaration])
		if ack != nil {
			ack(o)
		}
		var s *corev1.Service
		if s, err = w.client.CoreV1().Services(w.ns).Create(ctx, o, metav1.CreateOptions{}); err == nil {
			s = s.DeepCopy()
			s.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
			got = s
		}
	}
	switch {
	case lose:
		return serviceAnswer{Err: context.DeadlineExceeded}
	case err != nil:
		return statusAnswer(err)
	case as/100 == 2:
		return answer(as, got)
	case as != 0:
		return answer(as, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: int32(as)})
	}
	return answer(http.StatusCreated, got)
}

func (w *modelWriter) deleteAnswer(ctx context.Context, k serviceKind, name string, uid types.UID) serviceAnswer {
	lose, _, _ := w.take(w.deletes, k)
	w.mu.Lock()
	w.uids[kindName(k)] = uid
	w.mu.Unlock()
	var (
		cur types.UID
		err error
	)
	if k == deploymentObject {
		var d *appsv1.Deployment
		if d, err = w.client.AppsV1().Deployments(w.ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			cur = d.UID
		}
	} else {
		var s *corev1.Service
		if s, err = w.client.CoreV1().Services(w.ns).Get(ctx, name, metav1.GetOptions{}); err == nil {
			cur = s.UID
		}
	}
	switch {
	case err != nil:
		return statusAnswer(err)
	case cur != uid:
		return answer(http.StatusConflict, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Code: http.StatusConflict, Reason: metav1.StatusReasonConflict})
	}
	if k == deploymentObject {
		err = w.client.AppsV1().Deployments(w.ns).Delete(ctx, name, metav1.DeleteOptions{})
	} else {
		err = w.client.CoreV1().Services(w.ns).Delete(ctx, name, metav1.DeleteOptions{})
	}
	switch {
	case lose:
		return serviceAnswer{Err: context.DeadlineExceeded}
	case err != nil:
		return statusAnswer(err)
	}
	return answer(http.StatusOK, metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess})
}

func (w *modelWriter) sends(kind string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.creates[kind]
}

func (w *modelWriter) deleted(kind string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deletes[kind]
}

func testServiceConfig() ServiceConfig {
	return ServiceConfig{
		target: "kube",
		grants: map[string][]string{"alice": {testServiceImage}},
		profile: serviceProfile{
			namespaceUID: "ns-uid", serviceAccountUID: "sa-uid", pullSecrets: []string{"registry"},
			cpuMillis: 2000, memoryBytes: 4 << 30,
		},
	}
}

// newTestServiceRuntime returns a runtime over a fake clientset in
// namespace "workloads", sharing client and writer so that a second call
// models a restarted process. Cleanup requires a clean join.
func newTestServiceRuntime(t *testing.T, client *fake.Clientset, w *modelWriter) *serviceRuntime {
	t.Helper()
	return newTestServiceRuntimeJoin(t, client, w, func(err error) {
		if err != nil {
			t.Errorf("join service runtime: %v", err)
		}
	})
}

// newTestServiceRuntimeJoin is newTestServiceRuntime whose cleanup passes
// the join's result to check.
func newTestServiceRuntimeJoin(t *testing.T, client *fake.Clientset, w *modelWriter, check func(error)) *serviceRuntime {
	t.Helper()
	b, err := testBackend(client, "workloads", "owner")
	if err != nil {
		t.Fatal(err)
	}
	effects, end := context.WithCancel(context.Background())
	rt := &serviceRuntime{b: b, target: "kube", config: testServiceConfig(), writer: w,
		effects: effects, endEffects: end,
		locks: map[string]chan struct{}{}, declared: map[string]serving.Native{}, resolved: map[string]types.UID{},
		withdrawn: map[string]bool{}, drained: make(chan struct{})}
	rt.serve = func(w http.ResponseWriter, r *http.Request, target portTarget) *portFrame {
		t.Error("unexpected forward")
		p := &portFrame{envFrame: newEnvFrame()}
		p.enter()
		p.leave(nil)
		return p
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		check(rt.join(ctx))
	})
	return rt
}

func testRevision(t *testing.T) serving.Revision {
	t.Helper()
	h, err := serving.NewHistory(serving.Spec{
		Image:     testServiceImage,
		Argv:      []string{"/bin/web"},
		Port:      8080,
		Readiness: serving.Readiness{Path: "/healthz"},
		Resources: serving.Resources{CPUMillis: 250, MemoryBytes: 256 << 20},
		Scaling:   serving.Scaling{Replicas: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	rev, _ := h.Revision(1)
	return rev
}

func testRef(native serving.Native) serving.Ref {
	return testRefOf(testServiceID, native)
}

func testRefOf(id string, native serving.Native) serving.Ref {
	return serving.Ref{Service: id, Owner: testServiceOwner, Nonce: testServiceNonce, Binding: testBinding, Native: native}
}

// applyAll calls Apply as the controller does, committing each changed
// Native, until it returns unchanged.
func applyAll(t *testing.T, rt *serviceRuntime, native serving.Native) (serving.Native, error) {
	t.Helper()
	return applyAllOf(t, rt, testServiceID, native)
}

func applyAllOf(t *testing.T, rt *serviceRuntime, id string, native serving.Native) (serving.Native, error) {
	t.Helper()
	rev := testRevision(t)
	for range 8 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := rt.Apply(ctx, testRefOf(id, native), rev, 1)
		cancel()
		if out == "" || out == native {
			return native, err
		}
		native = out
	}
	t.Fatal("apply did not settle")
	return "", nil
}

func mustApply(t *testing.T, rt *serviceRuntime) serving.Native {
	t.Helper()
	native, err := applyAll(t, rt, "")
	if err != nil {
		t.Fatal(err)
	}
	return native
}

func getDeployment(t *testing.T, client *fake.Clientset) *appsv1.Deployment {
	t.Helper()
	return getDeploymentOf(t, client, testServiceID)
}

func getService(t *testing.T, client *fake.Clientset) *corev1.Service {
	t.Helper()
	return getServiceOf(t, client, testServiceID)
}

// getDeploymentOf returns the Deployment of service id.
func getDeploymentOf(t *testing.T, client *fake.Clientset, id string) *appsv1.Deployment {
	t.Helper()
	d, err := client.AppsV1().Deployments("workloads").Get(context.Background(), serviceObjectName(id), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// getServiceOf returns the Service of service id.
func getServiceOf(t *testing.T, client *fake.Clientset, id string) *corev1.Service {
	t.Helper()
	s, err := client.CoreV1().Services("workloads").Get(context.Background(), serviceObjectName(id), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func observe(t *testing.T, rt *serviceRuntime, native serving.Native) (serving.Observation, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return rt.Observe(ctx, testRef(native))
}

func TestServiceApplyDeclaresBeforeSending(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	rev := testRevision(t)
	ctx := context.Background()

	n1, err := rt.Apply(ctx, testRef(""), rev, 1)
	if !errors.Is(err, errServiceWritePending) || n1 == "" {
		t.Fatalf("first apply = %q, %v; want a declaration", n1, err)
	}
	if w.sends("deployment") != 0 {
		t.Fatal("create sent before its declaration was committed")
	}
	d1, _ := decodeServiceNative(n1)
	if d1.Deployment.Spec == "" || d1.Deployment.Pod == "" {
		t.Fatalf("declaration without witnesses: %+v", d1.Deployment)
	}
	n2, err := rt.Apply(ctx, testRef(n1), rev, 1)
	if err != nil || n2 == "" || w.sends("deployment") != 1 {
		t.Fatalf("committed declaration: %q, %v, %d sends", n2, err, w.sends("deployment"))
	}
	// If committing n2 failed, the controller calls again with n1: the
	// declaration was taken, so it resolves by its accepted object and
	// never resends.
	n3, err := rt.Apply(ctx, testRef(n1), rev, 1)
	d2, _ := decodeServiceNative(n2)
	d3, _ := decodeServiceNative(n3)
	if err != nil || d3.Deployment.UID == "" || d3.Deployment.UID != d2.Deployment.UID || w.sends("deployment") != 1 {
		t.Fatalf("repeated declaration: %q, %v, %d sends", n3, err, w.sends("deployment"))
	}
	native, err := applyAll(t, rt, n2)
	if err != nil {
		t.Fatal(err)
	}
	if w.sends("deployment") != 1 || w.sends("service") != 1 {
		t.Fatalf("sends = %v, want one of each", w.creates)
	}
	n, err := decodeServiceNative(native)
	if err != nil || n.Deployment.UID == "" || n.Service.UID == "" {
		t.Fatalf("native %q: %v", native, err)
	}
	d := getDeployment(t, client)
	if s := d.Spec.Strategy.RollingUpdate; *d.Spec.Replicas != 1 || s == nil || s.MaxSurge.IntValue() != 0 {
		t.Fatalf("deployment strategy %+v", d.Spec)
	}
	if pod := d.Spec.Template.Spec; *pod.AutomountServiceAccountToken || pod.ServiceAccountName != environmentServiceAccount {
		t.Fatalf("pod identity %+v", pod)
	}
}

func TestServiceRestartNeverResends(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	first := newTestServiceRuntime(t, client, w)
	rev := testRevision(t)
	declared, err := first.Apply(context.Background(), testRef(""), rev, 1)
	if !errors.Is(err, errServiceWritePending) {
		t.Fatal(err)
	}
	// A new process with the committed declaration may not send it.
	restarted := newTestServiceRuntime(t, client, w)
	out, err := restarted.Apply(context.Background(), testRef(declared), rev, 1)
	if !errors.Is(err, errServiceCreateUnresolved) || out != "" {
		t.Fatalf("restarted apply = %q, %v; want unresolved", out, err)
	}
	if w.sends("deployment") != 0 {
		t.Fatal("restarted process sent an earlier declaration")
	}
	// Unresolved is never absent.
	if _, err := observe(t, restarted, declared); !errors.Is(err, errServiceCreateUnresolved) {
		t.Fatalf("observe = %v, want unresolved", err)
	}
	o, err := restarted.Delete(context.Background(), testRef(declared))
	if err == nil && o.Absent {
		t.Fatal("delete reported absence of an unresolved create")
	}
}

func TestServiceLostResponseResolvedByRendering(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	rev := testRevision(t)
	declared, _ := rt.Apply(context.Background(), testRef(""), rev, 1)
	w.lose = true // MODELED: applied, answer lost
	sent, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
	if err == nil || sent == "" {
		t.Fatalf("lost response apply = %q, %v", sent, err)
	}
	n, _ := decodeServiceNative(sent)
	if !n.Deployment.Sent || n.Deployment.UID != "" || n.Deployment.Refused {
		t.Fatalf("lost response native %+v", n.Deployment)
	}
	// A later process resolves it by the exact rendering, without sending
	// again.
	restarted := newTestServiceRuntime(t, client, w)
	if _, err := applyAll(t, restarted, sent); err != nil {
		t.Fatal(err)
	}
	if w.sends("deployment") != 1 {
		t.Fatalf("deployment sends = %d, want 1", w.sends("deployment"))
	}
}

// TestServiceRecoveryRefusesChangedCarrier: an object that carries the
// declaration but is not exactly the rendering never resolves a lost
// create.
func TestServiceRecoveryRefusesChangedCarrier(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	rev := testRevision(t)
	declared, _ := rt.Apply(context.Background(), testRef(""), rev, 1)
	w.lose = true
	w.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true } // MODELED
	sent, _ := rt.Apply(context.Background(), testRef(declared), rev, 1)
	restarted := newTestServiceRuntime(t, client, w)
	out, err := restarted.Apply(context.Background(), testRef(sent), rev, 1)
	if !errors.Is(err, errServiceCreateUnresolved) || out != "" {
		t.Fatalf("recovery over a changed carrier = %q, %v; want unresolved", out, err)
	}
	if _, err := observe(t, restarted, sent); !errors.Is(err, errServiceCreateUnresolved) {
		t.Fatalf("observe = %v, want unresolved", err)
	}
	if _, err := restarted.Delete(context.Background(), testRef(sent)); err == nil {
		t.Fatal("delete of an unresolved create returned no error")
	}
	if w.deleted("deployment") != 0 {
		t.Fatal("deleted an object that is not the rendering")
	}
}

// TestServiceKnownBadAckRefused: an acknowledged object that is not
// exactly the rendering is refused for good, with the answer's witness;
// neither a later exact read nor Delete accepts it, and it is never Absent.
func TestServiceKnownBadAckRefused(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	rev := testRevision(t)
	declared, _ := rt.Apply(context.Background(), testRef(""), rev, 1)
	w.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true } // MODELED
	refused, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
	if !errors.Is(err, serving.ErrIdentityConflict) || refused == "" {
		t.Fatalf("bad ack apply = %q, %v; want a refused native", refused, err)
	}
	n, err := decodeServiceNative(refused)
	if err != nil || !n.Deployment.Refused || n.Deployment.UID != "" || n.Deployment.Refusal == nil ||
		n.Deployment.Refusal.Status != http.StatusCreated || !n.Deployment.Refusal.Decoded {
		t.Fatalf("refused native %+v: %v", n.Deployment, err)
	}
	// MODELED: the object is later read back exactly as rendered, with the
	// acknowledged UID. It is still refused.
	d := getDeployment(t, client)
	d.Spec.Template.Spec.HostNetwork = false
	if _, err := client.AppsV1().Deployments("workloads").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if out, err := rt.Apply(context.Background(), testRef(refused), rev, 1); !errors.Is(err, serving.ErrIdentityConflict) || out != "" {
			t.Fatalf("apply after refusal = %q, %v", out, err)
		}
		o, err := observe(t, rt, refused)
		if err != nil || o.Absent || o.Reason != serving.IdentityConflict {
			t.Fatalf("observe after refusal = %+v, %v; want identity conflict", o, err)
		}
		o, err = rt.Delete(context.Background(), testRef(refused))
		if err != nil || o.Absent || o.Reason != serving.IdentityConflict {
			t.Fatalf("delete after refusal = %+v, %v; want identity conflict", o, err)
		}
	}
	if w.deleted("deployment") != 0 || w.sends("deployment") != 1 {
		t.Fatalf("refused object: %d deletes, %d creates", w.deleted("deployment"), w.sends("deployment"))
	}
}

// TestServiceRefusedAnswers: every complete create answer but a 201 of
// exactly the rendering is a terminal refusal, recorded with its status and
// no UID. A refusal is not proof of absence: whether the API server stored
// nothing (MODELED with status) or stored exactly the rendering (MODELED
// with answerAs), the create is observed as an identity conflict, never
// Absent; no later Apply, Observe, Delete or restarted process sends again,
// adopts the stored object or deletes anything.
func TestServiceRefusedAnswers(t *testing.T) {
	statuses := []int{
		http.StatusOK, http.StatusAccepted, http.StatusNoContent,
		http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect,
		http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusConflict, http.StatusUnprocessableEntity, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout,
	}
	for _, status := range statuses {
		for _, stored := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d stored %v", status, stored), func(t *testing.T) {
				client := fake.NewSimpleClientset()
				w := newModelWriter(client)
				rt := newTestServiceRuntime(t, client, w)
				rev := testRevision(t)
				declared, err := rt.Apply(context.Background(), testRef(""), rev, 1)
				if !errors.Is(err, errServiceWritePending) {
					t.Fatal(err)
				}
				if stored {
					w.answerAs = status
				} else {
					w.status = status
				}
				refused, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
				if !errors.Is(err, serving.ErrIdentityConflict) || refused == "" {
					t.Fatalf("apply = %q, %v; want a refused native", refused, err)
				}
				n, err := decodeServiceNative(refused)
				if err != nil || !n.Deployment.Sent || !n.Deployment.Refused || n.Deployment.UID != "" ||
					n.Deployment.Refusal == nil || n.Deployment.Refusal.Status != status {
					t.Fatalf("refused native %+v: %v", n.Deployment, err)
				}
				if n.Service.Sent {
					t.Fatal("service create sent after a refused deployment")
				}
				if stored && deploymentIdentity(getDeployment(t, client)).witness() != n.Deployment.Spec {
					t.Fatal("stored object is not the rendering")
				}
				for _, proc := range []*serviceRuntime{rt, newTestServiceRuntime(t, client, w)} {
					for range 2 {
						if out, err := proc.Apply(context.Background(), testRef(refused), rev, 1); !errors.Is(err, serving.ErrIdentityConflict) || out != "" {
							t.Fatalf("apply after refusal = %q, %v", out, err)
						}
						o, err := observe(t, proc, refused)
						if err != nil || o.Absent || o.Reason != serving.IdentityConflict {
							t.Fatalf("observe after refusal = %+v, %v; want identity conflict", o, err)
						}
						o, err = proc.Delete(context.Background(), testRef(refused))
						if err != nil || o.Absent || o.Reason != serving.IdentityConflict {
							t.Fatalf("delete after refusal = %+v, %v; want identity conflict", o, err)
						}
					}
				}
				if w.sends("deployment") != 1 || w.sends("service") != 0 || w.deleted("deployment") != 0 || w.deleted("service") != 0 {
					t.Fatalf("after refusal: %d+%d creates, %d+%d deletes; want 1+0, 0+0",
						w.sends("deployment"), w.sends("service"), w.deleted("deployment"), w.deleted("service"))
				}
				if stored && getDeployment(t, client).UID != types.UID("deployment-"+n.Deployment.Declaration) {
					t.Fatal("stored object changed")
				}
			})
		}
	}
}

// TestServiceCreatedExactAccepted is the positive of the refusals above: a
// complete 201 of exactly the rendering records its UID.
func TestServiceCreatedExactAccepted(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	native := mustApply(t, rt)
	n, err := decodeServiceNative(native)
	if err != nil || n.Deployment.Refused || n.Service.Refused ||
		n.Deployment.UID != getDeployment(t, client).UID || n.Service.UID != getService(t, client).UID {
		t.Fatalf("created native %+v / %+v: %v", n.Deployment, n.Service, err)
	}
	if w.sends("deployment") != 1 || w.sends("service") != 1 {
		t.Fatalf("creates %d+%d, want 1+1", w.sends("deployment"), w.sends("service"))
	}
}

const bystanderID = "abcdefabcdefabcdefabcdefabcdef01"

var errModeledClose = errors.New("modeled response body close error")

// closeFailed is a join check requiring the sticky Close failure.
func closeFailed(t *testing.T) func(error) {
	return func(err error) {
		if !errors.Is(err, errServiceCloseFailed) || !errors.Is(err, errModeledClose) {
			t.Errorf("join = %v, want the failed original close kept", err)
		}
	}
}

// bystanderApplies requires that another service of the same runtime,
// whose own answers are healthy, still applies completely.
func bystanderApplies(t *testing.T, rt *serviceRuntime, client *fake.Clientset) {
	t.Helper()
	native, err := applyAllOf(t, rt, bystanderID, "")
	if err != nil {
		t.Fatalf("bystander apply: %v", err)
	}
	n, err := decodeServiceNative(native)
	if err != nil || n.Deployment.UID != getDeploymentOf(t, client, bystanderID).UID || n.Service.UID != getServiceOf(t, client, bystanderID).UID {
		t.Fatalf("bystander native %+v: %v", n, err)
	}
}

// TestServiceCreateCloseOutcomes sends the Deployment create with each
// answer, paired with a MODELED original response body Close that returns
// nil, fails, or is held. A standard response proves no such Close failure
// ran; these are faults of the model writer.
//
// A complete exact 201 grants its UID whatever the Close does: that truth is
// kept. A failed Close is returned with every other cause, read errors
// included, and from then on fences that service alone in this process: no
// declaration of the next object, no read, no send, no observation and no
// delete, and the runtime's join is never clean. A held Close fences it
// until that original Close returns, keeps the runtime's admission, and
// drains only by that return. A healthy bystander service is never fenced.
func TestServiceCreateCloseOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		setup  func(w *modelWriter)
		closer string // "nil", "failed" or "held"
		check  func(t *testing.T, n serviceNative, err error)
	}{
		{"exact201 nil close", nil, "nil", func(t *testing.T, n serviceNative, err error) {
			if err != nil || n.Deployment.UID == "" {
				t.Fatalf("native %+v, %v", n.Deployment, err)
			}
		}},
		{"exact201 failed close", nil, "failed", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, errServiceCloseFailed) || !errors.Is(err, errModeledClose) || n.Deployment.UID == "" || n.Deployment.Refused {
				t.Fatalf("native %+v, %v; want the UID kept with the close failure", n.Deployment, err)
			}
		}},
		{"exact201 held close", nil, "held", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, errServiceClosePending) || n.Deployment.UID == "" {
				t.Fatalf("native %+v, %v; want the UID kept with the close pending", n.Deployment, err)
			}
		}},
		{"bad201 failed close", func(w *modelWriter) {
			w.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true }
		}, "failed", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, serving.ErrIdentityConflict) || !errors.Is(err, errServiceCloseFailed) || !n.Deployment.Refused || n.Deployment.UID != "" {
				t.Fatalf("native %+v, %v; want refusal and close failure", n.Deployment, err)
			}
		}},
		{"non201 failed close", func(w *modelWriter) { w.status = http.StatusConflict }, "failed", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, serving.ErrIdentityConflict) || !errors.Is(err, errServiceCloseFailed) || !n.Deployment.Refused ||
				n.Deployment.Refusal == nil || n.Deployment.Refusal.Status != http.StatusConflict {
				t.Fatalf("native %+v, %v; want a 409 refusal and close failure", n.Deployment, err)
			}
		}},
		{"read error failed close", func(w *modelWriter) { w.readErr = errModeledRead }, "failed", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, errModeledRead) || !errors.Is(err, errServiceCloseFailed) || !n.Deployment.Sent || n.Deployment.UID != "" || n.Deployment.Refused {
				t.Fatalf("native %+v, %v; want unresolved with both causes", n.Deployment, err)
			}
		}},
		{"read error nil close", func(w *modelWriter) { w.readErr = errModeledRead }, "nil", func(t *testing.T, n serviceNative, err error) {
			if !errors.Is(err, errModeledRead) || errors.Is(err, errServiceCloseFailed) || n.Deployment.UID != "" || n.Deployment.Refused {
				t.Fatalf("native %+v, %v; want unresolved by the read only", n.Deployment, err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			w := newModelWriter(client)
			check := func(err error) {
				if err != nil {
					t.Errorf("join service runtime: %v", err)
				}
			}
			if tt.closer == "failed" {
				check = closeFailed(t)
			}
			rt := newTestServiceRuntimeJoin(t, client, w, check)
			hold := make(chan struct{})
			released := false
			release := func() {
				if !released {
					released = true
					close(hold)
				}
			}
			t.Cleanup(release) // runs before the runtime's join
			rev := testRevision(t)
			declared, err := rt.Apply(context.Background(), testRef(""), rev, 1)
			if !errors.Is(err, errServiceWritePending) {
				t.Fatal(err)
			}
			if tt.setup != nil {
				tt.setup(w)
			}
			switch tt.closer {
			case "nil":
				w.close = modeledClose(nil, nil)
			case "failed":
				w.close = modeledClose(errModeledClose, nil)
			case "held":
				w.close = modeledClose(nil, hold)
			}
			heldClose := w.close
			if tt.closer != "held" {
				<-w.close.done // the Close has returned before the answer is read
			}
			sent, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
			if sent == "" {
				t.Fatalf("apply = %q, %v; want the sent native", sent, err)
			}
			n, derr := decodeServiceNative(sent)
			if derr != nil {
				t.Fatal(derr)
			}
			tt.check(t, n, err)
			if tt.closer == "nil" {
				bystanderApplies(t, rt, client)
				return
			}
			fenced := func(want error) {
				t.Helper()
				before := []int{w.sends("deployment"), w.sends("service"), w.deleted("deployment"), w.deleted("service")}
				out, err := rt.Apply(context.Background(), testRef(sent), rev, 1)
				if !errors.Is(err, want) || out != "" {
					t.Fatalf("apply while fenced = %q, %v; want %v", out, err, want)
				}
				if n.Deployment.Refused && !errors.Is(err, serving.ErrIdentityConflict) {
					t.Fatalf("fenced apply of a refused create = %v; want the refusal kept", err)
				}
				if _, err := observe(t, rt, sent); !errors.Is(err, want) {
					t.Fatalf("observe while fenced = %v; want %v", err, want)
				}
				if _, err := rt.Delete(context.Background(), testRef(sent)); !errors.Is(err, want) {
					t.Fatalf("delete while fenced = %v; want %v", err, want)
				}
				after := []int{w.sends("deployment"), w.sends("service"), w.deleted("deployment"), w.deleted("service")}
				if !slices.Equal(before, after) {
					t.Fatalf("fenced calls changed write counts: %v -> %v", before, after)
				}
			}
			if tt.closer == "failed" {
				for range 2 {
					fenced(errServiceCloseFailed)
				}
				bystanderApplies(t, rt, client)
				return
			}
			// Held: fenced until the original Close returns and its fact
			// is offered, acknowledged and drained; the runtime's admission
			// is kept meanwhile.
			fenced(errServiceClosePending)
			bystanderApplies(t, rt, client)
			rt.mu.Lock()
			active := rt.active
			rt.mu.Unlock()
			if active != 1 {
				t.Fatalf("active = %d with one original close held, want 1", active)
			}
			if _, token, err := rt.Drain(testRef(sent)); token != 0 || !errors.Is(err, errServiceClosePending) {
				t.Fatalf("drain with the close held = %d, %v", token, err)
			}
			release()
			<-heldClose.done // the original Close's own return, not a sleep
			// Returned is not acknowledged: still fenced.
			fenced(errServiceFactsUnacknowledged)
			drainAcked(t, rt, testRef(sent), 1)
			native, err := applyAll(t, rt, sent)
			if err != nil {
				t.Fatalf("apply after the original close returned: %v", err)
			}
			if got, _ := decodeServiceNative(native); got.Service.UID == "" || got.Deployment.UID != n.Deployment.UID {
				t.Fatalf("drained native %+v", got)
			}
		})
	}
}

// TestServiceHeldCloseKeepsJoin: join is not clean, and keeps its admission,
// while an original Close (MODELED) is held; it becomes clean only when
// that Close returns nil, and stays failed when it returns an error.
func TestServiceHeldCloseKeepsJoin(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail %v", fail), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			w := newModelWriter(client)
			check := func(err error) {
				if err != nil {
					t.Errorf("join after the close returned nil: %v", err)
				}
			}
			var cerr error
			if fail {
				check, cerr = closeFailed(t), errModeledClose
			}
			rt := newTestServiceRuntimeJoin(t, client, w, check)
			hold := make(chan struct{})
			rev := testRevision(t)
			declared, _ := rt.Apply(context.Background(), testRef(""), rev, 1)
			c := modeledClose(cerr, hold)
			w.close = c
			sent, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
			if !errors.Is(err, errServiceClosePending) {
				close(hold)
				t.Fatalf("apply = %v, want the close pending", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err = rt.join(ctx)
			cancel()
			if !errors.Is(err, errServiceClosePending) || !errors.Is(err, context.DeadlineExceeded) {
				close(hold)
				t.Fatalf("join with the close held = %v", err)
			}
			close(hold)
			<-c.done
			// Returned but not acknowledged: join waits for the original
			// only, and is not clean.
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := rt.join(ctx); !errors.Is(err, errServiceFactsUnacknowledged) {
				t.Fatalf("join with the returned close unacknowledged = %v", err)
			}
			_, token, oerr := rt.Drain(testRef(sent))
			if token == 0 || fail != errors.Is(oerr, errServiceCloseFailed) {
				t.Fatalf("drain = %d, %v; want the close's offer", token, oerr)
			}
			if err := rt.Ack(testRef(sent), token); err != nil {
				t.Fatal(err)
			}
			_, token, derr := rt.Drain(testRef(sent))
			if token != 0 || fail != errors.Is(derr, errServiceCloseFailed) || !fail && derr != nil {
				t.Fatalf("drain after ack = %d, %v", token, derr)
			}
			err = rt.join(ctx)
			if fail != errors.Is(err, errServiceCloseFailed) || !fail && err != nil {
				t.Fatalf("join after the original close returned = %v", err)
			}
		})
	}
}

// TestServiceDeleteCloseOutcomes: a selected DELETE success (200) whose
// original response Close fails (MODELED) keeps that cause, sends no
// further delete, and fences the service; repeated Deletes send nothing
// and never report absence. The nil-Close positive deletes both objects
// and observes them absent.
func TestServiceDeleteCloseOutcomes(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail %v", fail), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			w := newModelWriter(client)
			check := func(err error) {
				if err != nil {
					t.Errorf("join service runtime: %v", err)
				}
			}
			cerr := error(nil)
			if fail {
				check, cerr = closeFailed(t), errModeledClose
			}
			rt := newTestServiceRuntimeJoin(t, client, w, check)
			native := mustApply(t, rt)
			w.close = modeledClose(cerr, nil)
			<-w.close.done
			o, err := rt.Delete(context.Background(), testRef(native))
			if !fail {
				if err != nil || !o.Absent || w.deleted("deployment") != 1 || w.deleted("service") != 1 {
					t.Fatalf("delete = %+v, %v; %d+%d deletes", o, err, w.deleted("deployment"), w.deleted("service"))
				}
				return
			}
			if !errors.Is(err, errServiceCloseFailed) || !errors.Is(err, errModeledClose) || o.Absent {
				t.Fatalf("delete = %+v, %v; want the close failure", o, err)
			}
			for range 2 {
				if o, err := rt.Delete(context.Background(), testRef(native)); !errors.Is(err, errServiceCloseFailed) || o.Absent {
					t.Fatalf("repeated delete = %+v, %v", o, err)
				}
			}
			if w.deleted("deployment") != 1 || w.deleted("service") != 0 {
				t.Fatalf("deletes %d+%d, want 1+0: nothing after the failed close", w.deleted("deployment"), w.deleted("service"))
			}
			bystanderApplies(t, rt, client)
		})
	}
}

// drainAcked drains ref's owned exchanges as the controller does once each
// offer's Native is committed: it acknowledges each offer, with its exact
// token and Native, until Drain reports done, and requires want offers.
func drainAcked(t *testing.T, rt *serviceRuntime, ref serving.Ref, want int) serving.Native {
	t.Helper()
	offers := 0
	for range 8 {
		native, token, err := rt.Drain(ref)
		if token == 0 {
			if err != nil || offers != want {
				t.Fatalf("drain = %v after %d offers, want done after %d", err, offers, want)
			}
			return native
		}
		offers++
		ref.Native = native
		if err := rt.Ack(ref, token); err != nil {
			t.Fatalf("ack offer %d: %v", token, err)
		}
	}
	t.Fatal("drain did not settle")
	return ""
}

// heldWriter is a modelWriter whose next write, if hold is set, returns at
// once with its original exchange pending, as the actual writer does once
// its caller's wait ends; answer later delivers the model's answer to that
// same exchange, as its Do and read returning. Both are MODELED: no
// transport runs. The exchange's request body is closed by the test.
//
// release makes every original of every exchange w held return. Tests
// register it as a cleanup as soon as w exists, before any Fatal, together
// with the release of any response Close hold; the runtime's join can
// return only then. Nothing else closes an exchange's done.
type heldWriter struct {
	*modelWriter
	hold     bool
	x        *serviceExchange
	answerX  func() // answers x once
	releases []func()
}

func (w *heldWriter) create(ctx context.Context, k serviceKind, obj any) serviceAnswer {
	return w.pend(w.modelWriter.create(ctx, k, obj))
}

func (w *heldWriter) delete(ctx context.Context, k serviceKind, name string, uid types.UID) serviceAnswer {
	return w.pend(w.modelWriter.delete(ctx, k, name, uid))
}

func (w *heldWriter) pend(a serviceAnswer) serviceAnswer {
	if !w.hold {
		return a
	}
	w.hold = false
	x := &serviceExchange{body: newOneShotBody(nil), done: make(chan struct{})}
	var once sync.Once
	answer := func() {
		once.Do(func() {
			x.a = a
			close(x.done)
		})
	}
	w.x, w.answerX = x, answer
	w.releases = append(w.releases, func() {
		answer()
		x.body.Close()
	})
	return x.answer(x.facts())
}

// answer makes the held exchange's Do and read return its model answer.
func (w *heldWriter) answer() { w.answerX() }

// release makes the Do, read and request body Close of every exchange w
// held return; it is idempotent.
func (w *heldWriter) release() {
	for _, f := range w.releases {
		f()
	}
}

// TestServiceDrainTokenRace: one owned delete exchange (MODELED originals)
// produces three facts, each after the offer before it was made and before
// its Ack, with the Native unchanged throughout. Each offer repeats
// unchanged until acknowledged; an Ack acknowledges only its own offer's
// facts, so each later fact gets a new token; zero, forged, stale,
// wrong-Ref, wrong-Native and retired tokens change nothing; only the Drain
// after the last Ack reports done. A retired token never acknowledges a
// later exchange of the same Ref and Native.
func TestServiceDrainTokenRace(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	hold := make(chan struct{})
	rt := newTestServiceRuntime(t, client, w)
	t.Cleanup(func() { // before the runtime's join
		select {
		case <-hold:
		default:
			close(hold)
		}
	})
	native := mustApply(t, rt)
	ref := testRef(native)
	hw := &heldWriter{modelWriter: w, hold: true}
	t.Cleanup(hw.release) // runs before the hold's release and the runtime's join
	rt.writer = hw
	c := modeledClose(nil, hold)
	w.close = c
	if o, err := rt.Delete(context.Background(), ref); !errors.Is(err, errServiceAnswerPending) || o.Absent {
		t.Fatalf("delete = %+v, %v; want its answer pending", o, err)
	}
	if n, token, err := rt.Drain(ref); token != 0 || n != native || !errors.Is(err, errServiceAnswerPending) {
		t.Fatalf("drain before any fact = %q, %d, %v", n, token, err)
	}
	fenced := func() {
		t.Helper()
		if o, err := rt.Delete(context.Background(), ref); err == nil || o.Absent {
			t.Fatalf("delete while held = %+v, %v", o, err)
		}
		if w.deleted("deployment") != 1 || w.deleted("service") != 0 {
			t.Fatalf("deletes %d+%d while held, want 1+0", w.deleted("deployment"), w.deleted("service"))
		}
	}
	fenced()

	hw.answer() // fact 1
	n1, t1, e1 := rt.Drain(ref)
	if t1 == 0 || n1 != native || !errors.Is(e1, errServiceRequestClosePending) || !errors.Is(e1, errServiceClosePending) {
		t.Fatalf("first offer = %q, %d, %v", n1, t1, e1)
	}
	hw.x.body.Close() // fact 2, before Ack(t1), Native unchanged
	if n, token, err := rt.Drain(ref); n != n1 || token != t1 || err != e1 {
		t.Fatalf("repeated offer = %q, %d, %v; want %q, %d, %v unchanged", n, token, err, n1, t1, e1)
	}
	other := ref
	other.Nonce = testServiceOwner
	for _, bad := range []struct {
		name  string
		ref   serving.Ref
		token uint64
	}{
		{"zero", ref, 0},
		{"forged", ref, t1 + 1000},
		{"wrong service", testRefOf(bystanderID, native), t1},
		{"wrong nonce", other, t1},
		{"wrong native", testRef(native + "x"), t1},
	} {
		if err := rt.Ack(bad.ref, bad.token); err == nil {
			t.Fatalf("ack %s accepted", bad.name)
		}
		if _, token, _ := rt.Drain(ref); token != t1 {
			t.Fatalf("after ack %s, offer %d, want %d unchanged", bad.name, token, t1)
		}
	}
	fenced()
	if err := rt.Ack(ref, t1); err != nil {
		t.Fatal(err)
	}
	if err := rt.Ack(ref, t1); err == nil {
		t.Fatal("retired token acknowledged twice")
	}
	n2, t2, e2 := rt.Drain(ref)
	if t2 == 0 || t2 == t1 || n2 != native || errors.Is(e2, errServiceRequestClosePending) || !errors.Is(e2, errServiceClosePending) {
		t.Fatalf("second offer = %q, %d, %v; want a new token for the request close", n2, t2, e2)
	}
	close(hold) // fact 3, before Ack(t2)
	<-c.done
	if err := rt.Ack(ref, t2); err != nil {
		t.Fatal(err)
	}
	fenced()
	n3, t3, e3 := rt.Drain(ref)
	if t3 == 0 || t3 == t2 || t3 == t1 || n3 != native || e3 != nil {
		t.Fatalf("third offer = %q, %d, %v; want a new token for the response close", n3, t3, e3)
	}
	if err := rt.Ack(ref, t3); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if n, token, err := rt.Drain(ref); n != native || token != 0 || err != nil {
			t.Fatalf("drain after the last ack = %q, %d, %v; want done", n, token, err)
		}
	}

	// A later exchange of the same Ref and Native: no retired token
	// acknowledges it.
	hw.hold = true
	if _, err := rt.Delete(context.Background(), ref); !errors.Is(err, errServiceAnswerPending) {
		t.Fatalf("second delete = %v", err)
	}
	hw.answer()
	hw.x.body.Close()
	_, t4, _ := rt.Drain(ref)
	if t4 <= t3 {
		t.Fatalf("new exchange's token %d, want above %d", t4, t3)
	}
	for _, old := range []uint64{t1, t2, t3} {
		if err := rt.Ack(ref, old); err == nil {
			t.Fatalf("retired token %d acknowledged a new exchange", old)
		}
	}
	if err := rt.Ack(ref, t4); err != nil {
		t.Fatal(err)
	}
	drainAcked(t, rt, ref, 0)
	if o, err := rt.Delete(context.Background(), ref); err != nil || !o.Absent {
		t.Fatalf("delete after drain = %+v, %v", o, err)
	}
}

// TestServiceDrainLateCreate: a create whose write returned before its
// answer (MODELED) is Sent and unresolved, and fences its service: no
// declaration or send of the Service. The late answer is offered as the
// Native it makes of the returned one: an exact 201 its UID, a bad 201 a
// refusal with ErrIdentityConflict. An Ack under the old Native is refused.
// Once acknowledged and drained, the valid UID's service proceeds; the
// refused one stays refused, with nothing sent.
func TestServiceDrainLateCreate(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprintf("bad %v", bad), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			w := newModelWriter(client)
			rt := newTestServiceRuntime(t, client, w)
			rev := testRevision(t)
			declared, err := rt.Apply(context.Background(), testRef(""), rev, 1)
			if !errors.Is(err, errServiceWritePending) {
				t.Fatal(err)
			}
			hw := &heldWriter{modelWriter: w, hold: true}
			t.Cleanup(hw.release) // before any Fatal, and before the runtime's join
			rt.writer = hw
			if bad {
				w.ack = func(obj any) { obj.(*appsv1.Deployment).Spec.Template.Spec.HostNetwork = true }
			}
			sent, err := rt.Apply(context.Background(), testRef(declared), rev, 1)
			n, derr := decodeServiceNative(sent)
			if derr != nil || !errors.Is(err, errServiceAnswerPending) || !n.Deployment.Sent || n.Deployment.UID != "" || n.Deployment.Refused {
				t.Fatalf("apply = %+v, %v; want sent and unresolved, answer pending", n.Deployment, err)
			}
			if out, err := rt.Apply(context.Background(), testRef(sent), rev, 1); out != "" || !errors.Is(err, errServiceAnswerPending) {
				t.Fatalf("apply while held = %q, %v", out, err)
			}
			if w.sends("service") != 0 || w.sends("deployment") != 1 {
				t.Fatalf("creates %d+%d while held", w.sends("deployment"), w.sends("service"))
			}
			hw.answer()
			hw.x.body.Close()
			late, token, oerr := rt.Drain(testRef(sent))
			m, derr := decodeServiceNative(late)
			switch {
			case derr != nil || token == 0:
				t.Fatalf("late offer = %q, %d, %v", late, token, derr)
			case !bad && (oerr != nil || m.Deployment.UID != getDeployment(t, client).UID):
				t.Fatalf("late exact 201 = %+v, %v; want its UID", m.Deployment, oerr)
			case bad && (!errors.Is(oerr, serving.ErrIdentityConflict) || !m.Deployment.Refused || m.Deployment.UID != ""):
				t.Fatalf("late bad 201 = %+v, %v; want a refusal", m.Deployment, oerr)
			}
			if err := rt.Ack(testRef(sent), token); !errors.Is(err, errServiceCustody) {
				t.Fatalf("ack under the old native = %v", err)
			}
			if err := rt.Ack(testRef(late), token); err != nil {
				t.Fatal(err)
			}
			if _, token, err := rt.Drain(testRef(late)); token != 0 || err != nil {
				t.Fatalf("drain after ack = %d, %v", token, err)
			}
			native, err := applyAll(t, rt, late)
			if bad {
				if !errors.Is(err, serving.ErrIdentityConflict) || w.sends("service") != 0 || w.sends("deployment") != 1 {
					t.Fatalf("apply after a late refusal = %v; creates %d+%d", err, w.sends("deployment"), w.sends("service"))
				}
				return
			}
			got, _ := decodeServiceNative(native)
			if err != nil || got.Deployment.UID != m.Deployment.UID || got.Service.UID == "" || w.sends("deployment") != 1 || w.sends("service") != 1 {
				t.Fatalf("apply after the late UID = %+v, %v", got, err)
			}
		})
	}
}

// TestServiceDrainTokensExhausted: with no token left a fresh fact is never
// offered and the exchange stays held, for good; the offer made with the
// last token is still acknowledged.
func TestServiceDrainTokensExhausted(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntimeJoin(t, client, w, func(err error) {
		if !errors.Is(err, errServiceFactsUnacknowledged) {
			t.Errorf("join = %v, want the exhausted exchange held", err)
		}
	})
	native := mustApply(t, rt)
	ref := testRef(native)
	hw := &heldWriter{modelWriter: w, hold: true}
	t.Cleanup(hw.release) // before any Fatal, and before the runtime's join
	rt.writer = hw
	if _, err := rt.Delete(context.Background(), ref); !errors.Is(err, errServiceAnswerPending) {
		t.Fatal(err)
	}
	rt.mu.Lock()
	rt.token = ^uint64(0) - 1
	rt.mu.Unlock()
	hw.answer()
	_, last, _ := rt.Drain(ref)
	if last != ^uint64(0) {
		t.Fatalf("token %d, want the last", last)
	}
	if err := rt.Ack(ref, last); err != nil {
		t.Fatal(err)
	}
	hw.x.body.Close()
	for range 2 {
		if _, token, err := rt.Drain(ref); token != 0 || !errors.Is(err, errServiceTokensExhausted) {
			t.Fatalf("drain with no token left = %d, %v", token, err)
		}
	}
	if _, err := rt.Delete(context.Background(), ref); err == nil || w.deleted("service") != 0 {
		t.Fatalf("delete with the exchange held = %v; %d service deletes", err, w.deleted("service"))
	}
}

func TestServiceForeignNameRefused(t *testing.T) {
	client := fake.NewSimpleClientset(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: serviceObjectName(testServiceID), Namespace: "workloads", UID: "foreign",
		Labels: map[string]string{serviceLabel: testServiceID, serviceNonceLabel: testServiceNonce},
	}})
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	native, err := applyAll(t, rt, "")
	if !errors.Is(err, serving.ErrIdentityConflict) {
		t.Fatalf("apply over a foreign name = %v, want identity conflict", err)
	}
	// Matching labels are not ownership: the foreign object is never
	// recorded or deleted.
	n, _ := decodeServiceNative(native)
	if n.Deployment.UID != "" || !n.Deployment.Refused || n.Deployment.Refusal.Status != http.StatusConflict {
		t.Fatalf("foreign object adopted: %+v", n.Deployment)
	}
	if _, err := rt.Delete(context.Background(), testRef(native)); err != nil {
		t.Fatal(err)
	}
	if w.deleted("deployment") != 0 {
		t.Fatal("deleted a foreign deployment")
	}
	if getDeployment(t, client).UID != "foreign" {
		t.Fatal("foreign deployment changed")
	}
}

// TestServiceWitnessNegatives changes one field of an accepted object
// (MODELED: a namespace writer between calls) and requires each to be
// refused as not the rendering, by observation and by forward.
func TestServiceWitnessNegatives(t *testing.T) {
	gpu := resource.MustParse("1")
	two := int32(2)
	for name, change := range map[string]func(d *appsv1.Deployment, s *corev1.Service){
		"host network": func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Template.Spec.HostNetwork = true },
		"volume": func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "x"}}
		},
		"service account": func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Template.Spec.ServiceAccountName = "other" },
		"env from": func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "s"}}}}
		},
		"ephemeral container": func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "e", Image: "busybox"}}}
		},
		"extra resource": func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": gpu}
		},
		"resource claim": func(d *appsv1.Deployment, _ *corev1.Service) {
			d.Spec.Template.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "c"}}
		},
		"replicas":     func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Replicas = &two },
		"selector":     func(d *appsv1.Deployment, _ *corev1.Service) { d.Spec.Selector.MatchLabels["x"] = "y" },
		"annotation":   func(d *appsv1.Deployment, _ *corev1.Service) { d.Annotations["x"] = "y" },
		"target port":  func(_ *appsv1.Deployment, s *corev1.Service) { s.Spec.Ports[0].TargetPort = intstr.FromInt32(9090) },
		"service type": func(_ *appsv1.Deployment, s *corev1.Service) { s.Spec.Type = corev1.ServiceTypeNodePort },
		"service selector": func(_ *appsv1.Deployment, s *corev1.Service) {
			s.Spec.Selector = map[string]string{serviceLabel: testServiceID}
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			rt := newTestServiceRuntime(t, client, newModelWriter(client))
			native := mustApply(t, rt)
			d, s := getDeployment(t, client), getService(t, client)
			change(d, s)
			ctx := context.Background()
			if _, err := client.AppsV1().Deployments("workloads").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().Services("workloads").Update(ctx, s, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if o, err := observe(t, rt, native); err != nil || o.Reason != serving.IdentityConflict {
				t.Fatalf("observe = %+v, %v; want identity conflict", o, err)
			}
			n, _ := decodeServiceNative(native)
			if _, err := rt.readyPod(ctx, testRef(native), n); err == nil {
				t.Fatal("forward target accepted a changed object")
			}
		})
	}
}

// TestServiceWitnessNormalizations: the documented server additions keep
// an object its rendering.
func TestServiceWitnessNormalizations(t *testing.T) {
	client := fake.NewSimpleClientset()
	rt := newTestServiceRuntime(t, client, newModelWriter(client))
	native := mustApply(t, rt)
	d, s := getDeployment(t, client), getService(t, client)
	d.Annotations[deploymentRevision] = "1"
	d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("256Mi")
	d.Spec.Template.Spec.Containers[0].ResizePolicy = defaultResizePolicy
	s.Spec.ClusterIP, s.Spec.ClusterIPs = "10.0.0.1", []string{"10.0.0.1"}
	s.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol}
	ctx := context.Background()
	if _, err := client.AppsV1().Deployments("workloads").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Services("workloads").Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if o, err := observe(t, rt, native); err != nil || o.Reason == serving.IdentityConflict || o.Absent {
		t.Fatalf("observe = %+v, %v; want present, not conflict", o, err)
	}
}

// TestServiceQuantityIdentity: a resource quantity is its exact value. A
// stored spelling that Quantity.Cmp finds equal to the rendering's keeps the
// object its rendering; any other value, however close, a value that does
// not fit an int64, or an added resource name is a conflict.
func TestServiceQuantityIdentity(t *testing.T) {
	for _, tt := range []struct {
		name     string
		memory   string // the stored memory request; "" keeps it
		cpu      string // the stored cpu request; "" keeps it
		extra    corev1.ResourceName
		conflict bool
	}{
		{name: "binary suffix", memory: "256Mi"},
		{name: "plain bytes", memory: "268435456"},
		{name: "milli bytes", memory: "268435456000m"},
		{name: "exponent", memory: "0.268435456e9"},
		{name: "fractional gibi", memory: "0.25Gi"},
		{name: "cpu decimal", cpu: "0.25"},
		{name: "cpu nano", cpu: "250000000n"},
		{name: "fraction below", memory: "268435455.9999", conflict: true},
		{name: "one milli above", memory: "268435456001m", conflict: true},
		{name: "one nano above", memory: "268435456000000001n", conflict: true},
		{name: "cpu one nano above", cpu: "250000001n", conflict: true},
		{name: "beyond int64", memory: "9223372036854775808000", conflict: true},
		{name: "zero", memory: "0", conflict: true},
		{name: "added class", extra: "example.com/accelerator", conflict: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			rt := newTestServiceRuntime(t, client, newModelWriter(client))
			native := mustApply(t, rt)
			d := getDeployment(t, client)
			r := &d.Spec.Template.Spec.Containers[0].Resources
			if tt.memory != "" {
				r.Requests[corev1.ResourceMemory] = resource.MustParse(tt.memory)
			}
			if tt.cpu != "" {
				r.Requests[corev1.ResourceCPU] = resource.MustParse(tt.cpu)
			}
			if tt.extra != "" {
				r.Requests[tt.extra] = resource.MustParse("1")
			}
			if _, err := client.AppsV1().Deployments("workloads").Update(context.Background(), d, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			o, err := observe(t, rt, native)
			if err != nil || (o.Reason == serving.IdentityConflict) != tt.conflict || o.Absent {
				t.Fatalf("observe = %+v, %v; want conflict %v", o, err, tt.conflict)
			}
		})
	}
}

// TestExactQuantity checks the projection's spelling directly, and that
// pod-level and overhead quantities are canonical too.
func TestExactQuantity(t *testing.T) {
	for _, tt := range []struct {
		a, b  string
		equal bool
	}{
		{"256Mi", "268435456", true},
		{"1", "1000m", true},
		{"1k", "1000", true},
		{"1Ki", "1024", true},
		{"100m", "0.1", true},
		{"0", "0Mi", true},
		{"1", "1000000001n", false},
		{"268435455.9999", "268435456", false},
		{"9223372036854775807", "9223372036854775808", false},
		{"1e30", "1e30", true},
	} {
		a, b := resource.MustParse(tt.a), resource.MustParse(tt.b)
		qa, qb := exactQuantity(a), exactQuantity(b)
		got := qa.String() == qb.String()
		if got != tt.equal || got != (a.Cmp(b) == 0) {
			t.Errorf("%s vs %s: spelling equal %v (%s, %s), Cmp %d; want %v", tt.a, tt.b, got, qa.String(), qb.String(), a.Cmp(b), tt.equal)
		}
	}
	spec := func(q string) corev1.PodSpec {
		l := corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(q)}
		return corev1.PodSpec{Resources: &corev1.ResourceRequirements{Requests: l}, Overhead: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(q)}}
	}
	same := templateIdentity("workloads", corev1.PodTemplateSpec{Spec: spec("1Ki")}).witness()
	if templateIdentity("workloads", corev1.PodTemplateSpec{Spec: spec("1024")}).witness() != same {
		t.Error("pod-level spelling changed the witness")
	}
	if templateIdentity("workloads", corev1.PodTemplateSpec{Spec: spec("1025")}).witness() == same {
		t.Error("pod-level value change kept the witness")
	}
	limits := func(cpu, memory string) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)}}}}}}
	}
	limited := templateIdentity("workloads", limits("500m", "512Mi")).witness()
	if templateIdentity("workloads", limits("0.5", "536870912")).witness() != limited {
		t.Error("container limit spelling changed the witness")
	}
	if templateIdentity("workloads", limits("500m", "536870913")).witness() == limited {
		t.Error("container memory limit one byte above kept the witness")
	}
	if templateIdentity("workloads", limits("500000001n", "512Mi")).witness() == limited {
		t.Error("container cpu limit one nano above kept the witness")
	}
	if templateIdentity("workloads", corev1.PodTemplateSpec{Spec: corev1.PodSpec{}}).witness() == same {
		t.Error("added pod-level resources kept the witness")
	}
}

// readyFixture adds a ReplicaSet of the accepted Deployment, a ready Pod of
// its template as the scheduler and admission leave it, and an EndpointSlice
// of the accepted Service listing it, then marks the rollout complete. Each
// change, if set, is applied before the objects are stored.
type readyFixture struct {
	rs    *appsv1.ReplicaSet
	pod   *corev1.Pod
	slice *discoveryv1.EndpointSlice
}

func addReady(t *testing.T, client *fake.Clientset, change func(*readyFixture)) {
	t.Helper()
	addReadyAt(t, client, testServiceID, change)
}

// addReadyAt is addReady for the objects of service id.
func addReadyAt(t *testing.T, client *fake.Clientset, id string, change func(*readyFixture)) {
	t.Helper()
	ctx := context.Background()
	d, s := getDeploymentOf(t, client, id), getServiceOf(t, client, id)
	yes, no := true, false
	seconds := int64(300)
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	labels := map[string]string{}
	for k, v := range d.Spec.Template.Labels {
		labels[k] = v
	}
	labels[appsv1.DefaultDeploymentUniqueLabelKey] = "abc123"
	spec := *d.Spec.Template.Spec.DeepCopy()
	spec.NodeName = "node-1"
	spec.Priority, spec.PreemptionPolicy = &priority, &preempt
	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		spec.Tolerations = append(spec.Tolerations, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds})
	}
	f := readyFixture{
		rs: &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{
			Name: d.Name + "-abc123", Namespace: "workloads", UID: "rs-1", Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: &yes}},
		}},
		pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: d.Name + "-abc123-x", Namespace: "workloads", UID: "pod-1", Labels: labels,
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: d.Name + "-abc123", UID: "rs-1", Controller: &yes}},
			},
			Spec: spec,
			Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			}},
		},
		slice: &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name: s.Name + "-x", Namespace: "workloads", Labels: map[string]string{discoveryv1.LabelServiceName: s.Name},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: s.Name, UID: s.UID, Controller: &yes}},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Ports:       []discoveryv1.EndpointPort{{Name: ptr("http"), Port: ptr(int32(8080)), Protocol: ptr(corev1.ProtocolTCP)}},
			Endpoints: []discoveryv1.Endpoint{{
				Addresses:  []string{"10.1.0.5"},
				Conditions: discoveryv1.EndpointConditions{Ready: &yes, Terminating: &no},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: d.Name + "-abc123-x", Namespace: "workloads", UID: "pod-1"},
			}},
		},
	}
	if change != nil {
		change(&f)
	}
	if _, err := client.AppsV1().ReplicaSets(f.rs.Namespace).Create(ctx, f.rs, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Pods(f.pod.Namespace).Create(ctx, f.pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.DiscoveryV1().EndpointSlices(f.slice.Namespace).Create(ctx, f.slice, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: 1, UpdatedReplicas: 1, ReadyReplicas: 1, AvailableReplicas: 1}
	if _, err := client.AppsV1().Deployments("workloads").Update(ctx, d, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestServiceReadiness(t *testing.T) {
	for _, tt := range []struct {
		name      string
		change    func(*readyFixture)
		ready     bool // ReadyGeneration 1
		endpoints int32
		conflict  bool
	}{
		{name: "ready, scheduled and admitted", ready: true, endpoints: 1},
		{name: "unscheduled node name", change: func(f *readyFixture) { f.pod.Status.Conditions[0].Status = corev1.ConditionFalse }, conflict: true},
		{name: "pod host network", change: func(f *readyFixture) { f.pod.Spec.HostNetwork = true }, conflict: true},
		{name: "pod extra resource", change: func(f *readyFixture) {
			f.pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")}
		}, conflict: true},
		{name: "pod other owner", change: func(f *readyFixture) { f.rs.OwnerReferences[0].UID = "other" }},
		{name: "pod other namespace", change: func(f *readyFixture) { f.rs.Namespace, f.pod.Namespace = "other", "other" }},
		{name: "slice wrong port", change: func(f *readyFixture) { f.slice.Ports[0].Port = ptr(int32(9090)) }, ready: true},
		{name: "slice wrong port name", change: func(f *readyFixture) { f.slice.Ports[0].Name = ptr("metrics") }, ready: true},
		{name: "slice wrong owner", change: func(f *readyFixture) { f.slice.OwnerReferences[0].UID = "other" }, ready: true},
		{name: "endpoint other namespace", change: func(f *readyFixture) { f.slice.Endpoints[0].TargetRef.Namespace = "other" }, ready: true},
		{name: "endpoint terminating", change: func(f *readyFixture) { f.slice.Endpoints[0].Conditions.Terminating = ptr(true) }, ready: true},
		{name: "endpoint not a pod", change: func(f *readyFixture) { f.slice.Endpoints[0].TargetRef.Kind = "Node" }, ready: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			rt := newTestServiceRuntime(t, client, newModelWriter(client))
			native := mustApply(t, rt)
			addReady(t, client, tt.change)
			o, err := observe(t, rt, native)
			if err != nil {
				t.Fatal(err)
			}
			if tt.conflict {
				if o.Reason != serving.IdentityConflict {
					t.Fatalf("observe = %+v, want identity conflict", o)
				}
				return
			}
			if (o.ReadyGeneration == 1) != tt.ready || o.ReadyEndpoints != tt.endpoints {
				t.Fatalf("observe = %+v, want ready %v with %d endpoints", o, tt.ready, tt.endpoints)
			}
			n, _ := decodeServiceNative(native)
			pod, err := rt.readyPod(context.Background(), testRef(native), n)
			if (err == nil) != (tt.endpoints == 1) {
				t.Fatalf("ready pod = %q, %v", pod, err)
			}
		})
	}
}

func TestServiceDeleteKeepsBorrowed(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "ns-uid"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "workloads", UID: "sa-uid"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry", Namespace: "workloads"}, Type: corev1.SecretTypeDockerConfigJson},
	)
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	if err := rt.verifyBorrowed(context.Background()); err != nil {
		t.Fatal(err)
	}
	native := mustApply(t, rt)
	o, err := rt.Delete(context.Background(), testRef(native))
	if err != nil || !o.Absent {
		t.Fatalf("delete = %+v, %v; want absent", o, err)
	}
	ctx := context.Background()
	if _, err := client.CoreV1().Namespaces().Get(ctx, "workloads", metav1.GetOptions{}); err != nil {
		t.Error("namespace removed")
	}
	if _, err := client.CoreV1().ServiceAccounts("workloads").Get(ctx, "default", metav1.GetOptions{}); err != nil {
		t.Error("service account removed")
	}
	if _, err := client.CoreV1().Secrets("workloads").Get(ctx, "registry", metav1.GetOptions{}); err != nil {
		t.Error("pull secret removed")
	}
}

// TestServiceDeleteRetriesSamePrecondition: after a lost delete answer
// (MODELED), a later Delete sends the same UID-preconditioned delete, and
// only a fresh observation reports absence.
func TestServiceDeleteRetriesSamePrecondition(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	native := mustApply(t, rt)
	n, _ := decodeServiceNative(native)
	w.lose = true
	if o, err := rt.Delete(context.Background(), testRef(native)); err == nil || o.Absent {
		t.Fatalf("delete with a lost answer = %+v, %v; want an error", o, err)
	}
	o, err := rt.Delete(context.Background(), testRef(native))
	if err != nil || !o.Absent {
		t.Fatalf("retried delete = %+v, %v; want absent", o, err)
	}
	if w.deleted("deployment") != 2 || w.uids["deployment"] != n.Deployment.UID || w.uids["service"] != n.Service.UID {
		t.Fatalf("deletes %v with preconditions %v", w.deletes, w.uids)
	}
}

// TestServiceDeleteNeverRemovesReplacement: an object that replaced a
// recorded one at its name (MODELED) is never deleted, and the service is
// an identity conflict, not absent.
func TestServiceDeleteNeverRemovesReplacement(t *testing.T) {
	client := fake.NewSimpleClientset()
	w := newModelWriter(client)
	rt := newTestServiceRuntime(t, client, w)
	native := mustApply(t, rt)
	ctx := context.Background()
	d := getDeployment(t, client)
	if err := client.AppsV1().Deployments("workloads").Delete(ctx, d.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	d.UID, d.ResourceVersion = "replacement", ""
	if _, err := client.AppsV1().Deployments("workloads").Create(ctx, d, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	o, err := rt.Delete(ctx, testRef(native))
	if err != nil || o.Absent || o.Reason != serving.IdentityConflict {
		t.Fatalf("delete = %+v, %v; want identity conflict", o, err)
	}
	if getDeployment(t, client).UID != "replacement" {
		t.Fatal("replacement deleted")
	}
}

func TestServiceVerifyBorrowedRefusesReplaced(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "other"}},
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "workloads", UID: "sa-uid"}},
	)
	rt := newTestServiceRuntime(t, client, newModelWriter(client))
	if err := rt.verifyBorrowed(context.Background()); err == nil || strings.Contains(err.Error(), "workloads") {
		t.Fatalf("verify = %v, want a nameless identity mismatch", err)
	}
}

func TestServiceCheckAndAdmit(t *testing.T) {
	client := fake.NewSimpleClientset()
	rt := newTestServiceRuntime(t, client, newModelWriter(client))
	s := testRevision(t).Spec
	if err := rt.Check(s); err != nil {
		t.Fatal(err)
	}
	big := s
	big.Resources.CPUMillis = 4000
	if err := rt.Check(big); !errors.Is(err, serving.ErrUnsupported) {
		t.Fatalf("over ceiling: %v", err)
	}
	if err := rt.Admit("alice", s); err != nil {
		t.Fatal(err)
	}
	if err := rt.Admit("bob", s); !errors.Is(err, auth.ErrPermissionDenied) {
		t.Fatalf("ungranted principal: %v", err)
	}
}

// TestServiceForwardHeldUntilFrameSettles: Forward sends only
// ForwardHeader's headers and returns only after the frame's last original
// owner returned, even after the handler wrote and the caller canceled.
func TestServiceForwardHeldUntilFrameSettles(t *testing.T) {
	client := fake.NewSimpleClientset()
	rt := newTestServiceRuntime(t, client, newModelWriter(client))
	native := mustApply(t, rt)
	addReady(t, client, nil)
	p := &portFrame{envFrame: newEnvFrame()}
	p.enter() // an original owner that has not returned
	var got http.Header
	var target portTarget
	served := make(chan struct{})
	rt.serve = func(w http.ResponseWriter, r *http.Request, pt portTarget) *portFrame {
		got, target = r.Header.Clone(), pt
		w.WriteHeader(http.StatusNoContent)
		close(served)
		return p
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	r.Header.Set("Authorization", "Bearer operator")
	r.Header.Set("Cookie", "s=1")
	r.Header.Set("Impersonate-User", "admin")
	r.Header.Set("Impersonate-Extra-Scopes", "all")
	r.Header.Set("X-Skygo-Session", "proof")
	r.Header.Set("Skygo-Audit", "a")
	r.Header.Set("Accept", "text/plain")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Forward(ctx, testRef(native), httptest.NewRecorder(), r) }()
	select {
	case <-served:
	case err := <-done:
		t.Fatalf("forward returned before serving: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("forward returned before its frame settled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	p.leave(nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("forward did not return after its frame settled")
	}
	for _, h := range []string{"Authorization", "Cookie", "Impersonate-User", "Impersonate-Extra-Scopes", "X-Skygo-Session", "Skygo-Audit"} {
		if got.Get(h) != "" {
			t.Errorf("forwarded %s", h)
		}
	}
	if got.Get("Accept") != "text/plain" || target.Pod != "skygo-svc-0123456789abcdef0123-abc123-x" || target.Port != 8080 {
		t.Errorf("forwarded %v to %+v", got, target)
	}
}

func TestDecodeServiceNative(t *testing.T) {
	hex64 := strings.Repeat("a", 64)
	good := serviceNative{Version: serviceNativeV1, Port: 8080, Generation: 1,
		Deployment: serviceObject{Declaration: "d", Spec: hex64, Pod: hex64, Sent: true, UID: "u"}}
	refused := serviceNative{Version: serviceNativeV1, Port: 8080, Generation: 1,
		Deployment: serviceObject{Declaration: "d", Spec: hex64, Pod: hex64, Sent: true, Refused: true,
			Refusal: &serviceRefusal{Status: 201, Size: 10, SHA256: hex64, Decoded: true}}}
	for _, n := range []serviceNative{good, refused} {
		if _, err := decodeServiceNative(n.encode()); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []serving.Native{
		`{"v":2}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"uid":"u"},"service":{}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"declaration":"d","refused":true},"service":{}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"declaration":"d"},"service":{}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"declaration":"d","spec":"` + serving.Native(hex64) + `"},"service":{}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"declaration":"d","spec":"` + serving.Native(hex64) + `","pod":"` + serving.Native(hex64) + `","sent":true,"refused":true},"service":{}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{},"service":{"declaration":"d","spec":"` + serving.Native(hex64) + `","pod":"` + serving.Native(hex64) + `"}}`,
		`{"v":1,"port":0,"generation":0,"deployment":{"spec":"` + serving.Native(hex64) + `"},"service":{}}`,
		`{"v":1,"extra":1}`,
		good.encode() + " ",
	} {
		if _, err := decodeServiceNative(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
}
