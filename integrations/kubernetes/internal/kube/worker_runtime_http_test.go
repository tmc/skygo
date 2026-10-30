package kube

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// workerAPI MODELS the API server's create and get of worker Pods and
// Services over HTTP. newWorkerHTTPRuntime builds its target by fromREST, so
// every request runs through the production client: client-go's REST
// request, retry and decoding code and the target's transport. Modelled:
// server-assigned UIDs and resourceVersions, a Service cluster IP,
// AlreadyExists for a Create at a stored name, NotFound, and the task
// container's log of a Pod as plain text or a failure status. A fault
// applies to the next POST of its resource only. It is not a capture of any
// server version.
type workerAPI struct {
	t         *testing.T
	mu        sync.Mutex
	uid       int
	objs      map[string]runtime.Object // by resource/name
	faults    map[string]workerFault    // by resource
	logs      map[string]string         // task log by Pod name
	logStatus map[string]int            // if set, the log response status by Pod name
	onLog     func(name string)         // if set, runs under mu before a log response
	replies   map[string]*workerReply   // explicit HTTP fault, independent of producer
	calls     []string                  // "METHOD resource[/name[/log]]"
}

// workerFault is the outcome of one POST: apply stores the object first, as
// the server would before its response is lost or replaced. A zero status
// drops the connection without a response.
type workerFault struct {
	apply      bool
	status     int
	retryAfter bool // the response carries Retry-After: 0
}

// workerReply replaces one MODELED API response, optionally after applying the
// requested effect. n selects an explicit request; zero status loses its reply.
type workerReply struct {
	n      int
	apply  bool
	status int
	raw    string
}

func (a *workerAPI) reply(key string, r workerReply) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.replies == nil {
		a.replies = make(map[string]*workerReply)
	}
	a.replies[key] = &r
}

// workerPosts counts the POSTs of each resource.
type workerPosts struct{ pods, services int }

// workerHTTPToken is the fixture target's bearer token. It is not a secret.
const workerHTTPToken = "worker-test-token"

// workerHTTPJSON is the content type newWorkerHTTPRuntime configures. It is
// an oracle control, not a production requirement: the generated typed
// clients prefer protobuf when the content type is unset, and workerAPI
// decodes only JSON. With JSON configured, workerPost and a typed Create (the
// F3 code that mutants M28 and M29 restore) send the same wire format, so
// both reach the same fault and the POST count is the witness. Production
// sends the REST client's configured type; TestWorkerHTTPCreateDefault
// covers the unset default.
const workerHTTPJSON = "application/json"

// newWorkerHTTPRuntime returns a worker runtime whose target is built by
// fromREST against a workerAPI, with rest.Config.ContentType contentType.
func newWorkerHTTPRuntime(t *testing.T, contentType string) (*workerRuntime, *workerAPI) {
	t.Helper()
	return newWorkerHTTPRuntimeWrap(t, contentType, nil)
}

// newWorkerHTTPRuntimeWrap injects a private test fault below the worker frame
// transport before any call. Production configurations cannot supply a wrapper.
func newWorkerHTTPRuntimeWrap(t *testing.T, contentType string, wrap func(http.RoundTripper) http.RoundTripper) (*workerRuntime, *workerAPI) {
	t.Helper()
	return newWorkerHTTPRuntimeIn(t, contentType, wrap, "workloads")
}

func newWorkerHTTPRuntimeIn(t *testing.T, contentType string, wrap func(http.RoundTripper) http.RoundTripper, namespace string) (*workerRuntime, *workerAPI) {
	t.Helper()
	api := &workerAPI{t: t, objs: map[string]runtime.Object{}, faults: map[string]workerFault{},
		logs: map[string]string{}, logStatus: map[string]int{}}
	_, c, config := newDataServer(t, nil, func(*dataAPI) http.Handler { return http.HandlerFunc(api.serve) })
	config.BearerToken = workerHTTPToken
	config.ContentType = contentType
	c.Namespace = namespace
	c.Resources = workerBackend().policy
	target, err := fromREST(c, "owner", config)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := newWorkerRuntime(target, workerRegistrySnapshot(), rest.CopyConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	if wrap != nil {
		rt.http.transport.base = wrap(rt.http.transport.base)
	}
	return rt, api
}

func workerHTTPGroup(t *testing.T, rt *workerRuntime, mode backend.WorkerMode) backend.WorkerGroup {
	t.Helper()
	task := backend.Task{Argv: []string{"python", "train.py"}, Image: taskImage}
	raw, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	return backend.WorkerGroup{Execution: backend.Reference{ID: workerJob, Task: task, Binding: rt.b.binding},
		AttemptID: workerAttempt, TaskDigest: sha256.Sum256(raw), Spec: backend.WorkerSpec{Count: 2, Mode: mode}}
}

func (a *workerAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	path := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var namespace, resource, name, sub string
	if len(path) >= 5 && path[0] == "api" && path[1] == "v1" && path[2] == "namespaces" {
		namespace, resource = path[3], path[4]
		if len(path) > 5 {
			name = path[5]
		}
		if len(path) > 6 {
			sub = path[6]
		}
	} else if len(path) >= 6 && path[0] == "apis" && path[1] == "discovery.k8s.io" && path[2] == "v1" && path[3] == "namespaces" {
		namespace, resource = path[4], path[5]
	}
	entry := resource
	if name != "" {
		entry += "/" + name
	}
	if sub != "" {
		entry += "/" + sub
	}
	a.calls = append(a.calls, r.Method+" "+entry)
	if namespace == "" || (resource != "pods" && resource != "services" && resource != "endpointslices") {
		workerHTTPStatus(w, http.StatusNotFound)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+workerHTTPToken {
		a.t.Errorf("request missing target credentials")
		workerHTTPStatus(w, http.StatusUnauthorized)
		return
	}
	if fault := a.replies[r.Method+" "+entry]; fault != nil {
		fault.n--
		if fault.n == 0 {
			delete(a.replies, r.Method+" "+entry)
			emit := func() {
				if fault.status == 0 {
					c, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						a.t.Error(err)
					} else {
						c.Close()
					}
					return
				}
				if fault.raw != "" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(fault.status)
					io.WriteString(w, fault.raw)
					return
				}
				workerHTTPStatus(w, fault.status)
			}
			if !fault.apply {
				emit()
				return
			}
			original := w
			w = httptest.NewRecorder()
			defer func() { w = original; emit() }()
		}
	}
	switch {
	case r.Method == http.MethodPost && name == "":
		a.create(w, r, resource)
	case r.Method == http.MethodGet && sub == "log":
		a.log(w, r, name)
	case r.Method == http.MethodGet && resource == "endpointslices":
		list := &discoveryv1.EndpointSliceList{TypeMeta: metav1.TypeMeta{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSliceList"}}
		key, value, _ := strings.Cut(r.URL.Query().Get("labelSelector"), "=")
		for k, obj := range a.objs {
			if strings.HasPrefix(k, "endpointslices/") {
				v := obj.(*discoveryv1.EndpointSlice)
				if key == "" || v.Labels[key] == value {
					list.Items = append(list.Items, *v.DeepCopy())
				}
			}
		}
		workerHTTPWrite(w, http.StatusOK, list)
	case r.Method == http.MethodGet && name != "":
		obj, ok := a.objs[resource+"/"+name]
		if !ok {
			workerHTTPStatus(w, http.StatusNotFound)
			return
		}
		workerHTTPWrite(w, http.StatusOK, obj)
	case r.Method == http.MethodPut && name != "":
		a.update(w, r, resource, name)
	case r.Method == http.MethodDelete && name != "":
		a.delete(w, r, resource, name)
	default:
		workerHTTPStatus(w, http.StatusMethodNotAllowed)
	}
}

// update/delete MODEL API-server UID+resourceVersion conflicts and finalizers.
// Scheduler, container execution and endpoint-controller changes remain explicit.
func (a *workerAPI) update(w http.ResponseWriter, r *http.Request, resource, name string) {
	cur := a.objs[resource+"/"+name]
	if cur == nil {
		workerHTTPStatus(w, 404)
		return
	}
	var obj runtime.Object = new(corev1.Pod)
	if resource == "services" {
		obj = new(corev1.Service)
	}
	if json.NewDecoder(r.Body).Decode(obj) != nil {
		workerHTTPStatus(w, 400)
		return
	}
	got, _ := meta.Accessor(obj)
	old, _ := meta.Accessor(cur)
	if got.GetUID() != old.GetUID() || got.GetResourceVersion() != old.GetResourceVersion() {
		workerHTTPStatus(w, 409)
		return
	}
	a.uid++
	got.SetResourceVersion(strconv.Itoa(100 + a.uid))
	if got.GetDeletionTimestamp() != nil && len(got.GetFinalizers()) == 0 {
		delete(a.objs, resource+"/"+name)
	} else {
		a.objs[resource+"/"+name] = obj.DeepCopyObject()
	}
	workerHTTPWrite(w, 200, obj)
}
func (a *workerAPI) delete(w http.ResponseWriter, r *http.Request, resource, name string) {
	cur := a.objs[resource+"/"+name]
	if cur == nil {
		workerHTTPStatus(w, 404)
		return
	}
	old, _ := meta.Accessor(cur)
	var opts metav1.DeleteOptions
	if json.NewDecoder(r.Body).Decode(&opts) != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil || opts.Preconditions.ResourceVersion == nil || *opts.Preconditions.UID != old.GetUID() || *opts.Preconditions.ResourceVersion != old.GetResourceVersion() {
		workerHTTPStatus(w, 409)
		return
	}
	if len(old.GetFinalizers()) == 0 {
		delete(a.objs, resource+"/"+name)
	} else {
		now := metav1.Now()
		old.SetDeletionTimestamp(&now)
		a.uid++
		old.SetResourceVersion(strconv.Itoa(100 + a.uid))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200, Details: &metav1.StatusDetails{Name: name, UID: old.GetUID()}})
}

func (a *workerAPI) create(w http.ResponseWriter, r *http.Request, resource string) {
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		a.t.Errorf("POST %s content type %q", resource, ct)
		workerHTTPStatus(w, http.StatusUnsupportedMediaType)
		return
	}
	var obj interface {
		runtime.Object
		metav1.Object
	} = &corev1.Pod{}
	if resource == "services" {
		obj = &corev1.Service{}
	}
	if err := json.NewDecoder(r.Body).Decode(obj); err != nil || obj.GetNamespace() == "" || obj.GetName() == "" {
		a.t.Errorf("POST %s body %v, namespace %q", resource, err, obj.GetNamespace())
		workerHTTPStatus(w, http.StatusBadRequest)
		return
	}
	f, ok := a.faults[resource]
	delete(a.faults, resource)
	if !ok {
		f = workerFault{apply: true, status: http.StatusCreated}
	}
	key := resource + "/" + obj.GetName()
	if f.apply {
		if _, ok := a.objs[key]; ok {
			workerHTTPStatus(w, http.StatusConflict)
			return
		}
		a.uid++
		obj.SetUID(types.UID(fmt.Sprintf("%08x-0000-4000-8000-%012x", a.uid, a.uid)))
		obj.SetResourceVersion(strconv.Itoa(100 + a.uid))
		if s, ok := obj.(*corev1.Service); ok {
			s.Spec.ClusterIP, s.Spec.ClusterIPs = "10.43.0.10", []string{"10.43.0.10"}
		}
		a.objs[key] = obj
	}
	switch f.status {
	case 0:
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	case http.StatusCreated:
		workerHTTPWrite(w, f.status, obj)
	default:
		if f.retryAfter {
			w.Header().Set("Retry-After", "0")
		}
		workerHTTPStatus(w, f.status)
	}
}

func (a *workerAPI) log(w http.ResponseWriter, r *http.Request, name string) {
	if r.URL.Query().Get("container") != "task" {
		a.t.Errorf("GET log of %s with query %q", name, r.URL.RawQuery)
		workerHTTPStatus(w, http.StatusBadRequest)
		return
	}
	if _, ok := a.objs["pods/"+name]; !ok {
		workerHTTPStatus(w, http.StatusNotFound)
		return
	}
	if a.onLog != nil {
		a.onLog(name)
	}
	if code := a.logStatus[name]; code != 0 {
		workerHTTPStatus(w, code)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, a.logs[name])
}

func workerHTTPWrite(w http.ResponseWriter, code int, obj runtime.Object) {
	obj = obj.DeepCopyObject()
	switch o := obj.(type) {
	case *corev1.Pod:
		o.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	case *corev1.Service:
		o.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	case *discoveryv1.EndpointSliceList:
		o.TypeMeta = metav1.TypeMeta{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSliceList"}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(obj)
}

func workerHTTPStatus(w http.ResponseWriter, code int) {
	reason := map[int]metav1.StatusReason{
		http.StatusBadRequest:           metav1.StatusReasonBadRequest,
		http.StatusUnauthorized:         metav1.StatusReasonUnauthorized,
		http.StatusForbidden:            metav1.StatusReasonForbidden,
		http.StatusNotFound:             metav1.StatusReasonNotFound,
		http.StatusMethodNotAllowed:     metav1.StatusReasonMethodNotAllowed,
		http.StatusConflict:             metav1.StatusReasonAlreadyExists,
		http.StatusUnsupportedMediaType: metav1.StatusReasonUnsupportedMediaType,
		http.StatusTooManyRequests:      metav1.StatusReasonTooManyRequests,
		http.StatusServiceUnavailable:   metav1.StatusReasonServiceUnavailable,
	}[code]
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status: metav1.StatusFailure, Code: int32(code), Reason: reason})
}

// fail sets the outcome of the next POST of resource.
func (a *workerAPI) fail(resource string, f workerFault) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.faults[resource] = f
}

// put stores obj at its name, as another writer would.
func (a *workerAPI) put(resource string, obj interface {
	runtime.Object
	metav1.Object
}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.objs[resource+"/"+obj.GetName()] = obj.DeepCopyObject()
}

// editPod applies fn to the stored Pod name, as the scheduler or kubelet
// would.
func (a *workerAPI) editPod(name string, fn func(*corev1.Pod)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.objs["pods/"+name].(*corev1.Pod)
	if !ok {
		a.t.Fatalf("pod %s missing", name)
	}
	fn(p)
	a.uid++
	p.ResourceVersion = strconv.Itoa(100 + a.uid)
}

// stored returns the UID stored at resource/name, or "".
func (a *workerAPI) stored(resource, name string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	obj, ok := a.objs[resource+"/"+name]
	if !ok {
		return ""
	}
	return string(obj.(metav1.Object).GetUID())
}

func (a *workerAPI) posts() workerPosts {
	a.mu.Lock()
	defer a.mu.Unlock()
	var n workerPosts
	for _, c := range a.calls {
		switch c {
		case "POST pods":
			n.pods++
		case "POST services":
			n.services++
		}
	}
	return n
}

func (a *workerAPI) mark() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

func (a *workerAPI) since(mark int, filters ...string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := slices.Clone(a.calls[mark:])
	if len(filters) == 2 {
		prefix := strings.ToUpper(filters[0]) + " " + filters[1]
		out = slices.DeleteFunc(out, func(v string) bool { return v != prefix && !strings.HasPrefix(v, prefix+"/") })
	}
	return out
}

// TestWorkerHTTPCreate is the control: through the production client each
// Create is one POST, rank 0's Service first, and its receipt is the stored
// original.
func TestWorkerHTTPCreate(t *testing.T) {
	for _, mode := range []backend.WorkerMode{backend.WorkerIndependent, backend.WorkerTCP} {
		t.Run(string(mode), func(t *testing.T) {
			rt, api := newWorkerHTTPRuntime(t, workerHTTPJSON)
			g := workerHTTPGroup(t, rt, mode)
			var svc backend.WorkerResource
			for r := range g.Spec.Count {
				got, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: r})
				if err != nil {
					t.Fatalf("rank %d: CreateWorker: %v", r, err)
				}
				if r == 0 {
					svc = got.Service
				}
				name := workerPodName(workerAttempt, uint32(r))
				if uid := api.stored("pods", name); uid == "" || got.Pod != (backend.WorkerResource{Name: name, UID: uid}) {
					t.Fatalf("rank %d: pod receipt %+v, stored uid %q", r, got.Pod, uid)
				}
				if err := backend.ValidateWorkerRelease(backend.WorkerReference{Group: g, Rank: r, Pod: got.Pod, Service: svc}); err != nil {
					t.Fatalf("rank %d: %v", r, err)
				}
			}
			want := []string{"POST pods", "POST pods"}
			if mode == backend.WorkerTCP {
				want = slices.Insert(want, 0, "POST services")
				name := workerServiceName(workerAttempt)
				if uid := api.stored("services", name); uid == "" || svc != (backend.WorkerResource{Name: name, UID: uid}) {
					t.Fatalf("service receipt %+v, stored uid %q", svc, uid)
				}
			}
			if got := api.since(0); !slices.Equal(got, want) {
				t.Fatalf("requests %q, want %q", got, want)
			}
		})
	}
}

// TestWorkerHTTPCreateDefault: with the content type unset, as an operator's
// target normally has it, workerPost still sends one JSON POST and returns
// the stored original.
func TestWorkerHTTPCreateDefault(t *testing.T) {
	rt, api := newWorkerHTTPRuntime(t, "")
	g := workerHTTPGroup(t, rt, backend.WorkerIndependent)
	got, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
	if n := api.posts(); n != (workerPosts{pods: 1}) {
		t.Fatalf("POSTs %+v, want one pod", n)
	}
	name := workerPodName(workerAttempt, 0)
	if uid := api.stored("pods", name); err != nil || uid == "" || got.Pod != (backend.WorkerResource{Name: name, UID: uid}) {
		t.Fatalf("CreateWorker = %+v, %v; stored uid %q", got, err, uid)
	}
}

// TestWorkerHTTPCreateUncertain: a Create whose result is unknown is sent
// once. In particular an applied Create answered 503 with Retry-After, which
// client-go would otherwise resend into AlreadyExists, stays unknown rather
// than refused. Its original is found only by ResolveWorker, whose receipt
// permits cleanup only, and a Service receipt already returned is kept.
func TestWorkerHTTPCreateUncertain(t *testing.T) {
	applied503 := workerFault{apply: true, status: http.StatusServiceUnavailable, retryAfter: true}
	tests := []struct {
		name    string
		mode    backend.WorkerMode
		res     string
		fault   workerFault
		posts   workerPosts
		service bool // CreateWorker returns the Service receipt
		resolve struct{ pod, service bool }
	}{
		{"independent pod applied 503", backend.WorkerIndependent, "pods", applied503,
			workerPosts{pods: 1}, false, struct{ pod, service bool }{true, false}},
		{"tcp service applied 503", backend.WorkerTCP, "services", applied503,
			workerPosts{services: 1}, false, struct{ pod, service bool }{false, true}},
		{"tcp pod after service applied 503", backend.WorkerTCP, "pods", applied503,
			workerPosts{pods: 1, services: 1}, true, struct{ pod, service bool }{true, true}},
		{"pod applied response lost", backend.WorkerIndependent, "pods", workerFault{apply: true},
			workerPosts{pods: 1}, false, struct{ pod, service bool }{true, false}},
		{"pod throttled not applied", backend.WorkerIndependent, "pods",
			workerFault{status: http.StatusTooManyRequests, retryAfter: true},
			workerPosts{pods: 1}, false, struct{ pod, service bool }{false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, api := newWorkerHTTPRuntime(t, workerHTTPJSON)
			g := workerHTTPGroup(t, rt, tt.mode)
			api.fail(tt.res, tt.fault)
			c := backend.WorkerCreate{Group: g, Rank: 0}
			got, err := rt.CreateWorker(t.Context(), c)
			// The count comes first: a resent POST is the failure under test.
			if n := api.posts(); n != tt.posts {
				t.Fatalf("POSTs %+v, want %+v", n, tt.posts)
			}
			if err == nil || errors.Is(err, backend.ErrWorkerCreateRefused) {
				t.Fatalf("CreateWorker error %v, want an unknown result, not refused", err)
			}
			pod := backend.WorkerResource{Name: workerPodName(workerAttempt, 0)}
			svc := backend.WorkerResource{Name: workerServiceName(workerAttempt)}
			pod.UID, svc.UID = api.stored("pods", pod.Name), api.stored("services", svc.Name)
			var want backend.WorkerCreated
			if tt.service {
				want.Service = svc
			}
			if got != want {
				t.Fatalf("receipts %+v, want %+v", got, want)
			}

			originalErr := err
			mark := api.mark()
			res, err := rt.ResolveWorker(t.Context(), c)
			if errors.Is(originalErr, backend.ErrWorkerUnsettled) {
				if !errors.Is(err, backend.ErrWorkerUnsettled) || res != (backend.WorkerCreated{}) || len(api.since(mark)) != 0 {
					t.Fatalf("unsettled Create lost original custody: %+v %v %v", res, err, api.since(mark))
				}
				return
			}
			for _, call := range api.since(mark) {
				if !strings.HasPrefix(call, "GET ") {
					t.Fatalf("ResolveWorker sent %s", call)
				}
			}
			want = backend.WorkerCreated{}
			if tt.resolve.pod {
				want.Pod = pod
			}
			if tt.resolve.service {
				want.Service = svc
			}
			if want == (backend.WorkerCreated{}) {
				if !errors.Is(err, errWorkerNotFound) || res != want {
					t.Fatalf("ResolveWorker = %+v, %v; want not found", res, err)
				}
				return
			}
			if tt.resolve.pod && pod.UID == "" || tt.resolve.service && svc.UID == "" {
				t.Fatalf("original not stored: pod %q service %q", pod.UID, svc.UID)
			}
			if err != nil || res != want {
				t.Fatalf("ResolveWorker = %+v, %v; want the originals %+v", res, err, want)
			}
		})
	}
}

// TestWorkerHTTPCreateRefused is the definitive control: a 403 or a 409
// AlreadyExists is one POST and refused, the existing object is never
// adopted, and a Service receipt already returned is kept.
func TestWorkerHTTPCreateRefused(t *testing.T) {
	forbidden := workerFault{status: http.StatusForbidden}
	pod := workerPodName(workerAttempt, 0)
	svc := workerServiceName(workerAttempt)
	tests := []struct {
		name    string
		mode    backend.WorkerMode
		setup   func(*workerAPI)
		posts   workerPosts
		service bool   // CreateWorker returns the Service receipt
		foreign string // the resource of a foreign object at the name
	}{
		{"independent pod forbidden", backend.WorkerIndependent, func(a *workerAPI) { a.fail("pods", forbidden) },
			workerPosts{pods: 1}, false, ""},
		{"tcp service forbidden", backend.WorkerTCP, func(a *workerAPI) { a.fail("services", forbidden) },
			workerPosts{services: 1}, false, ""},
		{"tcp pod after service forbidden", backend.WorkerTCP, func(a *workerAPI) { a.fail("pods", forbidden) },
			workerPosts{pods: 1, services: 1}, true, ""},
		{"independent pod exists", backend.WorkerIndependent, func(a *workerAPI) {
			a.put("pods", &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: pod, Namespace: "workloads", UID: "foreign-uid"}})
		}, workerPosts{pods: 1}, false, "pods"},
		{"tcp service exists", backend.WorkerTCP, func(a *workerAPI) {
			a.put("services", &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: svc, Namespace: "workloads", UID: "foreign-uid"}})
		}, workerPosts{services: 1}, false, "services"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt, api := newWorkerHTTPRuntime(t, workerHTTPJSON)
			g := workerHTTPGroup(t, rt, tt.mode)
			tt.setup(api)
			c := backend.WorkerCreate{Group: g, Rank: 0}
			got, err := rt.CreateWorker(t.Context(), c)
			if n := api.posts(); n != tt.posts {
				t.Fatalf("POSTs %+v, want %+v", n, tt.posts)
			}
			if !errors.Is(err, backend.ErrWorkerCreateRefused) {
				t.Fatalf("CreateWorker error %v, want refused", err)
			}
			var want backend.WorkerCreated
			if tt.service {
				want.Service = backend.WorkerResource{Name: svc, UID: api.stored("services", svc)}
			}
			if got != want || tt.service && want.Service.UID == "" {
				t.Fatalf("receipts %+v, want %+v", got, want)
			}
			if tt.foreign == "" {
				return
			}
			if uid := api.stored(tt.foreign, map[string]string{"pods": pod, "services": svc}[tt.foreign]); uid != "foreign-uid" {
				t.Fatalf("foreign object replaced by %q", uid)
			}
			if res, err := rt.ResolveWorker(t.Context(), c); !errors.Is(err, backend.ErrIdentityMismatch) || res != (backend.WorkerCreated{}) {
				t.Fatalf("ResolveWorker of a foreign object = %+v, %v; want identity mismatch", res, err)
			}
		})
	}
}

// newWorkerHTTPLogs returns a runtime whose responses pass through ft, and
// the references of both members of an Independent group. Each member's Pod
// runs its task, and its log is "fake logs".
func newWorkerHTTPLogs(t *testing.T) (*workerRuntime, *workerAPI, *faultTransport, []backend.WorkerReference) {
	t.Helper()
	ft := &faultTransport{}
	rt, api := newWorkerHTTPRuntimeWrap(t, workerHTTPJSON, ft.wrap)
	g := workerHTTPGroup(t, rt, backend.WorkerIndependent)
	var refs []backend.WorkerReference
	for r := range g.Spec.Count {
		got, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: r})
		if err != nil {
			t.Fatalf("rank %d: CreateWorker: %v", r, err)
		}
		api.editPod(got.Pod.Name, func(p *corev1.Pod) { p.Spec.SchedulingGates = nil; workerPrepare(p) })
		api.mu.Lock()
		api.logs[got.Pod.Name] = "fake logs"
		api.mu.Unlock()
		refs = append(refs, backend.WorkerReference{Group: g, Rank: r, Pod: got.Pod})
	}
	return rt, api, ft, refs
}

// workerTrips returns the number of requests that reached the network
// transport: a request the data transport refuses is not counted.
func workerTrips(ft *faultTransport) int {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	return ft.calls
}

// workerLogFrame returns the log frame of ref's member and the claim count
// of its key. The frame is nil unless exactly one is retained.
func workerLogFrame(rt *workerRuntime, ref backend.WorkerReference) (*dataFrame, int) {
	return heldFrame(rt.b, workerCallKey(ref.Group, ref.Rank))
}

// workerFrameOpen reports whether f retains body as a registered body whose
// Close has not returned.
func workerFrameOpen(f *dataFrame, body *faultBody) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.open, func(d *dataBody) bool { return d.ReadCloser == io.ReadCloser(body) })
}

// workerNoChunk reports whether c is the zero chunk.
func workerNoChunk(c backend.LogChunk) bool {
	return c.Data == nil && c.NextOffset == 0 && !c.Complete && !c.Truncated
}

// checkWorkerHeld verifies that err and the member's frame show a held log
// read: the error wraps errDataBodyHeld, the chunk is empty, and the frame
// and its claim are retained with exactly the failed original bodies, in
// order, each with its Close error and closed once. Unlike checkHeld, err
// may carry a log stream's Close error.
func checkWorkerHeld(t *testing.T, rt *workerRuntime, ref backend.WorkerReference, c backend.LogChunk, err error, bodies ...*faultBody) {
	t.Helper()
	if !errors.Is(err, backend.ErrWorkerUnsettled) || !workerNoChunk(c) {
		t.Fatalf("WorkerLogs = %+v, %v; want no chunk and held", c, err)
	}
	f, n := workerLogFrame(rt, ref)
	if f == nil || !f.held || len(f.failed) != len(slices.DeleteFunc(slices.Clone(bodies), func(b *faultBody) bool { return b.closeErr == nil })) || n != 1 {
		t.Fatalf("custody = %+v, claim %d", f, n)
	}
	failed := 0
	for _, body := range bodies {
		if body.closeErr != nil {
			if d := f.failed[failed]; d.ReadCloser != io.ReadCloser(body) || d.err != body.closeErr {
				t.Fatalf("failed[%d] = %p %v, want %p %v", failed, d.ReadCloser, d.err, body, body.closeErr)
			}
			failed++
		}
		if c := body.closes.Load(); c != 1 {
			t.Fatalf("original Close calls = %d, want 1", c)
		}
	}
}

// checkWorkerRefused verifies that a WorkerLogs of ref's member is refused
// as held before any request reaches the network, leaving the frame and
// claim as they were.
func checkWorkerRefused(t *testing.T, rt *workerRuntime, ft *faultTransport, ref backend.WorkerReference) {
	t.Helper()
	f, n := workerLogFrame(rt, ref)
	trips := workerTrips(ft)
	c, err := rt.WorkerLogs(t.Context(), ref, 0, 10)
	if !errors.Is(err, backend.ErrWorkerUnsettled) || !workerNoChunk(c) {
		t.Fatalf("WorkerLogs = %+v, %v; want refused as held", c, err)
	}
	if d := workerTrips(ft) - trips; d != 0 {
		t.Fatalf("refused read sent %d requests", d)
	}
	if g, m := workerLogFrame(rt, ref); g != f || m != n {
		t.Fatalf("refusal changed custody: %p claim %d, was %p claim %d", g, m, f, n)
	}
}

// checkWorkerReleased verifies that ref's member has no log frame or claim
// and that every original body so far closed exactly once.
func checkWorkerReleased(t *testing.T, rt *workerRuntime, ft *faultTransport, ref backend.WorkerReference) {
	t.Helper()
	if f, n := workerLogFrame(rt, ref); f != nil || n != 0 {
		t.Fatalf("frame %+v, claim %d after a clean read", f, n)
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	for i, b := range ft.bodies {
		if c := b.closes.Load(); c != 1 {
			t.Fatalf("body %d: %d Close calls, want 1", i, c)
		}
	}
}

// TestWorkerHTTPLogs is the control through the production client: each
// read of a running task is three requests (the Pod, its log, the Pod
// again), closes each body once and releases its frame and claim.
func TestWorkerHTTPLogs(t *testing.T) {
	rt, api, ft, refs := newWorkerHTTPLogs(t)
	a := refs[0]
	trips := workerTrips(ft)
	c, err := rt.WorkerLogs(t.Context(), a, 0, 4)
	if err != nil || string(c.Data) != "fake" || c.NextOffset != 4 || c.Complete {
		t.Fatalf("running logs = %+v, %v", c, err)
	}
	if d := workerTrips(ft) - trips; d != 3 {
		t.Fatalf("%d requests, want 3", d)
	}
	checkWorkerReleased(t, rt, ft, a)
	api.editPod(a.Pod.Name, workerExited(0))
	c, err = rt.WorkerLogs(t.Context(), a, 4, 100)
	if err != nil || string(c.Data) != " logs" || !c.Complete {
		t.Fatalf("exited logs = %+v, %v", c, err)
	}
	checkWorkerReleased(t, rt, ft, a)
	// A replacement during the read voids the chunk.
	api.mu.Lock()
	api.onLog = func(name string) { api.objs["pods/"+name].(*corev1.Pod).UID = "replacement" }
	api.mu.Unlock()
	if c, err := rt.WorkerLogs(t.Context(), a, 0, 10); !errors.Is(err, errWorkerPodChanged) || !workerNoChunk(c) {
		t.Fatalf("replaced during read = %+v, %v", c, err)
	}
	checkWorkerReleased(t, rt, ft, a)
}

// workerJoinBound bounds the wait for an original WorkerLogs to return
// after its blocked Close is released. Reaching it is a failure, never a
// completion.
const workerJoinBound = 10 * time.Second

// TestWorkerHTTPLogsBlockedClose covers AGREEMENT-1 L1 (P-L1) and test 9:
// while the log stream's original Close blocks, WorkerLogs has not returned,
// the member's frame holds that same body, a second read of the member is
// refused before any request and leaves no state, and the other member
// reads. Canceling the read's context (AM1) changes none of that. After a
// controlled release and join, a failed Close leaves the frame held and the
// member refused; a returned Close releases it and the member reads again.
// A cleanup registered before the first possible Fatal releases the original
// Close exactly once and joins the original call, so no failure leaves it
// blocked.
func TestWorkerHTTPLogsBlockedClose(t *testing.T) {
	for _, tt := range []struct {
		name     string
		closeErr error
		cancel   bool
	}{
		{"returns", nil, false},
		{"fails", errClose, false},
		{"returns after cancel", nil, true},
		{"fails after cancel", errClose, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt, _, ft, refs := newWorkerHTTPLogs(t)
			a, b := refs[0], refs[1]
			body := &faultBody{closeErr: tt.closeErr, entered: make(chan struct{}), release: make(chan struct{})}
			ft.arm("GET", "/pods/"+a.Pod.Name+"/log", 1, body)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var res struct {
				c   backend.LogChunk
				err error
			}
			returned := make(chan struct{}) // closed after res is set
			go func() {
				defer close(returned)
				res.c, res.err = rt.WorkerLogs(ctx, a, 0, 100)
			}()
			// join releases the original Close once and waits, bounded, for
			// the original call to return. It reports whether it returned.
			var joinOnce sync.Once
			joined := false
			join := func() bool {
				joinOnce.Do(func() {
					close(body.release)
					select {
					case <-returned:
						joined = true
					case <-time.After(workerJoinBound):
					}
				})
				return joined
			}
			t.Cleanup(func() {
				if !join() {
					t.Errorf("cleanup: original WorkerLogs did not return within %v of its release", workerJoinBound)
					return
				}
				if c := body.closes.Load(); c != 1 {
					t.Errorf("cleanup: original Close calls = %d, want 1", c)
				}
			})
			select {
			case <-body.entered:
			case <-returned:
				t.Fatalf("returned before its Close blocked: %+v, %v", res.c, res.err)
			}
			blocked := func(step string) {
				t.Helper()
				select {
				case <-returned:
					t.Fatalf("%s: returned during blocked Close: %+v, %v", step, res.c, res.err)
				default:
				}
				f, n := workerLogFrame(rt, a)
				if f == nil || !workerFrameOpen(f, body) || n != 1 {
					t.Fatalf("%s: frame %+v, claim %d; want the blocked body", step, f, n)
				}
				trips := workerTrips(ft)
				chunk, err := rt.WorkerLogs(t.Context(), a, 0, 10)
				if !errors.Is(err, errDataBodyHeld) || !workerNoChunk(chunk) || workerTrips(ft) != trips {
					t.Fatalf("active original reader admission = %+v %v", chunk, err)
				}
				if current, owners := workerLogFrame(rt, a); current != f || owners != n || !workerFrameOpen(current, body) {
					t.Fatal("active-reader refusal dropped the original owner")
				}
				mark := workerTrips(ft)
				if _, err := rt.RemoveWorker(t.Context(), a); err == nil || workerTrips(ft) != mark {
					t.Fatalf("removal entered with original reader active: %v", err)
				}
			}
			blocked("blocked")
			if c, err := rt.WorkerLogs(t.Context(), b, 0, 100); err != nil || string(c.Data) != "fake logs" {
				t.Fatalf("other member beside a blocked Close = %+v, %v", c, err)
			}
			checkWorkerReleased(t, rt, ft, b)
			if tt.cancel {
				cancel()
				blocked("canceled")
			}
			if !join() {
				t.Fatalf("original WorkerLogs did not return within %v of its release", workerJoinBound)
			}
			if c := body.closes.Load(); c != 1 {
				t.Fatalf("original Close calls = %d, want 1", c)
			}
			if tt.closeErr != nil {
				if !errors.Is(res.err, errClose) {
					t.Fatalf("Close cause lost: %v", res.err)
				}
				checkWorkerHeld(t, rt, a, res.c, res.err, body)
				checkWorkerRefused(t, rt, ft, a)
				return
			}
			switch {
			case tt.cancel:
				// The post-read Pod read is never sent on a canceled context.
				if res.err == nil || errors.Is(res.err, errDataBodyHeld) || !workerNoChunk(res.c) {
					t.Fatalf("canceled read = %+v, %v", res.c, res.err)
				}
			case res.err != nil || string(res.c.Data) != "fake logs":
				t.Fatalf("released read = %+v, %v", res.c, res.err)
			}
			checkWorkerReleased(t, rt, ft, a)
			if c, err := rt.WorkerLogs(t.Context(), a, 0, 100); err != nil || string(c.Data) != "fake logs" {
				t.Fatalf("read after a clean end = %+v, %v", c, err)
			}
		})
	}
}

// TestWorkerHTTPLogsFailure covers AGREEMENT-1 L2 and the held reads: every
// cause of a failed read is in its error, no chunk is returned, the frame is
// held exactly when an original Close failed, and then the next read of the
// member is refused before any request. Each row counts the requests of the
// read and the Close calls of each original body.
func TestWorkerHTTPLogsFailure(t *testing.T) {
	errRead := errors.New("read failed")
	for _, tt := range []struct {
		name   string
		status int    // log response status, if not 200
		suffix string // "log" for the log stream, or the n-th Pod read
		n      int
		read   error // the original body's Read error
		close  error // the original body's Close error
		trips  int
		want   []error // causes in the error
		held   bool
	}{
		// L2-1: combined causes, the same owner.
		{"status with drain and close failures", http.StatusInternalServerError, "log", 1,
			errRead, errClose, 2, []error{errWorkerLogsStatus, errRead, errClose}, true},
		// L2-2: primary read failures.
		{"read failure", 0, "log", 1, errRead, nil, 2, []error{errRead}, true},
		{"read and close failures", 0, "log", 1,
			errRead, errClose, 2, []error{errRead, errClose}, true},
		// L2-4: the status alone.
		{"status", http.StatusServiceUnavailable, "log", 1, nil, nil, 2, []error{errWorkerLogsStatus}, false},
		{"close failure", 0, "log", 1, nil, errClose, 2, []error{errClose}, true},
		// The Pod reads' bodies are owned the same way; client-go discards
		// their Close errors, so only the frame records them.
		{"first pod read close failure", 0, "pod", 1, nil, errClose, 1, []error{errClose}, true},
		{"post-read pod read close failure", 0, "pod", 2, nil, errClose, 3, []error{errClose}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt, api, ft, refs := newWorkerHTTPLogs(t)
			a := refs[0]
			if tt.status != 0 {
				api.mu.Lock()
				api.logStatus[a.Pod.Name] = tt.status
				api.mu.Unlock()
			}
			body := &faultBody{readErr: tt.read, closeErr: tt.close}
			suffix := "/pods/" + a.Pod.Name
			if tt.suffix == "log" {
				suffix += "/log"
			}
			ft.arm("GET", suffix, tt.n, body)
			trips := workerTrips(ft)
			c, err := rt.WorkerLogs(t.Context(), a, 0, 100)
			if c := body.closes.Load(); c != 1 {
				t.Fatalf("original Close calls = %d, want 1", c)
			}
			if d := workerTrips(ft) - trips; d != tt.trips {
				t.Fatalf("%d requests, want %d", d, tt.trips)
			}
			for _, cause := range tt.want {
				if !errors.Is(err, cause) {
					t.Fatalf("err = %v, want cause %v", err, cause)
				}
			}
			if !tt.held {
				if err == nil || errors.Is(err, errDataBodyHeld) || !workerNoChunk(c) {
					t.Fatalf("WorkerLogs = %+v, %v; want no chunk, not held", c, err)
				}
				checkWorkerReleased(t, rt, ft, a)
				// The next read is admitted.
				api.mu.Lock()
				delete(api.logStatus, a.Pod.Name)
				api.mu.Unlock()
				if c, err := rt.WorkerLogs(t.Context(), a, 0, 100); err != nil || string(c.Data) != "fake logs" {
					t.Fatalf("next read = %+v, %v", c, err)
				}
				return
			}
			checkWorkerHeld(t, rt, a, c, err, body)
			// L2-3: the next read of the member is refused.
			checkWorkerRefused(t, rt, ft, a)
		})
	}
}

// Fixture methods used by runtime and rendezvous tests operate on this same
// HTTP server's MODELED object map; production requests never use a fake client.
type workerObjectKey struct{ resource, name string }

func (a *workerAPI) get(resource, name string) runtime.Object {
	a.mu.Lock()
	defer a.mu.Unlock()
	if o := a.objs[resource+"/"+name]; o != nil {
		return o.DeepCopyObject()
	}
	return nil
}
func (a *workerAPI) remove(resource, name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.objs, resource+"/"+name)
}
func (a *workerAPI) edit(resource, name string, fn func(runtime.Object)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	o := a.objs[resource+"/"+name]
	if o == nil {
		a.t.Fatal("missing modeled object")
	}
	fn(o)
	m, _ := meta.Accessor(o)
	a.uid++
	m.SetResourceVersion(strconv.Itoa(100 + a.uid))
}
func (a *workerAPI) keys() []workerObjectKey {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []workerObjectKey
	for k := range a.objs {
		r, n, _ := strings.Cut(k, "/")
		out = append(out, workerObjectKey{r, n})
	}
	return out
}
func (a *workerAPI) putObject(obj runtime.Object) runtime.Object {
	a.mu.Lock()
	defer a.mu.Unlock()
	o := obj.DeepCopyObject()
	m, _ := meta.Accessor(o)
	resource := "pods"
	switch o.(type) {
	case *corev1.Service:
		resource = "services"
	case *discoveryv1.EndpointSlice:
		resource = "endpointslices"
	}
	if m.GetUID() == "" {
		a.uid++
		m.SetUID(types.UID(fmt.Sprintf("%08x-0000-4000-8000-%012x", a.uid, a.uid)))
	}
	a.uid++
	m.SetResourceVersion(strconv.Itoa(100 + a.uid))
	a.objs[resource+"/"+m.GetName()] = o
	return o.DeepCopyObject()
}

// TestWorkerHTTPDeleteWitness distinguishes a clean original acknowledgement
// from a lost DELETE response, and from a later lost confirmation. The API
// explicitly models application; native retirement is not inferred from it.
func TestWorkerHTTPDeleteWitness(t *testing.T) {
	for _, tt := range []struct {
		name, method string
		n, status    int
		apply, sent  bool
		raw          string
	}{
		{name: "acknowledged then confirmation fails", method: "GET", n: 2, status: 503, sent: true},
		{name: "lost delete response", method: "DELETE", n: 1, apply: true},
		{name: "delete refused", method: "DELETE", n: 1, status: 403},
		{name: "delete unavailable", method: "DELETE", n: 1, status: 503},
		{name: "foreign typed acknowledgement", method: "DELETE", n: 1, status: 200, apply: true, raw: `{"apiVersion":"v1","kind":"Status","status":"Success","code":200,"details":{"uid":"foreign"}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt, a := newWorkerTestRuntime(t)
			g := workerTestGroup(t, rt, backend.WorkerIndependent, 2)
			ref := workerCreateAll(t, rt, g)[0]
			a.editPod(ref.Pod.Name, workerExited(0))
			mark := a.mark()
			a.reply(tt.method+" pods/"+ref.Pod.Name, workerReply{n: tt.n, status: tt.status, apply: tt.apply, raw: tt.raw})
			got, err := rt.RemoveWorker(t.Context(), ref)
			if err == nil || got.PodRemoved || got.PodDispatched != tt.sent || got.ServiceDispatched {
				t.Fatalf("Remove = %+v, %v", got, err)
			}
			if n := len(a.since(mark, "DELETE", "pods")); n != 1 {
				t.Fatalf("DELETE count %d; requests %v", n, a.since(mark))
			}
			if !tt.sent && errors.Is(err, backend.ErrWorkerUnsettled) {
				before := a.mark()
				_, again := rt.RemoveWorker(t.Context(), ref)
				if !errors.Is(again, backend.ErrWorkerUnsettled) || len(a.since(before)) != 0 {
					t.Fatalf("unsettled original not fenced: %v %v", again, a.since(before))
				}
			}
		})
	}
}

func TestWorkerHTTPCreateServiceClosePreservesIdentity(t *testing.T) {
	ft := &faultTransport{}
	rt, a := newWorkerHTTPRuntimeWrap(t, workerHTTPJSON, ft.wrap)
	g := workerHTTPGroup(t, rt, backend.WorkerTCP)
	body := &faultBody{closeErr: errClose}
	ft.arm("POST", "/services", 1, body)
	got, err := rt.CreateWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
	if got.Service.UID == "" || got.Service.UID != a.stored("services", got.Service.Name) || got.Pod != (backend.WorkerResource{}) || !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, errClose) || errors.Is(err, backend.ErrWorkerCreateRefused) {
		t.Fatalf("partial Create = %+v, %v", got, err)
	}
	if p := a.posts(); p.services != 1 || p.pods != 0 {
		t.Fatalf("partial Create requests %+v", p)
	}
	if n := body.closes.Load(); n != 1 {
		t.Fatalf("original Close count %d", n)
	}
	mark := a.mark()
	again, err := rt.ResolveWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
	if again != (backend.WorkerCreated{}) || !errors.Is(err, backend.ErrWorkerUnsettled) || len(a.since(mark)) != 0 {
		t.Fatalf("original owner lost on Resolve: %+v, %v, %v", again, err, a.since(mark))
	}
}

func TestWorkerHTTPDeleteCloseCannotDispatch(t *testing.T) {
	ft := &faultTransport{}
	rt, a := newWorkerHTTPRuntimeWrap(t, workerHTTPJSON, ft.wrap)
	g := workerHTTPGroup(t, rt, backend.WorkerIndependent)
	ref := workerCreateAll(t, rt, g)[0]
	a.editPod(ref.Pod.Name, workerExited(0))
	body := &faultBody{closeErr: errClose}
	ft.arm("DELETE", "/pods/"+ref.Pod.Name, 1, body)
	mark := a.mark()
	got, err := rt.RemoveWorker(t.Context(), ref)
	if got.PodDispatched || got.PodRemoved || !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, errClose) {
		t.Fatalf("Remove = %+v, %v", got, err)
	}
	if calls := a.since(mark); len(calls) != 2 || calls[1] != "DELETE pods/"+ref.Pod.Name {
		t.Fatalf("request after unclosed acknowledgement: %v", calls)
	}
	if n := body.closes.Load(); n != 1 {
		t.Fatalf("original Close count %d", n)
	}
}

func TestWorkerHTTPRemovalPartialKinds(t *testing.T) {
	rt, a := newWorkerTestRuntime(t)
	g := workerTestGroup(t, rt, backend.WorkerTCP, 2)
	ref := workerCreateAll(t, rt, g)[0]
	a.editPod(ref.Pod.Name, workerExited(0))
	a.reply("GET services/"+ref.Service.Name, workerReply{n: 1, status: 503})
	got, err := rt.RemoveWorker(t.Context(), ref)
	if err == nil || !got.PodRemoved || !got.PodDispatched || got.ServiceRemoved || got.ServiceDispatched {
		t.Fatalf("partial original removal = %+v, %v", got, err)
	}
	if a.stored("pods", ref.Pod.Name) != "" || a.stored("services", ref.Service.Name) != ref.Service.UID {
		t.Fatal("partial original identities changed")
	}
}

// No metadata method may erase a body owner just because client-go decoded a
// successful response before discarding its Close failure.
func TestWorkerHTTPMetadataCloseRetainsOriginal(t *testing.T) {
	for _, name := range []string{"resolve", "release", "start", "cancel", "observe barrier", "observe worker", "remove"} {
		t.Run(name, func(t *testing.T) {
			ft := &faultTransport{}
			rt, a := newWorkerHTTPRuntimeWrap(t, workerHTTPJSON, ft.wrap)
			g := workerHTTPGroup(t, rt, backend.WorkerIndependent)
			ref := workerCreateAll(t, rt, g)[0]
			a.editPod(ref.Pod.Name, func(p *corev1.Pod) { p.Spec.SchedulingGates = nil; workerPrepare(p) })
			body := &faultBody{closeErr: errClose}
			ft.arm("GET", "/pods/"+ref.Pod.Name, 1, body)
			mark := a.mark()
			var err error
			switch name {
			case "resolve":
				var got backend.WorkerCreated
				got, err = rt.ResolveWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
				if got.Pod != ref.Pod {
					t.Fatalf("decoded original resolve identity lost: %+v", got)
				}
			case "release":
				var got backend.WorkerReleased
				got, err = rt.ReleaseWorker(t.Context(), ref)
				if got.Acknowledged {
					t.Fatal("unclosed read acknowledged release")
				}
			case "start":
				var got backend.WorkerStarted
				got, err = rt.StartWorker(t.Context(), ref, workerToken(t, ref))
				if got.Acknowledged {
					t.Fatal("unclosed read acknowledged start")
				}
			case "cancel":
				var got backend.WorkerCanceled
				got, err = rt.CancelWorker(t.Context(), ref)
				if got.Acknowledged {
					t.Fatal("unclosed read acknowledged cancel")
				}
			case "observe barrier":
				var got backend.WorkerBarrier
				got, err = rt.ObserveBarrier(t.Context(), ref)
				if got.Prepared || got.Started || got.Stop != backend.WorkerStopUnproven {
					t.Fatalf("unclosed read qualified facts: %+v", got)
				}
			case "observe worker":
				var got backend.WorkerObservation
				got, err = rt.ObserveWorker(t.Context(), ref)
				if got.Status.State != "" || got.Exit != nil {
					t.Fatalf("unclosed read qualified status: %+v", got)
				}
			case "remove":
				var got backend.WorkerRemoval
				got, err = rt.RemoveWorker(t.Context(), ref)
				if got.PodRemoved || got.PodDispatched {
					t.Fatalf("unclosed read qualified retirement: %+v", got)
				}
			}
			if !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, errClose) || errors.Is(err, backend.ErrWorkerVanished) || errors.Is(err, backend.ErrWorkerCreateRefused) {
				t.Fatalf("marker/cause precedence: %v", err)
			}
			if calls := a.since(mark); len(calls) != 1 || calls[0] != "GET pods/"+ref.Pod.Name {
				t.Fatalf("unclosed read sent next request: %v", calls)
			}
			before := a.mark()
			_, again := rt.CancelWorker(t.Context(), ref)
			if !errors.Is(again, backend.ErrWorkerUnsettled) || len(a.since(before)) != 0 {
				t.Fatalf("next effect not fenced: %v %v", again, a.since(before))
			}
			if body.closes.Load() != 1 {
				t.Fatal("original Close repeated")
			}
		})
	}
}

// Each foreign response is decoded by the real SDK, including its original
// Close failure, before Resolve decides whether its UID can be a cleanup ref.
func TestWorkerHTTPResolveRejectsForeignDecodedIdentity(t *testing.T) {
	for _, resource := range []string{"pods", "services"} {
		for _, closeFail := range []bool{false, true} {
			for _, field := range []string{"name", "namespace", "task", "binding", "spec"} {
				t.Run(fmt.Sprintf("%s/%s/close=%t", resource, field, closeFail), func(t *testing.T) {
					ft := &faultTransport{}
					rt, a := newWorkerHTTPRuntimeWrap(t, workerHTTPJSON, ft.wrap)
					mode := backend.WorkerIndependent
					if resource == "services" {
						mode = backend.WorkerTCP
					}
					g := workerHTTPGroup(t, rt, mode)
					ref := workerCreateAll(t, rt, g)[0]
					saved := ref.Pod
					if resource == "services" {
						saved = ref.Service
					}
					obj := a.get(resource, saved.Name)
					m, _ := meta.Accessor(obj)
					switch v := obj.(type) {
					case *corev1.Pod:
						v.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
						if field == "spec" {
							v.Spec.Containers[0].Image = "foreign:v1"
						}
					case *corev1.Service:
						v.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
						if field == "spec" {
							v.Spec.Selector[workerAttemptLabel] = "foreign"
						}
					}
					switch field {
					case "name":
						m.SetName("foreign")
					case "namespace":
						m.SetNamespace("foreign")
					case "task":
						m.GetAnnotations()[taskAnnotation] = "foreign"
					case "binding":
						m.GetAnnotations()[bindingAnnotation] = "foreign"
					}
					raw, err := json.Marshal(obj)
					if err != nil {
						t.Fatal(err)
					}
					a.reply("GET "+resource+"/"+saved.Name, workerReply{n: 1, status: 200, raw: string(raw)})
					var body *faultBody
					if closeFail {
						body = &faultBody{closeErr: errClose}
						ft.arm("GET", "/"+resource+"/"+saved.Name, 1, body)
					}
					mark := a.mark()
					got, err := rt.ResolveWorker(t.Context(), backend.WorkerCreate{Group: g, Rank: 0})
					if got != (backend.WorkerCreated{}) || !errors.Is(err, backend.ErrIdentityMismatch) {
						t.Fatalf("foreign decoded identity = %+v, %v", got, err)
					}
					if closeFail {
						var held *workerResolvedError
						if !errors.Is(err, backend.ErrWorkerUnsettled) || !errors.Is(err, errClose) || !errors.As(err, &held) || held.object == nil || body.closes.Load() != 1 {
							t.Fatalf("foreign response lost original owner/object/cause: %v", err)
						}
					}
					if len(a.since(mark)) != 1 {
						t.Fatalf("foreign response sent next request: %v", a.since(mark))
					}
				})
			}
		}
	}
}

// A successful HTTP PUT and a resourceVersion do not alone confirm mutation of
// the saved Pod. The actual returned identity/spec and gate/token must agree.
func TestWorkerHTTPMutationAcknowledgesReturnedOriginal(t *testing.T) {
	for _, operation := range []string{"release", "start"} {
		for _, field := range []string{"name", "namespace", "uid", "task", "binding", "spec", "gate", "token", "revision"} {
			t.Run(operation+"/"+field, func(t *testing.T) {

				rt, a := newWorkerTestRuntime(t)
				g := workerTestGroup(t, rt, backend.WorkerIndependent, 2)
				ref := workerCreateAll(t, rt, g)[0]
				if operation == "start" {
					if _, err := rt.ReleaseWorker(t.Context(), ref); err != nil {
						t.Fatal(err)
					}
					a.editPod(ref.Pod.Name, workerPrepare)
				}
				returned := workerStored(t, a, ref.Pod.Name)
				returned.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
				returned.Spec.SchedulingGates = nil
				returned.ResourceVersion = "next"
				token := workerToken(t, ref)
				if operation == "start" {
					returned.Annotations[workerStartAnnotation] = fmt.Sprintf("%x", token)
				}
				switch field {
				case "name":
					returned.Name = "foreign"
				case "namespace":
					returned.Namespace = "foreign"
				case "uid":
					returned.UID = "foreign"
				case "task":
					returned.Annotations[taskAnnotation] = "foreign"
				case "binding":
					returned.Annotations[bindingAnnotation] = "foreign"
				case "spec":
					returned.Spec.Containers[0].Image = "foreign:v1"
				case "gate":
					returned.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: workerSchedulingGate}}
				case "token":
					if operation == "start" {
						delete(returned.Annotations, workerStartAnnotation)
					} else {
						returned.Annotations[workerStartAnnotation] = "foreign"
					}
				case "revision":
					returned.ResourceVersion = ""
				}
				raw, err := json.Marshal(returned)
				if err != nil {
					t.Fatal(err)
				}
				a.reply("PUT pods/"+ref.Pod.Name, workerReply{n: 1, apply: true, status: 200, raw: string(raw)})
				mark := a.mark()
				if operation == "release" {
					got, err := rt.ReleaseWorker(t.Context(), ref)
					if err == nil || got.Acknowledged || got.Revision != "" || got.Pod != ref.Pod {
						t.Fatalf("foreign release acknowledgment = %+v %v", got, err)
					}
				} else {
					got, err := rt.StartWorker(t.Context(), ref, token)
					if err == nil || got.Acknowledged || got.Revision != "" || got.Pod != ref.Pod {
						t.Fatalf("foreign start acknowledgment = %+v %v", got, err)
					}
				}
				if n := len(a.since(mark, "PUT", "pods")); n != 1 {
					t.Fatalf("mutation PUT count %d", n)
				}
			})
		}
	}
}

func TestWorkerHTTPDeleteStatusDetails(t *testing.T) {
	for _, tt := range []struct {
		name    string
		details func(backend.WorkerResource) *metav1.StatusDetails
		sent    bool
	}{
		{"omitted", func(backend.WorkerResource) *metav1.StatusDetails { return nil }, true},
		{"exact", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{Name: r.Name, UID: types.UID(r.UID), Kind: "pods"}
		}, true},
		{"empty", func(backend.WorkerResource) *metav1.StatusDetails { return &metav1.StatusDetails{} }, false},
		{"name only", func(r backend.WorkerResource) *metav1.StatusDetails { return &metav1.StatusDetails{Name: r.Name} }, false},
		{"uid only", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{UID: types.UID(r.UID)}
		}, false},
		{"foreign name", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{Name: "foreign", UID: types.UID(r.UID)}
		}, false},
		{"foreign uid", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{Name: r.Name, UID: "foreign"}
		}, false},
		{"foreign kind", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{Name: r.Name, UID: types.UID(r.UID), Kind: "services"}
		}, false},
		{"foreign group", func(r backend.WorkerResource) *metav1.StatusDetails {
			return &metav1.StatusDetails{Name: r.Name, UID: types.UID(r.UID), Group: "foreign.example"}
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rt, a := newWorkerTestRuntime(t)
			g := workerTestGroup(t, rt, backend.WorkerIndependent, 2)
			ref := workerCreateAll(t, rt, g)[0]
			a.editPod(ref.Pod.Name, workerExited(0))
			ack := metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess, Code: 200, Details: tt.details(ref.Pod)}
			raw, err := json.Marshal(ack)
			if err != nil {
				t.Fatal(err)
			}
			a.reply("DELETE pods/"+ref.Pod.Name, workerReply{n: 1, apply: true, status: 200, raw: string(raw)})
			got, err := rt.RemoveWorker(t.Context(), ref)
			if got.PodDispatched != tt.sent || got.PodRemoved != tt.sent || (err == nil) != tt.sent {
				t.Fatalf("typed Details witness = %+v, %v", got, err)
			}
		})
	}
}
