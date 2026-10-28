package kube

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// receiptAPI is a loopback API server for Jobs and Pods in namespace
// workloads. It stores what clients send and runs no controllers; Pod status
// and controller-owned Pods are MODELED by the tests.
type receiptAPI struct {
	mu       sync.Mutex
	jobs     map[string]*batchv1.Job
	pods     map[string]*corev1.Pod
	posted   []byte
	requests []string
	writes   int
	nextUID  int

	createCode   int // failure status for a create; 0 replies 201
	createReason metav1.StatusReason
	store        bool // store the object despite createCode
	drop         bool // store the object, then drop the connection
	notCreated   bool // reply 200 instead of 201
	truncate     bool // send the create status and half its body, then abort
	// admit models API server defaulting and admission: it changes a
	// created object before it is stored and returned.
	admit        func(obj any)
	deleteCode   int
	deleteReason metav1.StatusReason

	// createReply, if set, returns the create response body for a copy of
	// the stored object: an object, or []byte sent as is. replied is the
	// exact body of the last create response.
	createReply func(obj any) any
	replied     []byte
	// reply, if set, returns the body of the nth GET of one object since
	// replyWith, from a copy of the stored object, as createReply does. The
	// stored object is not changed.
	reply func(n int, obj any) any
	gets  map[string]int
	// holdStored holds a create response after the object is stored, so
	// that it is visible to reads while the response is held.
	holdStored bool

	// MODEL hold points. Cleanup releases every one, and stop, before the
	// server closes, so that a failed assertion never leaves a handler held.
	holdCreate, createEntered, createDone *gate
	holdLog, logEntered                   *gate
	// holdGet holds the response to the getN-th GET of a Job or Pod after
	// its body was taken from the stored object.
	holdGet, getEntered *gate
	getN                int
	stop                *gate
	logs                string

	fixture *receiptFixture
}

const (
	jobsPath = "/apis/batch/v1/namespaces/workloads/jobs"
	podsPath = "/api/v1/namespaces/workloads/pods"
)

// gate is a MODEL hold point, released at most once by the test or by
// cleanup.
type gate struct {
	once sync.Once
	ch   chan struct{}
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) release() {
	if g != nil {
		g.once.Do(func() { close(g.ch) })
	}
}

// wait waits within receiptJoinTimeout for g to be released.
func (g *gate) wait(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.ch:
	case <-time.After(receiptJoinTimeout):
		t.Fatalf("%s: timed out", what)
	}
}

// newReceiptAPI returns a loopback API, its URL and a private state root.
// Cleanups run in reverse: release every gate, close the server, which
// waits for its handlers, then retire the fixture once every original
// operation has returned.
func newReceiptAPI(t *testing.T) (*receiptAPI, string, string) {
	t.Helper()
	fixture := newReceiptFixture(t)
	api := &receiptAPI{jobs: make(map[string]*batchv1.Job), pods: make(map[string]*corev1.Pod), logs: "output\n",
		stop: newGate(), fixture: fixture}
	server := httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(server.Close)
	t.Cleanup(api.releaseAll)
	return api, server.URL, fixture.dir
}

// hold installs fresh create or log gates.
func (a *receiptAPI) hold(create bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if create {
		a.holdCreate, a.createEntered, a.createDone = newGate(), newGate(), newGate()
	} else {
		a.holdLog, a.logEntered = newGate(), newGate()
	}
}

// holdRead installs fresh gates holding the nth following GET of a Job or
// Pod, counted from 1.
func (a *receiptAPI) holdRead(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.holdGet, a.getEntered, a.getN = newGate(), newGate(), n
}

func (a *receiptAPI) releaseAll() {
	a.mu.Lock()
	gates := []*gate{a.holdCreate, a.createEntered, a.createDone, a.holdLog, a.logEntered, a.holdGet, a.getEntered, a.stop}
	a.mu.Unlock()
	for _, g := range gates {
		g.release()
	}
}

// newReceiptBackend constructs a Backend as FromTargetWithState does, but
// from a loopback REST configuration. Each call models a restarted process:
// a new Backend with empty volatile state over the same receipt directory.
func newReceiptBackend(t *testing.T, root, url string) *Backend {
	t.Helper()
	return newReceiptBackendConfig(t, root, &rest.Config{Host: url})
}

// newReceiptBackendConfig is newReceiptBackend from config, through the same
// frozen configuration and receipt client FromTargetWithState uses.
func newReceiptBackendConfig(t *testing.T, root string, config *rest.Config) *Backend {
	t.Helper()
	target, frozen, err := fromRESTFrozen(context.Background(), TargetConfig{Name: "test", Namespace: "workloads"}, "owner", config)
	if err != nil {
		t.Fatal(err)
	}
	target, err = withReceipts(target, frozen, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiptHTTP(target.Backend.(*Backend).client.CoreV1().RESTClient()); err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	if target.Retention == nil || !target.SupportsRetry {
		t.Fatal("receipts changed target capabilities")
	}
	return b
}

func apiStatus(w http.ResponseWriter, code int, reason metav1.StatusReason) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: reason})
}

func (a *receiptAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.Method+" "+r.URL.Path)
	if r.Method == "POST" || r.Method == "DELETE" {
		a.writes++
	}
	hold, entered, done := a.holdCreate, a.createEntered, a.createDone
	holdLog, logEntered, stop := a.holdLog, a.logEntered, a.stop
	holdStored := a.holdStored
	var heldGet, getEntered *gate
	job := strings.HasPrefix(r.URL.Path, jobsPath+"/")
	parent := r.Method == "GET" && (job || strings.HasPrefix(r.URL.Path, podsPath+"/") && !strings.HasSuffix(r.URL.Path, "/log"))
	if parent && a.getN > 0 {
		if a.getN--; a.getN == 0 {
			heldGet, getEntered = a.holdGet, a.getEntered
		}
	}
	a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if heldGet != nil {
		// Take the body from the stored object now, reply after the hold.
		prefix := podsPath + "/"
		if job {
			prefix = jobsPath + "/"
		}
		rec := httptest.NewRecorder()
		a.object(rec, r, job, strings.TrimPrefix(r.URL.Path, prefix))
		getEntered.release()
		select {
		case <-heldGet.ch:
		case <-stop.ch:
		case <-r.Context().Done():
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		w.Write(rec.Body.Bytes())
		return
	}
	switch {
	case r.Method == "POST" && (r.URL.Path == jobsPath || r.URL.Path == podsPath):
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			apiStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		if hold != nil && holdStored {
			// Store now, reply after the hold. Not combined with drop.
			rec := httptest.NewRecorder()
			a.create(rec, r.Header.Get("Content-Type"), r.URL.Path == jobsPath, raw)
			entered.release()
			select {
			case <-hold.ch:
			case <-stop.ch:
			}
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			w.Write(rec.Body.Bytes())
			done.release()
			return
		}
		if hold != nil {
			entered.release()
			select {
			case <-hold.ch:
			case <-stop.ch:
			}
		}
		a.create(w, r.Header.Get("Content-Type"), r.URL.Path == jobsPath, raw)
		done.release()
	case r.Method == "GET" && r.URL.Path == podsPath:
		a.list(w, r)
	case r.Method == "GET" && strings.HasPrefix(r.URL.Path, podsPath+"/") && strings.HasSuffix(r.URL.Path, "/log"):
		w.Header().Set("Content-Type", "text/plain")
		a.mu.Lock()
		logs := a.logs
		a.mu.Unlock()
		fmt.Fprint(w, logs)
		if holdLog != nil {
			w.(http.Flusher).Flush()
			logEntered.release()
			select {
			case <-holdLog.ch:
			case <-stop.ch:
			case <-r.Context().Done():
			}
		}
	case strings.HasPrefix(r.URL.Path, jobsPath+"/"):
		a.object(w, r, true, strings.TrimPrefix(r.URL.Path, jobsPath+"/"))
	case strings.HasPrefix(r.URL.Path, podsPath+"/"):
		a.object(w, r, false, strings.TrimPrefix(r.URL.Path, podsPath+"/"))
	default:
		apiStatus(w, 400, metav1.StatusReasonBadRequest)
	}
}

// decodeRequest decodes raw, the body of a write request sent with Content-Type
// ctype, into into. Typed client-go requests for built-in kinds are sent as
// protobuf unless the configuration sets a content type; others are sent as
// JSON. A protobuf body must decode to exactly into.
func decodeRequest(ctype string, raw []byte, into runtime.Object) error {
	media, _, _ := mime.ParseMediaType(ctype)
	if media != runtime.ContentTypeProtobuf {
		return json.Unmarshal(raw, into)
	}
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), runtime.ContentTypeProtobuf)
	if !ok {
		return errors.New("no protobuf serializer")
	}
	obj, _, err := info.Serializer.Decode(raw, nil, into)
	if err != nil {
		return err
	}
	if obj != into {
		return fmt.Errorf("decoded %T, want %T", obj, into)
	}
	return nil
}

func (a *receiptAPI) create(w http.ResponseWriter, ctype string, job bool, raw []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.posted = raw
	a.nextUID++
	uid := types.UID(fmt.Sprintf("uid-%d", a.nextUID))
	var obj any
	var exists bool
	if job {
		j := new(batchv1.Job)
		if decodeRequest(ctype, raw, j) != nil {
			apiStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		j.TypeMeta = metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
		j.UID, j.ResourceVersion = uid, "1"
		_, exists = a.jobs[j.Name]
		obj = j
	} else {
		p := new(corev1.Pod)
		if decodeRequest(ctype, raw, p) != nil {
			apiStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		p.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		p.UID, p.ResourceVersion = uid, "1"
		_, exists = a.pods[p.Name]
		obj = p
	}
	if exists {
		apiStatus(w, 409, metav1.StatusReasonAlreadyExists)
		return
	}
	if a.admit != nil {
		a.admit(obj)
	}
	if a.createCode == 0 || a.store || a.drop {
		a.put(obj)
	}
	if a.drop {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
		return
	}
	if a.createCode != 0 {
		apiStatus(w, a.createCode, a.createReason)
		return
	}
	code := 201
	if a.notCreated {
		code = 200
	}
	var body any = obj
	if a.createReply != nil {
		body = a.createReply(copyObject(obj))
	}
	a.replied = encodeBody(body)
	w.WriteHeader(code)
	if a.truncate {
		// The status line arrives, the body never completes. Not combined
		// with holdStored.
		w.Write(a.replied[:len(a.replied)/2])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
	w.Write(a.replied)
}

// encodeBody encodes body as json.Encoder does, or returns it if it is
// already bytes.
func encodeBody(body any) []byte {
	if raw, ok := body.([]byte); ok {
		return raw
	}
	data, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return append(data, '\n')
}

func copyObject(obj any) any {
	switch o := obj.(type) {
	case *batchv1.Job:
		return o.DeepCopy()
	case *corev1.Pod:
		return o.DeepCopy()
	}
	return obj
}

// replyWith installs reply and restarts its GET count.
func (a *receiptAPI) replyWith(reply func(n int, obj any) any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reply, a.gets = reply, make(map[string]int)
}

// since returns the requests after the first n.
func (a *receiptAPI) since(n int) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests[n:])
}

func (a *receiptAPI) put(obj any) {
	switch o := obj.(type) {
	case *batchv1.Job:
		a.jobs[o.Name] = o
	case *corev1.Pod:
		a.pods[o.Name] = o
	}
}

func (a *receiptAPI) list(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	sel, err := labels.Parse(r.URL.Query().Get("labelSelector"))
	if err != nil {
		apiStatus(w, 400, metav1.StatusReasonBadRequest)
		return
	}
	list := corev1.PodList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "PodList"}}
	names := make([]string, 0, len(a.pods))
	for name := range a.pods {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if p := a.pods[name]; sel.Matches(labels.Set(p.Labels)) {
			list.Items = append(list.Items, *p)
		}
	}
	json.NewEncoder(w).Encode(list)
}

func (a *receiptAPI) object(w http.ResponseWriter, r *http.Request, job bool, name string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var obj any
	var uid types.UID
	if j := a.jobs[name]; job && j != nil {
		obj, uid = j, j.UID
	}
	if p := a.pods[name]; !job && p != nil {
		obj, uid = p, p.UID
	}
	switch r.Method {
	case "GET":
		if obj == nil {
			apiStatus(w, 404, metav1.StatusReasonNotFound)
			return
		}
		var body any = obj
		if a.reply != nil {
			a.gets[name]++
			body = a.reply(a.gets[name], copyObject(obj))
		}
		w.Write(encodeBody(body))
	case "DELETE":
		var opts metav1.DeleteOptions
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			err = decodeRequest(r.Header.Get("Content-Type"), raw, &opts)
		}
		if err != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil {
			apiStatus(w, 400, metav1.StatusReasonBadRequest)
			return
		}
		switch {
		case a.deleteCode != 0:
			apiStatus(w, a.deleteCode, a.deleteReason)
		case obj == nil:
			apiStatus(w, 404, metav1.StatusReasonNotFound)
		case *opts.Preconditions.UID != uid:
			apiStatus(w, 409, metav1.StatusReasonConflict)
		default:
			if job {
				delete(a.jobs, name)
			} else {
				delete(a.pods, name)
			}
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Success"})
		}
	default:
		apiStatus(w, 405, metav1.StatusReasonMethodNotAllowed)
	}
}

func (a *receiptAPI) counts() (requests, writes int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests), a.writes
}

func (a *receiptAPI) job(name string) *batchv1.Job {
	a.mu.Lock()
	defer a.mu.Unlock()
	if j := a.jobs[name]; j != nil {
		return j.DeepCopy()
	}
	return nil
}

func (a *receiptAPI) pod(name string) *corev1.Pod {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p := a.pods[name]; p != nil {
		return p.DeepCopy()
	}
	return nil
}

// meta mutates the stored parent of kind named name.
func (a *receiptAPI) meta(kind, name string, f func(*metav1.ObjectMeta)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if kind == "Job" {
		f(&a.jobs[name].ObjectMeta)
	} else {
		f(&a.pods[name].ObjectMeta)
	}
}

// addJobPod adds a MODELED controller-owned Pod of Job j.
func (a *receiptAPI) addJobPod(j *batchv1.Job, name string, phase corev1.PodPhase) {
	a.mu.Lock()
	defer a.mu.Unlock()
	controller := true
	a.pods[name] = &corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "workloads", UID: types.UID(name + "-uid"),
			Labels:          map[string]string{ownerLabel: j.Labels[ownerLabel], idLabel: j.Labels[idLabel]},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: j.Name, UID: j.UID, Controller: &controller}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// terminateScheduled is terminateRetry for a receipted Pod. A kubelet runs
// only a Pod the scheduler bound, which records PodScheduled; a node name
// without it is refused by every receipted read.
func terminateScheduled(p *corev1.Pod) {
	terminateRetry(p)
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue})
}

func jobTask() backend.Task {
	return backend.Task{Argv: []string{"trainer"}, Image: "trainer:v1", Env: map[string]string{"A": "1"}, CPUMillis: 250, MemoryBytes: 1 << 20, TimeoutSeconds: 60}
}

func receiptRef(b *Backend, kind string) backend.Reference {
	if kind == "Job" {
		return backend.Reference{ID: testID, Task: jobTask(), Binding: b.binding}
	}
	return backend.Reference{ID: testID, Task: retryTask(), Binding: b.binding}
}

func (a *receiptAPI) parent(kind string) (uid string, ok bool) {
	name := "skygo-" + testID
	if kind == "Job" {
		if j := a.job(name); j != nil {
			return string(j.UID), true
		}
		return "", false
	}
	if p := a.pod(name); p != nil {
		return string(p.UID), true
	}
	return "", false
}

func receiptBytes(t *testing.T, b *Backend, id string) []byte {
	t.Helper()
	data, err := os.ReadFile(receiptFile(b.receipts, id))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return data
}

func TestReceiptJobAcrossRestart(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Job")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	j := api.job("skygo-" + testID)
	raw, _ := backend.TaskBytes(ref.Task)
	if j.Annotations[taskAnnotation] != fmt.Sprintf("%x", sha256.Sum256(raw)) || len(j.Annotations[nonceAnnotation]) != 64 || j.Annotations[specAnnotation] == "" {
		t.Fatalf("receipted annotations: %v", j.Annotations)
	}
	r, err := b.receipts.load(testID)
	if err != nil || r.Phase != phasePinned || r.UID != string(j.UID) || r.Nonce != j.Annotations[nonceAnnotation] {
		t.Fatalf("receipt: %+v %v", r, err)
	}

	restarted := newReceiptBackend(t, root, url)
	status, err := restarted.Observe(ctx, ref)
	if err != nil || status.State != backend.Starting || status.Instance != string(j.UID) {
		t.Fatalf("restart observe: %+v %v", status, err)
	}
	// The reported UID is the opaque instance core pins; it is accepted back.
	ref.Instance = status.Instance
	api.addJobPod(j, "skygo-"+testID+"-a", corev1.PodRunning)
	status, err = restarted.Observe(ctx, ref)
	if err != nil || status.State != backend.Running || status.Instance != string(j.UID) {
		t.Fatalf("running: %+v %v", status, err)
	}
	chunk, err := restarted.Logs(ctx, ref, 0, 64)
	if err != nil || string(chunk.Data) != "output\n" || chunk.Complete {
		t.Fatalf("logs: %+v %v", chunk, err)
	}
	_, writes := api.counts()
	if err := restarted.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
		t.Fatalf("second start: %v", err)
	}
	wrong := ref
	wrong.Instance = "other-uid"
	requests, _ := api.counts()
	if _, err := restarted.Observe(ctx, wrong); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("wrong instance: %v", err)
	}
	if n, w := api.counts(); n != requests || w != writes {
		t.Fatal("wrong instance reached the API")
	}
	// Ambiguous controller-owned Pods still refuse logs.
	api.addJobPod(j, "skygo-"+testID+"-b", corev1.PodRunning)
	if _, err := restarted.Logs(ctx, ref, 0, 64); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("ambiguous logs: %v", err)
	}

	if err := restarted.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if api.job("skygo-"+testID) != nil {
		t.Fatal("job not deleted")
	}
	r, err = restarted.receipts.load(testID)
	if err != nil || r.Cancel != stepDelivered {
		t.Fatalf("cancel receipt: %+v %v", r, err)
	}
	third := newReceiptBackend(t, root, url)
	status, err = third.Observe(ctx, ref)
	if err != nil || status.State != backend.Canceled || status.Instance != string(j.UID) {
		t.Fatalf("canceled after restart: %+v %v", status, err)
	}
	if _, writes2 := api.counts(); writes2 != writes+1 {
		t.Fatalf("writes: %d, want %d", writes2, writes+1)
	}
}

func TestReceiptRetryAcrossRestart(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	p := api.pod("skygo-" + testID)
	restarted := newReceiptBackend(t, root, url)
	status, err := restarted.Observe(ctx, ref)
	if err != nil || status.State != backend.Starting || status.Instance != string(p.UID) {
		t.Fatalf("restart observe: %+v %v", status, err)
	}
	ref.Instance = status.Instance
	api.mu.Lock()
	terminateScheduled(api.pods[p.Name])
	api.mu.Unlock()
	status, err = restarted.Observe(ctx, ref)
	if err != nil || status.State != backend.Failed || status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("stopped: %+v %v", status, err)
	}
	chunk, err := restarted.Logs(ctx, ref, 0, 64)
	if err != nil || string(chunk.Data) != "output\n" || !chunk.Complete {
		t.Fatalf("logs: %+v %v", chunk, err)
	}
	_, writes := api.counts()
	if err := restarted.Start(ctx, ref.ID, ref.Task); err == nil {
		t.Fatal("second create accepted")
	}
	if _, w := api.counts(); w != writes {
		t.Fatal("second create reached the API")
	}

	// A distinct attempt has its own receipt and Pod.
	next := receiptRef(b, "Pod")
	next.ID = otherID
	if err := restarted.Start(ctx, next.ID, next.Task); err != nil {
		t.Fatal(err)
	}
	q := api.pod("skygo-" + otherID)
	r, err := restarted.receipts.load(otherID)
	if err != nil || r.UID != string(q.UID) || r.UID == ref.Instance || r.Nonce != q.Annotations[nonceAnnotation] || r.Nonce == p.Annotations[nonceAnnotation] {
		t.Fatalf("second attempt: %+v %v", r, err)
	}
	if err := restarted.Cancel(ctx, next); err != nil {
		t.Fatal(err)
	}
	if r, err := restarted.receipts.load(otherID); err != nil || r.Cancel != stepDelivered {
		t.Fatalf("cancel: %+v %v", r, err)
	}

	// Cleanup after positive stopped evidence retires the durable receipt
	// to a tombstone that keeps its UID and authorizes nothing.
	third := newReceiptBackend(t, root, url)
	if err := third.removeExecution(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if r, err := third.receipts.load(testID); api.pod(p.Name) != nil || err != nil || r.Phase != phaseRetired || r.UID != ref.Instance {
		t.Fatalf("cleanup incomplete: %+v %v", r, err)
	}
	requests, _ := api.counts()
	if _, err := third.Observe(ctx, ref); !errors.Is(err, errReceiptRetired) {
		t.Fatalf("retired observe: %v", err)
	}
	if n, _ := api.counts(); n != requests {
		t.Fatal("retired receipt reached the API")
	}
}

func TestReceiptCrashPoints(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		for _, tt := range []struct {
			name     string
			step     string // failed persistence step, or "drop"
			phase    string // phase of the record whose step fails
			writes   int
			stored   string // receipt phase after Start, "" if none
			observed backend.State
			instance bool
		}{
			{"before-intent", "sync", phaseIntent, 0, "", "", false},
			{"intent-before-grant", "sync", phaseSent, 0, phaseIntent, backend.Starting, false},
			{"grant-before-request", "send", phaseSent, 0, phaseSent, backend.Starting, false},
			{"lost-response", "drop", "", 1, phaseSent, backend.Starting, true},
			{"before-pin", "rename", phasePinned, 1, phaseSent, backend.Starting, true},
			{"pin-not-synced", "dirsync", phasePinned, 1, phasePinned, backend.Starting, true},
		} {
			t.Run(kind+"/"+tt.name, func(t *testing.T) {
				ctx := context.Background()
				api, url, root := newReceiptAPI(t)
				b := newReceiptBackend(t, root, url)
				ref := receiptRef(b, kind)
				if tt.step == "drop" {
					api.drop = true
				} else {
					b.receipts.fault = func(step string, r *executionReceipt) error {
						if step == tt.step && r.Phase == tt.phase {
							return errors.New("injected crash")
						}
						return nil
					}
				}
				if err := b.Start(ctx, ref.ID, ref.Task); err == nil {
					t.Fatal("interrupted start succeeded")
				}
				if _, w := api.counts(); w != tt.writes {
					t.Fatalf("writes: %d, want %d", w, tt.writes)
				}
				restarted := newReceiptBackend(t, root, url)
				r, err := restarted.receipts.load(testID)
				if tt.stored == "" {
					if !errors.Is(err, errReceiptMissing) {
						t.Fatalf("receipt: %+v %v", r, err)
					}
					requests, _ := api.counts()
					if _, err := restarted.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
						t.Fatalf("observe without receipt: %v", err)
					}
					if n, _ := api.counts(); n != requests {
						t.Fatal("missing receipt reached the API")
					}
					return
				}
				if err != nil || r.Phase != tt.stored {
					t.Fatalf("receipt: %+v %v", r, err)
				}
				status, err := restarted.Observe(ctx, ref)
				if err != nil || status.State != tt.observed {
					t.Fatalf("observe: %+v %v", status, err)
				}
				uid, created := api.parent(kind)
				if tt.instance != created || tt.instance && status.Instance != uid || !tt.instance && status.Instance != "" {
					t.Fatalf("instance %q, created %v %q", status.Instance, created, uid)
				}
				if tt.instance {
					if r, err := restarted.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
						t.Fatalf("recovered receipt: %+v %v", r, err)
					}
				} else {
					if err := restarted.Cancel(ctx, ref); err == nil {
						t.Fatal("uncertain create canceled")
					}
					if _, err := restarted.Logs(ctx, ref, 0, 64); err == nil {
						t.Fatal("uncertain create logs")
					}
				}
				if err := restarted.Start(ctx, ref.ID, ref.Task); err == nil {
					t.Fatal("restart resent create")
				}
				if _, w := api.counts(); w != tt.writes {
					t.Fatalf("writes after restart: %d, want %d", w, tt.writes)
				}
			})
		}
	}
}

func TestReceiptRecoveryRefusesForeignObject(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		for _, tt := range []struct {
			name   string
			pinned bool
			change func(*metav1.ObjectMeta)
		}{
			{"nonce", false, func(m *metav1.ObjectMeta) { m.Annotations[nonceAnnotation] = strings.Repeat("0", 64) }},
			{"spec-annotation", false, func(m *metav1.ObjectMeta) { m.Annotations[specAnnotation] = strings.Repeat("0", 64) }},
			{"binding", false, func(m *metav1.ObjectMeta) { m.Annotations[bindingAnnotation] = "sha256:other" }},
			{"task", false, func(m *metav1.ObjectMeta) { m.Annotations[taskAnnotation] = strings.Repeat("0", 64) }},
			{"owner", false, func(m *metav1.ObjectMeta) { m.Labels[ownerLabel] = "other" }},
			{"namespace", false, func(m *metav1.ObjectMeta) { m.Namespace = "other" }},
			{"controller", false, func(m *metav1.ObjectMeta) {
				m.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ReplicaSet", Name: "rs", UID: "rs-uid"}}
			}},
			{"replacement-uid", true, func(m *metav1.ObjectMeta) { m.UID = "replacement-uid" }},
		} {
			t.Run(kind+"/"+tt.name, func(t *testing.T) {
				ctx := context.Background()
				api, url, root := newReceiptAPI(t)
				b := newReceiptBackend(t, root, url)
				ref := receiptRef(b, kind)
				api.drop = !tt.pinned
				b.Start(ctx, ref.ID, ref.Task)
				api.meta(kind, "skygo-"+testID, tt.change)
				restarted := newReceiptBackend(t, root, url)
				before := receiptBytes(t, restarted, testID)
				_, writes := api.counts()
				if _, err := restarted.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("observe: %v", err)
				}
				if err := restarted.Cancel(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("cancel: %v", err)
				}
				if _, err := restarted.Logs(ctx, ref, 0, 64); err == nil {
					t.Fatal("logs of a foreign object")
				}
				if _, w := api.counts(); w != writes {
					t.Fatal("foreign object was mutated")
				}
				if !bytes.Equal(before, receiptBytes(t, restarted, testID)) {
					t.Fatal("foreign object changed the receipt")
				}
			})
		}
	}
}

// TestReceiptSpecChange refuses a recovered object whose rendered
// specification differs, although its metadata matches.
func TestReceiptSpecChange(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Job")
	api.drop = true
	b.Start(ctx, ref.ID, ref.Task)
	api.mu.Lock()
	api.jobs["skygo-"+testID].Spec.Template.Spec.Containers[0].Image = "other:v1"
	api.mu.Unlock()
	if _, err := b.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("changed image: %v", err)
	}
	// A resource the rendering does not set, such as a LimitRange default,
	// is execution behavior and is refused as well.
	api.mu.Lock()
	c := &api.jobs["skygo-"+testID].Spec.Template.Spec.Containers[0]
	c.Image = "trainer:v1"
	c.Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: c.Resources.Requests[corev1.ResourceCPU]}
	api.mu.Unlock()
	if _, err := b.Observe(ctx, ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("added limit: %v", err)
	}
	if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseSent || r.UID != "" {
		t.Fatalf("refused object pinned: %+v %v", r, err)
	}
	// Positive twin: the object as posted is recovered.
	api.mu.Lock()
	c = &api.jobs["skygo-"+testID].Spec.Template.Spec.Containers[0]
	c.Resources.Limits = nil
	api.mu.Unlock()
	status, err := b.Observe(ctx, ref)
	if err != nil || status.Instance == "" {
		t.Fatalf("as posted: %+v %v", status, err)
	}
}

func TestReceiptCreateOutcomes(t *testing.T) {
	ctx := context.Background()
	t.Run("legacy-object", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		legacy, err := fromREST(TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: url})
		if err != nil {
			t.Fatal(err)
		}
		ref := receiptRef(legacy.Backend.(*Backend), "Job")
		if err := legacy.Backend.Start(ctx, ref.ID, ref.Task); err != nil {
			t.Fatal(err)
		}
		before, _ := json.Marshal(api.job("skygo-" + testID))
		b := newReceiptBackend(t, root, url)
		if err := b.Start(ctx, ref.ID, ref.Task); err == nil {
			t.Fatal("receipted start adopted a legacy job")
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRejected {
			t.Fatalf("receipt: %+v %v", r, err)
		}
		requests, writes := api.counts()
		for _, op := range []func() error{
			func() error { _, err := b.Observe(ctx, ref); return err },
			func() error { return b.Cancel(ctx, ref) },
			func() error { _, err := b.Logs(ctx, ref, 0, 64); return err },
		} {
			if err := op(); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("rejected create authorized an operation: %v", err)
			}
		}
		after, _ := json.Marshal(api.job("skygo-" + testID))
		if n, w := api.counts(); n != requests || w != writes || !bytes.Equal(before, after) {
			t.Fatal("legacy job was read or changed")
		}
	})
	t.Run("not-created", func(t *testing.T) {
		// A complete 200 response with the exact receipted object is a
		// known non-201 acknowledgment: refused (root-ratified, C5), not
		// left sent for recovery.
		api, url, root := newReceiptAPI(t)
		api.notCreated = true
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Pod")
		if err := b.Start(ctx, ref.ID, ref.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
			t.Fatalf("Created=false: %v", err)
		}
		uid, _ := api.parent("Pod")
		api.mu.Lock()
		replied := api.replied
		schedulePod(api.pods["skygo-"+testID], "node-a") // later fully acceptable
		api.mu.Unlock()
		r, err := b.receipts.load(testID)
		if !errors.Is(err, errReceiptRefused) || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil {
			t.Fatalf("receipt: %+v %v", r, err)
		}
		sum := sha256.Sum256(replied)
		want := receiptWitness{Status: 200, ResponseSHA256: fmt.Sprintf("%x", sum), ResponseBytes: int64(len(replied)),
			Decode: witnessDecoded, UID: uid, Nonce: r.Nonce, SpecAnnotation: r.SpecSHA256, SpecSHA256: r.SpecSHA256}
		if *r.Witness != want {
			t.Fatalf("witness: %+v, want %+v", *r.Witness, want)
		}
		before, stored := receiptBytes(t, b, testID), parentJSON(api, "Pod")
		for i, c := range []*Backend{b, newReceiptBackend(t, root, url)} {
			requests, writes := api.counts()
			if err := c.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
				t.Fatalf("backend %d start: %v", i, err)
			}
			for name, op := range refusedOps(ctx, c, ref, "Pod", uid) {
				if err := op(); !errors.Is(err, errReceiptRefused) {
					t.Fatalf("backend %d %s: %v", i, name, err)
				}
			}
			if n, w := api.counts(); n != requests || w != writes {
				t.Fatalf("backend %d reached the API", i)
			}
		}
		if !bytes.Equal(before, receiptBytes(t, b, testID)) || !bytes.Equal(stored, parentJSON(api, "Pod")) {
			t.Fatal("refused attempt changed state")
		}
		if _, w := api.counts(); w != 1 {
			t.Fatalf("writes: %d", w)
		}
	})
	t.Run("not-created-incomplete", func(t *testing.T) {
		// Twin: the same 200 status line with a body that never completes
		// is not a known acknowledgment. The receipt stays sent and the
		// stored object is recovered exactly once.
		api, url, root := newReceiptAPI(t)
		api.notCreated, api.truncate = true, true
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Pod")
		if err := b.Start(ctx, ref.ID, ref.Task); err == nil || errors.Is(err, backend.ErrIdentityMismatch) {
			t.Fatalf("incomplete response: %v", err)
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseSent || r.Witness != nil {
			t.Fatalf("receipt: %+v %v", r, err)
		}
		status, err := newReceiptBackend(t, root, url).Observe(ctx, ref)
		if uid, _ := api.parent("Pod"); err != nil || status.Instance != uid {
			t.Fatalf("recovery: %+v %v", status, err)
		}
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phasePinned {
			t.Fatalf("pinned: %+v %v", r, err)
		}
		if _, w := api.counts(); w != 1 {
			t.Fatalf("writes: %d", w)
		}
	})
	t.Run("late-create", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		api.createCode, api.createReason = 504, metav1.StatusReasonTimeout
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Job")
		if err := b.Start(ctx, ref.ID, ref.Task); err == nil {
			t.Fatal("timeout accepted")
		}
		for range 2 {
			status, err := b.Observe(ctx, ref)
			if err != nil || status.State != backend.Starting || status.Instance != "" || status.Reason != "kubernetes create outcome uncertain" {
				t.Fatalf("uncertain: %+v %v", status, err)
			}
		}
		// The request is applied late.
		api.mu.Lock()
		j := new(batchv1.Job)
		json.Unmarshal(api.posted, j)
		j.UID = "late-uid"
		api.jobs[j.Name] = j
		api.mu.Unlock()
		status, err := b.Observe(ctx, ref)
		if err != nil || status.Instance != "late-uid" {
			t.Fatalf("late: %+v %v", status, err)
		}
		if _, w := api.counts(); w != 1 {
			t.Fatalf("writes: %d", w)
		}
	})
	t.Run("unknown-conflict", func(t *testing.T) {
		api, url, root := newReceiptAPI(t)
		api.createCode, api.createReason = 409, metav1.StatusReasonUnknown
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Job")
		b.Start(ctx, ref.ID, ref.Task)
		if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseSent {
			t.Fatalf("an unproved conflict is not a rejection: %+v %v", r, err)
		}
	})
}

func TestReceiptCancelCustody(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T) (*receiptAPI, *Backend, backend.Reference) {
		api, url, root := newReceiptAPI(t)
		b := newReceiptBackend(t, root, url)
		ref := receiptRef(b, "Job")
		if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
			t.Fatal(err)
		}
		return api, b, ref
	}
	t.Run("intent-not-persisted", func(t *testing.T) {
		api, b, ref := setup(t)
		b.receipts.fault = func(step string, r *executionReceipt) error {
			if step == "sync" && r.Cancel == stepIntent {
				return errors.New("injected crash")
			}
			return nil
		}
		_, writes := api.counts()
		if err := b.Cancel(ctx, ref); err == nil {
			t.Fatal("cancel without durable intent")
		}
		if _, w := api.counts(); w != writes {
			t.Fatal("delete sent before intent was durable")
		}
		if r, _ := b.receipts.load(testID); r.Cancel != "" {
			t.Fatalf("receipt: %+v", r)
		}
	})
	t.Run("not-found-is-not-delivery", func(t *testing.T) {
		api, b, ref := setup(t)
		api.deleteCode, api.deleteReason = 404, metav1.StatusReasonNotFound
		if err := b.Cancel(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if r, _ := b.receipts.load(testID); r.Cancel != stepIntent {
			t.Fatalf("receipt: %+v", r)
		}
		api.mu.Lock()
		delete(api.jobs, "skygo-"+testID)
		api.mu.Unlock()
		status, err := b.Observe(ctx, ref)
		if err != nil || status.State != backend.Unknown {
			t.Fatalf("missing after undelivered cancel: %+v %v", status, err)
		}
	})
	t.Run("conflict", func(t *testing.T) {
		api, b, ref := setup(t)
		api.deleteCode, api.deleteReason = 409, metav1.StatusReasonConflict
		if err := b.Cancel(ctx, ref); err == nil {
			t.Fatal("conflicting delete accepted")
		}
		if r, _ := b.receipts.load(testID); r.Cancel != stepIntent {
			t.Fatalf("receipt: %+v", r)
		}
		if api.job("skygo-"+testID) == nil {
			t.Fatal("job deleted")
		}
	})
	t.Run("completed", func(t *testing.T) {
		api, b, ref := setup(t)
		api.mu.Lock()
		api.jobs["skygo-"+testID].Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		api.mu.Unlock()
		_, writes := api.counts()
		if err := b.Cancel(ctx, ref); err != nil {
			t.Fatal(err)
		}
		if r, _ := b.receipts.load(testID); r.Cancel != "" {
			t.Fatalf("completion lost to cancellation: %+v", r)
		}
		if _, w := api.counts(); w != writes {
			t.Fatal("completed job deleted")
		}
	})
}

func TestReceiptConcurrentStarts(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			api, url, root := newReceiptAPI(t)
			backends := []*Backend{newReceiptBackend(t, root, url), newReceiptBackend(t, root, url)}
			ref := receiptRef(backends[0], kind)
			var ops []*receiptOp
			for i := range 8 {
				ops = append(ops, api.fixture.run(fmt.Sprintf("start %d", i), func() error {
					return backends[i%2].Start(context.Background(), ref.ID, ref.Task)
				}))
			}
			started := 0
			for _, op := range ops {
				if op.wait(t) == nil {
					started++
				}
			}
			if _, w := api.counts(); started != 1 || w != 1 {
				t.Fatalf("started %d, writes %d", started, w)
			}
		})
	}
}

func TestReceiptCanceledContextBeforeResponse(t *testing.T) {
	api, url, root := newReceiptAPI(t)
	api.hold(true)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := api.fixture.run("start", func() error { return b.Start(ctx, ref.ID, ref.Task) })
	api.createEntered.wait(t, "create entered")
	cancel()
	if err := start.wait(t); err == nil {
		t.Fatal("canceled start succeeded")
	}
	api.holdCreate.release()
	api.createDone.wait(t, "create done")
	if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseSent {
		t.Fatalf("receipt: %+v %v", r, err)
	}
	status, err := newReceiptBackend(t, root, url).Observe(context.Background(), ref)
	if uid, _ := api.parent("Pod"); err != nil || status.Instance != uid {
		t.Fatalf("recovery: %+v %v", status, err)
	}
}

func TestReceiptRetireWhileLogsHeld(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	terminateScheduled(api.pods["skygo-"+testID])
	api.mu.Unlock()
	api.hold(false)
	status, err := b.Observe(ctx, ref)
	if err != nil || status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("proof: %+v %v", status, err)
	}
	ref.Instance = status.Instance
	logs := api.fixture.run("logs", func() error {
		_, err := b.Logs(ctx, ref, 0, 64)
		return err
	})
	api.logEntered.wait(t, "logs entered")
	_, writes := api.counts()
	before := receiptBytes(t, b, testID)
	if err := b.removeExecution(ctx, ref); !errors.Is(err, errOperationBusy) {
		t.Fatalf("retired while logs held: %v", err)
	}
	if _, w := api.counts(); w != writes || !bytes.Equal(before, receiptBytes(t, b, testID)) {
		t.Fatal("retirement acted while logs held")
	}
	api.holdLog.release()
	if err := logs.wait(t); err != nil {
		t.Fatal(err)
	}
	if err := b.removeExecution(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRetired {
		t.Fatalf("receipt after cleanup: %+v %v", r, err)
	}
}

// TestReceiptCrossBackendLogsHeld holds Logs in one Backend while a Backend
// of another constructor over the same scope attempts cleanup.
func TestReceiptCrossBackendLogsHeld(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	other := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	terminateScheduled(api.pods["skygo-"+testID])
	api.mu.Unlock()
	status, err := b.Observe(ctx, ref)
	if err != nil || status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("proof: %+v %v", status, err)
	}
	ref.Instance = status.Instance
	api.hold(false)
	logs := api.fixture.run("logs", func() error {
		chunk, err := b.Logs(ctx, ref, 0, 64)
		if err == nil && string(chunk.Data) != "output\n" {
			err = fmt.Errorf("logs: %q", chunk.Data)
		}
		return err
	})
	api.logEntered.wait(t, "logs entered")
	requests, writes := api.counts()
	before := receiptBytes(t, other, testID)
	if err := other.removeExecution(ctx, ref); !errors.Is(err, errOperationBusy) {
		t.Fatalf("cleanup while another Backend holds logs: %v", err)
	}
	if n, w := api.counts(); n != requests || w != writes || !bytes.Equal(before, receiptBytes(t, other, testID)) || api.pod("skygo-"+testID) == nil {
		t.Fatal("busy cleanup acted")
	}
	// Positive twin: shared operations of the other Backend proceed.
	if status, err := other.Observe(ctx, ref); err != nil || status.Instance != ref.Instance {
		t.Fatalf("shared observe: %+v %v", status, err)
	}
	api.holdLog.release()
	if err := logs.wait(t); err != nil {
		t.Fatal(err)
	}
	if err := other.removeExecution(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if r, err := b.receipts.load(testID); err != nil || r.Phase != phaseRetired || r.UID != ref.Instance || api.pod("skygo-"+testID) != nil {
		t.Fatalf("cleanup: %+v %v", r, err)
	}
}

// TestReceiptConcurrentFirstSend holds the single create of one Backend
// while a Backend of another constructor starts the same attempt.
func TestReceiptConcurrentFirstSend(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			api, url, root := newReceiptAPI(t)
			b := newReceiptBackend(t, root, url)
			other := newReceiptBackend(t, root, url)
			ref := receiptRef(b, kind)
			api.hold(true)
			first := api.fixture.run("first start", func() error { return b.Start(ctx, ref.ID, ref.Task) })
			api.createEntered.wait(t, "create entered")
			if err := other.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
				t.Fatalf("second Backend start: %v", err)
			}
			if kind == "Pod" {
				pending := ref
				pending.Instance = "uid-1"
				if err := other.removeExecution(ctx, pending); !errors.Is(err, errOperationBusy) {
					t.Fatalf("cleanup during create: %v", err)
				}
			}
			if _, w := api.counts(); w != 1 {
				t.Fatalf("writes during held create: %d", w)
			}
			api.holdCreate.release()
			if err := first.wait(t); err != nil {
				t.Fatal(err)
			}
			api.createDone.wait(t, "create done")
			uid, _ := api.parent(kind)
			if r, err := other.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
				t.Fatalf("receipt: %+v %v", r, err)
			}
			if _, w := api.counts(); w != 1 {
				t.Fatalf("writes: %d", w)
			}
		})
	}
}

// TestReceiptRetiredRestartNoResend checks that a retired tombstone survives
// restarts and authorizes no create, read, cancel or cleanup.
func TestReceiptRetiredRestartNoResend(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	api.mu.Lock()
	terminateScheduled(api.pods["skygo-"+testID])
	api.mu.Unlock()
	status, err := b.Observe(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	ref.Instance = status.Instance
	if err := b.removeExecution(ctx, ref); err != nil {
		t.Fatal(err)
	}
	tomb := receiptBytes(t, b, testID)
	for range 2 {
		restarted := newReceiptBackend(t, root, url)
		requests, writes := api.counts()
		if err := restarted.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
			t.Fatalf("restart start: %v", err)
		}
		for name, op := range map[string]func() error{
			"observe": func() error { _, err := restarted.Observe(ctx, ref); return err },
			"cancel":  func() error { return restarted.Cancel(ctx, ref) },
			"logs":    func() error { _, err := restarted.Logs(ctx, ref, 0, 64); return err },
			"cleanup": func() error { return restarted.removeExecution(ctx, ref) },
		} {
			if err := op(); !errors.Is(err, errReceiptRetired) {
				t.Fatalf("%s of a tombstone: %v", name, err)
			}
		}
		if n, w := api.counts(); n != requests || w != writes || !bytes.Equal(tomb, receiptBytes(t, restarted, testID)) {
			t.Fatal("tombstone authorized an effect")
		}
	}
	// Positive twin: a distinct attempt still starts once.
	_, writes := api.counts()
	next := ref
	next.ID, next.Instance = otherID, ""
	if err := newReceiptBackend(t, root, url).Start(ctx, next.ID, next.Task); err != nil {
		t.Fatal(err)
	}
	if _, w := api.counts(); w != writes+1 {
		t.Fatalf("writes: %d, want %d", w, writes+1)
	}
}

// TestReceiptRefusalsBeforeEffects checks that each refusal makes no API
// request and leaves the stored Job and receipt unchanged.
func TestReceiptRefusalsBeforeEffects(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name    string
		started bool
		prepare func(t *testing.T, b *Backend, ref *backend.Reference)
		run     func(b *Backend, ref backend.Reference) error
	}{
		{"invalid-id", false, nil, func(b *Backend, ref backend.Reference) error {
			return b.Start(ctx, "../"+testID, ref.Task)
		}},
		{"unchecked-task", false, nil, func(b *Backend, ref backend.Reference) error {
			ref.Task.Image = ""
			return b.Start(ctx, ref.ID, ref.Task)
		}},
		{"capacity", false, func(t *testing.T, b *Backend, _ *backend.Reference) { b.receipts.limit = 0 }, func(b *Backend, ref backend.Reference) error {
			return b.Start(ctx, ref.ID, ref.Task)
		}},
		{"missing-receipt", false, nil, func(b *Backend, ref backend.Reference) error {
			_, err := b.Observe(ctx, ref)
			return err
		}},
		{"wrong-binding", true, func(t *testing.T, _ *Backend, ref *backend.Reference) { ref.Binding = "sha256:other" }, func(b *Backend, ref backend.Reference) error {
			_, err := b.Observe(ctx, ref)
			return err
		}},
		{"different-task", true, func(t *testing.T, _ *Backend, ref *backend.Reference) { ref.Task.Argv = []string{"other"} }, func(b *Backend, ref backend.Reference) error {
			return b.Cancel(ctx, ref)
		}},
		{"corrupt-receipt", true, func(t *testing.T, b *Backend, _ *backend.Reference) {
			path := receiptFile(b.receipts, testID)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			writeReceiptBytes(t, path, data[:len(data)-1])
		}, func(b *Backend, ref backend.Reference) error {
			return b.Cancel(ctx, ref)
		}},
		{"modeled-dependents", true, func(t *testing.T, b *Backend, _ *backend.Reference) {
			// MODELED inventory: no backend method acts on dependent rows yet.
			r, err := b.receipts.load(testID)
			if err == nil {
				_, err = b.receipts.update(testID, func(x *executionReceipt) error {
					x.Objects = []receiptObject{{Kind: "ResourceClaimTemplate", Name: "t", OwnerUID: r.UID, SpecSHA256: strings.Repeat("d", 64), Phase: objectIntent}}
					return nil
				})
			}
			if err != nil {
				t.Fatal(err)
			}
		}, func(b *Backend, ref backend.Reference) error {
			return b.Cancel(ctx, ref)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api, url, root := newReceiptAPI(t)
			b := newReceiptBackend(t, root, url)
			ref := receiptRef(b, "Job")
			if tt.started {
				if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
					t.Fatal(err)
				}
			}
			if tt.prepare != nil {
				tt.prepare(t, b, &ref)
			}
			job, _ := json.Marshal(api.job("skygo-" + testID))
			receipt := receiptBytes(t, b, testID)
			requests, writes := api.counts()
			if err := tt.run(b, ref); err == nil {
				t.Fatal("refusal accepted")
			}
			if n, w := api.counts(); n != requests || w != writes {
				t.Fatal("refused operation reached the API")
			}
			after, _ := json.Marshal(api.job("skygo-" + testID))
			if !bytes.Equal(job, after) || !bytes.Equal(receipt, receiptBytes(t, b, testID)) {
				t.Fatal("refused operation changed state")
			}
		})
	}
}

func TestReceiptStatelessUnchanged(t *testing.T) {
	ctx := context.Background()
	api, url, _ := newReceiptAPI(t)
	target, err := fromREST(TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: url})
	if err != nil {
		t.Fatal(err)
	}
	b := target.Backend.(*Backend)
	if b.receipts != nil {
		t.Fatal("receipts enabled without state")
	}
	ref := receiptRef(b, "Job")
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(api.posted, []byte("creation-nonce")) || bytes.Contains(api.posted, []byte("spec-sha256")) {
		t.Fatalf("stateless create changed: %s", api.posted)
	}
	// The state-less path still adopts by owner, task and binding.
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	status, err := b.Observe(ctx, ref)
	if err != nil || status.Instance != "" {
		t.Fatalf("stateless observe: %+v %v", status, err)
	}
}
