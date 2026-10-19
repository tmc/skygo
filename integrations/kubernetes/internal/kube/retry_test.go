package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type retryAPI struct {
	mu            sync.Mutex
	pod           *corev1.Pod
	requests      []string
	createCode    int
	createUID     string
	dropResponse  bool
	getCount      int
	changeGet     int
	changeRuntime int
	getError      int
	deleteCode    int
	deletedUID    types.UID
	logs          string
}

func newRetryAPI(t *testing.T) (*Backend, *retryAPI) {
	t.Helper()
	api := &retryAPI{createCode: http.StatusCreated, createUID: "original-uid", logs: "retry output\n"}
	server := httptest.NewServer(http.HandlerFunc(api.serveHTTP))
	t.Cleanup(server.Close)
	target, err := fromREST(TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if !target.SupportsRetry {
		t.Fatal("implemented retry capability missing")
	}
	return target.Backend.(*Backend), api
}

func (a *retryAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	failure := func(code int) {
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Code: int32(code), Reason: metav1.StatusReasonUnknown})
	}
	if !strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/workloads/pods") {
		failure(400)
		return
	}
	switch {
	case r.Method == "POST":
		p := &corev1.Pod{}
		if err := json.NewDecoder(r.Body).Decode(p); err != nil {
			failure(400)
			return
		}
		p.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
		p.UID, p.ResourceVersion = types.UID(a.createUID), "1"
		a.pod = p
		if a.dropResponse {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if a.createCode >= 400 {
			failure(a.createCode)
			return
		}
		w.WriteHeader(a.createCode)
		json.NewEncoder(w).Encode(p)
	case strings.HasSuffix(r.URL.Path, "/log"):
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, a.logs)
	case r.Method == "GET":
		a.getCount++
		if a.getCount == a.getError {
			failure(503)
			return
		}
		if a.pod == nil {
			failure(404)
			return
		}
		if a.getCount == a.changeRuntime {
			a.pod.Status.ContainerStatuses[0].ContainerID = "containerd://changed"
			a.pod.Status.ContainerStatuses[0].State.Terminated.ContainerID = "containerd://changed"
		}
		if a.getCount == a.changeGet {
			a.pod.UID = "replacement-uid"
		}
		json.NewEncoder(w).Encode(a.pod)
	case r.Method == "DELETE":
		var opts metav1.DeleteOptions
		raw, err := io.ReadAll(r.Body)
		if err == nil {
			_, _, err = scheme.Codecs.UniversalDeserializer().Decode(raw, nil, &opts)
		}
		if err != nil || opts.Preconditions == nil || opts.Preconditions.UID == nil {
			failure(400)
			return
		}
		a.deletedUID = *opts.Preconditions.UID
		if a.deleteCode != 0 {
			failure(a.deleteCode)
			return
		}
		a.pod = nil
		json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Success"})
	default:
		failure(400)
	}
}

func retryTask() backend.Task {
	return backend.Task{Argv: []string{"worker"}, Image: "image", Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7, 137}}}
}
func retryRef(b *Backend) backend.Reference {
	return backend.Reference{ID: testID, Task: retryTask(), Binding: b.binding}
}
func (a *retryAPI) update(f func(*corev1.Pod)) { a.mu.Lock(); defer a.mu.Unlock(); f(a.pod) }
func (a *retryAPI) count() int                 { a.mu.Lock(); defer a.mu.Unlock(); return len(a.requests) }
func terminateRetry(p *corev1.Pod) {
	start := metav1.NewTime(time.Unix(1700000000, 0))
	finish := metav1.NewTime(start.Add(time.Second))
	p.Status.Phase = corev1.PodFailed
	p.Spec.NodeName = "node"
	p.Status.StartTime = &start
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "task", ContainerID: "containerd://actual", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 7, ContainerID: "containerd://actual", StartedAt: start, FinishedAt: finish}}}}
}

func TestRetryCreateReceipt(t *testing.T) {
	b, api := newRetryAPI(t)
	ref := retryRef(b)
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if api.pod.Spec.RestartPolicy != corev1.RestartPolicyNever || len(api.pod.OwnerReferences) != 0 {
		t.Fatal("retry Pod has a replacement controller")
	}
	status, err := b.Observe(context.Background(), ref)
	if err != nil || status.Instance != "original-uid" || status.Termination != backend.UnknownTermination {
		t.Fatalf("receipt: %+v %v", status, err)
	}
	api.update(terminateRetry)
	status, err = b.Observe(context.Background(), ref)
	if err != nil || status.State != backend.Failed || status.Termination != backend.StoppedWithExitCode || status.ExitCode == nil || *status.ExitCode != 7 {
		t.Fatalf("stopped: %+v %v", status, err)
	}
	chunk, err := b.Logs(context.Background(), ref, 0, 64)
	if err != nil || string(chunk.Data) != "retry output\n" || !chunk.Complete {
		t.Fatalf("logs: %+v %v", chunk, err)
	}
	if err := b.Cancel(context.Background(), ref); err != nil {
		t.Fatalf("cancel: %T %#v", err, err)
	}
	if api.deletedUID != "" {
		t.Fatal("deleted completed retry logs")
	}
	n := api.count()
	if err := b.Start(context.Background(), ref.ID, ref.Task); err == nil || api.count() != n {
		t.Fatal("repeated native create")
	}
	// A restarted backend can use the durable instance, but cannot discover it.
	b.mu.Lock()
	b.retries = nil
	b.mu.Unlock()
	n = api.count()
	if _, err := b.Observe(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) || api.count() != n {
		t.Fatal("adopted unpinned Pod after restart")
	}
	ref.Instance = status.Instance
	status, err = b.Observe(context.Background(), ref)
	if err != nil || status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("durable pin: %+v %v", status, err)
	}
}

func TestRetryCreateUncertainty(t *testing.T) {
	for _, tt := range []struct {
		name string
		code int
		uid  string
		drop bool
	}{{"not-created", 200, "uid", false}, {"collision", 409, "uid", false}, {"lost-response", 504, "uid", false}, {"dropped-response", 201, "uid", true}, {"missing-uid", 201, "", false}, {"oversized-uid", 201, strings.Repeat("x", 129), false}} {
		t.Run(tt.name, func(t *testing.T) {
			b, api := newRetryAPI(t)
			api.createCode = tt.code
			api.createUID = tt.uid
			api.dropResponse = tt.drop
			ref := retryRef(b)
			if err := b.Start(context.Background(), ref.ID, ref.Task); err == nil {
				t.Fatal("uncertain create accepted")
			}
			api.update(terminateRetry)
			n := api.count()
			for _, op := range []func() error{
				func() error { _, err := b.Observe(context.Background(), ref); return err },
				func() error { return b.Cancel(context.Background(), ref) },
				func() error { _, err := b.Logs(context.Background(), ref, 0, 64); return err },
				func() error { return b.Start(context.Background(), ref.ID, ref.Task) },
			} {
				if err := op(); err == nil {
					t.Fatal("uncertainty authorized operation")
				}
			}
			if api.count() != n {
				t.Fatal("uncertain receipt reached native API")
			}
		})
	}
}

func TestRetryIdentityFences(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*backend.Reference)
	}{
		{"binding", func(r *backend.Reference) { r.Binding = "wrong" }},
		{"instance", func(r *backend.Reference) { r.Instance = "wrong" }},
		{"instance-control-byte", func(r *backend.Reference) { r.Instance = "bad\n" }},
		{"task", func(r *backend.Reference) { r.Task.Image = "different" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, api := newRetryAPI(t)
			ref := retryRef(b)
			if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
				t.Fatal(err)
			}
			tt.change(&ref)
			n := api.count()
			if _, err := b.Observe(context.Background(), ref); err == nil {
				t.Fatal("observe passed")
			}
			if err := b.Cancel(context.Background(), ref); err == nil {
				t.Fatal("cancel passed")
			}
			if _, err := b.Logs(context.Background(), ref, 0, 64); err == nil {
				t.Fatal("logs passed")
			}
			if api.count() != n {
				t.Fatal("mismatch reached API")
			}
		})
	}
	for _, field := range []string{"uid", "owner", "hash", "binding"} {
		t.Run("native-"+field, func(t *testing.T) {
			b, api := newRetryAPI(t)
			ref := retryRef(b)
			if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
				t.Fatal(err)
			}
			api.update(func(p *corev1.Pod) {
				switch field {
				case "uid":
					p.UID = "replacement"
				case "owner":
					p.Labels[ownerLabel] = "other"
				case "hash":
					p.Annotations[taskAnnotation] = "other"
				case "binding":
					p.Annotations[bindingAnnotation] = "other"
				}
			})
			for _, op := range []func() error{func() error { _, e := b.Observe(context.Background(), ref); return e }, func() error { return b.Cancel(context.Background(), ref) }, func() error { _, e := b.Logs(context.Background(), ref, 0, 64); return e }} {
				if err := op(); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("native fence: %v", err)
				}
			}
			for _, request := range api.requests {
				if strings.HasPrefix(request, "DELETE") || strings.HasSuffix(request, "/log") {
					t.Fatal("wrong native effects", request)
				}
			}
		})
	}
}

func TestRetryStoppedEvidence(t *testing.T) {
	mutations := []struct {
		name   string
		change func(*corev1.Pod)
	}{
		{"phase-only", func(p *corev1.Pod) { p.Status.ContainerStatuses = nil }},
		{"runtime-missing", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "" }},
		{"runtime-id-empty", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "containerd://"
			p.Status.ContainerStatuses[0].ContainerID = "containerd://"
		}},
		{"runtime-mismatch", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].ContainerID = "containerd://other" }},
		{"epoch-start", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated.StartedAt = metav1.NewTime(time.Unix(0, 0))
		}},
		{"epoch-finish", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(time.Unix(0, 0))
		}},
		{"reversed-time", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(time.Unix(1699999999, 0))
		}},
		{"last-state", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].LastTerminationState = p.Status.ContainerStatuses[0].State
			p.Status.ContainerStatuses[0].State = corev1.ContainerState{}
		}},
		{"restart", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].RestartCount = 1 }},
		{"zero-exit", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 0 }},
		{"unselected-exit", func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 8 }},
		{"synthetic-137", func(p *corev1.Pod) {
			p.Status.ContainerStatuses[0].State.Terminated = &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "ContainerStatusUnknown"}
		}},
		{"eviction", func(p *corev1.Pod) { p.Status.Reason = "Evicted" }},
		{"deadline", func(p *corev1.Pod) { p.Status.Reason = "DeadlineExceeded" }},
		{"deleting", func(p *corev1.Pod) { v := metav1.Now(); p.DeletionTimestamp = &v }},
		{"extra-container", func(p *corev1.Pod) { p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "extra"}) }},
		{"extra-status", func(p *corev1.Pod) {
			p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{Name: "extra"})
		}},
		{"init", func(p *corev1.Pod) { p.Spec.InitContainers = []corev1.Container{{Name: "init"}} }},
		{"ephemeral", func(p *corev1.Pod) {
			p.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
		}},
		{"owner-controller", func(p *corev1.Pod) { p.OwnerReferences = []metav1.OwnerReference{{UID: "controller"}} }},
		{"restart-policy", func(p *corev1.Pod) { p.Spec.RestartPolicy = corev1.RestartPolicyOnFailure }},
	}
	for _, tt := range mutations {
		t.Run(tt.name, func(t *testing.T) {
			b, api := newRetryAPI(t)
			ref := retryRef(b)
			if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
				t.Fatal(err)
			}
			api.update(func(p *corev1.Pod) { terminateRetry(p); tt.change(p) })
			status, err := b.Observe(context.Background(), ref)
			if err != nil || status.Termination != backend.UnknownTermination {
				t.Fatalf("false stopped evidence: %+v %v", status, err)
			}
		})
	}
	t.Run("reported-137", func(t *testing.T) {
		b, api := newRetryAPI(t)
		ref := retryRef(b)
		if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
			t.Fatal(err)
		}
		api.update(func(p *corev1.Pod) {
			terminateRetry(p)
			p.Status.ContainerStatuses[0].State.Terminated.ExitCode = 137
			p.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled"
		})
		status, err := b.Observe(context.Background(), ref)
		if err != nil || status.Termination != backend.StoppedWithExitCode {
			t.Fatalf("reported exit not ordinary claim: %+v %v", status, err)
		}
	})
}

func TestRetryTerminationReread(t *testing.T) {
	for _, fault := range []string{"replacement", "runtime", "unavailable"} {
		t.Run(fault, func(t *testing.T) {
			b, api := newRetryAPI(t)
			ref := retryRef(b)
			if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
				t.Fatal(err)
			}
			api.update(terminateRetry)
			if fault == "replacement" {
				api.changeGet = 2
			} else if fault == "runtime" {
				api.changeRuntime = 2
			} else {
				api.getError = 2
			}
			status, err := b.Observe(context.Background(), ref)
			if (fault != "runtime" && err == nil) || status.Termination != backend.UnknownTermination || (fault == "runtime" && status.State != backend.Unknown) {
				t.Fatalf("reread failure granted proof: %+v %v", status, err)
			}
		})
	}
}

func TestRetryCancellation(t *testing.T) {
	b, api := newRetryAPI(t)
	ref := retryRef(b)
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(context.Background(), ref); err != nil {
		t.Fatalf("cancel: %T %#v", err, err)
	}
	if api.deletedUID != "original-uid" {
		t.Fatal("missing UID deletion precondition")
	}
	status, err := b.Observe(context.Background(), ref)
	if err != nil || status.State != backend.Unknown || status.Termination != backend.UnknownTermination {
		t.Fatalf("deletion manufactured stopped: %+v %v", status, err)
	}
	if _, err := b.Logs(context.Background(), ref, 0, 64); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing logs: %v", err)
	}
}

func TestRetryFailedCancelIntent(t *testing.T) {
	b, api := newRetryAPI(t)
	api.deleteCode = 403
	ref := retryRef(b)
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(context.Background(), ref); err == nil {
		t.Fatal("delete failure lost")
	}
	api.update(terminateRetry)
	status, err := b.Observe(context.Background(), ref)
	if err != nil || status.State != backend.Canceled || status.Termination != backend.UnknownTermination {
		t.Fatalf("cancel intent allowed retry proof: %+v %v", status, err)
	}
}

func TestRetryReceiptBound(t *testing.T) {
	b, api := newRetryAPI(t)
	b.retries = make(map[string]*retryReceipt)
	for i := 0; i < maxRetryReceipts-1; i++ {
		b.retries[fmt.Sprintf("%032x", i)] = &retryReceipt{}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
		wg.Add(1)
		go func(id string) { defer wg.Done(); errs <- b.Start(context.Background(), id, retryTask()) }(id)
	}
	wg.Wait()
	close(errs)
	failures := 0
	for err := range errs {
		if err != nil {
			failures++
		}
	}
	if failures != 1 || api.count() != 1 || len(b.retries) != maxRetryReceipts {
		t.Fatalf("bound: failures=%d native=%d receipts=%d", failures, api.count(), len(b.retries))
	}
}

func TestRetryDeadlineAndPolicy(t *testing.T) {
	b, api := newRetryAPI(t)
	ref := retryRef(b)
	ref.Task.TimeoutSeconds = 1
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	if api.pod.Spec.ActiveDeadlineSeconds == nil || *api.pod.Spec.ActiveDeadlineSeconds != 1 {
		t.Fatal("pod deadline not enforced")
	}
	api.update(terminateRetry)
	status, err := b.Observe(context.Background(), ref)
	if err != nil || status.Termination != backend.UnknownTermination {
		t.Fatalf("elapsed deadline granted proof: %+v %v", status, err)
	}
	for _, policy := range []*backend.RetryPolicy{{MaxAttempts: 1}, {MaxAttempts: 2, ExitCodes: []int32{7}}, {MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7, 7}}} {
		task := retryTask()
		task.Retry = policy
		n := api.count()
		if err := b.Start(context.Background(), strings.Repeat("c", 32), task); err == nil || api.count() != n {
			t.Fatal("unnormalized retry reached API")
		}
	}
}

// A generated fake Create can return an object without ever receiving HTTP201.
// The retry path must reject this client rather than treat that as a receipt.
func TestRetryRejectsFakeCreateReceipt(t *testing.T) {
	b, err := testBackend(fake.NewSimpleClientset(), "workloads", "owner")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background(), testID, retryTask()); err == nil {
		t.Fatal("fake create accepted as an HTTP receipt")
	}
}

func TestRetryMissingReceiptHasNoEffects(t *testing.T) {
	b, api := newRetryAPI(t)
	ref := retryRef(b)
	if _, err := b.Observe(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("observe: %v", err)
	}
	if err := b.Cancel(context.Background(), ref); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := b.Logs(context.Background(), ref, 0, 64); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("logs: %v", err)
	}
	if api.count() != 0 {
		t.Fatal("missing receipt reached API")
	}
}
