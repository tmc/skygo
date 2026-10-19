package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type gcAPI struct {
	retryAPI
	loseDelete, keepDeleted, replaceDeleted  bool
	replaceBeforeDelete                      bool
	rejectedUID                              string
	getCode                                  int
	holdLog, logEntered, holdGet, getEntered chan struct{}
}

func newGCBackend(t *testing.T) (*Backend, *gcAPI, backend.Target) {
	t.Helper()
	a := &gcAPI{retryAPI: retryAPI{createCode: 201, createUID: "original-uid", logs: "output\n"}}
	server := httptest.NewServer(http.HandlerFunc(a.serve))
	t.Cleanup(server.Close)
	target, err := fromREST(TargetConfig{Name: "test", Namespace: "workloads"}, "owner", &rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	return target.Backend.(*Backend), a, target
}

func (a *gcAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	lose, keep, replace, code := a.loseDelete, a.keepDeleted, a.replaceDeleted, a.getCode
	holdLog, logEntered, holdGet, getEntered := a.holdLog, a.logEntered, a.holdGet, a.getEntered
	a.mu.Unlock()
	if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/log") && holdLog != nil {
		a.mu.Lock()
		a.requests = append(a.requests, r.Method+" "+r.URL.Path)
		a.mu.Unlock()
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(logEntered)
		select {
		case <-holdLog:
		case <-r.Context().Done():
		}
		return
	}
	if r.Method == "GET" && !strings.HasSuffix(r.URL.Path, "/log") {
		if holdGet != nil {
			a.mu.Lock()
			a.holdGet = nil
			a.mu.Unlock()
			close(getEntered)
			select {
			case <-holdGet:
			case <-r.Context().Done():
				return
			}
		}
		if code != 0 {
			a.mu.Lock()
			a.requests = append(a.requests, r.Method+" "+r.URL.Path)
			a.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonUnknown, Code: int32(code)})
			return
		}
	}
	if r.Method == "DELETE" {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 4097))
		var opts metav1.DeleteOptions
		if err == nil && len(raw) <= 4096 {
			_, _, err = scheme.Codecs.UniversalDeserializer().Decode(raw, nil, &opts)
		}
		if err != nil || len(raw) > 4096 || opts.Preconditions == nil || opts.Preconditions.UID == nil {
			w.WriteHeader(400)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		a.mu.Lock()
		if a.replaceBeforeDelete {
			a.pod.UID = "replacement-uid"
		}
		conflict := a.pod != nil && a.pod.UID != *opts.Preconditions.UID
		if conflict {
			a.rejectedUID = string(*opts.Preconditions.UID)
			a.requests = append(a.requests, r.Method+" "+r.URL.Path)
		}
		a.mu.Unlock()
		if conflict {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: "Failure", Reason: metav1.StatusReasonConflict, Code: 409})
			return
		}
	}
	if r.Method != "DELETE" || !lose && !keep && !replace {
		a.retryAPI.serveHTTP(w, r)
		return
	}
	a.mu.Lock()
	saved := a.pod.DeepCopy()
	a.mu.Unlock()
	rec := httptest.NewRecorder()
	a.retryAPI.serveHTTP(rec, r)
	a.mu.Lock()
	if keep {
		a.pod = saved
	}
	if replace {
		a.pod = saved
		a.pod.UID = "replacement-uid"
	}
	a.mu.Unlock()
	if lose {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.WriteHeader(rec.Code)
	w.Write(rec.Body.Bytes())
}

func gcReady(t *testing.T) (*Backend, *gcAPI, backend.Reference) {
	t.Helper()
	b, a, _ := newGCBackend(t)
	ref := retryRef(b)
	if err := b.Start(context.Background(), ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	a.update(terminateRetry)
	status, err := b.Observe(context.Background(), ref)
	if err != nil || status.Termination != backend.StoppedWithExitCode {
		t.Fatalf("proof: %+v %v", status, err)
	}
	ref.Instance = status.Instance
	return b, a, ref
}

func hasRetry(b *Backend, id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retries[id] != nil
}

func TestRetentionFacts(t *testing.T) {
	_, _, target := newGCBackend(t)
	if target.OutputStorage != backend.RemoteOutput || target.OutputReservation != 0 || target.Retention == nil || target.Retention.RemoveExecution == nil || target.Retention.CheckOutputClosed != nil || target.Retention.RemoveOutput != nil {
		t.Fatalf("facts: %+v", target)
	}
}

func TestRetentionRemovesProvedPod(t *testing.T) {
	b, a, ref := gcReady(t)
	b.mu.Lock()
	b.retries[strings.Repeat("b", 32)] = &retryReceipt{uid: "other", hash: "other"}
	b.mu.Unlock()
	if err := b.removeExecution(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if hasRetry(b, ref.ID) || !hasRetry(b, strings.Repeat("b", 32)) || a.deletedUID != "original-uid" {
		t.Fatal("incorrect receipt retirement")
	}
	if err := b.removeExecution(context.Background(), ref); err != nil {
		t.Fatalf("idempotent: %v", err)
	}
	n := 0
	for _, r := range a.requests {
		if strings.HasPrefix(r, "DELETE ") {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("delete count %d", n)
	}
}

func TestRetentionRestartAndLostDelete(t *testing.T) {
	t.Run("lost-response", func(t *testing.T) {
		b, a, ref := gcReady(t)
		a.loseDelete = true
		if err := b.removeExecution(context.Background(), ref); err == nil || !hasRetry(b, ref.ID) {
			t.Fatal("lost response credited cleanup")
		}
		if err := b.removeExecution(context.Background(), ref); err != nil || hasRetry(b, ref.ID) {
			t.Fatalf("absence retry: %v", err)
		}
	})
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "restart-present", true: "restart-missing"}[missing], func(t *testing.T) {
			b, a, ref := gcReady(t)
			b.mu.Lock()
			b.retries = nil
			b.mu.Unlock()
			if missing {
				a.mu.Lock()
				a.pod = nil
				a.mu.Unlock()
			}
			if err := b.removeExecution(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			if hasRetry(b, ref.ID) {
				t.Fatal("created cleanup receipt")
			}
		})
	}
}

func TestRetentionRefusesUnprovedOrChanged(t *testing.T) {
	for _, kind := range []string{"default", "success", "synthetic", "uid", "uid-on-reread", "runtime-on-reread", "owner", "hash", "binding", "replaced-after-delete", "replacement-before-delete", "present-after-delete", "forbidden-get", "forbidden-delete", "outage"} {
		t.Run(kind, func(t *testing.T) {
			b, a, ref := gcReady(t)
			switch kind {
			case "default":
				ref.Task.Retry = nil
			case "success":
				a.update(func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded })
			case "synthetic":
				a.update(func(p *corev1.Pod) { p.Status.ContainerStatuses[0].State.Terminated.ContainerID = "" })
			case "uid":
				a.update(func(p *corev1.Pod) { p.UID = "replacement" })
			case "uid-on-reread":
				a.changeGet = a.getCount + 2
			case "runtime-on-reread":
				a.changeRuntime = a.getCount + 2
			case "owner":
				a.update(func(p *corev1.Pod) { p.Labels[ownerLabel] = "other" })
			case "hash":
				a.update(func(p *corev1.Pod) { p.Annotations[taskAnnotation] = "other" })
			case "binding":
				a.update(func(p *corev1.Pod) { p.Annotations[bindingAnnotation] = "other" })
			case "replacement-before-delete":
				a.replaceBeforeDelete = true
			case "replaced-after-delete":
				a.replaceDeleted = true
			case "present-after-delete":
				a.keepDeleted = true
			case "forbidden-get":
				a.getCode = 403
			case "forbidden-delete":
				a.deleteCode = 403
			case "outage":
				a.getCode = 503
			}
			if err := b.removeExecution(context.Background(), ref); err == nil || !hasRetry(b, ref.ID) {
				t.Fatalf("%s credited cleanup: %v", kind, err)
			}
			if kind == "replacement-before-delete" {
				a.mu.Lock()
				intact := a.pod != nil && a.pod.UID == "replacement-uid" && a.rejectedUID == "original-uid"
				a.mu.Unlock()
				if !intact {
					t.Fatal("UID conflict deleted replacement or omitted precondition")
				}
			}
			if kind != "replaced-after-delete" && kind != "present-after-delete" && kind != "forbidden-delete" && a.deletedUID != "" {
				t.Fatal("unqualified native delete")
			}
		})
	}
}

func TestRetentionRejectsBeforeIO(t *testing.T) {
	for _, kind := range []string{"missing-instance", "invalid-instance", "binding", "task", "id", "ambiguous-receipt"} {
		t.Run(kind, func(t *testing.T) {
			b, a, ref := gcReady(t)
			switch kind {
			case "missing-instance":
				ref.Instance = ""
			case "invalid-instance":
				ref.Instance = "bad\n"
			case "binding":
				ref.Binding = "other"
			case "task":
				ref.Task.Image = "other"
			case "id":
				ref.ID = "bad"
			case "ambiguous-receipt":
				b.mu.Lock()
				b.retries[ref.ID].uid = ""
				b.mu.Unlock()
			}
			n := a.count()
			if err := b.removeExecution(context.Background(), ref); err == nil || a.count() != n || !hasRetry(b, testID) {
				t.Fatalf("bad reference effects: %v", err)
			}
		})
	}
}

func TestRetentionPendingOwners(t *testing.T) {
	b, a, ref := gcReady(t)
	a.holdLog, a.logEntered = make(chan struct{}), make(chan struct{})
	defer close(a.holdLog)
	logs := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _, err := b.Logs(ctx, ref, 0, 64); logs <- err }()
	select {
	case <-a.logEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	n := a.count()
	if err := b.removeExecution(ctx, ref); err == nil || a.count() != n || !hasRetry(b, ref.ID) {
		t.Fatal("retired a pending log owner")
	}
	cancel()
	<-logs
	if err := b.removeExecution(context.Background(), ref); err != nil || hasRetry(b, ref.ID) {
		t.Fatalf("released log cleanup: %v", err)
	}
	if err := b.Start(context.Background(), strings.Repeat("d", 32), retryTask()); err != nil {
		t.Fatalf("released owner capacity: %v", err)
	}
}

func TestRetentionExclusiveAndCapacity(t *testing.T) {
	b, a, ref := gcReady(t)
	a.holdGet, a.getEntered = make(chan struct{}), make(chan struct{})
	release := a.holdGet
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { done <- b.removeExecution(ctx, ref) }()
	select {
	case <-a.getEntered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	n := a.count()
	if _, err := b.Observe(ctx, ref); err == nil || a.count() != n {
		t.Fatal("observer entered active native cleanup")
	}
	if err := b.Cancel(ctx, ref); err == nil || a.count() != n {
		t.Fatal("cancel entered active native cleanup")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Use a fresh proved Pod to exercise actual receipt-capacity retirement.
	b, a, ref = gcReady(t)
	b.mu.Lock()
	for i := len(b.retries); i < maxRetryReceipts; i++ {
		id := fmt.Sprintf("%032x", i+100)
		b.retries[id] = &retryReceipt{}
	}
	b.mu.Unlock()
	if err := b.removeExecution(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	count := len(b.retries)
	b.mu.Unlock()
	if count != maxRetryReceipts-1 {
		t.Fatalf("receipt count %d", count)
	}
	if err := b.Start(context.Background(), strings.Repeat("c", 32), retryTask()); err != nil {
		t.Fatalf("capacity not reusable: %v", err)
	}
}

func TestRetentionDefaultMissingUnavailable(t *testing.T) {
	b, a, _ := newGCBackend(t)
	r := retryRef(b)
	r.Task.Retry = nil
	r.Instance = "uid"
	if err := b.removeExecution(context.Background(), r); !errors.Is(err, fs.ErrNotExist) || a.count() != 0 {
		t.Fatalf("default cleanup: %v", err)
	}
}
