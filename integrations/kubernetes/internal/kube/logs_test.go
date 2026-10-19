package kube

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestLogPrefix(t *testing.T) {
	controller := true
	ref := backend.Reference{Binding: testBinding, ID: testID, Task: backend.Task{Argv: []string{"work"}, Image: "image"}}
	raw, _ := json.Marshal(ref.Task)
	job := batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "skygo-" + testID, UID: "uid", Annotations: map[string]string{bindingAnnotation: testBinding, taskAnnotation: fmt.Sprintf("%x", sha256.Sum256(raw))}, Labels: map[string]string{ownerLabel: "test", idLabel: testID}}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: job.Labels, OwnerReferences: []metav1.OwnerReference{{Name: job.Name, UID: job.UID, Kind: "Job", Controller: &controller}}}}
	output := strings.Repeat("x", backend.MaxLogBytes+10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/workloads/jobs/" + job.Name:
			json.NewEncoder(w).Encode(job)
		case "/api/v1/namespaces/workloads/pods":
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod}})
		case "/api/v1/namespaces/workloads/pods/pod/log":
			if r.URL.Query().Get("tailLines") != "" || r.URL.Query().Get("limitBytes") != "" {
				t.Error("log prefix replaced with server tail")
			}
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprint(w, output)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := testBackend(client, "workloads", "test")
	chunk, err := b.Logs(context.Background(), ref, backend.MaxLogBytes-3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if string(chunk.Data) != "xxx" || !chunk.Truncated || !chunk.Complete || chunk.NextOffset != backend.MaxLogBytes {
		t.Fatalf("chunk: %+v", chunk)
	}
	if _, err = b.Logs(context.Background(), ref, backend.MaxLogBytes+1, 10); err == nil {
		t.Fatal("accepted offset beyond prefix")
	}
}
