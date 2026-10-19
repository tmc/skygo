package kube

import (
	"context"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestReferenceBindingFence(t *testing.T) {
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range []string{"", "sha256:different"} {
		ref := backend.Reference{ID: testID, Binding: binding, Task: backend.Task{Argv: []string{"work"}, Image: "image"}}
		for name, call := range map[string]func() error{
			"observe": func() error { _, err := b.Observe(context.Background(), ref); return err },
			"cancel":  func() error { return b.Cancel(context.Background(), ref) },
			"logs":    func() error { _, err := b.Logs(context.Background(), ref, 0, 10); return err },
		} {
			t.Run(name+binding, func(t *testing.T) {
				client.ClearActions()
				if err := call(); !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("got %v", err)
				}
				if len(client.Actions()) != 0 {
					t.Fatalf("binding mismatch reached API: %v", client.Actions())
				}
			})
		}
	}
}

func TestNativeBindingAndUID(t *testing.T) {
	for _, field := range []string{"binding", "uid"} {
		t.Run(field, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			b, err := testBackend(client, "workloads", "test")
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			task := backend.Task{Argv: []string{"work"}, Image: "image"}
			ref := backend.Reference{ID: testID, Task: task, Binding: b.binding}
			if err := b.Start(ctx, testID, task); err != nil {
				t.Fatal(err)
			}
			j, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if field == "binding" {
				delete(j.Annotations, bindingAnnotation)
			} else {
				j.UID = ""
			}
			if _, err := client.BatchV1().Jobs("workloads").Update(ctx, j, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			for name, call := range map[string]func() error{
				"start":   func() error { return b.Start(ctx, testID, task) },
				"observe": func() error { _, err := b.Observe(ctx, ref); return err },
				"cancel":  func() error { return b.Cancel(ctx, ref) },
				"logs":    func() error { _, err := b.Logs(ctx, ref, 0, 10); return err },
			} {
				t.Run(name, func(t *testing.T) {
					client.ClearActions()
					if err := call(); !errors.Is(err, backend.ErrIdentityMismatch) {
						t.Fatalf("got %v", err)
					}
					for _, a := range client.Actions() {
						if a.GetVerb() == "delete" || a.GetResource().Resource == "pods" {
							t.Fatalf("conflicting identity accessed: %v", a)
						}
					}
				})
			}
		})
	}
}

func TestDeleteNotFoundIsNotCancellation(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Image: "image"}
	ref := backend.Reference{ID: testID, Task: task, Binding: b.binding}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "jobs", func(a ktesting.Action) (bool, runtime.Object, error) {
		d := a.(ktesting.DeleteAction)
		if d.GetDeleteOptions().Preconditions == nil || d.GetDeleteOptions().Preconditions.UID == nil || *d.GetDeleteOptions().Preconditions.UID == "" {
			t.Error("delete lacks UID precondition")
		}
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, d.GetName())
	})
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if err := client.Tracker().Delete(batchv1.SchemeGroupVersion.WithResource("jobs"), "workloads", "skygo-"+testID); err != nil {
		t.Fatal(err)
	}
	status, err := b.Observe(ctx, ref)
	if err != nil || status.State != backend.Unknown {
		t.Fatalf("NotFound invented cancellation: %+v %v", status, err)
	}
	if err := b.Cancel(ctx, ref); err != nil {
		t.Fatal(err)
	}
	status, err = b.Observe(ctx, ref)
	if err != nil || status.State != backend.Unknown {
		t.Fatalf("GET NotFound invented cancellation: %+v %v", status, err)
	}
}

func TestSuccessfulDeleteFactSurvivesReplay(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Image: "image"}
	ref := backend.Reference{ID: testID, Task: task, Binding: b.binding}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := b.Cancel(ctx, ref); err != nil {
			t.Fatal(err)
		}
	}
	status, err := b.Observe(ctx, ref)
	if err != nil || status.State != backend.Canceled {
		t.Fatalf("lost successful delete: %+v %v", status, err)
	}
}

func TestWrongPodUIDIsNotRead(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Image: "image"}
	ref := backend.Reference{ID: testID, Task: task, Binding: b.binding}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	controller := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Labels: map[string]string{ownerLabel: "test", idLabel: testID}, OwnerReferences: []metav1.OwnerReference{{UID: "wrong", Name: "skygo-" + testID, Kind: "Job", Controller: &controller}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if _, err := client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	chunk, err := b.Logs(ctx, ref, 0, 10)
	if err != nil || len(chunk.Data) != 0 || chunk.Complete {
		t.Fatalf("wrong UID logs: %+v %v", chunk, err)
	}
	for _, a := range client.Actions() {
		if a.GetSubresource() == "log" {
			t.Fatal("read foreign Pod logs")
		}
	}
}
