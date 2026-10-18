package kube

import (
	"context"
	"errors"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

const testID = "0123456789abcdef0123456789abcdef"

func TestJobLifecycle(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, err := New(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"echo", "hello"}, Image: "busybox", Env: map[string]string{"Z": "last", "A": "first"}, Directory: "/work", CPUMillis: 250, MemoryBytes: 1024, Accelerator: "nvidia", AcceleratorCount: 1, TimeoutSeconds: 30}
	if err = b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	j, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if j.Spec.Template.Spec.AutomountServiceAccountToken == nil || *j.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatal("workload service account token mounted")
	}
	c := j.Spec.Template.Spec.Containers[0]
	if *j.Spec.BackoffLimit != 0 || j.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever || *j.Spec.ActiveDeadlineSeconds != 30 {
		t.Fatalf("bad lifecycle policy: %+v", j.Spec)
	}
	cpu := c.Resources.Requests[corev1.ResourceCPU]
	gpu := c.Resources.Limits["nvidia.com/gpu"]
	if cpu.MilliValue() != 250 || gpu.Value() != 1 || c.Env[0].Name != "A" || c.WorkingDir != "/work" {
		t.Fatalf("bad container: %+v", c)
	}
	if err = b.Start(ctx, testID, task); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	task.Image = "other"
	if err = b.Start(ctx, testID, task); err == nil {
		t.Fatal("adopted different task")
	}
	status, err := b.Observe(ctx, testID)
	if err != nil || status.State != backend.Starting {
		t.Fatalf("observe: %+v %v", status, err)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err = client.BatchV1().Jobs("workloads").UpdateStatus(ctx, j, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	status, err = b.Observe(ctx, testID)
	if err != nil || status.State != backend.Succeeded || status.ExitCode == nil || *status.ExitCode != 0 {
		t.Fatalf("complete: %+v %v", status, err)
	}
	if err = b.Cancel(ctx, testID); err != nil {
		t.Fatal(err)
	}
	status, err = b.Observe(ctx, testID)
	if err != nil || status.State != backend.Succeeded {
		t.Fatalf("cancel completed: %+v %v", status, err)
	}
	restarted, _ := New(client, "workloads", "test")
	status, err = restarted.Observe(ctx, testID)
	if err != nil || status.State != backend.Succeeded {
		t.Fatalf("restart completed: %+v %v", status, err)
	}
	// A distinct unfinished workload remains cancelable, including replay.
	activeID := "abcdef0123456789abcdef0123456789"
	if err = b.Start(ctx, activeID, task); err != nil {
		t.Fatal(err)
	}
	if err = b.Cancel(ctx, activeID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		status, err = b.Observe(ctx, activeID)
		if err != nil || status.State != backend.Canceled {
			t.Fatalf("cancel: %+v %v", status, err)
		}
	}
	status, err = restarted.Observe(ctx, activeID)
	if err != nil || status.State != backend.Unknown {
		t.Fatalf("restart missing: %+v %v", status, err)
	}
	if err = restarted.Cancel(ctx, activeID); err != nil {
		t.Fatal(err)
	}
	status, err = restarted.Observe(ctx, activeID)
	if err != nil || status.State != backend.Canceled {
		t.Fatalf("replay cancel: %+v %v", status, err)
	}

}

func TestOwnershipAndFailures(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := New(client, "workloads", "test")
	task := backend.Task{Argv: []string{"true"}, Image: "busybox"}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	other, _ := New(client, "workloads", "other")
	for name, run := range map[string]func() error{
		"start":   func() error { return other.Start(ctx, testID, task) },
		"observe": func() error { _, err := other.Observe(ctx, testID); return err },
		"cancel":  func() error { return other.Cancel(ctx, testID) },
		"logs":    func() error { _, err := other.Logs(ctx, testID, 0, 10); return err },
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "ownership") {
				t.Fatalf("want ownership error, got %v", err)
			}
		})
	}
	interrupted := true
	client.PrependReactor("get", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		if interrupted {
			return true, nil, errors.New("api unavailable")
		}
		return false, nil, nil
	})
	if _, err := b.Observe(ctx, testID); err == nil {
		t.Fatal("outage was treated as terminal state")
	}
	interrupted = false
	status, err := b.Observe(ctx, testID)
	if err != nil || status.State != backend.Starting {
		t.Fatalf("observe after API recovery: %+v %v", status, err)
	}
}

func TestCheck(t *testing.T) {
	b, _ := New(fake.NewSimpleClientset(), "workloads", "test")
	for _, test := range []struct {
		name string
		task backend.Task
	}{
		{"argv", backend.Task{Image: "busybox"}},
		{"image", backend.Task{Argv: []string{"true"}}},
		{"negative", backend.Task{Argv: []string{"true"}, Image: "busybox", CPUMillis: -1}},
		{"accelerator", backend.Task{Argv: []string{"true"}, Image: "busybox", Accelerator: "amd", AcceleratorCount: 1}},
		{"count", backend.Task{Argv: []string{"true"}, Image: "busybox", Accelerator: "nvidia"}},
		{"env", backend.Task{Argv: []string{"true"}, Image: "busybox", Env: map[string]string{"bad=name": "x"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := b.Check(test.task); err == nil {
				t.Fatal("invalid task accepted")
			}
		})
	}
	if err := b.Check(backend.Task{Argv: []string{"true"}, Image: "busybox"}); err != nil {
		t.Fatal(err)
	}
	if len(b.client.(*fake.Clientset).Actions()) != 0 {
		t.Fatal("Check made API calls")
	}
}

func TestPodObservation(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := New(client, "workloads", "test")
	if err := b.Start(ctx, testID, backend.Task{Argv: []string{"true"}, Image: "busybox"}); err != nil {
		t.Fatal(err)
	}
	j, _ := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
	j.UID = "job-uid"
	client.BatchV1().Jobs("workloads").Update(ctx, j, metav1.UpdateOptions{})
	controller := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: j.Labels, OwnerReferences: []metav1.OwnerReference{{UID: j.UID, Name: j.Name, Kind: "Job", Controller: &controller}}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable"}}}}
	client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{})
	status, err := b.Observe(ctx, testID)
	if err != nil || status.State != backend.Starting || !strings.Contains(status.Reason, "Unschedulable") {
		t.Fatalf("pending: %+v %v", status, err)
	}
	p.Status.Phase = corev1.PodRunning
	client.CoreV1().Pods("workloads").UpdateStatus(ctx, p, metav1.UpdateOptions{})
	status, err = b.Observe(ctx, testID)
	if err != nil || status.State != backend.Running {
		t.Fatalf("running: %+v %v", status, err)
	}
	p.Name = "second"
	client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{})
	if _, err = b.Logs(ctx, testID, 0, 10); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("multiple pods: %v", err)
	}
}

func TestDeletedJobIsNotRecreated(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := New(client, "workloads", "test")
	if err := b.Start(ctx, testID, backend.Task{Argv: []string{"true"}, Image: "busybox"}); err != nil {
		t.Fatal(err)
	}
	if err := client.BatchV1().Jobs("workloads").Delete(ctx, "skygo-"+testID, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	for i := 0; i < 2; i++ {
		status, err := b.Observe(ctx, testID)
		if err != nil || status.State != backend.Unknown {
			t.Fatalf("observe: %+v %v", status, err)
		}
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			t.Fatal("Observe recreated missing Job")
		}
	}
	if err := b.Check(backend.Task{Argv: []string{"true"}, Image: "busybox", Directory: "relative"}); err == nil {
		t.Fatal("accepted relative directory")
	}
}

func TestCompletedBeforeObservedCancellation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		condition batchv1.JobConditionType
		want      backend.State
	}{
		{"success", batchv1.JobComplete, backend.Succeeded}, {"failure", batchv1.JobFailed, backend.Failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s, err := store.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			client := fake.NewSimpleClientset()
			b, err := New(client, "workloads", s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			c, err := control.New(s, []backend.Target{{Name: "kube", Kind: "kubernetes", Backend: b}})
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"work"}, Image: "image"}
			j, err := c.Submit("completed", task)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Start(ctx, j.ID, task); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Update(j.ID, func(j *backend.Job) error { j.Status.State = backend.Running; return nil }); err != nil {
				t.Fatal(err)
			}
			remote, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+j.ID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			remote.Status.Conditions = []batchv1.JobCondition{{Type: tt.condition, Status: corev1.ConditionTrue}}
			if _, err := client.BatchV1().Jobs("workloads").UpdateStatus(ctx, remote, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Cancel(j.ID); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			runCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer cancel()
			if err := c.Run(runCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			got, err := c.Get(j.ID)
			if err != nil || got.Status.State != tt.want {
				t.Fatalf("completed cancellation: %+v %v", got, err)
			}
			for _, a := range client.Actions() {
				if a.GetVerb() == "delete" {
					t.Fatalf("deleted completed workload/log owner: %+v", a)
				}
			}
			if _, err := client.BatchV1().Jobs("workloads").Get(ctx, remote.Name, metav1.GetOptions{}); err != nil {
				t.Fatalf("log owner not retained: %v", err)
			}
		})
	}
}
