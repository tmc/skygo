package kube

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
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
	b, err := testBackend(client, "workloads", "test")
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
	originalTask := task
	task.Image = "other"
	if err = b.Start(ctx, testID, task); err == nil {
		t.Fatal("adopted different task")
	}
	task = originalTask
	status, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Starting {
		t.Fatalf("observe: %+v %v", status, err)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err = client.BatchV1().Jobs("workloads").UpdateStatus(ctx, j, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	status, err = b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Succeeded || status.ExitCode == nil || *status.ExitCode != 0 {
		t.Fatalf("complete: %+v %v", status, err)
	}
	if err = b.Cancel(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task}); err != nil {
		t.Fatal(err)
	}
	status, err = b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Succeeded {
		t.Fatalf("cancel completed: %+v %v", status, err)
	}
	restarted, _ := testBackend(client, "workloads", "test")
	status, err = restarted.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Succeeded {
		t.Fatalf("restart completed: %+v %v", status, err)
	}
	// A distinct unfinished workload remains cancelable, including replay.
	activeID := "abcdef0123456789abcdef0123456789"
	if err = b.Start(ctx, activeID, task); err != nil {
		t.Fatal(err)
	}
	if err = b.Cancel(ctx, backend.Reference{Binding: testBinding, ID: activeID, Task: task}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		status, err = b.Observe(ctx, backend.Reference{Binding: testBinding, ID: activeID, Task: task})
		if err != nil || status.State != backend.Canceled {
			t.Fatalf("cancel: %+v %v", status, err)
		}
	}
	status, err = restarted.Observe(ctx, backend.Reference{Binding: testBinding, ID: activeID, Task: task})
	if err != nil || status.State != backend.Unknown {
		t.Fatalf("restart missing: %+v %v", status, err)
	}
	if err = restarted.Cancel(ctx, backend.Reference{Binding: testBinding, ID: activeID, Task: task}); err != nil {
		t.Fatal(err)
	}
	status, err = restarted.Observe(ctx, backend.Reference{Binding: testBinding, ID: activeID, Task: task})
	if err != nil || status.State != backend.Unknown {
		t.Fatalf("replay cancel: %+v %v", status, err)
	}

}

func TestOwnershipAndFailures(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := testBackend(client, "workloads", "test")
	task := backend.Task{Argv: []string{"true"}, Image: "busybox"}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	other, _ := testBackend(client, "workloads", "other")
	for name, run := range map[string]func() error{
		"start": func() error { return other.Start(ctx, testID, task) },
		"observe": func() error {
			_, err := other.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
			return err
		},
		"cancel": func() error {
			return other.Cancel(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
		},
		"logs": func() error {
			_, err := other.Logs(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task}, 0, 10)
			return err
		},
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
	if _, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task}); err == nil {
		t.Fatal("outage was treated as terminal state")
	}
	interrupted = false
	status, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Starting {
		t.Fatalf("observe after API recovery: %+v %v", status, err)
	}
}

func TestCheck(t *testing.T) {
	b, _ := testBackend(fake.NewSimpleClientset(), "workloads", "test")
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
	b, _ := testBackend(client, "workloads", "test")
	task := backend.Task{Argv: []string{"true"}, Image: "busybox"}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	j, _ := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
	j.UID = "job-uid"
	client.BatchV1().Jobs("workloads").Update(ctx, j, metav1.UpdateOptions{})
	controller := true
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: j.Labels, OwnerReferences: []metav1.OwnerReference{{UID: j.UID, Name: j.Name, Kind: "Job", Controller: &controller}}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable"}}}}
	client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{})
	status, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Starting || !strings.Contains(status.Reason, "Unschedulable") {
		t.Fatalf("pending: %+v %v", status, err)
	}
	p.Status.Phase = corev1.PodRunning
	client.CoreV1().Pods("workloads").UpdateStatus(ctx, p, metav1.UpdateOptions{})
	status, err = b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
	if err != nil || status.State != backend.Running {
		t.Fatalf("running: %+v %v", status, err)
	}
	p.Name = "second"
	client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{})
	if _, err = b.Logs(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task}, 0, 10); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("multiple pods: %v", err)
	}
}

func TestDeletedJobIsNotRecreated(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := testBackend(client, "workloads", "test")
	task := backend.Task{Argv: []string{"true"}, Image: "busybox"}
	if err := b.Start(ctx, testID, task); err != nil {
		t.Fatal(err)
	}
	if err := client.BatchV1().Jobs("workloads").Delete(ctx, "skygo-"+testID, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	client.ClearActions()
	for i := 0; i < 2; i++ {
		status, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
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
			b, err := testBackend(client, "workloads", s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			engine := auth.NewLegacy("")
			actor, err := engine.Authenticate("")
			if err != nil {
				t.Fatal(err)
			}
			c, err := control.New(s, []backend.Target{{Name: "kube", Kind: "kubernetes", Backend: b, Binding: b.binding}}, engine)
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"work"}, Image: "image"}
			j, err := c.Submit(actor, "completed", task)
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
			if _, err := c.Cancel(actor, j.ID); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			runCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer cancel()
			if err := c.Run(runCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			got, err := c.Get(actor, j.ID)
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

// A conflicting Create response must not be bypassed by the next observation.
func TestConflictingCreateIsNotAdopted(t *testing.T) {
	ctx := context.Background()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", s.Owner())
	if err != nil {
		t.Fatal(err)
	}
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	c, err := control.New(s, []backend.Target{{Name: "kube", Backend: b, Binding: b.binding}}, engine)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}, Image: "wanted"}
	j, err := c.Submit(actor, "conflict", task)
	if err != nil {
		t.Fatal(err)
	}
	otherTask := task
	otherTask.Image = "different"
	if err := b.Start(ctx, j.ID, otherTask); err != nil {
		t.Fatal(err)
	}
	remote, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+j.ID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	remote.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("workloads").UpdateStatus(ctx, remote, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if err := c.Run(runCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	got, err := c.Get(actor, j.ID)
	if err != nil || got.Status.State != backend.Unknown || !strings.Contains(got.Status.Reason, "identity mismatch") {
		t.Fatalf("conflict outcome: %+v %v", got, err)
	}
}

func TestRestartRejectsMismatchedTask(t *testing.T) {
	ctx := context.Background()
	task := backend.Task{Argv: []string{"work"}, Image: "wanted"}
	ref := backend.Reference{Binding: testBinding, ID: testID, Task: task}
	for _, annotation := range []string{"", "different"} {
		t.Run(annotation, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			b, _ := testBackend(client, "workloads", "test")
			if err := b.Start(ctx, testID, task); err != nil {
				t.Fatal(err)
			}
			remote, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			remote.Annotations[taskAnnotation] = annotation
			if _, err := client.BatchV1().Jobs("workloads").Update(ctx, remote, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			recovered, _ := testBackend(client, "workloads", "test")
			client.ClearActions()
			for name, run := range map[string]func() error{
				"observe": func() error { _, err := recovered.Observe(ctx, ref); return err },
				"cancel":  func() error { return recovered.Cancel(ctx, ref) },
				"logs":    func() error { _, err := recovered.Logs(ctx, ref, 0, 10); return err },
			} {
				t.Run(name, func(t *testing.T) {
					if err := run(); !errors.Is(err, backend.ErrIdentityMismatch) {
						t.Fatalf("want identity mismatch, got %v", err)
					}
				})
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" {
					t.Fatalf("conflicting workload modified or accessed: %+v", action)
				}
			}
		})
	}
}

func TestMissingPodLogs(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	b, _ := testBackend(client, "workloads", "test")
	ref := backend.Reference{Binding: testBinding, ID: testID, Task: backend.Task{Argv: []string{"work"}, Image: "image"}}
	if err := b.Start(ctx, ref.ID, ref.Task); err != nil {
		t.Fatal(err)
	}
	chunk, err := b.Logs(ctx, ref, 0, 10)
	if err != nil || chunk.Complete {
		t.Fatalf("pending logs: %+v %v", chunk, err)
	}
	j, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+ref.ID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs("workloads").UpdateStatus(ctx, j, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Logs(ctx, ref, 0, 10); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lost logs: %v", err)
	}
}

func TestMissingJobLogs(t *testing.T) {
	b, err := testBackend(fake.NewSimpleClientset(), "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Logs(context.Background(), backend.Reference{Binding: testBinding, ID: testID}, 0, 10)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing job logs: %v", err)
	}
}

// TestFailedJobExitCode observes only the workload's known, current exit.
// Job failure alone, including an init or scheduling failure, proves no exit.
func TestFailedJobExitCode(t *testing.T) {
	terminated := func(name string, code int32) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code}}}
	}
	for _, tt := range []struct {
		name      string
		statuses  []corev1.ContainerStatus
		init      []corev1.ContainerStatus
		phase     corev1.PodPhase
		multiple  bool
		foreign   bool
		listError bool
		want      int32
	}{
		{name: "exit7", statuses: []corev1.ContainerStatus{terminated("task", 7)}, want: 7},
		{name: "sidecar first", statuses: []corev1.ContainerStatus{terminated("proxy", 9), terminated("task", 7)}, want: 7},
		{name: "sidecar only", statuses: []corev1.ContainerStatus{terminated("proxy", 9)}},
		{name: "init failed", init: []corev1.ContainerStatus{terminated("task", 9)}},
		{name: "task succeeded", statuses: []corev1.ContainerStatus{terminated("task", 0), terminated("proxy", 9)}},
		{name: "pending", phase: corev1.PodPending},
		{name: "running", statuses: []corev1.ContainerStatus{{Name: "task", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
		{name: "previous exit", statuses: []corev1.ContainerStatus{{Name: "task", LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 7}}}}},
		{name: "multiple pods", statuses: []corev1.ContainerStatus{terminated("task", 7)}, multiple: true},
		{name: "foreign owner", statuses: []corev1.ContainerStatus{terminated("task", 7)}, foreign: true},
		{name: "list unavailable", statuses: []corev1.ContainerStatus{terminated("task", 7)}, listError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset()
			b, err := testBackend(client, "workloads", "test")
			if err != nil {
				t.Fatal(err)
			}
			task := backend.Task{Argv: []string{"sh", "-c", "exit 7"}, Image: "busybox"}
			if err := b.Start(ctx, testID, task); err != nil {
				t.Fatal(err)
			}
			j, err := client.BatchV1().Jobs("workloads").Get(ctx, "skygo-"+testID, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			j.UID = "job-uid"
			j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "job failed"}}
			if _, err := client.BatchV1().Jobs("workloads").Update(ctx, j, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			controller := true
			p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: j.Labels, OwnerReferences: []metav1.OwnerReference{{UID: j.UID, Name: j.Name, Kind: "Job", Controller: &controller}}}, Status: corev1.PodStatus{Phase: tt.phase, ContainerStatuses: tt.statuses, InitContainerStatuses: tt.init}}
			if tt.foreign {
				p.OwnerReferences[0].UID = "replacement-job"
			}
			if _, err := client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if tt.multiple {
				p.Name = "second"
				if _, err := client.CoreV1().Pods("workloads").Create(ctx, p, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if tt.listError {
				client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("api unavailable") })
			}
			status, err := b.Observe(ctx, backend.Reference{Binding: testBinding, ID: testID, Task: task})
			if err != nil || status.State != backend.Failed || status.Reason != "BackoffLimitExceeded: job failed" {
				t.Fatalf("observe = %+v, %v", status, err)
			}
			if tt.want == 0 {
				if status.ExitCode != nil {
					t.Fatalf("invented exit code %d", *status.ExitCode)
				}
			} else if status.ExitCode == nil || *status.ExitCode != tt.want {
				t.Fatalf("exit code = %v, want %d", status.ExitCode, tt.want)
			}
		})
	}
}
