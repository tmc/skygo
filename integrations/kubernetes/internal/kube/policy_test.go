package kube

import (
	"context"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func modelPolicy() ResourcePolicy {
	return ResourcePolicy{MaxCPUMillis: 1000, MaxMemoryBytes: 8 << 20, MaxAccelerators: 2,
		NodeSelectors: map[string][]string{"skygo.test/pool": {"s04"}, "nvidia.com/gpu.product": {"A100"}},
		Accelerators:  map[string]AcceleratorPolicy{"A100": {ResourceName: "nvidia.com/gpu", RequiredNodeSelector: map[string]string{"nvidia.com/gpu.product": "A100"}}},
	}
}

func TestNativeResourcePolicy(t *testing.T) {
	client := fake.NewSimpleClientset()
	b, err := testBackend(client, "workloads", "test")
	if err != nil {
		t.Fatal(err)
	}
	b.policy = modelPolicy()
	task := backend.Task{Argv: []string{"work"}, Image: "image", CPUMillis: 250, CPULimitMillis: 500, MemoryBytes: 1 << 20, MemoryLimitBytes: 2 << 20, Accelerator: "A100", AcceleratorCount: 1, NodeSelector: map[string]string{"skygo.test/pool": "s04", "nvidia.com/gpu.product": "A100"}}
	if err := b.Start(context.Background(), testID, task); err != nil {
		t.Fatal(err)
	}
	j, err := client.BatchV1().Jobs("workloads").Get(context.Background(), "skygo-"+testID, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := j.Spec.Template.Spec.Containers[0].Resources
	for _, q := range []struct {
		name           corev1.ResourceName
		request, limit int64
	}{{corev1.ResourceCPU, 250, 500}, {corev1.ResourceMemory, 1 << 20, 2 << 20}, {"nvidia.com/gpu", 1, 1}} {
		a, z := r.Requests[q.name], r.Limits[q.name]
		av, zv := a.Value(), z.Value()
		if q.name == corev1.ResourceCPU {
			av, zv = a.MilliValue(), z.MilliValue()
		}
		if av != q.request || zv != q.limit {
			t.Fatalf("%s: requests %d, limits %d", q.name, av, zv)
		}
	}
	if got := j.Spec.Template.Spec.NodeSelector; len(got) != 2 || got["skygo.test/pool"] != "s04" || got["nvidia.com/gpu.product"] != "A100" {
		t.Fatalf("selector %v", got)
	}
	if j.Annotations[bindingAnnotation] != b.binding {
		t.Fatal("binding annotation missing")
	}
	changed := task
	changed.CPULimitMillis++
	if err := b.Start(context.Background(), testID, changed); err == nil {
		t.Fatal("adopted changed limits under unchanged task hash")
	}
}

func TestPolicyRejectsBeforeCreate(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*backend.Task)
	}{
		{"negative-limit", func(t *backend.Task) { t.CPULimitMillis = -1 }},
		{"limit-without-request", func(t *backend.Task) { t.CPUMillis = 0 }},
		{"small-limit", func(t *backend.Task) { t.CPULimitMillis = 1 }},
		{"cpu-request-max", func(t *backend.Task) { t.CPUMillis = 1001; t.CPULimitMillis = 0 }},
		{"cpu-limit-max", func(t *backend.Task) { t.CPULimitMillis = 1001 }},
		{"memory-limit-without-request", func(t *backend.Task) { t.MemoryBytes = 0 }},
		{"memory-limit-max", func(t *backend.Task) { t.MemoryLimitBytes = 9 << 20 }},
		{"memory-request-max", func(t *backend.Task) { t.MemoryBytes = 9 << 20; t.MemoryLimitBytes = 0 }},
		{"count-max", func(t *backend.Task) { t.AcceleratorCount = 3 }},
		{"unknown-model", func(t *backend.Task) { t.Accelerator = "B100" }},
		{"model-selector-missing", func(t *backend.Task) { delete(t.NodeSelector, "nvidia.com/gpu.product") }},
		{"disallowed-value", func(t *backend.Task) { t.NodeSelector["skygo.test/pool"] = "other" }},
		{"disallowed-key", func(t *backend.Task) { t.NodeSelector["other"] = "s04" }},
		{"invalid-key", func(t *backend.Task) { t.NodeSelector["bad/key/name"] = "s04" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			b, err := testBackend(client, "workloads", "test")
			if err != nil {
				t.Fatal(err)
			}
			b.policy = modelPolicy()
			task := backend.Task{Argv: []string{"work"}, Image: "image", CPUMillis: 250, CPULimitMillis: 500, MemoryBytes: 1 << 20, MemoryLimitBytes: 2 << 20, Accelerator: "A100", AcceleratorCount: 1, NodeSelector: map[string]string{"skygo.test/pool": "s04", "nvidia.com/gpu.product": "A100"}}
			test.change(&task)
			if err := b.Start(context.Background(), testID, task); err == nil {
				t.Fatal("policy admitted invalid task")
			}
			if len(client.Actions()) != 0 {
				t.Fatalf("policy rejection made API calls: %v", client.Actions())
			}
		})
	}
}
