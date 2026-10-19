package kube

import (
	"maps"
	"sort"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func (b *Backend) podSpec(t backend.Task) corev1.PodSpec {
	automount := false
	resources := corev1.ResourceList{}
	if t.CPUMillis > 0 {
		resources[corev1.ResourceCPU] = *resource.NewMilliQuantity(t.CPUMillis, resource.DecimalSI)
	}
	if t.MemoryBytes > 0 {
		resources[corev1.ResourceMemory] = *resource.NewQuantity(t.MemoryBytes, resource.BinarySI)
	}
	limits := corev1.ResourceList{}
	if t.CPULimitMillis > 0 {
		limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(t.CPULimitMillis, resource.DecimalSI)
	}
	if t.MemoryLimitBytes > 0 {
		limits[corev1.ResourceMemory] = *resource.NewQuantity(t.MemoryLimitBytes, resource.BinarySI)
	}
	if t.AcceleratorCount > 0 {
		q := *resource.NewQuantity(int64(t.AcceleratorCount), resource.DecimalSI)
		name := corev1.ResourceName(b.policy.Accelerators[t.Accelerator].ResourceName)
		resources[name] = q
		limits[name] = q
	}
	keys := make([]string, 0, len(t.Env))
	for k := range t.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]corev1.EnvVar, 0, len(keys))
	for _, k := range keys {
		env = append(env, corev1.EnvVar{Name: k, Value: t.Env[k]})
	}

	return corev1.PodSpec{
		AutomountServiceAccountToken: &automount,
		RestartPolicy:                corev1.RestartPolicyNever,
		NodeSelector:                 maps.Clone(t.NodeSelector),
		Containers: []corev1.Container{{Name: "task", Image: t.Image, Command: t.Argv,
			Env: env, WorkingDir: t.Directory,
			Resources: corev1.ResourceRequirements{Requests: resources, Limits: limits},
		}},
	}
}
