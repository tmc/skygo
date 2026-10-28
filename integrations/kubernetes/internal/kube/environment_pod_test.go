package kube

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnvironmentPod(t *testing.T) {
	b := environmentTestBackend(t)
	in, p := environmentTestInput(), environmentTestProfile()
	got, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}

	yes, no := true, false
	user := int64(65532)
	grace := int64(30)
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	seconds := int64(300)
	want := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "skygo-env-0123456789abcdef0123456789abcdef", Namespace: "workloads",
			Labels: map[string]string{"skygo.tmc.dev/owner": "owner", "skygo.tmc.dev/id": in.id},
			Annotations: map[string]string{
				"skygo.tmc.dev/target-binding":               b.binding,
				"skygo.tmc.dev/environment-generation":       "1",
				"skygo.tmc.dev/environment-spec-sha256":      hex.EncodeToString(in.specHash[:]),
				"skygo.tmc.dev/environment-profile-revision": "profile-1",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			DNSPolicy:                     corev1.DNSClusterFirst,
			AutomountServiceAccountToken:  &no,
			ShareProcessNamespace:         &no,
			SchedulerName:                 "default-scheduler",
			Tolerations: []corev1.Toleration{
				{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists,
					Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
				{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists,
					Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
			},
			Priority:           &priority,
			PreemptionPolicy:   &preempt,
			EnableServiceLinks: &no,
			ImagePullSecrets:   []corev1.LocalObjectReference{{Name: "registry-b"}, {Name: "registry-a"}},
			Containers: []corev1.Container{{
				Name:       "workspace",
				Image:      environmentTestImage,
				Command:    []string{"/usr/local/bin/idle", "--forever"},
				WorkingDir: "/workspace",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("256Mi")},
					Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi")},
				},
				VolumeMounts:    []corev1.VolumeMount{{Name: "workspace", MountPath: "/workspace"}},
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: &corev1.SecurityContext{
					RunAsNonRoot: &yes, RunAsUser: &user, RunAsGroup: &user,
					AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
					Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
					SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
				},
			}},
			Volumes: []corev1.Volume{{Name: "workspace", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "alice-home"}}}},
		},
	}
	if !equality.Semantic.DeepEqual(got, want) {
		t.Fatalf("pod = %+v\nwant %+v", got, want)
	}
	if !fixedPodSpecMatches(&want.Spec, &got.Spec) {
		t.Fatal("fixed spec does not match itself")
	}
	if got.Spec.ActiveDeadlineSeconds != nil || len(got.OwnerReferences) != 0 || got.Spec.SecurityContext != nil ||
		len(got.Spec.SchedulingGates) != 0 {
		t.Fatal("unexpected deadline, owner, pod security context or scheduling gate")
	}
}

func TestEnvironmentPodResources(t *testing.T) {
	b := environmentTestBackend(t)
	in, p := environmentTestInput(), environmentTestProfile()
	in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
	in.task.NodeSelector = map[string]string{"pool": "gpu"}
	got, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	if got.Spec.NodeSelector["pool"] != "gpu" || len(got.Spec.NodeSelector) != 1 {
		t.Fatalf("node selector = %v", got.Spec.NodeSelector)
	}
	r := got.Spec.Containers[0].Resources
	gpu := resource.MustParse("1")
	if q := r.Requests["nvidia.com/gpu"]; q.Cmp(gpu) != 0 {
		t.Fatalf("gpu request = %v", q)
	}
	if q := r.Limits["nvidia.com/gpu"]; q.Cmp(gpu) != 0 {
		t.Fatalf("gpu limit = %v", q)
	}
}

func TestEnvironmentPodRefuses(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mutate   func(*Backend, *environmentPodInput, *environmentPodProfile)
		mismatch bool
	}{
		{"unbound target", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { b.binding = "" }, true},
		{"invalid namespace", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { b.namespace = "" }, false},
		{"invalid owner", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { b.owner = "" }, false},
		{"invalid input", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { in.id = "x" }, false},
		{"invalid profile", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { p.mountPath = "/" }, false},
		{"other principal", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) { p.principal = "bob" }, true},
		{"image differs", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.Image = "registry.example.com/env/other@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		}, false},
		{"cpu over policy", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.CPULimitMillis = 4001
		}, false},
		{"memory over policy", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.MemoryLimitBytes = 8<<30 + 1
		}, false},
		{"selector not allowed", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.NodeSelector = map[string]string{"pool": "tpu"}
		}, false},
		{"selector key not allowed", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.NodeSelector = map[string]string{"zone": "a"}
		}, false},
		{"unknown accelerator", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.Accelerator, in.task.AcceleratorCount = "amd", 1
		}, false},
		{"accelerator without selector", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
		}, false},
		{"accelerator without count", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.Accelerator = "nvidia"
			in.task.NodeSelector = map[string]string{"pool": "gpu"}
		}, false},
		{"accelerators over policy", func(b *Backend, in *environmentPodInput, p *environmentPodProfile) {
			in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 3
			in.task.NodeSelector = map[string]string{"pool": "gpu"}
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := environmentTestBackend(t)
			in, p := environmentTestInput(), environmentTestProfile()
			tt.mutate(b, &in, &p)
			pod, err := b.environmentPod(in, p)
			if err == nil || pod != nil {
				t.Fatalf("environmentPod = %v, %v", pod, err)
			}
			if errors.Is(err, backend.ErrIdentityMismatch) != tt.mismatch {
				t.Fatalf("error = %v, mismatch want %v", err, tt.mismatch)
			}
		})
	}
}

// TestEnvironmentPodIsolation checks that the Pod shares no memory with its
// input, profile or Backend policy.
func TestEnvironmentPodIsolation(t *testing.T) {
	b := environmentTestBackend(t)
	in, p := environmentTestInput(), environmentTestProfile()
	in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
	in.task.NodeSelector = map[string]string{"pool": "gpu"}
	policy := ResourcePolicy{
		MaxCPUMillis: b.policy.MaxCPUMillis, MaxMemoryBytes: b.policy.MaxMemoryBytes,
		MaxAccelerators: b.policy.MaxAccelerators,
		NodeSelectors:   map[string][]string{"pool": {"cpu", "gpu"}},
		Accelerators: map[string]AcceleratorPolicy{"nvidia": {ResourceName: "nvidia.com/gpu",
			RequiredNodeSelector: map[string]string{"pool": "gpu"}}},
	}

	pod, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	want := pod.DeepCopy()

	// Changing the input and profile does not change the Pod.
	in.task.NodeSelector["pool"] = "cpu"
	p.idleArgv[0] = "/bin/sh"
	p.idleArgv[1] = "-c"
	p.pullSecrets[0] = "other"
	if !equality.Semantic.DeepEqual(pod, want) {
		t.Fatal("pod aliases its input or profile")
	}

	// Changing the Pod does not change the input, profile or policy.
	in, p = environmentTestInput(), environmentTestProfile()
	in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
	in.task.NodeSelector = map[string]string{"pool": "gpu"}
	pod, err = b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	pod.Spec.NodeSelector["pool"] = "cpu"
	pod.Spec.Containers[0].Command[0] = "/bin/sh"
	pod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("4")
	pod.Spec.Tolerations[0].Key = "x"
	*pod.Spec.Tolerations[1].TolerationSeconds = 1
	pod.Labels[ownerLabel] = "other"
	pod.Spec.ImagePullSecrets[0].Name = "other"
	if in.task.NodeSelector["pool"] != "gpu" || p.idleArgv[0] != "/usr/local/bin/idle" || p.pullSecrets[0] != "registry-b" {
		t.Fatal("pod aliases its input or profile")
	}
	if !equality.Semantic.DeepEqual(b.policy, policy) || b.owner != "owner" {
		t.Fatal("pod aliases the backend")
	}
	again, err := b.environmentPod(in, p)
	if err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(again, want) {
		t.Fatal("builder reuses state across calls")
	}
}
