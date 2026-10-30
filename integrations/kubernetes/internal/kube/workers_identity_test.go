package kube

import (
	"errors"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const workerUID = types.UID("a3219392-688c-4e60-8ce1-680a8956a01f")

// workerServerPod models the Create response for want. It applies exactly the
// additions of the server dry-run of the explicit fixed profile retained at
// S10/pod-defaults-native-c2 (and, for the fields that profile does not set
// explicitly, S10/pod-defaults-native-c1): the default container termination
// message path and policy, an empty pod security context, the default service
// account under both names, and server-owned metadata and status. It is a
// modeled fixture, not a native observation.
func workerServerPod(want *corev1.Pod) *corev1.Pod {
	got := want.DeepCopy()
	got.UID = workerUID
	got.Generation = 1
	got.CreationTimestamp = metav1.Unix(1792612228, 0)
	for i := range got.Spec.Containers {
		got.Spec.Containers[i].TerminationMessagePath = corev1.TerminationMessagePathDefault
		got.Spec.Containers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	got.Spec.SecurityContext = &corev1.PodSecurityContext{}
	got.Spec.ServiceAccountName = "default"
	got.Spec.DeprecatedServiceAccount = "default"
	got.Status = corev1.PodStatus{Phase: corev1.PodPending, QOSClass: corev1.PodQOSBurstable}
	return got
}

func TestWorkerPodMatchesDefaults(t *testing.T) {
	b := workerBackend()
	for _, mode := range []workerPodMode{workerIndependent, workerTCP} {
		for rank := range uint32(3) {
			p := workerPlan(rank, 3, mode)
			want := mustWorkerPod(t, b, p)
			exact := want.DeepCopy()
			exact.UID = workerUID
			for name, got := range map[string]*corev1.Pod{"exact": exact, "server": workerServerPod(want)} {
				if err := b.workerPodMatches(got, p, workerUID); err != nil {
					t.Errorf("mode %d rank %d %s: %v", mode, rank, name, err)
				}
			}
		}
	}
}

// TestWorkerPodMatchesAllowed lists every accepted difference from the
// expected Pod. Each is a documented API representation, a placement result,
// an unowned annotation or status.
func TestWorkerPodMatchesAllowed(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"server defaults", func(*corev1.Pod) {}},
		{"tolerations reordered", func(p *corev1.Pod) {
			s := p.Spec.Tolerations
			s[0], s[1] = s[1], s[0]
		}},
		{"nil pod security context", func(p *corev1.Pod) { p.Spec.SecurityContext = nil }},
		{"no service account names", func(p *corev1.Pod) {
			p.Spec.ServiceAccountName, p.Spec.DeprecatedServiceAccount = "", ""
		}},
		{"empty termination message fields", func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePath = ""
			p.Spec.Containers[0].TerminationMessagePolicy = ""
		}},
		{"nil shareProcessNamespace", func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = nil }},
		{"nil priority", func(p *corev1.Pod) { p.Spec.Priority = nil }},
		{"scheduled node", func(p *corev1.Pod) { p.Spec.NodeName = "node-1" }},
		{"unowned annotation", func(p *corev1.Pod) { p.Annotations["example.com/note"] = "x" }},
		{"running status", func(p *corev1.Pod) {
			p.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"}
		}},
		{"resource version", func(p *corev1.Pod) { p.ResourceVersion = "42" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			p := workerPlan(0, 2, workerTCP)
			got := workerServerPod(mustWorkerPod(t, b, p))
			tt.mutate(got)
			if err := b.workerPodMatches(got, p, workerUID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWorkerPodMatchesMutations applies one change to the modeled server
// response. Every change must be refused; a comparator that normalizes broadly
// accepts some of them.
func TestWorkerPodMatchesMutations(t *testing.T) {
	seconds := func(v int64) *int64 { return &v }
	yes := true
	for _, tt := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		// Identity metadata.
		{"uid", func(p *corev1.Pod) { p.UID = "b3219392-688c-4e60-8ce1-680a8956a01f" }},
		{"empty uid", func(p *corev1.Pod) { p.UID = "" }},
		{"name", func(p *corev1.Pod) { p.Name = "skygo-" + workerAttempt + "-1" }},
		{"namespace", func(p *corev1.Pod) { p.Namespace = "other" }},
		{"owner label", func(p *corev1.Pod) { p.Labels[ownerLabel] = "other" }},
		{"job label", func(p *corev1.Pod) { p.Labels[workerJobLabel] = workerAttempt }},
		{"attempt label", func(p *corev1.Pod) { p.Labels[workerAttemptLabel] = workerJob }},
		{"rank label", func(p *corev1.Pod) { p.Labels[workerRankLabel] = "1" }},
		{"world label", func(p *corev1.Pod) { p.Labels[workerWorldLabel] = "3" }},
		{"missing label", func(p *corev1.Pod) { delete(p.Labels, workerRankLabel) }},
		{"extra label", func(p *corev1.Pod) { p.Labels["app"] = "x" }},
		{"id label", func(p *corev1.Pod) { p.Labels[idLabel] = workerJob }},
		{"owner reference", func(p *corev1.Pod) {
			p.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "x", UID: "u"}}
		}},
		{"task hash", func(p *corev1.Pod) { p.Annotations[taskAnnotation] = profileHash }},
		{"binding", func(p *corev1.Pod) { p.Annotations[bindingAnnotation] = "sha256:other" }},
		{"group digest", func(p *corev1.Pod) { p.Annotations[workerGroupAnnotation] = profileHash }},
		{"profile version", func(p *corev1.Pod) { p.Annotations[workerProfileAnnotation] = "skygo-worker-pod-v0" }},
		{"missing group digest", func(p *corev1.Pod) { delete(p.Annotations, workerGroupAnnotation) }},
		{"extra skygo annotation", func(p *corev1.Pod) { p.Annotations["skygo.tmc.dev/extra"] = "x" }},
		{"data layout annotation", func(p *corev1.Pod) { p.Annotations[layoutAnnotation] = "x" }},
		{"apparmor annotation", func(p *corev1.Pod) {
			p.Annotations["container.apparmor.security.beta.kubernetes.io/task"] = "unconfined"
		}},
		{"missing finalizer", func(p *corev1.Pod) { p.Finalizers = nil }},
		{"other finalizer", func(p *corev1.Pod) { p.Finalizers = []string{"example.com/other"} }},
		{"extra finalizer", func(p *corev1.Pod) { p.Finalizers = append(p.Finalizers, "example.com/other") }},
		{"duplicate finalizer", func(p *corev1.Pod) { p.Finalizers = append(p.Finalizers, workerFinalizer) }},

		// Fixed profile fields; the first is the server default for a Pod
		// built without the profile.
		{"service links", func(p *corev1.Pod) { p.Spec.EnableServiceLinks = &yes }},
		{"nil service links", func(p *corev1.Pod) { p.Spec.EnableServiceLinks = nil }},
		{"third toleration", func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{
				Key: "node.kubernetes.io/memory-pressure", Operator: corev1.TolerationOpExists,
				Effect: corev1.TaintEffectNoSchedule})
		}},
		{"tolerate everything", func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
		}},
		{"one toleration", func(p *corev1.Pod) { p.Spec.Tolerations = p.Spec.Tolerations[:1] }},
		{"no tolerations", func(p *corev1.Pod) { p.Spec.Tolerations = nil }},
		{"duplicated toleration", func(p *corev1.Pod) {
			p.Spec.Tolerations = []corev1.Toleration{p.Spec.Tolerations[0], p.Spec.Tolerations[0]}
		}},
		{"toleration seconds", func(p *corev1.Pod) { p.Spec.Tolerations[0].TolerationSeconds = seconds(301) }},
		{"toleration forever", func(p *corev1.Pod) { p.Spec.Tolerations[1].TolerationSeconds = nil }},
		{"toleration effect", func(p *corev1.Pod) { p.Spec.Tolerations[0].Effect = corev1.TaintEffectNoSchedule }},
		{"priority", func(p *corev1.Pod) { v := int32(1000); p.Spec.Priority = &v }},
		{"priority class", func(p *corev1.Pod) { p.Spec.PriorityClassName = "high" }},
		{"preemption never", func(p *corev1.Pod) { v := corev1.PreemptNever; p.Spec.PreemptionPolicy = &v }},
		{"nil preemption", func(p *corev1.Pod) { p.Spec.PreemptionPolicy = nil }},
		{"dns policy", func(p *corev1.Pod) { p.Spec.DNSPolicy = corev1.DNSDefault }},
		{"scheduler", func(p *corev1.Pod) { p.Spec.SchedulerName = "other-scheduler" }},
		{"grace", func(p *corev1.Pod) { p.Spec.TerminationGracePeriodSeconds = seconds(0) }},
		{"nil grace", func(p *corev1.Pod) { p.Spec.TerminationGracePeriodSeconds = nil }},
		{"restart", func(p *corev1.Pod) { p.Spec.RestartPolicy = corev1.RestartPolicyOnFailure }},
		{"token", func(p *corev1.Pod) { p.Spec.AutomountServiceAccountToken = &yes }},
		{"service account", func(p *corev1.Pod) {
			p.Spec.ServiceAccountName, p.Spec.DeprecatedServiceAccount = "builder", "builder"
		}},
		{"share process namespace", func(p *corev1.Pod) { p.Spec.ShareProcessNamespace = &yes }},
		{"deadline", func(p *corev1.Pod) { p.Spec.ActiveDeadlineSeconds = seconds(61) }},
		{"no deadline", func(p *corev1.Pod) { p.Spec.ActiveDeadlineSeconds = nil }},
		{"node selector", func(p *corev1.Pod) { p.Spec.NodeSelector["zone"] = "a" }},
		{"affinity", func(p *corev1.Pod) {
			p.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
		}},
		{"image pull secret", func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "regcred"}}
		}},
		{"pull secrets cleared", func(p *corev1.Pod) { p.Spec.ImagePullSecrets = nil }},
		{"pull secrets reordered", func(p *corev1.Pod) {
			s := p.Spec.ImagePullSecrets
			s[0], s[1] = s[1], s[0]
		}},
		{"pull secret added", func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: "other"})
		}},
		{"pull secret renamed", func(p *corev1.Pod) { p.Spec.ImagePullSecrets[1].Name = "other" }},
		{"pull secret duplicated", func(p *corev1.Pod) {
			p.Spec.ImagePullSecrets = append(p.Spec.ImagePullSecrets, p.Spec.ImagePullSecrets[0])
		}},
		{"unrequested gpu toleration", func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, workerGPUToleration())
		}},
		{"gpu toleration twice", func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, workerGPUToleration(), workerGPUToleration())
		}},
		{"gpu toleration NoExecute", func(p *corev1.Pod) {
			t := workerGPUToleration()
			t.Effect = corev1.TaintEffectNoExecute
			p.Spec.Tolerations = append(p.Spec.Tolerations, t)
		}},
		{"gpu toleration seconds", func(p *corev1.Pod) {
			t := workerGPUToleration()
			t.TolerationSeconds = seconds(60)
			p.Spec.Tolerations = append(p.Spec.Tolerations, t)
		}},
		{"gpu toleration value", func(p *corev1.Pod) {
			t := workerGPUToleration()
			t.Operator, t.Value = corev1.TolerationOpEqual, "present"
			p.Spec.Tolerations = append(p.Spec.Tolerations, t)
		}},
		{"unrequested resource toleration", func(p *corev1.Pod) {
			t := workerGPUToleration()
			t.Key = "example.com/fpga"
			p.Spec.Tolerations = append(p.Spec.Tolerations, t)
		}},
		{"host network", func(p *corev1.Pod) { p.Spec.HostNetwork = true }},
		{"pod security context", func(p *corev1.Pod) {
			v := int64(0)
			p.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &v}
		}},
		{"runtime class", func(p *corev1.Pod) { v := "kata"; p.Spec.RuntimeClassName = &v }},
		{"volume", func(p *corev1.Pod) {
			p.Spec.Volumes = []corev1.Volume{{Name: "x", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}}
		}},
		{"init container", func(p *corev1.Pod) {
			p.Spec.InitContainers = []corev1.Container{{Name: "init", Image: profileImage}}
		}},
		{"sidecar", func(p *corev1.Pod) {
			p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "proxy", Image: profileImage})
		}},
		{"missing gate", func(p *corev1.Pod) { p.Spec.SchedulingGates = nil }},
		{"empty gates", func(p *corev1.Pod) { p.Spec.SchedulingGates = []corev1.PodSchedulingGate{} }},
		{"extra gate", func(p *corev1.Pod) {
			p.Spec.SchedulingGates = append(p.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: "example.com/hold"})
		}},
		{"extra gate first", func(p *corev1.Pod) {
			p.Spec.SchedulingGates = append([]corev1.PodSchedulingGate{{Name: "example.com/hold"}}, p.Spec.SchedulingGates...)
		}},
		{"duplicate gate", func(p *corev1.Pod) {
			p.Spec.SchedulingGates = append(p.Spec.SchedulingGates, p.Spec.SchedulingGates[0])
		}},
		{"renamed gate", func(p *corev1.Pod) { p.Spec.SchedulingGates[0].Name = "skygo.tmc.dev/worker" }},

		// The task container.
		{"image", func(p *corev1.Pod) { p.Spec.Containers[0].Image = profileHelp }},
		{"command", func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"sh"} }},
		{"args", func(p *corev1.Pod) { p.Spec.Containers[0].Args = []string{"-c"} }},
		{"working dir", func(p *corev1.Pod) { p.Spec.Containers[0].WorkingDir = "/" }},
		{"image pull always", func(p *corev1.Pod) { p.Spec.Containers[0].ImagePullPolicy = corev1.PullAlways }},
		{"image pull empty", func(p *corev1.Pod) { p.Spec.Containers[0].ImagePullPolicy = "" }},
		{"resize policy", func(p *corev1.Pod) {
			p.Spec.Containers[0].ResizePolicy = []corev1.ContainerResizePolicy{
				{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired}}
		}},
		{"termination message path", func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePath = "/etc/passwd"
		}},
		{"termination message policy", func(p *corev1.Pod) {
			p.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}},
		{"container security", func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: &yes}
		}},
		{"volume mount", func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "x", MountPath: "/x"}}
		}},
		{"cpu request", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("600m")
		}},
		{"memory limit", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("3Gi")
		}},
		{"extra resource", func(p *corev1.Pod) {
			p.Spec.Containers[0].Resources.Limits["example.com/fpga"] = resource.MustParse("1")
		}},
		{"rank env", func(p *corev1.Pod) { workerSetEnv(p, "RANK", "1") }},
		{"world env", func(p *corev1.Pod) { workerSetEnv(p, "WORLD_SIZE", "3") }},
		{"master addr env", func(p *corev1.Pod) { workerSetEnv(p, "MASTER_ADDR", "evil.example") }},
		{"master port env", func(p *corev1.Pod) { workerSetEnv(p, "MASTER_PORT", "1") }},
		{"task env", func(p *corev1.Pod) { workerSetEnv(p, "A", "9") }},
		{"injected env", func(p *corev1.Pod) {
			c := &p.Spec.Containers[0]
			c.Env = append(c.Env, corev1.EnvVar{Name: "ZZZ", Value: "x"})
		}},
		{"env from secret", func(p *corev1.Pod) {
			c := &p.Spec.Containers[0]
			c.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "s"}}}}
		}},
		{"no readiness", func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe = nil }},
		{"readiness port", func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe.TCPSocket.Port = intstr.FromInt32(29501)
		}},
		{"liveness", func(p *corev1.Pod) {
			p.Spec.Containers[0].LivenessProbe = p.Spec.Containers[0].ReadinessProbe.DeepCopy()
		}},
		{"container port", func(p *corev1.Pod) {
			p.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 29500, HostPort: 29500}}
		}},
		{"stdin", func(p *corev1.Pod) { p.Spec.Containers[0].Stdin = true }},
		{"container name", func(p *corev1.Pod) { p.Spec.Containers[0].Name = "main" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			p := workerPlan(0, 2, workerTCP)
			got := workerServerPod(mustWorkerPod(t, b, p))
			if err := b.workerPodMatches(got, p, workerUID); err != nil {
				t.Fatalf("unmutated: %v", err)
			}
			tt.mutate(got)
			err := b.workerPodMatches(got, p, workerUID)
			if !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("workerPodMatches = %v, want ErrIdentityMismatch", err)
			}
		})
	}
}

// workerSetEnv sets the value of name in the task container, which must have it.
func workerSetEnv(p *corev1.Pod, name, value string) {
	env := p.Spec.Containers[0].Env
	for i := range env {
		if env[i].Name == name {
			env[i].Value = value
			return
		}
	}
	panic("no env " + name)
}

// TestWorkerPodMatchesPlan checks an unchanged Pod against other plans and
// receivers: the Pod is not the original of any of them.
func TestWorkerPodMatchesPlan(t *testing.T) {
	b := workerBackend()
	p := workerPlan(1, 3, workerTCP)
	got := workerServerPod(mustWorkerPod(t, b, p))
	for _, tt := range []struct {
		name    string
		backend func(*Backend)
		plan    func(*workerPodPlan)
	}{
		{name: "rank", plan: func(p *workerPodPlan) { p.Rank = 2 }},
		{name: "count", plan: func(p *workerPodPlan) { p.Count = 4 }},
		{name: "mode", plan: func(p *workerPodPlan) { p.Mode = workerIndependent }},
		{name: "job", plan: func(p *workerPodPlan) { p.JobID = workerAttempt }},
		{name: "attempt", plan: func(p *workerPodPlan) { p.AttemptID = workerJob }},
		{name: "task argv", plan: func(p *workerPodPlan) { p.Task.Argv = []string{"python", "eval.py"} }},
		{name: "task env", plan: func(p *workerPodPlan) { p.Task.Env["C"] = "3" }},
		{name: "task timeout", plan: func(p *workerPodPlan) { p.Task.TimeoutSeconds = 61 }},
		{name: "task target", plan: func(p *workerPodPlan) { p.Task.Target = "other" }},
		{name: "pull secrets", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = []string{"regcred"} }},
		{name: "pull secret order", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = []string{"mirror", "regcred"} }},
		{name: "no pull secrets", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = nil }},
		{name: "owner", backend: func(b *Backend) { b.owner = "other" }},
		{name: "binding", backend: func(b *Backend) { b.binding = "sha256:other" }},
		{name: "namespace", backend: func(b *Backend) { b.namespace = "other" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			if tt.backend != nil {
				tt.backend(b)
			}
			q := workerPlan(1, 3, workerTCP)
			if tt.plan != nil {
				tt.plan(&q)
			}
			if _, err := b.workerPod(q); err != nil {
				t.Fatalf("plan is invalid: %v", err)
			}
			if err := b.workerPodMatches(got, q, workerUID); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("workerPodMatches = %v, want ErrIdentityMismatch", err)
			}
		})
	}
}

func TestWorkerPodMatchesOriginalUID(t *testing.T) {
	b := workerBackend()
	p := workerPlan(0, 2, workerIndependent)
	got := workerServerPod(mustWorkerPod(t, b, p))
	for _, tt := range []struct {
		name string
		uid  types.UID
	}{
		{"empty", ""},
		{"foreign", "b3219392-688c-4e60-8ce1-680a8956a01f"},
		{"control byte", "a3219392\n"},
		{"too long", types.UID(make([]byte, 129))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := b.workerPodMatches(got, p, tt.uid); !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("workerPodMatches = %v, want ErrIdentityMismatch", err)
			}
		})
	}
	if err := b.workerPodMatches(got, p, workerUID); err != nil {
		t.Fatalf("original uid: %v", err)
	}
	// An observed Pod without a UID never matches an unknown original.
	got.UID = ""
	if err := b.workerPodMatches(got, p, ""); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("empty uids = %v, want ErrIdentityMismatch", err)
	}
	if err := b.workerPodMatches(nil, p, workerUID); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("nil pod = %v, want ErrIdentityMismatch", err)
	}
}

func TestWorkerPodMatchesRefusedPlan(t *testing.T) {
	b := workerBackend()
	p := workerPlan(0, 2, workerTCP)
	got := workerServerPod(mustWorkerPod(t, b, p))
	p.Task.Setup = []string{"make"}
	if err := b.workerPodMatches(got, p, workerUID); err == nil {
		t.Fatal("workerPodMatches accepted a data plan")
	}
}

func TestWorkerPodMatchesNoMutation(t *testing.T) {
	b := workerBackend()
	p := workerPlan(0, 2, workerTCP)
	raw, err := backend.TaskBytes(p.Task)
	if err != nil {
		t.Fatal(err)
	}
	got := workerServerPod(mustWorkerPod(t, b, p))
	got.Spec.NodeName = "node-1"
	s := got.Spec.Tolerations
	s[0], s[1] = s[1], s[0]
	before := got.DeepCopy()
	if err := b.workerPodMatches(got, p, workerUID); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(got, before) || got.Spec.NodeName != "node-1" ||
		got.Spec.ServiceAccountName != "default" {
		t.Fatal("workerPodMatches modified got")
	}
	after, err := backend.TaskBytes(p.Task)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != string(after) {
		t.Fatal("workerPodMatches modified the task")
	}
}

// workerGPUToleration is a GPU toleration that a CPU-only worker must refuse.
func workerGPUToleration() corev1.Toleration {
	return corev1.Toleration{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists,
		Effect: corev1.TaintEffectNoSchedule}
}

// TestWorkerPodMatchesServiceAccountUID documents that the snapshot's service
// account UID is profile provenance kept by the caller, not a Pod field: a
// plan that differs only in that UID describes the same Pod.
func TestWorkerPodMatchesServiceAccountUID(t *testing.T) {
	b := workerBackend()
	p := workerPlan(0, 2, workerTCP)
	got := workerServerPod(mustWorkerPod(t, b, p))
	p.Registry.ServiceAccountUID = "6a1d7b3f-2e8c-4054-8b62-1d3e8c2f9b01"
	if err := b.workerPodMatches(got, p, workerUID); err != nil {
		t.Fatal(err)
	}
	p.Registry.ServiceAccountUID = ""
	if err := b.workerPodMatches(got, p, workerUID); err == nil {
		t.Fatal("workerPodMatches accepted a plan without a service account uid")
	}
}
