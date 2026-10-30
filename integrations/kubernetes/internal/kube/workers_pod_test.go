package kube

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	workerJob     = "0123456789abcdef0123456789abcdef"
	workerAttempt = "fedcba9876543210fedcba9876543210"
)

// workerBackend returns a bound Backend fixture with a resource policy. It is
// not a qualified target.
func workerBackend() *Backend {
	return &Backend{
		namespace: "jobs", owner: "owner", binding: "sha256:binding",
		policy: ResourcePolicy{
			MaxCPUMillis: 4000, MaxMemoryBytes: 8 << 30,
			NodeSelectors: map[string][]string{"pool": {"cpu"}},
		},
	}
}

func workerTask() backend.Task {
	return backend.Task{
		Argv: []string{"python", "train.py"}, Image: profileImage, Directory: "/app",
		Env:       map[string]string{"B": "2", "A": "1"},
		CPUMillis: 500, MemoryBytes: 1 << 30, CPULimitMillis: 1000, MemoryLimitBytes: 2 << 30,
		NodeSelector: map[string]string{"pool": "cpu"}, TimeoutSeconds: 60,
	}
}

// workerRegistrySnapshot returns a registry snapshot fixture whose names are
// deliberately out of sorted order.
func workerRegistrySnapshot() workerRegistry {
	return workerRegistry{ServiceAccountUID: "5f0c6a2e-1d7b-4f43-9a51-0c2d7b1e8a90",
		PullSecrets: []string{"regcred", "mirror"}}
}

func workerPlan(rank, count uint32, mode workerPodMode) workerPodPlan {
	return workerPodPlan{JobID: workerJob, AttemptID: workerAttempt,
		Rank: rank, Count: count, Task: workerTask(), Mode: mode, Registry: workerRegistrySnapshot()}
}

func mustWorkerPod(t *testing.T, b *Backend, p workerPodPlan) *corev1.Pod {
	t.Helper()
	pod, err := b.workerPod(p)
	if err != nil {
		t.Fatalf("workerPod(rank %d): %v", p.Rank, err)
	}
	return pod
}

func workerEnv(c corev1.Container) map[string]string {
	m := make(map[string]string)
	for _, e := range c.Env {
		if e.ValueFrom != nil {
			m[e.Name] = "<valueFrom>"
			continue
		}
		m[e.Name] = e.Value
	}
	return m
}

func TestWorkerPodRanks(t *testing.T) {
	b := workerBackend()
	task := workerTask()
	raw, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	group := workerGroupDigest(raw, 3, workerTCP)
	names := make(map[string]bool)
	for rank := range uint32(3) {
		p := workerPlan(rank, 3, workerTCP)
		pod := mustWorkerPod(t, b, p)
		if again := mustWorkerPod(t, b, p); !reflect.DeepEqual(pod, again) {
			t.Errorf("rank %d: workerPod is not deterministic", rank)
		}
		wantName := "skygo-" + workerAttempt + "-" + strconv.Itoa(int(rank))
		if pod.Name != wantName || pod.Namespace != "jobs" {
			t.Errorf("rank %d: name %s/%s, want jobs/%s", rank, pod.Namespace, pod.Name, wantName)
		}
		if names[pod.Name] {
			t.Errorf("rank %d: duplicate name %s", rank, pod.Name)
		}
		names[pod.Name] = true
		wantLabels := map[string]string{
			ownerLabel: "owner", workerJobLabel: workerJob, workerAttemptLabel: workerAttempt,
			workerRankLabel: strconv.Itoa(int(rank)), workerWorldLabel: "3",
		}
		if !reflect.DeepEqual(pod.Labels, wantLabels) {
			t.Errorf("rank %d: labels %v, want %v", rank, pod.Labels, wantLabels)
		}
		wantAnnotations := map[string]string{
			taskAnnotation: hash, bindingAnnotation: "sha256:binding",
			workerGroupAnnotation: group, workerProfileAnnotation: workerProfileVersion,
		}
		if !reflect.DeepEqual(pod.Annotations, wantAnnotations) {
			t.Errorf("rank %d: annotations %v, want %v", rank, pod.Annotations, wantAnnotations)
		}
		if !reflect.DeepEqual(pod.Finalizers, []string{workerFinalizer}) {
			t.Errorf("rank %d: finalizers %v", rank, pod.Finalizers)
		}
		if want := []corev1.PodSchedulingGate{{Name: workerSchedulingGate}}; !reflect.DeepEqual(pod.Spec.SchedulingGates, want) {
			t.Errorf("rank %d: scheduling gates %v, want %v", rank, pod.Spec.SchedulingGates, want)
		}
		if len(pod.OwnerReferences) != 0 || pod.UID != "" || pod.GenerateName != "" {
			t.Errorf("rank %d: unexpected owner, uid or generated name", rank)
		}
		if len(pod.Spec.Containers) != 1 || len(pod.Spec.InitContainers) != 0 ||
			len(pod.Spec.EphemeralContainers) != 0 || len(pod.Spec.Volumes) != 0 {
			t.Fatalf("rank %d: want exactly one container and no volumes", rank)
		}
		c := pod.Spec.Containers[0]
		wantEnv := map[string]string{
			"A": "1", "B": "2",
			"RANK": strconv.Itoa(int(rank)), "WORLD_SIZE": "3",
			"SKYGO_JOB_ID": workerJob, "SKYGO_ATTEMPT_ID": workerAttempt,
			"MASTER_ADDR": "skygo-" + workerAttempt + ".jobs.svc", "MASTER_PORT": "29500",
		}
		if got := workerEnv(c); !reflect.DeepEqual(got, wantEnv) {
			t.Errorf("rank %d: env %v, want %v", rank, got, wantEnv)
		}
		for i := 1; i < len(c.Env); i++ {
			if c.Env[i-1].Name >= c.Env[i].Name {
				t.Errorf("rank %d: env is not sorted: %v", rank, c.Env)
			}
		}
		if c.LivenessProbe != nil || c.StartupProbe != nil || c.Lifecycle != nil {
			t.Errorf("rank %d: unexpected liveness, startup probe or lifecycle", rank)
		}
		if rank == 0 {
			want := &corev1.Probe{
				ProbeHandler:   corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(29500)}},
				TimeoutSeconds: 1, PeriodSeconds: 10, SuccessThreshold: 1, FailureThreshold: 3,
			}
			if !reflect.DeepEqual(c.ReadinessProbe, want) {
				t.Errorf("rank 0: readiness %+v, want %+v", c.ReadinessProbe, want)
			}
		} else if c.ReadinessProbe != nil {
			t.Errorf("rank %d: readiness probe on a nonzero rank", rank)
		}
		if a := pod.Annotations[workerGroupAnnotation]; a != group {
			t.Errorf("rank %d: group %s, want %s", rank, a, group)
		}
	}
}

func TestWorkerPodProfile(t *testing.T) {
	b := workerBackend()
	pod := mustWorkerPod(t, b, workerPlan(1, 2, workerIndependent))
	s := pod.Spec
	no, grace, deadline := false, int64(30), int64(60)
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	seconds := int64(300)
	want := corev1.PodSpec{
		AutomountServiceAccountToken:  &no,
		EnableServiceLinks:            &no,
		ShareProcessNamespace:         &no,
		RestartPolicy:                 corev1.RestartPolicyNever,
		DNSPolicy:                     corev1.DNSClusterFirst,
		SchedulerName:                 "default-scheduler",
		TerminationGracePeriodSeconds: &grace,
		ActiveDeadlineSeconds:         &deadline,
		Priority:                      &priority,
		PreemptionPolicy:              &preempt,
		NodeSelector:                  map[string]string{"pool": "cpu"},
		SchedulingGates:               []corev1.PodSchedulingGate{{Name: "skygo.tmc.dev/worker-activation"}},
		ImagePullSecrets:              []corev1.LocalObjectReference{{Name: "regcred"}, {Name: "mirror"}},
		Tolerations: []corev1.Toleration{
			{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists,
				Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
			{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists,
				Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		},
		Containers: []corev1.Container{{
			Name: "task", Image: profileImage, Command: []string{"python", "train.py"},
			WorkingDir: "/app", ImagePullPolicy: corev1.PullIfNotPresent,
			Env: []corev1.EnvVar{
				{Name: "A", Value: "1"}, {Name: "B", Value: "2"},
				{Name: "RANK", Value: "1"},
				{Name: "SKYGO_ATTEMPT_ID", Value: workerAttempt},
				{Name: "SKYGO_JOB_ID", Value: workerJob},
				{Name: "WORLD_SIZE", Value: "2"},
			},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("1"),
					corev1.ResourceMemory: resource.MustParse("2Gi"),
				},
			},
		}},
	}
	if !equality.Semantic.DeepEqual(s, want) {
		t.Errorf("spec =\n%+v\nwant\n%+v", s, want)
	}
	if s.PriorityClassName != "" || s.ServiceAccountName != "" || s.DeprecatedServiceAccount != "" ||
		s.SecurityContext != nil || s.Affinity != nil || s.NodeName != "" ||
		s.HostNetwork || s.HostPID || s.HostIPC || s.RuntimeClassName != nil {
		t.Errorf("spec sets an unexpected scheduling, identity or host field: %+v", s)
	}
}

// TestWorkerSingleUnchanged checks that a single worker keeps the ordinary
// Pod spec, with no scheduling gate or worker profile, and is not a worker
// group.
func TestWorkerSingleUnchanged(t *testing.T) {
	b := workerBackend()
	ordinary := b.podSpec(workerTask())
	if ordinary.SchedulingGates != nil || ordinary.EnableServiceLinks != nil || ordinary.Tolerations != nil ||
		ordinary.Priority != nil || ordinary.PreemptionPolicy != nil || ordinary.TerminationGracePeriodSeconds != nil ||
		ordinary.Containers[0].ImagePullPolicy != "" || len(ordinary.Containers[0].Env) != 2 {
		t.Errorf("ordinary podSpec changed: %+v", ordinary)
	}
	for _, count := range []uint32{0, 1} {
		if pod, err := b.workerPod(workerPlan(0, count, workerIndependent)); err == nil || pod != nil {
			t.Errorf("count %d: workerPod = %v, %v; want refusal", count, pod, err)
		}
	}
}

func TestWorkerPodIndependent(t *testing.T) {
	b := workerBackend()
	for rank := range uint32(2) {
		pod := mustWorkerPod(t, b, workerPlan(rank, 2, workerIndependent))
		c := pod.Spec.Containers[0]
		env := workerEnv(c)
		if _, ok := env["MASTER_ADDR"]; ok {
			t.Errorf("rank %d: independent worker has MASTER_ADDR", rank)
		}
		if _, ok := env["MASTER_PORT"]; ok {
			t.Errorf("rank %d: independent worker has MASTER_PORT", rank)
		}
		if c.ReadinessProbe != nil {
			t.Errorf("rank %d: independent worker has a readiness probe", rank)
		}
	}
}

func TestWorkerPodTaskUnchanged(t *testing.T) {
	b := workerBackend()
	p := workerPlan(0, 2, workerTCP)
	before, err := backend.TaskBytes(p.Task)
	if err != nil {
		t.Fatal(err)
	}
	pod := mustWorkerPod(t, b, p)
	after, err := backend.TaskBytes(p.Task)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("TaskBytes changed:\n%s\n%s", before, after)
	}
	if len(p.Task.Env) != 2 {
		t.Fatalf("task env changed: %v", p.Task.Env)
	}

	// The Pod shares no slice or map with the task.
	c := &pod.Spec.Containers[0]
	c.Command[0] = "changed"
	c.Env[0].Value = "changed"
	pod.Spec.NodeSelector["pool"] = "changed"
	*pod.Spec.ActiveDeadlineSeconds = 1
	after, err = backend.TaskBytes(p.Task)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("Pod aliases task:\n%s\n%s", before, after)
	}

	// Pods of one plan share nothing either.
	x := mustWorkerPod(t, b, p)
	y := mustWorkerPod(t, b, p)
	x.Labels[ownerLabel] = "changed"
	x.Annotations[taskAnnotation] = "changed"
	x.Finalizers[0] = "changed"
	*x.Spec.Tolerations[0].TolerationSeconds = 1
	*x.Spec.EnableServiceLinks = true
	if y.Labels[ownerLabel] != "owner" || y.Annotations[taskAnnotation] == "changed" ||
		y.Finalizers[0] != workerFinalizer || *y.Spec.Tolerations[0].TolerationSeconds != 300 ||
		*y.Spec.EnableServiceLinks {
		t.Fatal("worker Pods share mutable state")
	}
}

func TestWorkerGroupDigest(t *testing.T) {
	// Vectors computed independently from the documented encoding.
	for _, tt := range []struct {
		raw   string
		count uint32
		mode  workerPodMode
		want  string
	}{
		{`{"argv":["x"]}`, 2, workerIndependent, "bde4467ce0c4f136c3cd254f02118b77812e9c54e85989a52143d7ae785f4bee"},
		{`{"argv":["x"]}`, 16, workerTCP, "c00a983f90356aa64ef9e3319d5634744b81402c1bd5a3464d082a2315ff478b"},
		{``, 2, workerIndependent, "1ba85706cd04a3aec59a94640657b9198bce2add21e6a5bcd31458e891c2f18b"},
	} {
		if got := workerGroupDigest([]byte(tt.raw), tt.count, tt.mode); got != tt.want {
			t.Errorf("workerGroupDigest(%q, %d, %d) = %s, want %s", tt.raw, tt.count, tt.mode, got, tt.want)
		}
	}

	b := workerBackend()
	group := func(p workerPodPlan) string {
		t.Helper()
		return mustWorkerPod(t, b, p).Annotations[workerGroupAnnotation]
	}
	base := group(workerPlan(0, 3, workerTCP))
	other := workerPlan(2, 3, workerTCP)
	other.JobID, other.AttemptID = workerAttempt, workerJob
	if got := group(other); got != base {
		t.Errorf("group digest depends on rank or IDs: %s != %s", got, base)
	}
	changed := workerPlan(0, 3, workerTCP)
	changed.Task.Argv = []string{"python", "eval.py"}
	for name, p := range map[string]workerPodPlan{
		"count": workerPlan(0, 4, workerTCP),
		"mode":  workerPlan(0, 3, workerIndependent),
		"task":  changed,
	} {
		if got := group(p); got == base {
			t.Errorf("%s: group digest unchanged", name)
		}
	}
}

func TestWorkerPodRefusals(t *testing.T) {
	for _, tt := range []struct {
		name     string
		backend  func(*Backend)
		plan     func(*workerPodPlan)
		identity bool // error wraps backend.ErrIdentityMismatch
	}{
		{name: "count 0", plan: func(p *workerPodPlan) { p.Count, p.Rank = 0, 0 }},
		{name: "count 1", plan: func(p *workerPodPlan) { p.Count, p.Rank = 1, 0 }},
		{name: "count 17", plan: func(p *workerPodPlan) { p.Count = 17 }},
		{name: "rank equals count", plan: func(p *workerPodPlan) { p.Rank = p.Count }},
		{name: "rank beyond count", plan: func(p *workerPodPlan) { p.Rank = 100 }},
		{name: "mode 0", plan: func(p *workerPodPlan) { p.Mode = 0 }},
		{name: "mode 3", plan: func(p *workerPodPlan) { p.Mode = 3 }},
		{name: "empty job id", plan: func(p *workerPodPlan) { p.JobID = "" }},
		{name: "uppercase job id", plan: func(p *workerPodPlan) { p.JobID = "0123456789ABCDEF0123456789ABCDEF" }},
		{name: "short attempt id", plan: func(p *workerPodPlan) { p.AttemptID = "0123" }},
		{name: "attempt id with suffix", plan: func(p *workerPodPlan) { p.AttemptID = workerAttempt + "-0" }},
		{name: "unbound", backend: func(b *Backend) { b.binding = "" }, identity: true},
		{name: "no namespace", backend: func(b *Backend) { b.namespace = "" }},
		{name: "no owner", backend: func(b *Backend) { b.owner = "" }},
		{name: "workdir", plan: func(p *workerPodPlan) { p.Task.Workdir = profileWorkdir() }},
		{name: "setup", plan: func(p *workerPodPlan) { p.Task.Setup = []string{"make"} }},
		{name: "outputs", plan: func(p *workerPodPlan) { p.Task.Outputs = []string{"out"} }},
		{name: "data mounts", plan: func(p *workerPodPlan) {
			p.Task.DataMounts = []backend.DataMount{{Reference: "models", Destination: "/mnt/skygo/models"}}
		}},
		{name: "retry", plan: func(p *workerPodPlan) {
			p.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}}
		}},
		{name: "env RANK", plan: func(p *workerPodPlan) { p.Task.Env["RANK"] = "0" }},
		{name: "env WORLD_SIZE", plan: func(p *workerPodPlan) { p.Task.Env["WORLD_SIZE"] = "2" }},
		{name: "env SKYGO_JOB_ID", plan: func(p *workerPodPlan) { p.Task.Env["SKYGO_JOB_ID"] = workerJob }},
		{name: "env SKYGO_ATTEMPT_ID", plan: func(p *workerPodPlan) { p.Task.Env["SKYGO_ATTEMPT_ID"] = workerAttempt }},
		{name: "env MASTER_ADDR", plan: func(p *workerPodPlan) { p.Task.Env["MASTER_ADDR"] = "x" }},
		{name: "env MASTER_PORT independent", plan: func(p *workerPodPlan) {
			p.Mode = workerIndependent
			p.Task.Env["MASTER_PORT"] = "1"
		}},
		{name: "no argv", plan: func(p *workerPodPlan) { p.Task.Argv = nil }},
		{name: "no image", plan: func(p *workerPodPlan) { p.Task.Image = "" }},
		{name: "relative directory", plan: func(p *workerPodPlan) { p.Task.Directory = "app" }},
		{name: "invalid env name", plan: func(p *workerPodPlan) { p.Task.Env["1BAD"] = "x" }},
		{name: "cpu over policy", plan: func(p *workerPodPlan) { p.Task.CPUMillis, p.Task.CPULimitMillis = 5000, 0 }},
		{name: "node selector off policy", plan: func(p *workerPodPlan) { p.Task.NodeSelector["pool"] = "other" }},
		{name: "accelerator", plan: func(p *workerPodPlan) { p.Task.Accelerator = "nvidia" }},
		{name: "accelerator count", plan: func(p *workerPodPlan) { p.Task.AcceleratorCount = 1 }},
		{name: "accelerator and count", plan: func(p *workerPodPlan) { p.Task.Accelerator, p.Task.AcceleratorCount = "nvidia", 1 }},
		{name: "no service account uid", plan: func(p *workerPodPlan) { p.Registry.ServiceAccountUID = "" }},
		{name: "service account uid control byte", plan: func(p *workerPodPlan) { p.Registry.ServiceAccountUID = "sa\n" }},
		{name: "service account uid too long", plan: func(p *workerPodPlan) {
			p.Registry.ServiceAccountUID = types.UID(strings.Repeat("a", 129))
		}},
		{name: "33 pull secrets", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = workerSecretNames(33) }},
		{name: "duplicate pull secret", plan: func(p *workerPodPlan) {
			p.Registry.PullSecrets = []string{"regcred", "mirror", "regcred"}
		}},
		{name: "empty pull secret", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = []string{""} }},
		{name: "invalid pull secret", plan: func(p *workerPodPlan) { p.Registry.PullSecrets = []string{"Reg_Cred"} }},
		{name: "long pull secret", plan: func(p *workerPodPlan) {
			p.Registry.PullSecrets = []string{strings.Repeat("a", 254)}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			if tt.backend != nil {
				tt.backend(b)
			}
			p := workerPlan(1, 3, workerTCP)
			if tt.plan != nil {
				tt.plan(&p)
			}
			pod, err := b.workerPod(p)
			if err == nil || pod != nil {
				t.Fatalf("workerPod = %v, %v; want refusal", pod, err)
			}
			if got := errors.Is(err, backend.ErrIdentityMismatch); got != tt.identity {
				t.Errorf("errors.Is(%v, ErrIdentityMismatch) = %v, want %v", err, got, tt.identity)
			}
		})
	}
}

func TestWorkerPodBounds(t *testing.T) {
	b := workerBackend()
	for _, count := range []uint32{2, 16} {
		for _, rank := range []uint32{0, count - 1} {
			for _, mode := range []workerPodMode{workerIndependent, workerTCP} {
				if _, err := b.workerPod(workerPlan(rank, count, mode)); err != nil {
					t.Errorf("count %d rank %d mode %d: %v", count, rank, mode, err)
				}
			}
		}
	}
	p := workerPlan(15, 16, workerTCP)
	if got, want := mustWorkerPod(t, b, p).Name, "skygo-"+workerAttempt+"-15"; got != want {
		t.Errorf("name %s, want %s", got, want)
	}
}

// TestWorkerPodBackendModes keeps the initial worker leaf separate from
// ordinary execution receipts and DRA, even for a task requesting only CPU.
func TestWorkerPodBackendModes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*Backend)
	}{
		{"dra runtime", func(b *Backend) { b.dra = &draRuntime{} }},
		{"dra gate", func(b *Backend) { b.gate = &draGate{} }},
		{"execution receipts", func(b *Backend) { b.receipts = &receiptStore{} }},
		{"unused dra policy", func(b *Backend) {
			b.policy.Accelerators = map[string]AcceleratorPolicy{"gpu": {DRA: &DRAPolicy{}}}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := workerBackend()
			tt.change(b)
			if got, err := b.workerPod(workerPlan(0, 2, workerIndependent)); err == nil || got != nil {
				t.Fatalf("workerPod = %v, %v; want unsupported backend", got, err)
			}
		})
	}
}

func workerSecretNames(n int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("secret-%02d", i)
	}
	return names
}

func TestWorkerPodPullSecrets(t *testing.T) {
	b := workerBackend()
	for _, tt := range []struct {
		name  string
		names []string
	}{
		{"none", nil},
		{"empty", []string{}},
		{"one", []string{"regcred"}},
		{"unsorted", []string{"zeta", "alpha", "mid"}},
		{"32", workerSecretNames(32)},
		{"subdomain", []string{"registry.example.com"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := workerPlan(0, 2, workerTCP)
			p.Registry.PullSecrets = tt.names
			pod := mustWorkerPod(t, b, p)
			var want []corev1.LocalObjectReference
			for _, name := range tt.names {
				want = append(want, corev1.LocalObjectReference{Name: name})
			}
			if !reflect.DeepEqual(pod.Spec.ImagePullSecrets, want) {
				t.Fatalf("ImagePullSecrets = %v, want %v", pod.Spec.ImagePullSecrets, want)
			}
			if err := b.workerPodMatches(workerServerPod(pod), p, workerUID); err != nil {
				t.Fatalf("workerPodMatches: %v", err)
			}
		})
	}

	// The Pod and the snapshot share nothing.
	p := workerPlan(0, 2, workerTCP)
	pod := mustWorkerPod(t, b, p)
	p.Registry.PullSecrets[0] = "changed"
	if pod.Spec.ImagePullSecrets[0].Name != "regcred" {
		t.Fatal("Pod aliases the registry snapshot")
	}
	q := workerPlan(0, 2, workerTCP)
	pod = mustWorkerPod(t, b, q)
	pod.Spec.ImagePullSecrets[0].Name = "changed"
	if q.Registry.PullSecrets[0] != "regcred" {
		t.Fatal("registry snapshot aliases the Pod")
	}
}
