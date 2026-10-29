package kube

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// admitParent changes obj, a parent just created by the loopback API, the
// way a Kubernetes 1.32 API server with its default admission plugins
// persists the object Skygo posts. It is written out field by field from the
// API server's behavior, not from the canonicalization under test.
func admitParent(obj any) {
	switch o := obj.(type) {
	case *batchv1.Job:
		admitJob(o)
	case *corev1.Pod:
		admitPod(o)
	}
}

func admitJob(j *batchv1.Job) {
	uid := string(j.UID)
	j.Spec.CompletionMode = ptr(batchv1.NonIndexedCompletion)
	j.Spec.Suspend = ptr(false)
	j.Spec.PodReplacementPolicy = ptr(batchv1.TerminatingOrFailed)
	j.Spec.ManualSelector = ptr(false)
	j.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"batch.kubernetes.io/controller-uid": uid}}
	l := j.Spec.Template.Labels
	l["batch.kubernetes.io/controller-uid"], l["controller-uid"] = uid, uid
	l["batch.kubernetes.io/job-name"], l["job-name"] = j.Name, j.Name
	admitPodSpec(&j.Spec.Template.Spec)
}

func admitPod(p *corev1.Pod) {
	admitPodSpec(&p.Spec)
	p.Spec.EnableServiceLinks = ptr(true)
	p.Spec.ServiceAccountName, p.Spec.DeprecatedServiceAccount = "default", "default"
	p.Spec.Priority, p.Spec.PreemptionPolicy = ptr(int32(0)), ptr(corev1.PreemptLowerPriority)
	p.Spec.Tolerations = []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: "Exists", Effect: "NoExecute", TolerationSeconds: ptr(int64(300))},
		{Key: "node.kubernetes.io/unreachable", Operator: "Exists", Effect: "NoExecute", TolerationSeconds: ptr(int64(300))},
	}
}

func admitPodSpec(s *corev1.PodSpec) {
	s.DNSPolicy, s.SchedulerName = "ClusterFirst", "default-scheduler"
	s.SecurityContext, s.TerminationGracePeriodSeconds = &corev1.PodSecurityContext{}, ptr(int64(30))
	for i := range s.Containers {
		c := &s.Containers[i]
		c.TerminationMessagePath, c.TerminationMessagePolicy = "/dev/termination-log", "File"
		// jobTask's image is tagged; retryTask's image is untagged.
		c.ImagePullPolicy = "IfNotPresent"
		if c.Image == "image" {
			c.ImagePullPolicy = "Always"
		}
	}
}

// schedulePod models the scheduler's binding of p to node.
func schedulePod(p *corev1.Pod, node string) {
	p.Spec.NodeName = node
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue})
}

func setQuantity(l *corev1.ResourceList, name corev1.ResourceName, q string) {
	if *l == nil {
		*l = corev1.ResourceList{}
	}
	(*l)[name] = resource.MustParse(q)
}

func setAnnotation(m *metav1.ObjectMeta, k, v string) {
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[k] = v
}

// identityChange is execution behavior added to a parent after admission,
// as a webhook or another writer could. spec applies to a Pod's spec or a
// Job's template spec, podMeta to a Pod's or a Job template's metadata,
// parent to the parent's metadata, job and pod to that kind only.
type identityChange struct {
	name    string
	spec    func(*corev1.PodSpec)
	podMeta func(*metav1.ObjectMeta)
	parent  func(*metav1.ObjectMeta)
	job     func(*batchv1.Job)
	pod     func(*corev1.Pod)
}

func (c identityChange) kinds() []string {
	var kinds []string
	shared := c.spec != nil || c.podMeta != nil || c.parent != nil
	if shared || c.job != nil {
		kinds = append(kinds, "Job")
	}
	if shared || c.pod != nil {
		kinds = append(kinds, "Pod")
	}
	return kinds
}

func (c identityChange) apply(obj any) {
	var spec *corev1.PodSpec
	var podMeta, parent *metav1.ObjectMeta
	switch o := obj.(type) {
	case *batchv1.Job:
		spec, podMeta, parent = &o.Spec.Template.Spec, &o.Spec.Template.ObjectMeta, &o.ObjectMeta
		if c.job != nil {
			c.job(o)
		}
	case *corev1.Pod:
		spec, podMeta, parent = &o.Spec, &o.ObjectMeta, &o.ObjectMeta
		if c.pod != nil {
			c.pod(o)
		}
	}
	if c.spec != nil {
		c.spec(spec)
	}
	if c.podMeta != nil {
		c.podMeta(podMeta)
	}
	if c.parent != nil {
		c.parent(parent)
	}
}

func task0(s *corev1.PodSpec) *corev1.Container { return &s.Containers[0] }

// identityChanges covers every family of behavior the C2 projection did not
// hash. Each changes the admitted object and nothing else.
var identityChanges = []identityChange{
	// Process.
	{name: "args", spec: func(s *corev1.PodSpec) { task0(s).Args = []string{"--exfiltrate"} }},
	{name: "command", spec: func(s *corev1.PodSpec) { task0(s).Command = append(task0(s).Command, "--more") }},
	{name: "env-from", spec: func(s *corev1.PodSpec) {
		task0(s).EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}}}}
	}},
	{name: "env-value-from", spec: func(s *corev1.PodSpec) {
		task0(s).Env = append(task0(s).Env, corev1.EnvVar{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "credentials"}, Key: "token"}}})
	}},
	{name: "working-dir", spec: func(s *corev1.PodSpec) { task0(s).WorkingDir = "/tmp" }},
	{name: "image", spec: func(s *corev1.PodSpec) { task0(s).Image = "other:v1" }},
	{name: "image-pull-policy", spec: func(s *corev1.PodSpec) { task0(s).ImagePullPolicy = corev1.PullNever }},
	{name: "lifecycle", spec: func(s *corev1.PodSpec) {
		task0(s).Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"sh"}}}}
	}},

	// Host namespaces and identity.
	{name: "host-network", spec: func(s *corev1.PodSpec) { s.HostNetwork = true }},
	{name: "host-pid", spec: func(s *corev1.PodSpec) { s.HostPID = true }},
	{name: "host-ipc", spec: func(s *corev1.PodSpec) { s.HostIPC = true }},
	{name: "share-process-namespace", spec: func(s *corev1.PodSpec) { s.ShareProcessNamespace = ptr(true) }},
	{name: "service-account", spec: func(s *corev1.PodSpec) { s.ServiceAccountName, s.DeprecatedServiceAccount = "builder", "builder" }},
	{name: "automount-token", spec: func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = ptr(true) }},
	{name: "image-pull-secrets", spec: func(s *corev1.PodSpec) {
		s.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "registry"}}
	}},

	// Storage.
	{name: "volume", spec: func(s *corev1.PodSpec) {
		s.Volumes = []corev1.Volume{{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}}
	}},
	{name: "volume-mount", spec: func(s *corev1.PodSpec) {
		task0(s).VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	}},

	// Security contexts.
	{name: "pod-security-context", spec: func(s *corev1.PodSpec) {
		s.SecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr(int64(0))}
	}},
	{name: "container-security-context", spec: func(s *corev1.PodSpec) {
		task0(s).SecurityContext = &corev1.SecurityContext{Privileged: ptr(true)}
	}},
	{name: "container-capabilities", spec: func(s *corev1.PodSpec) {
		task0(s).SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"SYS_ADMIN"}}}
	}},

	// Containers.
	{name: "ephemeral-container", spec: func(s *corev1.PodSpec) {
		s.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "busybox"}}}
	}},
	{name: "init-container", spec: func(s *corev1.PodSpec) {
		s.InitContainers = []corev1.Container{{Name: "init", Image: "busybox", Command: []string{"true"}}}
	}},
	{name: "sidecar", spec: func(s *corev1.PodSpec) {
		s.InitContainers = []corev1.Container{{Name: "proxy", Image: "proxy:v1", RestartPolicy: ptr(corev1.ContainerRestartPolicyAlways)}}
	}},
	{name: "extra-container", spec: func(s *corev1.PodSpec) {
		s.Containers = append(s.Containers, corev1.Container{Name: "miner", Image: "miner:v1"})
	}},
	{name: "container-name", spec: func(s *corev1.PodSpec) { task0(s).Name = "other" }},

	// Resources and claims; no name is filtered.
	{name: "extended-resource", spec: func(s *corev1.PodSpec) {
		setQuantity(&task0(s).Resources.Requests, "nvidia.com/gpu", "1")
		setQuantity(&task0(s).Resources.Limits, "nvidia.com/gpu", "1")
	}},
	{name: "extra-request", spec: func(s *corev1.PodSpec) { setQuantity(&task0(s).Resources.Requests, "example.com/fpga", "1") }},
	{name: "added-limit", spec: func(s *corev1.PodSpec) { setQuantity(&task0(s).Resources.Limits, corev1.ResourceCPU, "1") }},
	{name: "changed-request", spec: func(s *corev1.PodSpec) { setQuantity(&task0(s).Resources.Requests, corev1.ResourceCPU, "4") }},
	{name: "pod-resources", spec: func(s *corev1.PodSpec) {
		s.Resources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
	}},
	{name: "overhead", spec: func(s *corev1.PodSpec) { setQuantity(&s.Overhead, corev1.ResourceCPU, "100m") }},
	{name: "resource-claims", spec: func(s *corev1.PodSpec) {
		s.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimTemplateName: ptr("gpu")}}
	}},
	{name: "container-claims", spec: func(s *corev1.PodSpec) { task0(s).Resources.Claims = []corev1.ResourceClaim{{Name: "gpu"}} }},

	// Scheduling.
	{name: "node-selector", spec: func(s *corev1.PodSpec) { s.NodeSelector = map[string]string{"pool": "other"} }},
	{name: "node-name-request", spec: func(s *corev1.PodSpec) { s.NodeName = "node-a" }},
	{name: "affinity", spec: func(s *corev1.PodSpec) {
		s.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "pool", Operator: corev1.NodeSelectorOpIn, Values: []string{"other"}}}}},
		}}}
	}},
	{name: "toleration", spec: func(s *corev1.PodSpec) {
		s.Tolerations = append(s.Tolerations, corev1.Toleration{Key: "dedicated", Operator: corev1.TolerationOpExists})
	}},
	{name: "topology-spread", spec: func(s *corev1.PodSpec) {
		s.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: corev1.DoNotSchedule}}
	}},
	{name: "priority-class", spec: func(s *corev1.PodSpec) { s.PriorityClassName, s.Priority = "high", ptr(int32(1000)) }},
	{name: "scheduler-name", spec: func(s *corev1.PodSpec) { s.SchedulerName = "other" }},
	{name: "runtime-class", spec: func(s *corev1.PodSpec) { s.RuntimeClassName = ptr("gvisor") }},
	{name: "dns-policy", spec: func(s *corev1.PodSpec) { s.DNSPolicy = corev1.DNSDefault }},
	{name: "termination-grace", spec: func(s *corev1.PodSpec) { s.TerminationGracePeriodSeconds = ptr(int64(1)) }},
	{name: "restart-policy", spec: func(s *corev1.PodSpec) { s.RestartPolicy = corev1.RestartPolicyOnFailure }},
	{name: "pod-deadline", spec: func(s *corev1.PodSpec) { s.ActiveDeadlineSeconds = ptr(int64(999)) }},

	// Metadata.
	{name: "pod-annotation", podMeta: func(m *metav1.ObjectMeta) {
		setAnnotation(m, "container.apparmor.security.beta.kubernetes.io/task", "unconfined")
	}},
	{name: "pod-label", podMeta: func(m *metav1.ObjectMeta) { m.Labels["sidecar.istio.io/inject"] = "true" }},
	{name: "parent-finalizer", parent: func(m *metav1.ObjectMeta) { m.Finalizers = []string{"example.com/hold"} }},
	{name: "generate-name", parent: func(m *metav1.ObjectMeta) { m.GenerateName = "skygo-" }},

	// Job lifecycle policies.
	{name: "suspend", job: func(j *batchv1.Job) { j.Spec.Suspend = ptr(true) }},
	{name: "pod-failure-policy", job: func(j *batchv1.Job) {
		j.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
			Action:      batchv1.PodFailurePolicyActionIgnore,
			OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{Operator: batchv1.PodFailurePolicyOnExitCodesOpIn, Values: []int32{1}},
		}}}
	}},
	{name: "success-policy", job: func(j *batchv1.Job) {
		j.Spec.SuccessPolicy = &batchv1.SuccessPolicy{Rules: []batchv1.SuccessPolicyRule{{SucceededCount: ptr(int32(1))}}}
	}},
	{name: "ttl", job: func(j *batchv1.Job) { j.Spec.TTLSecondsAfterFinished = ptr(int32(0)) }},
	{name: "backoff-limit", job: func(j *batchv1.Job) { j.Spec.BackoffLimit = ptr(int32(6)) }},
	{name: "backoff-limit-per-index", job: func(j *batchv1.Job) { j.Spec.BackoffLimitPerIndex = ptr(int32(1)) }},
	{name: "parallelism", job: func(j *batchv1.Job) { j.Spec.Parallelism = ptr(int32(2)) }},
	{name: "completions", job: func(j *batchv1.Job) { j.Spec.Completions = ptr(int32(2)) }},
	{name: "job-deadline", job: func(j *batchv1.Job) { j.Spec.ActiveDeadlineSeconds = ptr(int64(1)) }},
	{name: "completion-mode", job: func(j *batchv1.Job) { j.Spec.CompletionMode = ptr(batchv1.IndexedCompletion) }},
	{name: "pod-replacement-policy", job: func(j *batchv1.Job) { j.Spec.PodReplacementPolicy = ptr(batchv1.Failed) }},
	{name: "managed-by", job: func(j *batchv1.Job) { j.Spec.ManagedBy = ptr("example.com/controller") }},
	{name: "always-pull-images", job: func(j *batchv1.Job) { task0(&j.Spec.Template.Spec).ImagePullPolicy = corev1.PullAlways }},
	{name: "parent-label", job: func(j *batchv1.Job) { j.Labels["team"] = "other" }},

	// Job selectors: only the generated shape of the Job's own UID and name
	// is server output.
	{name: "manual-selector", job: func(j *batchv1.Job) { j.Spec.ManualSelector = ptr(true) }},
	{name: "user-selector", job: func(j *batchv1.Job) {
		j.Spec.ManualSelector = ptr(true)
		j.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{ownerLabel: "owner"}}
	}},
	{name: "selector-wrong-uid", job: func(j *batchv1.Job) {
		j.Spec.Selector.MatchLabels["batch.kubernetes.io/controller-uid"] = "uid-other"
		j.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = "uid-other"
		j.Spec.Template.Labels["controller-uid"] = "uid-other"
	}},
	{name: "selector-wrong-name", job: func(j *batchv1.Job) {
		j.Spec.Template.Labels["batch.kubernetes.io/job-name"] = "skygo-other"
		j.Spec.Template.Labels["job-name"] = "skygo-other"
	}},
	{name: "selector-missing-legacy-label", job: func(j *batchv1.Job) { delete(j.Spec.Template.Labels, "controller-uid") }},
	{name: "selector-expression", job: func(j *batchv1.Job) {
		j.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: idLabel, Operator: metav1.LabelSelectorOpIn, Values: []string{testID}}}
	}},
	{name: "selector-extra-label", job: func(j *batchv1.Job) { j.Spec.Selector.MatchLabels[idLabel] = testID }},

	// Direct Pod admission outputs other than the default shape.
	{name: "toleration-seconds", pod: func(p *corev1.Pod) {
		for i := range p.Spec.Tolerations {
			p.Spec.Tolerations[i].TolerationSeconds = ptr(int64(600))
		}
	}},
	{name: "default-priority-class", pod: func(p *corev1.Pod) { p.Spec.PriorityClassName = "cluster-default" }},
	{name: "deprecated-service-account", pod: func(p *corev1.Pod) { p.Spec.DeprecatedServiceAccount = "builder" }},
}

// TestReceiptIdentityRefusesBehavior adds each change to an admitted parent
// carrying the attempt's nonce, UID, task, binding and spec annotation. The
// actual create response is refused without pinning its UID: the receipt is
// durably refused with a witness of the response, and every later operation
// refuses without a request. After a lost response the recovery read is
// refused and the receipt stays sent. No second create or delete is sent and
// the receipt is unchanged after Start.
func TestReceiptIdentityRefusesBehavior(t *testing.T) {
	for _, tt := range identityChanges {
		for _, kind := range tt.kinds() {
			for _, mode := range []string{"create-response", "lost-response"} {
				t.Run(tt.name+"/"+kind+"/"+mode, func(t *testing.T) {
					ctx := context.Background()
					api, url, root := newReceiptAPI(t)
					api.admit = func(obj any) {
						admitParent(obj)
						tt.apply(obj)
					}
					api.drop = mode == "lost-response"
					b := newReceiptBackend(t, root, url)
					ref := receiptRef(b, kind)
					err := b.Start(ctx, ref.ID, ref.Task)
					if mode == "create-response" && !errors.Is(err, backend.ErrIdentityMismatch) || err == nil {
						t.Fatalf("start: %v", err)
					}
					uid, created := api.parent(kind)
					if !created {
						t.Fatal("parent not stored")
					}
					refused := mode == "create-response"
					// The original Backend blocks a refused attempt in memory;
					// another store reads what was durably recorded.
					r, err := newReceiptBackend(t, root, url).receipts.load(testID)
					if refused {
						w := r.Witness
						if err != nil || r.Phase != phaseRefused || r.UID != "" || w == nil || w.UID != uid ||
							w.Nonce != r.Nonce || w.SpecAnnotation != r.SpecSHA256 || w.SpecSHA256 == "" || w.SpecSHA256 == r.SpecSHA256 {
							t.Fatalf("receipt after start: %+v %+v %v", r, w, err)
						}
					} else if err != nil || r.Phase != phaseSent || r.UID != "" || r.Witness != nil {
						t.Fatalf("receipt after start: %+v %v", r, err)
					}
					before := receiptBytes(t, b, testID)
					requests, _ := api.counts()

					restarted := newReceiptBackend(t, root, url)
					if err := restarted.Start(ctx, ref.ID, ref.Task); !errors.Is(err, errReceiptExists) {
						t.Fatalf("restart start: %v", err)
					}
					want := backend.ErrIdentityMismatch
					if refused {
						want = errReceiptRefused
					}
					if _, err := restarted.Observe(ctx, ref); !errors.Is(err, want) {
						t.Fatalf("recovery: %v", err)
					}
					// Presenting the actual UID does not pin it either.
					named := ref
					named.Instance = uid
					if _, err := restarted.Observe(ctx, named); !errors.Is(err, want) {
						t.Fatalf("recovery with instance: %v", err)
					}
					if err := restarted.Cancel(ctx, ref); !errors.Is(err, want) {
						t.Fatalf("cancel: %v", err)
					}
					if _, err := restarted.Logs(ctx, ref, 0, 64); err == nil || refused && !errors.Is(err, errReceiptRefused) {
						t.Fatalf("logs of a changed parent: %v", err)
					}
					if n, _ := api.counts(); refused && n != requests {
						t.Fatal("refused response reached the API again")
					}
					if _, w := api.counts(); w != 1 {
						t.Fatalf("writes: %d, want the single create", w)
					}
					if !bytes.Equal(before, receiptBytes(t, restarted, testID)) {
						t.Fatal("refused parent changed the receipt")
					}
				})
			}
		}
	}
}

// TestReceiptIdentityAdmittedTwins is the positive twin of
// TestReceiptIdentityRefusesBehavior: the parent exactly as the API server
// defaults and admits it is accepted from the actual create response and
// from recovery after a lost response, with a direct Pod bound by the
// scheduler before the recovery read.
func TestReceiptIdentityAdmittedTwins(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		for _, mode := range []string{"create-response", "lost-response"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				api, url, root := newReceiptAPI(t)
				api.admit = admitParent
				api.drop = mode == "lost-response"
				b := newReceiptBackend(t, root, url)
				ref := receiptRef(b, kind)
				if err := b.Start(ctx, ref.ID, ref.Task); (err == nil) != (mode == "create-response") {
					t.Fatalf("start: %v", err)
				}
				if mode == "lost-response" && kind == "Pod" {
					api.mu.Lock()
					schedulePod(api.pods["skygo-"+testID], "node-a")
					api.mu.Unlock()
				}
				status, err := newReceiptBackend(t, root, url).Observe(ctx, ref)
				uid, _ := api.parent(kind)
				if err != nil || status.Instance != uid {
					t.Fatalf("observe: %+v %v", status, err)
				}
				if r, err := b.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
					t.Fatalf("receipt: %+v %v", r, err)
				}
				if _, w := api.counts(); w != 1 {
					t.Fatalf("writes: %d", w)
				}
			})
		}
	}
}

// TestReceiptIdentityReadOutputs covers the server outputs accepted only in
// a read: a direct Pod's scheduler binding, a Job's foreground deletion
// finalizer, and a quantity written in another format.
func TestReceiptIdentityReadOutputs(t *testing.T) {
	name := "skygo-" + testID
	deleting := metav1.NewTime(time.Unix(1700000000, 0))
	for _, tt := range []struct {
		name   string
		kind   string
		lost   bool // recover after a lost response; otherwise read the pinned parent
		admit  func(any)
		change func(a *receiptAPI)
		ok     bool
	}{
		{"pod-scheduled", "Pod", true, admitParent, func(a *receiptAPI) { schedulePod(a.pods[name], "node-a") }, true},
		{"pod-node-without-binding", "Pod", true, admitParent, func(a *receiptAPI) { a.pods[name].Spec.NodeName = "node-a" }, false},
		{"pod-node-binding-false", "Pod", true, admitParent, func(a *receiptAPI) {
			a.pods[name].Spec.NodeName = "node-a"
			a.pods[name].Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse}}
		}, false},
		{"job-foreground-deletion", "Job", false, admitParent, func(a *receiptAPI) {
			a.jobs[name].DeletionTimestamp = &deleting
			a.jobs[name].Finalizers = []string{metav1.FinalizerDeleteDependents}
		}, true},
		{"job-foreground-finalizer-not-deleting", "Job", false, admitParent, func(a *receiptAPI) {
			a.jobs[name].Finalizers = []string{metav1.FinalizerDeleteDependents}
		}, false},
		{"job-deletion-other-finalizer", "Job", false, admitParent, func(a *receiptAPI) {
			a.jobs[name].DeletionTimestamp = &deleting
			a.jobs[name].Finalizers = []string{metav1.FinalizerDeleteDependents, "example.com/hold"}
		}, false},
		{"job-equal-quantity", "Job", true, nil, func(a *receiptAPI) {
			setQuantity(&task0(&a.jobs[name].Spec.Template.Spec).Resources.Requests, corev1.ResourceMemory, "1048576")
		}, true},
		{"job-different-quantity", "Job", true, nil, func(a *receiptAPI) {
			setQuantity(&task0(&a.jobs[name].Spec.Template.Spec).Resources.Requests, corev1.ResourceMemory, "1048577")
		}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			api, url, root := newReceiptAPI(t)
			api.admit, api.drop = tt.admit, tt.lost
			b := newReceiptBackend(t, root, url)
			ref := receiptRef(b, tt.kind)
			if err := b.Start(ctx, ref.ID, ref.Task); (err == nil) == tt.lost {
				t.Fatalf("start: %v", err)
			}
			api.mu.Lock()
			tt.change(api)
			api.mu.Unlock()
			before := receiptBytes(t, b, testID)
			restarted := newReceiptBackend(t, root, url)
			status, err := restarted.Observe(ctx, ref)
			uid, _ := api.parent(tt.kind)
			if tt.ok {
				if err != nil || status.Instance != uid {
					t.Fatalf("observe: %+v %v", status, err)
				}
				if r, err := restarted.receipts.load(testID); err != nil || r.Phase != phasePinned || r.UID != uid {
					t.Fatalf("receipt: %+v %v", r, err)
				}
			} else {
				if !errors.Is(err, backend.ErrIdentityMismatch) {
					t.Fatalf("observe: %+v %v", status, err)
				}
				if !bytes.Equal(before, receiptBytes(t, restarted, testID)) {
					t.Fatal("refused read changed the receipt")
				}
			}
			if _, w := api.counts(); w != 1 {
				t.Fatalf("writes: %d", w)
			}
		})
	}
}

// TestReceiptIdentityScheduledCreateResponse refuses a create response
// carrying a node binding: the response precedes any scheduling, so the
// node was requested or injected. The stored object also reports
// PodScheduled=True, which a recovery read would accept; the durable
// refusal keeps every later operation from reading it.
func TestReceiptIdentityScheduledCreateResponse(t *testing.T) {
	ctx := context.Background()
	api, url, root := newReceiptAPI(t)
	api.admit = func(obj any) {
		admitParent(obj)
		schedulePod(obj.(*corev1.Pod), "node-a")
	}
	b := newReceiptBackend(t, root, url)
	ref := receiptRef(b, "Pod")
	if err := b.Start(ctx, ref.ID, ref.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("start: %v", err)
	}
	uid, _ := api.parent("Pod")
	restarted := newReceiptBackend(t, root, url)
	r, err := restarted.receipts.load(testID)
	if err != nil || r.Phase != phaseRefused || r.UID != "" || r.Witness == nil || r.Witness.UID != uid || r.Witness.Nonce != r.Nonce {
		t.Fatalf("receipt: %+v %v", r, err)
	}
	requests, _ := api.counts()
	ref.Instance = uid
	if _, err := restarted.Observe(ctx, ref); !errors.Is(err, errReceiptRefused) {
		t.Fatalf("scheduled read after a refused response: %v", err)
	}
	if n, w := api.counts(); n != requests || w != 1 {
		t.Fatalf("requests %d (want %d), writes %d", n, requests, w)
	}
}

// TestReceiptIdentityStrictDecode refuses a parent carrying a field the
// vendored API types do not represent, which typed decoding would drop.
func TestReceiptIdentityStrictDecode(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  string
		ok   bool
	}{
		{"known", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"task","image":"image"}]},"status":{"phase":"Pending","futureStatus":1}}`, true},
		{"unknown-spec-field", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"futureSpecField":"x","containers":[{"name":"task","image":"image"}]}}`, false},
		{"unknown-container-field", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{"containers":[{"name":"task","image":"image","futureField":true}]}}`, false},
		{"unknown-top-level", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{},"extra":{}}`, false},
		{"wrong-kind", `{"apiVersion":"v1","kind":"Service","metadata":{"name":"p"},"spec":{}}`, false},
		{"trailing", `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"},"spec":{}} {}`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeParent("Pod", []byte(tt.raw))
			if (err == nil) != tt.ok || err != nil && !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("decode: %v", err)
			}
		})
	}
}
