package kube

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
)

// The two specs below are the spec objects of one server dry-run create
// (kubectl create --dry-run=server -o json; no Pod persisted) retained at
// S10/pod-defaults-native-c2: fixedPodCaptureInput from input.json and
// fixedPodCaptureOutput from stdout.json, with whitespace and key order
// compacted. They record one API server's defaulted representation of the
// fixed profile; the capture does not record the server version, and it is
// not a runtime qualification.
const fixedPodCaptureInput = `{
  "restartPolicy": "Never",
  "automountServiceAccountToken": false,
  "shareProcessNamespace": false,
  "dnsPolicy": "ClusterFirst",
  "schedulerName": "default-scheduler",
  "terminationGracePeriodSeconds": 30,
  "containers": [{
    "name": "workspace",
    "image": "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e",
    "imagePullPolicy": "IfNotPresent",
    "command": ["/bin/sleep", "60"],
    "workingDir": "/workspace",
    "resources": {"requests": {"cpu": "100m", "memory": "32Mi"}, "limits": {"cpu": "100m", "memory": "32Mi"}},
    "securityContext": {
      "runAsNonRoot": true, "runAsUser": 65532, "readOnlyRootFilesystem": true,
      "allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]},
      "seccompProfile": {"type": "RuntimeDefault"}
    },
    "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}]
  }],
  "volumes": [{"name": "workspace", "persistentVolumeClaim": {"claimName": "data", "readOnly": false}}],
  "enableServiceLinks": false,
  "preemptionPolicy": "PreemptLowerPriority",
  "priority": 0,
  "tolerations": [
    {"effect": "NoExecute", "key": "node.kubernetes.io/not-ready", "operator": "Exists", "tolerationSeconds": 300},
    {"effect": "NoExecute", "key": "node.kubernetes.io/unreachable", "operator": "Exists", "tolerationSeconds": 300}
  ]
}`

const fixedPodCaptureOutput = `{
  "automountServiceAccountToken": false,
  "containers": [{
    "command": ["/bin/sleep", "60"],
    "image": "docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e",
    "imagePullPolicy": "IfNotPresent",
    "name": "workspace",
    "resources": {"limits": {"cpu": "100m", "memory": "32Mi"}, "requests": {"cpu": "100m", "memory": "32Mi"}},
    "securityContext": {
      "allowPrivilegeEscalation": false, "capabilities": {"drop": ["ALL"]},
      "readOnlyRootFilesystem": true, "runAsNonRoot": true, "runAsUser": 65532,
      "seccompProfile": {"type": "RuntimeDefault"}
    },
    "terminationMessagePath": "/dev/termination-log",
    "terminationMessagePolicy": "File",
    "volumeMounts": [{"mountPath": "/workspace", "name": "workspace"}],
    "workingDir": "/workspace"
  }],
  "dnsPolicy": "ClusterFirst",
  "enableServiceLinks": false,
  "preemptionPolicy": "PreemptLowerPriority",
  "priority": 0,
  "restartPolicy": "Never",
  "schedulerName": "default-scheduler",
  "securityContext": {},
  "serviceAccount": "default",
  "serviceAccountName": "default",
  "shareProcessNamespace": false,
  "terminationGracePeriodSeconds": 30,
  "tolerations": [
    {"effect": "NoExecute", "key": "node.kubernetes.io/not-ready", "operator": "Exists", "tolerationSeconds": 300},
    {"effect": "NoExecute", "key": "node.kubernetes.io/unreachable", "operator": "Exists", "tolerationSeconds": 300}
  ],
  "volumes": [{"name": "workspace", "persistentVolumeClaim": {"claimName": "data"}}]
}`

// fixedPodDecode decodes a captured spec, refusing any field the corev1
// types do not declare, so that no captured field is silently dropped.
func fixedPodDecode(t *testing.T, raw string) *corev1.PodSpec {
	t.Helper()
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	var spec corev1.PodSpec
	if err := d.Decode(&spec); err != nil {
		t.Fatal(err)
	}
	return &spec
}

func fixedPodWant(t *testing.T) *corev1.PodSpec { return fixedPodDecode(t, fixedPodCaptureInput) }

func fixedPodGot(t *testing.T) *corev1.PodSpec { return fixedPodDecode(t, fixedPodCaptureOutput) }

// fixedPodFields returns v re-encoded from its Go type as a JSON object.
func fixedPodFields(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// fixedPodChanged returns the sorted keys whose values differ between a and b.
func fixedPodChanged(a, b map[string]any) []string {
	var keys []string
	for k, v := range a {
		if !reflect.DeepEqual(v, b[k]) {
			keys = append(keys, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}

// fixedPodServerDefaults sets on s the values that the captured server
// assigned: the default service account and alias, the pod security context
// and, on every container and init container, the termination message path
// and policy of the captured container. MODELED: the capture had no init
// container; the pinned corev1 type documents the same defaults for both.
func fixedPodServerDefaults(t *testing.T, s *corev1.PodSpec) {
	t.Helper()
	out := fixedPodGot(t)
	s.ServiceAccountName, s.DeprecatedServiceAccount = out.ServiceAccountName, out.DeprecatedServiceAccount
	s.SecurityContext = out.SecurityContext.DeepCopy()
	path, policy := out.Containers[0].TerminationMessagePath, out.Containers[0].TerminationMessagePolicy
	for i := range s.Containers {
		s.Containers[i].TerminationMessagePath, s.Containers[i].TerminationMessagePolicy = path, policy
	}
	for i := range s.InitContainers {
		s.InitContainers[i].TerminationMessagePath, s.InitContainers[i].TerminationMessagePolicy = path, policy
	}
}

// fixedPodMatchesUnchanged calls fixedPodSpecMatches and fails if it modified
// either argument.
func fixedPodMatchesUnchanged(t *testing.T, want, got *corev1.PodSpec) bool {
	t.Helper()
	wantBefore, gotBefore := want.DeepCopy(), got.DeepCopy()
	m := fixedPodSpecMatches(want, got)
	if !reflect.DeepEqual(want, wantBefore) || !reflect.DeepEqual(got, gotBefore) {
		t.Fatal("fixedPodSpecMatches modified its arguments")
	}
	return m
}

// TestFixedPodSpecMatchesCapture checks the captured dry-run response against
// the captured request, and that the capture differs from its request in
// exactly the fields the matcher accepts.
func TestFixedPodSpecMatchesCapture(t *testing.T) {
	want, got := fixedPodWant(t), fixedPodGot(t)
	if equality.Semantic.DeepEqual(want, got) {
		t.Fatal("capture has no server defaults")
	}
	in, out := fixedPodFields(t, want), fixedPodFields(t, got)
	if keys, wantKeys := fixedPodChanged(in, out), []string{"containers", "securityContext", "serviceAccount", "serviceAccountName"}; !slices.Equal(keys, wantKeys) {
		t.Fatalf("captured spec changed %q, want %q", keys, wantKeys)
	}
	c0 := func(m map[string]any) map[string]any { return m["containers"].([]any)[0].(map[string]any) }
	if keys, wantKeys := fixedPodChanged(c0(in), c0(out)), []string{"terminationMessagePath", "terminationMessagePolicy"}; !slices.Equal(keys, wantKeys) {
		t.Fatalf("captured container changed %q, want %q", keys, wantKeys)
	}
	if !fixedPodMatchesUnchanged(t, want, got) {
		t.Fatal("captured dry-run response does not match its request")
	}
	if !fixedPodMatchesUnchanged(t, want, want) || !fixedPodMatchesUnchanged(t, got, got) {
		t.Fatal("spec does not match itself")
	}
	got.NodeName = "node-a"
	if !fixedPodMatchesUnchanged(t, want, got) {
		t.Fatal("scheduled capture does not match")
	}
	// Without the explicit scheduling fields the request is not the fixed
	// profile, and the response does not satisfy it.
	implicit := want.DeepCopy()
	implicit.EnableServiceLinks, implicit.PreemptionPolicy, implicit.Priority, implicit.Tolerations = nil, nil, nil, nil
	if fixedPodMatchesUnchanged(t, want, implicit) || fixedPodMatchesUnchanged(t, implicit, fixedPodGot(t)) {
		t.Fatal("spec without explicit scheduling fields matched")
	}
}

func TestFixedPodSpecMatchesNil(t *testing.T) {
	if fixedPodSpecMatches(nil, nil) || fixedPodSpecMatches(fixedPodWant(t), nil) || fixedPodSpecMatches(nil, fixedPodGot(t)) {
		t.Fatal("nil spec matched")
	}
}

// TestFixedPodSpecMatchesRendered checks Pods from environmentPod read back
// with the captured server defaults, with and without artifacts and an
// accelerator.
func TestFixedPodSpecMatchesRendered(t *testing.T) {
	gpu := corev1.Toleration{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
	accelerator := func(t *testing.T, b *Backend, in *environmentPodInput) {
		in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
		in.task.NodeSelector = map[string]string{"pool": "gpu"}
	}
	artifacts := func(t *testing.T, b *Backend, in *environmentPodInput) {
		b.data = &dataProfile{helperImage: profileHelp}
		arts, err := environmentArtifacts(envArtifactSpec().Artifacts)
		if err != nil {
			t.Fatal(err)
		}
		in.artifacts = arts
	}
	for _, tt := range []struct {
		name    string
		prepare func(*testing.T, *Backend, *environmentPodInput)
		extra   []corev1.Toleration
		drift   func(*corev1.PodSpec)
	}{
		{"workspace", func(*testing.T, *Backend, *environmentPodInput) {}, nil, func(s *corev1.PodSpec) {
			s.Containers[0].Args = []string{"-c", "id"}
		}},
		{"accelerator", accelerator, nil, func(s *corev1.PodSpec) {
			delete(s.Containers[0].Resources.Limits, "nvidia.com/gpu")
		}},
		{"accelerator with admission toleration", accelerator, []corev1.Toleration{gpu}, func(s *corev1.PodSpec) {
			s.Tolerations = append(s.Tolerations, gpu)
		}},
		{"artifacts", artifacts, nil, func(s *corev1.PodSpec) {
			s.InitContainers[0].Command = s.InitContainers[0].Command[:3]
		}},
		{"artifacts writable mount", artifacts, nil, func(s *corev1.PodSpec) {
			s.Containers[0].VolumeMounts[1].ReadOnly = false
		}},
		{"artifacts init security", artifacts, nil, func(s *corev1.PodSpec) {
			s.InitContainers[0].SecurityContext.Privileged = new(bool)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := environmentTestBackend(t)
			in, p := environmentTestInput(), environmentTestProfile()
			tt.prepare(t, b, &in)
			pod, err := b.environmentPod(in, p)
			if err != nil {
				t.Fatal(err)
			}
			got := pod.Spec.DeepCopy()
			fixedPodServerDefaults(t, got)
			got.Tolerations = append(got.Tolerations, tt.extra...)
			got.NodeName = "node-a"
			if !fixedPodMatchesUnchanged(t, &pod.Spec, got) {
				t.Fatal("defaulted read-back does not match")
			}
			tt.drift(got)
			if fixedPodMatchesUnchanged(t, &pod.Spec, got) {
				t.Fatal("drifted read-back matched")
			}
		})
	}
}

// TestFixedPodSpecMatchesRepresentations checks each accepted representation
// against the captured response.
func TestFixedPodSpecMatchesRepresentations(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(want, got *corev1.PodSpec)
	}{
		{"tolerations reversed", func(want, got *corev1.PodSpec) {
			got.Tolerations[0], got.Tolerations[1] = got.Tolerations[1], got.Tolerations[0]
		}},
		{"want tolerations reversed", func(want, got *corev1.PodSpec) {
			want.Tolerations[0], want.Tolerations[1] = want.Tolerations[1], want.Tolerations[0]
		}},
		{"no service account", func(want, got *corev1.PodSpec) {
			got.ServiceAccountName, got.DeprecatedServiceAccount = "", ""
		}},
		{"nil pod security context", func(want, got *corev1.PodSpec) { got.SecurityContext = nil }},
		{"empty want pod security context", func(want, got *corev1.PodSpec) {
			want.SecurityContext, got.SecurityContext = &corev1.PodSecurityContext{}, nil
		}},
		{"empty termination message", func(want, got *corev1.PodSpec) {
			got.Containers[0].TerminationMessagePath, got.Containers[0].TerminationMessagePolicy = "", ""
		}},
		{"explicit termination message", func(want, got *corev1.PodSpec) {
			want.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
			want.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}},
		{"nil share process namespace", func(want, got *corev1.PodSpec) { got.ShareProcessNamespace = nil }},
		{"nil priority", func(want, got *corev1.PodSpec) { got.Priority = nil }},
		{"scheduled", func(want, got *corev1.PodSpec) { got.NodeName = "node-a" }},
		{"pinned node", func(want, got *corev1.PodSpec) { want.NodeName, got.NodeName = "node-a", "node-a" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			tt.mutate(want, got)
			if !fixedPodMatchesUnchanged(t, want, got) {
				t.Fatal("accepted representation did not match")
			}
		})
	}
}

// TestFixedPodSpecMismatch checks that a change to any other field of the
// captured response is a mismatch.
func TestFixedPodSpecMismatch(t *testing.T) {
	yes, no := true, false
	seconds := func(v int64) *int64 { return &v }
	int32p := func(v int32) *int32 { return &v }
	never := corev1.PreemptNever
	root := int64(0)
	claim := "gpu-claim"
	for _, tt := range []struct {
		name   string
		mutate func(*corev1.PodSpec)
	}{
		{"extra toleration", func(s *corev1.PodSpec) {
			s.Tolerations = append(s.Tolerations, corev1.Toleration{Key: "gpu", Operator: corev1.TolerationOpExists})
		}},
		{"extra standard toleration", func(s *corev1.PodSpec) { s.Tolerations = append(s.Tolerations, s.Tolerations[0]) }},
		{"tolerate everything", func(s *corev1.PodSpec) {
			s.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
		}},
		{"missing toleration", func(s *corev1.PodSpec) { s.Tolerations = s.Tolerations[:1] }},
		{"no tolerations", func(s *corev1.PodSpec) { s.Tolerations = nil }},
		{"duplicate toleration", func(s *corev1.PodSpec) { s.Tolerations[1] = s.Tolerations[0] }},
		{"toleration seconds", func(s *corev1.PodSpec) { s.Tolerations[0].TolerationSeconds = seconds(301) }},
		{"toleration forever", func(s *corev1.PodSpec) { s.Tolerations[1].TolerationSeconds = nil }},
		{"toleration key", func(s *corev1.PodSpec) { s.Tolerations[0].Key = corev1.TaintNodeUnschedulable }},
		{"toleration effect", func(s *corev1.PodSpec) { s.Tolerations[0].Effect = corev1.TaintEffectNoSchedule }},
		{"toleration operator", func(s *corev1.PodSpec) {
			s.Tolerations[0].Operator, s.Tolerations[0].Value = corev1.TolerationOpEqual, "x"
		}},
		{"priority class", func(s *corev1.PodSpec) { s.PriorityClassName = "high" }},
		{"priority", func(s *corev1.PodSpec) { s.Priority = int32p(1000) }},
		{"preemption never", func(s *corev1.PodSpec) { s.PreemptionPolicy = &never }},
		{"preemption nil", func(s *corev1.PodSpec) { s.PreemptionPolicy = nil }},
		{"node affinity", func(s *corev1.PodSpec) { s.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}} }},
		{"pod anti affinity", func(s *corev1.PodSpec) {
			s.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}
		}},
		{"image pull secret injected", func(s *corev1.PodSpec) {
			s.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
		}},
		{"service links", func(s *corev1.PodSpec) { s.EnableServiceLinks = &yes }},
		{"service links nil", func(s *corev1.PodSpec) { s.EnableServiceLinks = nil }},
		{"automount token", func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = &yes }},
		{"automount nil", func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = nil }},
		{"service account", func(s *corev1.PodSpec) { s.ServiceAccountName, s.DeprecatedServiceAccount = "builder", "builder" }},
		{"service account alias", func(s *corev1.PodSpec) { s.DeprecatedServiceAccount = "builder" }},
		{"service account name only", func(s *corev1.PodSpec) { s.DeprecatedServiceAccount = "" }},
		{"share process namespace", func(s *corev1.PodSpec) { s.ShareProcessNamespace = &yes }},
		{"host network", func(s *corev1.PodSpec) { s.HostNetwork = true }},
		{"host pid", func(s *corev1.PodSpec) { s.HostPID = true }},
		{"host ipc", func(s *corev1.PodSpec) { s.HostIPC = true }},
		{"host users", func(s *corev1.PodSpec) { s.HostUsers = &no }},
		{"pod run as root", func(s *corev1.PodSpec) { s.SecurityContext = &corev1.PodSecurityContext{RunAsUser: &root} }},
		{"pod supplemental groups", func(s *corev1.PodSpec) {
			s.SecurityContext = &corev1.PodSecurityContext{SupplementalGroups: []int64{0}}
		}},
		{"pod fs group", func(s *corev1.PodSpec) { s.SecurityContext = &corev1.PodSecurityContext{FSGroup: &root} }},
		{"pod sysctls", func(s *corev1.PodSpec) {
			s.SecurityContext = &corev1.PodSecurityContext{Sysctls: []corev1.Sysctl{{Name: "kernel.shm_rmid_forced", Value: "0"}}}
		}},
		{"pod seccomp unconfined", func(s *corev1.PodSpec) {
			s.SecurityContext = &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}}
		}},
		{"pod apparmor", func(s *corev1.PodSpec) {
			s.SecurityContext = &corev1.PodSecurityContext{AppArmorProfile: &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}}
		}},
		{"dns policy", func(s *corev1.PodSpec) { s.DNSPolicy = corev1.DNSDefault }},
		{"dns policy empty", func(s *corev1.PodSpec) { s.DNSPolicy = "" }},
		{"dns config", func(s *corev1.PodSpec) { s.DNSConfig = &corev1.PodDNSConfig{Nameservers: []string{"1.1.1.1"}} }},
		{"host aliases", func(s *corev1.PodSpec) {
			s.HostAliases = []corev1.HostAlias{{IP: "10.0.0.1", Hostnames: []string{"registry"}}}
		}},
		{"hostname", func(s *corev1.PodSpec) { s.Hostname = "h" }},
		{"subdomain", func(s *corev1.PodSpec) { s.Subdomain = "s" }},
		{"hostname as fqdn", func(s *corev1.PodSpec) { s.SetHostnameAsFQDN = &yes }},
		{"scheduler", func(s *corev1.PodSpec) { s.SchedulerName = "other" }},
		{"scheduler empty", func(s *corev1.PodSpec) { s.SchedulerName = "" }},
		{"grace", func(s *corev1.PodSpec) { s.TerminationGracePeriodSeconds = seconds(60) }},
		{"grace nil", func(s *corev1.PodSpec) { s.TerminationGracePeriodSeconds = nil }},
		{"restart always", func(s *corev1.PodSpec) { s.RestartPolicy = corev1.RestartPolicyAlways }},
		{"deadline", func(s *corev1.PodSpec) { s.ActiveDeadlineSeconds = seconds(60) }},
		{"runtime class", func(s *corev1.PodSpec) { name := "kata"; s.RuntimeClassName = &name }},
		{"node selector", func(s *corev1.PodSpec) { s.NodeSelector = map[string]string{"pool": "x"} }},
		{"topology spread", func(s *corev1.PodSpec) {
			s.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "zone",
				WhenUnsatisfiable: corev1.DoNotSchedule}}
		}},
		{"overhead", func(s *corev1.PodSpec) { s.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")} }},
		{"scheduling gate", func(s *corev1.PodSpec) { s.SchedulingGates = []corev1.PodSchedulingGate{{Name: "g"}} }},
		{"readiness gate", func(s *corev1.PodSpec) { s.ReadinessGates = []corev1.PodReadinessGate{{ConditionType: "x"}} }},
		{"os", func(s *corev1.PodSpec) { s.OS = &corev1.PodOS{Name: corev1.Linux} }},
		{"pod resource claim", func(s *corev1.PodSpec) {
			s.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: &claim}}
		}},
		{"pod resources", func(s *corev1.PodSpec) {
			s.Resources = &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}}
		}},
		{"sidecar", func(s *corev1.PodSpec) {
			c := *s.Containers[0].DeepCopy()
			c.Name = "proxy"
			s.Containers = append(s.Containers, c)
		}},
		{"init container", func(s *corev1.PodSpec) { s.InitContainers = []corev1.Container{*s.Containers[0].DeepCopy()} }},
		{"ephemeral container", func(s *corev1.PodSpec) {
			s.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug"}}}
		}},
		{"container name", func(s *corev1.PodSpec) { s.Containers[0].Name = "other" }},
		{"image", func(s *corev1.PodSpec) { s.Containers[0].Image = "busybox:latest" }},
		{"image pull always", func(s *corev1.PodSpec) { s.Containers[0].ImagePullPolicy = corev1.PullAlways }},
		{"image pull empty", func(s *corev1.PodSpec) { s.Containers[0].ImagePullPolicy = "" }},
		{"container restart policy", func(s *corev1.PodSpec) {
			always := corev1.ContainerRestartPolicyAlways
			s.Containers[0].RestartPolicy = &always
		}},
		{"resize policy", func(s *corev1.PodSpec) {
			s.Containers[0].ResizePolicy = []corev1.ContainerResizePolicy{{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired}}
		}},
		{"command", func(s *corev1.PodSpec) { s.Containers[0].Command = []string{"/bin/sh"} }},
		{"args", func(s *corev1.PodSpec) { s.Containers[0].Args = []string{"-c", "id"} }},
		{"env", func(s *corev1.PodSpec) { s.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "1"}} }},
		{"env from secret", func(s *corev1.PodSpec) {
			s.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "creds"}}}}
		}},
		{"env from config map", func(s *corev1.PodSpec) {
			s.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: "config"}}}}
		}},
		{"working dir", func(s *corev1.PodSpec) { s.Containers[0].WorkingDir = "/" }},
		{"ports", func(s *corev1.PodSpec) {
			s.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 80, HostPort: 80}}
		}},
		{"cpu limit", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("200m")
		}},
		{"memory request", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("64Mi")
		}},
		{"limits removed", func(s *corev1.PodSpec) { s.Containers[0].Resources.Limits = nil }},
		{"extra resource", func(s *corev1.PodSpec) { s.Containers[0].Resources.Limits["nvidia.com/gpu"] = resource.MustParse("1") }},
		{"container resource claim", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "gpu"}}
		}},
		{"liveness probe", func(s *corev1.PodSpec) {
			s.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
		}},
		{"readiness probe", func(s *corev1.PodSpec) { s.Containers[0].ReadinessProbe = &corev1.Probe{} }},
		{"startup probe", func(s *corev1.PodSpec) { s.Containers[0].StartupProbe = &corev1.Probe{} }},
		{"lifecycle", func(s *corev1.PodSpec) { s.Containers[0].Lifecycle = &corev1.Lifecycle{} }},
		{"stdin", func(s *corev1.PodSpec) { s.Containers[0].Stdin = true }},
		{"tty", func(s *corev1.PodSpec) { s.Containers[0].TTY = true }},
		{"termination path", func(s *corev1.PodSpec) { s.Containers[0].TerminationMessagePath = "/etc/passwd" }},
		{"termination policy", func(s *corev1.PodSpec) {
			s.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}},
		{"container security nil", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext = nil }},
		{"privileged", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.Privileged = &yes }},
		{"root", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.RunAsUser = &root }},
		{"run as non root nil", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.RunAsNonRoot = nil }},
		{"writable root", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.ReadOnlyRootFilesystem = &no }},
		{"escalation", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.AllowPrivilegeEscalation = &yes }},
		{"capability add", func(s *corev1.PodSpec) {
			s.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"SYS_ADMIN"}
		}},
		{"capability drop", func(s *corev1.PodSpec) { s.Containers[0].SecurityContext.Capabilities.Drop = nil }},
		{"seccomp", func(s *corev1.PodSpec) {
			s.Containers[0].SecurityContext.SeccompProfile.Type = corev1.SeccompProfileTypeUnconfined
		}},
		{"proc mount", func(s *corev1.PodSpec) {
			unmasked := corev1.UnmaskedProcMount
			s.Containers[0].SecurityContext.ProcMount = &unmasked
		}},
		{"container apparmor", func(s *corev1.PodSpec) {
			s.Containers[0].SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}
		}},
		{"mount read only", func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].ReadOnly = true }},
		{"mount path", func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].MountPath = "/" }},
		{"mount sub path", func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].SubPath = "x" }},
		{"mount sub path expr", func(s *corev1.PodSpec) { s.Containers[0].VolumeMounts[0].SubPathExpr = "$(X)" }},
		{"mount propagation", func(s *corev1.PodSpec) {
			bidi := corev1.MountPropagationBidirectional
			s.Containers[0].VolumeMounts[0].MountPropagation = &bidi
		}},
		{"volume device", func(s *corev1.PodSpec) {
			s.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "workspace", DevicePath: "/dev/x"}}
		}},
		{"extra mount", func(s *corev1.PodSpec) {
			s.Containers[0].VolumeMounts = append(s.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "host", MountPath: "/host"})
		}},
		{"claim", func(s *corev1.PodSpec) { s.Volumes[0].PersistentVolumeClaim.ClaimName = "other" }},
		{"claim read only", func(s *corev1.PodSpec) { s.Volumes[0].PersistentVolumeClaim.ReadOnly = true }},
		{"volume source", func(s *corev1.PodSpec) {
			s.Volumes[0].VolumeSource = corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
		}},
		{"host path volume", func(s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "host",
				VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}})
		}},
		{"secret volume", func(s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "creds",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "creds"}}})
		}},
		{"service account token volume", func(s *corev1.PodSpec) {
			s.Volumes = append(s.Volumes, corev1.Volume{Name: "kube-api-access",
				VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{
					ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: seconds(3607)}}}}}})
		}},
		{"no volumes", func(s *corev1.PodSpec) { s.Volumes = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			if !fixedPodSpecMatches(want, got) {
				t.Fatal("baseline does not match")
			}
			tt.mutate(got)
			if fixedPodMatchesUnchanged(t, want, got) {
				t.Fatal("mutation matched")
			}
		})
	}
}

// TestFixedPodSpecMatchesExpected checks that each accepted representation
// applies only when want leaves the field at the matching value.
func TestFixedPodSpecMatchesExpected(t *testing.T) {
	yes, no := true, false
	for _, tt := range []struct {
		name string
		want func(*corev1.PodSpec)
		got  func(*corev1.PodSpec)
	}{
		{"default service account with token", func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = &yes },
			func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = &yes }},
		{"default service account without explicit automount", func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = nil },
			func(s *corev1.PodSpec) { s.AutomountServiceAccountToken = nil }},
		{"default service account for named", func(s *corev1.PodSpec) {
			s.ServiceAccountName, s.DeprecatedServiceAccount = "runner", "runner"
		}, func(s *corev1.PodSpec) {}},
		{"nil pod context for nonempty", func(s *corev1.PodSpec) {
			v := int64(65532)
			s.SecurityContext = &corev1.PodSecurityContext{FSGroup: &v}
		}, func(s *corev1.PodSpec) { s.SecurityContext = nil }},
		{"default termination path for explicit", func(s *corev1.PodSpec) {
			s.Containers[0].TerminationMessagePath = "/dev/custom"
		}, func(s *corev1.PodSpec) {}},
		{"default termination policy for explicit", func(s *corev1.PodSpec) {
			s.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
		}, func(s *corev1.PodSpec) {}},
		{"nil share process namespace for true", func(s *corev1.PodSpec) { s.ShareProcessNamespace = &yes },
			func(s *corev1.PodSpec) { s.ShareProcessNamespace = nil }},
		{"nil priority for nonzero", func(s *corev1.PodSpec) { v := int32(10); s.Priority = &v },
			func(s *corev1.PodSpec) { s.Priority = nil }},
		{"other node for pinned node", func(s *corev1.PodSpec) { s.NodeName = "node-a" },
			func(s *corev1.PodSpec) { s.NodeName = "node-b" }},
		{"no node for pinned node", func(s *corev1.PodSpec) { s.NodeName = "node-a" }, func(s *corev1.PodSpec) {}},
		{"reversed tolerations for other set", func(s *corev1.PodSpec) { s.Tolerations[1].TolerationSeconds = nil },
			func(s *corev1.PodSpec) {
				s.Tolerations[0], s.Tolerations[1] = s.Tolerations[1], s.Tolerations[0]
				s.Tolerations[0].TolerationSeconds = nil
			}},
		{"standard tolerations for none", func(s *corev1.PodSpec) { s.Tolerations = nil }, func(s *corev1.PodSpec) {}},
		{"service links false for nil", func(s *corev1.PodSpec) { s.EnableServiceLinks = nil },
			func(s *corev1.PodSpec) { s.EnableServiceLinks = &no }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			tt.want(want)
			tt.got(got)
			if fixedPodMatchesUnchanged(t, want, got) {
				t.Fatal("representation applied for a different expected value")
			}
		})
	}
}

// TestFixedPodSpecTerminationContainers checks the termination message
// defaults per container and per init container.
func TestFixedPodSpecTerminationContainers(t *testing.T) {
	const custom = "/dev/custom"
	def, file, logs := corev1.TerminationMessagePathDefault, corev1.TerminationMessageReadFile, corev1.TerminationMessageFallbackToLogsOnError
	set := func(c *corev1.Container, path string, policy corev1.TerminationMessagePolicy) {
		c.TerminationMessagePath, c.TerminationMessagePolicy = path, policy
	}
	two := func(path0 string, policy0 corev1.TerminationMessagePolicy, path1 string, policy1 corev1.TerminationMessagePolicy) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) {
			second := *s.Containers[0].DeepCopy()
			second.Name = "second"
			s.Containers = append(s.Containers, second)
			set(&s.Containers[0], path0, policy0)
			set(&s.Containers[1], path1, policy1)
		}
	}
	initial := func(path string, policy corev1.TerminationMessagePolicy) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) {
			c := *s.Containers[0].DeepCopy()
			c.Name = "init"
			set(&c, path, policy)
			s.InitContainers = []corev1.Container{c}
		}
	}
	for _, tt := range []struct {
		name      string
		want, got func(*corev1.PodSpec)
		match     bool
	}{
		{"both defaulted", two("", "", "", ""), two(def, file, def, file), true},
		{"explicit first defaulted second", two(custom, logs, "", ""), two(custom, logs, def, file), true},
		{"defaulted first explicit second", two("", "", custom, logs), two(def, file, custom, logs), true},
		{"explicit second defaulted", two("", "", custom, logs), two(def, file, def, file), false},
		{"second custom path", two("", "", "", ""), two(def, file, custom, file), false},
		{"second custom policy", two("", "", "", ""), two(def, file, def, logs), false},
		{"init defaulted", initial("", ""), initial(def, file), true},
		{"init custom path", initial("", ""), initial(custom, file), false},
		{"init custom policy", initial("", ""), initial(def, logs), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			tt.want(want)
			tt.got(got)
			if m := fixedPodMatchesUnchanged(t, want, got); m != tt.match {
				t.Fatalf("fixedPodSpecMatches = %v, want %v", m, tt.match)
			}
		})
	}
}

// fixedPodDerived returns the toleration that ExtendedResourceToleration
// admission adds for resource.
func fixedPodDerived(resource string) corev1.Toleration {
	return corev1.Toleration{Key: resource, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
}

// TestFixedPodSpecExtendedResourceTolerations checks the tolerations that
// ExtendedResourceToleration admission derives from want's requests: all or
// none, each exactly once. This is read-back compatibility, not accelerator
// availability.
func TestFixedPodSpecExtendedResourceTolerations(t *testing.T) {
	const gpu, fpga = "nvidia.com/gpu", "example.com/fpga"
	one := resource.MustParse("1")
	request := func(c *corev1.Container, names ...string) {
		for _, name := range names {
			c.Resources.Requests[corev1.ResourceName(name)] = one
			c.Resources.Limits[corev1.ResourceName(name)] = one
		}
	}
	initial := func(s *corev1.PodSpec, names ...string) {
		c := *s.Containers[0].DeepCopy()
		c.Name = "init"
		s.InitContainers = []corev1.Container{c}
		request(&s.InitContainers[0], names...)
	}
	regular := func(names ...string) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) { request(&s.Containers[0], names...) }
	}
	tolerations := func(extra ...corev1.Toleration) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) { s.Tolerations = append(s.Tolerations, extra...) }
	}
	first := func(extra ...corev1.Toleration) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) { s.Tolerations = append(slices.Clone(extra), s.Tolerations...) }
	}
	both := func(fs ...func(*corev1.PodSpec)) func(*corev1.PodSpec) {
		return func(s *corev1.PodSpec) {
			for _, f := range fs {
				f(s)
			}
		}
	}
	zero := int64(0)
	wrong := func(mutate func(*corev1.Toleration)) corev1.Toleration {
		t := fixedPodDerived(gpu)
		mutate(&t)
		return t
	}
	for _, tt := range []struct {
		name      string
		want, got func(*corev1.PodSpec)
		match     bool
	}{
		{"plugin enabled", regular(gpu), both(regular(gpu), tolerations(fixedPodDerived(gpu))), true},
		{"plugin enabled first", regular(gpu), both(regular(gpu), first(fixedPodDerived(gpu))), true},
		{"plugin absent", regular(gpu), regular(gpu), true},
		{"zero quantity request", func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests[gpu] = resource.MustParse("0")
		}, both(func(s *corev1.PodSpec) {
			s.Containers[0].Resources.Requests[gpu] = resource.MustParse("0")
		}, tolerations(fixedPodDerived(gpu))), true},
		{"init only", func(s *corev1.PodSpec) { initial(s, fpga) },
			both(func(s *corev1.PodSpec) { initial(s, fpga) }, tolerations(fixedPodDerived(fpga))), true},
		{"init and main same resource", both(regular(gpu), func(s *corev1.PodSpec) { initial(s, gpu) }),
			both(regular(gpu), func(s *corev1.PodSpec) { initial(s, gpu) }, tolerations(fixedPodDerived(gpu))), true},
		{"multiple sorted", regular(gpu, fpga),
			both(regular(gpu, fpga), tolerations(fixedPodDerived(fpga), fixedPodDerived(gpu))), true},
		{"multiple reversed", regular(gpu, fpga),
			both(regular(gpu, fpga), tolerations(fixedPodDerived(gpu), fixedPodDerived(fpga))), true},
		{"multiple split", regular(gpu, fpga),
			both(regular(gpu, fpga), first(fixedPodDerived(gpu)), tolerations(fixedPodDerived(fpga))), true},
		{"want already tolerates", both(regular(gpu), tolerations(fixedPodDerived(gpu))),
			both(regular(gpu), tolerations(fixedPodDerived(gpu))), true},
		{"want already tolerates duplicated", both(regular(gpu), tolerations(fixedPodDerived(gpu))),
			both(regular(gpu), tolerations(fixedPodDerived(gpu), fixedPodDerived(gpu))), false},
		{"multiple partial", regular(gpu, fpga), both(regular(gpu, fpga), tolerations(fixedPodDerived(gpu))), false},
		{"multiple one duplicated", regular(gpu, fpga),
			both(regular(gpu, fpga), tolerations(fixedPodDerived(gpu), fixedPodDerived(gpu))), false},
		{"unrequested resource", regular(gpu),
			both(regular(gpu), tolerations(fixedPodDerived(gpu), fixedPodDerived(fpga))), false},
		{"no requests", func(s *corev1.PodSpec) {}, tolerations(fixedPodDerived(gpu)), false},
		{"got only resource", func(s *corev1.PodSpec) {}, both(regular(gpu), tolerations(fixedPodDerived(gpu))), false},
		{"limits only", func(s *corev1.PodSpec) { s.Containers[0].Resources.Limits[gpu] = one },
			both(func(s *corev1.PodSpec) { s.Containers[0].Resources.Limits[gpu] = one }, tolerations(fixedPodDerived(gpu))), false},
		{"native resource", func(s *corev1.PodSpec) {}, tolerations(fixedPodDerived("cpu")), false},
		{"kubernetes.io resource", regular("example.kubernetes.io/dev"),
			both(regular("example.kubernetes.io/dev"), tolerations(fixedPodDerived("example.kubernetes.io/dev"))), false},
		{"requests prefix resource", regular("requests.example.com/dev"),
			both(regular("requests.example.com/dev"), tolerations(fixedPodDerived("requests.example.com/dev"))), false},
		{"wrong effect", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) {
			t.Effect = corev1.TaintEffectNoExecute
		}))), false},
		{"empty effect", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) { t.Effect = "" }))), false},
		{"wrong operator", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) {
			t.Operator = corev1.TolerationOpEqual
		}))), false},
		{"wrong value", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) { t.Value = "x" }))), false},
		{"wrong seconds", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) {
			t.TolerationSeconds = &zero
		}))), false},
		{"wildcard", regular(gpu), both(regular(gpu), tolerations(wrong(func(t *corev1.Toleration) { t.Key = "" }))), false},
		{"duplicate", regular(gpu), both(regular(gpu), tolerations(fixedPodDerived(gpu), fixedPodDerived(gpu))), false},
		{"foreign", regular(gpu), both(regular(gpu),
			tolerations(fixedPodDerived(gpu), corev1.Toleration{Key: "x", Operator: corev1.TolerationOpExists})), false},
		{"missing standard pair", regular(gpu), both(regular(gpu), func(s *corev1.PodSpec) {
			s.Tolerations = []corev1.Toleration{fixedPodDerived(gpu)}
		}), false},
		{"one standard toleration", regular(gpu), both(regular(gpu), func(s *corev1.PodSpec) {
			s.Tolerations = []corev1.Toleration{s.Tolerations[0], fixedPodDerived(gpu)}
		}), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			tt.want(want)
			tt.got(got)
			if m := fixedPodMatchesUnchanged(t, want, got); m != tt.match {
				t.Fatalf("fixedPodSpecMatches = %v, want %v", m, tt.match)
			}
		})
	}
}

// TestFixedPodSpecImagePullSecrets checks that image pull secrets are compared
// exactly: callers set the list explicitly, so a copied, reordered or
// missing reference is a mismatch.
func TestFixedPodSpecImagePullSecrets(t *testing.T) {
	refs := func(names ...string) []corev1.LocalObjectReference {
		var r []corev1.LocalObjectReference
		for _, name := range names {
			r = append(r, corev1.LocalObjectReference{Name: name})
		}
		return r
	}
	for _, tt := range []struct {
		name      string
		want, got []corev1.LocalObjectReference
		match     bool
	}{
		{"none", nil, nil, true},
		{"same", refs("a", "b"), refs("a", "b"), true},
		{"admission copied", nil, refs("a"), false},
		{"missing", refs("a"), nil, false},
		{"reordered", refs("a", "b"), refs("b", "a"), false},
		{"extra", refs("a"), refs("a", "b"), false},
		{"different", refs("a"), refs("b"), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			want.ImagePullSecrets, got.ImagePullSecrets = tt.want, tt.got
			if m := fixedPodMatchesUnchanged(t, want, got); m != tt.match {
				t.Fatalf("fixedPodSpecMatches = %v, want %v", m, tt.match)
			}
		})
	}
}

// TestFixedPodSpecSchedulingGates checks that scheduling gates are compared
// exactly.
func TestFixedPodSpecSchedulingGates(t *testing.T) {
	gate := func(names ...string) []corev1.PodSchedulingGate {
		var gates []corev1.PodSchedulingGate
		for _, name := range names {
			gates = append(gates, corev1.PodSchedulingGate{Name: name})
		}
		return gates
	}
	const g = "skygo.tmc.dev/worker"
	for _, tt := range []struct {
		name      string
		want, got []corev1.PodSchedulingGate
		match     bool
	}{
		{"none", nil, nil, true},
		{"nil and empty", nil, []corev1.PodSchedulingGate{}, true},
		{"unexpected gate", nil, gate(g), false},
		{"missing gate", gate(g), nil, false},
		{"same gate", gate(g), gate(g), true},
		{"extra gate", gate(g), gate(g, "example.com/other"), false},
		{"different gate", gate(g), gate("example.com/other"), false},
		{"duplicate gate", gate(g), gate(g, g), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, got := fixedPodWant(t), fixedPodGot(t)
			want.SchedulingGates, got.SchedulingGates = tt.want, tt.got
			if m := fixedPodMatchesUnchanged(t, want, got); m != tt.match {
				t.Fatalf("fixedPodSpecMatches = %v, want %v", m, tt.match)
			}
		})
	}
}
