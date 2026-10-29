package kube

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// An owned object is accepted only as exactly its rendering. Its identity
// projection is its name, namespace, labels, annotations, owner references
// and whole spec; the witness is the SHA-256 of the projection's JSON. The
// witness of the rendering is fixed in the Native with the declaration,
// before the create is sent, and every object the runtime accepts — the
// create's acknowledgment, the object recovered after a lost response, and
// each object read to verify, observe, delete or forward — must have it.
//
// The renderer sets every field the API server would otherwise default, so
// the projection removes only these documented differences, none of which
// changes what runs:
//
//   - Deployment: the deployment controller's revision annotation.
//   - Pod templates and Pods: the spelling of resource quantities, which are
//     compared by exact value (see exactQuantity), and the ResizePolicy that
//     pkg/apis/core/v1
//     defaulting adds, NotRequired for cpu and memory, when the
//     InPlacePodVerticalScaling feature is enabled.
//   - Pods: the ReplicaSet controller's pod-template-hash label; the
//     scheduler's spec.nodeName, only with a PodScheduled condition True,
//     which is placement and not proof of any node or device
//     qualification; the DefaultTolerationSeconds admission plugin's
//     not-ready and unreachable NoExecute tolerations of 300 seconds; and the
//     Priority admission plugin's priority 0 and PreemptLowerPriority policy
//     with no priority class.
//   - Services: a single allocated cluster IP and its single IP family.
//
// Everything else is compared exactly: replicas, selector, strategy, host
// namespaces, service account, environment and its sources, volumes and
// mounts, security contexts, init and ephemeral containers, devices,
// resource claims and every resource, node selection, tolerations,
// priority, ports and target ports.

// identity is an object's identity projection.
type identity struct {
	Name        string                  `json:"name"`
	Namespace   string                  `json:"namespace"`
	Labels      map[string]string       `json:"labels"`
	Annotations map[string]string       `json:"annotations"`
	Owners      []metav1.OwnerReference `json:"owners"`
	Spec        any                     `json:"spec"`
}

// witness returns the SHA-256 of p's JSON, or "" if it does not encode,
// which no witness equals.
func (p identity) witness() string {
	raw, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// deploymentRevision is the annotation the deployment controller sets.
const deploymentRevision = "deployment.kubernetes.io/revision"

func deploymentIdentity(d *appsv1.Deployment) identity {
	annotations := maps.Clone(d.Annotations)
	delete(annotations, deploymentRevision)
	spec := *d.Spec.DeepCopy()
	spec.Template.Spec = canonicalPodSpec(spec.Template.Spec)
	return identity{
		Name:        d.Name,
		Namespace:   d.Namespace,
		Labels:      nonEmpty(d.Labels),
		Annotations: nonEmpty(annotations),
		Owners:      d.OwnerReferences,
		Spec:        spec,
	}
}

// templateIdentity is the projection of the Pods of template t in
// namespace.
func templateIdentity(namespace string, t corev1.PodTemplateSpec) identity {
	return identity{
		Namespace:   namespace,
		Labels:      nonEmpty(t.Labels),
		Annotations: nonEmpty(t.Annotations),
		Owners:      nil,
		Spec:        canonicalPodSpec(t.Spec),
	}
}

// podIdentity is the projection of pod as a Pod of its template: its
// controller owner, which ownedPods checks, and the documented scheduler,
// admission and controller additions are removed.
func podIdentity(pod *corev1.Pod) identity {
	labels := maps.Clone(pod.Labels)
	delete(labels, appsv1.DefaultDeploymentUniqueLabelKey)
	var owners []metav1.OwnerReference
	for _, o := range pod.OwnerReferences {
		if o.Controller == nil || !*o.Controller {
			owners = append(owners, o)
		}
	}
	s := canonicalPodSpec(pod.Spec)
	if s.NodeName != "" && podScheduled(pod) {
		s.NodeName = ""
	}
	s.Tolerations = withoutDefaultTolerations(s.Tolerations)
	if s.PriorityClassName == "" && s.Priority != nil && *s.Priority == 0 {
		s.Priority = nil
	}
	if s.PriorityClassName == "" && s.PreemptionPolicy != nil && *s.PreemptionPolicy == corev1.PreemptLowerPriority {
		s.PreemptionPolicy = nil
	}
	return identity{
		Namespace:   pod.Namespace,
		Labels:      nonEmpty(labels),
		Annotations: nonEmpty(pod.Annotations),
		Owners:      owners,
		Spec:        s,
	}
}

func serviceIdentity(s *corev1.Service) identity {
	spec := *s.Spec.DeepCopy()
	if spec.ClusterIP != corev1.ClusterIPNone && len(spec.ClusterIPs) <= 1 &&
		(len(spec.ClusterIPs) == 0 || spec.ClusterIPs[0] == spec.ClusterIP) {
		spec.ClusterIP, spec.ClusterIPs = "", nil
	}
	if len(spec.IPFamilies) == 1 {
		spec.IPFamilies = nil
	}
	return identity{
		Name:        s.Name,
		Namespace:   s.Namespace,
		Labels:      nonEmpty(s.Labels),
		Annotations: nonEmpty(s.Annotations),
		Owners:      s.OwnerReferences,
		Spec:        spec,
	}
}

// canonicalPodSpec returns a copy of s with every resource quantity — of
// init, regular and ephemeral containers, of the Pod and of its overhead —
// in its exact spelling, and a defaulted ResizePolicy removed.
func canonicalPodSpec(s corev1.PodSpec) corev1.PodSpec {
	s = *s.DeepCopy()
	for _, cs := range [][]corev1.Container{s.InitContainers, s.Containers} {
		for i := range cs {
			canonicalResources(&cs[i].Resources)
			if reflect.DeepEqual(cs[i].ResizePolicy, defaultResizePolicy) {
				cs[i].ResizePolicy = nil
			}
		}
	}
	for i := range s.EphemeralContainers {
		canonicalResources(&s.EphemeralContainers[i].Resources)
	}
	if s.Resources != nil {
		canonicalResources(s.Resources)
	}
	canonicalList(s.Overhead)
	return s
}

var defaultResizePolicy = []corev1.ContainerResizePolicy{
	{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired},
	{ResourceName: corev1.ResourceMemory, RestartPolicy: corev1.NotRequired},
}

func canonicalResources(r *corev1.ResourceRequirements) {
	canonicalList(r.Limits)
	canonicalList(r.Requests)
}

// canonicalList replaces each quantity of l with exactQuantity's. Its
// resource names are kept as they are: a name added, removed or changed
// changes the projection.
func canonicalList(l corev1.ResourceList) {
	for name, q := range l {
		l[name] = exactQuantity(q)
	}
}

// exactQuantity returns q's whole decimal value in one spelling, the
// DecimalExponent form of its unscaled mantissa without trailing zeros and
// an exponent that is a multiple of three. Nothing is rounded and nothing
// overflows, so two quantities have the same spelling exactly when
// Quantity.Cmp reports them equal: 256Mi and 268435456 share one, while
// 268435455.9999 and 268435456 do not.
func exactQuantity(q resource.Quantity) resource.Quantity {
	if q.IsZero() {
		return resource.Quantity{Format: resource.DecimalExponent}
	}
	return *resource.NewDecimalQuantity(*q.AsDec(), resource.DecimalExponent)
}

// defaultTolerations are those of the DefaultTolerationSeconds admission
// plugin.
var defaultTolerations = func() []corev1.Toleration {
	seconds := int64(300)
	var out []corev1.Toleration
	for _, key := range []string{corev1.TaintNodeNotReady, corev1.TaintNodeUnreachable} {
		out = append(out, corev1.Toleration{Key: key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds})
	}
	return out
}()

func withoutDefaultTolerations(ts []corev1.Toleration) []corev1.Toleration {
	var out []corev1.Toleration
	for _, t := range ts {
		builtin := false
		for _, d := range defaultTolerations {
			if reflect.DeepEqual(t, d) {
				builtin = true
			}
		}
		if !builtin {
			out = append(out, t)
		}
	}
	return out
}

// podScheduled reports whether pod has a PodScheduled condition True.
func podScheduled(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodScheduled {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func nonEmpty(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return m
}

// accepts reports whether an object read with uid and projection witness
// is the one o records: its rendering, and the recorded UID if one is.
func (o serviceObject) accepts(uid types.UID, witness string) bool {
	return uid != "" && o.Spec != "" && witness == o.Spec && (o.UID == "" || uid == o.UID)
}
