package kube

import (
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation"
)

// fixedPodSpecMatches reports whether an observed Pod spec got equals the
// explicit fixed-profile spec want. Callers build want with every scheduling
// field explicit: enableServiceLinks false, preemptionPolicy
// PreemptLowerPriority, priority 0 with no priority class, exactly the two
// standard 300s NoExecute not-ready and unreachable tolerations, ClusterFirst,
// default-scheduler, 30s grace, explicit imagePullPolicy, no service account
// token and no process namespace sharing. Callers also set image pull secrets
// explicitly, so that ServiceAccount admission does not copy a list into the
// Pod.
//
// Every typed field of the two specs must be equal except these API
// representations, compared on deep copies:
//
//   - the same two standard tolerations in the other order
//   - the tolerations that ExtendedResourceToleration admission adds: for
//     every extended resource requested by a container or init container of
//     want and not already tolerated exactly by want, one toleration with
//     that key, operator Exists, no value, effect NoSchedule and no seconds,
//     all present exactly once or all absent
//   - serviceAccountName and serviceAccount "default" when want names no
//     service account and sets automountServiceAccountToken false
//   - a nil or empty pod security context when want's is nil or empty
//   - container and init container terminationMessagePath
//     /dev/termination-log and terminationMessagePolicy File when want
//     leaves them empty
//   - a nil shareProcessNamespace or priority when want's is false or 0
//   - a nodeName assigned by the scheduler when want has none
//
// The scheduler's node assignment is a placement result only; it does not
// qualify the node. A nodeName in want is compared exactly. Scheduling gates
// and image pull secrets are compared exactly. Any other toleration, priority
// class, affinity, image pull secret or policy, resize policy, container,
// argument, environment source, probe, volume, resource claim or security
// difference is a mismatch, as is any typed field this list does not name.
// Fields that the corev1 Go types do not declare are dropped when the Pod is
// decoded, so this cannot detect them. A nil spec never matches, and neither
// argument is modified.
func fixedPodSpecMatches(want, got *corev1.PodSpec) bool {
	if want == nil || got == nil {
		return false
	}
	w, g := want.DeepCopy(), got.DeepCopy()

	seconds := int64(300)
	standard := []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	reversed := []corev1.Toleration{standard[1], standard[0]}
	isStandard := func(t []corev1.Toleration) bool {
		return equality.Semantic.DeepEqual(t, standard) || equality.Semantic.DeepEqual(t, reversed)
	}
	g.Tolerations = withoutDerivedTolerations(w, g.Tolerations)
	if isStandard(w.Tolerations) && isStandard(g.Tolerations) {
		g.Tolerations = w.Tolerations
	}

	if w.ServiceAccountName == "" && w.DeprecatedServiceAccount == "" &&
		w.AutomountServiceAccountToken != nil && !*w.AutomountServiceAccountToken &&
		g.ServiceAccountName == "default" && g.DeprecatedServiceAccount == "default" {
		g.ServiceAccountName, g.DeprecatedServiceAccount = "", ""
	}
	empty := &corev1.PodSecurityContext{}
	if (w.SecurityContext == nil || equality.Semantic.DeepEqual(w.SecurityContext, empty)) &&
		(g.SecurityContext == nil || equality.Semantic.DeepEqual(g.SecurityContext, empty)) {
		g.SecurityContext = w.SecurityContext
	}
	if g.ShareProcessNamespace == nil && w.ShareProcessNamespace != nil && !*w.ShareProcessNamespace {
		g.ShareProcessNamespace = w.ShareProcessNamespace
	}
	if g.Priority == nil && w.Priority != nil && *w.Priority == 0 {
		g.Priority = w.Priority
	}
	if w.NodeName == "" {
		g.NodeName = ""
	}
	// The termination message path is a kubelet bind mount; accept only its
	// default location and policy.
	termination := func(g, w []corev1.Container) {
		if len(g) != len(w) {
			return
		}
		for i := range g {
			if w[i].TerminationMessagePath == "" && g[i].TerminationMessagePath == corev1.TerminationMessagePathDefault {
				g[i].TerminationMessagePath = ""
			}
			if w[i].TerminationMessagePolicy == "" && g[i].TerminationMessagePolicy == corev1.TerminationMessageReadFile {
				g[i].TerminationMessagePolicy = ""
			}
		}
	}
	termination(g.InitContainers, w.InitContainers)
	termination(g.Containers, w.Containers)
	return equality.Semantic.DeepEqual(w, g)
}

// withoutDerivedTolerations returns got without the tolerations that
// ExtendedResourceToleration admission derives from want's requests, when
// each one that want does not already have appears exactly once in got.
// Otherwise it returns got unchanged.
func withoutDerivedTolerations(want *corev1.PodSpec, got []corev1.Toleration) []corev1.Toleration {
	same := func(a corev1.Toleration) func(corev1.Toleration) bool {
		return func(b corev1.Toleration) bool { return equality.Semantic.DeepEqual(a, b) }
	}
	var derived []corev1.Toleration
	for _, c := range slices.Concat(want.Containers, want.InitContainers) {
		for name := range c.Resources.Requests {
			t := corev1.Toleration{Key: string(name), Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
			if extendedResourceName(name) && !slices.ContainsFunc(derived, same(t)) && !slices.ContainsFunc(want.Tolerations, same(t)) {
				derived = append(derived, t)
			}
		}
	}
	if len(derived) == 0 {
		return got
	}
	count := make([]int, len(derived))
	var rest []corev1.Toleration
	for _, t := range got {
		if i := slices.IndexFunc(derived, same(t)); i >= 0 {
			count[i]++
			continue
		}
		rest = append(rest, t)
	}
	for _, n := range count {
		if n != 1 {
			return got
		}
	}
	return rest
}

// extendedResourceName reports whether name is an extended resource, as
// Kubernetes IsExtendedResourceName defines it: prefixed, not in the
// kubernetes.io namespace, not prefixed "requests.", and a qualified name
// once prefixed "requests.".
func extendedResourceName(name corev1.ResourceName) bool {
	s := string(name)
	if !strings.Contains(s, "/") || strings.Contains(s, corev1.ResourceDefaultNamespacePrefix) ||
		strings.HasPrefix(s, corev1.DefaultResourceRequestsPrefix) {
		return false
	}
	return len(validation.IsQualifiedName(corev1.DefaultResourceRequestsPrefix+s)) == 0
}
