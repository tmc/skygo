package kube

import (
	"maps"
	"slices"
	"strconv"

	"github.com/tmc/skygo/internal/serving"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// serviceDeployment renders the owned Deployment of service ref at revision
// rev and rollout generation, created under declaration. It runs exactly one
// replica and rolls out with no surge, so the admission charge of one
// replica is never exceeded. The Pod uses the profile's borrowed service
// account without its token, the profile's pull secrets and a restricted
// security context. It sets every field the API server would default, so
// that the stored object's projection is the rendering's (see
// serving_accept.go).
func serviceDeployment(namespace string, ref serving.Ref, rev serving.Revision, generation uint64, declaration string, p serviceProfile) *appsv1.Deployment {
	s := rev.Spec
	labels := serviceSelector(ref)
	one := int32(1)
	zero := intstr.FromInt32(0)
	unavailable := intstr.FromInt32(1)
	falseValue, trueValue := false, true
	grace := int64(s.Rollout.TerminationGraceSeconds)
	container := corev1.Container{
		Name:    serviceContainer,
		Image:   s.Image,
		Command: slices.Clone(s.Argv),
		Ports:   []corev1.ContainerPort{{Name: "http", ContainerPort: s.Port, Protocol: corev1.ProtocolTCP}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: s.Readiness.Path, Port: intstr.FromInt32(s.Port), Scheme: corev1.URISchemeHTTP,
			}},
			InitialDelaySeconds: s.Readiness.InitialDelaySeconds,
			PeriodSeconds:       s.Readiness.PeriodSeconds,
			TimeoutSeconds:      s.Readiness.TimeoutSeconds,
			FailureThreshold:    s.Readiness.FailureThreshold,
			SuccessThreshold:    1,
		},
		Resources: serviceResources(s.Resources),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: &falseValue,
			RunAsNonRoot:             &trueValue,
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		TerminationMessagePath:   corev1.TerminationMessagePathDefault,
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		ImagePullPolicy:          corev1.PullIfNotPresent,
	}
	for _, e := range s.Env {
		container.Env = append(container.Env, corev1.EnvVar{Name: e.Name, Value: e.Value})
	}
	var pullSecrets []corev1.LocalObjectReference
	for _, name := range p.pullSecrets {
		pullSecrets = append(pullSecrets, corev1.LocalObjectReference{Name: name})
	}
	var nodeSelector map[string]string
	for _, l := range s.NodeSelector {
		if nodeSelector == nil {
			nodeSelector = map[string]string{}
		}
		nodeSelector[l.Key] = l.Value
	}
	podLabels := maps.Clone(labels)
	podLabels[serviceOwnerLabel] = ref.Owner
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceObjectName(ref.Service),
			Namespace: namespace,
			Labels:    podLabels,
			Annotations: map[string]string{
				serviceDeclaration: declaration,
				serviceGeneration:  strconv.FormatUint(generation, 10),
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Strategy: appsv1.DeploymentStrategy{
				Type:          appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{MaxSurge: &zero, MaxUnavailable: &unavailable},
			},
			ProgressDeadlineSeconds: &s.Rollout.ProgressDeadlineSeconds,
			RevisionHistoryLimit:    &one,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec: corev1.PodSpec{
					Containers:                    []corev1.Container{container},
					ServiceAccountName:            environmentServiceAccount,
					DeprecatedServiceAccount:      environmentServiceAccount,
					AutomountServiceAccountToken:  &falseValue,
					ImagePullSecrets:              pullSecrets,
					NodeSelector:                  nodeSelector,
					TerminationGracePeriodSeconds: &grace,
					EnableServiceLinks:            &falseValue,
					RestartPolicy:                 corev1.RestartPolicyAlways,
					DNSPolicy:                     corev1.DNSClusterFirst,
					SchedulerName:                 corev1.DefaultSchedulerName,
					SecurityContext:               &corev1.PodSecurityContext{},
				},
			},
		},
	}
}

func serviceResources(r serving.Resources) corev1.ResourceRequirements {
	out := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(r.CPUMillis, resource.DecimalSI),
			corev1.ResourceMemory: *resource.NewQuantity(r.MemoryBytes, resource.BinarySI),
		},
	}
	if r.CPULimitMillis != 0 || r.MemoryLimitBytes != 0 {
		out.Limits = corev1.ResourceList{}
	}
	if r.CPULimitMillis != 0 {
		out.Limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(r.CPULimitMillis, resource.DecimalSI)
	}
	if r.MemoryLimitBytes != 0 {
		out.Limits[corev1.ResourceMemory] = *resource.NewQuantity(r.MemoryLimitBytes, resource.BinarySI)
	}
	return out
}

// serviceService renders the owned ClusterIP Service of service ref. It
// exists so that ready endpoints are observable; the forward route reaches
// a Pod over the API server's port-forward, not through the Service.
func serviceService(namespace string, ref serving.Ref, port int32, declaration string) *corev1.Service {
	labels := serviceSelector(ref)
	meta := maps.Clone(labels)
	meta[serviceOwnerLabel] = ref.Owner
	single := corev1.IPFamilyPolicySingleStack
	cluster := corev1.ServiceInternalTrafficPolicyCluster
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceObjectName(ref.Service),
			Namespace:   namespace,
			Labels:      meta,
			Annotations: map[string]string{serviceDeclaration: declaration},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name: "http", Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP,
			}},
			SessionAffinity:       corev1.ServiceAffinityNone,
			IPFamilyPolicy:        &single,
			InternalTrafficPolicy: &cluster,
		},
	}
}

// ownedPods returns the Pods of Deployment d: those in d's namespace whose
// controller is a ReplicaSet, in that namespace, whose controller is d's
// UID. Labels only narrow the lists.
func ownedPods(rsList []appsv1.ReplicaSet, pods []corev1.Pod, d *appsv1.Deployment) []corev1.Pod {
	sets := map[types.UID]bool{}
	for _, rs := range rsList {
		c := metav1.GetControllerOf(&rs)
		if rs.Namespace == d.Namespace && rs.UID != "" && c != nil && c.Kind == "Deployment" && c.UID == d.UID {
			sets[rs.UID] = true
		}
	}
	var out []corev1.Pod
	for _, p := range pods {
		c := metav1.GetControllerOf(&p)
		if p.Namespace == d.Namespace && p.UID != "" && c != nil && c.Kind == "ReplicaSet" && sets[c.UID] {
			out = append(out, p)
		}
	}
	return out
}

// podReady reports whether pod is running, ready and not deleting.
func podReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// readyEndpoints returns the UIDs of Pods that slices in namespace list as
// ready, not terminating, endpoints of the Service of uid, on a slice port
// "http" of TCP port.
func readyEndpoints(eps []discoveryv1.EndpointSlice, namespace, service string, uid types.UID, port int32) map[types.UID]bool {
	out := map[types.UID]bool{}
	for _, s := range eps {
		if s.Namespace != namespace || s.Labels[discoveryv1.LabelServiceName] != service {
			continue
		}
		if c := metav1.GetControllerOf(&s); c == nil || c.Kind != "Service" || c.UID != uid {
			continue
		}
		if s.AddressType != discoveryv1.AddressTypeIPv4 && s.AddressType != discoveryv1.AddressTypeIPv6 || !slicePort(s.Ports, port) {
			continue
		}
		for _, e := range s.Endpoints {
			c := e.Conditions
			if c.Ready == nil || !*c.Ready || c.Terminating != nil && *c.Terminating {
				continue
			}
			if t := e.TargetRef; t != nil && t.Kind == "Pod" && t.UID != "" && (t.Namespace == "" || t.Namespace == namespace) {
				out[t.UID] = true
			}
		}
	}
	return out
}

func slicePort(ports []discoveryv1.EndpointPort, port int32) bool {
	for _, p := range ports {
		if p.Name != nil && *p.Name == "http" && p.Port != nil && *p.Port == port && (p.Protocol == nil || *p.Protocol == corev1.ProtocolTCP) {
			return true
		}
	}
	return false
}

// serviceReason classifies a not-ready Pod's container state.
func serviceReason(pods []corev1.Pod) serving.Reason {
	for _, p := range pods {
		for _, s := range p.Status.ContainerStatuses {
			if w := s.State.Waiting; w != nil {
				switch w.Reason {
				case "ErrImagePull":
					return serving.ImagePull
				case "ImagePullBackOff":
					return serving.ImagePullBackOff
				case "CrashLoopBackOff":
					return serving.CrashLoop
				case "CreateContainerConfigError", "CreateContainerError":
					return serving.ContainerConfig
				case "InvalidImageName":
					return serving.InvalidImage
				}
			}
		}
	}
	return serving.NoReason
}

// progressDeadline reports whether d's rollout exceeded its deadline.
func progressDeadline(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded" {
			return true
		}
	}
	return false
}
