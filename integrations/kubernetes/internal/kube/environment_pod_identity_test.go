package kube

import (
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// environmentObserved holds modeled API objects for one environment. The UIDs,
// generation and resource version are synthetic comparison fixtures, not
// evidence of a Kubernetes allocation or of a running Pod.
type environmentObserved struct {
	identity  environmentPodIdentity
	pod       *corev1.Pod
	namespace *corev1.Namespace
	pvc       *corev1.PersistentVolumeClaim
}

// environmentTestObserved returns the builder's Pod as the API server returns
// it: with the defaults that S10/pod-defaults-native-c2 recorded for the fixed
// profile (container termination message path and policy, the default service
// account and alias, an empty pod security context) and an assigned UID,
// generation and resource version.
func environmentTestObserved(t *testing.T, b *Backend) environmentObserved {
	t.Helper()
	pod, err := b.environmentPod(environmentTestInput(), environmentTestProfile())
	if err != nil {
		t.Fatal(err)
	}
	pod.UID, pod.Generation, pod.ResourceVersion = "pod-uid-1", 1, "100"
	pod.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	pod.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	pod.Spec.ServiceAccountName, pod.Spec.DeprecatedServiceAccount = "default", "default"
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	pod.Status.Phase = corev1.PodPending
	filesystem := corev1.PersistentVolumeFilesystem
	return environmentObserved{
		identity: environmentPodIdentity{podUID: "pod-uid-1", podGeneration: 1},
		pod:      pod,
		namespace: &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "workloads", UID: "namespace-uid-1"},
			Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
		},
		pvc: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "alice-home", Namespace: "workloads", UID: "claim-uid-1"},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeName:  "pv-alice-home", VolumeMode: &filesystem,
			},
			Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
		},
	}
}

func (o environmentObserved) match(b *Backend, in environmentPodInput, p environmentPodProfile) error {
	return b.matchEnvironmentPod(in, p, o.identity, o.pod, o.namespace, o.pvc)
}

func TestMatchEnvironmentPod(t *testing.T) {
	b := environmentTestBackend(t)
	now := metav1.Now()
	for _, tt := range []struct {
		name   string
		mutate func(*environmentObserved)
	}{
		{"original", func(*environmentObserved) {}},
		{"resource version", func(o *environmentObserved) {
			o.pod.ResourceVersion, o.namespace.ResourceVersion, o.pvc.ResourceVersion = "200", "201", "202"
		}},
		{"scheduled", func(o *environmentObserved) { o.pod.Spec.NodeName = "node-a" }},
		{"running", func(o *environmentObserved) {
			o.pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"}
		}},
		{"failed", func(o *environmentObserved) { o.pod.Status.Phase = corev1.PodFailed }},
		{"server annotation", func(o *environmentObserved) { o.pod.Annotations["example.com/x"] = "y" }},
		{"tolerations reversed", func(o *environmentObserved) {
			o.pod.Spec.Tolerations[0], o.pod.Spec.Tolerations[1] = o.pod.Spec.Tolerations[1], o.pod.Spec.Tolerations[0]
		}},
		{"nil volume mode", func(o *environmentObserved) { o.pvc.Spec.VolumeMode = nil }},
		{"zero generation", func(o *environmentObserved) { o.pod.Generation, o.identity.podGeneration = 0, 0 }},
		{"namespace finalizer", func(o *environmentObserved) {
			o.namespace.Spec.Finalizers = []corev1.FinalizerName{corev1.FinalizerKubernetes}
		}},
		{"claim creation time", func(o *environmentObserved) { o.pvc.CreationTimestamp = now }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			tt.mutate(&o)
			if err := o.match(b, environmentTestInput(), environmentTestProfile()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestMatchEnvironmentPodExtendedResource checks a GPU Pod read back with and
// without the toleration that ExtendedResourceToleration admission adds.
func TestMatchEnvironmentPodExtendedResource(t *testing.T) {
	b := environmentTestBackend(t)
	in, p := environmentTestInput(), environmentTestProfile()
	in.task.Accelerator, in.task.AcceleratorCount = "nvidia", 1
	in.task.NodeSelector = map[string]string{"pool": "gpu"}
	derived := corev1.Toleration{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}
	for _, tt := range []struct {
		name  string
		extra []corev1.Toleration
		match bool
	}{
		{"plugin absent", nil, true},
		{"plugin enabled", []corev1.Toleration{derived}, true},
		{"duplicate", []corev1.Toleration{derived, derived}, false},
		{"other resource", []corev1.Toleration{{Key: "example.com/fpga", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			pod, err := b.environmentPod(in, p)
			if err != nil {
				t.Fatal(err)
			}
			pod.ObjectMeta, pod.Spec.Containers[0].TerminationMessagePath, pod.Spec.Containers[0].TerminationMessagePolicy =
				o.pod.ObjectMeta, corev1.TerminationMessagePathDefault, corev1.TerminationMessageReadFile
			pod.Spec.Tolerations = append(pod.Spec.Tolerations, tt.extra...)
			o.pod = pod
			err = o.match(b, in, p)
			if tt.match && err != nil || !tt.match && !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("match = %v, want match %v", err, tt.match)
			}
		})
	}
}

func TestMatchEnvironmentPodMismatch(t *testing.T) {
	b := environmentTestBackend(t)
	now := metav1.Now()
	yes := true
	block := corev1.PersistentVolumeBlock
	for _, tt := range []struct {
		name   string
		mutate func(*environmentObserved, *environmentPodInput, *environmentPodProfile)
	}{
		{"pod uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.UID = "pod-uid-2"
		}},
		{"identity uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.identity.podUID = "pod-uid-2"
		}},
		{"pod generation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Generation = 2
		}},
		{"pod name", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Name = "skygo-env-other"
		}},
		{"pod namespace", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Namespace = "other"
		}},
		{"owner label", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Labels[ownerLabel] = "other"
		}},
		{"id label", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Labels[idLabel] = "fedcba9876543210fedcba9876543210"
		}},
		{"extra label", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Labels["app"] = "x"
		}},
		{"binding annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Annotations[bindingAnnotation] = "sha256:other"
		}},
		{"missing generation annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			delete(o.pod.Annotations, environmentGenerationAnnotation)
		}},
		{"spec annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Annotations[environmentSpecAnnotation] = "00"
		}},
		{"revision annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Annotations[environmentRevisionAnnotation] = "profile-2"
		}},
		{"extra skygo annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Annotations["skygo.tmc.dev/task-sha256"] = "x"
		}},
		{"apparmor annotation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Annotations["container.apparmor.security.beta.kubernetes.io/workspace"] = "unconfined"
		}},
		{"owner reference", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: "j", UID: "u", Controller: &yes}}
		}},
		{"input spec hash", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			in.specHash = sha256.Sum256([]byte("other spec"))
		}},
		{"input resources", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			in.task.CPUMillis = 400
		}},
		{"input id", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			in.id = "fedcba9876543210fedcba9876543210"
		}},
		{"profile revision", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.revision = "profile-2"
		}},
		{"profile idle command", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.idleArgv = []string{"/usr/local/bin/idle"}
		}},
		{"profile claim name", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.pvcName = "bob-home"
		}},
		{"profile claim uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.pvcUID = "claim-uid-2"
		}},
		{"profile volume", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.volumeName = "pv-other"
		}},
		{"profile access mode", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.accessMode = corev1.ReadWriteOncePod
		}},
		{"profile namespace uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.namespaceUID = "namespace-uid-2"
		}},
		{"namespace name", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.namespace.Name = "other"
		}},
		{"namespace uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.namespace.UID = "namespace-uid-2"
		}},
		{"claim namespace", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Namespace = "other"
		}},
		{"claim name", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Name = "other"
		}},
		{"claim uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.UID = "claim-uid-2"
		}},
		{"claim volume", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Spec.VolumeName = "pv-other"
		}},
		{"claim no access modes", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Spec.AccessModes = nil
		}},
		{"claim read write many", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany}
		}},
		{"claim extra access mode", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Spec.AccessModes = append(o.pvc.Spec.AccessModes, corev1.ReadOnlyMany)
		}},
		{"claim block", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Spec.VolumeMode = &block
		}},
		{"deleting foreign pod", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.DeletionTimestamp = &now
			o.pod.UID = "pod-uid-2"
		}},
		{"unbound foreign claim", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pvc.Status.Phase = corev1.ClaimPending
			o.pvc.UID = "claim-uid-2"
		}},
		{"foreign pod without namespace", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.namespace = nil
			o.pod.Labels[ownerLabel] = "other"
		}},
		{"foreign claim without pod", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod = nil
			o.pvc.UID = "claim-uid-2"
		}},
		// Spec drift; the field table in pod_identity_test.go covers the rest.
		{"sidecar", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Containers = append(o.pod.Spec.Containers, corev1.Container{Name: "proxy", Image: "proxy"})
		}},
		{"injected toleration", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Tolerations = append(o.pod.Spec.Tolerations, corev1.Toleration{Operator: corev1.TolerationOpExists})
		}},
		{"image pull secret", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
		}},
		{"priority class", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.PriorityClassName = "high"
		}},
		{"affinity", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Affinity = &corev1.Affinity{}
		}},
		{"service links", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.EnableServiceLinks = &yes
		}},
		{"container security", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Containers[0].SecurityContext.RunAsGroup = nil
		}},
		{"claim read only", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly = true
		}},
		{"claim name in pod", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = "bob-home"
		}},
		{"scheduling gate", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "skygo.tmc.dev/worker"}}
		}},
		{"admission pull secret", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.pod.Spec.ImagePullSecrets = append(o.pod.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: "copied"})
		}},
		{"pull secrets reordered", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			s := o.pod.Spec.ImagePullSecrets
			s[0], s[1] = s[1], s[0]
		}},
		{"profile pull secrets", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.pullSecrets = []string{"registry-b"}
		}},
		{"empty profile admission copied", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.pullSecrets = nil
		}},
		{"deadline", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			deadline := int64(60)
			o.pod.Spec.ActiveDeadlineSeconds = &deadline
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			in, p := environmentTestInput(), environmentTestProfile()
			if err := o.match(b, in, p); err != nil {
				t.Fatalf("baseline: %v", err)
			}
			tt.mutate(&o, &in, &p)
			err := o.match(b, in, p)
			if !errors.Is(err, backend.ErrIdentityMismatch) {
				t.Fatalf("match = %v, want identity mismatch", err)
			}
			if errors.Is(err, errEnvironmentObjectMissing) || errors.Is(err, errEnvironmentObjectUnavailable) {
				t.Fatalf("match = %v, want only identity mismatch", err)
			}
		})
	}
}

func TestMatchEnvironmentPodMissing(t *testing.T) {
	b := environmentTestBackend(t)
	now := metav1.Now()
	for _, tt := range []struct {
		name   string
		mutate func(*environmentObserved)
	}{
		{"pod", func(o *environmentObserved) { o.pod = nil }},
		{"claim", func(o *environmentObserved) { o.pvc = nil }},
		{"namespace", func(o *environmentObserved) { o.namespace = nil }},
		{"all", func(o *environmentObserved) { o.pod, o.pvc, o.namespace = nil, nil, nil }},
		{"pod with deleting namespace", func(o *environmentObserved) {
			o.pod = nil
			o.namespace.DeletionTimestamp = &now
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			tt.mutate(&o)
			err := o.match(b, environmentTestInput(), environmentTestProfile())
			if !errors.Is(err, errEnvironmentObjectMissing) {
				t.Fatalf("match = %v, want missing", err)
			}
			if errors.Is(err, backend.ErrIdentityMismatch) || errors.Is(err, errEnvironmentObjectUnavailable) {
				t.Fatalf("match = %v, want only missing", err)
			}
		})
	}
}

func TestMatchEnvironmentPodUnavailable(t *testing.T) {
	b := environmentTestBackend(t)
	now := metav1.Now()
	for _, tt := range []struct {
		name   string
		mutate func(*environmentObserved)
	}{
		{"pod deleting", func(o *environmentObserved) { o.pod.DeletionTimestamp = &now }},
		{"claim deleting", func(o *environmentObserved) { o.pvc.DeletionTimestamp = &now }},
		{"namespace deleting", func(o *environmentObserved) { o.namespace.DeletionTimestamp = &now }},
		{"namespace terminating", func(o *environmentObserved) {
			o.namespace.Status.Phase = corev1.NamespaceTerminating
		}},
		{"claim pending", func(o *environmentObserved) {
			o.pvc.Status.Phase = corev1.ClaimPending
			o.pvc.Spec.VolumeName = ""
		}},
		{"claim pending with volume", func(o *environmentObserved) { o.pvc.Status.Phase = corev1.ClaimPending }},
		{"claim lost", func(o *environmentObserved) { o.pvc.Status.Phase = corev1.ClaimLost }},
		{"claim bound without volume", func(o *environmentObserved) { o.pvc.Spec.VolumeName = "" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			tt.mutate(&o)
			err := o.match(b, environmentTestInput(), environmentTestProfile())
			if !errors.Is(err, errEnvironmentObjectUnavailable) {
				t.Fatalf("match = %v, want unavailable", err)
			}
			if errors.Is(err, backend.ErrIdentityMismatch) || errors.Is(err, errEnvironmentObjectMissing) {
				t.Fatalf("match = %v, want only unavailable", err)
			}
		})
	}
}

func TestMatchEnvironmentPodInvalid(t *testing.T) {
	b := environmentTestBackend(t)
	for _, tt := range []struct {
		name   string
		mutate func(*environmentObserved, *environmentPodInput, *environmentPodProfile)
	}{
		{"empty identity uid", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.identity.podUID = ""
		}},
		{"negative identity generation", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.identity.podGeneration, o.pod.Generation = -1, -1
		}},
		{"invalid input", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			in.generation = 2
		}},
		{"invalid profile", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			p.image = "busybox"
		}},
		{"invalid identity and missing pod", func(o *environmentObserved, in *environmentPodInput, p *environmentPodProfile) {
			o.identity.podUID, o.pod = "", nil
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := environmentTestObserved(t, b)
			in, p := environmentTestInput(), environmentTestProfile()
			tt.mutate(&o, &in, &p)
			err := o.match(b, in, p)
			if err == nil || errors.Is(err, backend.ErrIdentityMismatch) ||
				errors.Is(err, errEnvironmentObjectMissing) || errors.Is(err, errEnvironmentObjectUnavailable) {
				t.Fatalf("match = %v, want a validation error", err)
			}
		})
	}
}

// TestMatchEnvironmentPodArguments checks that matching modifies no argument.
func TestMatchEnvironmentPodArguments(t *testing.T) {
	b := environmentTestBackend(t)
	o := environmentTestObserved(t, b)
	o.pod.Spec.NodeName = "node-a"
	o.pod.Spec.Tolerations[0], o.pod.Spec.Tolerations[1] = o.pod.Spec.Tolerations[1], o.pod.Spec.Tolerations[0]
	o.pod.Spec.SecurityContext = nil
	pod, namespace, pvc := o.pod.DeepCopy(), o.namespace.DeepCopy(), o.pvc.DeepCopy()
	in, p := environmentTestInput(), environmentTestProfile()
	if err := o.match(b, in, p); err != nil {
		t.Fatal(err)
	}
	if !equality.Semantic.DeepEqual(o.pod, pod) || !equality.Semantic.DeepEqual(o.namespace, namespace) ||
		!equality.Semantic.DeepEqual(o.pvc, pvc) {
		t.Fatal("match modified an observed object")
	}
	if !reflect.DeepEqual(in, environmentTestInput()) || !reflect.DeepEqual(p, environmentTestProfile()) {
		t.Fatal("match modified its input or profile")
	}
}
