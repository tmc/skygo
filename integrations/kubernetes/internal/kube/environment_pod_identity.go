package kube

import (
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
)

// matchEnvironmentPod verifies observed objects against one environment
// generation: the namespace, the borrowed claim and the Pod that the owner
// originally created with identity.
//
// The namespace must have the profile's name and UID. The claim must have the
// profile's namespace, name and UID, exactly the profile's access mode, a
// filesystem volume mode, and, once bound, the profile's volume. The Pod must
// have identity's UID and generation, exactly the metadata of environmentPod,
// no owner references, and a spec that fixedPodSpecMatches accepts. The
// resource version is not compared.
//
// A present object that differs wraps backend.ErrIdentityMismatch. Otherwise
// an absent object wraps errEnvironmentObjectMissing, and a deleting object or
// an unbound claim wraps errEnvironmentObjectUnavailable. None of these
// permits creating, adopting or deleting an object. A nil result means only
// that identity and profile match; it says nothing about Pod readiness, the
// claim's contents or other writers. No argument is modified.
func (b *Backend) matchEnvironmentPod(in environmentPodInput, p environmentPodProfile,
	identity environmentPodIdentity, pod *corev1.Pod,
	namespace *corev1.Namespace, pvc *corev1.PersistentVolumeClaim) error {
	want, err := b.environmentPod(in, p)
	if err != nil {
		return err
	}
	if identity.podUID == "" || identity.podGeneration < 0 {
		return errors.New("invalid environment pod identity")
	}

	if namespace != nil && (namespace.Name != b.namespace || namespace.UID != p.namespaceUID) {
		return fmt.Errorf("%w: kubernetes environment namespace differs", backend.ErrIdentityMismatch)
	}
	if pvc != nil && !environmentClaimMatches(p, b.namespace, pvc) {
		return fmt.Errorf("%w: kubernetes environment claim differs", backend.ErrIdentityMismatch)
	}
	if pod != nil && (pod.UID != identity.podUID || pod.Generation != identity.podGeneration ||
		!profileMetaMatches(want.ObjectMeta, pod.ObjectMeta) || !fixedPodSpecMatches(&want.Spec, &pod.Spec)) {
		return fmt.Errorf("%w: kubernetes environment pod differs", backend.ErrIdentityMismatch)
	}

	switch {
	case namespace == nil:
		return fmt.Errorf("kubernetes environment namespace: %w", errEnvironmentObjectMissing)
	case pvc == nil:
		return fmt.Errorf("kubernetes environment claim: %w", errEnvironmentObjectMissing)
	case pod == nil:
		return fmt.Errorf("kubernetes environment pod: %w", errEnvironmentObjectMissing)
	}

	switch {
	case namespace.DeletionTimestamp != nil || namespace.Status.Phase == corev1.NamespaceTerminating:
		return fmt.Errorf("kubernetes environment namespace: %w", errEnvironmentObjectUnavailable)
	case pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "":
		return fmt.Errorf("kubernetes environment claim: %w", errEnvironmentObjectUnavailable)
	case pod.DeletionTimestamp != nil:
		return fmt.Errorf("kubernetes environment pod: %w", errEnvironmentObjectUnavailable)
	}
	return nil
}

// environmentClaimMatches reports whether pvc is the profile's claim. An
// unbound claim with no volume name matches; binding is checked separately.
func environmentClaimMatches(p environmentPodProfile, namespace string, pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Namespace != namespace || pvc.Name != p.pvcName || pvc.UID != p.pvcUID {
		return false
	}
	if pvc.Spec.VolumeName != "" && pvc.Spec.VolumeName != p.volumeName {
		return false
	}
	if len(pvc.Spec.AccessModes) != 1 || pvc.Spec.AccessModes[0] != p.accessMode {
		return false
	}
	return pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode == corev1.PersistentVolumeFilesystem
}
