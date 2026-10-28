package kube

import (
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// environmentPod returns the exact Pod for one environment generation on the
// principal's borrowed claim: one workspace container running the profile's
// idle command, with the claim mounted read-write at /workspace, and, if the
// input has artifacts, the stage init container and volumes of
// addEnvironmentArtifacts.
//
// Every scheduling field is explicit, so that fixedPodSpecMatches can compare
// a read-back Pod exactly: no service links, PreemptLowerPriority at priority
// 0, only the two standard 300s NoExecute not-ready and unreachable
// tolerations, ClusterFirst, the default scheduler, 30s grace, IfNotPresent,
// no service account token, no shared process namespace, no scheduling gate,
// and the profile's image pull secrets. The Pod has no deadline, owner
// reference or Secret volume, and no init container unless it has artifacts.
//
// It reads only the target's namespace, owner, binding, resource policy and,
// for artifacts, data helper image. It neither contacts Kubernetes nor
// reserves the claim, and the returned Pod shares no memory with in, p or b.
func (b *Backend) environmentPod(in environmentPodInput, p environmentPodProfile) (*corev1.Pod, error) {
	if b.binding == "" {
		return nil, fmt.Errorf("%w: kubernetes target is unbound", backend.ErrIdentityMismatch)
	}
	if len(validation.IsDNS1123Label(b.namespace)) != 0 || b.owner == "" || len(validation.IsValidLabelValue(b.owner)) != 0 {
		return nil, errors.New("invalid kubernetes target")
	}
	name, err := in.check()
	if err != nil {
		return nil, err
	}
	if err := p.check(in.principal); err != nil {
		return nil, err
	}
	if in.task.Image != p.image {
		return nil, errors.New("environment image differs from profile")
	}
	if len(in.artifacts) > 0 && b.data == nil {
		return nil, errors.New("environment artifacts require a kubernetes data helper image")
	}

	// Validate and convert resources and selectors exactly as an ordinary
	// task, with the profile's command and directory.
	t := backend.Task{
		Argv:             slices.Clone(p.idleArgv),
		Directory:        p.mountPath,
		Image:            p.image,
		CPUMillis:        in.task.CPUMillis,
		MemoryBytes:      in.task.MemoryBytes,
		CPULimitMillis:   in.task.CPULimitMillis,
		MemoryLimitBytes: in.task.MemoryLimitBytes,
		Accelerator:      in.task.Accelerator,
		AcceleratorCount: in.task.AcceleratorCount,
		NodeSelector:     maps.Clone(in.task.NodeSelector),
	}
	if err := b.Check(t); err != nil {
		return nil, err
	}
	base := b.podSpec(t)

	no := false
	grace := int64(30)
	priority := int32(0)
	preempt := corev1.PreemptLowerPriority
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: b.namespace,
			Labels: map[string]string{ownerLabel: b.owner, idLabel: in.id},
			Annotations: map[string]string{
				bindingAnnotation:               b.binding,
				environmentGenerationAnnotation: strconv.FormatUint(in.generation, 10),
				environmentSpecAnnotation:       hex.EncodeToString(in.specHash[:]),
				environmentRevisionAnnotation:   p.revision,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &grace,
			DNSPolicy:                     corev1.DNSClusterFirst,
			NodeSelector:                  base.NodeSelector,
			AutomountServiceAccountToken:  base.AutomountServiceAccountToken,
			ShareProcessNamespace:         &no,
			SchedulerName:                 corev1.DefaultSchedulerName,
			Tolerations:                   environmentTolerations(),
			Priority:                      &priority,
			PreemptionPolicy:              &preempt,
			EnableServiceLinks:            &no,
			ImagePullSecrets:              environmentPullSecrets(p.pullSecrets),
			Containers: []corev1.Container{{
				Name:            environmentContainer,
				Image:           p.image,
				Command:         slices.Clone(p.idleArgv),
				WorkingDir:      p.mountPath,
				Resources:       *base.Containers[0].Resources.DeepCopy(),
				VolumeMounts:    []corev1.VolumeMount{{Name: profileWorkspaceVolume, MountPath: p.mountPath}},
				ImagePullPolicy: corev1.PullIfNotPresent,
				SecurityContext: profileContainerSecurity(),
			}},
			Volumes: []corev1.Volume{{
				Name: profileWorkspaceVolume,
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: p.pvcName,
				}},
			}},
		},
	}
	if len(in.artifacts) > 0 {
		b.addEnvironmentArtifacts(&pod.Spec, in.artifacts)
	}
	return pod, nil
}

// environmentPullSecrets returns references to names, or nil if there are
// none.
func environmentPullSecrets(names []string) []corev1.LocalObjectReference {
	var refs []corev1.LocalObjectReference
	for _, name := range names {
		refs = append(refs, corev1.LocalObjectReference{Name: name})
	}
	return refs
}

// environmentTolerations returns the two standard 300s NoExecute tolerations
// that the API server adds to a Pod without them.
func environmentTolerations() []corev1.Toleration {
	seconds := func() *int64 { v := int64(300); return &v }
	return []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: seconds()},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute, TolerationSeconds: seconds()},
	}
}
