package kube

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// layoutAnnotation records the fixed data layout identifier on every data
// Pod and Secret.
const layoutAnnotation = "skygo.tmc.dev/data-layout"

// profileAnnotationPrefix is the annotation namespace owned by skygo. A server
// or webhook may add other annotations, never ones under this prefix.
const profileAnnotationPrefix = "skygo.tmc.dev/"

// profileSecretMode is the projection mode of every Secret volume.
const profileSecretMode = int32(0o440)

// The helper command and environment protocol, ratified with the helper
// (cmd/skygo-data). Token bytes are never part of it; they are mounted files.
const (
	profileStageVerb   = "stage"
	profileExecVerb    = "exec"
	profileCollectVerb = "collect"

	profileEnvOrigin       = "SKYGO_DATA_ORIGIN"
	profileEnvCAPEM        = "SKYGO_DATA_CA_PEM"
	profileEnvHelperSHA256 = "SKYGO_DATA_HELPER_SHA256"
	profileEnvInputID      = "SKYGO_DATA_INPUT_ID"
	profileEnvInputSHA256  = "SKYGO_DATA_INPUT_SHA256"
	profileEnvInputSize    = "SKYGO_DATA_INPUT_SIZE"
)

// Fixed volume names of the data layout. Alias volumes are "alias-<i>".
const (
	profileWorkspaceVolume   = "workspace"
	profileHelperVolume      = "helper"
	profileInputTokenVolume  = "input-token"
	profileOutputTokenVolume = "output-token"
)

// dataSecretName returns the name of the scoped token Secret, where scope is
// "input" or "output".
func dataSecretName(id, scope string) string {
	return "skygo-" + id + "-" + scope
}

// profileMeta returns the shared labels and annotations of data objects.
func (b *Backend) profileMeta(name, id, hash string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: name, Namespace: b.namespace,
		Labels: map[string]string{ownerLabel: b.owner, idLabel: id},
		Annotations: map[string]string{taskAnnotation: hash,
			bindingAnnotation: b.binding, layoutAnnotation: dataLayout},
	}
}

// dataSecret returns the immutable token Secret for one scope. It copies token,
// which the caller continues to own.
func (b *Backend) dataSecret(id, scope string, token []byte, hash string) *corev1.Secret {
	immutable := true
	return &corev1.Secret{
		ObjectMeta: b.profileMeta(dataSecretName(id, scope), id, hash),
		Type:       corev1.SecretTypeOpaque,
		Immutable:  &immutable,
		Data:       map[string][]byte{tokenKey: bytes.Clone(token)},
	}
}

// dataPod returns the exact finite Pod for a data task: stage, setup, task
// and collector for a Workdir task, or a single task container for a
// mount-only task. It neither validates the task's acceptance nor contacts
// Kubernetes; callers run Check first.
func (b *Backend) dataPod(id string, t backend.Task, budgets backend.DataBudgets, hash string) (*corev1.Pod, error) {
	if b.data == nil {
		return nil, errors.New("kubernetes target has no data profile")
	}
	name, err := jobName(id)
	if err != nil {
		return nil, err
	}
	deadline, err := budgets.TotalSeconds()
	if err != nil {
		return nil, err
	}
	if len(t.Argv) == 0 {
		return nil, errors.New("argv is required")
	}
	if t.Workdir == nil && (len(t.Setup) != 0 || len(t.Outputs) != 0) {
		return nil, errors.New("data setup and outputs require workdir")
	}
	if t.Workdir != nil && t.Directory != "" {
		return nil, errors.New("data workdir conflicts with directory")
	}
	aliasVolumes, aliasMounts, err := b.profileAliases(t.DataMounts)
	if err != nil {
		return nil, err
	}

	// Setup and task use the ordinary task resources, environment and node
	// selection, exactly as podSpec constructs them.
	base := b.podSpec(t)
	workload := func(name string, command []string) corev1.Container {
		c := base.Containers[0]
		return corev1.Container{
			Name: name, Image: t.Image, Command: command,
			Env: slices.Clone(c.Env), WorkingDir: c.WorkingDir,
			Resources: *c.Resources.DeepCopy(), SecurityContext: profileContainerSecurity(),
		}
	}

	var volumes []corev1.Volume
	var inits, mains []corev1.Container
	if t.Workdir == nil {
		task := workload("task", slices.Clone(t.Argv))
		task.VolumeMounts = aliasMounts
		mains = append(mains, task)
	} else {
		volumes = append(volumes,
			corev1.Volume{Name: profileWorkspaceVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			corev1.Volume{Name: profileHelperVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			profileTokenVolume(profileInputTokenVolume, dataSecretName(id, "input")))
		inits = append(inits, corev1.Container{
			Name: "stage", Image: b.data.helperImage,
			Command: []string{helperEntry, profileStageVerb},
			Env: profileEnv(map[string]string{
				profileEnvOrigin:       b.data.origin,
				profileEnvCAPEM:        string(b.data.caPEM),
				profileEnvHelperSHA256: b.data.helperSHA256,
				profileEnvInputID:      t.Workdir.ID,
				profileEnvInputSHA256:  t.Workdir.SHA256,
				profileEnvInputSize:    strconv.FormatInt(t.Workdir.Size, 10),
			}),
			VolumeMounts: []corev1.VolumeMount{
				{Name: profileWorkspaceVolume, MountPath: workspaceMount},
				{Name: profileHelperVolume, MountPath: helperMount},
				{Name: profileInputTokenVolume, MountPath: inputTokenDir, ReadOnly: true},
			},
			SecurityContext: profileContainerSecurity(),
		})
		workloadMounts := func() []corev1.VolumeMount {
			return append([]corev1.VolumeMount{
				{Name: profileWorkspaceVolume, MountPath: workspaceMount},
				{Name: profileHelperVolume, MountPath: helperMount, ReadOnly: true},
			}, aliasMounts...)
		}
		if len(t.Setup) != 0 {
			setup := workload("setup", profileExec(t.Setup))
			setup.WorkingDir = workPath
			setup.VolumeMounts = workloadMounts()
			inits = append(inits, setup)
		}
		task := workload("task", profileExec(t.Argv))
		task.WorkingDir = workPath
		task.VolumeMounts = workloadMounts()
		mains = append(mains, task)
		if len(t.Outputs) != 0 {
			volumes = append(volumes, profileTokenVolume(profileOutputTokenVolume, dataSecretName(id, "output")))
			mains = append(mains, corev1.Container{
				Name: "collector", Image: b.data.helperImage,
				Command:    append([]string{helperEntry, profileCollectVerb, "--"}, t.Outputs...),
				WorkingDir: workPath,
				Env: profileEnv(map[string]string{
					profileEnvOrigin: b.data.origin,
					profileEnvCAPEM:  string(b.data.caPEM),
				}),
				VolumeMounts: []corev1.VolumeMount{
					{Name: profileWorkspaceVolume, MountPath: workspaceMount, ReadOnly: true},
					{Name: profileOutputTokenVolume, MountPath: outputTokenDir, ReadOnly: true},
				},
				SecurityContext: profileContainerSecurity(),
			})
		}
	}
	volumes = append(volumes, aliasVolumes...)

	no := false
	user := int64(dataUser)
	onRootMismatch := corev1.FSGroupChangeOnRootMismatch
	yes := true
	return &corev1.Pod{
		ObjectMeta: b.profileMeta(name, id, hash),
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        &deadline,
			AutomountServiceAccountToken: &no,
			EnableServiceLinks:           &no,
			ShareProcessNamespace:        &no,
			NodeSelector:                 base.NodeSelector,
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: &yes, RunAsUser: &user, RunAsGroup: &user, FSGroup: &user,
				FSGroupChangePolicy: &onRootMismatch,
				SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: inits,
			Containers:     mains,
			Volumes:        volumes,
		},
	}, nil
}

// profileAliases returns alias-<i> volumes and their workload mounts for the
// task's selected operator references.
func (b *Backend) profileAliases(mounts []backend.DataMount) ([]corev1.Volume, []corev1.VolumeMount, error) {
	var volumes []corev1.Volume
	var result []corev1.VolumeMount
	for i, m := range mounts {
		ref, ok := b.data.references[m.Reference]
		if !ok {
			return nil, nil, errors.New("unknown data mount reference")
		}
		name := "alias-" + strconv.Itoa(i)
		v := corev1.Volume{Name: name}
		readOnly := m.ReadOnly
		switch {
		case ref.pvc != "" && ref.secret == "":
			if !readOnly && !ref.allowWrite {
				return nil, nil, errors.New("data mount reference is read-only")
			}
			v.PersistentVolumeClaim = &corev1.PersistentVolumeClaimVolumeSource{ClaimName: ref.pvc, ReadOnly: readOnly}
		case ref.secret != "" && ref.pvc == "":
			readOnly = true
			mode := profileSecretMode
			optional := false
			items := make([]corev1.KeyToPath, 0, len(ref.keys))
			for _, k := range ref.keys {
				items = append(items, corev1.KeyToPath{Key: k, Path: k})
			}
			v.Secret = &corev1.SecretVolumeSource{SecretName: ref.secret, Items: items,
				DefaultMode: &mode, Optional: &optional}
		default:
			return nil, nil, errors.New("invalid data mount reference")
		}
		volumes = append(volumes, v)
		result = append(result, corev1.VolumeMount{Name: name, MountPath: m.Destination, ReadOnly: readOnly})
	}
	return volumes, result, nil
}

// profileTokenVolume projects only the token key of a scoped Secret.
func profileTokenVolume(name, secret string) corev1.Volume {
	mode := profileSecretMode
	return corev1.Volume{Name: name, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
		SecretName: secret, Items: []corev1.KeyToPath{{Key: tokenKey, Path: tokenKey}}, DefaultMode: &mode,
	}}}
}

// profileExec returns the copied helper's exec command for argv.
func profileExec(argv []string) []string {
	return append([]string{helperCopy, profileExecVerb, "--"}, argv...)
}

// profileEnv returns env sorted by name.
func profileEnv(env map[string]string) []corev1.EnvVar {
	result := make([]corev1.EnvVar, 0, len(env))
	for _, k := range slices.Sorted(maps.Keys(env)) {
		result = append(result, corev1.EnvVar{Name: k, Value: env[k]})
	}
	return result
}

// profileContainerSecurity returns the fixed container security context.
func profileContainerSecurity() *corev1.SecurityContext {
	yes, no := true, false
	user := int64(dataUser)
	return &corev1.SecurityContext{
		RunAsNonRoot: &yes, RunAsUser: &user, RunAsGroup: &user,
		AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
		Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
}

// profileMetaMatches checks object identity metadata: exact name, namespace and
// labels, the expected skygo annotations and no others under the skygo prefix,
// and no owner references.
func profileMetaMatches(want, got metav1.ObjectMeta) bool {
	if got.Name != want.Name || got.Namespace != want.Namespace ||
		!maps.Equal(got.Labels, want.Labels) || len(got.OwnerReferences) != 0 {
		return false
	}
	for k, v := range want.Annotations {
		if g, ok := got.Annotations[k]; !ok || g != v {
			return false
		}
	}
	for k := range got.Annotations {
		if _, ok := want.Annotations[k]; !ok && profileReservedAnnotation(k) {
			return false
		}
	}
	return true
}

// profileReservedAnnotation reports whether an unexpected annotation is
// skygo-owned or can change container security on older servers, such as
// AppArmor or seccomp annotations that predate the security context fields.
func profileReservedAnnotation(key string) bool {
	for _, prefix := range []string{profileAnnotationPrefix,
		"container.apparmor.security.beta.kubernetes.io/",
		"seccomp.security.alpha.kubernetes.io/",
		"container.seccomp.security.alpha.kubernetes.io/"} {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// dataPodMatches verifies an observed Pod against the expected Pod from
// dataPod. Every spec field must equal the expected value, except these
// server- or admission-defaulted fields that skygo does not set and that do not
// add containers, volumes, privileges or namespaces:
//
//   - container ImagePullPolicy and ResizePolicy, and TerminationMessagePath
//     and TerminationMessagePolicy only when empty or the server defaults
//   - pod DNSPolicy, SchedulerName, ServiceAccountName,
//     DeprecatedServiceAccount, ImagePullSecrets, Tolerations, Priority,
//     PriorityClassName, PreemptionPolicy, TerminationGracePeriodSeconds and
//     NodeName
//
// A nil ShareProcessNamespace is accepted as false. Status is not compared.
func (b *Backend) dataPodMatches(want, got *corev1.Pod) error {
	mismatch := fmt.Errorf("%w: kubernetes data pod profile differs", backend.ErrIdentityMismatch)
	if want == nil || got == nil || !profileMetaMatches(want.ObjectMeta, got.ObjectMeta) {
		return mismatch
	}
	if len(got.Spec.InitContainers) != len(want.Spec.InitContainers) ||
		len(got.Spec.Containers) != len(want.Spec.Containers) {
		return mismatch
	}
	spec := got.Spec.DeepCopy()
	w := &want.Spec
	spec.DNSPolicy = w.DNSPolicy
	spec.SchedulerName = w.SchedulerName
	spec.ServiceAccountName = w.ServiceAccountName
	spec.DeprecatedServiceAccount = w.DeprecatedServiceAccount
	spec.ImagePullSecrets = w.ImagePullSecrets
	spec.Tolerations = w.Tolerations
	spec.Priority = w.Priority
	spec.PriorityClassName = w.PriorityClassName
	spec.PreemptionPolicy = w.PreemptionPolicy
	spec.TerminationGracePeriodSeconds = w.TerminationGracePeriodSeconds
	spec.NodeName = w.NodeName
	if spec.ShareProcessNamespace == nil {
		no := false
		spec.ShareProcessNamespace = &no
	}
	profileContainerDefaults(spec.InitContainers, w.InitContainers)
	profileContainerDefaults(spec.Containers, w.Containers)
	if !equality.Semantic.DeepEqual(spec, w) {
		return mismatch
	}
	return nil
}

// profileContainerDefaults copies the ignored server-defaulted container fields
// from want into got, which has the same length.
func profileContainerDefaults(got, want []corev1.Container) {
	for i := range got {
		// The termination message path is a kubelet bind mount; accept
		// only its default location and policy.
		if p := got[i].TerminationMessagePath; p == "" || p == corev1.TerminationMessagePathDefault {
			got[i].TerminationMessagePath = want[i].TerminationMessagePath
		}
		if p := got[i].TerminationMessagePolicy; p == "" || p == corev1.TerminationMessageReadFile {
			got[i].TerminationMessagePolicy = want[i].TerminationMessagePolicy
		}
		got[i].ImagePullPolicy = want[i].ImagePullPolicy
		got[i].ResizePolicy = want[i].ResizePolicy
	}
}

// dataSecretMatches verifies an observed token Secret against the expected
// Secret from dataSecret. It checks only the length of the token, never its
// bytes.
func (b *Backend) dataSecretMatches(want, got *corev1.Secret) error {
	mismatch := fmt.Errorf("%w: kubernetes data secret profile differs", backend.ErrIdentityMismatch)
	if want == nil || got == nil || !profileMetaMatches(want.ObjectMeta, got.ObjectMeta) {
		return mismatch
	}
	if got.Type != want.Type || got.Immutable == nil || !*got.Immutable ||
		len(got.StringData) != 0 || len(got.Data) != 1 {
		return mismatch
	}
	token, ok := got.Data[tokenKey]
	if !ok || len(token) != len(want.Data[tokenKey]) {
		return mismatch
	}
	return nil
}
