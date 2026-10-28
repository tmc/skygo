package kube

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Reserved annotations of an environment Pod.
const (
	environmentGenerationAnnotation = "skygo.tmc.dev/environment-generation"
	environmentSpecAnnotation       = "skygo.tmc.dev/environment-spec-sha256"
	environmentRevisionAnnotation   = "skygo.tmc.dev/environment-profile-revision"
)

// environmentContainer is the name of the single environment container.
const environmentContainer = "workspace"

// Bounds of the fixed idle command and pull secret snapshot.
const (
	maxEnvironmentArgs        = 64
	maxEnvironmentArgBytes    = 8192
	maxEnvironmentPullSecrets = 32
)

// errEnvironmentObjectMissing reports an absent Pod, namespace or claim. It is
// not evidence of deletion and never permits creating the object again.
var errEnvironmentObjectMissing = errors.New("environment object missing")

// errEnvironmentObjectUnavailable reports an object whose identity matches
// but which is deleting or, for a claim, not yet bound.
var errEnvironmentObjectUnavailable = errors.New("environment object unavailable")

// environmentPodInput is one accepted environment generation.
//
// The id is 32 lowercase hexadecimal digits. The principal has the Store
// principal shape and never appears in a label or annotation. Generation is 1.
// specHash is the nonzero digest computed by the acceptance owner over the
// canonical environment spec, which includes the principal's alias policy, the
// profile revision, the claim UID and volume, the image, the idle command and
// the namespace binding, the default service account UID and its image pull
// secrets. This package neither computes nor verifies it.
//
// task carries only Image, CPUMillis, MemoryBytes, CPULimitMillis,
// MemoryLimitBytes, Accelerator, AcceleratorCount and NodeSelector; every other
// field must be zero.
//
// artifacts are the spec's distinct artifact references, as
// environmentArtifacts returns them.
type environmentPodInput struct {
	id, principal string
	generation    uint64
	specHash      [32]byte
	task          backend.Task
	artifacts     []environmentArtifact
}

// environmentPodProfile is the operator's private profile for one principal's
// borrowed claim. It is a trusted in-memory value, never parsed from a remote
// request, and it is not by itself proof that the principal may use the
// claim. The operator prepares claim ownership for UID 65532.
//
// serviceAccountUID and pullSecrets are the operator's snapshot of the
// namespace's default service account: its UID and its image pull secret
// names, in order. The Pod sets them explicitly, so ServiceAccount admission
// copies nothing into it. The snapshot is provenance of this profile; it is
// never read from the observed Pod.
//
// volumeUID, when not empty, is the operator's UID of the claim's bound
// PersistentVolume. The profile file requires it: Plan records it as the
// environment's accepted volume UID, so a same-name volume replacement is
// refused from the first Create on, and a profile whose volume UID differs
// from an environment's accepted one refuses that environment rather than
// adopting the new volume. An empty volumeUID, which only in-memory
// fixtures use, keeps the earlier rule: the UID is recorded at the first
// Create.
type environmentPodProfile struct {
	revision     string
	namespaceUID types.UID
	principal    string
	image        string
	idleArgv     []string
	alias        string
	pvcName      string
	pvcUID       types.UID
	volumeName   string
	volumeUID    types.UID
	accessMode   corev1.PersistentVolumeAccessMode
	mountPath    string

	serviceAccountUID types.UID
	pullSecrets       []string
}

// environmentPodIdentity is the identity of the Pod that the environment owner
// originally created and read back. It is never derived from the object under
// comparison.
type environmentPodIdentity struct {
	podUID        types.UID
	podGeneration int64
}

// check validates the input and returns the Pod name.
func (in environmentPodInput) check() (string, error) {
	if _, err := jobName(in.id); err != nil {
		return "", errors.New("invalid environment id")
	}
	if !environmentPrincipal(in.principal) {
		return "", errors.New("invalid environment principal")
	}
	if in.generation != 1 {
		return "", errors.New("unsupported environment generation")
	}
	if in.specHash == [32]byte{} {
		return "", errors.New("environment spec hash is required")
	}
	t := in.task
	switch {
	case len(t.Argv) != 0:
		return "", errors.New("environment task argv is not supported")
	case len(t.Env) != 0:
		return "", errors.New("environment task env is not supported")
	case t.Directory != "":
		return "", errors.New("environment task directory is not supported")
	case t.Target != "" || len(t.TargetLabels) != 0:
		return "", errors.New("environment task target is not supported")
	case t.TimeoutSeconds != 0:
		return "", errors.New("environment task timeout is not supported")
	case t.Retry != nil:
		return "", errors.New("environment task retry is not supported")
	case dataTask(t):
		return "", errors.New("environment task data fields are not supported")
	}
	if t.CPUMillis <= 0 || t.MemoryBytes <= 0 || t.CPULimitMillis <= 0 || t.MemoryLimitBytes <= 0 {
		return "", errors.New("environment requires positive cpu and memory requests and limits")
	}
	if t.CPULimitMillis < t.CPUMillis || t.MemoryLimitBytes < t.MemoryBytes {
		return "", errors.New("environment request exceeds limit")
	}
	if len(in.artifacts) > maxEnvironmentArtifacts {
		return "", errors.New("too many environment artifacts")
	}
	return "skygo-env-" + in.id, nil
}

// check validates the profile for principal.
func (p environmentPodProfile) check(principal string) error {
	if !qualificationText(p.revision, 128) {
		return errors.New("invalid environment profile revision")
	}
	if !qualificationText(string(p.namespaceUID), 128) {
		return errors.New("invalid environment namespace uid")
	}
	if p.principal != principal {
		return fmt.Errorf("%w: environment profile principal differs", backend.ErrIdentityMismatch)
	}
	if !configImage(p.image) {
		return errors.New("environment image must be digest pinned")
	}
	if len(p.idleArgv) == 0 || len(p.idleArgv) > maxEnvironmentArgs || !path.IsAbs(p.idleArgv[0]) {
		return errors.New("invalid environment idle command")
	}
	n := 0
	for _, arg := range p.idleArgv {
		n += len(arg)
		if !utf8.ValidString(arg) || strings.IndexByte(arg, 0) >= 0 {
			return errors.New("invalid environment idle command")
		}
	}
	if n > maxEnvironmentArgBytes {
		return errors.New("invalid environment idle command")
	}
	if len(validation.IsDNS1123Label(p.alias)) != 0 {
		return errors.New("invalid environment alias")
	}
	if len(validation.IsDNS1123Subdomain(p.pvcName)) != 0 || len(validation.IsDNS1123Subdomain(p.volumeName)) != 0 {
		return errors.New("invalid environment claim or volume name")
	}
	if !qualificationText(string(p.pvcUID), 128) {
		return errors.New("invalid environment claim uid")
	}
	if p.volumeUID != "" && !qualificationText(string(p.volumeUID), 128) {
		return errors.New("invalid environment volume uid")
	}
	if p.accessMode != corev1.ReadWriteOnce && p.accessMode != corev1.ReadWriteOncePod {
		return errors.New("invalid environment access mode")
	}
	if p.mountPath != workspaceMount {
		return errors.New("invalid environment mount path")
	}
	if !qualificationText(string(p.serviceAccountUID), 128) {
		return errors.New("invalid environment service account uid")
	}
	if len(p.pullSecrets) > maxEnvironmentPullSecrets {
		return errors.New("too many environment image pull secrets")
	}
	for i, name := range p.pullSecrets {
		if len(validation.IsDNS1123Subdomain(name)) != 0 || slices.Contains(p.pullSecrets[:i], name) {
			return errors.New("invalid environment image pull secret")
		}
	}
	return nil
}

// environmentPrincipal reports whether s has the Store principal shape: 1 to
// 64 bytes, a lowercase letter followed by lowercase letters, digits and
// hyphens.
func environmentPrincipal(s string) bool {
	if len(s) == 0 || len(s) > 64 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
