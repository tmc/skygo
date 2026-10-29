package kube

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// A DRA attempt has exactly two native objects, declared together before
// either is created:
//
//   - a standalone ResourceClaim named skygo-<attempt>-<16 hex digits of its
//     nonce>, with one request "accelerator" for exactly the task's count of
//     devices of the policy's DeviceClass (ExactCount, int64 count) that
//     satisfy every operator selector, and no owner reference, finalizer,
//     admin access, first-available alternative, constraint, configuration,
//     toleration or capacity request;
//   - a directly managed Pod named skygo-<attempt>, the ordinary retry Pod
//     without an extended resource, whose spec.resourceClaims has the single
//     entry "accelerator" with resourceClaimName set to that claim, and whose
//     task container lists the claim entry and request in resources.claims.
//
// Both carry the owner and attempt labels, the task and binding annotations,
// and their own creation nonce and spec digest annotations. Their spec
// digests cover the canonical identity: metadata as identityMeta, and the
// complete typed claim spec or Pod spec. Claims are canonicalized only by
// removing the built-in resource.kubernetes.io/delete-protection finalizer,
// exactly when it is the only finalizer (k8s.io/api resource/v1 types.go:33),
// which the claim controller adds when it allocates. The claim spec
// defaults of v1.35.9 (pkg/apis/resource/v1/defaults.go:31-39) fill only an
// empty allocation mode or zero count, which this mode always sets. Pods use
// canonicalParent unchanged; the v1.35.9 Pod defaults and admission outputs
// it fills match the 1.32 set (K8S-ADMISSION-DEFAULTS). Every other
// difference is refused.

// draClaimName returns the claim name of attempt id with claim nonce nonce.
func draClaimName(id, nonce string) (string, error) {
	name, err := jobName(id)
	if err != nil {
		return "", err
	}
	if !isLowerHex(nonce, 64) {
		return "", errors.New("invalid kubernetes dra claim nonce")
	}
	return name + "-" + nonce[:16], nil
}

// draClaim renders the expected claim of attempt id.
func (b *Backend) draClaim(id, name, hash string, p *DRAPolicy, count int64) *resourcev1.ResourceClaim {
	var selectors []resourcev1.DeviceSelector
	for _, s := range p.Selectors {
		selectors = append(selectors, resourcev1.DeviceSelector{CEL: &resourcev1.CELDeviceSelector{Expression: s}})
	}
	return &resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: b.namespace,
			Labels:      map[string]string{ownerLabel: b.owner, idLabel: id},
			Annotations: map[string]string{taskAnnotation: hash, bindingAnnotation: b.binding},
		},
		Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{{
			Name: draRequestName,
			Exactly: &resourcev1.ExactDeviceRequest{
				DeviceClassName: p.DeviceClass,
				Selectors:       selectors,
				AllocationMode:  resourcev1.DeviceAllocationModeExactCount,
				Count:           count,
			},
		}}}},
	}
}

// draPod renders the expected Pod of attempt id, which uses claim claimName.
func (b *Backend) draPod(id, hash, claimName string, t backend.Task) *corev1.Pod {
	name, _ := jobName(id)
	pod := b.retryPod(id, name, hash, t)
	pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: draPodClaimName, ResourceClaimName: &claimName}}
	pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: draPodClaimName, Request: draRequestName}}
	return pod
}

// draClaimIdentity is the canonical identity of a claim.
type draClaimIdentity struct {
	Kind string                       `json:"kind"`
	Meta identityMeta                 `json:"metadata"`
	Spec resourcev1.ResourceClaimSpec `json:"spec"`
}

// draClaimSpec returns the spec digest of claim c.
func draClaimSpec(c *resourcev1.ResourceClaim) (string, error) {
	m := *c.ObjectMeta.DeepCopy()
	if slices.Equal(m.Finalizers, []string{resourcev1.Finalizer}) {
		m.Finalizers = nil
	}
	data, err := json.Marshal(draClaimIdentity{Kind: "ResourceClaim", Meta: identityMetaOf(m), Spec: *c.Spec.DeepCopy()})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// draSpec returns the spec digest of obj, a claim or a Pod. read reports a
// later read rather than the expected object or a create response.
func draSpec(obj runtime.Object, read bool) (string, error) {
	if c, ok := obj.(*resourcev1.ResourceClaim); ok {
		return draClaimSpec(c)
	}
	if _, ok := obj.(*corev1.Pod); ok {
		return receiptSpec(obj, read)
	}
	return "", errors.New("unsupported kubernetes dra kind")
}

func draMeta(obj runtime.Object) *metav1.ObjectMeta {
	switch o := obj.(type) {
	case *resourcev1.ResourceClaim:
		return &o.ObjectMeta
	case *corev1.Pod:
		return &o.ObjectMeta
	}
	return nil
}

// decodeDRAClaim decodes raw, a claim create response or read, strictly:
// every field of the object, its status included, must be represented by the
// vendored v1.35.9 types, so that no unknown feature is silently dropped.
func decodeDRAClaim(raw []byte) (*resourcev1.ResourceClaim, error) {
	var o strictObject[resourcev1.ResourceClaimSpec]
	var status json.RawMessage
	if err := o.decode(raw, "resource.k8s.io/v1", "ResourceClaim", &status); err != nil {
		return nil, err
	}
	c := &resourcev1.ResourceClaim{TypeMeta: metav1.TypeMeta{APIVersion: "resource.k8s.io/v1", Kind: "ResourceClaim"}, ObjectMeta: o.Metadata, Spec: o.Spec}
	if len(o.Status) != 0 {
		dec := json.NewDecoder(bytes.NewReader(o.Status))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c.Status); err != nil {
			return nil, fmt.Errorf("%w: decode kubernetes ResourceClaim status: %v", backend.ErrIdentityMismatch, err)
		}
	}
	return c, nil
}

// decodeDRA decodes raw as an object of kind, "ResourceClaim" or "Pod".
func decodeDRA(kind string, raw []byte) (runtime.Object, error) {
	if kind == "ResourceClaim" {
		return decodeDRAClaim(raw)
	}
	return decodeParent("Pod", raw)
}

// decodeDeviceClass decodes raw, a DeviceClass read, strictly.
func decodeDeviceClass(raw []byte) (*resourcev1.DeviceClass, error) {
	c := new(resourcev1.DeviceClass)
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(c)
	if err == nil {
		if _, terr := dec.Token(); terr != io.EOF {
			err = errors.New("trailing data")
		}
	}
	if err == nil && (c.APIVersion != "resource.k8s.io/v1" || c.Kind != "DeviceClass") {
		err = errors.New("unexpected type")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: decode kubernetes DeviceClass: %v", backend.ErrIdentityMismatch, err)
	}
	return c, nil
}

// draClassSpec returns the digest of a DeviceClass spec.
func draClassSpec(c *resourcev1.DeviceClass) (string, error) {
	data, err := json.Marshal(c.Spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// draMatch validates obj, a create response or, if read, a later read of
// object o of record r, and returns its UID. Labels, annotations, nonce,
// spec digest, namespace and name must be exactly the declared ones, without
// owner references, and a pinned object must have its pinned UID.
func (b *Backend) draMatch(obj runtime.Object, r *draRecord, o *draObject, read bool) (string, error) {
	m := draMeta(obj)
	if m == nil {
		return "", fmt.Errorf("%w: unexpected kubernetes object", backend.ErrIdentityMismatch)
	}
	uid := string(m.UID)
	spec, err := draSpec(obj, read)
	if err != nil || spec != o.SpecSHA256 || m.Name != o.Name || m.Namespace != b.namespace || len(m.OwnerReferences) != 0 ||
		m.Labels[ownerLabel] != b.owner || m.Labels[idLabel] != r.Attempt ||
		m.Annotations[taskAnnotation] != r.TaskSHA256 || m.Annotations[bindingAnnotation] != b.binding ||
		m.Annotations[nonceAnnotation] != o.Nonce || m.Annotations[specAnnotation] != o.SpecSHA256 ||
		!validInstance(uid) || o.Send == phasePinned && uid != o.UID {
		return "", fmt.Errorf("%w: kubernetes dra %s differs from its record", backend.ErrIdentityMismatch, o.kind())
	}
	return uid, nil
}

// draWitness returns the witness of raw, the complete body of a refused create
// response for an object of kind with HTTP status code. Only well formed
// values of a strictly decoded body are recorded.
func draWitness(kind string, code int, raw []byte) receiptWitness {
	sum := sha256.Sum256(raw)
	w := receiptWitness{Status: code, ResponseSHA256: hex.EncodeToString(sum[:]),
		ResponseBytes: int64(len(raw)), Decode: witnessUndecodable}
	obj, err := decodeDRA(kind, raw)
	if err != nil {
		return w
	}
	w.Decode = witnessDecoded
	m := draMeta(obj)
	if uid := string(m.UID); validInstance(uid) {
		w.UID = uid
	}
	if v := m.Annotations[nonceAnnotation]; isLowerHex(v, 64) {
		w.Nonce = v
	}
	if v := m.Annotations[specAnnotation]; isLowerHex(v, 64) {
		w.SpecAnnotation = v
	}
	if spec, err := draSpec(obj, false); err == nil {
		w.SpecSHA256 = spec
	}
	return w
}

// draConsumers classifies the reservations of claim c for record r: foreign
// reports any consumer other than the pinned Pod; own reports the pinned Pod.
// A reservation is ours only with resource pods, the empty API group, the
// Pod's name and its pinned UID.
func draConsumers(c *resourcev1.ResourceClaim, r *draRecord) (own, foreign bool) {
	for _, ref := range c.Status.ReservedFor {
		if r.Pod.Send == phasePinned && ref.APIGroup == "" && ref.Resource == "pods" &&
			ref.Name == r.Pod.Name && string(ref.UID) == r.Pod.UID {
			own = true
			continue
		}
		foreign = true
	}
	return own, foreign
}

// draSucceeded reports a witnessed ordinary success of the pinned Pod p: phase
// Succeeded without a deletion in progress, exactly the task container with
// one terminated status of exit code 0, no restart and a runtime container
// ID. Under the trusted kubelet contract (docs/dra.md) the kubelet publishes
// this terminal phase only after its containers stopped and its DRA
// resources were unprepared; this predicate does not observe that itself.
func draSucceeded(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodSucceeded || p.DeletionTimestamp != nil ||
		len(p.Spec.Containers) != 1 || p.Spec.Containers[0].Name != "task" ||
		len(p.Spec.InitContainers) != 0 || len(p.Spec.EphemeralContainers) != 0 ||
		len(p.Status.ContainerStatuses) != 1 || len(p.Status.InitContainerStatuses) != 0 || len(p.Status.EphemeralContainerStatuses) != 0 {
		return false
	}
	c := p.Status.ContainerStatuses[0]
	exit := c.State.Terminated
	if c.Name != "task" || c.RestartCount != 0 || exit == nil || c.State.Running != nil || c.State.Waiting != nil ||
		exit.ExitCode != 0 || exit.ContainerID == "" || c.ContainerID != exit.ContainerID ||
		exit.FinishedAt.UnixNano() <= 0 || exit.FinishedAt.Before(&exit.StartedAt) {
		return false
	}
	return true
}
