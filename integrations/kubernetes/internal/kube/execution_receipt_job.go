package kube

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// Receipted objects additionally carry their creation nonce and expected
// specification digest. Objects created without receipts never carry them.
const (
	nonceAnnotation = "skygo.tmc.dev/creation-nonce"
	specAnnotation  = "skygo.tmc.dev/spec-sha256"
)

// createUncertain reports a receipted create whose outcome is not yet known.
// It is reconcilable state, not failure: the create is never sent again.
type createUncertain string

func (e createUncertain) Error() string { return string(e) }

// receiptIdentity is the canonical identity of a receipted parent: its kind,
// the metadata that identifies or configures it, and its complete typed
// JobSpec or PodSpec. The receipt's spec digest is the SHA-256 of its JSON
// encoding. The expected object and every observed object are canonicalized
// by the same rules (canonicalParent), so that an object matches exactly
// when it is the expected object as the Kubernetes 1.32 API server persists
// it. Each rule is documented at its function and in docs/dra.md.
type receiptIdentity struct {
	Kind string           `json:"kind"`
	Meta identityMeta     `json:"metadata"`
	Job  *batchv1.JobSpec `json:"job,omitempty"`
	Pod  *corev1.PodSpec  `json:"pod,omitempty"`
}

// identityMeta is the part of ObjectMeta that names, places or configures a
// parent. The remaining fields are not identity: uid is compared with the
// receipt separately; resourceVersion, generation, creationTimestamp,
// managedFields and selfLink are API server bookkeeping; deletionTimestamp
// and deletionGracePeriodSeconds record a deletion in progress. The nonce
// and spec annotations are compared with the receipt separately, because
// the digest is computed before they exist.
type identityMeta struct {
	Name         string                  `json:"name"`
	GenerateName string                  `json:"generateName,omitempty"`
	Namespace    string                  `json:"namespace"`
	Labels       map[string]string       `json:"labels,omitempty"`
	Annotations  map[string]string       `json:"annotations,omitempty"`
	Owners       []metav1.OwnerReference `json:"ownerReferences,omitempty"`
	Finalizers   []string                `json:"finalizers,omitempty"`
}

// The generated Job selector labels without the batch.kubernetes.io prefix,
// which the API server still adds; k8s.io/api does not export them.
const (
	legacyJobNameLabel       = "job-name"
	legacyControllerUIDLabel = "controller-uid"
)

// receiptSpec returns the spec digest of obj. read reports whether obj is a
// later read rather than the expected object or a create response; only a
// read may carry scheduler output.
func receiptSpec(obj runtime.Object, read bool) (string, error) {
	id, err := canonicalParent(obj, read)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(id)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalParent returns the canonical identity of a copy of obj. It never
// drops a field by kind or name: every rule below either fills a value that
// the API server would fill for an unset field, or removes one exact server
// output shape bound to obj itself. Any other difference, including fields
// set by other admission plugins or webhooks, remains and is refused.
func canonicalParent(obj runtime.Object, read bool) (receiptIdentity, error) {
	switch o := obj.(type) {
	case *batchv1.Job:
		j := o.DeepCopy()
		generatedSelector(j)
		deletionFinalizer(&j.ObjectMeta)
		defaultJobSpec(&j.Spec)
		defaultPodSpec(&j.Spec.Template.Spec)
		canonicalQuantities(&j.Spec.Template.Spec)
		return receiptIdentity{Kind: "Job", Meta: identityMetaOf(j.ObjectMeta), Job: &j.Spec}, nil
	case *corev1.Pod:
		p := o.DeepCopy()
		if read {
			scheduledNode(p)
		}
		defaultPodSpec(&p.Spec)
		defaultPod(&p.Spec)
		admittedPod(&p.Spec)
		canonicalQuantities(&p.Spec)
		return receiptIdentity{Kind: "Pod", Meta: identityMetaOf(p.ObjectMeta), Pod: &p.Spec}, nil
	}
	return receiptIdentity{}, errors.New("unsupported kubernetes receipt kind")
}

func identityMetaOf(m metav1.ObjectMeta) identityMeta {
	annotations := maps.Clone(m.Annotations)
	delete(annotations, nonceAnnotation)
	delete(annotations, specAnnotation)
	return identityMeta{Name: m.Name, GenerateName: m.GenerateName, Namespace: m.Namespace,
		Labels: m.Labels, Annotations: annotations, Owners: m.OwnerReferences, Finalizers: m.Finalizers}
}

// generatedSelector removes the selector and template labels that the API
// server generates for a Job without a manual selector (job registry
// generateSelector, Kubernetes 1.27 and later). They are removed only in
// their exact generated shape: a selector of the single label
// batch.kubernetes.io/controller-uid equal to j's own UID, and template
// labels batch.kubernetes.io/controller-uid and controller-uid equal to that
// UID and batch.kubernetes.io/job-name and job-name equal to j's name. Any
// other selector, a selector or label of another UID or name, or a partial
// set remains and is refused.
func generatedSelector(j *batchv1.Job) {
	uid, name, sel := string(j.UID), j.Name, j.Spec.Selector
	if uid == "" || name == "" || sel == nil || len(sel.MatchExpressions) != 0 ||
		len(sel.MatchLabels) != 1 || sel.MatchLabels[batchv1.ControllerUidLabel] != uid {
		return
	}
	generated := map[string]string{
		batchv1.ControllerUidLabel: uid, legacyControllerUIDLabel: uid,
		batchv1.JobNameLabel: name, legacyJobNameLabel: name,
	}
	labels := j.Spec.Template.Labels
	for k, v := range generated {
		if got, ok := labels[k]; !ok || got != v {
			return
		}
	}
	for k := range generated {
		delete(labels, k)
	}
	j.Spec.Selector = nil
}

// deletionFinalizer removes the garbage collector's foregroundDeletion
// finalizer from a Job being deleted with foreground propagation, as Cancel
// requests. It remains on a Job not being deleted, as does any other
// finalizer.
func deletionFinalizer(m *metav1.ObjectMeta) {
	if m.DeletionTimestamp != nil && slices.Equal(m.Finalizers, []string{metav1.FinalizerDeleteDependents}) {
		m.Finalizers = nil
	}
}

// defaultJobSpec fills the JobSpec defaults of the API server
// (SetDefaults_Job, batch/v1, Kubernetes 1.32) for unset fields. Skygo sets
// backoffLimit, parallelism and completions itself. podReplacementPolicy
// TerminatingOrFailed is the default without a pod failure policy when the
// JobPodReplacementPolicy feature is enabled, and the behavior when it is
// not. manualSelector false, as the API server reports it, is equivalent to
// unset; true remains and is refused.
func defaultJobSpec(s *batchv1.JobSpec) {
	if s.Completions == nil && s.Parallelism == nil {
		s.Completions = ptr(int32(1))
	}
	if s.Parallelism == nil {
		s.Parallelism = ptr(int32(1))
	}
	if s.BackoffLimit == nil && s.BackoffLimitPerIndex == nil {
		s.BackoffLimit = ptr(int32(6))
	}
	if s.CompletionMode == nil {
		s.CompletionMode = ptr(batchv1.NonIndexedCompletion)
	}
	if s.Suspend == nil {
		s.Suspend = ptr(false)
	}
	if s.PodReplacementPolicy == nil && s.PodFailurePolicy == nil {
		s.PodReplacementPolicy = ptr(batchv1.TerminatingOrFailed)
	}
	if s.ManualSelector == nil {
		s.ManualSelector = ptr(false)
	}
}

// defaultPodSpec fills the PodSpec and Container defaults of the API server
// (SetDefaults_PodSpec and SetDefaults_Container, core/v1, Kubernetes 1.32),
// which apply to Pods and Job templates alike. Ephemeral containers are not
// defaulted: none may exist.
func defaultPodSpec(s *corev1.PodSpec) {
	if s.DNSPolicy == "" {
		s.DNSPolicy = corev1.DNSClusterFirst
	}
	if s.RestartPolicy == "" {
		s.RestartPolicy = corev1.RestartPolicyAlways
	}
	if s.SecurityContext == nil {
		s.SecurityContext = &corev1.PodSecurityContext{}
	}
	if s.TerminationGracePeriodSeconds == nil {
		s.TerminationGracePeriodSeconds = ptr(int64(corev1.DefaultTerminationGracePeriodSeconds))
	}
	if s.SchedulerName == "" {
		s.SchedulerName = corev1.DefaultSchedulerName
	}
	for _, cs := range [][]corev1.Container{s.InitContainers, s.Containers} {
		for i := range cs {
			c := &cs[i]
			if c.ImagePullPolicy == "" {
				c.ImagePullPolicy = corev1.PullIfNotPresent
				if imageTag(c.Image) == "latest" {
					c.ImagePullPolicy = corev1.PullAlways
				}
			}
			if c.TerminationMessagePath == "" {
				c.TerminationMessagePath = corev1.TerminationMessagePathDefault
			}
			if c.TerminationMessagePolicy == "" {
				c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}
		}
	}
}

// imageTag returns the tag of an image reference as image pull policy
// defaulting reads it: "latest" when the reference has neither a tag nor a
// digest. A reference this misreads yields a policy differing from the API
// server's, which is refused, never accepted.
func imageTag(image string) string {
	name, digest, _ := strings.Cut(image, "@")
	tag := ""
	if i := strings.LastIndexByte(name, ':'); i > strings.LastIndexByte(name, '/') {
		tag = name[i+1:]
	}
	if tag == "" && digest == "" {
		return "latest"
	}
	return tag
}

// defaultPod fills the defaults the API server applies to Pods but not to
// templates (SetDefaults_Pod, core/v1, Kubernetes 1.32): a container
// request equal to its limit for each limited resource without a request,
// and enableServiceLinks true. No resource name is added beyond the limits
// and none is removed.
func defaultPod(s *corev1.PodSpec) {
	for _, cs := range [][]corev1.Container{s.InitContainers, s.Containers} {
		for i := range cs {
			r := &cs[i].Resources
			for name, q := range r.Limits {
				if _, ok := r.Requests[name]; !ok {
					if r.Requests == nil {
						r.Requests = corev1.ResourceList{}
					}
					r.Requests[name] = q.DeepCopy()
				}
			}
		}
	}
	if s.EnableServiceLinks == nil {
		s.EnableServiceLinks = ptr(corev1.DefaultEnableServiceLinks)
	}
}

// admittedPod fills the output of the admission plugins that the API server
// enables by default and that change a Pod Skygo creates, in the shape they
// produce for it:
//
//   - ServiceAccount: serviceAccountName "default", which the v1 API also
//     reports as the deprecated serviceAccount. With automounting disabled
//     no token volume is added. Image pull secrets it copies from the
//     service account are not filled and are refused.
//   - Priority: priority 0 and preemptionPolicy PreemptLowerPriority when
//     there is no global default PriorityClass. A default class sets
//     priorityClassName and is refused.
//   - DefaultTolerationSeconds: the not-ready and unreachable NoExecute
//     tolerations of 300 seconds, appended to a Pod without tolerations.
//     Other durations, configured by API server flags, are refused.
//
// The fill applies only where the field is unset, so a Pod admitted without
// these plugins matches as well. Job templates are not admitted and are not
// filled. LimitRanger, AlwaysPullImages, Pod security mutation and
// webhooks are not filled; their changes are refused.
func admittedPod(s *corev1.PodSpec) {
	if s.ServiceAccountName == "" {
		s.ServiceAccountName = "default"
	}
	if s.DeprecatedServiceAccount == "" {
		s.DeprecatedServiceAccount = s.ServiceAccountName
	}
	if s.Priority == nil && s.PriorityClassName == "" {
		s.Priority = ptr(int32(0))
	}
	if s.PreemptionPolicy == nil {
		s.PreemptionPolicy = ptr(corev1.PreemptLowerPriority)
	}
	if len(s.Tolerations) == 0 {
		s.Tolerations = []corev1.Toleration{
			{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr(int64(300))},
			{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr(int64(300))},
		}
	}
}

// scheduledNode removes the node a direct Pod was bound to by the scheduler.
// It applies to reads only, and only to a Pod with the PodScheduled
// condition that binding records; a create response never carries a
// binding, so a node name there was requested or injected and is refused,
// durably (see startReceipted). After a lost response, a read's node name
// with PodScheduled=True is accepted as trusted placement output of the API
// server. It is not proof that the scheduler made the original choice, that
// the node or its devices qualify, or that a trusted cluster administrator
// with the same access did not set both.
func scheduledNode(p *corev1.Pod) {
	if p.Spec.NodeName == "" {
		return
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionTrue {
			p.Spec.NodeName = ""
			return
		}
	}
}

// canonicalQuantities writes every resource quantity of s in one format, so
// that semantically equal quantities encode equally. Every resource name is
// kept.
func canonicalQuantities(s *corev1.PodSpec) {
	lists := []corev1.ResourceList{s.Overhead}
	if s.Resources != nil {
		lists = append(lists, s.Resources.Requests, s.Resources.Limits)
	}
	for _, cs := range [][]corev1.Container{s.InitContainers, s.Containers} {
		for _, c := range cs {
			lists = append(lists, c.Resources.Requests, c.Resources.Limits)
		}
	}
	for _, l := range lists {
		for name, q := range l {
			l[name] = *resource.NewDecimalQuantity(*q.AsDec(), resource.DecimalSI)
		}
	}
}

func ptr[T any](v T) *T { return &v }

// strictObject is a Job or Pod as the API server encodes it. Decoding it
// refuses any field the vendored API types do not represent, which would
// otherwise be dropped from the identity. Status is decoded leniently.
type strictObject[S any] struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Spec       S                 `json:"spec"`
	Status     json.RawMessage   `json:"status"`
}

func (o *strictObject[S]) decode(raw []byte, apiVersion, kind string, status any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err := dec.Decode(o)
	if err == nil {
		if _, terr := dec.Token(); terr != io.EOF {
			err = errors.New("trailing data")
		}
	}
	if err == nil && (o.APIVersion != "" && o.APIVersion != apiVersion || o.Kind != "" && o.Kind != kind) {
		err = errors.New("unexpected type")
	}
	if err == nil && len(o.Status) != 0 {
		err = json.Unmarshal(o.Status, status)
	}
	if err != nil {
		return fmt.Errorf("%w: decode kubernetes %s: %v", backend.ErrIdentityMismatch, kind, err)
	}
	return nil
}

// decodeParent decodes raw, a create response or a read of a parent of kind.
func decodeParent(kind string, raw []byte) (runtime.Object, error) {
	if kind == "Job" {
		var o strictObject[batchv1.JobSpec]
		j := &batchv1.Job{}
		if err := o.decode(raw, "batch/v1", kind, &j.Status); err != nil {
			return nil, err
		}
		j.TypeMeta, j.ObjectMeta, j.Spec = metav1.TypeMeta{APIVersion: "batch/v1", Kind: kind}, o.Metadata, o.Spec
		return j, nil
	}
	var o strictObject[corev1.PodSpec]
	p := &corev1.Pod{}
	if err := o.decode(raw, "v1", kind, &p.Status); err != nil {
		return nil, err
	}
	p.TypeMeta, p.ObjectMeta, p.Spec = metav1.TypeMeta{APIVersion: "v1", Kind: kind}, o.Metadata, o.Spec
	return p, nil
}

func receiptMeta(obj runtime.Object) *metav1.ObjectMeta {
	switch o := obj.(type) {
	case *batchv1.Job:
		return &o.ObjectMeta
	case *corev1.Pod:
		return &o.ObjectMeta
	}
	return nil
}

// expectedParent renders the expected parent of attempt id for t.
func (b *Backend) expectedParent(kind, id string, t backend.Task) (runtime.Object, string, error) {
	name, err := jobName(id)
	if err != nil {
		return nil, "", err
	}
	hash, err := taskHash(t)
	if err != nil {
		return nil, "", err
	}
	if kind == "Job" {
		return b.jobObject(id, name, hash, t), hash, nil
	}
	return b.retryPod(id, name, hash, t), hash, nil
}

// matchReceipted validates obj, a create response or, if read, a later read,
// against receipt r, whose spec digest is that of the expected object. It
// returns obj's UID.
func (b *Backend) matchReceipted(obj runtime.Object, r executionReceipt, read bool) (string, error) {
	m := receiptMeta(obj)
	if m == nil {
		return "", fmt.Errorf("%w: unexpected kubernetes object", backend.ErrIdentityMismatch)
	}
	uid := string(m.UID)
	spec, err := receiptSpec(obj, read)
	if err != nil || spec != r.SpecSHA256 || m.Name != r.Name || m.Namespace != b.namespace || len(m.OwnerReferences) != 0 ||
		m.Labels[ownerLabel] != b.owner || m.Labels[idLabel] != r.Attempt ||
		m.Annotations[taskAnnotation] != r.TaskSHA256 || m.Annotations[bindingAnnotation] != b.binding ||
		m.Annotations[nonceAnnotation] != r.Nonce || m.Annotations[specAnnotation] != r.SpecSHA256 ||
		!validInstance(uid) || r.Phase == phasePinned && uid != r.UID {
		return "", fmt.Errorf("%w: kubernetes execution differs from its receipt", backend.ErrIdentityMismatch)
	}
	return uid, nil
}

// receiptClient returns the REST client of the receipt client (see
// newReceiptClient) for parents of kind, and their resource.
func (b *Backend) receiptClient(kind string) (*rest.RESTClient, string, error) {
	var c rest.Interface
	resource := "pods"
	if kind == "Job" {
		c, resource = b.client.BatchV1().RESTClient(), "jobs"
	} else {
		c = b.client.CoreV1().RESTClient()
	}
	client, err := receiptHTTP(c)
	if err != nil {
		return nil, "", err
	}
	return client, resource, nil
}

// startReceipted creates obj, the rendered parent of attempt id, at most once.
// It durably records the identity and nonce, consumes the single create grant,
// sends one request without client retries or redirects and with a body no
// transport layer can obtain again (see newReceiptClient), validates the
// response as strictly as a recovery read, and pins the returned UID. Any
// failure leaves the receipt in a phase that never permits another request.
//
// A complete success response that is not 201 Created, or does not decode or
// match the receipt, and any complete 3xx response, are refused durably
// before Start returns, so that no later read adopts the object a create may
// have made, with a witness of the actual status, body digest and size and
// decode outcome. A 3xx is classified by receiptTransport's own record of the
// status and body, whatever error client-go derives from it, so that it is
// neither taken for a definite rejection nor reported as a lost response. A
// 3xx whose body did not arrive completely is not known and leaves the
// receipt sent. If the refusal write fails or is uncertain, Start returns the
// storage error with the refusal; this store then authorizes nothing for the
// attempt, but another process may still find the record sent. A lost,
// incomplete or failed response without a definite
// rejection leaves the receipt sent, and only the exact precommitted object
// may then be recovered by a read. A definite rejection proves that nothing
// was persisted and is recorded rejected, never refused.
func (b *Backend) startReceipted(ctx context.Context, id, hash string, obj runtime.Object) error {
	kind := "Pod"
	if _, ok := obj.(*batchv1.Job); ok {
		kind = "Job"
	}
	client, resource, err := b.receiptClient(kind)
	if err != nil {
		return err
	}
	spec, err := receiptSpec(obj, false)
	if err != nil {
		return err
	}
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("kubernetes creation nonce: %w", err)
	}
	r := executionReceipt{Attempt: id, TaskSHA256: hash, Kind: kind, Nonce: hex.EncodeToString(nonce[:]), SpecSHA256: spec, Phase: phaseIntent}
	r.Name, _ = jobName(id)
	m := receiptMeta(obj)
	m.Annotations[nonceAnnotation] = r.Nonce
	m.Annotations[specAnnotation] = spec
	if err := b.receipts.create(r); err != nil {
		return err
	}
	if _, err := b.receipts.update(id, func(x *executionReceipt) error {
		if x.Phase != phaseIntent || x.Nonce != r.Nonce {
			return errors.New("kubernetes create grant unavailable")
		}
		x.Phase = phaseSent
		return nil
	}); err != nil {
		return err
	}
	r.Phase = phaseSent
	if err := b.receipts.step("send", &r); err != nil {
		return err
	}
	refuse := func(err error, code int, raw []byte) error {
		if rerr := b.receipts.refuse(id, r.Nonce, responseWitness(kind, code, raw)); rerr != nil {
			return errors.Join(err, rerr)
		}
		return err
	}
	// Decode the response alone, strictly, so that it is validated by itself.
	var code int
	x := new(receiptExchange)
	res := client.Post().Namespace(b.namespace).Resource(resource).
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(obj).
		MaxRetries(0).Do(context.WithValue(ctx, receiptExchangeKey{}, x)).StatusCode(&code)
	if status, raw, complete := x.redirect(); status != 0 {
		// The redirect was not followed. A complete one is a known
		// response that does not report this request's creation.
		if !complete {
			return errors.Join(fmt.Errorf("kubernetes create response status %d incomplete", status), res.Error())
		}
		return refuse(fmt.Errorf("%w: kubernetes create returned status %d", backend.ErrIdentityMismatch, status), status, raw)
	}
	if err = res.Error(); err != nil {
		if definiteRejection(err) {
			if _, uerr := b.receipts.update(id, func(x *executionReceipt) error {
				if x.Phase != phaseSent || x.Nonce != r.Nonce {
					return errors.New("kubernetes create receipt changed")
				}
				x.Phase = phaseRejected
				return nil
			}); uerr != nil {
				return errors.Join(err, uerr)
			}
		}
		return err
	}
	// The client read the whole body without error, so code is the actual
	// status of a complete response. A success status is required to know
	// that; anything else remains uncertain.
	if code < 200 || code > 299 {
		return fmt.Errorf("kubernetes create response status %d unknown", code)
	}
	raw, _ := res.Raw()
	var uid string
	result, err := decodeParent(kind, raw)
	if err == nil {
		uid, err = b.matchReceipted(result, r, false)
	}
	if err == nil && code != http.StatusCreated {
		// A complete success response other than 201 does not report this
		// request's creation, even with a matching body: the object it
		// describes is never adopted.
		err = fmt.Errorf("%w: kubernetes create returned status %d, not created", backend.ErrIdentityMismatch, code)
	}
	if err != nil {
		return refuse(err, code, raw)
	}
	_, err = b.receipts.update(id, func(x *executionReceipt) error { return pinReceipt(x, r.Nonce, uid) })
	if errors.Is(err, errReceiptPinned) {
		// A recovery read pinned another UID meanwhile; the response
		// conflicts with it.
		return refuse(err, code, raw)
	}
	return err
}

// responseWitness returns the witness of raw, the complete body of a refused
// create response for a parent of kind, received with HTTP status code. Only
// values of a strictly decoded body that are well formed are recorded.
func responseWitness(kind string, code int, raw []byte) receiptWitness {
	sum := sha256.Sum256(raw)
	w := receiptWitness{Status: code, ResponseSHA256: hex.EncodeToString(sum[:]),
		ResponseBytes: int64(len(raw)), Decode: witnessUndecodable}
	obj, err := decodeParent(kind, raw)
	if err != nil {
		return w
	}
	w.Decode = witnessDecoded
	m := receiptMeta(obj)
	if uid := string(m.UID); validInstance(uid) {
		w.UID = uid
	}
	if v := m.Annotations[nonceAnnotation]; isLowerHex(v, 64) {
		w.Nonce = v
	}
	if v := m.Annotations[specAnnotation]; isLowerHex(v, 64) {
		w.SpecAnnotation = v
	}
	if spec, err := receiptSpec(obj, false); err == nil {
		w.SpecSHA256 = spec
	}
	return w
}

// definiteRejection reports an API status proving the create was not
// persisted. Any other failure leaves the outcome uncertain.
func definiteRejection(err error) bool {
	switch apierrors.ReasonForError(err) {
	case metav1.StatusReasonAlreadyExists, metav1.StatusReasonInvalid, metav1.StatusReasonForbidden:
		return true
	}
	return false
}

// errReceiptPinned reports a UID that differs from the one already pinned.
var errReceiptPinned = fmt.Errorf("%w: kubernetes execution receipt pinned another instance", backend.ErrIdentityMismatch)

func pinReceipt(x *executionReceipt, nonce, uid string) error {
	if x.Nonce != nonce {
		return fmt.Errorf("%w: kubernetes execution receipt changed", backend.ErrIdentityMismatch)
	}
	if x.Phase == phasePinned {
		if x.UID == uid {
			return nil
		}
		return errReceiptPinned
	}
	if x.Phase != phaseSent {
		return fmt.Errorf("%w: kubernetes execution receipt differs", backend.ErrIdentityMismatch)
	}
	x.Phase, x.UID = phasePinned, uid
	return nil
}

// receiptFor validates ref against its durable receipt without API requests.
// It returns the receipt and the expected parent rendered from ref.Task.
// A retired or refused receipt authorizes nothing.
func (b *Backend) receiptFor(ref backend.Reference, kind string) (executionReceipt, runtime.Object, error) {
	if b.binding == "" || ref.Binding != b.binding {
		return executionReceipt{}, nil, fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	if ref.Instance != "" && !validInstance(ref.Instance) {
		return executionReceipt{}, nil, fmt.Errorf("%w: invalid kubernetes instance", backend.ErrIdentityMismatch)
	}
	if err := b.Check(ref.Task); err != nil {
		return executionReceipt{}, nil, err
	}
	want, hash, err := b.expectedParent(kind, ref.ID, ref.Task)
	if err != nil {
		return executionReceipt{}, nil, err
	}
	spec, err := receiptSpec(want, false)
	if err != nil {
		return executionReceipt{}, nil, err
	}
	r, err := b.receipts.load(ref.ID)
	if err != nil {
		return r, nil, err
	}
	switch r.Phase {
	case phaseRetired:
		return r, nil, errReceiptRetired
	case phaseRefused:
		return r, nil, errReceiptRefused
	}
	if r.Kind != kind || r.TaskSHA256 != hash || r.SpecSHA256 != spec {
		return r, nil, fmt.Errorf("%w: kubernetes execution receipt differs", backend.ErrIdentityMismatch)
	}
	if len(r.Objects) != 0 {
		return r, nil, errors.New("kubernetes dependent object custody is not implemented")
	}
	if ref.Instance != "" && (r.Phase == phasePinned && r.UID != ref.Instance || r.Phase != phasePinned && r.Phase != phaseSent) {
		return r, nil, fmt.Errorf("%w: kubernetes instance differs from its receipt", backend.ErrIdentityMismatch)
	}
	return r, want, nil
}

// receiptedParent reads the parent recorded by ref's receipt. Before its UID
// is pinned, it recovers a lost create response only from the exact
// precommitted object, matched by nonce, specification, owner, binding and
// namespace. A missing object then remains createUncertain. Once pinned, a
// missing object is returned as the API's NotFound error.
//
// The receipt is loaded before the GET, which may be held arbitrarily long.
// After the response is matched, or found missing, and before anything is
// returned, the current receipt is read again (recheckReceipt): a record
// refused or retired meanwhile returns its error instead of the read, so
// that a read admitted before a refusal never returns authority after it.
// The returned receipt is the current one.
func (b *Backend) receiptedParent(ctx context.Context, ref backend.Reference, kind string) (runtime.Object, executionReceipt, error) {
	r, _, err := b.receiptFor(ref, kind)
	if err != nil {
		return nil, r, err
	}
	switch r.Phase {
	case phaseIntent:
		return nil, r, createUncertain("kubernetes create pending")
	case phaseRejected:
		return nil, r, fmt.Errorf("%w: kubernetes create was rejected", backend.ErrIdentityMismatch)
	}
	client, resource, err := b.receiptClient(kind)
	if err != nil {
		return nil, r, err
	}
	res := client.Get().Namespace(b.namespace).Resource(resource).Name(r.Name).Do(ctx)
	err = res.Error()
	if apierrors.IsNotFound(err) {
		cur, cerr := b.recheckReceipt(ref.ID, r)
		if cerr != nil {
			return nil, cur, cerr
		}
		if cur.Phase == phaseSent {
			return nil, cur, createUncertain("kubernetes create outcome uncertain")
		}
		return nil, cur, err
	}
	if err != nil {
		return nil, r, err
	}
	raw, _ := res.Raw()
	obj, err := decodeParent(kind, raw)
	if err != nil {
		return nil, r, err
	}
	uid, err := b.matchReceipted(obj, r, true)
	if err != nil {
		return nil, r, err
	}
	if ref.Instance != "" && uid != ref.Instance {
		return nil, r, fmt.Errorf("%w: kubernetes instance differs from its receipt", backend.ErrIdentityMismatch)
	}
	if r.Phase == phaseSent {
		// update reads the current record under the journal lock and
		// refuses a refused, blocked or retired one: it is the recheck.
		nonce := r.Nonce
		r, err = b.receipts.update(ref.ID, func(x *executionReceipt) error { return pinReceipt(x, nonce, uid) })
		if err != nil {
			return nil, r, err
		}
		return obj, r, nil
	}
	cur, err := b.recheckReceipt(ref.ID, r)
	if err != nil {
		return nil, cur, err
	}
	return obj, cur, nil
}

// recheckReceipt returns the attempt's current receipt after a native read
// performed under r, loaded before it. It returns errReceiptRefused or
// errReceiptRetired for a record refused (durably, or blocked in this store)
// or retired meanwhile, and refuses any other change of identity or phase
// except sent becoming pinned by a concurrent recovery read.
//
// Writes that keep the identity, phase and UID, such as recorded
// cancellation steps, advance the generation without invalidating the read;
// the current record is returned, never r, so that callers act on fresh
// state. The recheck is a short read under the journal lock, which is not
// held across the native request.
func (b *Backend) recheckReceipt(id string, r executionReceipt) (executionReceipt, error) {
	cur, err := b.receipts.load(id)
	if err != nil {
		return cur, err
	}
	switch cur.Phase {
	case phaseRetired:
		return cur, errReceiptRetired
	case phaseRefused:
		return cur, errReceiptRefused
	}
	same := cur.Nonce == r.Nonce && cur.Kind == r.Kind && cur.Name == r.Name &&
		cur.TaskSHA256 == r.TaskSHA256 && cur.SpecSHA256 == r.SpecSHA256 && cur.Generation >= r.Generation
	switch {
	case !same:
	case r.Phase == phasePinned && cur.Phase == phasePinned && cur.UID == r.UID:
		return cur, nil
	case r.Phase == phaseSent && (cur.Phase == phaseSent || cur.Phase == phasePinned):
		return cur, nil
	}
	return cur, fmt.Errorf("%w: kubernetes execution receipt changed during the read", backend.ErrIdentityMismatch)
}

// receiptCurrent reports whether ref's receipt still authorizes its pinned
// instance uid, without a native request. It is the boundary before a
// native request that follows an earlier read, and before log bytes read
// under an earlier check are released.
func (b *Backend) receiptCurrent(ref backend.Reference, kind, uid string) error {
	r, _, err := b.receiptFor(ref, kind)
	if err != nil {
		return err
	}
	if r.Phase != phasePinned || r.UID != uid {
		return fmt.Errorf("%w: kubernetes execution receipt changed", backend.ErrIdentityMismatch)
	}
	return nil
}

// receiptedJob is get for a receipted target. It also returns the pinned UID.
func (b *Backend) receiptedJob(ctx context.Context, ref backend.Reference) (*batchv1.Job, string, error) {
	obj, r, err := b.receiptedParent(ctx, ref, "Job")
	if err != nil {
		return nil, "", err
	}
	return obj.(*batchv1.Job), r.UID, nil
}

func (b *Backend) observeJobReceipted(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	obj, r, err := b.receiptedParent(ctx, ref, "Job")
	var pending createUncertain
	if errors.As(err, &pending) {
		return backend.Status{State: backend.Starting, Reason: string(pending)}, nil
	}
	if apierrors.IsNotFound(err) {
		if r.Cancel == stepDelivered {
			return backend.Status{State: backend.Canceled, Instance: r.UID, Reason: "kubernetes job deleted"}, nil
		}
		return backend.Status{State: backend.Unknown, Instance: r.UID, Reason: "kubernetes job missing"}, nil
	}
	if err != nil {
		return backend.Status{}, err
	}
	status, err := b.jobStatus(ctx, obj.(*batchv1.Job), ref.ID)
	if err != nil {
		return backend.Status{}, err
	}
	// jobStatus lists Pods after the recheck; refuse to report status under
	// a receipt refused meanwhile.
	if err := b.receiptCurrent(ref, "Job", r.UID); err != nil {
		return backend.Status{}, err
	}
	status.Instance = r.UID
	return status, nil
}

// cancelJobReceipted records cancellation intent before the UID-preconditioned
// deletion and its delivery after a successful response. NotFound proves
// neither delivery nor release; the receipt then stays at intent.
func (b *Backend) cancelJobReceipted(ctx context.Context, ref backend.Reference) error {
	obj, r, err := b.receiptedParent(ctx, ref, "Job")
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	j := obj.(*batchv1.Job)
	for _, c := range j.Status.Conditions {
		if c.Status == corev1.ConditionTrue && (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) {
			return nil
		}
	}
	if err := b.recordCancel(ref.ID, r.UID, stepIntent); err != nil {
		return err
	}
	uid := types.UID(r.UID)
	policy := metav1.DeletePropagationForeground
	err = b.client.BatchV1().Jobs(b.namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &policy, Preconditions: &metav1.Preconditions{UID: &uid}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return b.recordCancel(ref.ID, r.UID, stepDelivered)
}

func (b *Backend) recordCancel(id, uid, step string) error {
	_, err := b.receipts.update(id, func(x *executionReceipt) error {
		if x.Phase != phasePinned || x.UID != uid {
			return fmt.Errorf("%w: kubernetes execution receipt differs", backend.ErrIdentityMismatch)
		}
		if x.Cancel != stepDelivered {
			x.Cancel = step
		}
		return nil
	})
	return err
}

// resolveRetry pins a retry receipt whose create response was lost. A pinned
// receipt needs no request here; a missing pinned Pod is left to the caller,
// whose getRetry matches every read of a pinned Pod as receiptedParent does.
func (b *Backend) resolveRetry(ctx context.Context, ref backend.Reference) error {
	r, _, err := b.receiptFor(ref, "Pod")
	if err != nil || r.Phase == phasePinned {
		return err
	}
	_, _, err = b.receiptedParent(ctx, ref, "Pod")
	return err
}

// retryCanceled reports recorded cancellation intent for a retry attempt.
// With receipts it is also the last check before Observe reports a stopped
// Pod, so a receipt refused or retired meanwhile refuses.
func (b *Backend) retryCanceled(id string) (bool, error) {
	if b.receipts != nil {
		r, err := b.receipts.load(id)
		if err != nil {
			return false, err
		}
		switch r.Phase {
		case phaseRefused:
			return false, errReceiptRefused
		case phaseRetired:
			return false, errReceiptRetired
		}
		return r.Cancel != "", nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retries[id] != nil && b.retries[id].cancel, nil
}
