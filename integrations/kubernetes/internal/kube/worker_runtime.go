package kube

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// The worker barrier profile. Its Pods run the skygo worker helper, which
// prepares, reports ready and executes the task argv only after the Pod's own
// start token arrives in workerStartAnnotation.
const (
	// workerBarrierProfile replaces workerProfileVersion on barrier Pods and
	// their Service. It identifies this builder, the helper verbs, mounts and
	// environment below and the start token encoding.
	workerBarrierProfile = "skygo-worker-pod-v3"

	// workerStartAnnotation holds the lowercase hex start token. Only
	// StartWorker writes it, by an update fenced by UID and resourceVersion.
	workerStartAnnotation = "skygo.tmc.dev/worker-start"

	workerHelperVolume = "skygo-helper"
	workerStartVolume  = "skygo-worker-start"
	workerStateVolume  = "skygo-worker-state"
	workerStartMount   = "/run/skygo/worker"
	workerStateMount   = "/run/skygo/worker-state"
	workerStartFile    = "start"

	workerInstallVerb = "install"
	workerHelperVerb  = "worker"
	workerReadyVerb   = "worker-ready"

	workerEnvMode       = "SKYGO_WORKER_MODE"
	workerEnvTaskSHA256 = "SKYGO_TASK_SHA256"
	workerEnvPodUID     = "SKYGO_POD_UID"

	// workerStopDeadline is the activeDeadlineSeconds CancelWorker sets on a
	// bound member: the kubelet then stops its containers and records a
	// terminal phase.
	workerStopDeadline = int64(1)
)

var (
	errWorkerStopUnproven = errors.New("kubernetes worker has no positive stop evidence")
	errWorkerRemoving     = errors.New("kubernetes worker resource is still being deleted")
	errWorkerNotFound     = errors.New("kubernetes worker resource not found; its create remains unresolved")
	errWorkerPodChanged   = errors.New("kubernetes worker pod changed during the operation")
	errWorkerNotPrepared  = errors.New("kubernetes worker is not prepared")
)

// workerRuntime is the Kubernetes worker barrier backend of one bound target.
// It owns a worker-only HTTP client and borrows the Backend's bounded original
// operation frames. A returned unsettled error retains those same originals;
// neither cancellation nor restart is a closure proof. It is not registered
// or advertised.
type workerRuntime struct {
	b        *Backend
	registry workerRegistry
	http     *workerHTTP
}

var _ backend.WorkerBarrierBackend = (*workerRuntime)(nil)

// newWorkerRuntime returns the worker runtime of target, whose Backend must be
// bound to target.Binding and have a data helper image. registry is the
// operator's trusted snapshot of the namespace's default service account; it
// is copied.
func newWorkerRuntime(target backend.Target, registry workerRegistry, config *rest.Config) (*workerRuntime, error) {
	b, ok := target.Backend.(*Backend)
	if !ok || b == nil || b.binding == "" || b.binding != target.Binding {
		return nil, errors.New("kubernetes worker runtime requires a bound kubernetes target")
	}
	if b.data == nil || b.data.helperImage == "" || b.data.helperSHA256 == "" {
		return nil, errors.New("kubernetes workers require the target's helper image and digest")
	}
	if err := registry.check(); err != nil {
		return nil, err
	}
	h, err := newWorkerHTTP(b, config)
	if err != nil {
		return nil, err
	}
	return &workerRuntime{b: b, http: h, registry: workerRegistry{
		ServiceAccountUID: registry.ServiceAccountUID, PullSecrets: slices.Clone(registry.PullSecrets)}}, nil
}

// workerMember is one validated member reference with its plan, its expected
// Pod and its start token, which is empty when the reference cannot start.
type workerMember struct {
	ref   backend.WorkerReference
	plan  workerPodPlan
	want  *corev1.Pod
	token string
}

// owner reports whether m owns its group's rendezvous Service.
func (m workerMember) owner() bool {
	return m.ref.Group.Spec.Mode == backend.WorkerTCP && m.ref.Rank == 0
}

// member validates ref and builds its plan and Pod before any request.
func (rt *workerRuntime) member(ref backend.WorkerReference, cleanup bool) (workerMember, error) {
	if err := backend.ValidateWorkerReference(ref, cleanup); err != nil {
		return workerMember{}, err
	}
	m, err := rt.plan(ref.Group, ref.Rank)
	if err != nil {
		return workerMember{}, err
	}
	m.ref = ref
	mismatch := fmt.Errorf("%w: kubernetes worker name differs", backend.ErrIdentityMismatch)
	if ref.Pod != (backend.WorkerResource{}) && ref.Pod.Name != m.want.Name {
		return workerMember{}, mismatch
	}
	if ref.Service != (backend.WorkerResource{}) && ref.Service.Name != workerServiceName(m.plan.AttemptID) {
		return workerMember{}, mismatch
	}
	if t, err := backend.WorkerStartToken(ref); err == nil {
		m.token = hex.EncodeToString(t[:])
	}
	return m, nil
}

// plan returns rank of group g with its expected Pod. The caller has
// validated g.
func (rt *workerRuntime) plan(g backend.WorkerGroup, rank uint8) (workerMember, error) {
	if g.Execution.Binding != rt.b.binding {
		return workerMember{}, fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	mode := workerIndependent
	if g.Spec.Mode == backend.WorkerTCP {
		mode = workerTCP
	}
	p := workerPodPlan{JobID: g.Execution.ID, AttemptID: g.AttemptID, Rank: uint32(rank),
		Count: uint32(g.Spec.Count), Task: g.Execution.Task, Mode: mode, Registry: rt.registry}
	want, err := rt.pod(p)
	if err != nil {
		return workerMember{}, err
	}
	return workerMember{plan: p, want: want}, nil
}

// pod returns the barrier Pod of plan p: workerPod's Pod with the worker
// helper as the task command, a helper init container that installs the
// helper, a downward API start file and a prepared readiness check. It
// contacts no API.
func (rt *workerRuntime) pod(p workerPodPlan) (*corev1.Pod, error) {
	for _, k := range []string{workerEnvMode, workerEnvTaskSHA256, workerEnvPodUID} {
		if _, ok := p.Task.Env[k]; ok {
			return nil, fmt.Errorf("environment %s is reserved for workers", k)
		}
	}
	pod, err := rt.b.workerPod(p)
	if err != nil {
		return nil, err
	}
	pod.Annotations[workerProfileAnnotation] = workerBarrierProfile
	mode := backend.WorkerIndependent
	if p.Mode == workerTCP {
		mode = backend.WorkerTCP
	}

	spec := &pod.Spec
	c := &spec.Containers[0]
	c.Command = append([]string{helperCopy, workerHelperVerb, "--"}, p.Task.Argv...)
	// The API server defaults; set explicitly for exact comparison.
	c.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{
			Command: []string{helperCopy, workerReadyVerb}}},
		TimeoutSeconds: 1, PeriodSeconds: 2, SuccessThreshold: 1, FailureThreshold: 3,
	}
	c.Env = append(c.Env,
		corev1.EnvVar{Name: workerEnvMode, Value: string(mode)},
		corev1.EnvVar{Name: workerEnvTaskSHA256, Value: pod.Annotations[taskAnnotation]},
		corev1.EnvVar{Name: workerEnvPodUID, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}})
	slices.SortFunc(c.Env, func(x, y corev1.EnvVar) int { return strings.Compare(x.Name, y.Name) })
	c.VolumeMounts = []corev1.VolumeMount{
		{Name: workerHelperVolume, MountPath: helperMount, ReadOnly: true},
		{Name: workerStartVolume, MountPath: workerStartMount, ReadOnly: true},
		{Name: workerStateVolume, MountPath: workerStateMount},
	}
	spec.InitContainers = []corev1.Container{{
		Name: "helper", Image: rt.b.data.helperImage,
		Command:         []string{helperEntry, workerInstallVerb},
		Env:             profileEnv(map[string]string{profileEnvHelperSHA256: rt.b.data.helperSHA256}),
		ImagePullPolicy: corev1.PullIfNotPresent,
		VolumeMounts:    []corev1.VolumeMount{{Name: workerHelperVolume, MountPath: helperMount}},
		SecurityContext: profileContainerSecurity(),
	}}
	readOnly := int32(0o444)
	spec.Volumes = []corev1.Volume{
		{Name: workerHelperVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: workerStartVolume, VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{
			Items: []corev1.DownwardAPIVolumeFile{{Path: workerStartFile, FieldRef: &corev1.ObjectFieldSelector{
				APIVersion: "v1", FieldPath: "metadata.annotations['" + workerStartAnnotation + "']"}}},
			DefaultMode: &readOnly}}},
		{Name: workerStateVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	return pod, nil
}

// workerFacts are the fields in which an original barrier Pod may differ
// from its expected Pod, each written only by this runtime or by deletion.
type workerFacts struct {
	gated     bool // the one skygo scheduling gate is present
	started   bool // the start annotation holds the member's token
	stopping  bool // CancelWorker's deadline is set or the Pod is being deleted
	finalized bool // being deleted with the skygo finalizer already removed
}

// workerBarrierMatches reports the facts of got if it is the barrier Pod
// want created with original UID uid, and an error wrapping
// backend.ErrIdentityMismatch otherwise. The start annotation must be absent
// or equal token; an empty token admits only its absence. Status is not
// compared.
func workerBarrierMatches(got *corev1.Pod, want *corev1.Pod, uid types.UID, token string) (workerFacts, error) {
	mismatch := fmt.Errorf("%w: kubernetes worker pod differs", backend.ErrIdentityMismatch)
	if uid == "" || !validInstance(string(uid)) {
		return workerFacts{}, fmt.Errorf("%w: worker pod original uid is unavailable", backend.ErrIdentityMismatch)
	}
	if got == nil || got.UID != uid {
		return workerFacts{}, mismatch
	}
	var f workerFacts
	meta := got.ObjectMeta.DeepCopy()
	if v, ok := meta.Annotations[workerStartAnnotation]; ok {
		if token == "" || v != token {
			return workerFacts{}, mismatch
		}
		f.started = true
		delete(meta.Annotations, workerStartAnnotation)
	}
	if !profileMetaMatches(want.ObjectMeta, *meta) {
		return workerFacts{}, mismatch
	}
	switch {
	case slices.Equal(got.Finalizers, want.Finalizers):
	case got.DeletionTimestamp != nil && len(got.Finalizers) == 0:
		f.finalized = true
	default:
		return workerFacts{}, mismatch
	}
	spec := got.Spec.DeepCopy()
	switch {
	case slices.Equal(spec.SchedulingGates, want.Spec.SchedulingGates):
		f.gated = true
	case len(spec.SchedulingGates) == 0:
		spec.SchedulingGates = want.Spec.SchedulingGates
	default:
		return workerFacts{}, mismatch
	}
	w := want.Spec.ActiveDeadlineSeconds
	if d := spec.ActiveDeadlineSeconds; d != nil && *d == workerStopDeadline && (w == nil || *w != workerStopDeadline) {
		f.stopping = true
		spec.ActiveDeadlineSeconds = w
	}
	if !fixedPodSpecMatches(&want.Spec, spec) {
		return workerFacts{}, mismatch
	}
	if got.DeletionTimestamp != nil {
		f.stopping = true
	}
	return f, nil
}

// read gets m's Pod by its original name, in a consistent read, and matches
// it against the original UID. A Pod that is missing or has another UID has
// vanished. got is the Pod read, if any, even alongside an error.
func (rt *workerRuntime) read(ctx context.Context, m workerMember) (*corev1.Pod, workerFacts, error) {
	got, err := rt.getPod(ctx, m.ref.Pod.Name)
	if err != nil {
		return nil, workerFacts{}, workerVanished(err)
	}
	if string(got.UID) != m.ref.Pod.UID {
		return got, workerFacts{}, fmt.Errorf("%w: %w: original uid absent", backend.ErrWorkerVanished, backend.ErrIdentityMismatch)
	}
	f, err := workerBarrierMatches(got, m.want, types.UID(m.ref.Pod.UID), m.token)
	return got, f, err
}

// workerVanished wraps a NotFound read of an original resource with
// backend.ErrWorkerVanished; other errors are returned unchanged.
func workerVanished(err error) error {
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		return err
	}
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: %w", backend.ErrWorkerVanished, err)
	}
	return err
}

// CreateWorker creates the gated barrier Pod of c, after rank 0's Service in
// a WorkerTCP group. It sends each Create once, by workerPost, and never
// retries, reads or adopts by name: AlreadyExists, a lost response and any
// other error leave that resource without a receipt. Only a response proving
// that the request was not applied wraps backend.ErrWorkerCreateRefused. A
// response stored differently from the request is returned as a receipt, so
// that it can be cleaned up, with an error; it grants no release.
func (rt *workerRuntime) CreateWorker(ctx context.Context, c backend.WorkerCreate) (out backend.WorkerCreated, err error) {
	if err := backend.ValidateWorkerCreated(c, out, true); err != nil {
		return out, err
	}
	m, err := rt.plan(c.Group, c.Rank)
	if err != nil {
		return out, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(c.Group, c.Rank), false, false)
	if err != nil {
		if errors.Is(err, errWorkerHTTPAdmission) && !errors.Is(err, backend.ErrWorkerUnsettled) {
			return out, fmt.Errorf("%w: %w", backend.ErrWorkerCreateRefused, err)
		}
		return out, err
	}
	defer func() { err = call.finish(err) }()
	client, ok := rt.http.client.CoreV1().RESTClient().(*rest.RESTClient)
	if !ok || client == nil {
		return out, errors.New("kubernetes workers require a REST client")
	}
	if c.Group.Spec.Mode == backend.WorkerTCP && c.Rank == 0 {
		want, err := rt.service(m.plan)
		if err != nil {
			return out, err
		}
		got := &corev1.Service{}
		err = errors.Join(workerPost(rt.http.request(ctx), client, rt.b.namespace, "services", want, got), rt.http.check(ctx))
		if got.Name == want.Name && validInstance(string(got.UID)) {
			out.Service = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
		}
		if err != nil {
			return out, workerCreateError(err)
		}
		if got.Name != want.Name || !validInstance(string(got.UID)) {
			return out, errors.New("kubernetes worker service create returned no identity")
		}
		out.Service = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
		if _, err := rt.serviceMatches(got, m.plan, got.UID); err != nil {
			return out, fmt.Errorf("kubernetes worker service stored altered: %w", err)
		}
	}
	got := &corev1.Pod{}
	err = errors.Join(workerPost(rt.http.request(ctx), client, rt.b.namespace, "pods", m.want, got), rt.http.check(ctx))
	if got.Name == m.want.Name && validInstance(string(got.UID)) {
		out.Pod = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
	}
	if err != nil {
		return out, workerCreateError(err)
	}
	if got.Name != m.want.Name || !validInstance(string(got.UID)) {
		return out, errors.New("kubernetes worker pod create returned no identity")
	}
	out.Pod = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
	if f, err := workerBarrierMatches(got, m.want, got.UID, ""); err != nil || !f.gated || f.stopping {
		return out, errors.New("kubernetes worker pod stored altered")
	}
	return out, nil
}

// workerPost sends one Create of obj to resource in namespace by client, the
// target's REST client with its credentials and codec, and decodes the
// response into result. It disables client-go's retries for this request
// only. A typed Create would resend the POST after a 5xx or 429 response
// carrying Retry-After, even when the first request was applied; the resend
// then meets AlreadyExists, which would turn an unknown result into a
// refusal and hide the original object.
func workerPost(ctx context.Context, client *rest.RESTClient, namespace, resource string, obj, result runtime.Object) error {
	return client.Post().Namespace(namespace).Resource(resource).
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(obj).
		MaxRetries(0).Do(ctx).Into(result)
}

// workerCreateError wraps a Create error that proves the request was not
// applied with backend.ErrWorkerCreateRefused.
func workerCreateError(err error) error {
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		return err
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		code := status.Status().Code
		if code >= 400 && code < 500 && code != 408 && code != 429 {
			return fmt.Errorf("%w: %w", backend.ErrWorkerCreateRefused, err)
		}
	}
	return err
}

// ResolveWorker looks for the resources a Create of c with an unknown result
// may have made: rank c's Pod and, for rank 0 of a WorkerTCP group, the
// Service, each at its attempt-specific name and matching that attempt's
// expected object exactly. The attempt ID is random per attempt and each
// member has one Create, so only that Create can have made them. A found
// resource is returned for cleanup only. It returns errWorkerNotFound when
// it finds nothing; that is not absence.
func (rt *workerRuntime) ResolveWorker(ctx context.Context, c backend.WorkerCreate) (out backend.WorkerCreated, err error) {
	if err := backend.ValidateWorkerCreated(c, out, true); err != nil {
		return out, err
	}
	m, err := rt.plan(c.Group, c.Rank)
	if err != nil {
		return out, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(c.Group, c.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	if c.Group.Spec.Mode == backend.WorkerTCP && c.Rank == 0 {
		got, err := rt.getService(ctx, workerServiceName(m.plan.AttemptID))
		if errors.Is(err, backend.ErrWorkerUnsettled) {
			if _, matchErr := rt.serviceMatches(got, m.plan, got.UID); matchErr != nil {
				return out, &workerResolvedError{object: got, cause: errors.Join(err, matchErr)}
			}
			out.Service = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
		}
		switch {
		case errors.Is(err, backend.ErrWorkerUnsettled):
			return out, err
		case apierrors.IsNotFound(err):
		case err != nil:
			return out, err
		default:
			if _, err := rt.serviceMatches(got, m.plan, got.UID); err != nil {
				return out, err
			}
			out.Service = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
		}
	}
	got, err := rt.getPod(ctx, m.want.Name)
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		if _, matchErr := workerBarrierMatches(got, m.want, got.UID, ""); matchErr != nil {
			return out, &workerResolvedError{object: got, cause: errors.Join(err, matchErr)}
		}
		out.Pod = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
	}
	switch {
	case errors.Is(err, backend.ErrWorkerUnsettled):
		return out, err
	case apierrors.IsNotFound(err):
	case err != nil:
		return out, err
	default:
		if _, err := workerBarrierMatches(got, m.want, got.UID, ""); err != nil {
			return out, err
		}
		out.Pod = backend.WorkerResource{Name: got.Name, UID: string(got.UID)}
	}
	if out == (backend.WorkerCreated{}) {
		return out, errWorkerNotFound
	}
	return out, backend.ValidateWorkerCreated(c, out, true)
}

// workerResolvedError retains a decoded foreign object with the original HTTP
// owner and causes, without turning its UID into an authoritative cleanup ref.
type workerResolvedError struct {
	object runtime.Object
	cause  error
}

func (e *workerResolvedError) Error() string { return "kubernetes worker resolve response differs" }
func (e *workerResolvedError) Unwrap() error { return e.cause }

// ReleaseWorker removes the skygo scheduling gate from the exact original
// Pod by an update carrying its UID and the observed resourceVersion. In a
// WorkerTCP group the group's original Service must be present first. A Pod
// already without the gate is acknowledged without a write.
func (rt *workerRuntime) ReleaseWorker(ctx context.Context, ref backend.WorkerReference) (out backend.WorkerReleased, err error) {
	out = backend.WorkerReleased{Pod: ref.Pod}
	if err := backend.ValidateWorkerRelease(ref); err != nil {
		return out, err
	}
	m, err := rt.member(ref, false)
	if err != nil {
		return out, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	if ref.Group.Spec.Mode == backend.WorkerTCP {
		if err := rt.serviceLive(ctx, m); err != nil {
			return out, err
		}
	}
	got, f, err := rt.read(ctx, m)
	if err != nil {
		return out, err
	}
	if f.stopping || f.started {
		return out, errors.New("kubernetes worker is stopping or started")
	}
	if !f.gated {
		out.Acknowledged, out.Revision = true, got.ResourceVersion
		return out, nil
	}
	u := got.DeepCopy()
	u.Spec.SchedulingGates = nil
	res, err := rt.updatePod(ctx, u)
	if err != nil {
		return out, err
	}
	facts, matchErr := workerBarrierMatches(res, m.want, types.UID(ref.Pod.UID), m.token)
	if matchErr != nil {
		return out, matchErr
	}
	if facts.gated || facts.started || facts.stopping || res.ResourceVersion == "" {
		return out, errors.New("worker release response does not confirm gate removal")
	}
	out.Acknowledged, out.Revision = true, res.ResourceVersion
	return out, nil
}

// StartWorker delivers token, which must be ref's WorkerStartToken, to the
// exact original Pod by an update carrying its UID and the observed
// resourceVersion. The Pod must be ungated, bound, prepared and without
// stop evidence, and in a WorkerTCP group the group's original Service must
// be live. A Pod that already holds the token is acknowledged without
// a write, so a repeat delivers only the same authorization.
func (rt *workerRuntime) StartWorker(ctx context.Context, ref backend.WorkerReference, token [32]byte) (out backend.WorkerStarted, err error) {
	out = backend.WorkerStarted{Pod: ref.Pod}
	m, err := rt.member(ref, false)
	if err != nil {
		return out, err
	}
	if m.token == "" || m.token != hex.EncodeToString(token[:]) {
		return out, errors.New("invalid worker start token")
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	if ref.Group.Spec.Mode == backend.WorkerTCP {
		if err := rt.serviceLive(ctx, m); err != nil {
			return out, err
		}
	}
	got, f, err := rt.read(ctx, m)
	if err != nil {
		return out, err
	}
	if f.started {
		out.Acknowledged, out.Revision = true, got.ResourceVersion
		return out, nil
	}
	if stop, _ := workerStop(got); f.gated || f.stopping || stop != backend.WorkerStopUnproven || !workerPrepared(got) {
		return out, errWorkerNotPrepared
	}
	u := got.DeepCopy()
	u.Annotations[workerStartAnnotation] = m.token
	res, err := rt.updatePod(ctx, u)
	if err != nil {
		return out, err
	}
	facts, matchErr := workerBarrierMatches(res, m.want, types.UID(ref.Pod.UID), m.token)
	if matchErr != nil {
		return out, matchErr
	}
	if facts.gated || !facts.started || facts.stopping || res.ResourceVersion == "" || !workerPrepared(res) {
		return out, errors.New("worker start response does not confirm the original token")
	}
	out.Acknowledged, out.Revision = true, res.ResourceVersion
	return out, nil
}

// ObserveBarrier reports the barrier facts and stop evidence of the exact
// original member, and rank 0's rendezvous in a WorkerTCP group. A vanished
// Pod or Service is an error wrapping backend.ErrWorkerVanished, never
// evidence.
func (rt *workerRuntime) ObserveBarrier(ctx context.Context, ref backend.WorkerReference) (out backend.WorkerBarrier, err error) {
	m, err := rt.member(ref, false)
	if err != nil {
		return backend.WorkerBarrier{}, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	got, f, err := rt.read(ctx, m)
	if err != nil {
		return backend.WorkerBarrier{}, err
	}
	stop, exit := workerStop(got)
	o := backend.WorkerBarrier{
		Pod: ref.Pod, Revision: got.ResourceVersion, Gated: f.gated, Node: got.Spec.NodeName,
		Prepared: stop == backend.WorkerStopUnproven && !f.stopping && workerPrepared(got),
		Started:  f.started, Deleting: got.DeletionTimestamp != nil, Stop: stop, Exit: exit,
	}
	if m.owner() {
		if o.Rendezvous, err = rt.rendezvous(ctx, m, got); err != nil {
			return backend.WorkerBarrier{}, err
		}
	}
	return o, nil
}

// ObserveWorker reports the exact original member as a portable status. A
// vanished Pod is Unknown, never stopped. Only a native task exit is StoppedWithExitCode;
// a Pod deleted before binding is Canceled without an exit, and a member
// whose helper waits for its token is Starting.
func (rt *workerRuntime) ObserveWorker(ctx context.Context, ref backend.WorkerReference) (out backend.WorkerObservation, err error) {
	m, err := rt.member(ref, false)
	if err != nil {
		return backend.WorkerObservation{}, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	got, f, err := rt.read(ctx, m)
	if errors.Is(err, backend.ErrWorkerUnsettled) {
		return out, err
	}
	if errors.Is(err, backend.ErrWorkerVanished) {
		return backend.WorkerObservation{Pod: ref.Pod,
			Status: backend.Status{State: backend.Unknown, Reason: "kubernetes worker vanished"}}, nil
	}
	if err != nil {
		return backend.WorkerObservation{}, err
	}
	o := backend.WorkerObservation{Pod: ref.Pod, Gated: f.gated, Node: got.Spec.NodeName}
	status := func(s backend.State, reason string) backend.Status {
		return backend.Status{State: s, Reason: reason, Instance: ref.Pod.UID}
	}
	switch stop, exit := workerStop(got); {
	case exit != nil:
		code := exit.ExitCode
		s := status(backend.Failed, "kubernetes worker task exited")
		if code == 0 {
			s.State = backend.Succeeded
		}
		s.ExitCode, s.Termination = &code, backend.StoppedWithExitCode
		o.Status, o.Exit = s, exit
	case stop == backend.WorkerNeverBound:
		o.Status = status(backend.Canceled, "kubernetes worker pod deleted before binding")
	case stop == backend.WorkerTerminated:
		o.Status = status(backend.Failed, "kubernetes worker stopped before its task ran")
	case f.gated:
		o.Status = status(backend.Queued, "kubernetes worker gated")
	case got.Spec.NodeName == "":
		o.Status = status(backend.Starting, "kubernetes worker unscheduled")
	case !f.started:
		o.Status = status(backend.Starting, "kubernetes worker waiting for start")
	default:
		if s := workerTaskStatus(got); s != nil && s.State.Running != nil && classifyState(s) == classifyRunning {
			o.Status = status(backend.Running, "")
			o.Start = &backend.ContainerStart{ContainerID: s.ContainerID, StartedAt: s.State.Running.StartedAt.Time.UTC()}
		} else {
			o.Status = status(backend.Starting, "kubernetes worker container pending")
		}
	}
	return o, nil
}

// CancelWorker requests termination of the exact original member. An
// unbound Pod is deleted with UID and resourceVersion preconditions, after
// which it can never be bound. A bound Pod gets workerStopDeadline by an
// update carrying its UID and resourceVersion, which the kubelet enforces.
// A Pod already being deleted, already stopping or terminal is acknowledged
// without a write. The acknowledgement is not termination.
func (rt *workerRuntime) CancelWorker(ctx context.Context, ref backend.WorkerReference) (out backend.WorkerCanceled, err error) {
	out = backend.WorkerCanceled{Pod: ref.Pod}
	m, err := rt.member(ref, false)
	if err != nil {
		return out, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), false, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	got, f, err := rt.read(ctx, m)
	if err != nil {
		return out, err
	}
	if stop, _ := workerStop(got); f.stopping || stop != backend.WorkerStopUnproven {
		out.Acknowledged = true
		return out, nil
	}
	if got.Spec.NodeName == "" {
		uid, rv := got.UID, got.ResourceVersion
		_, err = rt.deleteWorkerResource(ctx, "pods", ref.Pod, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
	} else {
		u := got.DeepCopy()
		deadline := workerStopDeadline
		u.Spec.ActiveDeadlineSeconds = &deadline
		_, err = rt.updatePod(ctx, u)
	}
	if err != nil {
		return out, err
	}
	out.Acknowledged = true
	return out, nil
}

// RemoveWorker retires the member's recorded resources: the Pod, after
// positive stop evidence in its own read, and then rank 0's Service in a
// WorkerTCP group. Each is deleted with UID and resourceVersion
// preconditions and then has the skygo finalizer removed by an update fenced
// the same way; it is retired only when a later consistent read finds no
// object with its UID after that update. A resource that vanished before is
// an error wrapping backend.ErrWorkerVanished, never a retirement.
func (rt *workerRuntime) RemoveWorker(ctx context.Context, ref backend.WorkerReference) (backend.WorkerRemoval, error) {
	return rt.remove(ctx, ref, false)
}

// RecoverWorkerRemoval is RemoveWorker under the caller's attestation r.
// Only when backend.ValidateWorkerRecovery accepts r is a resource that a
// consistent read does not find reported retired; otherwise its absence is
// an error wrapping backend.ErrWorkerVanished.
func (rt *workerRuntime) RecoverWorkerRemoval(ctx context.Context, r backend.WorkerRecovery) (backend.WorkerRemoval, error) {
	if err := backend.ValidateWorkerRecovery(r); err != nil {
		return backend.WorkerRemoval{}, err
	}
	if r.Evidence == backend.WorkerNeverReleased && (r.Ref.Pod != (backend.WorkerResource{}) || r.Ref.Group.Spec.Mode != backend.WorkerTCP || r.Ref.Rank != 0 || r.Ref.Service == (backend.WorkerResource{})) {
		return backend.WorkerRemoval{}, errors.New("worker no-pod recovery requires a trusted service-only reference")
	}
	return rt.remove(ctx, r.Ref, true)
}

// remove retires ref's resources. attested reports a valid WorkerRecovery.
func (rt *workerRuntime) remove(ctx context.Context, ref backend.WorkerReference, attested bool) (out backend.WorkerRemoval, err error) {
	out = backend.WorkerRemoval{Pod: ref.Pod}
	m, err := rt.member(ref, true)
	if err != nil {
		return out, err
	}
	ctx, call, err := rt.http.begin(ctx, workerCallKey(ref.Group, ref.Rank), true, false)
	if err != nil {
		return out, err
	}
	defer func() { err = call.finish(err) }()
	if m.owner() {
		out.Service = ref.Service
	}
	if ref.Pod != (backend.WorkerResource{}) {
		if out.PodRemoved, out.PodDispatched, err = rt.removePod(ctx, m, attested); err != nil {
			return out, err
		}
	}
	if out.Service != (backend.WorkerResource{}) {
		if out.ServiceRemoved, out.ServiceDispatched, err = rt.removeService(ctx, m, attested); err != nil {
			return out, err
		}
	}
	return out, nil
}

// removePod retires m's Pod; see RemoveWorker.
func (rt *workerRuntime) removePod(ctx context.Context, m workerMember, attested bool) (removed, dispatched bool, err error) {
	uid := types.UID(m.ref.Pod.UID)
	gone := func(got *corev1.Pod, err error) bool {
		return apierrors.IsNotFound(err) || err == nil && got.UID != uid
	}
	got, err := rt.getPod(ctx, m.ref.Pod.Name)
	if gone(got, err) && !errors.Is(err, backend.ErrWorkerUnsettled) {
		if attested {
			return true, false, nil
		}
		return false, false, fmt.Errorf("%w: kubernetes worker pod", backend.ErrWorkerVanished)
	}
	if err != nil {
		return false, false, err
	}
	f, err := workerBarrierMatches(got, m.want, uid, m.token)
	if err != nil {
		return false, false, err
	}
	if stop, _ := workerStop(got); stop == backend.WorkerStopUnproven {
		return false, false, errWorkerStopUnproven
	}
	if got.DeletionTimestamp == nil {
		rv := got.ResourceVersion
		dispatched, err = rt.deleteWorkerResource(ctx, "pods", m.ref.Pod, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
		if err != nil {
			return false, dispatched, err
		}
		got, err = rt.getPod(ctx, m.ref.Pod.Name)
		if err != nil {
			return false, dispatched, workerVanished(err)
		}
		f, err = workerBarrierMatches(got, m.want, uid, m.token)
		if err != nil {
			return false, dispatched, err
		}
		if got.DeletionTimestamp == nil {
			return false, dispatched, errWorkerPodChanged
		}
	}
	if !f.finalized {
		u := got.DeepCopy()
		u.Finalizers = nil
		if _, err = rt.updatePod(ctx, u); err != nil {
			return false, dispatched, err
		}
	}
	got, err = rt.getPod(ctx, m.ref.Pod.Name)
	if gone(got, err) && !errors.Is(err, backend.ErrWorkerUnsettled) {
		return true, dispatched, nil
	}
	if err != nil {
		return false, dispatched, err
	}
	return false, dispatched, errWorkerRemoving
}

// workerCallKey keeps all original calls of one rank under the same bounded
// owner, including Create before a Pod UID exists and service-only cleanup.
func workerCallKey(g backend.WorkerGroup, rank uint8) string {
	return "worker/" + g.Execution.ID + "/" + g.AttemptID + "/" + strconv.Itoa(int(rank))
}

func (rt *workerRuntime) getPod(ctx context.Context, name string) (*corev1.Pod, error) {
	out := new(corev1.Pod)
	err := rt.http.client.CoreV1().RESTClient().Get().Namespace(rt.b.namespace).Resource("pods").Name(name).MaxRetries(0).Do(rt.http.request(ctx)).Into(out)
	return out, errors.Join(err, rt.http.check(ctx))
}
func (rt *workerRuntime) getService(ctx context.Context, name string) (*corev1.Service, error) {
	out := new(corev1.Service)
	err := rt.http.client.CoreV1().RESTClient().Get().Namespace(rt.b.namespace).Resource("services").Name(name).MaxRetries(0).Do(rt.http.request(ctx)).Into(out)
	return out, errors.Join(err, rt.http.check(ctx))
}
func (rt *workerRuntime) updatePod(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	out := new(corev1.Pod)
	err := rt.http.client.CoreV1().RESTClient().Put().Namespace(rt.b.namespace).Resource("pods").Name(pod.Name).VersionedParams(&metav1.UpdateOptions{}, scheme.ParameterCodec).Body(pod).MaxRetries(0).Do(rt.http.request(ctx)).Into(out)
	return out, errors.Join(err, rt.http.check(ctx))
}
func (rt *workerRuntime) updateService(ctx context.Context, service *corev1.Service) (*corev1.Service, error) {
	out := new(corev1.Service)
	err := rt.http.client.CoreV1().RESTClient().Put().Namespace(rt.b.namespace).Resource("services").Name(service.Name).VersionedParams(&metav1.UpdateOptions{}, scheme.ParameterCodec).Body(service).MaxRetries(0).Do(rt.http.request(ctx)).Into(out)
	return out, errors.Join(err, rt.http.check(ctx))
}

// deleteWorkerResource reports only a positively acknowledged original DELETE.
// Later confirmation may fail, but method entry, trace callbacks, non-2xx and
// an unclosed original body never attest dispatch. The saved UID is mandatory.
func (rt *workerRuntime) deleteWorkerResource(ctx context.Context, resource string, ref backend.WorkerResource, options metav1.DeleteOptions) (bool, error) {
	if (resource != "pods" && resource != "services") || ref.Name == "" || ref.UID == "" || options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != ref.UID || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion == "" {
		return false, errors.New("worker delete requires its exact original uid and revision")
	}
	var code int
	obj, err := rt.http.client.CoreV1().RESTClient().Delete().Namespace(rt.b.namespace).Resource(resource).Name(ref.Name).Body(&options).MaxRetries(0).Do(rt.http.request(ctx)).StatusCode(&code).Get()
	if err = errors.Join(err, rt.http.check(ctx)); err != nil {
		return false, err
	}
	if code < 200 || code > 299 {
		return false, errors.New("worker delete lacks a successful response")
	}
	switch v := obj.(type) {
	case *metav1.Status:
		if v.Status != metav1.StatusSuccess || v.Code != 0 && int(v.Code) != code {
			return false, errors.New("worker delete returned an invalid acknowledgment")
		}
		if v.Details != nil && (v.Details.Name != ref.Name || string(v.Details.UID) != ref.UID || v.Details.Kind != "" && v.Details.Kind != resource || v.Details.Group != "") {
			return false, errors.New("worker delete acknowledgment identity differs")
		}
	case *corev1.Pod:
		if resource != "pods" || v.Name != ref.Name || string(v.UID) != ref.UID || v.Namespace != rt.b.namespace {
			return false, errors.New("worker delete acknowledgment identity differs")
		}
	case *corev1.Service:
		if resource != "services" || v.Name != ref.Name || string(v.UID) != ref.UID || v.Namespace != rt.b.namespace {
			return false, errors.New("worker delete acknowledgment identity differs")
		}
	default:
		return false, errors.New("worker delete lacks a typed acknowledgment")
	}
	return true, nil
}

// workerTaskStatus returns the task container's status, if reported.
func workerTaskStatus(p *corev1.Pod) *corev1.ContainerStatus {
	for i := range p.Status.ContainerStatuses {
		if p.Status.ContainerStatuses[i].Name == "task" {
			return &p.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// workerStop returns the positive stop evidence of an original barrier Pod
// and, when the task container ran, its exit. A Pod being deleted with no
// node can never run. A Pod in a terminal phase whose helper and task
// statuses are each a qualified never-restarted terminated or never-created
// state has stopped. Anything else, including a missing status, is
// unproven.
func workerStop(p *corev1.Pod) (backend.WorkerStop, *backend.ContainerExit) {
	if p.DeletionTimestamp != nil && p.Spec.NodeName == "" {
		return backend.WorkerNeverBound, nil
	}
	if p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
		return backend.WorkerStopUnproven, nil
	}
	parts := map[string]classifyPart{}
	if !classifyStatuses(parts, p.Status.InitContainerStatuses, []string{"helper"}) ||
		!classifyStatuses(parts, p.Status.ContainerStatuses, []string{"task"}) {
		return backend.WorkerStopUnproven, nil
	}
	for _, part := range parts {
		if part.kind == classifyRunning {
			return backend.WorkerStopUnproven, nil
		}
	}
	task := parts["task"]
	if task.kind != classifyTerminated {
		return backend.WorkerTerminated, nil
	}
	x := task.status.State.Terminated
	return backend.WorkerTerminated, &backend.ContainerExit{ContainerID: x.ContainerID, ExitCode: x.ExitCode,
		StartedAt: x.StartedAt.Time.UTC(), FinishedAt: x.FinishedAt.Time.UTC()}
}

// workerPrepared reports whether the kubelet reports the prepared check of
// a bound, running original Pod as ready: the helper init container exited
// 0 and the never-restarted task container is running and ready.
func workerPrepared(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil || p.Spec.NodeName == "" || p.Status.Phase != corev1.PodRunning {
		return false
	}
	parts := map[string]classifyPart{}
	if !classifyStatuses(parts, p.Status.InitContainerStatuses, []string{"helper"}) ||
		!classifyStatuses(parts, p.Status.ContainerStatuses, []string{"task"}) {
		return false
	}
	h, t := parts["helper"], parts["task"]
	return h.kind == classifyTerminated && h.status.State.Terminated.ExitCode == 0 &&
		t.kind == classifyRunning && t.status.Ready
}
