package kube

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/environment"
	"github.com/tmc/skygo/internal/environmentcontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

// The environment runtime is the environmentcontrol.Runtime of one
// Kubernetes target: one environment Pod per environment, on the
// principal's borrowed claim, with bounded exec and one declared-port
// gateway. It is not registered as a capability.
//
// Ownership. The runtime never adopts an object by name. A Pod is ours by
// the UID recorded from our own Create response or, before that, by its
// spec annotation, which commits to the environment's nonce.
//
// Writes. Every Pod write is declared in a returned Native, with
// environmentcontrol.ErrWritePending, and sent at most once, by the process
// that declared it, in a later call whose Ref.Native is that declaration.
// Each write makes one network send: client-go's resend of a write is
// disabled. The send runs in a create frame that the runtime keeps, so a
// result that arrives after the call returns is recorded by a later call.
//
// A Create send may land at any later time, including after the Pod it
// would have collided with is deleted. A declared send counts as possibly
// sent until its own response, or a Pod at its name that carries the
// nonce, resolves it. A missing Pod, an empty read, a timeout, a restart or
// elapsed time never does. While a send is unresolved the runtime declares
// no other, Ensure never reports Ready and Delete never reports Deleted. A
// stop or remove names the recorded Pod's UID and resourceVersion, so if it
// lands late it can do only what was declared.
//
// Stop. Neither a missing Pod nor a deleting one proves that its containers
// stopped. Delete first sets the Pod's activeDeadlineSeconds, which the
// kubelet enforces by killing the containers and recording a terminal
// phase, and records that phase as stop evidence before it deletes the Pod.
// An unscheduled Pod is deleted directly: its deletion is immediate and a
// deleting Pod cannot be bound. Without that evidence a removed Pod is
// Unknown, never Deleted. The claim and its volume are never deleted.
//
// Artifacts. Ensure reports the recorded Pod's UID as the Observation's
// Incarnation whenever one consistent read matched that Pod. Materialize
// stages one artifact reference into that Pod, fenced by the helper, as
// environment_artifact.go describes.
//
// Frames. Exec, Forward and Materialize reserve a slot before any read and
// keep it until the frame of the original call settles. Delete cancels the
// environment's frames and refuses new ones in this process; a canceled
// exec reports no exit, and nothing infers that its command stopped. stop
// does the same for every environment and also refuses new create sends and
// cancels those in flight; join then waits for every frame and send to
// return. Neither stops or deletes a Pod: environments outlive the server.
//
// Target. The runtime accepts only environments whose Spec.Target is the
// name of the target it was built from; Plan refuses another as
// unsupported and resolve as an identity mismatch, before any request.
//
// Reads that decide identity, readiness or absence are consistent Gets.

const (
	environmentKind          = "kubernetes-pod"
	environmentNativeVersion = 1
	environmentHashFormat    = "skygo-kubernetes-environment-v1"
	// maxEnvironmentCreates bounds the declared Create sends of one
	// environment.
	maxEnvironmentCreates = 4
	// maxEnvironmentCreateFrames bounds the create frames of one runtime.
	maxEnvironmentCreateFrames = 64
	// environmentCreateTimeout bounds one create frame. A send it ends may
	// still land.
	environmentCreateTimeout = 2 * time.Minute
	// environmentReadTimeout bounds the reads of Exec, which has no caller
	// context.
	environmentReadTimeout = 30 * time.Second
	maxEnvironmentFrames   = 64 // per runtime
	maxFramesPerEnv        = 8
	// environmentStopDeadline is the activeDeadlineSeconds that Delete sets.
	environmentStopDeadline = int64(1)
)

var (
	errEnvironmentDeleting    = errors.New("environment deleting")
	errEnvironmentCreateLimit = errors.New("environment create sends exhausted")
	errEnvironmentUnresolved  = errors.New("environment create send unresolved")
	errEnvironmentNotReady    = errors.New("environment not ready")
	errEnvironmentStopUnknown = errors.New("environment pod removed without stop evidence")
	// errEnvironmentPodChanged reports that the Pod at the environment's
	// name was not the recorded one after an exec. The exec's exit is not
	// attributed to the environment.
	errEnvironmentPodChanged = errors.New("environment pod changed during exec")
)

// Declared write operations.
const (
	environmentWriteCreate = "create"
	environmentWriteStop   = "stop"   // set activeDeadlineSeconds
	environmentWriteRemove = "remove" // delete the stopped Pod
	// environmentWriteRemoveUnbound deletes an unscheduled Pod; its success
	// is stop evidence.
	environmentWriteRemoveUnbound = "remove-unbound"
)

// environmentNative is the runtime's native state. Field order is the
// encoding.
type environmentNative struct {
	Version   int       `json:"version"`
	Profile   string    `json:"profile"`
	Alias     string    `json:"alias"`
	Namespace types.UID `json:"namespace_uid"`
	Claim     types.UID `json:"claim_uid"`
	Volume    string    `json:"volume"`
	// VolumeUID is the bound PersistentVolume's UID, recorded before the
	// first Create send.
	VolumeUID types.UID `json:"volume_uid,omitempty"`
	// Sends counts declared Create sends; Resolved counts those whose
	// outcome is known. At most one is unresolved.
	Sends    uint32                  `json:"sends,omitempty"`
	Resolved uint32                  `json:"resolved,omitempty"`
	Write    *environmentNativeWrite `json:"write,omitempty"`
	Pod      *environmentNativePod   `json:"pod,omitempty"`
	// Closing records that Delete started. Ensure and Exec refuse it.
	Closing bool `json:"closing,omitempty"`
	// Stopped records evidence that the recorded Pod's containers stopped.
	Stopped bool `json:"stopped,omitempty"`
}

// environmentNativeWrite is a declared write. Token is new for each
// declaration, so no other declaration, by this process or another, has
// the same bytes. A stop or remove names the recorded Pod at
// ResourceVersion.
type environmentNativeWrite struct {
	Op              string `json:"op"`
	Token           string `json:"token"`
	ResourceVersion string `json:"resource_version,omitempty"`
}

type environmentNativePod struct {
	UID        types.UID `json:"uid"`
	Generation int64     `json:"generation"`
	// Altered reports a Pod of ours that was stored differently from the
	// one we sent. It is never trusted, only deleted.
	Altered bool `json:"altered,omitempty"`
}

func (n environmentNative) valid() bool {
	switch {
	case n.Version != environmentNativeVersion, !qualificationText(n.Profile, 128),
		len(validation.IsDNS1123Label(n.Alias)) != 0,
		!qualificationText(string(n.Namespace), 128), !qualificationText(string(n.Claim), 128),
		len(validation.IsDNS1123Subdomain(n.Volume)) != 0:
		return false
	case n.VolumeUID != "" && !qualificationText(string(n.VolumeUID), 128),
		n.Sends > 0 && n.VolumeUID == "",
		n.Resolved > n.Sends, n.Sends-n.Resolved > 1, n.Sends > maxEnvironmentCreates:
		return false
	case n.Pod != nil && (n.Sends == 0 || n.unresolved() || !qualificationText(string(n.Pod.UID), 128) || n.Pod.Generation < 0),
		n.Stopped && (n.Pod == nil || !n.Closing):
		return false
	}
	if w := n.Write; w != nil {
		if !environment.ValidID(w.Token) {
			return false
		}
		switch w.Op {
		case environmentWriteCreate:
			return n.unresolved() && n.Pod == nil && w.ResourceVersion == ""
		case environmentWriteStop, environmentWriteRemoveUnbound:
			return n.Closing && n.Pod != nil && !n.Stopped && qualificationText(w.ResourceVersion, 128)
		case environmentWriteRemove:
			return n.Closing && n.Pod != nil && n.Stopped && qualificationText(w.ResourceVersion, 128)
		}
		return false
	}
	return true
}

// unresolved reports whether a declared Create send may still land.
func (n environmentNative) unresolved() bool { return n.Resolved < n.Sends }

func (n environmentNative) encode() []byte {
	raw, _ := json.Marshal(n) // fixed field types; cannot fail
	return raw
}

// decodeEnvironmentNative strictly decodes raw: one value of the known
// version, in its own encoding.
func decodeEnvironmentNative(raw []byte) (environmentNative, error) {
	invalid := fmt.Errorf("%w: invalid kubernetes environment native state", backend.ErrIdentityMismatch)
	if len(raw) > environmentcontrol.MaxNativeBytes {
		return environmentNative{}, invalid
	}
	var n environmentNative
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&n); err != nil {
		return environmentNative{}, invalid
	}
	if _, err := dec.Token(); err != io.EOF {
		return environmentNative{}, invalid
	}
	if string(n.encode()) != string(raw) || !n.valid() {
		return environmentNative{}, invalid
	}
	return n, nil
}

type environmentProfileKey struct{ principal, alias string }

// environmentRuntime is the environment runtime of one target. It reads only
// Backend fields that never change after construction.
type environmentRuntime struct {
	b *Backend
	// target is the name of the one target whose environments this runtime
	// accepts: Plan and resolve refuse a Spec.Target that differs.
	target   string
	profiles map[environmentProfileKey]environmentPodProfile
	port     *envTransport
	pods     environmentPodWriter
	serve    func(http.ResponseWriter, *http.Request, portTarget) *portFrame

	// effects is the parent of every context the runtime's own frames use;
	// stop cancels it. It never deletes or stops a Pod.
	effects    context.Context
	endEffects context.CancelFunc

	mu       sync.Mutex
	locks    map[string]*environmentLock
	declared map[string]string // per environment, the Native that declared a write this process has not sent
	creates  map[string]*environmentCreate
	frames   map[string]*environmentFrames
	stages   map[environmentStageKey]bool // references with an unsettled Materialize
	active   int                          // reserved frame slots
	seq      uint64                       // frame slot keys
	// sending counts create frames whose send has not returned. A frame
	// dropped from creates is still counted until its goroutine returns.
	sending int
	// stopped refuses every new frame and create send; drained is closed
	// once stopped and no frame or send is held.
	stopped bool
	drained chan struct{}
}

type environmentLock struct {
	sem  chan struct{} // capacity 1
	refs int           // guarded by environmentRuntime.mu
}

// environmentCreate is the frame of Create send number send. pod and err
// are set before done is closed.
type environmentCreate struct {
	send uint32
	done chan struct{}
	pod  *corev1.Pod
	err  error
}

// environmentFrames is the frame custody of one environment. held maps each
// reserved slot to its cancel function, nil until attached.
type environmentFrames struct {
	closing bool
	held    map[uint64]func()
}

var (
	_ environmentcontrol.Runtime      = (*environmentRuntime)(nil)
	_ environmentcontrol.Materializer = (*environmentRuntime)(nil)
)

// newEnvironmentRuntime returns the environment runtime of target, a target
// built by fromRESTContext, using config, the REST configuration that target
// was built from, and the operator's private profiles, at most one per
// principal and alias. config must not change afterwards. It makes no
// request.
func newEnvironmentRuntime(target backend.Target, config *rest.Config, profiles []environmentPodProfile) (*environmentRuntime, error) {
	b, ok := target.Backend.(*Backend)
	if !ok || b == nil || b.binding == "" || b.binding != target.Binding {
		return nil, errors.New("kubernetes environment runtime requires a bound kubernetes target")
	}
	if config == nil {
		return nil, errors.New("kubernetes configuration is required")
	}
	c := rest.CopyConfig(config)
	host, err := canonicalServer(c.Host)
	if err != nil {
		return nil, err
	}
	c.Host = host
	if c.CAFile != "" || c.Transport != nil || c.Dial != nil {
		return nil, errors.New("kubernetes environment runtime requires the target's frozen configuration")
	}
	binding, err := targetBinding(TargetConfig{Namespace: b.namespace, Labels: target.Labels, Resources: b.policy}, c, b.data, b.dataQual)
	if err != nil || binding != b.binding {
		return nil, errors.New("kubernetes environment configuration differs from target binding")
	}
	m := make(map[environmentProfileKey]environmentPodProfile, len(profiles))
	for _, p := range profiles {
		if err := p.check(p.principal); err != nil {
			return nil, err
		}
		k := environmentProfileKey{p.principal, p.alias}
		if _, dup := m[k]; dup {
			return nil, errors.New("duplicate kubernetes environment profile")
		}
		p.idleArgv, p.pullSecrets = slices.Clone(p.idleArgv), slices.Clone(p.pullSecrets)
		m[k] = p
	}
	pods, err := newEnvironmentPodClient(c, b.namespace)
	if err != nil {
		return nil, err
	}
	port, err := newEnvTransport(c, b.namespace)
	if err != nil {
		return nil, err
	}
	effects, endEffects := context.WithCancel(context.Background())
	return &environmentRuntime{b: b, target: target.Name, profiles: m, port: port, pods: pods, serve: port.servePort,
		effects: effects, endEffects: endEffects,
		locks: make(map[string]*environmentLock), declared: make(map[string]string),
		creates: make(map[string]*environmentCreate), frames: make(map[string]*environmentFrames),
		stages: make(map[environmentStageKey]bool), drained: make(chan struct{})}, nil
}

// errEnvironmentStopped reports a frame or create send refused because the
// runtime is stopping. Nothing was sent.
var errEnvironmentStopped = errors.New("kubernetes environment runtime stopped")

// stop stops the runtime's effects: it refuses every new frame and create
// send, and cancels every held frame and send in progress and every read
// they make. It returns at once; cancellation is not a join, and a canceled
// send may still land. It never stops or deletes a Pod, claim or volume.
// It is safe to call more than once.
func (rt *environmentRuntime) stop() {
	rt.mu.Lock()
	rt.stopped = true
	var cancels []func()
	for _, f := range rt.frames {
		for _, c := range f.held {
			if c != nil {
				cancels = append(cancels, c)
			}
		}
	}
	rt.checkDrainedLocked()
	rt.mu.Unlock()
	rt.endEffects()
	for _, c := range cancels {
		c()
	}
}

// join calls stop and waits until every frame and create send this runtime
// started has returned, or ctx ends. It returns nil only if none is held.
func (rt *environmentRuntime) join(ctx context.Context) error {
	rt.stop()
	select {
	case <-rt.drained:
		return nil
	case <-ctx.Done():
	}
	rt.mu.Lock()
	frames, sends := rt.active, rt.sending
	rt.mu.Unlock()
	if frames == 0 && sends == 0 {
		return nil
	}
	return errors.Join(fmt.Errorf("%d kubernetes environment frames and %d create sends unsettled", frames, sends), ctx.Err())
}

// checkDrainedLocked closes drained once the runtime is stopped and holds
// nothing. rt.mu is held.
func (rt *environmentRuntime) checkDrainedLocked() {
	if !rt.stopped || rt.active != 0 || rt.sending != 0 {
		return
	}
	select {
	case <-rt.drained:
	default:
		close(rt.drained)
	}
}

// environmentPodWriter makes the runtime's Pod writes. Each call makes at
// most one request.
type environmentPodWriter interface {
	create(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error)
	update(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error)
	remove(ctx context.Context, name string, opts metav1.DeleteOptions) error
}

// environmentPodClient writes Pods in one namespace as the typed client
// does, with the same options and bodies, but with one request per call.
// client-go otherwise resends a write answered with 429, or with a 5xx
// status and Retry-After, and such a write may have been applied.
type environmentPodClient struct {
	client    rest.Interface
	namespace string
}

func newEnvironmentPodClient(config *rest.Config, namespace string) (environmentPodClient, error) {
	core, err := corev1client.NewForConfig(config)
	if err != nil {
		return environmentPodClient{}, fmt.Errorf("kubernetes environment client: %w", err)
	}
	return environmentPodClient{client: core.RESTClient(), namespace: namespace}, nil
}

func (c environmentPodClient) create(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	got := &corev1.Pod{}
	err := c.client.Post().Namespace(c.namespace).Resource("pods").
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(pod).
		MaxRetries(0).Do(ctx).Into(got)
	return got, err
}

func (c environmentPodClient) update(ctx context.Context, pod *corev1.Pod) (*corev1.Pod, error) {
	got := &corev1.Pod{}
	err := c.client.Put().Namespace(c.namespace).Resource("pods").Name(pod.Name).
		VersionedParams(&metav1.UpdateOptions{}, scheme.ParameterCodec).Body(pod).
		MaxRetries(0).Do(ctx).Into(got)
	return got, err
}

func (c environmentPodClient) remove(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().Namespace(c.namespace).Resource("pods").Name(name).
		Body(&opts).MaxRetries(0).Do(ctx).Error()
}

// environmentTask returns the Pod leaf's task for s. The Pod requires a
// claim and explicit limits; none is implied.
func environmentTask(s environment.Spec) (backend.Task, error) {
	switch {
	case s.StorageAlias == "":
		return backend.Task{}, errors.New("kubernetes environment requires a storage alias")
	case s.CPULimitMillis == 0 || s.MemoryLimitBytes == 0:
		return backend.Task{}, errors.New("kubernetes environment requires cpu and memory limits")
	}
	return backend.Task{
		Image:            s.Image,
		CPUMillis:        s.CPUMillis,
		MemoryBytes:      s.MemoryBytes,
		CPULimitMillis:   s.CPULimitMillis,
		MemoryLimitBytes: s.MemoryLimitBytes,
		Accelerator:      s.Accelerator,
		AcceleratorCount: s.AcceleratorCount,
		NodeSelector:     maps.Clone(s.NodeSelector),
	}, nil
}

// environmentSpecHash returns the Pod's spec annotation: a digest of the
// accepted environment, its nonce, the target binding and the profile. Only
// a holder of the nonce can produce it for this environment.
func environmentSpecHash(ref environmentcontrol.Ref, p environmentPodProfile) [32]byte {
	v := struct {
		Format         string                            `json:"format"`
		Installation   string                            `json:"installation"`
		ID             string                            `json:"id"`
		Principal      string                            `json:"principal"`
		Spec           string                            `json:"spec_sha256"`
		Binding        string                            `json:"binding"`
		Nonce          string                            `json:"nonce"`
		Profile        string                            `json:"profile"`
		Namespace      types.UID                         `json:"namespace_uid"`
		Alias          string                            `json:"alias"`
		Claim          string                            `json:"claim"`
		ClaimUID       types.UID                         `json:"claim_uid"`
		Volume         string                            `json:"volume"`
		AccessMode     corev1.PersistentVolumeAccessMode `json:"access_mode"`
		ServiceAccount types.UID                         `json:"service_account_uid"`
		PullSecrets    []string                          `json:"pull_secrets"`
		Image          string                            `json:"image"`
		IdleArgv       []string                          `json:"idle_argv"`
	}{
		environmentHashFormat, ref.Identity.Installation, ref.Identity.ID, ref.Principal,
		hex.EncodeToString(ref.SpecDigest[:]), ref.Binding, ref.Nonce, p.revision, p.namespaceUID, p.alias,
		p.pvcName, p.pvcUID, p.volumeName, p.accessMode, p.serviceAccountUID, p.pullSecrets, p.image, p.idleArgv,
	}
	data, _ := json.Marshal(v) // fixed field types; cannot fail
	return sha256.Sum256(data)
}

// Kind returns the environments' checkpoint kind.
func (rt *environmentRuntime) Kind() string { return environmentKind }

// Plan checks spec for principal against the profile that its storage alias
// names and returns the target binding and the initial native state, which
// holds the profile's identities. It makes no request.
func (rt *environmentRuntime) Plan(principal string, spec environment.Spec, digest [32]byte) (string, []byte, error) {
	if spec.Target != rt.target {
		return "", nil, fmt.Errorf("%w: kubernetes environment target differs", environmentcontrol.ErrUnsupported)
	}
	raw, err := environment.Canonical(spec)
	if err != nil {
		return "", nil, err
	}
	if sha256.Sum256(raw) != digest {
		return "", nil, errors.New("environment spec digest differs")
	}
	t, err := environmentTask(spec)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", environmentcontrol.ErrUnsupported, err)
	}
	arts, err := environmentArtifacts(spec.Artifacts)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", environmentcontrol.ErrUnsupported, err)
	}
	p, ok := rt.profiles[environmentProfileKey{principal, spec.StorageAlias}]
	if !ok {
		return "", nil, fmt.Errorf("%w: no kubernetes environment profile for storage alias", environmentcontrol.ErrUnsupported)
	}
	// Run every Pod check with a placeholder identity.
	probe := environmentPodInput{id: strings.Repeat("0", 32), principal: principal, generation: environment.FirstGeneration,
		specHash: sha256.Sum256(nil), task: t, artifacts: arts}
	if _, err := rt.b.environmentPod(probe, p); err != nil {
		return "", nil, fmt.Errorf("%w: %w", environmentcontrol.ErrUnsupported, err)
	}
	// A profile's volume UID is the accepted one from the start.
	n := environmentNative{Version: environmentNativeVersion, Profile: p.revision, Alias: p.alias,
		Namespace: p.namespaceUID, Claim: p.pvcUID, Volume: p.volumeName, VolumeUID: p.volumeUID}
	return rt.b.binding, n.encode(), nil
}

// environmentResolved is a checked Ref.
type environmentResolved struct {
	id      string
	name    string
	in      environmentPodInput
	profile environmentPodProfile
	native  environmentNative
	want    *corev1.Pod
}

// resolve checks ref against the target and the profile recorded at
// acceptance, and builds the environment's exact Pod.
func (rt *environmentRuntime) resolve(ref environmentcontrol.Ref) (environmentResolved, error) {
	var r environmentResolved
	if ref.Spec.Target != rt.target {
		return r, fmt.Errorf("%w: kubernetes environment target differs", backend.ErrIdentityMismatch)
	}
	if ref.Binding != rt.b.binding {
		return r, fmt.Errorf("%w: kubernetes environment target binding differs", backend.ErrIdentityMismatch)
	}
	if !ref.Identity.Valid() || !environment.ValidID(ref.Nonce) {
		return r, errors.New("invalid environment identity")
	}
	raw, err := environment.Canonical(ref.Spec)
	if err != nil {
		return r, err
	}
	if sha256.Sum256(raw) != ref.SpecDigest {
		return r, fmt.Errorf("%w: environment spec digest differs", backend.ErrIdentityMismatch)
	}
	t, err := environmentTask(ref.Spec)
	if err != nil {
		return r, err
	}
	arts, err := environmentArtifacts(ref.Spec.Artifacts)
	if err != nil {
		return r, err
	}
	n, err := decodeEnvironmentNative(ref.Native)
	if err != nil {
		return r, err
	}
	p, ok := rt.profiles[environmentProfileKey{ref.Principal, ref.Spec.StorageAlias}]
	if !ok || p.revision != n.Profile || p.alias != n.Alias || p.namespaceUID != n.Namespace ||
		p.pvcUID != n.Claim || p.volumeName != n.Volume || p.volumeUID != "" && p.volumeUID != n.VolumeUID {
		return r, fmt.Errorf("%w: kubernetes environment profile differs from acceptance", backend.ErrIdentityMismatch)
	}
	in := environmentPodInput{id: ref.Identity.ID, principal: ref.Principal, generation: ref.Identity.Generation,
		specHash: environmentSpecHash(ref, p), task: t, artifacts: arts}
	want, err := rt.b.environmentPod(in, p)
	if err != nil {
		return r, err
	}
	return environmentResolved{id: ref.Identity.ID, name: want.Name, in: in, profile: p, native: n, want: want}, nil
}

// acquire serializes Ensure and Delete of one environment. rt.mu is never
// held while waiting.
func (rt *environmentRuntime) acquire(ctx context.Context, id string) (func(), error) {
	rt.mu.Lock()
	l := rt.locks[id]
	if l == nil {
		l = &environmentLock{sem: make(chan struct{}, 1)}
		rt.locks[id] = l
	}
	l.refs++
	rt.mu.Unlock()
	unref := func() {
		rt.mu.Lock()
		if l.refs--; l.refs == 0 {
			delete(rt.locks, id)
		}
		rt.mu.Unlock()
	}
	select {
	case l.sem <- struct{}{}:
		return func() { <-l.sem; unref() }, nil
	case <-ctx.Done():
		unref()
		return nil, ctx.Err()
	}
}

// takeDeclared reports whether native is a declaration of environment id
// that this process made and has not sent, and forgets it either way: the
// declared write may be sent only by this call.
func (rt *environmentRuntime) takeDeclared(id string, native []byte) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	d, ok := rt.declared[id]
	delete(rt.declared, id)
	return ok && d == string(native)
}

// pending declares a write of op in n and returns the Observation that
// records it. Only this process may send the write, once the Observation
// is committed.
func (rt *environmentRuntime) pending(id string, phase environment.Phase, n environmentNative, op, rv string) (environmentcontrol.Observation, error) {
	var token [16]byte
	rand.Read(token[:]) // never returns an error
	n.Write = &environmentNativeWrite{Op: op, Token: hex.EncodeToString(token[:]), ResourceVersion: rv}
	native := n.encode()
	rt.mu.Lock()
	rt.declared[id] = string(native)
	rt.mu.Unlock()
	return environmentcontrol.Observation{Phase: phase, Native: native}, environmentcontrol.ErrWritePending
}

// environmentFound returns obj, or nil for NotFound.
func environmentFound[T any](obj *T, err error) (*T, error) {
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// environmentRead is one consistent read of an environment's objects. A nil
// field is NotFound.
type environmentRead struct {
	namespace *corev1.Namespace
	claim     *corev1.PersistentVolumeClaim
	volume    *corev1.PersistentVolume
	pod       *corev1.Pod
}

func (rt *environmentRuntime) read(ctx context.Context, r environmentResolved) (environmentRead, error) {
	var o environmentRead
	c := rt.b.client.CoreV1()
	var err error
	if o.namespace, err = environmentFound(c.Namespaces().Get(ctx, rt.b.namespace, metav1.GetOptions{})); err != nil {
		return o, err
	}
	if o.claim, err = environmentFound(c.PersistentVolumeClaims(rt.b.namespace).Get(ctx, r.profile.pvcName, metav1.GetOptions{})); err != nil {
		return o, err
	}
	if o.volume, err = environmentFound(c.PersistentVolumes().Get(ctx, r.profile.volumeName, metav1.GetOptions{})); err != nil {
		return o, err
	}
	if o.pod, err = rt.getPod(ctx, r); err != nil {
		return o, err
	}
	return o, nil
}

// getPod returns the Pod at r's name, or nil for NotFound.
func (rt *environmentRuntime) getPod(ctx context.Context, r environmentResolved) (*corev1.Pod, error) {
	return environmentFound(rt.b.client.CoreV1().Pods(rt.b.namespace).Get(ctx, r.name, metav1.GetOptions{}))
}

// environmentVolumeMatches verifies the claim's PersistentVolume: the
// profile's name, bound back to the profile's claim by namespace, name and
// UID, and, once recorded, the recorded UID. It classifies as
// matchEnvironmentPod does.
func environmentVolumeMatches(p environmentPodProfile, namespace string, recorded types.UID, pv *corev1.PersistentVolume) error {
	if pv == nil {
		return fmt.Errorf("kubernetes environment volume: %w", errEnvironmentObjectMissing)
	}
	ref := pv.Spec.ClaimRef
	if pv.Name != p.volumeName || ref == nil || ref.Namespace != namespace || ref.Name != p.pvcName ||
		ref.UID != p.pvcUID || !qualificationText(string(pv.UID), 128) || recorded != "" && pv.UID != recorded {
		return fmt.Errorf("%w: kubernetes environment volume differs", backend.ErrIdentityMismatch)
	}
	if pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound {
		return fmt.Errorf("kubernetes environment volume: %w", errEnvironmentObjectUnavailable)
	}
	return nil
}

// environmentOutcome reports whether err is an identity or availability
// outcome rather than a failed request.
func environmentOutcome(err error) bool {
	return errors.Is(err, backend.ErrIdentityMismatch) ||
		errors.Is(err, errEnvironmentObjectMissing) || errors.Is(err, errEnvironmentObjectUnavailable)
}

// environmentNonceMatch reports whether got carries want's labels, binding
// and spec annotation, which commits to our nonce, and a valid UID.
func environmentNonceMatch(want, got *corev1.Pod) bool {
	a := got.Annotations
	return got.Name == want.Name && maps.Equal(got.Labels, want.Labels) && qualificationText(string(got.UID), 128) &&
		a[bindingAnnotation] == want.Annotations[bindingAnnotation] &&
		a[environmentSpecAnnotation] == want.Annotations[environmentSpecAnnotation]
}

// environmentExact reports whether got is exactly the Pod want describes.
func environmentExact(want, got *corev1.Pod) bool {
	return profileMetaMatches(want.ObjectMeta, got.ObjectMeta) && fixedPodSpecMatches(&want.Spec, &got.Spec)
}

// environmentPodStopped reports kubelet evidence that pod's containers
// stopped: a terminal phase and no running container.
func environmentPodStopped(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodFailed && pod.Status.Phase != corev1.PodSucceeded {
		return false
	}
	for _, s := range pod.Status.ContainerStatuses {
		if s.State.Running != nil {
			return false
		}
	}
	return true
}

// environmentPodReady reports a running Pod whose workspace container is
// running and ready.
func environmentPodReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, s := range pod.Status.ContainerStatuses {
		if s.Name == environmentContainer {
			return s.Ready && s.State.Running != nil
		}
	}
	return false
}

// observe reports the state of n's recorded Pod from one consistent read.
// An identity or availability outcome is Unknown, with its error. The
// returned incarnation is the recorded Pod's UID if that read matched it,
// and empty otherwise.
func (rt *environmentRuntime) observe(ctx context.Context, r environmentResolved, n environmentNative) (environment.Phase, string, error) {
	if n.Pod.Altered {
		return environment.Unknown, "", fmt.Errorf("%w: kubernetes environment pod stored altered", backend.ErrIdentityMismatch)
	}
	o, err := rt.read(ctx, r)
	if err != nil {
		return 0, "", err
	}
	identity := environmentPodIdentity{podUID: n.Pod.UID, podGeneration: n.Pod.Generation}
	errs := []error{
		rt.b.matchEnvironmentPod(r.in, r.profile, identity, o.pod, o.namespace, o.claim),
		environmentVolumeMatches(r.profile, rt.b.namespace, n.VolumeUID, o.volume),
	}
	for _, err := range errs {
		if err != nil && !environmentOutcome(err) {
			return 0, "", err
		}
	}
	if err := errors.Join(errs...); err != nil {
		return environment.Unknown, "", err
	}
	incarnation := environmentIncarnation(string(o.pod.UID))
	switch {
	case environmentPodStopped(o.pod):
		return environment.Unknown, incarnation, errors.New("kubernetes environment pod stopped")
	case environmentPodReady(o.pod):
		return environment.Ready, incarnation, nil
	}
	return environment.Creating, incarnation, nil
}

// checkVolume verifies the namespace, the bound claim and its volume before
// a Create. It returns the volume's UID and the Pod at the environment's
// name.
func (rt *environmentRuntime) checkVolume(ctx context.Context, r environmentResolved) (types.UID, *corev1.Pod, error) {
	o, err := rt.read(ctx, r)
	if err != nil {
		return "", nil, err
	}
	ns, pvc := o.namespace, o.claim
	switch {
	case ns == nil || pvc == nil:
		return "", nil, fmt.Errorf("kubernetes environment namespace or claim: %w", errEnvironmentObjectMissing)
	case ns.Name != rt.b.namespace || ns.UID != r.profile.namespaceUID || !environmentClaimMatches(r.profile, rt.b.namespace, pvc):
		return "", nil, fmt.Errorf("%w: kubernetes environment namespace or claim differs", backend.ErrIdentityMismatch)
	case ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating ||
		pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "":
		return "", nil, fmt.Errorf("kubernetes environment namespace or claim: %w", errEnvironmentObjectUnavailable)
	}
	if err := environmentVolumeMatches(r.profile, rt.b.namespace, r.native.VolumeUID, o.volume); err != nil {
		return "", nil, err
	}
	return o.volume.UID, o.pod, nil
}

// environmentRecord resolves n's unresolved send with pod, its result.
func environmentRecord(n *environmentNative, want, pod *corev1.Pod) {
	n.Resolved++
	n.Pod = &environmentNativePod{UID: pod.UID, Generation: pod.Generation, Altered: !environmentExact(want, pod)}
}

// createResult applies the outcome of this process's send of n's
// unresolved send to n. A send with an unknown outcome stays unresolved.
func createResult(n *environmentNative, want, pod *corev1.Pod, err error) error {
	switch {
	case err == nil && !qualificationText(string(pod.UID), 128):
		// A Pod may exist; the send stays unresolved.
		return errors.New("kubernetes environment pod has an invalid uid")
	case err == nil:
		environmentRecord(n, want, pod)
		if n.Pod.Altered {
			return fmt.Errorf("%w: kubernetes environment pod stored altered", backend.ErrIdentityMismatch)
		}
		return nil
	case environmentCreateRefused(err):
		n.Resolved++ // not applied
	}
	return err
}

// environmentCreateRefused reports a response to one Create POST proving
// that it was not applied: AlreadyExists, Forbidden or Invalid. Any other
// outcome, including a failure to dial, leaves the send unresolved.
func environmentCreateRefused(err error) bool {
	return apierrors.IsAlreadyExists(err) || apierrors.IsForbidden(err) || apierrors.IsInvalid(err)
}

// createFrame returns the create frame of environment id's send number
// send, or nil.
func (rt *environmentRuntime) createFrame(id string, send uint32) *environmentCreate {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if c := rt.creates[id]; c != nil && c.send == send {
		return c
	}
	return nil
}

func (rt *environmentRuntime) dropCreate(id string, c *environmentCreate) {
	rt.mu.Lock()
	if rt.creates[id] == c {
		delete(rt.creates, id)
	}
	rt.mu.Unlock()
}

// startCreate sends n's declared send in a new create frame. At the frame
// limit, or once the runtime is stopped, it sends nothing and returns an
// error. stop cancels the send but does not resolve it: it may still land.
func (rt *environmentRuntime) startCreate(r environmentResolved, n environmentNative) (*environmentCreate, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	switch {
	case rt.stopped:
		return nil, errEnvironmentStopped
	case len(rt.creates) >= maxEnvironmentCreateFrames:
		return nil, errors.New("kubernetes environment create frame limit reached")
	}
	c := &environmentCreate{send: n.Sends, done: make(chan struct{})}
	rt.creates[r.id] = c
	rt.sending++
	pod := r.want.DeepCopy()
	go func() {
		ctx, cancel := context.WithTimeout(rt.effects, environmentCreateTimeout)
		c.pod, c.err = rt.pods.create(ctx, pod)
		cancel()
		close(c.done)
		rt.mu.Lock()
		rt.sending--
		rt.checkDrainedLocked()
		rt.mu.Unlock()
	}()
	return c, nil
}

// settle resolves n's unresolved send from evidence: the result of this
// process's send of it, once its frame settles, or a Pod at its name that
// carries the nonce. It reports whether this process's send of it is still
// in flight.
func (rt *environmentRuntime) settle(ctx context.Context, r environmentResolved, n *environmentNative) (bool, error) {
	if c := rt.createFrame(r.id, n.Sends); c != nil {
		select {
		case <-c.done:
		default:
			return true, nil
		}
		rt.dropCreate(r.id, c)
		if err := createResult(n, r.want, c.pod, c.err); !n.unresolved() {
			return false, err
		}
	}
	got, err := rt.getPod(ctx, r)
	if err != nil || got == nil || !environmentNonceMatch(r.want, got) {
		return false, err
	}
	// Only the unresolved send can have created a Pod with our nonce.
	environmentRecord(n, r.want, got)
	return false, nil
}

// Ensure creates the environment's Pod once, then observes it.
func (rt *environmentRuntime) Ensure(ctx context.Context, ref environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	r, err := rt.resolve(ref)
	if err != nil {
		return environmentcontrol.Observation{}, err
	}
	release, err := rt.acquire(ctx, r.id)
	if err != nil {
		return environmentcontrol.Observation{}, err
	}
	defer release()
	mine := rt.takeDeclared(r.id, ref.Native)
	n := r.native
	result := func(phase environment.Phase, err error) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: phase, Native: n.encode()}, err
	}

	if n.Closing {
		return result(0, errEnvironmentDeleting)
	}
	if n.Write != nil {
		n.Write = nil
		if mine {
			return rt.sendCreate(ctx, r, n)
		}
		// Declared by another process, or before a restart: it may have
		// been sent, and stays unresolved.
	}
	if n.unresolved() {
		inflight, err := rt.settle(ctx, r, &n)
		switch {
		case n.unresolved() && err != nil:
			return result(0, err)
		case inflight:
			return result(environment.Creating, nil)
		case n.unresolved():
			return result(environment.Unknown, errEnvironmentUnresolved)
		case n.Pod == nil:
			return result(environment.Creating, err) // refused; declared again by a later call
		}
	}
	if n.Pod != nil {
		phase, incarnation, err := rt.observe(ctx, r, n)
		o, err := result(phase, err)
		o.Incarnation = incarnation
		return o, err
	}

	// Every send is resolved and none created a Pod: declare a send.
	volumeUID, got, err := rt.checkVolume(ctx, r)
	switch {
	case err != nil && environmentOutcome(err):
		return result(environment.Unknown, err)
	case err != nil:
		return result(0, err)
	case got != nil:
		return result(environment.Unknown, fmt.Errorf("%w: kubernetes environment pod name is held by another object", backend.ErrIdentityMismatch))
	case n.Sends == maxEnvironmentCreates:
		return result(environment.Unknown, errEnvironmentCreateLimit)
	}
	n.VolumeUID = volumeUID
	n.Sends++
	return rt.pending(r.id, environment.Creating, n, environmentWriteCreate, "")
}

// sendCreate sends n's unresolved send, which this process declared and has
// not sent, and waits for its result until ctx ends.
func (rt *environmentRuntime) sendCreate(ctx context.Context, r environmentResolved, n environmentNative) (environmentcontrol.Observation, error) {
	result := func(phase environment.Phase, err error) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: phase, Native: n.encode()}, err
	}
	if err := ctx.Err(); err != nil {
		n.Resolved++ // not sent
		return result(environment.Creating, err)
	}
	c, err := rt.startCreate(r, n)
	if err != nil {
		n.Resolved++ // not sent
		return result(environment.Creating, err)
	}
	select {
	case <-c.done:
	case <-ctx.Done():
		return result(environment.Creating, ctx.Err())
	}
	rt.dropCreate(r.id, c)
	if err := createResult(&n, r.want, c.pod, c.err); err != nil {
		if n.unresolved() || n.Pod != nil {
			return result(environment.Unknown, err)
		}
		return result(environment.Creating, err)
	}
	phase, incarnation, err := rt.observe(ctx, r, n)
	o, err := result(phase, err)
	o.Incarnation = incarnation
	return o, err
}

// sendWrite sends w, a stop or remove of n's recorded Pod that this process
// declared and has not sent, and records any stop evidence it yields in n.
func (rt *environmentRuntime) sendWrite(ctx context.Context, r environmentResolved, n *environmentNative, w environmentNativeWrite) error {
	if w.Op == environmentWriteStop {
		got, err := rt.getPod(ctx, r)
		if err != nil || got == nil || got.UID != n.Pod.UID || got.ResourceVersion != w.ResourceVersion {
			return err // not sent; Delete reads the Pod again
		}
		body := got.DeepCopy()
		deadline := environmentStopDeadline
		body.Spec.ActiveDeadlineSeconds = &deadline
		_, err = rt.pods.update(ctx, body)
		if err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	uid, rv := n.Pod.UID, w.ResourceVersion
	err := rt.pods.remove(ctx, r.name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &rv}})
	switch {
	case err == nil && w.Op == environmentWriteRemoveUnbound:
		// The precondition proves that the Pod was still unbound.
		n.Stopped = true
	case err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err):
		return err
	}
	return nil
}

// Delete closes the environment's frames, stops its Pod with evidence and
// then deletes it. It reports Deleted only once no Create send may still
// land and the recorded Pod, if any, was observed stopped and then gone. It
// deletes nothing but Pods of ours, each by UID and resourceVersion, and
// never the claim or volume.
func (rt *environmentRuntime) Delete(ctx context.Context, ref environmentcontrol.Ref) (environmentcontrol.Observation, error) {
	r, err := rt.resolve(ref)
	if err != nil {
		return environmentcontrol.Observation{}, err
	}
	rt.closeFrames(r.id)
	release, err := rt.acquire(ctx, r.id)
	if err != nil {
		return environmentcontrol.Observation{}, err
	}
	defer release()
	mine := rt.takeDeclared(r.id, ref.Native)
	n := r.native
	n.Closing = true
	result := func(phase environment.Phase, err error) (environmentcontrol.Observation, error) {
		return environmentcontrol.Observation{Phase: phase, Native: n.encode()}, err
	}

	if w := n.Write; w != nil {
		n.Write = nil
		switch {
		case w.Op == environmentWriteCreate && mine:
			n.Resolved++ // never sent
		case w.Op == environmentWriteCreate:
			// Possibly sent; it stays unresolved.
		case mine:
			if err := rt.sendWrite(ctx, r, &n, *w); err != nil {
				return result(environment.Deleting, err)
			}
		}
	}
	if n.unresolved() {
		if _, err := rt.settle(ctx, r, &n); n.unresolved() {
			return result(environment.Deleting, errors.Join(errEnvironmentUnresolved, err))
		}
	}
	if n.Pod == nil {
		return result(environment.Deleted, nil)
	}
	got, err := rt.getPod(ctx, r)
	if err != nil {
		return result(0, err)
	}
	if got == nil || got.UID != n.Pod.UID {
		// Every send is resolved, so no other Pod of ours can exist.
		if !n.Stopped {
			return result(environment.Unknown, errEnvironmentStopUnknown)
		}
		n.Pod, n.Stopped = nil, false
		return result(environment.Deleted, nil)
	}

	// got is the recorded Pod.
	write := func(op string) (environmentcontrol.Observation, error) {
		return rt.pending(r.id, environment.Deleting, n, op, got.ResourceVersion)
	}
	if !n.Stopped {
		switch {
		case environmentPodStopped(got), got.Spec.NodeName == "" && got.DeletionTimestamp != nil:
			// A deleting Pod cannot be bound to a node.
			n.Stopped = true
		case got.DeletionTimestamp != nil:
			return result(environment.Deleting, nil)
		case got.Spec.NodeName == "":
			return write(environmentWriteRemoveUnbound)
		case got.Spec.ActiveDeadlineSeconds == nil || *got.Spec.ActiveDeadlineSeconds > environmentStopDeadline:
			return write(environmentWriteStop)
		default:
			return result(environment.Deleting, nil) // the kubelet has not enforced the deadline yet
		}
	}
	if got.DeletionTimestamp != nil {
		return result(environment.Deleting, nil)
	}
	return write(environmentWriteRemove)
}

// reserve reserves a frame slot of environment id.
func (rt *environmentRuntime) reserve(id string) (uint64, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.stopped {
		return 0, errEnvironmentStopped
	}
	f := rt.frames[id]
	if f == nil {
		f = &environmentFrames{held: make(map[uint64]func())}
		rt.frames[id] = f
	}
	switch {
	case f.closing:
		return 0, errEnvironmentDeleting
	case rt.active >= maxEnvironmentFrames, len(f.held) >= maxFramesPerEnv:
		if len(f.held) == 0 {
			delete(rt.frames, id)
		}
		return 0, errors.New("environment session limit reached")
	}
	rt.seq++
	rt.active++
	f.held[rt.seq] = nil
	return rt.seq, nil
}

// attach records the cancel function of a reserved slot, and calls it at
// once if the environment is closing or the runtime stopped, so that a frame
// reserved before either is still canceled.
func (rt *environmentRuntime) attach(id string, key uint64, cancel func()) {
	rt.mu.Lock()
	f := rt.frames[id]
	f.held[key] = cancel
	closing := f.closing || rt.stopped
	rt.mu.Unlock()
	if closing {
		cancel()
	}
}

func (rt *environmentRuntime) release(id string, key uint64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	f := rt.frames[id]
	delete(f.held, key)
	rt.active--
	if len(f.held) == 0 && !f.closing {
		delete(rt.frames, id)
	}
	rt.checkDrainedLocked()
}

// isStopped reports whether stop was called.
func (rt *environmentRuntime) isStopped() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.stopped
}

// closeFrames latches the environment closing in this process and cancels
// its attached frames. It does not wait for them.
func (rt *environmentRuntime) closeFrames(id string) {
	rt.mu.Lock()
	f := rt.frames[id]
	if f == nil {
		f = &environmentFrames{held: make(map[uint64]func())}
		rt.frames[id] = f
	}
	f.closing = true
	var cancels []func()
	for _, c := range f.held {
		if c != nil {
			cancels = append(cancels, c)
		}
	}
	rt.mu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// ready checks that r is a recorded, ready environment that is not
// deleting.
func (rt *environmentRuntime) ready(ctx context.Context, r environmentResolved) error {
	n := r.native
	switch {
	case n.Closing:
		return errEnvironmentDeleting
	case n.Pod == nil:
		return errEnvironmentNotReady
	}
	phase, _, err := rt.observe(ctx, r, n)
	if phase != environment.Ready {
		return errors.Join(fmt.Errorf("%w: %v", errEnvironmentNotReady, phase), err)
	}
	return nil
}

// environmentExec is the frame of one exec. done is closed once the exec
// frame settled and the Pod at the environment's name was read again.
type environmentExec struct {
	x       *execFrame
	done    chan struct{}
	changed error // set before done is closed
}

func (e *environmentExec) Done() <-chan struct{} { return e.done }
func (e *environmentExec) Cancel()               { e.x.cancel() }

func (e *environmentExec) Result() environmentcontrol.ExecResult {
	res := e.x.result()
	out := environmentcontrol.ExecResult{Err: res.Err, Code: res.Code,
		Stdout: res.Stdout, Stderr: res.Stderr, Truncated: res.Truncated}
	select {
	case <-e.done:
	default:
		return out
	}
	out.Settled = true
	out.Refused = execRefused(e.x, res.Err)
	out.Exited = res.Reported && e.changed == nil
	if e.changed != nil {
		out.Err = errors.Join(out.Err, e.changed)
	}
	return out
}

// execRefused reports whether the settled frame x knows that no command
// started: it never began its upgrade request, its one connection was never
// dialed, or the upgrade was answered with a refusal. Any other failure,
// including one in TLS, which this frame cannot place before or after the
// request was written, is not a refusal.
func execRefused(x *execFrame, err error) bool {
	x.mu.Lock()
	dialed := x.dialed
	x.mu.Unlock()
	var op *net.OpError
	return !dialed || errors.As(err, &op) && op.Op == "dial" ||
		errors.Is(err, errEnvRefused) || errors.Is(err, errEnvRedirect) ||
		errors.Is(err, errEnvProtocol) || errors.Is(err, errEnvProxyRefused)
}

// Exec starts req in the environment's workspace container. An error means
// that no exec request was sent. The frame's exit is attributed to the
// environment only if the recorded Pod is still at its name afterwards.
func (rt *environmentRuntime) Exec(ref environmentcontrol.Ref, req environmentcontrol.ExecRequest) (environmentcontrol.Exec, error) {
	r, err := rt.resolve(ref)
	if err != nil {
		return nil, err
	}
	key, err := rt.reserve(r.id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(rt.effects, environmentReadTimeout)
	defer cancel()
	if err := rt.ready(ctx, r); err != nil {
		rt.release(r.id, key)
		return nil, err
	}
	if rt.isStopped() {
		rt.release(r.id, key)
		return nil, errEnvironmentStopped
	}
	x, err := rt.port.startExec(execRequest{Pod: r.name, Container: environmentContainer,
		Argv: slices.Clone(req.Argv), Stdin: slices.Clone(req.Stdin)})
	if err != nil {
		rt.release(r.id, key)
		return nil, err
	}
	rt.attach(r.id, key, x.cancel)
	e := &environmentExec{x: x, done: make(chan struct{})}
	go func() {
		<-x.done
		ctx, cancel := context.WithTimeout(rt.effects, environmentReadTimeout)
		defer cancel()
		got, err := rt.getPod(ctx, r)
		switch {
		case err != nil:
			e.changed = errors.Join(errEnvironmentPodChanged, err)
		case got == nil || got.UID != r.native.Pod.UID:
			e.changed = errEnvironmentPodChanged
		}
		rt.release(r.id, key)
		close(e.done)
	}()
	return e, nil
}

// environmentForward is the frame of one gateway exchange.
type environmentForward struct {
	f      *portFrame
	cancel context.CancelFunc
}

func (f *environmentForward) Done() <-chan struct{} { return f.f.done }
func (f *environmentForward) Cancel()               { f.cancel(); f.f.cancel() }

func (f *environmentForward) Result() environmentcontrol.ForwardResult {
	res := f.f.result()
	return environmentcontrol.ForwardResult{Settled: res.Settled, Err: res.Err, Status: res.Status}
}

// environmentRefusal is the settled frame of an exchange that the runtime
// refused before any request.
type environmentRefusal struct {
	done chan struct{}
	err  error
}

func (f environmentRefusal) Done() <-chan struct{} { return f.done }
func (f environmentRefusal) Cancel()               {}

func (f environmentRefusal) Result() environmentcontrol.ForwardResult {
	return environmentcontrol.ForwardResult{Settled: true, Err: f.err, Status: http.StatusServiceUnavailable}
}

// Forward relays one exchange to the environment's declared port. A slot is
// reserved before any read and released when the exchange's frame settles.
// Delete cancels the exchange through its request context.
func (rt *environmentRuntime) Forward(w http.ResponseWriter, req *http.Request, ref environmentcontrol.Ref) environmentcontrol.Forward {
	refuse := func(err error) environmentcontrol.Forward {
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		done := make(chan struct{})
		close(done)
		return environmentRefusal{done: done, err: err}
	}
	r, err := rt.resolve(ref)
	if err != nil {
		return refuse(err)
	}
	if ref.Spec.Port != environment.Port {
		return refuse(errors.New("environment declares no port"))
	}
	key, err := rt.reserve(r.id)
	if err != nil {
		return refuse(err)
	}
	// The exchange, including its ready read, ends with the request or when
	// the runtime stops.
	ctx, cancel := context.WithCancel(req.Context())
	unlink := context.AfterFunc(rt.effects, cancel)
	if err := rt.ready(ctx, r); err != nil {
		unlink()
		cancel()
		rt.release(r.id, key)
		return refuse(err)
	}
	rt.attach(r.id, key, cancel)
	f := rt.serve(w, req.WithContext(ctx), portTarget{Pod: r.name, Port: environment.Port})
	go func() {
		<-f.done
		unlink()
		cancel()
		rt.release(r.id, key)
	}()
	return &environmentForward{f: f, cancel: cancel}
}
