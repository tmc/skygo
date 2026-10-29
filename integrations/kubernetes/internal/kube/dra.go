package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The DRA mode (docs/dra.md) runs a task that names a DRA accelerator as one
// standalone ResourceClaim, created first, and then exactly one directly
// managed Pod that uses it. It is experimental, disabled unless configured,
// and not native qualification: its tests use a loopback API whose status is
// MODELED.
//
// Its operating contract, which Skygo assumes and does not attest:
//
//   - The namespace is dedicated to this installation and its admission is
//     trusted: no other writer creates, replaces, mutates or consumes its
//     claims and Pods, schedules Pods directly onto nodes, or deletes them by
//     force. The namespace UID and ExactCount are not a consumer ACL.
//   - A single deployed server writes the DRA and execution state roots and
//     holds the namespace credentials. The operator stops older executables
//     and withholds the new roots and namespace credentials from them; a new
//     marker does not fence an old binary, which never reads it.
//   - The kubelet, container runtime and DRA driver are standard and
//     trusted: the kubelet publishes a Pod's terminal phase only after its
//     containers stopped and its DRA resources were unprepared (v1.35.9
//     pkg/kubelet/kubelet.go:2224-2289; status_manager.go:1281-1295).
//
// Under it, a witnessed Succeeded Pod lets API cleanup delete the exact Pod and
// then the exact claim, after log retention expires; their absence is API
// cleanup only, never proof of physical release. Every other outcome keeps
// the claim and its record in HOLD for the operator.

// Bounds of DRA requests and maintenance.
const (
	draReadTimeout = 30 * time.Second // construction discovery and reads
	draPassTimeout = 30 * time.Second // one record's maintenance pass
	draInterval    = 10 * time.Second // between maintenance passes
)

// draRuntime is the DRA custody of one Backend: its journal scope, the
// namespace and classes pinned at construction, and the registries of its
// original operations and native calls, which Close joins.
type draRuntime struct {
	store        *draStore
	namespaceUID string
	classes      map[string]draClass // by DeviceClass name
	retention    time.Duration       // zero retains succeeded Pods and claims
	now          func() time.Time
	orig         *draOriginals

	mu       sync.Mutex
	closed   bool
	ops      int
	idle     chan struct{} // closed when ops returns to zero; nil while zero
	starting map[string]int
}

// draClass is a DeviceClass pinned by UID and complete spec digest.
type draClass struct{ uid, spec string }

var errDRAClosed = errors.New("kubernetes dra integration closed")

// begin registers one original operation, a Start of attempt start if start
// is not empty. The caller calls the returned function once the operation,
// its last response, body and record write have returned.
func (d *draRuntime) begin(start string) (func(), error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, errDRAClosed
	}
	if d.ops == 0 {
		d.idle = make(chan struct{})
	}
	d.ops++
	if start != "" {
		d.starting[start]++
	}
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if start != "" {
			if d.starting[start]--; d.starting[start] == 0 {
				delete(d.starting, start)
			}
		}
		if d.ops--; d.ops == 0 {
			close(d.idle)
			d.idle = nil
		}
	}, nil
}

// operation admits one original operation of attempt (a Start of it if
// start) and returns its frame and ctx carrying it. The frame keeps the
// registration, and every permit added to it, until the caller ends it and
// each original of its calls has finished.
func (d *draRuntime) operation(ctx context.Context, attempt string, start bool) (context.Context, *draFrame, error) {
	s := ""
	if start {
		s = attempt
	}
	done, err := d.begin(s)
	if err != nil {
		return nil, nil, err
	}
	ctx, f := withDRAFrame(ctx, d.orig, attempt)
	f.hold(done)
	return ctx, f, nil
}

// startInFlight reports whether a Start of attempt id runs in this process.
func (d *draRuntime) startInFlight(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starting[id] != 0
}

// holdClosed durably records a failed original Close of a call of attempt id
// as the attempt's HOLD. Its original's frame still holds every permit. An
// attempt already in HOLD keeps its first reason, which fences it alike
// (authority), in this process and a replacement.
func (d *draRuntime) holdClosed(id string) error {
	return d.store.hold(id, draHoldClose)
}

var errDRAHold = errors.New("kubernetes dra attempt is in HOLD")

// authority reports whether attempt r may still continue: no original Close
// of it failed in this process, and its record has no HOLD. Every HOLD,
// whatever its reason, its age or the process that recorded it, is the same
// fence: no recovery pin, create, cancellation, cleanup deletion or
// retirement follows it, and what it already recorded stays. Its reason is
// reported as recorded, not classified: only draHoldClose, which this code
// writes for a failed Close, is also reported as one.
func (d *draRuntime) authority(r draRecord) error {
	if _, failed := d.orig.state(r.Attempt); failed != nil {
		return fmt.Errorf("%w: %w", errDRAHold, failed)
	}
	switch r.Hold {
	case "":
		return nil
	case draHoldClose:
		return fmt.Errorf("%w: %w", errDRAHold, errDRACloseFailed)
	}
	return fmt.Errorf("%w: %s", errDRAHold, r.Hold)
}

// admit consumes the authority of one native mutation of attempt id and
// admits its single call in the frame of ctx, in one step under the journal
// lock, after the caller's namespace and class rechecks: the current record
// must have no HOLD and no original Close of the attempt may have failed
// (authority); intent, which records the mutation's grant or intent, then
// sees that current record; and only then is the call registered
// (draOriginals.add), which refuses it if a failure was published since.
//
// That registration is the admission boundary. A failed original Close is
// published in memory before its HOLD is written (draCall.close), and the
// HOLD is written under the same journal lock, so a failure published or a
// HOLD recorded before admission admits nothing. A call admitted before it
// may still be sent and deliver; its response, UID and delivery are kept as
// truth and never recalled. No network I/O happens under the lock. The
// caller sends the admitted call once (draCall.send) or settles it unsent.
func (d *draRuntime) admit(ctx context.Context, id string, intent func(*draRecord) error) (draRecord, *draCall, context.Context, error) {
	var c *draCall
	var cctx context.Context
	r, err := d.store.update(id, func(x *draRecord) error {
		if err := d.authority(*x); err != nil {
			return err
		}
		if err := intent(x); err != nil {
			return err
		}
		var err error
		c, cctx, err = newDRACall(ctx, true)
		return err
	})
	if err != nil {
		if c != nil {
			c.settle() // registered, but its intent was not recorded: never sent
		}
		return r, nil, nil, err
	}
	return r, c, cctx, nil
}

// pin pins the claim, or the Pod if pod, of attempt id as uid under the
// journal lock, unless the current record forbids it (authority).
func (d *draRuntime) pin(id string, pod bool, nonce, uid string) (draRecord, error) {
	return d.store.update(id, func(x *draRecord) error {
		if err := d.authority(*x); err != nil {
			return err
		}
		return pinDRA(draObjectOf(x, pod), nonce, uid)
	})
}

// fence refuses every later operation and native call. It is never undone.
func (d *draRuntime) fence() {
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	d.orig.fence()
}

// close fences d and joins the operations in progress, and the originals of
// their calls, until ctx is done. A failed original Close is reported every
// time. A log body of the scope whose Close was uncertain keeps the operation
// permit for the process lifetime (execution_logs.go); close then reports it,
// so that the server keeps everything the integration depends on.
func (d *draRuntime) close(ctx context.Context) error {
	d.fence()
	d.mu.Lock()
	idle := d.idle
	d.mu.Unlock()
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
			// An operation whose failed Close could not be recorded keeps
			// its permits for the process lifetime: report that failure too.
			return errors.Join(fmt.Errorf("kubernetes dra operations remain held: %w", ctx.Err()), d.orig.failure())
		}
	}
	if err := d.orig.join(ctx); err != nil {
		return err
	}
	logCustodies.mu.Lock()
	defer logCustodies.mu.Unlock()
	if slices.ContainsFunc(logCustodies.kept, func(c *logCustody) bool { return c.store == d.store.s }) {
		return fmt.Errorf("kubernetes dra: %w", errLogBodyHeld)
	}
	return nil
}

// draDiscover confirms the stable resource.k8s.io/v1 API and reads the
// namespace UID and each DRA policy's class. It sends only reads, each a
// call in the frame of ctx on the receipt client's own transport, bounded by
// ctx; discovery does not use the SDK's ServerResourcesForGroupVersion,
// whose request has no context (discovery/discovery_client.go:413).
func (b *Backend) draDiscover(ctx context.Context) (string, map[string]draClass, error) {
	client, err := receiptHTTP(b.client.Discovery().RESTClient())
	if err != nil {
		return "", nil, err
	}
	res, err := draDo(ctx, client.Get().AbsPath("/apis/resource.k8s.io/v1"))
	if err != nil {
		return "", nil, fmt.Errorf("kubernetes dra discovery: %w", err)
	}
	if err := res.Error(); err != nil {
		var status apierrors.APIStatus
		if !errors.As(err, &status) {
			// No complete reply: the caller's bound, or a transport error.
			return "", nil, fmt.Errorf("kubernetes dra discovery: %w", err)
		}
		return "", nil, errors.New("kubernetes dra requires resource.k8s.io/v1")
	}
	raw, _ := res.Raw()
	list := new(metav1.APIResourceList)
	if err := json.Unmarshal(raw, list); err != nil || list.GroupVersion != "resource.k8s.io/v1" {
		return "", nil, errors.New("kubernetes dra requires resource.k8s.io/v1")
	}
	need := []struct {
		name       string
		namespaced bool
		verbs      []string
	}{
		{"resourceclaims", true, []string{"create", "get", "delete"}},
		{"deviceclasses", false, []string{"get"}},
	}
	for _, n := range need {
		i := slices.IndexFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Name == n.name })
		if i < 0 || list.APIResources[i].Namespaced != n.namespaced {
			return "", nil, fmt.Errorf("kubernetes dra requires resource.k8s.io/v1 %s", n.name)
		}
		for _, v := range n.verbs {
			if !slices.Contains(list.APIResources[i].Verbs, v) {
				return "", nil, fmt.Errorf("kubernetes dra requires resource.k8s.io/v1 %s %s", n.name, v)
			}
		}
	}
	uid, err := b.draNamespace(ctx)
	if err != nil {
		return "", nil, err
	}
	classes := make(map[string]draClass)
	for _, a := range b.policy.Accelerators {
		if a.DRA == nil {
			continue
		}
		if _, ok := classes[a.DRA.DeviceClass]; ok {
			continue
		}
		c, err := b.draClass(ctx, a.DRA.DeviceClass)
		if err != nil {
			return "", nil, err
		}
		classes[a.DRA.DeviceClass] = c
	}
	return uid, classes, nil
}

// draNamespace reads the target namespace and returns its UID. A complete
// response showing it missing, terminating or different is positive drift,
// an identity difference; a lost, incomplete or unclosed response is not.
func (b *Backend) draNamespace(ctx context.Context) (string, error) {
	var ns *corev1.Namespace
	_, err := draSend(ctx, func(ctx context.Context) error {
		var err error
		ns, err = b.client.CoreV1().Namespaces().Get(ctx, b.namespace, metav1.GetOptions{})
		return err
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: kubernetes dra namespace is missing", backend.ErrIdentityMismatch)
		}
		return "", fmt.Errorf("read kubernetes dra namespace: %w", err)
	}
	if ns.Name != b.namespace || !validInstance(string(ns.UID)) {
		return "", fmt.Errorf("%w: kubernetes dra namespace differs", backend.ErrIdentityMismatch)
	}
	if ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating {
		return "", fmt.Errorf("%w: kubernetes dra namespace is terminating", backend.ErrIdentityMismatch)
	}
	return string(ns.UID), nil
}

// draClass reads the DeviceClass name strictly and pins it. Its errors are
// classified as draNamespace's.
func (b *Backend) draClass(ctx context.Context, name string) (draClass, error) {
	client, err := receiptHTTP(b.client.ResourceV1().RESTClient())
	if err != nil {
		return draClass{}, err
	}
	res, err := draDo(ctx, client.Get().Resource("deviceclasses").Name(name))
	if err != nil {
		return draClass{}, fmt.Errorf("read kubernetes dra device class: %w", err)
	}
	if err := res.Error(); err != nil {
		if apierrors.IsNotFound(err) {
			return draClass{}, fmt.Errorf("%w: kubernetes dra device class is missing", backend.ErrIdentityMismatch)
		}
		return draClass{}, fmt.Errorf("read kubernetes dra device class: %w", err)
	}
	raw, _ := res.Raw()
	c, err := decodeDeviceClass(raw)
	if err != nil {
		return draClass{}, err
	}
	if c.Name != name || c.Namespace != "" || !validInstance(string(c.UID)) || len(c.OwnerReferences) != 0 {
		return draClass{}, fmt.Errorf("%w: kubernetes dra device class differs", backend.ErrIdentityMismatch)
	}
	if c.DeletionTimestamp != nil {
		return draClass{}, fmt.Errorf("%w: kubernetes dra device class is being deleted", backend.ErrIdentityMismatch)
	}
	spec, err := draClassSpec(c)
	if err != nil {
		return draClass{}, err
	}
	return draClass{uid: string(c.UID), spec: spec}, nil
}

// draRecheck reads the namespace and class again before each native
// mutation of record r, its creates, its cancellation and each API cleanup
// deletion, before that mutation's admission (admit), and refuses any change
// of either's UID or of the class spec. A
// changed class is never followed and never falls back to an extended
// resource. Only an identity difference is drift; any other error is
// transient and proves nothing.
func (b *Backend) draRecheck(ctx context.Context, r *draRecord) error {
	uid, err := b.draNamespace(ctx)
	if err != nil {
		return err
	}
	if uid != r.NamespaceUID {
		return fmt.Errorf("%w: kubernetes dra namespace changed", backend.ErrIdentityMismatch)
	}
	c, err := b.draClass(ctx, r.ClassName)
	if err != nil {
		return err
	}
	if c.uid != r.ClassUID || c.spec != r.ClassSHA256 {
		return fmt.Errorf("%w: kubernetes dra device class changed", backend.ErrIdentityMismatch)
	}
	return nil
}
