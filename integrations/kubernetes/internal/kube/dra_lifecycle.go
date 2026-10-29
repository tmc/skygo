package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// startDRA runs attempt id of DRA task t at most once:
//
//  1. it declares both objects durably, with independent nonces, their names,
//     spec digests, the task digest and the namespace and class pinned at
//     construction, each object's send grant unconsumed (intent);
//  2. it rechecks the namespace and class, consumes the claim grant (sent)
//     and sends the claim create once, without retries, redirects or a
//     replayable body, and pins the UID of an exact 201 response;
//  3. it rechecks the namespace, class and the pinned claim, consumes the Pod
//     grant and sends the Pod create once in the same way, and pins its UID.
//
// No Pod is sent before the claim is pinned, and nothing is pinned before the
// original Close of its create response returned nil. A complete response,
// read to io.EOF, is terminal: a definite rejection records rejected, and
// any other status, 500, 429, 409 and redirections included, or a 201 that
// is not exactly the declared object, records refused with a witness and is
// never adopted by a later read. A lost or incomplete response leaves the
// grant consumed: only a later read of the exact declared object may pin
// it, and nothing is resent. An exact 201 whose original Close is held or
// failed records its UID as seen, truth without authority, and ends Start.
// Any failure before a grant is consumed withdraws the unconsumed grants, so
// a claim whose response was lost never gets a Pod. Each grant is consumed
// and its create admitted in one step against the current record (admit):
// any HOLD forbids both.
func (b *Backend) startDRA(ctx context.Context, id string, t backend.Task) error {
	d := b.dra
	if d == nil {
		return errDRAUnavailable
	}
	if b.binding == "" {
		return fmt.Errorf("%w: kubernetes target is unbound", backend.ErrIdentityMismatch)
	}
	if err := b.Check(t); err != nil {
		return err
	}
	ctx, f, err := d.operation(ctx, id, true)
	if err != nil {
		return err
	}
	defer f.end()
	release, err := d.store.s.operate(ctx)
	if err != nil {
		return err
	}
	f.hold(release)
	r, claim, pod, err := b.draDeclare(id, t)
	if err != nil {
		return err
	}
	if err := d.store.create(r); err != nil {
		return err
	}
	// From here every failure leaves a record that never permits a resend.
	if err := b.draRecheck(ctx, &r); err != nil {
		return b.draWithdraw(id, err)
	}
	var call *draCall
	var cctx context.Context
	if r, call, cctx, err = d.admit(ctx, id, consumeDRA(false, r.Claim.Nonce)); err != nil {
		return err
	}
	if err := d.store.step("send", &r); err != nil {
		call.settle()
		return err
	}
	if _, err := b.draCreate(cctx, call, r, false, claim); err != nil {
		return b.draWithdraw(id, err)
	}
	if r, err = d.store.load(id); err != nil {
		return err
	}
	if err := b.draRecheck(ctx, &r); err != nil {
		return b.draWithdraw(id, err)
	}
	c, err := b.draReadClaim(ctx, &r)
	if err == nil {
		// No Pod of this attempt exists yet: any reservation is foreign.
		if len(c.Status.ReservedFor) != 0 {
			err = fmt.Errorf("%w: kubernetes dra claim has a foreign consumer", backend.ErrIdentityMismatch)
		}
	}
	if err != nil {
		return b.draWithdraw(id, err)
	}
	if r, call, cctx, err = d.admit(ctx, id, consumeDRA(true, r.Pod.Nonce)); err != nil {
		return err
	}
	if err := d.store.step("send", &r); err != nil {
		call.settle()
		return err
	}
	_, err = b.draCreate(cctx, call, r, true, pod)
	return err
}

// draDeclare renders the declared record and both objects of attempt id.
func (b *Backend) draDeclare(id string, t backend.Task) (draRecord, *resourcev1.ResourceClaim, *corev1.Pod, error) {
	p := b.draPolicy(t)
	hash, err := taskHash(t)
	if err != nil {
		return draRecord{}, nil, nil, err
	}
	var nonces [2][32]byte
	for i := range nonces {
		if _, err := rand.Read(nonces[i][:]); err != nil {
			return draRecord{}, nil, nil, fmt.Errorf("kubernetes creation nonce: %w", err)
		}
	}
	cn, pn := hex.EncodeToString(nonces[0][:]), hex.EncodeToString(nonces[1][:])
	claimName, err := draClaimName(id, cn)
	if err != nil {
		return draRecord{}, nil, nil, err
	}
	claim := b.draClaim(id, claimName, hash, p, int64(t.AcceleratorCount))
	pod := b.draPod(id, hash, claimName, t)
	cs, err := draSpec(claim, false)
	if err != nil {
		return draRecord{}, nil, nil, err
	}
	ps, err := draSpec(pod, false)
	if err != nil {
		return draRecord{}, nil, nil, err
	}
	claim.Annotations[nonceAnnotation], claim.Annotations[specAnnotation] = cn, cs
	pod.Annotations[nonceAnnotation], pod.Annotations[specAnnotation] = pn, ps
	pin, ok := b.dra.classes[p.DeviceClass]
	if !ok {
		return draRecord{}, nil, nil, errDRAUnavailable
	}
	r := draRecord{Attempt: id, TaskSHA256: hash, NamespaceUID: b.dra.namespaceUID,
		ClassName: p.DeviceClass, ClassUID: pin.uid, ClassSHA256: pin.spec, Count: int64(t.AcceleratorCount),
		Claim: draObject{Name: claimName, Nonce: cn, SpecSHA256: cs, Send: phaseIntent},
		Pod:   draObject{Name: pod.Name, Nonce: pn, SpecSHA256: ps, Send: phaseIntent}}
	return r, claim, pod, nil
}

// consumeDRA consumes the claim's or the Pod's single send grant.
func consumeDRA(pod bool, nonce string) func(*draRecord) error {
	return func(x *draRecord) error {
		o := &x.Claim
		if pod {
			o = &x.Pod
		}
		if o.Send != phaseIntent || o.Nonce != nonce || x.Cancel != "" || x.Hold != "" ||
			pod && x.Claim.Send != phasePinned {
			return errors.New("kubernetes dra create grant unavailable")
		}
		o.Send = phaseSent
		return nil
	}
}

// draWithdraw withdraws every unconsumed grant of attempt id and returns cause
// joined with any storage error.
func (b *Backend) draWithdraw(id string, cause error) error {
	_, err := b.dra.store.update(id, func(x *draRecord) error {
		if x.Claim.Send == phaseIntent {
			x.Claim.Send = draWithdrawn
		}
		if x.Pod.Send == phaseIntent {
			x.Pod.Send = draWithdrawn
		}
		return nil
	})
	return errors.Join(cause, err)
}

func pinDRA(o *draObject, nonce, uid string) error {
	if o.Nonce != nonce {
		return fmt.Errorf("%w: kubernetes dra record changed", backend.ErrIdentityMismatch)
	}
	if o.Send == phasePinned {
		if o.UID == uid {
			return nil
		}
		return errReceiptPinned
	}
	if o.Send != phaseSent {
		return fmt.Errorf("%w: kubernetes dra record differs", backend.ErrIdentityMismatch)
	}
	if o.Seen != "" && o.Seen != uid {
		return errReceiptPinned
	}
	o.Send, o.UID = phasePinned, uid
	return nil
}

func draObjectOf(x *draRecord, pod bool) *draObject {
	if pod {
		return &x.Pod
	}
	return &x.Claim
}

// draREST returns the receipt REST client and resource of claims or Pods.
func (b *Backend) draREST(pod bool) (*rest.RESTClient, string, error) {
	var c rest.Interface = b.client.ResourceV1().RESTClient()
	resource := "resourceclaims"
	if pod {
		c, resource = b.client.CoreV1().RESTClient(), "pods"
	}
	client, err := receiptHTTP(c)
	return client, resource, err
}

// draCreate sends the single create of obj, the declared claim or Pod of
// record r whose grant was consumed by the admission of call c (admit), with
// cctx, classifies its response by what was actually received (startDRA),
// and records the result. It returns the pinned UID. A HOLD recorded since
// the admission fences the pin: the exact 201 is then kept as seen truth.
func (b *Backend) draCreate(cctx context.Context, c *draCall, r draRecord, pod bool, obj runtime.Object) (string, error) {
	d := b.dra
	o := draObjectOf(&r, pod)
	kind, nonce := o.kind(), o.Nonce
	client, resource, err := b.draREST(pod)
	if err != nil {
		c.settle() // admitted, never sent
		return "", err
	}
	res := client.Post().Namespace(b.namespace).Resource(resource).
		VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(obj).
		MaxRetries(0).Do(cctx)
	serr := c.settle()
	code, raw, complete := c.response()
	if !complete {
		// Lost, or headers without the whole body: recoverable only as
		// the exact declared object, by a later read.
		return "", errors.Join(fmt.Errorf("kubernetes dra create response lost or incomplete (status %d)", code), res.Error(), serr)
	}
	refuse := func(err error) error {
		if rerr := d.store.refuse(r.Attempt, pod, nonce, draWitness(kind, code, raw)); rerr != nil {
			err = errors.Join(err, rerr)
		}
		return errors.Join(err, serr)
	}
	if code != http.StatusCreated {
		err := res.Error()
		if err == nil {
			err = fmt.Errorf("%w: kubernetes dra create returned status %d, not created", backend.ErrIdentityMismatch, code)
		}
		if code >= 400 && definiteRejection(err) {
			if _, uerr := d.store.update(r.Attempt, func(x *draRecord) error {
				o := draObjectOf(x, pod)
				if o.Send != phaseSent || o.Nonce != nonce {
					return errors.New("kubernetes dra create record changed")
				}
				o.Send = phaseRejected
				if x.Pod.Send == phaseIntent {
					x.Pod.Send = draWithdrawn
				}
				return nil
			}); uerr != nil {
				return "", errors.Join(err, uerr, serr)
			}
			return "", errors.Join(err, serr)
		}
		// Every other complete status is a terminal refusal.
		return "", refuse(err)
	}
	var uid string
	got, err := decodeDRA(kind, raw)
	if err == nil {
		uid, err = b.draMatch(got, &r, o, false)
	}
	if err == nil && !pod && !draEmptyStatus(got.(*resourcev1.ResourceClaim)) {
		err = fmt.Errorf("%w: kubernetes dra claim create response carries status", backend.ErrIdentityMismatch)
	}
	if err != nil {
		return "", refuse(err)
	}
	// seen keeps the UID of the exact created object as truth, without
	// authority, when cause forbids its pin. A failed Close is also a HOLD
	// of the attempt; an earlier HOLD stays.
	seen := func(cause error) error {
		_, uerr := d.store.update(r.Attempt, func(x *draRecord) error {
			o := draObjectOf(x, pod)
			if o.Send != phaseSent || o.Nonce != nonce || o.Seen != "" && o.Seen != uid {
				return nil // a newer refusal, pin or cancellation wins
			}
			o.Seen = uid
			if errors.Is(cause, errDRACloseFailed) && x.Hold == "" {
				x.Hold = draHoldClose // normally recorded already by its original
			}
			return nil
		})
		return errors.Join(cause, uerr)
	}
	if serr != nil {
		return "", seen(serr)
	}
	_, err = d.pin(r.Attempt, pod, nonce, uid)
	switch {
	case errors.Is(err, errReceiptPinned):
		return "", refuse(err)
	case errors.Is(err, errDRAHold):
		return "", seen(err)
	}
	return uid, err
}

// draEmptyStatus reports a claim without any status, as a create returns it.
func draEmptyStatus(c *resourcev1.ResourceClaim) bool {
	data, err := json.Marshal(c.Status)
	return err == nil && string(data) == "{}"
}

// draReadClaim reads the declared claim of r and matches it exactly.
func (b *Backend) draReadClaim(ctx context.Context, r *draRecord) (*resourcev1.ResourceClaim, error) {
	obj, err := b.draRead(ctx, r, false)
	if err != nil {
		return nil, err
	}
	return obj.(*resourcev1.ResourceClaim), nil
}

// draReadPod reads the declared Pod of r and matches it exactly.
func (b *Backend) draReadPod(ctx context.Context, r *draRecord) (*corev1.Pod, error) {
	obj, err := b.draRead(ctx, r, true)
	if err != nil {
		return nil, err
	}
	return obj.(*corev1.Pod), nil
}

func (b *Backend) draRead(ctx context.Context, r *draRecord, pod bool) (runtime.Object, error) {
	o := draObjectOf(r, pod)
	client, resource, err := b.draREST(pod)
	if err != nil {
		return nil, err
	}
	res, err := draDo(ctx, client.Get().Namespace(b.namespace).Resource(resource).Name(o.Name))
	if err != nil {
		return nil, err
	}
	if err := res.Error(); err != nil {
		return nil, err
	}
	raw, _ := res.Raw()
	obj, err := decodeDRA(o.kind(), raw)
	if err != nil {
		return nil, err
	}
	if _, err := b.draMatch(obj, r, o, true); err != nil {
		return nil, err
	}
	return obj, nil
}

// draRecordFor validates ref against its durable record without requests:
// binding, task digest, both declared spec digests rendered again from
// ref.Task, and the pinned Pod UID as ref.Instance.
func (b *Backend) draRecordFor(ref backend.Reference) (draRecord, error) {
	if b.dra == nil {
		return draRecord{}, errDRAUnavailable
	}
	if b.binding == "" || ref.Binding != b.binding {
		return draRecord{}, fmt.Errorf("%w: kubernetes target binding differs", backend.ErrIdentityMismatch)
	}
	if ref.Instance != "" && !validInstance(ref.Instance) {
		return draRecord{}, fmt.Errorf("%w: invalid kubernetes instance", backend.ErrIdentityMismatch)
	}
	if err := b.Check(ref.Task); err != nil {
		return draRecord{}, err
	}
	hash, err := taskHash(ref.Task)
	if err != nil {
		return draRecord{}, err
	}
	r, err := b.dra.store.load(ref.ID)
	if err != nil {
		return r, err
	}
	p := b.draPolicy(ref.Task)
	cs, cerr := draSpec(b.draClaim(ref.ID, r.Claim.Name, hash, p, int64(ref.Task.AcceleratorCount)), false)
	ps, perr := draSpec(b.draPod(ref.ID, hash, r.Claim.Name, ref.Task), false)
	if cerr != nil || perr != nil || r.TaskSHA256 != hash || r.Claim.SpecSHA256 != cs || r.Pod.SpecSHA256 != ps {
		return r, fmt.Errorf("%w: kubernetes dra record differs", backend.ErrIdentityMismatch)
	}
	if ref.Instance != "" && (r.Pod.Send != phasePinned || r.Pod.UID != ref.Instance) {
		return r, fmt.Errorf("%w: kubernetes instance differs from its record", backend.ErrIdentityMismatch)
	}
	return r, nil
}

// observeDRA reports the attempt of ref, reconciling its record.
func (b *Backend) observeDRA(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	d := b.dra
	if d == nil {
		return backend.Status{}, errDRAUnavailable
	}
	ctx, f, err := d.operation(ctx, ref.ID, false)
	if err != nil {
		return backend.Status{}, err
	}
	defer f.end()
	release, err := d.store.s.operate(ctx)
	if err != nil {
		return backend.Status{}, err
	}
	f.hold(release)
	r, err := b.draRecordFor(ref)
	if errors.Is(err, errReceiptRefused) {
		return backend.Status{State: backend.Unknown, Reason: "kubernetes dra create response refused"}, nil
	}
	if err != nil {
		return backend.Status{}, err
	}
	return b.draReconcile(ctx, r)
}

// draHoldClose is the HOLD of an attempt an original Close of whose calls
// failed. Like every HOLD (draRuntime.authority), it fences every later
// continuation of the attempt in any process: no recovery pin, create,
// cancellation, cleanup deletion or retirement. The truths already recorded,
// a seen or pinned UID, a delivery, a witnessed outcome, stay as they are.
// In the first mode every HOLD is left to the operator.
const draHoldClose = "kubernetes dra response close failed"

// draHold records reason as the attempt's sticky HOLD, unless it has one.
// A HOLD keeps the claim and its record for the operator: it is never
// repaired by a later observation.
func (b *Backend) draHold(r draRecord, reason string) (draRecord, error) {
	if r.Hold != "" {
		return r, nil
	}
	return b.dra.store.update(r.Attempt, func(x *draRecord) error {
		if x.Hold == "" {
			x.Hold = reason
		}
		return nil
	})
}

// draHoldRead records a HOLD for a read that did not match its record.
func (b *Backend) draHoldRead(r draRecord, what string, err error) error {
	if errors.Is(err, backend.ErrIdentityMismatch) {
		_, herr := b.draHold(r, "kubernetes dra "+what+" identity differs")
		return errors.Join(err, herr)
	}
	return err
}

// draReconcile observes the objects of record r, records what the exact
// objects show and returns the attempt's status. It is shared by Observe and
// maintenance, and so continues after the core stops observing.
//
// Execution is reported from the exact pinned Pod, separately from cleanup:
// a witnessed success or failure is recorded once and reported thereafter.
// A missing Pod without a witness is Unknown; a missing or replaced object, a
// foreign claim consumer, a failed Pod or an unusual success is a sticky
// HOLD. Nothing here deletes anything.
func (b *Backend) draReconcile(ctx context.Context, r draRecord) (backend.Status, error) {
	d := b.dra
	id := r.Attempt
	if r.State == draRetired {
		return backend.Status{State: backend.Succeeded, ExitCode: int32ptr(0), Instance: r.Pod.UID, Reason: "kubernetes dra api cleanup complete"}, nil
	}
	var err error
	// Grants no Start of this process can still consume are withdrawn.
	if (r.Claim.Send == phaseIntent || r.Pod.Send == phaseIntent) && !d.startInFlight(id) {
		if r, err = d.store.update(id, func(x *draRecord) error {
			if x.Claim.Send == phaseIntent {
				x.Claim.Send = draWithdrawn
			}
			if x.Pod.Send == phaseIntent {
				x.Pod.Send = draWithdrawn
			}
			return nil
		}); err != nil {
			return backend.Status{}, err
		}
	}
	if r.Claim.Send == phaseSent || r.Pod.Send == phaseSent {
		// No recovery read pins anything while an original create of this
		// process is still held, after any original Close of the attempt
		// failed, in this process or an earlier one, or under any other
		// HOLD (authority). A failure of this process is a HOLD; an earlier
		// HOLD keeps its reason.
		held, _ := d.orig.state(id)
		if d.authority(r) != nil {
			if r, err = b.draHold(r, draHoldClose); err != nil {
				return backend.Status{}, err
			}
			return backend.Status{State: backend.Unknown, Reason: r.Hold}, nil
		}
		if held {
			return backend.Status{State: backend.Starting, Reason: "kubernetes dra create response not closed"}, nil
		}
	}
	if r.Claim.Send == phaseSent {
		// Recovery of a lost claim response pins only the exact object,
		// and only the UID a complete 201 showed, if one did.
		c, err := b.draReadClaim(ctx, &r)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			return backend.Status{}, b.draHoldRead(r, "claim", err)
		default:
			if r, err = d.pin(id, false, r.Claim.Nonce, string(c.UID)); err != nil {
				return backend.Status{}, b.draHoldRead(r, "claim", err)
			}
		}
	}
	if r.Claim.Send == phasePinned && r.Claim.Delete == "" {
		c, err := b.draReadClaim(ctx, &r)
		switch {
		case apierrors.IsNotFound(err):
			r, err = b.draHold(r, "kubernetes dra claim missing")
		case err != nil:
			return backend.Status{}, b.draHoldRead(r, "claim", err)
		default:
			if _, foreign := draConsumers(c, &r); foreign {
				r, err = b.draHold(r, "kubernetes dra claim has a foreign consumer")
			}
		}
		if err != nil {
			return backend.Status{}, err
		}
	}
	switch r.Pod.Send {
	case draWithdrawn:
		return backend.Status{State: backend.Failed, Reason: "kubernetes dra pod was never created"}, nil
	case phaseRejected:
		return backend.Status{State: backend.Failed, Reason: "kubernetes dra pod create was rejected"}, nil
	case phaseRefused:
		return backend.Status{State: backend.Unknown, Reason: "kubernetes dra pod create response refused"}, nil
	case phaseIntent:
		return backend.Status{State: backend.Starting, Reason: "kubernetes dra create pending"}, nil
	case phaseSent:
		p, err := b.draReadPod(ctx, &r)
		if apierrors.IsNotFound(err) {
			return backend.Status{State: backend.Starting, Reason: "kubernetes dra pod create outcome uncertain"}, nil
		}
		if err != nil {
			return backend.Status{}, b.draHoldRead(r, "pod", err)
		}
		if r, err = d.pin(id, true, r.Pod.Nonce, string(p.UID)); err != nil {
			return backend.Status{}, b.draHoldRead(r, "pod", err)
		}
	}
	p, err := b.draReadPod(ctx, &r)
	if apierrors.IsNotFound(err) {
		if r.Pod.Delete == "" {
			if r, err = b.draHold(r, "kubernetes dra pod missing"); err != nil {
				return backend.Status{}, err
			}
		}
		return draWitnessed(r, backend.Status{State: backend.Unknown, Instance: r.Pod.UID, Reason: "kubernetes dra pod missing"}), nil
	}
	if err != nil {
		return backend.Status{}, b.draHoldRead(r, "pod", err)
	}
	status := backend.Status{State: backend.Starting, Instance: r.Pod.UID}
	if r.Execution != "" {
		// The outcome is witnessed once; a later read, such as of the Pod
		// during its API cleanup deletion, does not change it.
		return draWitnessed(r, status), nil
	}
	switch p.Status.Phase {
	case corev1.PodRunning:
		status.State = backend.Running
	case corev1.PodPending, "":
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
				status.Reason = c.Reason + ": " + c.Message
			}
		}
	case corev1.PodSucceeded:
		if !draSucceeded(p) {
			if r, err = b.draHold(r, "kubernetes dra pod success is not ordinary"); err != nil {
				return backend.Status{}, err
			}
			status.State, status.Reason = backend.Unknown, "kubernetes dra pod success is not ordinary"
			break
		}
		if r, err = b.draOutcome(r, draExecSucceeded, int32ptr(0)); err != nil {
			return backend.Status{}, err
		}
	case corev1.PodFailed:
		// Generic Failed, including a phase PodGC writes without the kubelet,
		// proves neither stopped work nor device release.
		var exit *int32
		if len(p.Status.ContainerStatuses) == 1 && p.Status.ContainerStatuses[0].Name == "task" {
			if t := p.Status.ContainerStatuses[0].State.Terminated; t != nil {
				exit = int32ptr(t.ExitCode)
			}
		}
		if r, err = b.draOutcome(r, draExecFailed, exit); err != nil {
			return backend.Status{}, err
		}
		if r, err = b.draHold(r, "kubernetes dra pod failed; stopped work and release unproven"); err != nil {
			return backend.Status{}, err
		}
	default:
		status.State, status.Reason = backend.Unknown, "kubernetes dra pod state unknown"
	}
	return draWitnessed(r, status), nil
}

// draOutcome records the first witnessed execution outcome of r.
func (b *Backend) draOutcome(r draRecord, outcome string, exit *int32) (draRecord, error) {
	if r.Execution != "" {
		return r, nil
	}
	now := b.dra.now().UTC().Format(time.RFC3339Nano)
	return b.dra.store.update(r.Attempt, func(x *draRecord) error {
		if x.Execution == "" {
			x.Execution, x.ExitCode, x.WitnessedAt = outcome, exit, now
		}
		return nil
	})
}

// draWitnessed returns the recorded execution outcome of r, if any, in place
// of the current observation s. A delivered cancellation is reported as a
// fact beside s, never as its outcome: a missing Pod without a witnessed
// terminal state stays Unknown (backend.Backend's Observe contract), since
// its deletion proves neither stopped work nor release.
func draWitnessed(r draRecord, s backend.Status) backend.Status {
	switch r.Execution {
	case draExecSucceeded:
		return backend.Status{State: backend.Succeeded, ExitCode: int32ptr(0), Instance: r.Pod.UID}
	case draExecFailed:
		if r.Cancel != "" {
			return backend.Status{State: backend.Canceled, Instance: r.Pod.UID, Reason: "canceled by operator"}
		}
		return backend.Status{State: backend.Failed, ExitCode: r.ExitCode, Instance: r.Pod.UID, Reason: "kubernetes dra pod failed"}
	}
	if s.State == backend.Unknown && r.Cancel == stepDelivered {
		s.Reason += "; cancellation was delivered"
	}
	return s
}

// cancelDRA cancels the attempt of ref. Before the Pod grant is consumed it
// withdraws it, so that no Pod is ever sent. With a pinned Pod that has not
// finished it records cancellation intent and sends one UID-preconditioned
// graceful deletion of the Pod, after rechecking the namespace and class and
// then admitting it against the current record (admit), and records its
// delivery. Drift refuses the deletion and is a HOLD; any HOLD of the
// attempt, a failed original Close (draHoldClose) included, refuses it as
// well and is left for the operator. It never deletes the claim: a canceled
// attempt keeps its claim in HOLD.
func (b *Backend) cancelDRA(ctx context.Context, ref backend.Reference) error {
	d := b.dra
	if d == nil {
		return errDRAUnavailable
	}
	ctx, f, err := d.operation(ctx, ref.ID, false)
	if err != nil {
		return err
	}
	defer f.end()
	release, err := d.store.s.operate(ctx)
	if err != nil {
		return err
	}
	f.hold(release)
	r, err := b.draRecordFor(ref)
	if err != nil {
		return err
	}
	switch r.Pod.Send {
	case phaseIntent:
		return b.draWithdraw(r.Attempt, nil)
	case draWithdrawn, phaseRejected:
		return nil
	case phaseSent, phaseRefused:
		return fmt.Errorf("%w: kubernetes dra pod instance is unresolved", backend.ErrIdentityMismatch)
	}
	p, err := b.draReadPod(ctx, &r)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return b.draHoldRead(r, "pod", err)
	}
	// An observed terminal outcome wins over a later cancellation.
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return nil
	}
	step := func(s string) func(*draRecord) error {
		return func(x *draRecord) error {
			if x.Pod.Send != phasePinned || x.Pod.UID != r.Pod.UID {
				return fmt.Errorf("%w: kubernetes dra record differs", backend.ErrIdentityMismatch)
			}
			if x.Cancel != stepDelivered {
				x.Cancel = s
			}
			return nil
		}
	}
	// Any HOLD of the attempt, a failed original Close in particular,
	// refuses the deletion. This early check of r saves the rechecks; the
	// admission below decides on the current record.
	if err := d.authority(r); err != nil {
		return err
	}
	if err := b.draRecheck(ctx, &r); err != nil {
		return b.draHoldRead(r, "namespace or device class", err)
	}
	_, c, cctx, err := d.admit(ctx, r.Attempt, step(stepIntent))
	if err != nil {
		return err
	}
	uid := types.UID(r.Pod.UID)
	delivered, err := c.send(cctx, func(ctx context.Context) error {
		return b.client.CoreV1().Pods(b.namespace).Delete(ctx, r.Pod.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	})
	if delivered {
		// Truth of delivery, recorded even if the original Close is held
		// or failed, or a HOLD was recorded since the admission.
		if _, serr := d.store.update(r.Attempt, step(stepDelivered)); serr != nil {
			return errors.Join(err, serr)
		}
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// logsDRA reads the bounded log prefix of the attempt's pinned Pod, as
// receipted retry logs do: one log request in a log custody of the DRA
// scope, whose body is closed, positively, before the final checks.
func (b *Backend) logsDRA(ctx context.Context, ref backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
	if offset < 0 || limit <= 0 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("invalid log offset or limit: %w", fs.ErrInvalid)
	}
	d := b.dra
	if d == nil {
		return backend.LogChunk{}, errDRAUnavailable
	}
	ctx, f, err := d.operation(ctx, ref.ID, false)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer f.end()
	release, err := d.store.s.operate(ctx)
	if err != nil {
		return backend.LogChunk{}, err
	}
	f.hold(release)
	if _, err := receiptHTTP(b.client.CoreV1().RESTClient()); err != nil {
		return backend.LogChunk{}, err
	}
	custody, err := admitLogs(d.store.s, ref.ID)
	if err != nil {
		return backend.LogChunk{}, err
	}
	defer custody.settle()
	r, err := b.draRecordFor(ref)
	if err != nil {
		return backend.LogChunk{}, err
	}
	if r.Pod.Send != phasePinned {
		return backend.LogChunk{}, fmt.Errorf("%w: kubernetes dra pod instance is unresolved", backend.ErrIdentityMismatch)
	}
	p, err := b.draReadPod(ctx, &r)
	if apierrors.IsNotFound(err) {
		return backend.LogChunk{}, fmt.Errorf("dra pod logs are no longer retained: %w", fs.ErrNotExist)
	}
	if err != nil {
		return backend.LogChunk{}, err
	}
	if _, err := b.draRecordFor(ref); err != nil {
		return backend.LogChunk{}, err
	}
	data, err := b.receiptedLogs(ctx, custody, p.Name)
	if err != nil {
		return backend.LogChunk{}, err
	}
	// Pod logs have no UID precondition: the record and the exact Pod are
	// checked again before any byte is released.
	if _, err := b.draRecordFor(ref); err != nil {
		return backend.LogChunk{}, err
	}
	if _, err := b.draReadPod(ctx, &r); err != nil {
		return backend.LogChunk{}, err
	}
	truncated := len(data) > backend.MaxLogBytes
	if truncated {
		data = data[:backend.MaxLogBytes]
	}
	if offset > int64(len(data)) {
		return backend.LogChunk{}, fmt.Errorf("log offset exceeds retained prefix: %w", fs.ErrInvalid)
	}
	end := min(int64(len(data)), offset+int64(limit))
	complete := p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed
	return backend.LogChunk{Data: data[offset:end], NextOffset: end, Complete: (complete || truncated) && end == int64(len(data)), Truncated: truncated}, nil
}
