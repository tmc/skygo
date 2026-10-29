package kube

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// maintainDRA runs one bounded maintenance pass over every record of the
// Backend's DRA scope, including attempts the core no longer observes
// (terminal or Unknown) and those of earlier processes. For each open record
// it reconciles exactly as Observe does, under a shared operation permit, and
// then tries API cleanup under the exclusive permit. Errors of one record do
// not stop the pass; they are returned joined, and the next pass retries.
func (b *Backend) maintainDRA(ctx context.Context) error {
	d := b.dra
	ctx, f, err := d.operation(ctx, "", false)
	if err != nil {
		return err
	}
	defer f.end()
	ids, err := d.store.list()
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		if ctx.Err() != nil {
			break
		}
		pass, cancel := context.WithTimeout(ctx, draPassTimeout)
		pass, rf := f.child(pass, id)
		err := b.maintainDRARecord(pass, rf, id)
		rf.end()
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("kubernetes dra attempt %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// maintainDRARecord maintains record id in frame f. A permit whose calls'
// originals are still held stays in f, and so is not released, until they
// finish.
func (b *Backend) maintainDRARecord(ctx context.Context, f *draFrame, id string) error {
	d := b.dra
	rctx, rf := f.child(ctx, id)
	release, err := d.store.s.operate(rctx)
	if err != nil {
		rf.end()
		return err
	}
	rf.hold(release)
	r, err := d.store.load(id)
	if err == nil && r.State == draOpen {
		_, err = b.draReconcile(rctx, r)
	}
	rf.end()
	held, failed := d.orig.state(id)
	if held {
		return errors.Join(err, errDRAHeld) // no cleanup while an original is held
	}
	if failed != nil {
		// Its original recorded the HOLD (draHoldClose), or kept its permits.
		if err == nil {
			err = failed
		}
		return err
	}
	if errors.Is(err, errReceiptRefused) {
		return nil // HOLD: a refused response authorizes nothing
	}
	if err != nil {
		return err
	}
	return b.draCleanup(ctx, f, id)
}

// draEligible reports whether record r may proceed with API cleanup at now:
// a witnessed ordinary success of the exact Pod, both objects pinned,
// without HOLD or cancellation, and the configured log retention expired.
// A zero retention retains every succeeded Pod and claim.
func (d *draRuntime) draEligible(r draRecord, now time.Time) bool {
	if r.State != draOpen || r.Execution != draExecSucceeded || r.Hold != "" || r.Cancel != "" ||
		r.Claim.Send != phasePinned || r.Pod.Send != phasePinned || d.retention <= 0 {
		return false
	}
	at, err := time.Parse(time.RFC3339Nano, r.WitnessedAt)
	return err == nil && !now.Before(at.Add(d.retention))
}

// draCleanup performs API cleanup of an eligible record, at most one step
// sequence per call, holding the exclusive operation permit: if any original
// operation of the scope runs, in any Backend or process, or a log body of it
// was kept after an uncertain Close, it does nothing. Each deletion is
// recorded as intent first and delivered after its response, is
// preconditioned on the pinned UID, and is graceful: no grace period is set,
// so the Pod's own applies. It deletes the exact Pod first and, once a read
// finds it absent, the exact claim; once a read finds that absent too, the
// record is retired as a tombstone. It never removes a finalizer, clears
// status, forces deletion or creates anything. Absence is API cleanup only.
// Before each deletion it rechecks the namespace and class: drift refuses
// the deletion and is a HOLD; a transient read error only defers it.
func (b *Backend) draCleanup(ctx context.Context, f *draFrame, id string) error {
	d := b.dra
	r, err := d.store.load(id)
	if err != nil || !d.draEligible(r, d.now()) {
		return ignoreRefused(err)
	}
	release, err := d.store.s.exclusive()
	if errors.Is(err, errOperationBusy) {
		return nil
	}
	if err != nil {
		return err
	}
	// The exclusive permit stays with the record's frame until every
	// original of its deletions and reads has finished.
	ctx, cf := f.child(ctx, id)
	defer cf.end()
	cf.hold(release)
	// Reload under the exclusive permit: nothing else changes it now.
	if r, err = d.store.load(id); err != nil || !d.draEligible(r, d.now()) {
		return ignoreRefused(err)
	}
	// Any HOLD fences the attempt, in every process, and so does a failed
	// original Close here, whose HOLD may not have been written. Each
	// deletion is then admitted against the current record (admit).
	if err := d.authority(r); err != nil {
		return err
	}
	if !r.Pod.Gone {
		if r, err = b.draCleanupPod(ctx, r); err != nil || !r.Pod.Gone || r.Hold != "" {
			return err
		}
	}
	if r, err = b.draCleanupClaim(ctx, r); err != nil || !r.Claim.Gone || r.Hold != "" {
		return err
	}
	_, err = d.store.update(id, func(x *draRecord) error {
		if err := d.authority(*x); err != nil {
			return err
		}
		if !x.Pod.Gone || !x.Claim.Gone {
			return errors.New("kubernetes dra cleanup incomplete")
		}
		x.State = draRetired
		return nil
	})
	return err
}

func ignoreRefused(err error) error {
	if errors.Is(err, errReceiptRefused) {
		return nil
	}
	return err
}

// draCleanupStep records step on the Pod's or claim's deletion, or Gone.
func (b *Backend) draCleanupStep(r draRecord, pod bool, step string) (draRecord, error) {
	return b.dra.store.update(r.Attempt, cleanupStep(r, pod, step))
}

// draCleanupDelete, after the caller's namespace and class rechecks, records
// the intent of the Pod's or claim's deletion and admits it against the
// current record (admit), then sends it once, preconditioned on the pinned
// UID, and records its delivery.
func (b *Backend) draCleanupDelete(ctx context.Context, r draRecord, pod bool) (draRecord, error) {
	r, c, cctx, err := b.dra.admit(ctx, r.Attempt, cleanupStep(r, pod, stepIntent))
	if err != nil {
		return r, err
	}
	o := draObjectOf(&r, pod)
	uid := types.UID(o.UID)
	opts := metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}
	delivered, err := c.send(cctx, func(ctx context.Context) error {
		if pod {
			return b.client.CoreV1().Pods(b.namespace).Delete(ctx, o.Name, opts)
		}
		return b.client.ResourceV1().ResourceClaims(b.namespace).Delete(ctx, o.Name, opts)
	})
	if delivered {
		// Truth of delivery, recorded even under a HOLD since the admission.
		var serr error
		if r, serr = b.draCleanupStep(r, pod, stepDelivered); serr != nil {
			return r, errors.Join(err, serr)
		}
	}
	return r, err
}

func cleanupStep(r draRecord, pod bool, step string) func(*draRecord) error {
	return func(x *draRecord) error {
		o := draObjectOf(x, pod)
		if o.Send != phasePinned || o.UID != draObjectOf(&r, pod).UID {
			return fmt.Errorf("%w: kubernetes dra record differs", backend.ErrIdentityMismatch)
		}
		switch {
		case step == "gone":
			if o.Delete == "" {
				return errors.New("kubernetes dra deletion was not recorded")
			}
			o.Gone = true
		case o.Delete != stepDelivered:
			o.Delete = step
		}
		return nil
	}
}

func (b *Backend) draCleanupPod(ctx context.Context, r draRecord) (draRecord, error) {
	p, err := b.draReadPod(ctx, &r)
	switch {
	case apierrors.IsNotFound(err):
		if r.Pod.Delete == "" {
			// Absent before our deletion: not ours to explain.
			return b.draHold(r, "kubernetes dra pod disappeared before cleanup")
		}
		return b.draCleanupStep(r, true, "gone")
	case err != nil:
		return r, b.draHoldRead(r, "pod", err)
	}
	// The exact Pod still shows its witnessed success. A deletion in
	// progress is ours only after our recorded intent.
	if p.Status.Phase != corev1.PodSucceeded || p.DeletionTimestamp != nil && r.Pod.Delete == "" {
		return b.draHold(r, "kubernetes dra pod changed after success")
	}
	c, err := b.draReadClaim(ctx, &r)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return b.draHold(r, "kubernetes dra claim missing")
		}
		return r, b.draHoldRead(r, "claim", err)
	}
	if _, foreign := draConsumers(c, &r); foreign {
		return b.draHold(r, "kubernetes dra claim has a foreign consumer")
	}
	if err := b.draRecheck(ctx, &r); err != nil {
		return r, b.draHoldRead(r, "namespace or device class", err)
	}
	if r, err = b.draCleanupDelete(ctx, r, true); err != nil && !apierrors.IsNotFound(err) {
		return r, err
	}
	if _, err = b.draReadPod(ctx, &r); apierrors.IsNotFound(err) {
		return b.draCleanupStep(r, true, "gone")
	}
	return r, err // still present: graceful termination continues
}

func (b *Backend) draCleanupClaim(ctx context.Context, r draRecord) (draRecord, error) {
	c, err := b.draReadClaim(ctx, &r)
	switch {
	case apierrors.IsNotFound(err):
		if r.Claim.Delete == "" {
			return b.draHold(r, "kubernetes dra claim disappeared before cleanup")
		}
		return b.draCleanupStep(r, false, "gone")
	case err != nil:
		return r, b.draHoldRead(r, "claim", err)
	}
	own, foreign := draConsumers(c, &r)
	if foreign {
		return b.draHold(r, "kubernetes dra claim has a foreign consumer")
	}
	if own {
		return r, nil // the claim controller has not yet removed our Pod's reservation
	}
	if err := b.draRecheck(ctx, &r); err != nil {
		return r, b.draHoldRead(r, "namespace or device class", err)
	}
	if r, err = b.draCleanupDelete(ctx, r, false); err != nil && !apierrors.IsNotFound(err) {
		return r, err
	}
	if _, err = b.draReadClaim(ctx, &r); apierrors.IsNotFound(err) {
		return b.draCleanupStep(r, false, "gone")
	}
	return r, err // finalizers or deallocation still pending
}
