package kube

import (
	"context"
	"errors"
	"fmt"
	"io/fs"

	"github.com/tmc/skygo/internal/backend"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// claimRetry serializes retirement against actual native operation ownership.
// Entries exist only while owned; durable attempt IDs are never reused.
func (b *Backend) claimRetry(id string, exclusive bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retryOwners == nil {
		b.retryOwners = make(map[string]int)
	}
	n, exists := b.retryOwners[id]
	if n < 0 || exclusive && n != 0 {
		return errors.New("kubernetes execution operation pending")
	}
	if !exists && len(b.retryOwners) >= maxRetryReceipts {
		return errors.New("kubernetes operation ownership limit reached")
	}
	if exclusive {
		b.retryOwners[id] = -1
	} else {
		b.retryOwners[id] = n + 1
	}
	return nil
}

func (b *Backend) releaseRetry(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.retryOwners[id]
	if n <= 1 {
		delete(b.retryOwners, id)
	} else {
		b.retryOwners[id] = n - 1
	}
}

func (b *Backend) claimRetryReference(ref backend.Reference) error {
	if _, _, err := b.retryIdentity(ref); err != nil {
		return err
	}
	return b.claimRetry(ref.ID, false)
}

// removeExecution is called only for an R1-validated durable NativeExecution
// intent carrying accepted positive stopped evidence. The trusted GC caller owns
// that precondition; Reference is not an inbound proof grant. Missing after that
// intent is cleanup absence, never a new stopped observation or retry permission.
func (b *Backend) removeExecution(ctx context.Context, ref backend.Reference) error {
	if ref.Task.Retry == nil {
		return fmt.Errorf("job execution cleanup unavailable: %w", fs.ErrNotExist)
	}
	if !validInstance(ref.Instance) {
		return fmt.Errorf("%w: cleanup requires a pinned pod instance", backend.ErrIdentityMismatch)
	}
	if b.receipts != nil {
		// Cleanup excludes every native operation of the scope, in any
		// Backend or process, and never waits for one.
		release, err := b.receipts.exclusive()
		if err != nil {
			return err
		}
		defer release()
	}
	uid, hash, err := b.retryIdentity(ref)
	if err != nil {
		return err
	}
	if err := b.claimRetry(ref.ID, true); err != nil {
		return err
	}
	defer b.releaseRetry(ref.ID)
	canceled, err := b.retryCanceled(ref.ID)
	if err != nil {
		return err
	}
	if canceled {
		return fmt.Errorf("canceled execution cleanup unavailable: %w", fs.ErrNotExist)
	}
	p, _, err := b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return b.retireRetry(ref.ID, uid, hash)
	}
	if err != nil {
		return err
	}
	if !stoppedRetry(p, ref.Task) {
		return fmt.Errorf("pod stopped receipt unavailable: %w", fs.ErrNotExist)
	}
	again, _, err := b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return b.retireRetry(ref.ID, uid, hash)
	}
	if err != nil {
		return err
	}
	if !stoppedRetry(again, ref.Task) || !sameTermination(p, again) {
		return fmt.Errorf("pod stopped receipt changed: %w", fs.ErrNotExist)
	}
	if b.receipts != nil {
		// Cleanup intent is durable before the deletion is sent.
		if _, err := b.receipts.update(ref.ID, func(x *executionReceipt) error {
			if x.Phase != phasePinned || x.UID != uid || x.TaskSHA256 != hash {
				return fmt.Errorf("%w: cleanup receipt differs", backend.ErrIdentityMismatch)
			}
			x.Cleanup = stepIntent
			return nil
		}); err != nil {
			return err
		}
	}
	expected := types.UID(uid)
	err = b.client.CoreV1().Pods(b.namespace).Delete(ctx, p.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &expected}})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	_, _, err = b.getRetry(ctx, ref)
	if apierrors.IsNotFound(err) {
		return b.retireRetry(ref.ID, uid, hash)
	}
	if err != nil {
		return err
	}
	return errors.New("kubernetes execution deletion remains pending")
}

// retireRetry releases the attempt's receipt. A durable receipt becomes a
// retired tombstone only after the same proof the volatile receipt requires;
// the tombstone authorizes nothing and is never reclaimed.
func (b *Backend) retireRetry(id, uid, hash string) error {
	if b.receipts != nil {
		return b.receipts.retire(id, func(x *executionReceipt) error {
			if x.Phase != phasePinned || x.UID != uid || x.TaskSHA256 != hash || x.Cancel != "" || len(x.Objects) != 0 {
				return fmt.Errorf("%w: cleanup receipt differs", backend.ErrIdentityMismatch)
			}
			return nil
		})
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if r := b.retries[id]; r != nil {
		if r.uid != uid || r.hash != hash {
			return fmt.Errorf("%w: cleanup receipt differs", backend.ErrIdentityMismatch)
		}
		delete(b.retries, id)
	}
	return nil
}
