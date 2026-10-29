package kube

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
)

// draSucceededAttempt starts attempt testID, MODELS its ordinary success and
// lets Observe witness it.
func draSucceededAttempt(t *testing.T, f *draFixture, b *Backend) {
	t.Helper()
	ctx := context.Background()
	if err := b.Start(ctx, testID, draTask(1)); err != nil {
		t.Fatal(err)
	}
	f.api.succeed(testID)
	if s, err := b.Observe(ctx, draRef(b, testID, draTask(1))); err != nil || s.State != backend.Succeeded {
		t.Fatalf("status = %+v %v", s, err)
	}
}

func claimExists(f *draFixture) bool {
	found := false
	f.api.claim(testID, func(*resourcev1.ResourceClaim) { found = true })
	return found
}

func TestDRAAPICleanupAfterSuccess(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	draSucceededAttempt(t, f, b)
	// Log retention has not expired: nothing is deleted and logs remain.
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.api.deleted()) != 0 {
		t.Fatal("deleted before log retention expired")
	}
	if _, err := b.Logs(ctx, draRef(b, testID, draTask(1)), 0, 10); err != nil {
		t.Fatalf("logs: %v", err)
	}
	// The built-in finalizer keeps the deleted claim until the MODELED
	// claim controller removes it.
	f.api.claim(testID, func(c *resourcev1.ResourceClaim) { c.Finalizers = []string{resourcev1.Finalizer} })
	f.now = f.now.Add(2 * time.Hour)
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	r := draRecordOf(t, b, testID)
	deletes := f.api.deleted()
	if !r.Pod.Gone || r.Claim.Delete != stepDelivered || r.Claim.Gone || r.State != draOpen || len(deletes) != 2 {
		t.Fatalf("record = %+v, deletes %d", r, len(deletes))
	}
	for i, uid := range []string{r.Pod.UID, r.Claim.UID} {
		o := deletes[i]
		if o.Preconditions == nil || o.Preconditions.UID == nil || string(*o.Preconditions.UID) != uid ||
			o.GracePeriodSeconds != nil || o.PropagationPolicy != nil {
			t.Fatalf("delete %d options = %+v", i, o)
		}
	}
	// No finalizer is removed by Skygo: the claim waits.
	f.api.claim(testID, func(c *resourcev1.ResourceClaim) {
		if len(c.Finalizers) != 1 || c.DeletionTimestamp == nil {
			t.Errorf("claim = %+v", c.ObjectMeta)
		}
	})
	f.api.set(func() {
		for name := range f.api.claims {
			delete(f.api.claims, name) // MODELED controller: deallocated, finalizer removed
		}
	})
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b, testID); r.State != draRetired || !r.Claim.Gone {
		t.Fatalf("record = %+v", r)
	}
	// The tombstone keeps the outcome and grants nothing.
	s, err := b.Observe(ctx, draRef(b, testID, draTask(1)))
	if err != nil || s.State != backend.Succeeded || s.Reason != "kubernetes dra api cleanup complete" {
		t.Fatalf("status = %+v %v", s, err)
	}
	if err := b.Start(ctx, testID, draTask(1)); !errors.Is(err, errReceiptExists) {
		t.Fatalf("start after tombstone: %v", err)
	}
	if f.api.count("resourceclaims") != 1 || f.api.count("pods") != 1 || len(f.api.deleted()) != 2 {
		t.Fatal("tombstone authorized more requests")
	}
}

func TestDRACleanupWaitsForReservation(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	draSucceededAttempt(t, f, b)
	r := draRecordOf(t, b, testID)
	ours := resourcev1.ResourceClaimConsumerReference{Resource: "pods", Name: r.Pod.Name, UID: types.UID(r.Pod.UID)}
	f.api.claim(testID, func(c *resourcev1.ResourceClaim) {
		c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{ours}
	})
	f.now = f.now.Add(2 * time.Hour)
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b, testID); !r.Pod.Gone || r.Claim.Delete != "" || len(f.api.deleted()) != 1 {
		t.Fatalf("claim deleted with our reservation: %+v", r)
	}
	f.api.claim(testID, func(c *resourcev1.ResourceClaim) { c.Status.ReservedFor = nil })
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b, testID); r.State != draRetired {
		t.Fatalf("record = %+v", r)
	}
}

// TestDRACleanupCounterexamples checks that no observation other than a
// witnessed ordinary success of the exact Pod leads to deletion, even after
// log retention, and that none of them is repaired later.
func TestDRACleanupCounterexamples(t *testing.T) {
	running := func(a *draAPI) { a.pod(testID, func(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }) }
	for _, tc := range []struct {
		name  string
		model func(a *draAPI)
	}{
		// MODELED PodGC: Failed without the kubelet, then deletion.
		{"podgc failed", func(a *draAPI) {
			a.pod(testID, func(p *corev1.Pod) { p.Status.Phase, p.Status.Reason = corev1.PodFailed, "" })
		}},
		{"pod 404", func(a *draAPI) { a.set(func() { delete(a.pods, "skygo-"+testID) }) }},
		// Allocation cleared and no reservation while the Pod still runs.
		{"allocation nil", func(a *draAPI) {
			running(a)
			a.claim(testID, func(c *resourcev1.ResourceClaim) { c.Status = resourcev1.ResourceClaimStatus{} })
		}},
		// MODELED kubelet: unprepare keeps failing, so containers stopped but
		// no terminal phase is published (kubelet.go:2275-2289).
		{"unprepare error", func(a *draAPI) {
			a.pod(testID, func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodRunning
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "task",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, ContainerID: "containerd://abc"}}}}
			})
		}},
		{"timeout then later allocation", func(a *draAPI) {
			running(a)
			a.claim(testID, func(c *resourcev1.ResourceClaim) {
				c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "next", UID: "next-uid"}}
			})
		}},
		{"foreign consumer after success", func(a *draAPI) {
			a.succeed(testID)
			a.claim(testID, func(c *resourcev1.ResourceClaim) {
				c.Status.ReservedFor = []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: "other", UID: "other"}}
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDRAFixture(t)
			_, b := f.integration(t)
			ctx := context.Background()
			if err := b.Start(ctx, testID, draTask(1)); err != nil {
				t.Fatal(err)
			}
			tc.model(f.api)
			f.now = f.now.Add(48 * time.Hour)
			for range 2 {
				b.maintainDRA(ctx)
			}
			if len(f.api.deleted()) != 0 {
				t.Fatal("counterexample deleted an object")
			}
			if r := draRecordOf(t, b, testID); r.Execution == draExecSucceeded && r.Hold == "" {
				t.Fatalf("counterexample witnessed an ordinary success: %+v", r)
			}
			if !claimExists(f) {
				t.Fatal("claim removed")
			}
		})
	}
	t.Run("zero retention", func(t *testing.T) {
		f := newDRAFixture(t)
		_, b := f.integration(t)
		b.dra.retention = 0
		draSucceededAttempt(t, f, b)
		f.now = f.now.Add(1000 * time.Hour)
		if err := b.maintainDRA(context.Background()); err != nil || len(f.api.deleted()) != 0 {
			t.Fatalf("zero retention cleaned up: %v", err)
		}
	})
}

// TestDRACleanupWaitsForOriginals checks that an operation of the scope in
// another process, modeled by a second store holding the shared permit,
// forbids retirement until it returns.
func TestDRACleanupWaitsForOriginals(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	draSucceededAttempt(t, f, b)
	f.now = f.now.Add(2 * time.Hour)
	other := restartedStore(b.dra.store.s)
	release, err := other.operate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.api.deleted()) != 0 {
		release()
		t.Fatal("cleanup ran while an original operation was pending")
	}
	release()
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b, testID); r.State != draRetired {
		t.Fatalf("positive control: %+v", r)
	}
}

// TestDRAMaintenanceRestart checks that a new process resumes custody from
// the record alone, without any core Observe: a recorded Pod deletion whose
// Pod was still terminating, and a Pod create whose reply was lost.
func TestDRAMaintenanceRestart(t *testing.T) {
	f := newDRAFixture(t)
	_, b := f.integration(t)
	ctx := context.Background()
	draSucceededAttempt(t, f, b)
	f.api.set(func() { f.api.keepDeleted["pods"] = true })
	f.now = f.now.Add(2 * time.Hour)
	if err := b.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b, testID); r.Pod.Delete != stepDelivered || r.Pod.Gone {
		t.Fatalf("record = %+v", r)
	}
	// Lost Pod reply of a second attempt.
	f.api.set(func() { f.api.drop["pods"] = true })
	if err := b.Start(ctx, otherID, draTask(1)); err == nil {
		t.Fatal("lost reply reported success")
	}
	f.api.set(func() {
		f.api.drop["pods"] = false
		delete(f.api.pods, "skygo-"+testID) // MODELED: graceful termination completed
	})
	_, b2 := f.integration(t)
	b2.dra.now = func() time.Time { return f.now }
	if err := b2.maintainDRA(ctx); err != nil {
		t.Fatal(err)
	}
	if r := draRecordOf(t, b2, testID); r.State != draRetired {
		t.Fatalf("first attempt = %+v", r)
	}
	if r := draRecordOf(t, b2, otherID); r.Pod.Send != phasePinned {
		t.Fatalf("second attempt = %+v", r)
	}
	if f.api.count("pods") != 2 || f.api.count("resourceclaims") != 2 {
		t.Fatal("maintenance created objects")
	}
}
