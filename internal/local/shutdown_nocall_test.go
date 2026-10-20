//go:build linux

package local

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func TestShutdownNoCallWaitsForOriginalRelease(t *testing.T) {
	f := newWorkspaceFixture(t)
	b := f.backend
	entered, resume, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	var releases atomic.Int32
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	if err := b.BindWorkspace(f.binding, func() {
		releases.Add(1)
		close(entered)
		<-resume
	}); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	w := b.workspaces.bindings[f.binding.AttemptID]
	b.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go func() {
		result <- b.Start(ctx, f.binding.AttemptID, f.binding.Task)
		close(returned)
	}()
	// Register only after launching this actual original call. Cleanup runs
	// before the fixture can close borrowed Files/Root or remove temporary data.
	// Only the original returned channel permits teardown; a test timeout does not.
	t.Cleanup(func() {
		unblock()
		<-returned
	})
	awaitDefault(t, entered, "original unused callback entry")
	b.mu.Lock()
	settled, released, attempted := w.noCallSettled, w.initialOwnerReleased, w.startAttempted
	parent, jobs := w.parent, len(b.jobs)
	b.mu.Unlock()
	if settled || released || !attempted || parent != nil || jobs != 0 {
		t.Fatalf("blocked callback published completion: settled=%v released=%v attempted=%v parent=%p jobs=%d", settled, released, attempted, parent, jobs)
	}
	select {
	case <-returned:
		t.Fatal("Start returned before its original callback")
	default:
	}
	if err := b.Start(context.Background(), f.binding.AttemptID, f.binding.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("second Start while callback blocked: %v", err)
	}
	unblock()
	awaitDefault(t, returned, "original canceled Start return")
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start: %v", err)
	}
	b.mu.Lock()
	settled, released = w.noCallSettled, w.initialOwnerReleased
	frontier := workspaceNoCallFrontier(w)
	noReceipt := w.observation.Writer == nil && !w.observation.Ready && w.parent == nil && len(b.jobs) == 0
	b.mu.Unlock()
	if !settled || !released || !frontier || !noReceipt || releases.Load() != 1 {
		t.Fatalf("original no-call settlement: settled=%v released=%v frontier=%v noReceipt=%v releases=%d", settled, released, frontier, noReceipt, releases.Load())
	}
	if err := b.Start(context.Background(), f.binding.AttemptID, f.binding.Task); !errors.Is(err, backend.ErrIdentityMismatch) {
		t.Fatalf("second Start after callback returned: %v", err)
	}
}

func TestShutdownNoCallMissingOriginalReleaseStaysHeld(t *testing.T) {
	f := newWorkspaceFixture(t)
	b := f.backend
	if err := b.BindWorkspace(f.binding, func() {}); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	w := b.workspaces.bindings[f.binding.AttemptID]
	// Deliberate protocol corruption, with no resource/permit in this callback:
	// nil must never be interpreted as an original callback return receipt.
	w.release = nil
	b.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Start(ctx, f.binding.AttemptID, f.binding.Task); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.noCallSettled || w.initialOwnerReleased || !w.startAttempted || w.parent != nil || len(b.jobs) != 0 {
		t.Fatal("missing original callback gained a no-call receipt")
	}
}
