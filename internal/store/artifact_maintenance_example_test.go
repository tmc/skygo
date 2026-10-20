package store

import (
	"errors"
	"fmt"
	"os"
)

// A dormant database offers no artifact cleanup authority. These are actual
// Store calls, not a private metadata fixture or invented filesystem receipt.
// This runnable example is UNRUN under the S06 execution hold.
func ExampleStore_NextArtifactCleanup() {
	dir, err := os.MkdirTemp("", "skygo-cleanup-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()
	_, found, err := s.NextArtifactCleanup("")
	fmt.Println(found, errors.Is(err, ErrArtifactsDisabled))
	_, err = s.FinishArtifactDelete(ArtifactDeleteWork{}, ArtifactDeleteReceipt{})
	fmt.Println(errors.Is(err, ErrArtifactsDisabled))
	err = s.FinishWorkspaceCleanup(WorkspaceCleanupWork{}, WorkspaceCleanupReceipt{})
	fmt.Println(errors.Is(err, ErrArtifactsDisabled))
	// Output:
	// false true
	// true
	// true
}

// Real completion ownership is supplied by the original concrete coordinator.
// This workflow does not manufacture a physical proof or run during examples.
// HandoffWorkspaceCleanup intentionally has no Store Finish call: it keeps the
// committed work and every durable reservation for later exact-root cleanup.
func ExampleStore_FinishWorkspaceCleanup() {
	finish := func(s *Store, committed WorkspaceCommit, removeAndSync func(WorkspaceCleanupWork) (WorkspaceCleanupReceipt, error)) error {
		if committed.State != WorkspaceCommitMarked || committed.Cleanup == nil {
			return ErrArtifactConflict
		}
		work := *committed.Cleanup
		receipt, err := removeAndSync(work)
		if err != nil {
			return err
		}
		return s.FinishWorkspaceCleanup(work, receipt)
	}
	_ = finish // Atomic Complete supplied work; the caller owns actual unlink/Sync.
}

// A nil error need not mean an object was retired: at most64 closed download
// receipts are released in one transaction. The sole fair worker reselects a
// remaining exact intent on a later quantum instead of crediting its capacity.
func ExampleStore_FinishArtifactDelete() {
	finish := func(s *Store, original ArtifactDeleteWork, receipt ArtifactDeleteReceipt) (bool, error) {
		return s.FinishArtifactDelete(original, receipt)
	}
	_ = finish // receipt must come from the original joined Files operation.
}
