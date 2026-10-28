package environmentcontrol

import (
	"time"

	"github.com/tmc/skygo/internal/store"
)

// SetStoreFaults replaces the Store reads and commits that c's metadata
// updates use. It must be called before any other use of c.
func SetStoreFaults(c *Controller, read func(id string) (store.Environment, error), commit func(id string, revision uint64, next store.EnvironmentCommit) (store.Environment, error)) {
	c.read, c.commit = read, commit
}

// LifecycleBusy reports whether a Reconcile pass or a session admission of c
// holds the lifecycle gate.
func LifecycleBusy(c *Controller) bool {
	select {
	case c.gate <- struct{}{}:
		<-c.gate
		return false
	default:
		return true
	}
}

// SetBeforeGate makes every later session admission of c call f just before
// it waits for the lifecycle gate. It must not be called while an admission
// of c runs.
func SetBeforeGate(c *Controller, f func()) {
	c.beforeGate = f
}

// SetMaterializeFaults replaces the Store calls that c's artifact frames
// make. It must be called before any other use of c.
func SetMaterializeFaults(c *Controller,
	begin func(id string, revision, seq uint64, incarnation string, ref store.ArtifactRef, producer store.OriginalServerLifetime, now time.Time) (store.ArtifactLease, error),
	mark func(store.ArtifactDownloadClosure) (store.ArtifactDownloadClosure, error),
	finish func(store.ArtifactDownloadClosure, time.Time) error) {
	c.begin, c.mark, c.finish = begin, mark, finish
}

// SetMaterializeRetry sets how long c's artifact frames wait before they call
// the Store again. It must be called before any other use of c.
func SetMaterializeRetry(c *Controller, d time.Duration) {
	c.retry = d
}

// HoldsArtifacts reports whether c holds anything of a materialization of
// environment id, as its lifecycle sees it.
func HoldsArtifacts(c *Controller, id string) bool {
	return c.holdsArtifacts(id)
}

// FrameIncarnation returns the incarnation and sequence of c's frame of
// environment id, and false if c has none.
func FrameIncarnation(c *Controller, id string) (string, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.materializing[id]
	if f == nil {
		return "", 0, false
	}
	return f.e.Incarnation, f.e.IncarnationSeq, true
}

// KeptLeases returns the leases c's frame of environment id keeps on paths
// that leave a file possibly open, in the order they were kept.
func KeptLeases(c *Controller, id string) []store.ArtifactLease {
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.materializing[id]
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.ArtifactLease
	for _, g := range f.groups {
		for _, k := range g.kept {
			if l, ok := k.(store.ArtifactLease); ok {
				out = append(out, l)
			}
		}
	}
	return out
}
