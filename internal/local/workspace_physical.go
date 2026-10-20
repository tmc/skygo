package local

import (
	"sort"

	"github.com/tmc/skygo/internal/artifact"
)

// WorkspacePhysical describes copied original ownership facts, not authority or
// a byte total. Zero Root/Candidate with Attempted true means unknown, not absent.
// CandidateID is the accepted result identity, not an observed basename or grant;
// it is empty only when the accepted task has no outputs.
// Transitioning forbids sampler promotion. WriterActive is interval context;
// running workloads are not excluded from physical observations.
type WorkspacePhysical struct {
	AttemptID, Binding, Instance, RootID, CandidateID string
	Parent, Root, Candidate                           artifact.FileID
	RootAttempted, CandidateAttempted                 bool
	Transitioning, Uncertain, WriterActive, Cleanup   bool
}

// PhysicalState returns at most 64 immutable ownership observations. It performs
// no filesystem calls and reads only mirrors synchronized by Backend.mu. An
// empty result is not proof that no unexplained directories exist on disk.
func (b *Backend) PhysicalState() []WorkspacePhysical {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.workspaces == nil {
		return nil
	}
	out := make([]WorkspacePhysical, 0, len(b.workspaces.bindings)+len(b.workspaces.cleanupFrames))
	for _, w := range b.workspaces.bindings {
		out = append(out, w.physical)
	}
	for _, c := range b.workspaces.cleanupFrames {
		out = append(out, c.physical)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptID < out[j].AttemptID })
	return out
}

// Begin occurs outside every owner lock, before structural mutation. A scan
// registers its invalidatable frame before its first cohort copy. This callback
// can only invalidate that frame/cache and request the existing worker's sample.
func (b *Backend) beginWorkspacePhysical(w *workspaceExecution) {
	b.mu.Lock()
	w.physical.Transitioning = true
	b.mu.Unlock()
	b.workspaces.config.PhysicalChanged()
}

// The sole sequencer copies actual original values before exposing them. No
// copier dereferences these mutable owners, including while this mutex is held.
func (b *Backend) endWorkspacePhysical(w *workspaceExecution) {
	p := WorkspacePhysical{AttemptID: w.binding.AttemptID, Binding: w.binding.Binding, RootID: w.binding.RootID, CandidateID: w.binding.ResultID, Parent: b.workspaces.root, RootAttempted: w.rootAttempted, CandidateAttempted: w.candidateAttempted, Uncertain: w.rootErr != nil || w.collectionErr != nil || w.logCloseErr != nil || w.failedPhase != nil}
	if w.rootInfo != nil {
		var err error
		p.Root, err = workspaceFileID(w.rootInfo)
		if err != nil {
			p.Uncertain = true
		}
	}
	if w.candidate != nil {
		p.Candidate = w.candidate.Description().File
	} else {
		p.Candidate = w.candidateFile.File
	}
	b.mu.Lock()
	if w.parent != nil {
		p.Instance = w.parent.status.Instance
	}
	p.WriterActive = w.phase != nil
	w.physical = p
	b.mu.Unlock()
}

// Cleanup busy admission is already reserved when called; no c.mu is held.
func (b *Backend) beginCleanupPhysical(w *workspaceExecution, c *workspaceCleanup) {
	b.mu.Lock()
	if w != nil {
		w.physical.Transitioning = true
	} else {
		c.physical.Transitioning = true
	}
	b.mu.Unlock()
	b.workspaces.config.PhysicalChanged()
}

// The cleanup caller still holds c.mu and the admitted busy fence. The copier
// reads only the value assigned here, never any c-owned pointer or traversal.
func (b *Backend) endCleanupPhysical(w *workspaceExecution, c *workspaceCleanup) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var p WorkspacePhysical
	if w != nil {
		p = w.physical
	} else {
		p = c.physical
	}
	p.Transitioning = false
	p.Cleanup = true
	p.Uncertain = p.Uncertain || c.err != nil || c.blocked != nil
	if w == nil {
		// Durable marked root identity is checked by reopen before this mirror
		// can report it as an actual observed root. The external candidate is
		// represented by Store's marked projection, never adopted by Local.
		p.Uncertain = c.err != nil || c.blocked != nil || !c.initialized
		if c.initialized && !c.absent {
			p.Root = c.origin.Root
		}
	}
	if w != nil {
		w.physical = p
	} else {
		c.physical = p
	}
}
