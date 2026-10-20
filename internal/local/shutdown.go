package local

import "errors"

var errShutdownHeld = errors.New("local ownership remains held")

type shutdownWorkspace struct {
	id      string
	owner   *workspaceExecution
	cleanup *workspaceCleanup
}

// CheckShutdown checks retained producer and workspace cleanup receipts.
// The caller must first stop dispatch and integrate original Control calls.
// It performs no I/O, cancellation, join, cleanup or ownership release. It does
// not establish filesystem absence or settle Store, artifact or borrowed owners.
// Historical executions are scanned without a constant-time bound.
func (b *Backend) CheckShutdown() error {
	if b == nil {
		return errShutdownHeld
	}
	b.mu.Lock()
	if b.jobs == nil {
		b.mu.Unlock()
		return errShutdownHeld
	}
	if b.logReaderErr != nil {
		b.mu.Unlock()
		return errors.Join(errShutdownHeld, b.logReaderErr)
	}
	if b.unstartedOutput != nil || b.unstartedOutputErr != nil {
		err := errors.Join(errShutdownHeld, b.unstartedOutputErr)
		b.mu.Unlock()
		return err
	}
	state := b.workspaces
	var frames [64]shutdownWorkspace
	n := 0
	if state != nil {
		if len(state.bindings)+len(state.cleanupFrames) > len(frames) {
			b.mu.Unlock()
			return errShutdownHeld
		}
		for id, w := range state.bindings {
			if w == nil {
				b.mu.Unlock()
				return errShutdownHeld
			}
			frames[n] = shutdownWorkspace{id: id, owner: w, cleanup: w.cleanup}
			n++
		}
		for id, c := range state.cleanupFrames {
			if c == nil {
				b.mu.Unlock()
				return errShutdownHeld
			}
			frames[n] = shutdownWorkspace{id: id, cleanup: c}
			n++
		}
	}
	for _, e := range b.jobs {
		if e == nil {
			b.mu.Unlock()
			return errShutdownHeld
		}
		if !shutdownDone(e.done) {
			err := errors.Join(errShutdownHeld, e.shutdownErr)
			b.mu.Unlock()
			return err
		}
		if e.workspace == nil && e.owned == nil && e.retry == nil {
			// Actual cleanupEnd publishes this bit under b.mu only after the
			// same original done and positive cleanup-handle retirement. Nil
			// selects the audit class; it never supplies positive evidence.
			if !e.workspaceClosed {
				b.mu.Unlock()
				return errShutdownHeld
			}
			continue
		}
		workspace := false
		for i := 0; i < n; i++ {
			if w := frames[i].owner; w != nil && w.parent == e {
				if e.workspace != nil && e.workspace != w {
					b.mu.Unlock()
					return errShutdownHeld
				}
				workspace = true
				break
			}
		}
		if workspace {
			continue
		}
		// Any remaining workspace parent must have its exact surviving frame.
		if e.workspace != nil || e.owned == nil && e.retry == nil {
			b.mu.Unlock()
			return errShutdownHeld
		}
		if err := shutdownExecution(e); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	b.mu.Unlock()
	// Cleanup routines use c.mu -> b.mu. Never hold b.mu while acquiring it.
	for i := 0; i < n; i++ {
		if err := b.shutdownWorkspace(state, frames[i]); err != nil {
			return err
		}
	}
	return nil
}

func shutdownDone(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// shutdownExecution holds b.mu after the original done publication. Diagnostics
// may survive positive physical closure; their presence alone is not failure.
func shutdownExecution(e *execution) error {
	if !e.shutdownPublished || !e.shutdownClosed {
		return errors.Join(errShutdownHeld, e.shutdownErr)
	}
	if e.output == nil || e.owned != nil && e.retry != nil {
		return errShutdownHeld
	}
	if e.stdioWriter != nil || e.stdioWriterErr != nil || e.readerCloseErr != nil {
		return errors.Join(errShutdownHeld, e.stdioWriterErr, e.readerCloseErr)
	}
	if e.output != nil {
		e.output.mu.Lock()
		closed := e.output.closed && e.output.markerFile == nil && e.output.markerCloseErr == nil
		err := errors.Join(e.output.err, e.output.markerCloseErr)
		e.output.mu.Unlock()
		if !closed {
			return errors.Join(errShutdownHeld, err)
		}
	}
	return nil
}

func (b *Backend) shutdownWorkspace(state *workspaceState, f shutdownWorkspace) error {
	c := f.cleanup
	if c != nil {
		if !c.mu.TryLock() {
			return errShutdownHeld
		}
		defer c.mu.Unlock()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.workspaces != state || state == nil {
		return errShutdownHeld
	}
	if c != nil && (c.err != nil || c.closeHeld) {
		return errors.Join(errShutdownHeld, c.err)
	}
	w := f.owner
	if w != nil {
		if state.bindings[f.id] != w || w.cleanup != c {
			return errShutdownHeld
		}
		if w.parent == nil {
			// No sequencer exists, so these original frontier fields are read
			// under b.mu only after its callback-return receipt is published.
			p := w.physical
			if !w.noCallSettled || !w.initialOwnerReleased || !w.startAttempted || w.release != nil ||
				!workspaceNoCallFrontier(w) || p.RootAttempted || p.CandidateAttempted ||
				p.Root.Device != 0 || p.Root.Inode != 0 || p.Candidate.Device != 0 || p.Candidate.Inode != 0 ||
				p.Transitioning || p.Uncertain || p.WriterActive || p.Cleanup {
				return errShutdownHeld
			}
			return nil
		}
		if !shutdownDone(w.parent.done) {
			return errShutdownHeld
		}
		// The sequencer's done supplies the publication fence. Mutable cleanup
		// fields below additionally require its original c.mu and busy fence.
		if c == nil || !w.observation.Ready || w.observation.Held || w.observation.Writer == nil ||
			!w.committed || w.phase != nil || w.failedPhase != nil || w.release != nil ||
			w.collectionRelease != nil || w.root != nil || w.directory != nil || w.candidate != nil ||
			w.rootErr != nil || w.collectionErr != nil || !w.logCloseAttempted || w.logCloseErr != nil ||
			w.taskDeadlineOpen || w.cleanupBusy || w.cleanupOwned ||
			w.physical.Transitioning || w.physical.WriterActive {
			return errors.Join(errShutdownHeld, w.logCloseErr, w.rootErr, w.collectionErr)
		}
		if !c.prepared || !c.nativeRetired {
			return errShutdownHeld
		}
	} else if c == nil || state.cleanupFrames[f.id] != c || !c.postmark {
		return errShutdownHeld
	}
	if c == nil || c.busy || c.owned || c.closeHeld || c.err != nil || !c.marked ||
		!(c.nativeRetired || c.postmark) || !(c.done || c.handed) ||
		c.parent != nil || c.parentDir != nil || c.root != nil || c.directory != nil || len(c.stack) != 0 ||
		!validRootIntent(c.intent) || c.intent.Origin != c.origin {
		if c != nil {
			return errors.Join(errShutdownHeld, c.err)
		}
		return errShutdownHeld
	}
	// A blocked namespace-removal diagnostic can coexist with positive handoff
	// of every original handle. It grants no unlink/candidate/Store credit.
	return nil
}
