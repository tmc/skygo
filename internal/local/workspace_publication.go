package local

import (
	"errors"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

var errWorkspaceCollectionHeld = errors.New("workspace collection identity is uncertain")

// No materializer has entered here. Only its genuinely unused one-shot owner
// can yield; actual filesystem/Close ownership uses settleWorkspacePermit.
func releaseUnusedWorkspaceOwner(w *workspaceExecution) {
	if w.release != nil {
		w.release()
		w.release = nil
		w.initialOwnerReleased = true
	}
}

func (b *Backend) admitWorkspaceMaterialization(w *workspaceExecution) bool {
	for {
		wake := b.publicationAttempt(w)
		if stopped, _, _ := b.workspaceStopped(w); stopped {
			releaseUnusedWorkspaceOwner(w)
			return false
		}
		if w.release == nil {
			release, ok := b.workspaces.config.TryCollectionIO()
			if !ok || release == nil {
				b.waitPublication(w, wake, "waiting-materialization")
				continue
			}
			w.release = onceWorkspaceRelease(release)
		}
		if stopped, _, _ := b.workspaceStopped(w); stopped {
			releaseUnusedWorkspaceOwner(w)
			return false
		}
		if err := b.workspaces.config.CheckGrowth(); err != nil {
			releaseUnusedWorkspaceOwner(w)
			b.waitPublication(w, wake, "waiting-materialization")
			continue
		}
		if stopped, _, _ := b.workspaceStopped(w); stopped {
			releaseUnusedWorkspaceOwner(w)
			return false
		}
		return true
	}
}

// ResumeWorkspacePublication requests another admission attempt by the same
// original sequencer. It performs no I/O and grants no capacity or completion.
func (b *Backend) ResumeWorkspacePublication(ref backend.Reference) error {
	if b == nil {
		return backend.ErrIdentityMismatch
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[ref.ID]
	if e == nil || e.workspace == nil {
		return backend.ErrIdentityMismatch
	}
	w := e.workspace
	if err := workspaceMatch(w, ref); err != nil {
		return err
	}
	select {
	case <-e.done:
		return nil
	default:
	}
	wakeWorkspace(w)
	return nil
}

// wakeWorkspace holds Backend.mu. Hints carry no queued operations or permits.
func wakeWorkspace(w *workspaceExecution) {
	if !w.wakePending {
		w.wakePending = true
		close(w.wake)
		w.wake = make(chan struct{})
	}
}

func (b *Backend) publicationAttempt(w *workspaceExecution) <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	w.wakePending = false
	return w.wake
}

func (b *Backend) workspaceTimeout(w *workspaceExecution) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.taskDeadlineOpen {
		b.stopWorkspace(w, backend.Failed, "execution timed out")
	}
}

func (b *Backend) waitPublication(w *workspaceExecution, wake <-chan struct{}, phase string) {
	b.mu.Lock()
	w.observation.Phase = phase
	pending := w.wakePending
	b.mu.Unlock()
	if !pending {
		<-wake
	}
}

// Only the sequencer calls this function, after its actual native/pipe joins.
// The attempted/result state prevents finishWorkspace from closing a second time.
func (b *Backend) closeWorkspaceLog(w *workspaceExecution) error {
	if w.logCloseAttempted {
		return w.logCloseErr
	}
	w.logCloseAttempted = true
	out := w.parent.output
	if out == nil {
		return nil
	}
	out.mu.Lock()
	err := b.defaultOps.closeOutput(out)
	if err == nil {
		out.closed = true
	}
	err = errors.Join(err, out.err, out.markerCloseErr)
	out.err = err
	out.mu.Unlock()
	w.logCloseErr = err
	return err
}

// releaseCollection is called only after actual operations returned. An uncertain
// original Close keeps the SAME permit; a time limit cannot release it.
func releaseCollection(w *workspaceExecution, err error) {
	if w.collectionRelease == nil {
		return
	}
	if errors.Is(err, artifact.ErrFileCloseUncertain) {
		w.collectionErr = err
		return
	}
	w.collectionRelease()
	w.collectionRelease = nil
}
