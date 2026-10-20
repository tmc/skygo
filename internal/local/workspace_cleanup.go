package local

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// WorkspaceCleanupIdentity is a trusted completed-subject snapshot. Completion
// and writer digests are opaque Store CAS fences, not physical owner proofs.
// TaskDigest must match the original accepted task, not a reconstructed Task.
type WorkspaceCleanupIdentity struct {
	Owner, JobID, AttemptID, Binding, Instance, RootID string
	TaskDigest, CompletionDigest, WriterReceipt        [32]byte
	InputRef                                           artifact.Ref
	ResultID, ResultState                              string
	ResultRef                                          artifact.Ref
	ResultFile                                         artifact.FileID
}

// WorkspaceRootState records positive original creation facts, never path absence.
type WorkspaceRootState uint8

const (
	WorkspaceRootNeverCreated WorkspaceRootState = iota + 1
	WorkspaceRootOwned
)

// WorkspaceCandidateDisposition keeps external candidate cleanup independent.
type WorkspaceCandidateDisposition uint8

const (
	WorkspaceNoOutputs WorkspaceCandidateDisposition = iota + 1
	WorkspaceCandidateNeverCreated
	WorkspaceCandidateClosedUnpublished
	WorkspaceResultPublishedIndependent
)

// WorkspaceCleanupOrigin comes from original retired native owners. Cleanup
// handles remain owned separately. No field independently proves quiescence.
type WorkspaceCleanupOrigin struct {
	Subject         WorkspaceCleanupIdentity
	Parent, Root    artifact.FileID
	RootState       WorkspaceRootState
	Disposition     WorkspaceCandidateDisposition
	CandidateID     string
	CandidateFile   artifact.FileID
	CandidateSize   int64
	CandidateSHA256 string
	Producer        ServerLifetime
}

// WorkspaceRootIntent is the exact already-committed cleanup work, supplied only
// by trusted assembly. Local cannot authenticate a Store commit from this DTO.
type WorkspaceRootIntent struct {
	ID       string
	Revision uint64
	Digest   [32]byte
	Origin   WorkspaceCleanupOrigin
}

// WorkspaceRootProgress describes root-only filesystem progress. Done grants no
// candidate, namespace, reservation or Store credit; assembly must independently
// settle the exact external candidate disposition before trusted Finish.
type WorkspaceRootProgress struct {
	Visited int
	Done    bool
}

type workspaceCleanup struct {
	mu                                  sync.Mutex
	physical                            WorkspacePhysical // guarded by Backend.mu; never contains live owners
	origin                              WorkspaceCleanupOrigin
	intent                              WorkspaceRootIntent
	marked, nativeRetired, done, handed bool
	postmark, initialized               bool
	prepared                            bool
	preorigin                           WorkspacePhysicalOrigin
	observation                         WorkspaceObservation
	busy, owned, closeHeld              bool // guarded by Backend.mu
	parent                              *os.Root
	parentDir                           *os.File
	root                                *os.Root
	directory                           *os.File
	stack                               []*workspaceCleanupFrame
	absent, rootClosed                  bool
	blocked                             error
	err                                 error // retains the exact original uncertain Close and its original FD
}
type workspaceCleanupFrame struct {
	root        *os.Root
	dir         *os.File
	name        string
	id          artifact.FileID
	closed      bool
	pending     string
	pendingInfo os.FileInfo
	readErr     error
}

// Only actual I/O fault seams, not a replacement filesystem or owner registry.
type workspaceCleanupOps struct {
	closeFile func(*os.File) error
	closeRoot func(*os.Root) error
	sync      func(*os.File) error
	remove    func(*os.Root, string) error
}

func (b *Backend) cleanupAdmission(id string) (*workspaceExecution, *workspaceCleanup, error) {
	if b == nil {
		return nil, nil, errors.New("workspace cleanup is unavailable")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.workspaces == nil {
		return nil, nil, errors.New("workspace cleanup is unavailable")
	}
	w := b.workspaces.bindings[id]
	if w == nil {
		if e := b.jobs[id]; e != nil {
			w = e.workspace
		}
	}
	if w == nil || w.parent == nil || w.cleanupBusy {
		return nil, nil, errors.New("workspace cleanup owner is unavailable")
	}
	select {
	case <-w.parent.done:
	default:
		return nil, nil, errors.New("workspace sequence is active")
	}
	if w.cleanup == nil {
		w.cleanup = &workspaceCleanup{}
	}
	w.cleanupBusy = true
	w.cleanup.busy = true
	return w, w.cleanup, nil
}
func (b *Backend) cleanupEnd(w *workspaceExecution, c *workspaceCleanup) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c.busy = false
	c.owned = !(c.done || c.handed) && (c.parent != nil || c.parentDir != nil || c.root != nil || c.directory != nil || len(c.stack) != 0)
	c.closeHeld = c.err != nil
	if w != nil {
		w.cleanupBusy = false
		w.cleanupOwned = c.owned
		// Exact intent now owns independent namespace cleanup. Keep only the
		// closed log/status row in jobs; no Retry proof is created or altered.
		if e := w.parent; e != nil && w.cleanup == c && e.workspace == w &&
			c.prepared && c.nativeRetired && c.marked && c.err == nil && (c.done || c.handed) &&
			c.parent == nil && c.parentDir == nil && c.root == nil && c.directory == nil &&
			len(c.stack) == 0 && !c.owned {
			select {
			case <-e.done:
				// Both locks protect the same original association/positive
				// receipts. A namespace refusal may still positively hand off
				// these handles; no root/candidate/Store credit follows.
				e.workspaceClosed = true
				e.workspace = nil
				if c.done {
					delete(b.workspaces.bindings, w.binding.AttemptID)
				}
			default:
			}
		}
	} else if c.done {
		delete(b.workspaces.cleanupFrames, c.origin.Subject.AttemptID)
	}
}

// Only trusted assembly may pass postmark intents from the durable selector.
// This creates cleanup ownership, never a job, binding, Task or native receipt.
func (b *Backend) cleanupIntentAdmission(intent WorkspaceRootIntent) (*workspaceExecution, *workspaceCleanup, error) {
	if b == nil {
		return nil, nil, errors.New("workspace cleanup is unavailable")
	}
	id := intent.Origin.Subject.AttemptID
	b.mu.Lock()
	if b.closed || b.workspaces == nil || !workspaceWriterPlatform() || !b.retryAvailable {
		b.mu.Unlock()
		return nil, nil, errors.New("workspace cleanup is unavailable")
	}
	w := b.workspaces.bindings[id]
	if w == nil {
		if e := b.jobs[id]; e != nil {
			w = e.workspace
		}
	}
	if w != nil {
		b.mu.Unlock()
		return b.cleanupAdmission(id)
	}
	c := b.workspaces.cleanupFrames[id]
	if c == nil {
		if len(b.workspaces.bindings)+len(b.workspaces.cleanupFrames) >= 64 {
			b.mu.Unlock()
			return nil, nil, errors.New("workspace cleanup reservation limit reached")
		}
		if !validRootIntent(intent) || intent.Origin.Subject.RootID != b.workspaces.config.RootID || intent.Origin.Subject.Binding != b.workspaces.binding || intent.Origin.Parent != b.workspaces.root {
			b.mu.Unlock()
			return nil, nil, backend.ErrIdentityMismatch
		}
		c = &workspaceCleanup{origin: intent.Origin, intent: intent, marked: true, postmark: true, physical: WorkspacePhysical{AttemptID: id, Binding: intent.Origin.Subject.Binding, Instance: intent.Origin.Subject.Instance, RootID: intent.Origin.Subject.RootID, CandidateID: intent.Origin.Subject.ResultID, Parent: intent.Origin.Parent, RootAttempted: intent.Origin.RootState == WorkspaceRootOwned, Cleanup: true, Transitioning: true, Uncertain: true}}
		b.workspaces.cleanupFrames[id] = c
	}
	if c.busy {
		b.mu.Unlock()
		return nil, nil, errors.New("workspace cleanup owner is active")
	}
	c.busy = true
	b.mu.Unlock()
	return nil, c, nil
}
func validRootIntent(intent WorkspaceRootIntent) bool {
	o := intent.Origin
	s := o.Subject
	if !validWorkspaceProducer(o.Producer) {
		return false
	}
	if !validID.MatchString(intent.ID) || intent.Revision == 0 || intent.Digest == ([32]byte{}) || !validID.MatchString(s.JobID) || !validID.MatchString(s.AttemptID) || !validID.MatchString(s.RootID) || s.Owner == "" || len(s.Owner) > 64 || s.Instance == "" || s.TaskDigest == ([32]byte{}) || s.CompletionDigest == ([32]byte{}) || s.WriterReceipt == ([32]byte{}) || s.InputRef.Validate() != nil || o.Parent.Device == 0 || o.Parent.Inode == 0 {
		return false
	}
	if o.RootState == WorkspaceRootNeverCreated {
		if o.Root != (artifact.FileID{}) {
			return false
		}
	} else if o.RootState != WorkspaceRootOwned || o.Root.Device != o.Parent.Device || o.Root.Inode == 0 || o.Root == o.Parent {
		return false
	}
	zeroPrefix := o.CandidateID == "" && o.CandidateFile == (artifact.FileID{}) && o.CandidateSize == 0 && o.CandidateSHA256 == ""
	switch o.Disposition {
	case WorkspaceNoOutputs:
		return s.ResultState == "none" && s.ResultID == "" && s.ResultRef == (artifact.Ref{}) && s.ResultFile == (artifact.FileID{}) && zeroPrefix
	case WorkspaceCandidateNeverCreated:
		return s.ResultState == "reserved" && validID.MatchString(s.ResultID) && s.ResultRef == (artifact.Ref{}) && s.ResultFile == (artifact.FileID{}) && zeroPrefix
	case WorkspaceCandidateClosedUnpublished:
		return o.RootState == WorkspaceRootOwned && s.ResultState == "reserved" && validID.MatchString(s.ResultID) && s.ResultRef == (artifact.Ref{}) && s.ResultFile == (artifact.FileID{}) && o.CandidateID == s.ResultID && o.CandidateFile.Device == o.Parent.Device && o.CandidateFile.Inode != 0 && o.CandidateSize >= 0 && o.CandidateSize <= artifact.MaxSize && cleanupHex(o.CandidateSHA256, 64)
	case WorkspaceResultPublishedIndependent:
		return o.RootState == WorkspaceRootOwned && s.ResultState == "complete" && s.ResultRef.Validate() == nil && s.ResultRef.ID == s.ResultID && s.ResultFile.Device == o.Parent.Device && s.ResultFile.Inode != 0 && zeroPrefix
	}
	return false
}
func (b *Backend) reopenWorkspaceCleanup(c *workspaceCleanup) error {
	if !c.postmark || c.initialized {
		return nil
	}
	facts, err := b.workspaces.config.Files.RootFacts()
	if err != nil {
		if errors.Is(err, artifact.ErrFileCloseUncertain) {
			c.err = err
		}
		return err
	}
	if facts.ID != c.origin.Subject.RootID || facts.File != c.origin.Parent {
		return backend.ErrIdentityMismatch
	}
	if c.parent == nil {
		c.parent, err = b.workspaces.config.Roots.OpenRoot(".")
		if err != nil {
			return err
		}
	}
	if c.parentDir == nil {
		c.parentDir, err = openWorkspaceDirectory(c.parent)
		if err != nil {
			return err
		}
	}
	if err := c.checkParent(); err != nil {
		return err
	}
	name := "workspace-" + c.origin.Subject.AttemptID
	info, err := c.parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		c.absent = true
		c.initialized = true
		return nil
	}
	if err != nil {
		return err
	}
	if c.origin.RootState != WorkspaceRootOwned {
		return backend.ErrIdentityMismatch
	}
	id, err := workspaceFileID(info)
	if err != nil || id != c.origin.Root {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	if c.root == nil {
		c.root, err = c.parent.OpenRoot(name)
		if err != nil {
			return err
		}
	}
	if c.directory == nil {
		c.directory, err = openWorkspaceDirectory(c.root)
		if err != nil {
			return err
		}
	}
	opened, err := c.directory.Stat()
	if err != nil {
		return err
	}
	openedID, err := workspaceFileID(opened)
	if err != nil || openedID != id {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	if err := c.checkRoot(); err != nil {
		return err
	}
	c.initialized = true
	return nil
}

func cleanupHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' && r < 'a' || r > 'f' {
			return false
		}
	}
	return true
}
func (c *workspaceCleanup) accept(intent WorkspaceRootIntent) error {
	if !c.marked || !(c.nativeRetired || c.postmark) || !validRootIntent(intent) || intent.Origin != c.origin {
		return backend.ErrIdentityMismatch
	}
	if c.marked && c.intent != intent {
		return backend.ErrIdentityMismatch
	}
	c.intent = intent
	c.marked = true
	return nil
}
func (b *Backend) cleanupFile(c *workspaceCleanup, file **os.File) error {
	if *file == nil {
		return nil
	}
	var err error
	if b.workspaceCleanupOps.closeFile != nil {
		err = b.workspaceCleanupOps.closeFile(*file)
	} else {
		err = (*file).Close()
	}
	if err != nil {
		c.err = errors.Join(artifact.ErrFileCloseUncertain, err)
		return c.err
	}
	*file = nil
	return nil
}
func (b *Backend) cleanupRoot(c *workspaceCleanup, root **os.Root) error {
	if *root == nil {
		return nil
	}
	var err error
	if b.workspaceCleanupOps.closeRoot != nil {
		err = b.workspaceCleanupOps.closeRoot(*root)
	} else {
		err = (*root).Close()
	}
	if err != nil {
		c.err = errors.Join(artifact.ErrFileCloseUncertain, err)
		return c.err
	}
	*root = nil
	return nil
}
func (b *Backend) cleanupSync(file *os.File) error {
	if b.workspaceCleanupOps.sync != nil {
		return b.workspaceCleanupOps.sync(file)
	}
	return file.Sync()
}
func (b *Backend) cleanupRemove(root *os.Root, name string) error {
	if b.workspaceCleanupOps.remove != nil {
		return b.workspaceCleanupOps.remove(root, name)
	}
	return root.Remove(name)
}
func (c *workspaceCleanup) checkParent() error {
	info, err := c.parentDir.Stat()
	if err != nil {
		return err
	}
	id, err := workspaceFileID(info)
	if err != nil || id != c.origin.Parent {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	info, err = c.parent.Lstat(".")
	if err != nil {
		return err
	}
	id, err = workspaceFileID(info)
	if err != nil || id != c.origin.Parent {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	return nil
}
func (c *workspaceCleanup) checkRoot() error {
	if err := c.checkParent(); err != nil {
		return err
	}
	info, err := c.parent.Lstat("workspace-" + c.origin.Subject.AttemptID)
	if err != nil {
		return err
	}
	id, err := workspaceFileID(info)
	if err != nil || id != c.origin.Root {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	if c.root != nil {
		info, err = c.root.Lstat(".")
		if err != nil {
			return err
		}
		id, err = workspaceFileID(info)
		if err != nil || id != c.origin.Root {
			return errors.Join(backend.ErrIdentityMismatch, err)
		}
	}
	if c.directory != nil {
		info, err = c.directory.Stat()
		if err != nil {
			return err
		}
		id, err = workspaceFileID(info)
		if err != nil || id != c.origin.Root {
			return errors.Join(backend.ErrIdentityMismatch, err)
		}
	}
	return nil
}

// RemoveWorkspaceRoot visits at most100 entries, with at most64 directory levels.
// Only original prepared owners plus an exact committed intent may enter. Done
// reports this root only; it never settles a candidate in the Files namespace.
// Close uncertainty retains the same original handles/error and worker permit.
func (b *Backend) RemoveWorkspaceRoot(ctx context.Context, intent WorkspaceRootIntent) (progress WorkspaceRootProgress, err error) {
	w, c, err := b.cleanupIntentAdmission(intent)
	if err != nil {
		return progress, err
	}
	b.beginCleanupPhysical(w, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer b.cleanupEnd(w, c)
	defer b.endCleanupPhysical(w, c)
	if err = c.accept(intent); err != nil {
		return progress, err
	}
	if c.err != nil {
		return progress, c.err
	}
	if c.handed {
		return progress, errors.New("workspace cleanup was handed off")
	}
	if c.done {
		progress.Done = true
		return progress, nil
	}
	if c.blocked != nil {
		return progress, c.blocked
	}
	if err = ctx.Err(); err != nil {
		return progress, err
	}
	if err = b.reopenWorkspaceCleanup(c); err != nil {
		return progress, err
	}
	if c.origin.RootState == WorkspaceRootNeverCreated || c.absent {
		return b.finishWorkspaceRoot(c)
	}
	if c.rootClosed {
		if err = c.checkParent(); err != nil {
			return progress, err
		}
		if _, statErr := c.parent.Lstat("workspace-" + c.origin.Subject.AttemptID); errors.Is(statErr, os.ErrNotExist) {
			c.absent = true
			return b.finishWorkspaceRoot(c)
		} else if statErr != nil {
			return progress, statErr
		}
		return b.unlinkWorkspaceRoot(c, 0)
	}
	if err = c.checkRoot(); err != nil {
		return progress, err
	}
	if len(c.stack) == 0 {
		c.stack = append(c.stack, &workspaceCleanupFrame{root: c.root, dir: c.directory, id: c.origin.Root})
	}
	for progress.Visited < 100 && len(c.stack) > 0 {
		if err = ctx.Err(); err != nil {
			return progress, err
		}
		top := c.stack[len(c.stack)-1]
		if top.closed {
			up := c.stack[len(c.stack)-2]
			progress.Visited++
			if err = b.checkCleanupEntry(c, up, top.name, top.id); err != nil && !errors.Is(err, os.ErrNotExist) {
				return progress, err
			}
			if err == nil {
				if err = b.cleanupRemove(up.root, top.name); err != nil && !errors.Is(err, os.ErrNotExist) {
					return progress, err
				}
			}
			up.pending = ""
			up.pendingInfo = nil
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}
		if top.dir == nil {
			top.dir, err = openWorkspaceDirectory(top.root)
			if err != nil {
				return progress, err
			}
		}
		opened, statErr := top.dir.Stat()
		if statErr != nil {
			return progress, statErr
		}
		openedID, idErr := workspaceCleanupEntryID(opened)
		if idErr != nil || openedID != top.id {
			return progress, errors.Join(backend.ErrIdentityMismatch, idErr)
		}
		if len(c.stack) > 1 {
			up := c.stack[len(c.stack)-2]
			if err = b.checkCleanupEntry(c, up, top.name, top.id); err != nil {
				return progress, err
			}
		}
		if top.pending == "" {
			if top.readErr != nil {
				return progress, top.readErr
			}
			entries, readErr := top.dir.ReadDir(1)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				top.readErr = readErr
			}
			if len(entries) == 0 {
				if readErr != nil && !errors.Is(readErr, io.EOF) {
					return progress, readErr
				}
				if len(c.stack) == 1 {
					break
				}
				if err = b.cleanupFile(c, &top.dir); err != nil {
					return progress, err
				}
				if err = b.cleanupRoot(c, &top.root); err != nil {
					return progress, err
				}
				top.closed = true
				continue
			}
			progress.Visited++
			name := entries[0].Name()
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
				c.blocked = backend.ErrIdentityMismatch
				return progress, c.blocked
			}
			top.pending = name
			info, statErr := top.root.Lstat(name)
			if errors.Is(statErr, os.ErrNotExist) {
				top.pending = ""
				continue
			}
			if statErr != nil {
				return progress, statErr
			}
			top.pendingInfo = info
		} else {
			progress.Visited++
		}
		if top.pendingInfo == nil {
			top.pendingInfo, err = top.root.Lstat(top.pending)
			if errors.Is(err, os.ErrNotExist) {
				top.pending = ""
				continue
			}
			if err != nil {
				return progress, err
			}
		}
		info := top.pendingInfo
		id, idErr := workspaceCleanupEntryID(info)
		if idErr != nil || id.Device != c.origin.Root.Device {
			c.blocked = errors.Join(backend.ErrIdentityMismatch, idErr)
			return progress, c.blocked
		}
		if err = b.checkCleanupEntry(c, top, top.pending, id); errors.Is(err, os.ErrNotExist) {
			top.pending = ""
			top.pendingInfo = nil
			continue
		} else if err != nil {
			return progress, err
		}
		if info.IsDir() {
			if len(c.stack) >= 64 {
				c.blocked = errors.New("workspace cleanup depth exceeds 64")
				return progress, c.blocked
			}
			child, openErr := top.root.OpenRoot(top.pending)
			if openErr != nil {
				return progress, openErr
			}
			frame := &workspaceCleanupFrame{root: child, name: top.pending, id: id}
			c.stack = append(c.stack, frame)
			// The new original Root remains retained even if the following open
			// or inode check fails; no recovered raw descriptor is substituted.
			continue
		}
		if !info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			c.blocked = errors.New("workspace cleanup refuses special file")
			return progress, c.blocked
		}
		if err = b.cleanupRemove(top.root, top.pending); err != nil && !errors.Is(err, os.ErrNotExist) {
			return progress, err
		}
		top.pending = ""
		top.pendingInfo = nil
	}
	if len(c.stack) > 1 || progress.Visited == 100 {
		return progress, nil
	}
	// EOF on the original root. This last observed-inode check is not an atomic
	// unlink against arbitrary concurrent same-user namespace replacement.
	if err = c.checkRoot(); err != nil {
		return progress, err
	}
	if err = b.cleanupFile(c, &c.directory); err != nil {
		return progress, err
	}
	if err = b.cleanupRoot(c, &c.root); err != nil {
		return progress, err
	}
	c.stack = nil
	c.rootClosed = true
	return b.unlinkWorkspaceRoot(c, progress.Visited)
}
func (b *Backend) checkCleanupEntry(c *workspaceCleanup, frame *workspaceCleanupFrame, name string, expected artifact.FileID) error {
	info, err := frame.root.Lstat(name)
	if err != nil {
		return err
	}
	id, err := workspaceCleanupEntryID(info)
	if err != nil || id != expected || id.Device != c.origin.Root.Device {
		return errors.Join(backend.ErrIdentityMismatch, err)
	}
	return nil
}
func (b *Backend) unlinkWorkspaceRoot(c *workspaceCleanup, visited int) (WorkspaceRootProgress, error) {
	progress := WorkspaceRootProgress{Visited: visited + 1}
	if err := c.checkRoot(); err != nil {
		return progress, err
	}
	if err := b.cleanupRemove(c.parent, "workspace-"+c.origin.Subject.AttemptID); err != nil {
		return progress, err
	}
	c.absent = true
	final, err := b.finishWorkspaceRoot(c)
	final.Visited = progress.Visited
	return final, err
}
func (b *Backend) finishWorkspaceRoot(c *workspaceCleanup) (WorkspaceRootProgress, error) {
	if err := c.checkParent(); err != nil {
		return WorkspaceRootProgress{}, err
	}
	// Intent-authorized absence does not retire owners from a partial reopen.
	// Join every original cleanup handle before parent sync or Done; a failed
	// Close keeps its original pointer/error and fences all raw Close retries.
	for i := len(c.stack) - 1; i > 0; i-- {
		frame := c.stack[i]
		if err := b.cleanupFile(c, &frame.dir); err != nil {
			return WorkspaceRootProgress{}, err
		}
		if err := b.cleanupRoot(c, &frame.root); err != nil {
			return WorkspaceRootProgress{}, err
		}
	}
	if err := b.cleanupFile(c, &c.directory); err != nil {
		return WorkspaceRootProgress{}, err
	}
	if err := b.cleanupRoot(c, &c.root); err != nil {
		return WorkspaceRootProgress{}, err
	}
	c.stack = nil
	if err := b.cleanupSync(c.parentDir); err != nil {
		return WorkspaceRootProgress{}, err
	}
	if err := b.cleanupFile(c, &c.parentDir); err != nil {
		return WorkspaceRootProgress{}, err
	}
	if err := b.cleanupRoot(c, &c.parent); err != nil {
		return WorkspaceRootProgress{}, err
	}
	c.done = true
	return WorkspaceRootProgress{Done: true}, nil
}

// HandoffWorkspaceCleanup positively closes only original cleanup handles before
// terminal Stop. It never unlinks, returns Done, changes counters, or settles the
// independent candidate. Its exact intent fence survives until final Local Close.
func (b *Backend) HandoffWorkspaceCleanup(ctx context.Context, intent WorkspaceRootIntent) error {
	w, c, err := b.cleanupIntentAdmission(intent)
	if err != nil {
		return err
	}
	b.beginCleanupPhysical(w, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer b.cleanupEnd(w, c)
	defer b.endCleanupPhysical(w, c)
	if err := c.accept(intent); err != nil {
		return err
	}
	if c.err != nil {
		return c.err
	}
	if c.handed || c.done {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.reopenWorkspaceCleanup(c); err != nil {
		return err
	}
	if err := c.checkParent(); err != nil {
		return err
	}
	if c.origin.RootState == WorkspaceRootOwned && !c.absent {
		if err := c.checkRoot(); err != nil {
			return err
		}
	}
	for i := len(c.stack) - 1; i > 0; i-- {
		frame := c.stack[i]
		if err := b.cleanupFile(c, &frame.dir); err != nil {
			return err
		}
		if err := b.cleanupRoot(c, &frame.root); err != nil {
			return err
		}
	}
	c.stack = nil
	if err := b.cleanupFile(c, &c.directory); err != nil {
		return err
	}
	if err := b.cleanupRoot(c, &c.root); err != nil {
		return err
	}
	if err := b.cleanupFile(c, &c.parentDir); err != nil {
		return err
	}
	if err := b.cleanupRoot(c, &c.parent); err != nil {
		return err
	}
	c.handed = true
	return nil
}
