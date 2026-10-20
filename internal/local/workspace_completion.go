package local

import (
	"errors"
	"os"
	"strings"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// WorkspacePhysicalOrigin is the original owner's immutable precommit handoff.
// It contains no Store subject, final digest, publication decision or operation ID.
// Producer is the once-configured actual self identity; zero means unavailable.
type WorkspacePhysicalOrigin struct {
	Parent, Root   artifact.FileID
	RootState      WorkspaceRootState
	CandidateState WorkspacePhysicalCandidateState
	Prefix         *WorkspaceCandidatePrefix
	Producer       ServerLifetime
}

// WorkspaceCandidatePrefix describes positively closed original bytes, including
// malformed or empty archives. It grants no result access or cleanup authority.
type WorkspaceCandidatePrefix struct {
	ID     string
	File   artifact.FileID
	Size   int64
	SHA256 string
}

// WorkspacePhysicalCandidateState records the actual original creation frontier.
// Its zero value is unknown and cannot describe a completed physical origin.
type WorkspacePhysicalCandidateState uint8

const (
	WorkspacePhysicalNoOutputs WorkspacePhysicalCandidateState = iota + 1
	WorkspacePhysicalNeverAttempted
	WorkspacePhysicalClosedPrefix
	WorkspacePhysicalSealed
)

func validWorkspaceProducer(p ServerLifetime) bool {
	if p == (ServerLifetime{}) {
		return true
	}
	return cleanupHex(p.Host, 32) && p.Host != strings.Repeat("0", 32) && cleanupHex(p.Boot, 32) && p.Boot != strings.Repeat("0", 32) && p.NamespaceInode != 0 && p.PID != 0 && p.PID <= 1<<31-1
}

func copyWorkspaceOrigin(o WorkspacePhysicalOrigin) WorkspacePhysicalOrigin {
	if o.Prefix != nil {
		p := *o.Prefix
		o.Prefix = &p
	}
	return o
}

func copyPreparedObservation(o WorkspaceObservation, task backend.Task) WorkspaceObservation {
	if o.Status.ExitCode != nil {
		code := *o.Status.ExitCode
		o.Status.ExitCode = &code
	}
	if o.Writer != nil {
		writer := *o.Writer
		writer.Execution.Task = backend.CloneTask(task)
		o.Writer = &writer
	}
	if o.Candidate != nil {
		candidate := *o.Candidate
		o.Candidate = &candidate
	}
	return o
}

// PrepareWorkspaceCompletion retires the ORIGINAL native/phase/pipe/log owners
// before atomic Store completion. The caller owns a real common artifact permit.
// It retains original cleanup handles and returns no deletion/result authority.
// Replay returns the same prepared facts, without another Start/Collect/capture.
func (b *Backend) PrepareWorkspaceCompletion(ref backend.Reference) (WorkspaceObservation, WorkspacePhysicalOrigin, error) {
	w, c, err := b.cleanupAdmission(ref.ID)
	if err != nil {
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
	}
	b.beginCleanupPhysical(w, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer b.cleanupEnd(w, c)
	defer b.endCleanupPhysical(w, c)
	if c.err != nil {
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, c.err
	}
	b.mu.Lock()
	if err := workspaceMatch(w, ref); err != nil {
		b.mu.Unlock()
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
	}
	if c.prepared {
		b.mu.Unlock()
		return copyPreparedObservation(c.observation, ref.Task), copyWorkspaceOrigin(c.preorigin), nil
	}
	if !w.observation.Ready || w.observation.Held || w.observation.Writer == nil || w.phase != nil || w.failedPhase != nil || w.parent.cmd != nil || w.parent.owned != nil || w.parent.retry != nil || w.release != nil || w.collectionRelease != nil || w.rootErr != nil || w.collectionErr != nil || w.candidate != nil || !w.logCloseAttempted || w.logCloseErr != nil || w.taskDeadlineOpen {
		b.mu.Unlock()
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.New("workspace original owners remain held")
	}
	out := w.parent.output
	if out != nil {
		out.mu.Lock()
		closed := out.closed && out.err == nil && out.markerCloseErr == nil
		out.mu.Unlock()
		if !closed {
			b.mu.Unlock()
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.New("workspace log owner remains held")
		}
	}
	origin := WorkspacePhysicalOrigin{Producer: b.workspaces.config.ServerLifetime}
	if !w.rootAttempted {
		origin.RootState = WorkspaceRootNeverCreated
	} else if w.created && w.root != nil && w.directory != nil && w.rootInfo != nil {
		origin.RootState = WorkspaceRootOwned
	} else {
		b.mu.Unlock()
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.New("workspace creation identity is unknown")
	}
	candidate := w.candidateFile
	if w.binding.ResultID == "" {
		if w.candidateAttempted || w.observation.Candidate != nil {
			b.mu.Unlock()
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, backend.ErrIdentityMismatch
		}
		origin.CandidateState = WorkspacePhysicalNoOutputs
	} else if !w.candidateAttempted {
		if w.observation.Candidate != nil {
			b.mu.Unlock()
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, backend.ErrIdentityMismatch
		}
		origin.CandidateState = WorkspacePhysicalNeverAttempted
	} else if sealed := w.observation.Candidate; sealed != nil {
		if origin.RootState != WorkspaceRootOwned || w.observation.Outcome != WorkspaceTaskExit || w.observation.Status.State != backend.Succeeded || sealed.Manifest.Reference.ID != w.binding.ResultID || sealed.File != candidate.File || sealed.Manifest.Reference.Validate() != nil {
			b.mu.Unlock()
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, backend.ErrIdentityMismatch
		}
		origin.CandidateState = WorkspacePhysicalSealed
	} else {
		if origin.RootState != WorkspaceRootOwned || candidate.ID != w.binding.ResultID || candidate.File.Device == 0 || candidate.File.Inode == 0 || candidate.Size < 0 || candidate.Size > artifact.MaxSize || !cleanupHex(candidate.SHA256, 64) {
			b.mu.Unlock()
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.New("workspace candidate identity is unknown")
		}
		origin.CandidateState = WorkspacePhysicalClosedPrefix
		origin.Prefix = &WorkspaceCandidatePrefix{ID: candidate.ID, File: candidate.File, Size: candidate.Size, SHA256: candidate.SHA256}
	}
	observation := copyPreparedObservation(w.observation, backend.Task{}) // no retained Task copy
	b.mu.Unlock()
	facts, err := b.workspaces.config.Files.RootFacts()
	if err != nil {
		if errors.Is(err, artifact.ErrFileCloseUncertain) {
			c.err = err
		}
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
	}
	if facts.ID != w.binding.RootID || facts.File != b.workspaces.root {
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, backend.ErrIdentityMismatch
	}
	if c.parent == nil {
		c.parent, err = b.workspaces.config.Roots.OpenRoot(".")
		if err != nil {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
		}
	}
	if c.parentDir == nil {
		c.parentDir, err = openWorkspaceDirectory(c.parent)
		if err != nil {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
		}
	}
	parentInfo, err := c.parentDir.Stat()
	if err != nil {
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
	}
	origin.Parent, err = workspaceFileID(parentInfo)
	if err != nil || origin.Parent != facts.File {
		return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.Join(backend.ErrIdentityMismatch, err)
	}
	if origin.RootState == WorkspaceRootNeverCreated {
		// The original positive no-call frontier supplies NeverCreated. This
		// sanity check never upgrades an attempted/unknown create to absence.
		if _, err := c.parent.Lstat("workspace-" + ref.ID); !errors.Is(err, os.ErrNotExist) {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.Join(backend.ErrIdentityMismatch, err)
		}
	} else {
		if err := w.checkWorkspaceRoot(); err != nil {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
		}
		info, err := c.parent.Lstat(w.rootName)
		if err != nil {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
		}
		origin.Root, err = workspaceFileID(info)
		if err != nil {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, err
		}
		owned, err := workspaceFileID(w.rootInfo)
		if err != nil || origin.Root != owned || origin.Root.Device != origin.Parent.Device || origin.Root == origin.Parent {
			return WorkspaceObservation{}, WorkspacePhysicalOrigin{}, errors.Join(backend.ErrIdentityMismatch, err)
		}
	}
	b.mu.Lock()
	if out != nil {
		out.mu.Lock()
		out.file = nil
		out.mu.Unlock()
	}
	c.preorigin = copyWorkspaceOrigin(origin)
	c.observation = observation
	c.prepared = true
	c.nativeRetired = true
	c.initialized = true
	c.root = w.root
	c.directory = w.directory
	w.root = nil
	w.directory = nil
	b.mu.Unlock()
	return copyPreparedObservation(observation, ref.Task), copyWorkspaceOrigin(origin), nil
}

// CommitWorkspace binds ONLY the exact intent returned by the trusted atomic
// Store coordinator. This DTO cannot authenticate a Store commit by itself.
// It performs no Store call, unlink, raw Close or root-removal credit.
func (b *Backend) CommitWorkspace(ref backend.Reference, intent WorkspaceRootIntent) error {
	w, c, err := b.cleanupAdmission(ref.ID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	defer b.cleanupEnd(w, c)
	if c.err != nil {
		return c.err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := workspaceMatch(w, ref); err != nil {
		return err
	}
	if !c.prepared || !c.nativeRetired || !validRootIntent(intent) || !preparedWorkspaceIntent(w, c, intent) {
		return backend.ErrIdentityMismatch
	}
	if c.marked {
		if c.intent != intent {
			return backend.ErrIdentityMismatch
		}
		return nil
	}
	c.origin = intent.Origin
	c.intent = intent
	c.marked = true
	w.committed = true
	w.parent.status = c.observation.Status
	w.parent.status.Instance = ref.Instance
	return nil
}

func preparedWorkspaceIntent(w *workspaceExecution, c *workspaceCleanup, intent WorkspaceRootIntent) bool {
	o := intent.Origin
	s := o.Subject
	p := c.preorigin
	if s.Owner != w.binding.Owner || s.JobID != w.binding.JobID || s.AttemptID != w.binding.AttemptID || s.Binding != w.binding.Binding || s.Instance != w.parent.status.Instance || s.RootID != w.binding.RootID || s.TaskDigest != w.taskHash || s.InputRef != w.binding.Input || s.ResultID != w.binding.ResultID || s.CompletionDigest == ([32]byte{}) || s.WriterReceipt == ([32]byte{}) || o.Parent != p.Parent || o.Root != p.Root || o.RootState != p.RootState || o.Producer != p.Producer {
		return false
	}
	switch p.CandidateState {
	case WorkspacePhysicalNoOutputs:
		return o.Disposition == WorkspaceNoOutputs && s.ResultState == "none"
	case WorkspacePhysicalNeverAttempted:
		return o.Disposition == WorkspaceCandidateNeverCreated && s.ResultState == "reserved"
	case WorkspacePhysicalClosedPrefix:
		return p.Prefix != nil && o.Disposition == WorkspaceCandidateClosedUnpublished && s.ResultState == "reserved" && o.CandidateID == p.Prefix.ID && o.CandidateFile == p.Prefix.File && o.CandidateSize == p.Prefix.Size && o.CandidateSHA256 == p.Prefix.SHA256
	case WorkspacePhysicalSealed:
		sealed := c.observation.Candidate
		return sealed != nil && o.Disposition == WorkspaceResultPublishedIndependent && s.ResultState == "complete" && s.ResultRef == sealed.Manifest.Reference && s.ResultFile == sealed.File
	}
	return false
}
