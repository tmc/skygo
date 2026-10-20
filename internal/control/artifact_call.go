package control

import (
	"errors"
	"sync"

	"github.com/tmc/skygo/internal/artifact"
)

// ErrArtifactBusy means all artifact I/O owners or the selected upload are held.
// A timeout or disconnected client does not release an original I/O owner.
var ErrArtifactBusy = errors.New("artifact operation busy")

// ArtifactCall retains one of four shared artifact operation slots. One actual
// operation may use it once. The caller must call Done after the enclosing HTTP
// response has finished, even if the operation returned an error.
// The zero value does not own a slot.
type ArtifactCall struct {
	service          *artifactService
	used             bool
	ioActive         bool
	uploadTransition bool
	httpDone         bool
	fileError        error
	upload           string
	download         *ArtifactDownload
}

// artifactService's mutex protects only bounded in-memory ownership. It never
// encloses Store/auth calls, filesystem operations, or HTTP writes.
type artifactService struct {
	physical                artifactPhysical
	files                   *artifact.Files
	root                    artifact.RootFacts
	mu                      sync.Mutex
	calls                   map[*ArtifactCall]struct{}
	uploads                 map[string]*ArtifactCall
	closing                 bool
	downloads               map[*ArtifactDownload]struct{}
	workspaceIO             map[*artifactIO]struct{}
	changed                 chan struct{}
	terminal                chan struct{}
	terminalClosed          bool
	draining                bool
	physicalWorkPending     bool
	physicalFrameRegistered bool
	physicalHeld            error
}

// TryArtifactCall acquires an artifact operation slot without waiting. An
// installed service has at most four original calls, including HTTP ownership.
func (c *Control) TryArtifactCall() (*ArtifactCall, error) {
	if err := c.beginExternal(); err != nil {
		return nil, err
	}
	defer c.endExternal()
	s := c.artifacts
	if s == nil {
		return nil, ErrUnsupported
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrUnavailable
	}
	if len(s.calls)+len(s.workspaceIO) >= 4 {
		return nil, ErrArtifactBusy
	}
	call := &ArtifactCall{service: s}
	s.calls[call] = struct{}{}
	return call, nil
}

// Done records actual HTTP completion, not I/O completion. Calling it early
// closes admission on this call but retains its slot and upload ownership until
// its original operation has actually returned with proven descriptor closure.
// An uncertain original close remains held. It is safe to call repeatedly.
func (a *ArtifactCall) Done() {
	if a == nil || a.service == nil {
		return
	}
	s := a.service
	s.mu.Lock()
	defer s.mu.Unlock()
	a.httpDone = true
	a.releaseLocked()
}

func (a *ArtifactCall) releaseLocked() {
	if !a.httpDone || a.ioActive || a.fileError != nil {
		return
	}
	if _, ok := a.service.calls[a]; !ok {
		return
	}
	delete(a.service.calls, a)
	if a.download != nil {
		a.download.owners--
		a.download = nil
	}
	if a.upload != "" && a.service.uploads[a.upload] == a {
		delete(a.service.uploads, a.upload)
	}
	a.service.artifactChangedLocked()
}

// beginArtifactCall is paired only with the actual synchronous method's defer.
// No context watcher or HTTP callback can invoke the returned I/O completion.
func (c *Control) beginArtifactCall(a *ArtifactCall, upload string) (func(), error) {
	if a == nil || a.service == nil || a.service != c.artifacts {
		return nil, ErrInvalid
	}
	s := a.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.calls[a]; !ok || a.used || a.httpDone {
		return nil, ErrInvalid
	}
	a.used = true
	if upload != "" {
		if s.uploads[upload] != nil {
			return nil, ErrArtifactBusy
		}
		s.uploads[upload] = a
		a.upload = upload
	}
	a.ioActive = true
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		a.ioActive = false
		a.releaseLocked()
	}, nil
}

// beginUploadStructure fences only an actual subject/identity/allowance change.
// The synchronous admitted caller finishes it after its original operation
// returns. Settled same-inode append/retransmit never enters this phase.
func (c *Control) beginUploadStructure(a *ArtifactCall) func() {
	s := a.service
	s.mu.Lock()
	a.uploadTransition = true
	s.artifactChangedLocked()
	s.mu.Unlock()
	c.physicalChanged()
	return func() {
		s.mu.Lock()
		a.uploadTransition = false
		s.artifactChangedLocked()
		s.mu.Unlock()
		c.physicalChanged()
	}
}

// artifactFileError retains the actual Files error and its original descriptor
// owner only when the concrete adapter says closure is unproved. Ordinary
// positively closed errors and subsequent Store CAS failures remain recoverable.
// No request callback can settle this hold or manufacture a joined receipt.
func (c *Control) artifactFileError(a *ArtifactCall, err error) error {
	if errors.Is(err, artifact.ErrFileCloseUncertain) || errors.Is(err, artifact.ErrFileConflict) {
		c.physicalChanged()
	}
	if !errors.Is(err, artifact.ErrFileCloseUncertain) {
		return err
	}
	if a == nil || a.service == nil || a.service != c.artifacts {
		return err
	}
	s := a.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.calls[a]; ok && a.used && a.ioActive && a.fileError == nil {
		a.fileError = err
		s.artifactChangedLocked()
	}
	return err
}
