package control

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/local"
)

// artifactIO is an original internal materialization/collection owner, not an
// HTTP call. Phase2's concrete Local adapter must finish it only after its actual
// original I/O returns. The shared map is bounded with calls, not separately.
type artifactIO struct {
	service   *artifactService
	finished  bool
	fileError error
}

func (c *Control) tryArtifactIO() (*artifactIO, error) {
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
	owner := &artifactIO{service: s}
	s.workspaceIO[owner] = struct{}{}
	return owner, nil
}

func (owner *artifactIO) finish(err error) {
	if owner == nil || owner.service == nil {
		return
	}
	s := owner.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workspaceIO[owner]; !ok || owner.finished {
		return
	}
	owner.finished = true
	if artifactCloseUncertain(err) {
		owner.fileError = err
		s.artifactChangedLocked()
		return
	}
	delete(s.workspaceIO, owner)
	s.artifactChangedLocked()
}

// Native observation and self-capture errors have their own concrete original
// File owners; Files' sentinel alone cannot classify their retained custody.
func artifactCloseUncertain(err error) bool {
	var queryClose *local.ServerStoppedCloseError
	var captureClose *local.ServerLifetimeCloseError
	return errors.Is(err, artifact.ErrFileCloseUncertain) || errors.As(err, &queryClose) || errors.As(err, &captureClose)
}

// StopArtifactAdmission stops new request, download and internal I/O owners.
// Existing owners retain their original completion obligations. It performs no
// filesystem or Store work and is safe to call repeatedly.
func (c *Control) StopArtifactAdmission() {
	s := c.artifacts
	if s == nil {
		return
	}
	c.requestPhysicalClose()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closing {
		s.closing = true
		s.artifactChangedLocked()
	}
}

// artifactChangedLocked signals actual ownership transitions. terminal closes
// only after stopped admission and zero original request, I/O and descriptor
// owners. No context or deadline can close it on an owner's behalf.
func (s *artifactService) artifactChangedLocked() {
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
	if s.terminal == nil {
		s.terminal = make(chan struct{})
	}
	if s.closing && !s.terminalClosed && len(s.calls) == 0 && len(s.workspaceIO) == 0 && len(s.downloads) == 0 && !s.physicalWorkPending && !s.physicalFrameRegistered && s.physicalHeld == nil {
		close(s.terminal)
		s.terminalClosed = true
	}
}

// Called only under service.mu; original error pointers remain retained by
// their actual owners. No cache lock, I/O or replacement close occurs here.
func (s *artifactService) artifactUnresolvedLocked() error {
	if s.physicalHeld != nil {
		return s.physicalHeld
	}
	for call := range s.calls {
		if call.worker != nil && call.worker.err != nil {
			return call.worker.err
		}
		if call.fileError != nil {
			return call.fileError
		}
	}
	for owner := range s.workspaceIO {
		if owner.fileError != nil {
			return owner.fileError
		}
	}
	return nil
}

// DrainArtifacts stops admission, joins original request/internal I/O owners,
// then settles at most32 original downloads. It never closes borrowed Files or
// roots. Cancellation while waiting returns Held without clearing owners or
// inventing closure. A genuinely terminal service can return success.
// An actual kernel call may block: no replacement goroutine or syscall wall-time
// guarantee is provided. The caller must retain this resource frame on failure.
func (c *Control) DrainArtifacts(ctx context.Context) error {
	c.StopArtifactAdmission()
	s := c.artifacts
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return ErrArtifactBusy
	}
	s.draining = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.draining = false; s.mu.Unlock() }()
	for {
		s.mu.Lock()
		unresolved := s.artifactUnresolvedLocked()
		if unresolved != nil {
			s.mu.Unlock()
			return fmt.Errorf("%w: artifact owner remains held: %w", ErrUnavailable, unresolved)
		}
		if len(s.calls) == 0 && len(s.workspaceIO) == 0 {
			s.mu.Unlock()
			break
		}
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("%w: artifact owners remain held: %w", ErrUnavailable, ctx.Err())
		}
	}
	s.mu.Lock()
	downloads := make([]*ArtifactDownload, 0, len(s.downloads))
	for d := range s.downloads {
		if d.remote != nil && (d.reader == nil || d.owners != 0 || d.uncertain || d.settling || d.closing || d.finished || d.remote.call != nil || d.remote.finish != nil || d.remote.published >= d.ref.Size) {
			s.mu.Unlock()
			return fmt.Errorf("%w: original remote descriptor handoff remains held", ErrUnavailable)
		}
		downloads = append(downloads, d)
	}
	s.mu.Unlock()
	sort.Slice(downloads, func(i, j int) bool { return downloads[i].ID() < downloads[j].ID() })
	for _, d := range downloads {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: artifact descriptors remain held: %w", ErrUnavailable, err)
		}
		if err := d.closeOriginal(nil); err != nil {
			return fmt.Errorf("%w: original download remains held: %w", ErrUnavailable, err)
		}
	}
	for {
		s.mu.Lock()
		if err := s.artifactUnresolvedLocked(); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("%w: artifact owner remains held: %w", ErrUnavailable, err)
		}
		s.artifactChangedLocked()
		if s.terminalClosed {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("%w: artifact drain remains held: %w", ErrUnavailable, ctx.Err())
		}
	}
}
