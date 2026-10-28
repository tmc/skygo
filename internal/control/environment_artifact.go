package control

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/store"
)

// ArtifactProducer returns the server lifetime this Control's artifact reads
// are admitted under. A lease OpenEnvironmentArtifact opens has this producer.
func (c *Control) ArtifactProducer() store.OriginalServerLifetime { return c.producer }

// OpenEnvironmentArtifact opens the exact file of an environment
// materialization read the Store admitted under this Control's producer. It
// serves trusted in-process materialization only: it is not an HTTP or RPC
// operation, and the caller authorizes the environment's principal.
//
// It first reserves one of the four internal I/O permits it shares with
// artifact calls, waiting while all are held until ctx ends, and refuses once
// artifact admission has stopped. It then opens the lease's file and verifies
// all of its bytes against the lease's reference. These bounded verification
// reads happen under the permit; no byte reaches the caller before
// OpenEnvironmentArtifact returns.
//
// The body yields the bytes once, in order, in reads of at most 64 KiB:
// exactly the reference's Size bytes, then io.EOF. Its Close returns after any
// Read has returned and the file is closed, and only then releases the
// permit. A Close error means the file may still be open: the permit stays
// held, and the caller must keep the read protected.
//
// An error that matches artifact.ErrFileCloseUncertain means a file may still
// be open: the permit stays held, the error retains the original owner, and
// the caller must keep both and keep the read protected. Any other error
// means that nothing is open and no permit is held.
func (c *Control) OpenEnvironmentArtifact(ctx context.Context, lease store.ArtifactLease) (io.ReadCloser, error) {
	s := c.artifacts
	if s == nil {
		return nil, ErrUnsupported
	}
	if !jobName.MatchString(lease.ID) {
		return nil, ErrInvalid
	}
	ref, err := artifactPortableRef(lease.Reference)
	if err != nil {
		return nil, err
	}
	if lease.Closing || lease.Producer != c.producer || lease.RootID != s.root.ID || lease.File.Device != s.root.File.Device || lease.File.Inode == 0 {
		return nil, ErrUnavailable
	}
	owner, err := c.waitArtifactIO(ctx)
	if err != nil {
		return nil, err
	}
	reader, err := s.files.OpenObject(ref, artifact.FileID{Device: lease.File.Device, Inode: lease.File.Inode})
	return c.openedEnvironmentArtifact(owner, reader, ref.Size, err)
}

// openedEnvironmentArtifact settles owner for one OpenObject result. A nil
// reader is not proof that nothing is open: an uncertain close keeps owner,
// with its original error.
func (c *Control) openedEnvironmentArtifact(owner *artifactIO, reader *artifact.Reader, size int64, err error) (io.ReadCloser, error) {
	if err != nil {
		owner.finish(err)
		return nil, c.artifactFileError(nil, err)
	}
	return &environmentArtifact{control: c, owner: owner, reader: reader, size: size}, nil
}

// waitArtifactIO is tryArtifactIO, waiting while every permit is held until
// ctx ends.
func (c *Control) waitArtifactIO(ctx context.Context) (*artifactIO, error) {
	s := c.artifacts
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.changed == nil {
			s.changed = make(chan struct{})
		}
		changed := s.changed
		s.mu.Unlock()
		owner, err := c.tryArtifactIO()
		if !errors.Is(err, ErrArtifactBusy) {
			return owner, err
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// environmentArtifact is the body OpenEnvironmentArtifact returns: one
// verified reader, read in order, and the permit it holds.
type environmentArtifact struct {
	control *Control
	owner   *artifactIO

	mu       sync.Mutex
	reader   *artifact.Reader
	size     int64
	offset   int64
	readErr  error
	closed   bool
	closeErr error
}

// Read reads the next bytes, at most 64 KiB. After an error it returns that
// error again and no bytes.
func (a *environmentArtifact) Read(p []byte) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, os.ErrClosed
	}
	if a.readErr != nil {
		return 0, a.readErr
	}
	if len(p) == 0 {
		return 0, nil
	}
	if a.offset == a.size {
		return 0, io.EOF
	}
	want := min(len(p), 64<<10)
	if int64(want) > a.size-a.offset {
		want = int(a.size - a.offset)
	}
	n, err := a.reader.ReadAt(p[:want], a.offset)
	if err == io.EOF && n == want {
		err = nil
	}
	if err == nil && n != want {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		// The reader's bytes are not trusted after an error.
		a.readErr = err
		return 0, err
	}
	a.offset += int64(n)
	return n, nil
}

// Close waits for any Read, closes the reader, and releases the permit only
// if that close succeeded. A later Close returns the same result.
func (a *environmentArtifact) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return a.closeErr
	}
	a.closed = true
	err := a.reader.Close()
	a.owner.finish(err)
	a.closeErr = a.control.artifactFileError(nil, err)
	return a.closeErr
}
