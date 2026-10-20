package control

import (
	"context"
	"fmt"
	"sync"

	"github.com/tmc/skygo/internal/backend"
)

// shutdownState is one constructor-owned lifecycle, not an operation registry.
// Its lock never encloses auth, Store, backend, service/cache locks or callbacks.
type shutdownState struct {
	mu             sync.Mutex
	externalClosed bool
	external       uint64
	request        chan struct{}
	changed        chan struct{}
	prepared       chan struct{}
	preparedClosed bool
	failure        error
}

func newShutdownState() *shutdownState {
	return &shutdownState{request: make(chan struct{}), changed: make(chan struct{}), prepared: make(chan struct{})}
}

func (s *shutdownState) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (c *Control) beginExternal() error {
	if c == nil || c.shutdown == nil {
		return fmt.Errorf("%w: control lifecycle is not initialized", ErrUnavailable)
	}
	s := c.shutdown
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.externalClosed || s.external == ^uint64(0) {
		return ErrUnavailable
	}
	s.external++
	return nil
}

func (c *Control) endExternal() {
	s := c.shutdown
	s.mu.Lock()
	defer s.mu.Unlock()
	s.external--
	s.notifyLocked()
}

// BeginShutdown stops new submissions and ArtifactCall registration and requests
// preparation by the original Run. Already admitted calls finish normally.
// It does not stop internal I/O, signal backends, close resources or acknowledge
// preparation. NewWithConfig must have constructed this Control; Run may follow.
func (c *Control) BeginShutdown() error {
	if c == nil || c.shutdown == nil {
		return fmt.Errorf("%w: control lifecycle is not initialized", ErrUnavailable)
	}
	s := c.shutdown
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.externalClosed {
		s.externalClosed = true
		close(s.request)
		s.notifyLocked()
	}
	return nil
}

// WaitShutdownPrepared waits for the original Run's preparation receipt. The
// caller must separately stop/join its external HTTP/direct callers. A deadline
// neither cancels Run nor joins producers, and a failed Run never prepares.
// After success, terminal artifact drain and final resource joins remain due.
func (c *Control) WaitShutdownPrepared(ctx context.Context) error {
	if c == nil || c.shutdown == nil {
		return fmt.Errorf("%w: control lifecycle is not initialized", ErrUnavailable)
	}
	s := c.shutdown
	for {
		s.mu.Lock()
		prepared, failed, requested := s.preparedClosed, s.failure, s.externalClosed
		changed, ready := s.changed, s.prepared
		s.mu.Unlock()
		if prepared {
			return nil
		}
		if failed != nil {
			return fmt.Errorf("%w: original preparation failed: %w", ErrUnavailable, failed)
		}
		if !requested {
			return fmt.Errorf("%w: shutdown preparation was not requested", ErrUnavailable)
		}
		select {
		case <-ready:
			return nil
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("%w: original preparation remains held: %w", ErrUnavailable, ctx.Err())
		}
	}
}

func (c *Control) shutdownRunReturned(err error) {
	s := c.shutdown
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.preparedClosed && s.failure == nil {
		if err == nil {
			err = fmt.Errorf("%w: original Run returned before preparation", ErrUnavailable)
		}
		s.failure = err
		s.notifyLocked()
	}
}

func (c *Control) shutdownEvents() (<-chan struct{}, <-chan struct{}) {
	s := c.shutdown
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.request, s.changed
}

func (c *Control) configureOwnedLocal(config Config) error {
	b := config.LocalBackend
	if config.LocalWorkspace != nil && config.LocalWorkspace.Backend != b {
		return fmt.Errorf("workspace requires the same owned local backend")
	}
	if b == nil {
		return nil
	}
	binding, err := backend.LocalBinding(c.store.Owner())
	if err != nil {
		return err
	}
	count := 0
	for _, t := range c.targets {
		if t.Backend == b {
			count++
			if t.Binding != binding {
				return fmt.Errorf("owned local backend binding differs from installation")
			}
		}
	}
	if count != 1 {
		return fmt.Errorf("owned local backend requires one actual target pointer")
	}
	c.localBackend = b
	return nil
}
