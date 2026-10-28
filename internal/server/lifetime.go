package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// RunError retains an original server lifetime whose ownership is unsettled.
// The caller must retain this error and stop; returning it does not transfer
// closure authority or permit another Run against the same resources. Its text
// omits paths and identity. Unwrap exposes the original cause for diagnostics.
// A retained pointer after a failed Close is not proof it remains open.
type RunError struct {
	stage string
	cause error
	frame *serverRun
}

func (e *RunError) Error() string { return "server lifetime: " + e.stage }

// Unwrap returns the exact original failure chain.
func (e *RunError) Unwrap() error { return e.cause }

// One synchronous server owner retains every dependency, original function and
// cancellation handle. No retry worker, late closer or replacement join exists.
type serverRun struct {
	cfg                          Config
	store                        *store.Store
	localBackend                 *local.Backend
	files                        *artifact.Files
	root                         *os.Root
	ownedFiles                   bool
	lifetime                     local.ServerLifetime
	captureErr                   error
	control                      *control.Control
	engine                       *auth.Engine
	targets                      []backend.Target
	rpcHTTP                      *rpc.HTTPHandler
	outer                        *artifactHTTPDrain
	listener                     *serverListener
	http                         *http.Server
	ready                        atomic.Bool
	cancelRun, cancelAuth        context.CancelFunc
	runDone, serveDone, authDone chan struct{}
	runErr, serveErr             error
	integration                  Integration
	integrationDone              chan struct{}
	integrationErr               error
	held                         *RunError
}

func (f *serverRun) hold(stage string, cause error) error {
	if f.held == nil {
		f.held = &RunError{stage: stage, cause: cause, frame: f}
	}
	return f.held
}

// acceptServerLifetime checks the capture's complete union before Store opens.
// In particular, an uncertain original Close is never ordinary unavailability.
func acceptServerLifetime(value local.ServerLifetime, err error) error {
	var uncertain *local.ServerLifetimeCloseError
	if errors.As(err, &uncertain) {
		return err
	}
	if err != nil {
		if value == (local.ServerLifetime{}) && errors.Is(err, local.ErrServerLifetimeUnavailable) {
			return nil
		}
		return err
	}
	validID := func(s string) bool {
		if len(s) != 32 {
			return false
		}
		nonzero := false
		for _, c := range s {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return false
			}
			if c != '0' {
				nonzero = true
			}
		}
		return nonzero
	}
	if !validID(value.Host) || !validID(value.Boot) || value.NamespaceInode == 0 || value.PID == 0 || value.PID > 1<<31-1 {
		return errors.New("invalid server lifetime capture")
	}
	return nil
}

// openArtifacts reuses the stopped initializer's private-directory identity
// checks and actual Files marker verifier. It creates no directory or marker.
// Store metadata and the opened original marker must agree before configuration.
func (f *serverRun) openArtifacts(directory string) error {
	installed, err := f.store.ArtifactInstallation()
	if err != nil {
		return err
	}
	if !installed.Enabled {
		return errors.New("artifact metadata not installed")
	}
	name, err := filepath.Abs(directory)
	if err != nil {
		return err
	}
	before, err := os.Lstat(name)
	if err != nil {
		return err
	}
	identity, err := artifactInitializerIdentity(before)
	if err != nil {
		return err
	}
	f.root, err = os.OpenRoot(name)
	if err != nil {
		return err
	}
	opened, err := f.root.Lstat(".")
	if err != nil {
		return err
	}
	actual, err := artifactInitializerIdentity(opened)
	if err != nil {
		return err
	}
	if actual != identity || !os.SameFile(before, opened) {
		return artifact.ErrFileConflict
	}
	// Even a nil Files result may carry its original owner in a typed Close
	// error. closeStartup classifies the complete chain before any lower close.
	f.files, err = artifact.Open(f.root, identity)
	if f.files != nil {
		f.ownedFiles = true
	}
	if err != nil {
		return err
	}
	facts, err := f.files.RootFacts()
	if err != nil {
		return err
	}
	if facts.ID != installed.ID || facts.Unit != installed.Unit || facts.Volume != installed.Volume || facts.File != identity {
		return artifact.ErrFileConflict
	}
	return nil
}

// closeStartup runs only before original Run/auth/Serve functions are launched.
// Ordinary validation failures release positively owned resources. Any original
// Close uncertainty stops before closing its dependencies, including nil-result
// acquisitions whose error retains the actual hidden owner.
func (f *serverRun) closeStartup(cause error) error {
	if f.held != nil {
		return f.held
	}
	if f.runDone != nil || f.serveDone != nil || f.authDone != nil || f.integrationDone != nil {
		return f.hold("startup functions already launched", cause)
	}
	var uncertain *local.ServerLifetimeCloseError
	if errors.As(cause, &uncertain) || errors.Is(cause, artifact.ErrFileCloseUncertain) {
		return f.hold("startup owner remains held", cause)
	}
	if f.listener != nil {
		if err := f.listener.Close(); err != nil {
			return f.hold("startup listener close", errors.Join(cause, err))
		}
		f.listener = nil
	}
	if err := f.closeIntegration(cause); err != nil {
		return err
	}
	return f.closeResources(cause)
}

func waitServerOwner(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *serverRun) shutdown(cause error) error {
	f.ready.Store(false)
	if err := f.control.BeginShutdown(); err != nil {
		return f.hold("request preparation", errors.Join(cause, err))
	}
	f.outer.Stop()
	phase, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.http.Shutdown(phase); err != nil {
		cause = errors.Join(cause, err)
		// Exactly one HTTP Close attempt. Its return does not join handlers or
		// sticky RPC bodies, and its failure prevents every dependency close.
		if closeErr := f.http.Close(); closeErr != nil {
			return f.hold("http close", errors.Join(cause, closeErr))
		}
	}
	if err := waitServerOwner(phase, f.serveDone); err != nil {
		return f.hold("original serve remains held", errors.Join(cause, err))
	}
	if f.serveErr != nil && !errors.Is(f.serveErr, http.ErrServerClosed) {
		cause = errors.Join(cause, f.serveErr)
	}
	// net/http may untrack its listener after a failed Close, and a later
	// Server.Close can then return nil. Only our same original owner records
	// whether that one underlying Close actually returned nil, including the
	// deferred Close on an unexpected Serve return.
	returned, closeErr := f.listener.closeResult()
	if !returned || closeErr != nil {
		return f.hold("original listener closure remains held", errors.Join(cause, closeErr))
	}
	if err := waitServerOwner(phase, f.outer.done); err != nil {
		return f.hold("original http handlers remain held", errors.Join(cause, err))
	}
	// This existing RPC API deliberately has no deadline. An uncertain body
	// may retain this same synchronous server frame forever; no background
	// waiter or timeout can manufacture its positive original join.
	f.rpcHTTP.WaitArtifactHTTP()
	prepare, cancelPrepare := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelPrepare()
	if err := f.control.WaitShutdownPrepared(prepare); err != nil {
		return f.hold("original preparation remains held", errors.Join(cause, err))
	}
	// An integration's artifact I/O is released only once its effects stop,
	// so they are stopped before the drain waits for that I/O.
	if stopper, ok := f.integration.(EffectStopper); ok {
		stopper.StopEffects()
	}
	f.control.StopArtifactAdmission()
	drain, cancelDrain := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDrain()
	if err := f.control.DrainArtifacts(drain); err != nil {
		return f.hold("original artifact drain remains held", errors.Join(cause, err))
	}
	f.cancelRun()
	join, cancelJoin := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelJoin()
	if err := waitServerOwner(join, f.runDone); err != nil {
		return f.hold("original run remains held", errors.Join(cause, err))
	}
	if err := f.control.Join(join); err != nil {
		return f.hold("original worker remains held", errors.Join(cause, err))
	}
	if f.runErr != nil && !errors.Is(f.runErr, context.Canceled) {
		return f.hold("original run failed", errors.Join(cause, f.runErr))
	}
	if f.integrationDone != nil {
		if err := waitServerOwner(join, f.integrationDone); err != nil {
			return f.hold("original integration run remains held", errors.Join(cause, err))
		}
		if err := f.integrationErr; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(cause, err) {
			cause = errors.Join(cause, err)
		}
	}
	if err := f.closeIntegration(cause); err != nil {
		return err
	}
	// Close is cooperative and may outlast join, so the same authentication
	// loop gets its own ten-second join; its cancellation is not its return.
	f.cancelAuth()
	authJoin, cancelAuthJoin := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAuthJoin()
	if err := waitServerOwner(authJoin, f.authDone); err != nil {
		return f.hold("original authentication remains held", errors.Join(cause, err))
	}
	return f.closeResources(cause)
}

// closeIntegration closes the installed original once, after Control and the
// integration's own Run have returned and before authentication stops. A
// failure is sticky and closes nothing the integration depends on. The
// ten-second ctx is cooperative: Close is called synchronously, and no timer
// or goroutine takes its place if it does not return.
func (f *serverRun) closeIntegration(cause error) error {
	if f.integration == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := f.integration.Close(ctx); err != nil {
		return f.hold("integration closure remains held", errors.Join(cause, err))
	}
	f.integration = nil
	return nil
}

// Each nonnil original is closed at most once. A failure is sticky and halts
// the sequence. Borrowed Files and additional targets are never closed here.
func (f *serverRun) closeResources(cause error) error {
	if f.held != nil {
		return f.held
	}
	if f.localBackend != nil {
		if err := f.localBackend.Close(); err != nil {
			return f.hold("local closure remains held", errors.Join(cause, err))
		}
		f.localBackend = nil
	}
	if f.ownedFiles && f.files != nil {
		if err := f.files.Close(); err != nil {
			return f.hold("files closure remains held", errors.Join(cause, err))
		}
		f.files = nil
	}
	if f.root != nil {
		if err := f.root.Close(); err != nil {
			return f.hold("root closure remains held", errors.Join(cause, err))
		}
		f.root = nil
	}
	if f.store != nil {
		if err := f.store.Close(); err != nil {
			return f.hold("state closure failed", errors.Join(cause, err))
		}
		f.store = nil
	}
	return cause
}

// serverListener retains the original net.Listen result and its one actual
// Close outcome across net/http's listener tracking and deferred Close paths.
// Nil from a later Server.Close cannot replace a recorded original error.
type serverListener struct {
	net.Listener
	once     sync.Once
	mu       sync.Mutex
	returned bool
	err      error
}

func (l *serverListener) Close() error {
	l.once.Do(func() {
		err := l.Listener.Close()
		l.mu.Lock()
		l.returned, l.err = true, err
		l.mu.Unlock()
	})
	_, err := l.closeResult()
	return err
}

// returned means the original Close returned, not that its outcome was positive.
// No mutex is held during native I/O, and this observation performs no retry.
func (l *serverListener) closeResult() (returned bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.returned, l.err
}
