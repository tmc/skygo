package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

var validID = regexp.MustCompile(`^[0-9a-f]{32}$`)

// Backend owns programs launched during one server lifetime. Use New to create one.
type Backend struct {
	mu        sync.Mutex
	directory string
	jobs      map[string]*execution
	closed    bool
	// One failed pre-execution Close blocks further ordinary output creation.
	// These retain the original Go owner/error, not a recoverable numeric FD.
	unstartedOutput    *output
	unstartedOutputErr error
	// The first uncertain public log-reader Close blocks opens and removal.
	logReaderErr        *logReaderCloseError
	retryOps            retryProcessOps
	retryAvailable      bool
	defaultOps          defaultProcessOps
	retention           *retentionState
	workspaces          *workspaceState
	workspaceCleanupOps workspaceCleanupOps
	resources           resourceState
	// durable is set once by EnableDurable, before any Start.
	durable *durableState
}

type execution struct {
	cmd       *exec.Cmd
	status    backend.Status
	done      chan struct{}
	output    *output
	retry     *retryExecution
	owned     *defaultExecution
	retained  *retainedOutput
	workspace *workspaceExecution
	phase     *workspacePhase
	// resources is released under Backend.mu only after positive closure.
	resources *resourceReservation
	// workspaceClosed records positive original Local producer/cleanup-handle
	// closure before detachment, under Backend.mu; no namespace/Store credit.
	workspaceClosed bool
	// Final ordinary execution snapshot, guarded by Backend.mu. Diagnostics
	// survive payload shedding independently of positive physical closure.
	shutdownPublished, shutdownClosed bool
	shutdownErr                       error
	stdioWriter                       *os.File
	stdioWriterErr                    error
	readerCloseOnce                   sync.Once
	readerCloseErr                    error
}

// closeReader closes the one original pipe reader at most once. Competing
// waiter/pump callers must not hold Backend.mu while joining the same Close.
// A cached error retains the original Go owner, not a recoverable numeric FD.
func (e *execution) closeReader() error {
	e.readerCloseOnce.Do(func() {
		if e.owned != nil {
			e.readerCloseErr = e.owned.reader.Close()
		} else {
			e.readerCloseErr = e.retry.reader.Close()
		}
	})
	return e.readerCloseErr
}

// closeWriter is called only by the original startup owner before producers
// launch. An uncertain original Close is never retried or discarded.
func (e *execution) closeWriter() error {
	e.stdioWriterErr = e.stdioWriter.Close()
	if e.stdioWriterErr == nil {
		e.stdioWriter = nil
	}
	return e.stdioWriterErr
}

type output struct {
	mu             sync.Mutex
	file           *os.File
	marker         string
	size           int64
	truncated      bool
	err            error
	markerCloseErr error
	markerFile     *os.File
	closed         bool
	retained       *retainedOutput
}

// New opens a private directory for retained output.
func New(directory string) (*Backend, error) {
	if !supported() {
		return nil, errors.New("local execution requires Linux or macOS")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, fmt.Errorf("create logs: %w", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return nil, fmt.Errorf("protect logs: %w", err)
	}
	return &Backend{directory: directory, jobs: make(map[string]*execution), retryOps: systemRetryProcessOps(), retryAvailable: retrySupported(), defaultOps: systemDefaultProcessOps()}, nil
}

// Check rejects requirements that local processes cannot enforce and
// resource requests this host could never satisfy. It probes the host,
// including the GPU inventory tool when accelerators are requested, but
// ignores reservations held by running executions.
func (b *Backend) Check(t backend.Task) error {
	if err := b.checkTask(t); err != nil {
		return err
	}
	return b.checkResources(t)
}

func (b *Backend) checkTask(t backend.Task) error {
	if err := backend.ValidateWorkspace(t); err != nil {
		return err
	}
	if t.Workdir != nil && !b.SupportsWorkspace() {
		return errors.New("local workspace ownership is unavailable")
	}
	policy, err := backend.NormalizeRetry(t.Retry)
	if err != nil {
		return err
	}
	if policy != nil && !b.SupportsRetry() {
		return errors.New("local retries require a qualified nonreaping wait")
	}
	if len(t.Argv) == 0 || t.Argv[0] == "" {
		return errors.New("argv must contain a program")
	}
	if t.Image != "" {
		return fmt.Errorf("%w: images", ErrUnsupported)
	}
	if err := validateResources(t); err != nil {
		return err
	}
	if t.CPULimitMillis != 0 || t.MemoryLimitBytes != 0 {
		return fmt.Errorf("%w: resource limits", ErrUnsupported)
	}
	if len(t.NodeSelector) != 0 {
		return fmt.Errorf("%w: node selectors", ErrUnsupported)
	}
	if t.Directory != "" && !filepath.IsAbs(t.Directory) {
		return errors.New("directory must be absolute")
	}
	if t.TimeoutSeconds < 0 || t.TimeoutSeconds > int64((1<<63-1)/time.Second) {
		return errors.New("invalid timeout")
	}
	for _, arg := range t.Argv {
		if strings.ContainsRune(arg, 0) {
			return errors.New("argv contains NUL")
		}
	}
	for k, v := range t.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return errors.New("invalid environment")
		}
	}
	return nil
}

// Start starts a job once. Its context only controls startup. Start probes
// the host and reserves requested resources for the execution; it reports
// backend.ErrResourcesUnavailable when they are held by other executions.
func (b *Backend) Start(ctx context.Context, id string, t backend.Task) error {
	if !validID.MatchString(id) {
		return errors.New("invalid job ID")
	}
	if err := b.checkTask(t); err != nil {
		return err
	}
	if t.Workdir != nil {
		return b.startWorkspace(ctx, id, t)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inv, err := b.probeHost(ctx, t)
	if err != nil {
		return err
	}
	b.mu.Lock()
	durable := b.durableEligible(t)
	b.mu.Unlock()
	if durable {
		return b.startDurable(ctx, id, t, inv)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return errors.New("local backend is closed")
	}
	if b.unstartedOutput != nil {
		return fmt.Errorf("local output close remains held: %w", b.unstartedOutputErr)
	}
	if _, ok := b.jobs[id]; ok {
		return errors.New("job already started")
	}
	held, err := b.reserveResources(id, t, inv)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			b.releaseResources(held) // no process was started under it
		}
	}()
	if err := b.admitOutput(ctx, id, t); err != nil {
		return err
	}
	// Exclusive creation also fences a repeated start after backend restart.
	f, err := os.OpenFile(filepath.Join(b.directory, id+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create job output: %w", err)
	}
	out := &output{file: f, marker: filepath.Join(b.directory, id+".truncated")}
	if b.retention != nil {
		out.retained = b.retention.outputs[id]
		info, err := f.Stat()
		if err != nil {
			return errors.Join(fmt.Errorf("identify output file: %w", err), b.closeUnstartedOutput(out))
		}
		out.retained.log = info
	}
	cmd := exec.Command(t.Argv[0], t.Argv[1:]...)
	cmd.Dir = t.Directory
	cmd.Env = os.Environ()
	for k, v := range t.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Env = visibleDevices(cmd.Env, held)
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.WaitDelay = 250 * time.Millisecond
	configure(cmd)
	preflight := cmd.Err != nil
	if t.Retry != nil && t.Retry.MaxAttempts > 1 {
		err = b.startRetry(cmd, out, id, t.TimeoutSeconds)
	} else {
		err = b.startDefault(cmd, out, id, t.TimeoutSeconds)
	}
	installed = b.installResources(id, held, preflight)
	return err
}

func (b *Backend) stop(e *execution, state backend.State, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e.workspace != nil {
		return b.stopWorkspace(e.workspace, state, reason)
	}
	if e.retry != nil {
		select {
		case <-e.done:
			return nil
		default:
		}
		return b.stopRetry(e, state, reason)
	}
	if e.owned != nil {
		return b.stopDefault(e, state, reason)
	}
	return nil
}

// Observe reports executions owned by this backend instance and, when durable
// execution is enabled, durable executions started by any earlier instance.
func (b *Backend) Observe(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	id := ref.ID
	if err := ctx.Err(); err != nil {
		return backend.Status{}, err
	}
	if !validID.MatchString(id) {
		return backend.Status{}, errors.New("invalid job ID")
	}
	b.mu.Lock()
	e := b.jobs[id]
	if e == nil {
		b.mu.Unlock()
		if s, ok, err := b.observeDurable(ref); ok {
			return s, err
		}
		return backend.Status{State: backend.Unknown, Reason: "execution is not owned by this server"}, nil
	}
	defer b.mu.Unlock()
	if err := checkInstance(e, ref.Instance); err != nil {
		return backend.Status{}, err
	}
	if e.workspace != nil && !e.workspace.committed {
		// Terminal outcome stays in WorkspaceObservation until trusted commit.
		// Before then report only the original parent's Starting/Running phase.
		state := backend.Running
		if e.status.State == backend.Starting {
			state = backend.Starting
		}
		return backend.Status{State: state, Instance: e.status.Instance}, nil
	}
	select {
	case <-e.done:
	default:
		return backend.Status{State: backend.Running, Instance: e.status.Instance}, nil
	}
	s := e.status
	// Bound the public diagnostic independently of original-owner custody.
	// The held execution and its exact errors remain unchanged.
	s.Reason = strings.Clone(retryReason(s.Reason))
	if s.ExitCode != nil {
		code := *s.ExitCode
		s.ExitCode = &code
	}
	return s, nil
}

// Cancel requests termination of the owned process group. For a durable
// execution it records the request and waits for the supervisor's terminal
// status, bounded by ctx.
func (b *Backend) Cancel(ctx context.Context, ref backend.Reference) error {
	id := ref.ID
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validID.MatchString(id) {
		return errors.New("invalid job ID")
	}
	b.mu.Lock()
	e := b.jobs[id]
	if e != nil {
		if err := checkInstance(e, ref.Instance); err != nil {
			b.mu.Unlock()
			return err
		}
	}
	b.mu.Unlock()
	if e == nil {
		if ok, err := b.cancelDurable(ctx, ref); ok {
			return err
		}
		return errors.New("execution is not owned by this server")
	}
	return b.stop(e, backend.Canceled, "canceled by operator")
}

// Close kills owned groups and uses a three-second timer for execution joins.
// Mutex acquisition, file I/O and cancellation callbacks have no time bound.
// An uncertain original log-reader Close remains held and is not retried.
// Workspace roots retain original ownership until explicit cleanup; Ready or a
// durable completion acknowledgement alone cannot make their drain successful.
// Durable executions are detached, not canceled: their supervisors keep
// running and a later backend observes them.
func (b *Backend) Close() error {
	b.mu.Lock()
	b.closed = true
	var unstarted []func()
	if b.workspaces != nil {
		for _, w := range b.workspaces.bindings {
			if w.parent == nil && w.release != nil {
				unstarted = append(unstarted, w.release)
				w.release = nil
				w.startAttempted = true
			}
		}
	}
	jobs := make([]*execution, 0, len(b.jobs))
	for _, e := range b.jobs {
		jobs = append(jobs, e)
	}
	b.mu.Unlock()
	for _, release := range unstarted {
		release()
	} // no materializer or filesystem owner was installed
	var result error
	b.mu.Lock()
	result = errors.Join(result, b.unstartedOutputErr)
	if b.logReaderErr != nil {
		result = errors.Join(result, b.logReaderErr)
	}
	if b.workspaces != nil {
		for _, c := range b.workspaces.cleanupFrames {
			if c.busy || c.owned {
				result = errors.Join(result, errors.New("workspace root cleanup remains pending"))
			}
			if c.closeHeld {
				result = errors.Join(result, errors.New("workspace ownership remains held"))
			}
		}
	}
	b.mu.Unlock()
	for _, e := range jobs {
		err := b.stop(e, backend.Canceled, "server shutting down")
		// Ordinary owners cache their first delivery cause. Report it only if
		// the actual joined physical snapshot remains held, not merely because
		// a later positively resolved delivery once returned a diagnostic.
		if e.owned == nil && e.retry == nil {
			result = errors.Join(result, err)
		}
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for _, e := range jobs {
		select {
		case <-e.done:
			if e.output != nil {
				e.output.mu.Lock()
				result = errors.Join(result, e.output.markerCloseErr)
				e.output.mu.Unlock()
			}
			b.mu.Lock()
			if w := e.workspace; w != nil {
				if w.root != nil || w.directory != nil || w.rootAttempted && (!w.created || w.rootErr != nil) || w.cleanupOwned || w.cleanupBusy {
					result = errors.Join(result, errors.New("workspace root cleanup remains pending"))
				}
				if w.observation.Held || w.cleanup != nil && w.cleanup.closeHeld {
					result = errors.Join(result, errors.New("workspace ownership remains held"))
				}
			}
			b.mu.Unlock()
			b.mu.Lock()
			if e.owned != nil || e.retry != nil {
				if !e.shutdownPublished || !e.shutdownClosed {
					result = errors.Join(result, errors.New("local execution ownership remains held"), e.shutdownErr)
				}
			} else if e.workspace == nil && !e.workspaceClosed {
				result = errors.Join(result, errors.New("local workspace closure receipt is unavailable"))
			}
			b.mu.Unlock()
			if e.retry != nil {
				select {
				case <-e.retry.reaped:
				default:
					result = errors.Join(result, errors.New("local process reaping remains uncertain"))
				}
			}
		case <-timer.C:
			b.mu.Lock()
			for _, pending := range jobs {
				result = errors.Join(result, pending.shutdownErr)
				if pending.owned != nil && !pending.shutdownPublished {
					result = errors.Join(result, pending.owned.cleanupErr)
				}
			}
			b.mu.Unlock()
			return errors.Join(result, errors.New("timed out waiting for local processes or output cleanup"))
		}
	}
	return result
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := len(p)
	keep := int64(n)
	if keep > backend.MaxLogBytes-o.size {
		keep = backend.MaxLogBytes - o.size
	}
	if keep > 0 {
		written, err := o.file.Write(p[:keep])
		o.size += int64(written)
		if err != nil {
			o.err = err
			return written, err
		}
	}
	if int64(n) > keep && !o.truncated {
		o.truncated = true
		flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		if o.retained != nil {
			flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
		}
		f, err := os.OpenFile(o.marker, flags, 0600)
		if err == nil {
			o.markerFile = f
			_, writeErr := f.Write([]byte("truncated\n"))
			var statErr error
			if o.retained != nil {
				o.retained.marker, statErr = f.Stat()
			}
			err = errors.Join(writeErr, statErr, o.closeMarker())
		}
		o.err = err
	}
	return n, nil
}

// closeMarker holds o.mu and never retries an uncertain original Close. The
// retained *os.File does not promise that Go kept its internal descriptor open.
func (o *output) closeMarker() error {
	if o.markerCloseErr != nil || o.markerFile == nil {
		return o.markerCloseErr
	}
	o.markerCloseErr = o.markerFile.Close()
	if o.markerCloseErr == nil {
		o.markerFile = nil
	}
	return o.markerCloseErr
}

// logReaderCloseError retains the original Go file and first Close cause.
// It does not establish whether an underlying native descriptor remains open.
type logReaderCloseError struct {
	file *os.File
	err  error
}

func (e *logReaderCloseError) Error() string { return "close log reader: " + e.err.Error() }
func (e *logReaderCloseError) Unwrap() error { return e.err }

// finishLogReader holds b.mu. Its caller owns one original Close attempt;
// admission has already excluded a previous held reader. Never retry it.
func (b *Backend) finishLogReader(f *os.File, chunk *backend.LogChunk, result *error) {
	if b.logReaderErr != nil {
		*chunk = backend.LogChunk{}
		*result = errors.Join(*result, b.logReaderErr)
		return
	}
	if err := f.Close(); err != nil {
		b.logReaderErr = &logReaderCloseError{file: f, err: err}
		*chunk = backend.LogChunk{}
		*result = errors.Join(*result, b.logReaderErr)
	}
}

// Logs reads the retained combined output prefix. Reads serialize with Backend
// operations after Observe; file I/O and mutex acquisition have no time bound.
// Close or an uncertain reader Close prevents further opens.
func (b *Backend) Logs(ctx context.Context, ref backend.Reference, offset int64, limit int) (chunk backend.LogChunk, result error) {
	id := ref.ID
	if err := ctx.Err(); err != nil {
		return backend.LogChunk{}, err
	}
	if !validID.MatchString(id) {
		return backend.LogChunk{}, errors.New("invalid job ID")
	}
	if offset < 0 || limit < 1 || limit > backend.MaxChunkBytes {
		return backend.LogChunk{}, fmt.Errorf("%w: invalid log range", fs.ErrInvalid)
	}
	s, err := b.Observe(ctx, ref)
	if err != nil {
		return backend.LogChunk{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return backend.LogChunk{}, err
	}
	if b.logReaderErr != nil {
		return backend.LogChunk{}, b.logReaderErr
	}
	if b.closed {
		return backend.LogChunk{}, errors.New("local backend is closed")
	}
	var expected, expectedMarker os.FileInfo
	var closedDomain bool
	if b.retention != nil {
		if err := b.checkOutputRoot(); err != nil {
			return backend.LogChunk{}, err
		}
		domain := b.retention.outputs[id]
		if domain == nil {
			if e := b.jobs[id]; e != nil {
				domain = e.retained
			}
		}
		if domain == nil || domain.removed {
			return backend.LogChunk{}, fs.ErrNotExist
		}
		expected = domain.log
		closedDomain = domain.closed
		if closedDomain {
			expectedMarker = domain.marker
		}
	}
	var f *os.File
	if b.retention != nil {
		f, err = openRetainedOutput(filepath.Join(b.directory, id+".log"))
	} else {
		f, err = os.Open(filepath.Join(b.directory, id+".log"))
	}
	if err != nil {
		return backend.LogChunk{}, fmt.Errorf("open output: %w", err)
	}
	defer b.finishLogReader(f, &chunk, &result)
	info, err := f.Stat()
	if err != nil {
		return backend.LogChunk{}, err
	}
	if b.retention != nil && (expected == nil || !info.Mode().IsRegular() || !os.SameFile(expected, info)) {
		return backend.LogChunk{}, backend.ErrIdentityMismatch
	}
	if offset > info.Size() {
		return backend.LogChunk{}, fmt.Errorf("%w: offset exceeds retained output", fs.ErrInvalid)
	}
	data := make([]byte, min(int64(limit), info.Size()-offset))
	n, err := f.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return backend.LogChunk{}, err
	}
	var marker os.FileInfo
	if b.retention != nil {
		marker, err = os.Lstat(filepath.Join(b.directory, id+".truncated"))
	} else {
		marker, err = os.Stat(filepath.Join(b.directory, id+".truncated"))
	}
	truncated := err == nil
	if b.retention != nil {
		if err == nil && (!marker.Mode().IsRegular() || closedDomain && (expectedMarker == nil || !os.SameFile(expectedMarker, marker))) {
			return backend.LogChunk{}, backend.ErrIdentityMismatch
		}
		if errors.Is(err, fs.ErrNotExist) && closedDomain && expectedMarker != nil {
			return backend.LogChunk{}, backend.ErrIdentityMismatch
		}
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return backend.LogChunk{}, err
	}
	return backend.LogChunk{Data: data[:n], NextOffset: offset + int64(n), Complete: s.State.Terminal() && offset+int64(n) >= info.Size(), Truncated: truncated}, nil
}
