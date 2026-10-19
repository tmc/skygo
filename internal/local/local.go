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
	mu             sync.Mutex
	directory      string
	jobs           map[string]*execution
	closed         bool
	retryOps       retryProcessOps
	retryAvailable bool
	defaultOps     defaultProcessOps
	retention      *retentionState
}

type execution struct {
	cmd      *exec.Cmd
	status   backend.Status
	done     chan struct{}
	output   *output
	retry    *retryExecution
	owned    *defaultExecution
	retained *retainedOutput
}

type output struct {
	mu             sync.Mutex
	file           *os.File
	marker         string
	size           int64
	truncated      bool
	err            error
	markerCloseErr error
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

// Check rejects requirements that local processes cannot enforce.
func (b *Backend) Check(t backend.Task) error {
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
	if t.Image != "" || t.CPUMillis != 0 || t.MemoryBytes != 0 || t.Accelerator != "" || t.AcceleratorCount != 0 {
		return errors.New("local execution does not support images or resource requests")
	}
	if t.CPULimitMillis != 0 || t.MemoryLimitBytes != 0 {
		return errors.New("local execution does not support resource limits")
	}
	if len(t.NodeSelector) != 0 {
		return errors.New("local execution does not support node selectors")
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

// Start starts a job once. Its context only controls startup.
func (b *Backend) Start(ctx context.Context, id string, t backend.Task) error {
	if !validID.MatchString(id) {
		return errors.New("invalid job ID")
	}
	if err := b.Check(t); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return errors.New("local backend is closed")
	}
	if _, ok := b.jobs[id]; ok {
		return errors.New("job already started")
	}
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
			b.closeUnstartedOutput(out)
			return fmt.Errorf("identify output file: %w", err)
		}
		out.retained.log = info
	}
	cmd := exec.Command(t.Argv[0], t.Argv[1:]...)
	cmd.Dir = t.Directory
	cmd.Env = os.Environ()
	for k, v := range t.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout = out
	cmd.Stderr = out
	cmd.WaitDelay = 250 * time.Millisecond
	configure(cmd)
	if t.Retry != nil && t.Retry.MaxAttempts > 1 {
		return b.startRetry(cmd, out, id, t.TimeoutSeconds)
	}
	return b.startDefault(cmd, out, id, t.TimeoutSeconds)
}

func (b *Backend) stop(e *execution, state backend.State, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
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

// Observe reports only executions owned by this backend instance.
func (b *Backend) Observe(ctx context.Context, ref backend.Reference) (backend.Status, error) {
	id := ref.ID
	if err := ctx.Err(); err != nil {
		return backend.Status{}, err
	}
	if !validID.MatchString(id) {
		return backend.Status{}, errors.New("invalid job ID")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	e := b.jobs[id]
	if e == nil {
		return backend.Status{State: backend.Unknown, Reason: "execution is not owned by this server"}, nil
	}
	if err := checkInstance(e, ref.Instance); err != nil {
		return backend.Status{}, err
	}
	select {
	case <-e.done:
	default:
		return backend.Status{State: backend.Running, Instance: e.status.Instance}, nil
	}
	s := e.status
	if s.ExitCode != nil {
		code := *s.ExitCode
		s.ExitCode = &code
	}
	return s, nil
}

// Cancel requests termination of the owned process group.
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
		return errors.New("execution is not owned by this server")
	}
	return b.stop(e, backend.Canceled, "canceled by operator")
}

// Close kills owned groups and waits at most three seconds for their reaping.
func (b *Backend) Close() error {
	b.mu.Lock()
	b.closed = true
	jobs := make([]*execution, 0, len(b.jobs))
	for _, e := range b.jobs {
		jobs = append(jobs, e)
	}
	b.mu.Unlock()
	var result error
	for _, e := range jobs {
		result = errors.Join(result, b.stop(e, backend.Canceled, "server shutting down"))
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for _, e := range jobs {
		select {
		case <-e.done:
			if e.owned != nil {
				b.mu.Lock()
				result = errors.Join(result, e.owned.cleanupErr)
				b.mu.Unlock()
			}
			if e.retry != nil {
				select {
				case <-e.retry.reaped:
				default:
					result = errors.Join(result, errors.New("local process reaping remains uncertain"))
				}
			}
		case <-timer.C:
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
		if o.retained == nil {
			o.err = os.WriteFile(o.marker, []byte("truncated\n"), 0600)
		} else {
			f, err := os.OpenFile(o.marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err == nil {
				_, err = f.Write([]byte("truncated\n"))
				info, statErr := f.Stat()
				closeErr := f.Close()
				o.markerCloseErr = closeErr
				o.retained.marker = info
				err = errors.Join(err, statErr, closeErr)
			}
			o.err = err
		}
	}
	return n, nil
}

// Logs reads the retained combined output prefix without blocking execution.
func (b *Backend) Logs(ctx context.Context, ref backend.Reference, offset int64, limit int) (backend.LogChunk, error) {
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
	var expected, expectedMarker os.FileInfo
	var closedDomain bool
	if b.retention != nil {
		b.mu.Lock()
		defer b.mu.Unlock()
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
	defer f.Close()
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
