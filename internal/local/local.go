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
}

type execution struct {
	cmd    *exec.Cmd
	status backend.Status
	done   chan struct{}
	output *output
}

type output struct {
	mu        sync.Mutex
	file      *os.File
	marker    string
	size      int64
	truncated bool
	err       error
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
	return &Backend{directory: directory, jobs: make(map[string]*execution)}, nil
}

// Check rejects requirements that local processes cannot enforce.
func (b *Backend) Check(t backend.Task) error {
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
	// Exclusive creation also fences a repeated start after backend restart.
	f, err := os.OpenFile(filepath.Join(b.directory, id+".log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create job output: %w", err)
	}
	out := &output{file: f, marker: filepath.Join(b.directory, id+".truncated")}
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
	e := &execution{cmd: cmd, status: backend.Status{State: backend.Starting}, done: make(chan struct{}), output: out}
	b.jobs[id] = e
	if err := cmd.Start(); err != nil {
		e.status = backend.Status{State: backend.Failed, Reason: "start: " + err.Error()}
		f.Close()
		close(e.done)
		return fmt.Errorf("start program: %w", err)
	}
	e.status.State = backend.Running
	go b.wait(e, t.TimeoutSeconds)
	return nil
}

func (b *Backend) wait(e *execution, seconds int64) {
	var timer *time.Timer
	if seconds > 0 {
		timer = time.AfterFunc(time.Duration(seconds)*time.Second, func() { b.stop(e, backend.Failed, "execution timed out") })
	}
	err := e.cmd.Wait()
	if timer != nil {
		timer.Stop()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Remove remaining members of the owned group even when the leader succeeded.
	kill(e.cmd)
	code := int32(e.cmd.ProcessState.ExitCode())
	e.status.ExitCode = &code
	if e.status.State == backend.Running {
		e.status.State = backend.Succeeded
		if errors.Is(err, exec.ErrWaitDelay) {
			// WaitDelay means the leader succeeded but inherited pipes stayed open.
			e.status.Reason = "output pipes remained open after exit; later output discarded"
		} else if err != nil {
			e.status.State = backend.Failed
			e.status.Reason = err.Error()
		}
	}
	e.output.mu.Lock()
	if e.output.err != nil {
		e.status.State = backend.Failed
		e.status.Reason = "retain output: " + e.output.err.Error()
	}
	e.output.file.Close()
	e.output.mu.Unlock()
	close(e.done)
}

func (b *Backend) stop(e *execution, state backend.State, reason string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e.status.State != backend.Running {
		return nil
	}
	if err := kill(e.cmd); err != nil {
		return fmt.Errorf("kill process group: %w", err)
	}
	// This is the winning termination intent. Observe exposes Running until Wait.
	e.status = backend.Status{State: state, Reason: reason}
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
	select {
	case <-e.done:
	default:
		return backend.Status{State: backend.Running}, nil
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
		case <-timer.C:
			return errors.Join(result, errors.New("timed out waiting for local processes"))
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
		o.err = os.WriteFile(o.marker, []byte("truncated\n"), 0600)
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
	f, err := os.Open(filepath.Join(b.directory, id+".log"))
	if err != nil {
		return backend.LogChunk{}, fmt.Errorf("open output: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return backend.LogChunk{}, err
	}
	if offset > info.Size() {
		return backend.LogChunk{}, fmt.Errorf("%w: offset exceeds retained output", fs.ErrInvalid)
	}
	data := make([]byte, min(int64(limit), info.Size()-offset))
	n, err := f.ReadAt(data, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return backend.LogChunk{}, err
	}
	_, err = os.Stat(filepath.Join(b.directory, id+".truncated"))
	truncated := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return backend.LogChunk{}, err
	}
	return backend.LogChunk{Data: data[:n], NextOffset: offset + int64(n), Complete: s.State.Terminal() && offset+int64(n) >= info.Size(), Truncated: truncated}, nil
}
