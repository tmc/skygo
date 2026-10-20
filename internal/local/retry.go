package local

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
)

// SupportsRetry reports whether this backend has a nonreaping wait and the
// required process-handle support. It does not promise whole-tree cleanup.
// Linux requires pidfd_open and pidfd_send_signal (Linux 5.3 or newer).
func (b *Backend) SupportsRetry() bool { return b.retryAvailable }

// The process operations are private seams for testing lifecycle ordering. No
// caller can substitute process identities or supply a recovered process group.
type retryProcessOps struct {
	wait          func(*exec.Cmd) error
	prepare       func(*exec.Cmd)
	recoverHandle func(*exec.Cmd) error
	kill          func(*exec.Cmd) error
	absent        func(*exec.Cmd) error
}

func systemRetryProcessOps() retryProcessOps {
	return retryProcessOps{wait: waitUnreaped, kill: kill, absent: groupAbsent, prepare: configureRetry, recoverHandle: recoverRetryHandle}
}

type retryExecution struct {
	reader       *os.File
	readDone     chan error
	outputDone   chan struct{}
	reaped       chan struct{}
	startErr     error
	waitErr      error // actual sole Wait result; published under Backend.mu before reaped
	handleClosed bool
	// All fields below are protected by Backend.mu. Once reaping is set, no
	// cancellation, timer or shutdown path may signal the group again.
	reaping bool
	intent  bool
}

func checkInstance(e *execution, instance string) error {
	if instance != "" && instance != e.status.Instance {
		return backend.ErrIdentityMismatch
	}
	return nil
}

// startRetry is called with Backend.mu held. The explicit combined pipe avoids
// exec.Cmd's hidden copier: a nonzero ExitError must not hide a missing EOF.
func (b *Backend) startRetry(cmd *exec.Cmd, out *output, id string, seconds int64) error {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return errors.Join(fmt.Errorf("create execution identity: %w", err), b.closeUnstartedOutput(out))
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return errors.Join(fmt.Errorf("create output pipe: %w", err), b.closeUnstartedOutput(out))
	}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = writer, writer, 0
	b.retryOps.prepare(cmd)
	e := &execution{cmd: cmd, stdioWriter: writer, status: backend.Status{State: backend.Starting, Instance: hex.EncodeToString(token[:])}, done: make(chan struct{}), output: out,
		retry: &retryExecution{reader: reader, readDone: make(chan error, 1), outputDone: make(chan struct{}), reaped: make(chan struct{})}}
	b.jobs[id] = e
	b.attachRetention(e, id)
	if err := cmd.Start(); err != nil {
		handleErr := releaseRetry(cmd)
		e.retry.handleClosed = handleErr == nil
		readerErr := e.closeReader()
		writerErr := e.closeWriter()
		closeErr := out.file.Close()
		if closeErr == nil {
			out.closed = true
		}
		e.status.State, e.status.Reason = backend.Failed, retryReason("start: "+err.Error())
		close(e.retry.reaped) // Failed native Start is not a positive Wait receipt.
		close(e.retry.outputDone)
		e.shutdownPublished = true
		e.shutdownErr = errors.Join(err, handleErr, readerErr, writerErr, closeErr)
		b.finishPayload(e)
		close(e.done)
		return fmt.Errorf("start program: %w", e.shutdownErr)
	}
	e.closeWriter()
	if !retryHandleReady(cmd) {
		// No waiter or reaper exists yet: the original unreaped child still pins
		// its PID. Acquire only an original-child handle, never signal that PID.
		e.retry.startErr = errors.Join(errors.New("fork did not return an original process handle"), b.retryOps.recoverHandle(cmd))
		e.retry.reaping = true // startup failure permits handle-only cleanup, never group delivery
	}
	e.status.State = backend.Running
	go readRetryOutput(e)
	go b.waitRetry(e, seconds)
	if e.retry.startErr != nil {
		return fmt.Errorf("start program: %w", e.retry.startErr)
	}
	return nil
}

func readRetryOutput(e *execution) {
	defer close(e.retry.outputDone)
	// Keep draining after a retention failure so a full pipe cannot strand the
	// leader. The first retention error is sticky and forbids positive evidence.
	buf := make([]byte, 32<<10)
	failed := false
	for {
		n, err := e.retry.reader.Read(buf)
		if n > 0 && !failed {
			_, writeErr := e.output.Write(buf[:n])
			failed = writeErr != nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			e.closeReader() // cached separately; do not change status/retry data precedence
			e.retry.readDone <- err
			return
		}
	}
}

// stopRetry records intent before attempting any signal, including a failing
// signal. The waiter owns the only handoff to reaping and the last group signal.
// Backend.mu is held by the caller.
func (b *Backend) stopRetry(e *execution, state backend.State, reason string) error {
	if !e.retry.intent {
		e.status.State, e.status.Reason = state, reason
	}
	e.retry.intent = true
	if e.retry.reaping {
		return nil
	}
	if err := b.retryOps.kill(e.cmd); err != nil {
		if e.shutdownErr == nil {
			e.shutdownErr = err
		}
		return fmt.Errorf("kill process group: %w", err)
	}
	return nil
}

func (b *Backend) waitRetry(e *execution, seconds int64) {
	var timer *time.Timer
	if seconds > 0 {
		timer = time.AfterFunc(time.Duration(seconds)*time.Second, func() { b.stop(e, backend.Failed, "execution timed out") })
	}
	pinErr := e.retry.startErr
	if pinErr == nil {
		pinErr = b.retryOps.wait(e.cmd)
	}
	b.mu.Lock()
	e.retry.reaping = true
	var killErr error
	// Failure to retain the leader loses the PID/PGID pin. Never issue another
	// group signal on that path, even if a later probe happens to find a group.
	if pinErr == nil {
		killErr = b.retryOps.kill(e.cmd)
	}
	b.mu.Unlock()
	// A failed nonreaping wait gives no group authority. The dedicated original
	// pidfd can stop only that original leader, never a recycled numeric PID.
	var abortErr error
	if pinErr != nil {
		abortErr = abortRetry(e.cmd)
		e.closeReader()
	}
	handleCloseErr := releaseRetry(e.cmd)
	e.retry.handleClosed = handleCloseErr == nil
	waitResult := make(chan error, 1)
	go func() {
		err := e.cmd.Wait() // Sole reaper; group signals are permanently fenced.
		b.mu.Lock()
		e.retry.waitErr = err
		if e.shutdownPublished {
			// Only this one original late result may extend the bounded bundle.
			// It never repairs the missing pin or upgrades aggregate closure.
			e.shutdownErr = errors.Join(e.shutdownErr, err)
		}
		close(e.retry.reaped) // actual result/ProcessState precede the receipt
		b.mu.Unlock()
		waitResult <- err
		b.mu.Lock()
		select {
		case <-e.done:
			b.finishPayload(e)
		default:
		}
		b.mu.Unlock()
	}()
	var waitErr error
	reaped := true
	if pinErr == nil {
		waitErr = <-waitResult
	} else {
		select {
		case waitErr = <-waitResult:
		case <-time.After(250 * time.Millisecond):
			reaped = false
			waitErr = errors.New("original process reaping remains uncertain")
		}
	}
	if timer != nil {
		timer.Stop()
	}
	var absentErr error
	if pinErr == nil && killErr == nil {
		absentErr = b.waitGroupAbsent(e.cmd)
	}
	var readErr, pumpErr, readTimeoutErr error
	select {
	case readErr = <-e.retry.readDone:
		pumpErr = readErr
	case <-time.After(250 * time.Millisecond):
		readTimeoutErr = errors.New("output pipes remained open after exit; later output discarded")
		readErr = readTimeoutErr
		e.closeReader()
		pumpErr = <-e.retry.readDone
	}
	<-e.retry.outputDone
	b.mu.Lock()
	defer b.mu.Unlock()
	if reaped && e.cmd.ProcessState != nil {
		code := int32(e.cmd.ProcessState.ExitCode())
		e.status.ExitCode = &code
	}
	if !e.retry.intent {
		e.status.State = backend.Succeeded
		if waitErr != nil {
			e.status.State, e.status.Reason = backend.Failed, waitErr.Error()
		}
	}
	var reapErr error
	if !reaped {
		reapErr = waitErr
	}
	for _, failure := range []struct {
		action string
		err    error
	}{
		{"retain leader", errors.Join(pinErr, abortErr, reapErr)}, {"stop process group", killErr}, {"confirm stopped process group", absentErr}, {"read output", readErr},
	} {
		if failure.err != nil {
			if !e.retry.intent {
				e.status.State = backend.Failed
				e.status.Reason = failure.action + ": " + failure.err.Error()
			}
			break
		}
	}
	e.output.mu.Lock()
	retainErr := e.output.err
	outputErr := retainErr
	closeErr := e.output.file.Close()
	if closeErr == nil {
		e.output.closed = true
	}
	if outputErr == nil {
		outputErr = closeErr
	}
	e.output.mu.Unlock()
	if outputErr != nil && !e.retry.intent {
		e.status.State, e.status.Reason = backend.Failed, "retain output: "+outputErr.Error()
	}
	e.status.Reason = retryReason(e.status.Reason)
	var exitErr *exec.ExitError
	ordinaryWait := waitErr == nil || errors.As(waitErr, &exitErr)
	// ExitCode>=0 distinguishes an ordinary exit from a signal on supported Unix
	// platforms. An ExitError for an ordinary nonzero exit does not erase proof.
	if ordinaryWait && !e.retry.intent && pinErr == nil && killErr == nil && absentErr == nil && readErr == nil && outputErr == nil && e.status.ExitCode != nil && *e.status.ExitCode >= 0 {
		e.status.Termination = backend.StoppedWithExitCode
	}
	// An actual Wait result is cached by the same original reaper before its
	// receipt. Incomplete timeout snapshots never read ProcessState or invent
	// a positive reap; late publication only adds that original result/cause.
	e.output.mu.Lock()
	outputClosed := e.output.closed && e.output.markerCloseErr == nil && e.output.markerFile == nil
	markerErr := e.output.markerCloseErr
	e.output.mu.Unlock()
	e.shutdownClosed = reaped && ordinaryWait && e.status.ExitCode != nil &&
		pinErr == nil && killErr == nil && absentErr == nil &&
		e.retry.reaping && handleCloseErr == nil && e.readerCloseErr == nil &&
		e.stdioWriterErr == nil && e.stdioWriter == nil && closeErr == nil && outputClosed
	// waitErr is status/timeout precedence. Include the actual cached Wait
	// cause exactly once, independently of that local result or cancellation.
	e.shutdownErr = errors.Join(e.shutdownErr, pinErr, abortErr,
		killErr, handleCloseErr, absentErr, reapErr, e.retry.waitErr,
		readTimeoutErr, pumpErr, e.readerCloseErr, e.stdioWriterErr,
		retainErr, closeErr, markerErr)
	e.shutdownPublished = true
	b.finishPayload(e)
	close(e.done)
}

func (b *Backend) waitGroupAbsent(cmd *exec.Cmd) error {
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		err := b.retryOps.absent(cmd)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func retryReason(s string) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= 1024 {
		return s
	}
	s = s[:1024]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
