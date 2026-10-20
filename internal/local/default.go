package local

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// The default path keeps its original public identity and status classification.
// These private operations permit tests to order and fail the actual lifecycle.
type defaultProcessOps struct {
	start       func(*exec.Cmd) error
	wait        func(*exec.Cmd) error
	reap        func(*exec.Cmd) error
	kill        func(*exec.Cmd) error
	absent      func(*exec.Cmd) error
	openHandle  func(*exec.Cmd) (defaultHandle, error)
	write       func(*output, []byte) (int, error)
	closeOutput func(*output) error
}

func systemDefaultProcessOps() defaultProcessOps {
	return defaultProcessOps{
		start: func(c *exec.Cmd) error { return c.Start() }, wait: waitDefaultTerminal, reap: func(c *exec.Cmd) error { return c.Wait() }, kill: kill, absent: defaultGroupAbsent,
		openHandle: openDefaultHandle, write: func(o *output, p []byte) (int, error) { return o.Write(p) },
		closeOutput: func(o *output) error { return o.file.Close() },
	}
}

// A successfully opened handle is owned only by the lifecycle waiter. It never
// aliases os.Process's private descriptor and is never reconstructed after wait.
type defaultHandle struct{ fd int }

type defaultOutputResult struct{ readErr, retainErr, closeErr, rawCloseErr, readerCloseErr error }

type defaultExecution struct {
	reader       *os.File
	outputDone   chan struct{}
	readDone     chan struct{}
	reaped       chan struct{}
	handle       defaultHandle
	handleErr    error
	handleClosed bool
	// Published before their respective completion channels close.
	outputResult defaultOutputResult
	waitErr      error
	// All fields below are protected by Backend.mu.
	signalClosed bool
	intent       bool // first successful public cancellation/timeout delivery
	cleanupErr   error
}

// startDefault holds Backend.mu. It deliberately does not add CLONE_PIDFD or
// require pidfds: old/filtered Linux hosts retain the normal nonreaping path.
func (b *Backend) startDefault(cmd *exec.Cmd, out *output, id string, seconds int64) error {
	reader, writer, err := os.Pipe()
	if err != nil {
		return errors.Join(fmt.Errorf("create output pipe: %w", err), b.closeUnstartedOutput(out))
	}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = writer, writer, 0
	e := &execution{cmd: cmd, stdioWriter: writer, status: backend.Status{State: backend.Starting}, done: make(chan struct{}), output: out,
		owned: &defaultExecution{reader: reader, outputDone: make(chan struct{}), readDone: make(chan struct{}), reaped: make(chan struct{}), handle: defaultHandle{fd: -1}}}
	b.jobs[id] = e
	b.attachRetention(e, id)
	if err := b.defaultOps.start(cmd); err != nil {
		readerErr := e.closeReader()
		writerErr := e.closeWriter()
		closeErr := out.file.Close()
		if closeErr == nil {
			out.closed = true
		}
		e.status = backend.Status{State: backend.Failed, Reason: "start: " + err.Error()}
		e.owned.signalClosed = true
		e.owned.handleClosed = true
		close(e.owned.readDone)
		close(e.owned.outputDone)
		close(e.owned.reaped) // Start failure is not an actual Wait receipt.
		e.shutdownPublished = true
		e.shutdownErr = errors.Join(err, readerErr, writerErr, closeErr)
		b.finishPayload(e)
		close(e.done)
		return fmt.Errorf("start program: %w", e.shutdownErr)
	}
	e.closeWriter()
	// No waiter/reaper exists yet. This exclusively owned unreaped child's PID
	// is still reserved even when the program has already exited.
	e.owned.handle, e.owned.handleErr = b.defaultOps.openHandle(cmd)
	e.status.State = backend.Running
	go b.readDefaultOutput(e)
	go b.waitDefault(e, seconds)
	return nil
}

func (b *Backend) readDefaultOutput(e *execution) {
	owned := e.owned
	defer func() {
		readerCloseErr := e.closeReader()
		owned.outputResult.readerCloseErr = readerCloseErr
		if e.phase != nil {
			close(owned.outputDone)
			return
		}
		b.mu.Lock()
		if e.retained != nil {
			e.output.mu.Lock()
			if e.output.closed && e.output.markerCloseErr == nil && e.output.markerFile == nil && readerCloseErr == nil && e.stdioWriterErr == nil && e.stdioWriter == nil {
				b.publishOutputClosure(e.retained)
			}
			e.output.mu.Unlock()
		}
		b.mu.Unlock()
		close(owned.outputDone)
	}()
	var result defaultOutputResult
	buf := make([]byte, 32<<10)
	for {
		n, err := owned.reader.Read(buf)
		if n > 0 && result.retainErr == nil {
			_, result.retainErr = b.defaultOps.write(e.output, buf[:n])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				result.readErr = err
			}
			break
		}
	}
	close(owned.readDone)
	if e.phase != nil {
		e.output.mu.Lock()
		if e.output.err != nil {
			result.retainErr = e.output.err
		}
		e.output.mu.Unlock()
		owned.outputResult = result
		return
	}
	// This goroutine alone finalizes the retained file. A blocked Write/Close
	// retains its original handles; the coordinator never closes underneath it.
	e.output.mu.Lock()
	if e.output.err != nil {
		result.retainErr = e.output.err
	}
	result.rawCloseErr = b.defaultOps.closeOutput(e.output)
	result.closeErr = result.rawCloseErr
	if result.closeErr == nil {
		e.output.closed = true
	}
	if errors.Is(result.closeErr, os.ErrClosed) {
		result.closeErr = nil
	}
	e.output.mu.Unlock()
	owned.outputResult = result
}

// stopDefault holds Backend.mu. A completed handoff is an idempotent no-op,
// not a signal-delivery receipt and not authority to relabel an exiting task.
func (b *Backend) stopDefault(e *execution, state backend.State, reason string) error {
	select {
	case <-e.done:
		return nil
	default:
	}
	if e.owned.signalClosed || e.owned.intent {
		return nil
	}
	if err := b.defaultOps.kill(e.cmd); err != nil {
		if e.phase == nil && e.shutdownErr == nil {
			e.shutdownErr = err
		}
		return fmt.Errorf("kill process group: %w", err)
	}
	e.owned.intent = true
	e.status = backend.Status{State: state, Reason: reason}
	return nil
}

func (b *Backend) waitDefault(e *execution, seconds int64) {
	owned := e.owned
	var timer *time.Timer
	if seconds > 0 {
		timer = time.AfterFunc(time.Duration(seconds)*time.Second, func() { b.stop(e, backend.Failed, "execution timed out") })
	}
	pinErr := b.defaultOps.wait(e.cmd)
	discarded := false
	if pinErr == nil && e.phase == nil {
		grace := time.NewTimer(250 * time.Millisecond)
		select {
		case <-owned.readDone:
		case <-grace.C:
			discarded = true
			e.closeReader()
		}
		grace.Stop()
	}
	b.mu.Lock()
	owned.signalClosed = true
	var cleanupErr error
	if pinErr == nil {
		cleanupErr = b.defaultOps.kill(e.cmd)
	}
	b.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if pinErr != nil {
		e.closeReader()
		// A failed terminal record never authorizes numeric group delivery. Only
		// an already retained original handle can assist exceptional leader cleanup.
		cleanupErr = errors.Join(pinErr, owned.handleErr, owned.handle.signal())
	}
	groupErr := cleanupErr
	handleCloseErr := owned.handle.close()
	owned.handleClosed = handleCloseErr == nil
	cleanupErr = errors.Join(cleanupErr, handleCloseErr)
	b.mu.Lock()
	owned.cleanupErr = cleanupErr
	b.mu.Unlock()
	go func() { owned.waitErr = b.defaultOps.reap(e.cmd); close(owned.reaped) }()
	// No shared lock is held during a reaper, writer or filesystem finalizer join.
	// Close uses its own total deadline and reports these pending original handles.
	<-owned.reaped
	var workspaceAbsenceErr error
	if pinErr == nil && (groupErr != nil || e.phase != nil) {
		// Darwin may refuse delivery to a group containing only its zombie leader.
		// Resolve ONLY that delivery error, using absence-only checks after reap.
		// A reused/live group or an uncertain probe never authorizes another signal.
		absenceErr := b.waitDefaultAbsent(e.cmd)
		workspaceAbsenceErr = absenceErr
		b.mu.Lock()
		if absenceErr == nil {
			owned.cleanupErr = handleCloseErr
		} else {
			owned.cleanupErr = errors.Join(groupErr, handleCloseErr, absenceErr)
		}
		b.mu.Unlock()
	}
	if e.phase != nil {
		grace := time.NewTimer(250 * time.Millisecond)
		select {
		case <-owned.readDone:
		case <-grace.C:
			discarded = true
			e.closeReader()
		}
		grace.Stop()
	}
	<-owned.outputDone
	result := owned.outputResult
	b.mu.Lock()
	defer b.mu.Unlock()
	owned.cleanupErr = errors.Join(owned.cleanupErr, result.closeErr)
	if e.cmd.ProcessState != nil {
		code := int32(e.cmd.ProcessState.ExitCode())
		e.status.ExitCode = &code
	}
	if !owned.intent {
		e.status.State = backend.Succeeded
		if owned.waitErr != nil {
			e.status.State, e.status.Reason = backend.Failed, owned.waitErr.Error()
		} else if discarded {
			e.status.Reason = "output pipes remained open after exit; later output discarded"
		} else if result.readErr != nil {
			e.status.State, e.status.Reason = backend.Failed, result.readErr.Error()
		}
		if pinErr != nil {
			e.status.State, e.status.Reason = backend.Failed, "retain leader: "+pinErr.Error()
		}
	}
	// Preserve the original retention-error precedence, including after cancel.
	if result.retainErr != nil {
		e.status.State, e.status.Reason = backend.Failed, "retain output: "+result.retainErr.Error()
	}
	var processErr error
	if e.cmd.ProcessState == nil {
		processErr = errors.New("local process result is unavailable")
		owned.cleanupErr = errors.Join(owned.cleanupErr, processErr)
	}
	if e.phase != nil {
		e.phase.writerClosed = workspaceWriterPlatform() && pinErr == nil && workspaceAbsenceErr == nil && owned.cleanupErr == nil && owned.handleErr == nil && owned.handleClosed && !discarded && result.readErr == nil && result.readerCloseErr == nil && result.retainErr == nil && e.cmd.ProcessState != nil
		var exitErr *exec.ExitError
		ordinaryWait := owned.waitErr == nil || errors.As(owned.waitErr, &exitErr)
		if e.phase.taskRetry && e.phase.writerClosed && !owned.intent && ordinaryWait && e.status.ExitCode != nil && *e.status.ExitCode >= 0 {
			e.status.Termination = backend.StoppedWithExitCode
		}
	} else {
		var exitErr *exec.ExitError
		ordinaryWait := owned.waitErr == nil || errors.As(owned.waitErr, &exitErr)
		e.output.mu.Lock()
		outputClosed := e.output.closed && e.output.markerCloseErr == nil && e.output.markerFile == nil
		markerErr := e.output.markerCloseErr
		e.output.mu.Unlock()
		// Returned data errors and ordinary ExitError remain diagnostics. Only
		// original joins, Close receipts and the existing native/group fence
		// establish physical closure; neither status nor cancellation does.
		e.shutdownClosed = pinErr == nil && ordinaryWait && processErr == nil &&
			owned.signalClosed && handleCloseErr == nil &&
			(groupErr == nil || workspaceAbsenceErr == nil) &&
			result.readerCloseErr == nil && e.stdioWriterErr == nil && e.stdioWriter == nil &&
			result.rawCloseErr == nil && outputClosed
		e.shutdownErr = errors.Join(e.shutdownErr, pinErr, groupErr, owned.handleErr,
			handleCloseErr, workspaceAbsenceErr, owned.waitErr, processErr,
			result.readErr, result.retainErr, result.rawCloseErr,
			result.readerCloseErr, e.stdioWriterErr, markerErr)
		e.shutdownPublished = true
		b.finishPayload(e)
	}
	close(e.done)
}

func (b *Backend) waitDefaultAbsent(cmd *exec.Cmd) error {
	deadline := time.Now().Add(250 * time.Millisecond)
	for {
		err := b.defaultOps.absent(cmd)
		if err == nil {
			return nil
		}
		if !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}
