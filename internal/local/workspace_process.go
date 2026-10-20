package local

import (
	"errors"
	"os"
	"os/exec"
	"runtime"

	"github.com/tmc/skygo/internal/backend"
)

func (b *Backend) runWorkspacePhase(w *workspaceExecution, argv []string, taskPhase bool) (backend.Status, bool, error) {
	if err := w.checkWorkspaceRoot(); err != nil {
		return backend.Status{}, false, err
	}
	cwd, err := workspaceCWD(w.directory)
	if err != nil {
		return backend.Status{}, false, err
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	for name, value := range w.binding.Task.Env {
		cmd.Env = append(cmd.Env, name+"="+value)
	}
	configure(cmd)
	input, err := os.Open(os.DevNull)
	if err != nil {
		return backend.Status{}, false, err
	}
	cmd.Stdin = input
	reader, writer, err := os.Pipe()
	if err != nil {
		closeErr := input.Close()
		if closeErr != nil {
			w.failedPhase = &execution{cmd: cmd, phase: &workspacePhase{startupInput: input, startupErr: closeErr}}
		}
		return backend.Status{}, false, errors.Join(err, closeErr)
	}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = writer, writer, 0
	phase := &execution{cmd: cmd, output: w.parent.output, done: make(chan struct{}), status: backend.Status{State: backend.Running, Instance: w.parent.status.Instance}, phase: &workspacePhase{startupInput: input, taskRetry: taskPhase && w.binding.Task.Retry != nil && w.binding.Task.Retry.MaxAttempts > 1}, owned: &defaultExecution{reader: reader, outputDone: make(chan struct{}), readDone: make(chan struct{}), reaped: make(chan struct{}), handle: defaultHandle{fd: -1}}}
	// No phase is installed while Cmd.Start mutates Process. Cancellation records
	// parent intent and is delivered only after the actual original owner exists.
	if stop, _, _ := b.workspaceStopped(w); stop {
		closeErr := b.closeWorkspaceStartupIO(w, phase, writer)
		return backend.Status{}, closeErr == nil, closeErr
	}
	preflightFailed := cmd.Err != nil
	startErr := b.defaultOps.start(cmd)
	runtime.KeepAlive(w.directory)
	if startErr != nil {
		closeErr := b.closeWorkspaceStartupIO(w, phase, writer)
		status := backend.Status{State: backend.Failed, Reason: "start workspace program: " + startErr.Error()}
		// Go1.24 Cmd.Start returns Cmd.Err BEFORE os.StartProcess. Only this
		// observed preflight branch plus joined explicit stdio and unchanged
		// original root proves no workload was created. errno/nil Process alone
		// cannot prove that a post-fork Start failure reaped its original child.
		if preflightFailed && cmd.Process == nil && closeErr == nil {
			if rootErr := w.checkWorkspaceRoot(); rootErr == nil {
				return status, true, nil
			} else {
				closeErr = rootErr
			}
		}
		phase.phase.startupErr = errors.Join(startErr, closeErr)
		w.failedPhase = phase
		cmd.Args = nil
		cmd.Env = nil
		cmd.Dir = ""
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil
		return status, false, closeErr
	}
	if cmd.Process == nil {
		err := errors.Join(errors.New("start returned no original process owner"), b.closeWorkspaceStartupIO(w, phase, writer))
		phase.phase.startupErr = err
		w.failedPhase = phase
		return backend.Status{}, false, err
	}
	// The exact ORIGINAL directory descriptor stays held throughout Start's exec
	// handshake. Linux chdir precedes descriptor remapping; fd3 is never assumed.
	writerErr := errors.Join(writer.Close(), input.Close())
	if writerErr != nil {
		phase.phase.startupWriter = writer
		phase.phase.startupErr = writerErr
	} else {
		phase.phase.startupInput = nil
	}
	phase.owned.handle, phase.owned.handleErr = b.defaultOps.openHandle(cmd)
	phase.owned.handleErr = errors.Join(phase.owned.handleErr, writerErr)
	b.mu.Lock()
	w.phase = phase
	w.physical.WriterActive = true
	if w.intent {
		b.stopDefault(phase, w.stopState, w.stopReason)
	}
	b.mu.Unlock()
	go b.readDefaultOutput(phase)
	go b.waitDefault(phase, 0)
	<-phase.done
	b.mu.Lock()
	w.phase = nil
	w.physical.WriterActive = false
	status := phase.status
	proof := phase.phase.writerClosed
	b.mu.Unlock()
	if status.ExitCode != nil {
		code := *status.ExitCode
		status.ExitCode = &code
	}
	// No original owner can now use these command payloads. Keep only the
	// bounded status and distinct proof; not full Task/argv/environment owners.
	if proof {
		phase.cmd = nil
		phase.owned.reader = nil
		phase.owned.handleErr = nil
		phase.owned.waitErr = nil
		phase.owned.outputResult = defaultOutputResult{}
	} else {
		// All actual command access ended at phase.done, but an uncertain owner
		// record remains held. Shed payload without discarding that original record.
		cmd.Args = nil
		cmd.Env = nil
		cmd.Dir = ""
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil
		w.failedPhase = phase
	}
	return status, proof, nil
}

// No Local waiter/reaper is installed on this path. A post-fork Start failure
// can still have opaque child ownership. Retain original stdio objects on any
// failed Close; no process absence, numeric recovery or completion is inferred.
func (b *Backend) closeWorkspaceStartupIO(w *workspaceExecution, phase *execution, writer *os.File) error {
	err := errors.Join(phase.owned.reader.Close(), writer.Close(), phase.phase.startupInput.Close())
	if err != nil {
		phase.phase.startupWriter = writer
		phase.phase.startupErr = err
		w.failedPhase = phase
	}
	return err
}
