package local

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// SupervisorCommand is the hidden first argument that selects RunSupervisor.
// A binary used as DurableConfig.Executable must dispatch it before any other
// command-line or signal handling.
const SupervisorCommand = "__skygo-local-supervisor"

// Durable supervisor timing. Cancellation and timeouts are polled; output
// grace bounds how long an exited leader's group may keep the pipe open.
const (
	durablePoll        = 50 * time.Millisecond
	durableOutputGrace = 250 * time.Millisecond
	durableOutputDrain = 2 * time.Second
	durableGroupDrain  = time.Second
)

// RunSupervisor runs one durable local job and returns a process exit code.
// args is the job's state directory. The launching server passes the
// exclusively created log file as descriptor 3 and a readiness pipe as
// descriptor 4. The supervisor launches the workload at most once, owns and
// reaps it, and writes the authoritative terminal status. It ignores SIGHUP
// and SIGINT so that it outlives the server that started it.
func RunSupervisor(args []string) int {
	if len(args) != 1 {
		return 2
	}
	signal.Ignore(syscall.SIGHUP, syscall.SIGINT)
	log := os.NewFile(3, "log")
	ready := os.NewFile(4, "ready")
	if log == nil || ready == nil {
		return 2
	}
	closeOnExec(log)
	closeOnExec(ready)
	defer ready.Close()
	err := supervise(args[0], log, ready)
	if err != nil {
		fmt.Fprintf(ready, "fail: %v\n", err)
		return 1
	}
	return 0
}

type supervisor struct {
	dir    string
	intent durableIntent
	cmd    *exec.Cmd
	out    *output

	mu          sync.Mutex
	reaping     bool          // the leader may be reaped; no more group signals
	stopped     backend.State // set by the first delivered cancel or timeout
	reason      string
	groupClosed bool
	// Native operation seams are private to deterministic closure tests.
	killGroup   func(*exec.Cmd) error
	groupAbsent func(*exec.Cmd) error
	// retainOutput, if set, replaces retain for deterministic output tests.
	retainOutput func([]byte) error
}

func supervise(dir string, log, ready *os.File) error {
	return superviseWithIdentity(dir, log, ready, func(cmd *exec.Cmd) (processIdentity, error) {
		return identifyProcess(cmd.Process.Pid)
	})
}

// The child-identity operation is private so a test can hold this same original
// unreaped child at the exited frontier, without a replacement process/waiter.
func superviseWithIdentity(dir string, log, ready *os.File, identify func(*exec.Cmd) (processIdentity, error)) error {
	if err := checkPrivateDir(dir); err != nil {
		log.Close()
		return err
	}
	s := &supervisor{dir: dir}
	if err := readDurable(dir, durableIntentFile, maxDurableIntent, &s.intent); err != nil {
		log.Close()
		return err
	}
	in := s.intent
	if filepath.Base(dir) != in.ID || !filepath.IsAbs(in.Logs) || validDurableIntent(in, in.ID, in.Logs) != nil {
		log.Close()
		return errors.New("invalid durable intent")
	}
	self, err := identifyProcess(os.Getpid())
	if err != nil {
		log.Close()
		return fmt.Errorf("identify supervisor: %w", err)
	}
	if err := createDurable(dir, durableClaimFile, durableClaim{Version: durableVersion, Instance: in.Instance, Supervisor: &self}); err != nil {
		log.Close()
		if errors.Is(err, fs.ErrExist) {
			return errors.New("durable job was already claimed")
		}
		return err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		log.Close()
		return s.launchFailed("create output pipe: " + err.Error())
	}
	cmd := exec.Command(in.Task.Argv[0], in.Task.Argv[1:]...)
	cmd.Dir = in.Task.Directory
	cmd.Env = os.Environ()
	for k, v := range in.Task.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	for k, v := range in.Launch.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = writer, writer
	configure(cmd)
	s.cmd = cmd
	s.out = &output{file: log, marker: filepath.Join(in.Logs, in.ID+".truncated")}
	err = cmd.Start()
	writer.Close()
	if err != nil {
		reader.Close()
		log.Close()
		return s.launchFailed("start: " + err.Error())
	}
	// The leader stays unreaped until its group is killed, so its PID and
	// process group cannot be reused while this supervisor signals them.
	child, err := identify(cmd)
	if errors.Is(err, errProcessGone) {
		// A successful fast child can already be a zombie. It is still our
		// unreaped original: classify its actual Wait, rather than turning
		// successful completion into a startup failure. No running identity
		// is invented. Ready is acknowledged after its status is committed.
		if err := s.wait(reader); err != nil {
			return err
		}
		_, err := fmt.Fprintln(ready, "ok")
		return err
	}
	if err == nil {
		err = writeDurable(dir, durableRunningFile, durableRunning{Version: durableVersion, Instance: in.Instance, Supervisor: self, Child: child})
	}
	if err != nil {
		s.stop(backend.Failed, "record running workload: "+err.Error())
	} else {
		fmt.Fprintln(ready, "ok")
	}
	ready.Close()
	return s.wait(reader)
}

// launchFailed records that the claimed workload never started.
func (s *supervisor) launchFailed(reason string) error {
	s.groupClosed = true // cmd.Start did not create a workload.
	if err := s.finish(backend.Failed, reason, nil, false); err != nil {
		return err
	}
	return errors.New(reason)
}

// stop kills the workload group once, unless the leader may already be reaped.
func (s *supervisor) stop(state backend.State, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reaping || s.stopped != "" {
		return
	}
	if err := s.signalGroup(); err != nil {
		return
	}
	s.stopped, s.reason = state, reason
}

func (s *supervisor) signalGroup() error {
	if s.killGroup != nil {
		return s.killGroup(s.cmd)
	}
	return kill(s.cmd)
}

// waitGroupAbsent only probes after reaping. These observations can establish
// absence but never authorize another signal to a numerically reused group.
func (s *supervisor) waitGroupAbsent() error {
	if s.groupAbsent != nil {
		return s.groupAbsent(s.cmd)
	}
	deadline := time.Now().Add(durableGroupDrain)
	for {
		err := defaultGroupAbsent(s.cmd)
		if err == nil || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// watch delivers a durable cancel request or the task timeout.
func (s *supervisor) watch(done <-chan struct{}) {
	var deadline time.Time
	if t := s.intent.Task.TimeoutSeconds; t > 0 {
		deadline = time.Now().Add(time.Duration(t) * time.Second)
	}
	tick := time.NewTicker(durablePoll)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
		}
		var c durableCancel
		if err := readDurable(s.dir, durableCancelFile, maxDurableRecord, &c); err == nil && c.Version == durableVersion && c.Instance == s.intent.Instance {
			s.stop(backend.Canceled, "canceled by operator")
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			s.stop(backend.Failed, "execution timed out")
		}
	}
}

// retain writes one drained chunk to the log. output.Write does not keep its
// first error: a later successful truncation marker resets o.err. So retain
// reports both Write's result and the error recorded by this call.
func (s *supervisor) retain(p []byte) error {
	if s.retainOutput != nil {
		return s.retainOutput(p)
	}
	if _, err := s.out.Write(p); err != nil {
		return err
	}
	s.out.mu.Lock()
	defer s.out.mu.Unlock()
	return s.out.err
}

func (s *supervisor) wait(reader *os.File) error {
	readDone := make(chan struct{})
	var readErr, retainErr error
	go func() {
		defer close(readDone)
		// The first retention error is final: later chunks are discarded, not
		// written, so the retained prefix can never look complete. Draining
		// continues so the workload never blocks on a full pipe.
		buf := make([]byte, 32<<10)
		for {
			n, err := reader.Read(buf)
			if n > 0 && retainErr == nil {
				retainErr = s.retain(buf[:n])
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					readErr = err
				}
				return
			}
		}
	}()
	watchDone := make(chan struct{})
	go s.watch(watchDone)

	pinErr := waitDefaultTerminal(s.cmd)
	if pinErr == nil {
		grace := time.NewTimer(durableOutputGrace)
		select {
		case <-readDone:
		case <-grace.C:
		}
		grace.Stop()
	}
	s.mu.Lock()
	s.reaping = true
	var groupSignalErr error
	if pinErr == nil {
		// The unreaped leader still reserves its group; descendants that kept
		// the pipe open are stopped before the leader is reaped.
		groupSignalErr = s.signalGroup()
	}
	s.mu.Unlock()
	close(watchDone)
	waitErr := s.cmd.Wait()
	groupErr := s.waitGroupAbsent()
	s.groupClosed = s.cmd.ProcessState != nil && groupErr == nil

	drain := time.NewTimer(durableOutputDrain)
	complete := true
	select {
	case <-readDone:
	case <-drain.C:
		complete = false
		reader.Close()
		<-readDone
	}
	drain.Stop()
	reader.Close()

	s.out.mu.Lock()
	syncErr := s.out.file.Sync()
	closeErr := s.out.file.Close()
	s.out.mu.Unlock()

	var code *int32
	if ps := s.cmd.ProcessState; ps != nil {
		c := int32(ps.ExitCode())
		code = &c
	}
	state, reason := backend.Succeeded, ""
	var exitErr *exec.ExitError
	switch {
	case s.stopped != "":
		state, reason = s.stopped, s.reason
	case pinErr != nil:
		state, reason = backend.Failed, "retain leader: "+pinErr.Error()
	case waitErr != nil && !errors.As(waitErr, &exitErr):
		state, reason = backend.Failed, waitErr.Error()
	case waitErr != nil:
		state = backend.Failed
	case !complete:
		state, reason = backend.Failed, "output pipe remained open after exit; output is incomplete"
	case readErr != nil:
		state, reason = backend.Failed, "read output: "+readErr.Error()
	}
	if err := errors.Join(retainErr, syncErr, closeErr); err != nil {
		state, reason = backend.Failed, "retain output: "+err.Error()
		complete = false
	}
	if !s.groupClosed {
		closureErr := errors.Join(groupSignalErr, groupErr)
		if closureErr == nil {
			closureErr = errors.New("original process result is unavailable")
		}
		state, reason = backend.Unknown, "execution group closure is unknown: "+closureErr.Error()
	}
	return s.finish(state, reason, code, complete && readErr == nil)
}

// finish writes the terminal status. A supervisor that cannot write it exits
// with an error and leaves the job unknown to every later observer.
func (s *supervisor) finish(state backend.State, reason string, code *int32, complete bool) error {
	return writeDurable(s.dir, durableStatusFile, durableStatus{
		Version: durableVersion, Instance: s.intent.Instance, State: state,
		Reason: retryReason(reason), ExitCode: code, OutputComplete: complete,
		GroupClosed: s.groupClosed,
	})
}
