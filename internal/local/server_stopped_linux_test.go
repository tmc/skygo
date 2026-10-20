//go:build linux

package local

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

type stoppedNativeFrame struct {
	cmd           *exec.Cmd
	files         []*os.File
	errors        []error
	started       bool
	waited        bool
	killAttempted bool
	killed        bool
}

var stoppedNativeKeepers struct {
	sync.Mutex
	frames []*stoppedNativeFrame
}

func keepStoppedNative(frame *stoppedNativeFrame, err error) {
	frame.errors = append(frame.errors, err)
	stoppedNativeKeepers.Lock()
	stoppedNativeKeepers.frames = append(stoppedNativeKeepers.frames, frame)
	stoppedNativeKeepers.Unlock()
}

func stoppedNativeCloseUncertain(err error) bool {
	var capture *ServerLifetimeCloseError
	var query *ServerStoppedCloseError
	return errors.As(err, &capture) || errors.As(err, &query)
}

func (frame *stoppedNativeFrame) closePipe(t *testing.T, index int) {
	file := frame.files[index]
	if file == nil {
		return
	}
	// Remove from the close list BEFORE the one actual attempt; the original
	// File remains reachable through the retained error if that attempt fails.
	frame.files[index] = nil
	if err := file.Close(); err != nil {
		keepStoppedNative(frame, &ServerStoppedCloseError{file: file, closeErr: err})
		t.Errorf("original test pipe Close uncertain: %v", err)
	}
}

func (frame *stoppedNativeFrame) waitKilled(t *testing.T) {
	err := frame.cmd.Wait()
	frame.waited = true
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		status, ok := exitErr.Sys().(syscall.WaitStatus)
		if ok && status.Signaled() && status.Signal() == syscall.SIGKILL {
			return
		}
	}
	// Keep the SAME Cmd/return instead of turning a diagnostic into join proof.
	keepStoppedNative(frame, err)
	t.Errorf("original killed child Wait did not prove SIGKILL: %v", err)
}

// Real native child oracle, not qualification of syscall mediation, descriptor
// nontransfer or arbitrary historical artifact producers.
func TestObserveServerStoppedNative(t *testing.T) {
	frame := new(stoppedNativeFrame)
	t.Cleanup(func() {
		if frame.started && !frame.waited && !frame.killAttempted {
			frame.killAttempted = true
			if err := frame.cmd.Process.Kill(); err != nil {
				keepStoppedNative(frame, err)
				t.Errorf("original child cleanup Kill: %v", err)
			} else {
				frame.killed = true
			}
		}
		if frame.killed && !frame.waited {
			frame.waitKilled(t)
		}
		for index := range frame.files {
			frame.closePipe(t, index)
		}
	})
	current, err := CaptureServerLifetime()
	if err != nil {
		if stoppedNativeCloseUncertain(err) {
			keepStoppedNative(frame, err)
			t.Fatal(err)
		}
		t.Skipf("native identity prerequisite unavailable: %v", err)
	}
	if !stoppedLifetimeValid(current) {
		t.Skip("strict native identity prerequisite unavailable")
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err == unix.ENOSYS || err == unix.EPERM || err == unix.EACCES {
		t.Skipf("pidfd prerequisite unavailable: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := closeStoppedQuery(os.NewFile(uintptr(fd), "self pidfd probe")); err != nil {
		keepStoppedNative(frame, err)
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	frame.files = []*os.File{reader, writer}
	cmd := exec.Command(os.Args[0], "-test.run=^TestObserveServerStoppedChild$")
	frame.cmd = cmd
	cmd.Env = append(os.Environ(), "SKYGO_STOPPED_OBSERVER_CHILD=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = writer
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	frame.started = true
	frame.closePipe(t, 1)
	if t.Failed() {
		return
	}
	var original ServerLifetime
	if err := json.NewDecoder(reader).Decode(&original); err != nil {
		t.Fatalf("child capture: %v", err)
	}
	if !stoppedLifetimePair(current, original) {
		t.Fatalf("child outside strict recovery domain: %+v", original)
	}
	if stopped, err := ObserveServerStopped(current, original); stopped || err != nil {
		if stoppedNativeCloseUncertain(err) {
			keepStoppedNative(frame, err)
		}
		t.Fatalf("live child observation = %v, %v", stopped, err)
	}
	forged := current
	forged.StartTicks++
	if stopped, err := ObserveServerStopped(forged, original); stopped || !errors.Is(err, ErrServerLifetimeUnavailable) {
		if stoppedNativeCloseUncertain(err) {
			keepStoppedNative(frame, err)
		}
		t.Fatalf("changed immutable self identity = %v, %v", stopped, err)
	}
	frame.killAttempted = true
	if err := cmd.Process.Kill(); err != nil {
		keepStoppedNative(frame, err)
		t.Fatal(err)
	}
	frame.killed = true
	// Same Cmd.Wait is the oracle only, never an implementation receipt.
	frame.waitKilled(t)
	if t.Failed() {
		return
	}
	stopped, err := ObserveServerStopped(current, original)
	if err != nil {
		if stoppedNativeCloseUncertain(err) {
			keepStoppedNative(frame, err)
		}
		t.Fatalf("killed/reaped child observation: %v; stderr: %s", err, stderr.String())
	}
	if !stopped {
		// Occupied/reused PID remains Held by design. This is no positive pass.
		t.Skip("post-Wait lookup remained occupied; isolated absent-PID positive prerequisite not demonstrated")
	}
}

func TestObserveServerStoppedChild(t *testing.T) {
	if os.Getenv("SKYGO_STOPPED_OBSERVER_CHILD") != "1" {
		return
	}
	value, err := CaptureServerLifetime()
	if err != nil {
		if stoppedNativeCloseUncertain(err) {
			keepStoppedNative(new(stoppedNativeFrame), err)
		}
		t.Fatal(err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
		t.Fatal(err)
	}
	for {
		if err := unix.Pause(); err != nil && err != unix.EINTR {
			t.Fatal(err)
		}
	}
}
