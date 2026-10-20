//go:build linux || darwin

package control

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

const finalizationCrashChild = "SKYGO_FINALIZATION_CRASH_CHILD"

type finalizationCrashReady struct {
	PID      int
	Nonce    string
	Boundary finalizationCrashBoundary
}

// This frame owns only the originally started child and its original pipes.
// No numeric PID is used to look up or reconstruct a process owner.
type finalizationCrashProcess struct {
	directory                         string
	command                           *exec.Cmd
	input, hold                       *os.File
	inputClosed, holdClosed           bool
	started, joined, killed, retained bool
	done                              chan error
	waitErr, held                     error
	stdout, stderr                    bytes.Buffer
}

var finalizationCrashProcesses struct {
	sync.Mutex
	frames []*finalizationCrashProcess
}

// TestArtifactFinalizationCrashChild is inert unless the parent supplies its
// private directory, nonce and one exact phase. It launches no workload child.
// READY follows completed synchronous APIs; interruption is at an idle boundary.
func TestArtifactFinalizationCrashChild(t *testing.T) {
	stage := os.Getenv(finalizationCrashChild)
	if stage == "" {
		return
	}
	if stage != "before-publication" && stage != "after-publication" {
		t.Fatal("invalid finalization crash stage")
	}
	directory, nonce := os.Getenv("SKYGO_FINALIZATION_CRASH_DIR"), os.Getenv("SKYGO_FINALIZATION_CRASH_NONCE")
	if !filepath.IsAbs(directory) || len(nonce) != 32 {
		t.Fatal("missing private finalization crash identity")
	}
	boundary := finalizationCrashPrepare(t, directory, stage)
	data, err := json.Marshal(finalizationCrashReady{PID: os.Getpid(), Nonce: nonce, Boundary: boundary})
	if err != nil {
		t.Fatal(err)
	}
	// Atomic publication is a test handshake, not a power-loss durability claim.
	if err := os.WriteFile(filepath.Join(directory, "ready.tmp"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(directory, "ready.tmp"), filepath.Join(directory, "ready.json")); err != nil {
		t.Fatal(err)
	}
	var input [1]byte
	_, err = os.Stdin.Read(input[:])
	t.Fatalf("parent released stdin instead of interrupting captured child: %v", err)
}

func TestArtifactFinalizationInterruptedAtDurableBoundary(t *testing.T) {
	for _, stage := range []string{"before-publication", "after-publication"} {
		t.Run(stage, func(t *testing.T) {
			home, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			parent := filepath.Join(home, "tmp", "skypilot", "20261020-finalizing-crash")
			if err := os.MkdirAll(parent, 0700); err != nil {
				t.Fatal(err)
			}
			directory, err := os.MkdirTemp(parent, "boundary-")
			if err != nil {
				t.Fatal(err)
			}
			f := &finalizationCrashProcess{directory: directory}
			t.Cleanup(func() { f.cleanup(t) })
			var random [16]byte
			if _, err := rand.Read(random[:]); err != nil {
				t.Fatal(err)
			}
			nonce := hex.EncodeToString(random[:])
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			f.command = exec.Command(executable, "-test.run=^TestArtifactFinalizationCrashChild$", "-test.timeout=60s")
			f.command.Env = append(os.Environ(), finalizationCrashChild+"="+stage, "SKYGO_FINALIZATION_CRASH_DIR="+directory, "SKYGO_FINALIZATION_CRASH_NONCE="+nonce)
			if testing.CoverMode() != "" {
				coverage := filepath.Join(directory, "coverage")
				if err := os.Mkdir(coverage, 0700); err != nil {
					t.Fatal(err)
				}
				f.command.Env = append(f.command.Env, "GOCOVERDIR="+coverage)
			}
			f.command.Stdout, f.command.Stderr = &f.stdout, &f.stderr
			f.input, f.hold, err = os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			f.command.Stdin = f.input
			if err := f.command.Start(); err != nil {
				f.held = err
				t.Fatal(err)
			}
			f.started = true
			f.done = make(chan error, 1)
			go func() { f.done <- f.command.Wait() }() // one original Wait, never retried
			f.inputClosed = true
			if err := f.input.Close(); err != nil {
				f.held = err
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			ready, readyErr := f.awaitReady(ctx)
			cancel()
			joinErr := f.interruptAndJoin()
			if readyErr != nil || joinErr != nil {
				f.held = errors.Join(f.held, readyErr, joinErr)
				if f.joined {
					t.Fatalf("durable boundary: %v\nstdout:%s\nstderr:%s", f.held, &f.stdout, &f.stderr)
				}
				t.Fatal(f.held)
			}
			if f.stdout.Len() != 0 || f.stderr.Len() != 0 {
				t.Fatalf("unexpected child diagnostic output: stdout:%s stderr:%s", &f.stdout, &f.stderr)
			}
			if ready.PID != f.command.Process.Pid || ready.Nonce != nonce || ready.Boundary.Stage != stage {
				t.Fatalf("readiness does not identify original child and boundary: %+v", ready)
			}
			if err := ready.Boundary.Reference.Validate(); err != nil {
				t.Fatal(err)
			}
			if ready.Boundary.File.Device == 0 || ready.Boundary.File.Inode == 0 || ready.Boundary.Acknowledged != uint64(ready.Boundary.Reference.Size) {
				t.Fatalf("invalid durable upload receipt: %+v", ready.Boundary)
			}
			// Recovery is forbidden until that SAME child's positive Wait receipt.
			// It reopens existing state/root and never begins a replacement upload.
			finalizationCrashRecover(t, directory, ready.Boundary)
		})
	}
}

func (f *finalizationCrashProcess) awaitReady(ctx context.Context) (finalizationCrashReady, error) {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(f.directory, "ready.json"))
		if err == nil {
			var ready finalizationCrashReady
			if err := json.Unmarshal(data, &ready); err != nil {
				return ready, err
			}
			return ready, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return finalizationCrashReady{}, err
		}
		select {
		case err := <-f.done:
			f.joined, f.waitErr = true, err
			return finalizationCrashReady{}, fmt.Errorf("child exited before readiness: %w", errors.Join(err, io.ErrUnexpectedEOF))
		case <-ctx.Done():
			return finalizationCrashReady{}, ctx.Err()
		case <-tick.C:
		}
	}
}

func (f *finalizationCrashProcess) interruptAndJoin() error {
	if f.joined {
		return errors.New("child exited before controlled interruption")
	}
	if !f.started || f.killed {
		return errors.New("original interruption owner unavailable")
	}
	f.killed = true
	killErr := f.command.Process.Kill() // ONLY this captured original child
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case f.waitErr = <-f.done:
		f.joined = true
	case <-timer.C:
		return errors.Join(killErr, errors.New("original child Wait remains unjoined"))
	}
	if killErr != nil {
		return killErr
	}
	var exit *exec.ExitError
	if !errors.As(f.waitErr, &exit) {
		return errors.Join(f.waitErr, errors.New("child did not report controlled signal exit"))
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return fmt.Errorf("unexpected child Wait receipt: %w", f.waitErr)
	}
	return f.closePipes()
}

func (f *finalizationCrashProcess) closePipes() error {
	var result error
	if f.input != nil && !f.inputClosed {
		f.inputClosed = true
		result = errors.Join(result, f.input.Close())
	}
	if f.hold != nil && !f.holdClosed {
		f.holdClosed = true
		result = errors.Join(result, f.hold.Close())
	}
	return result
}

func (f *finalizationCrashProcess) cleanup(t *testing.T) {
	t.Helper()
	if f.started && !f.joined && !f.killed {
		f.held = errors.Join(f.held, f.interruptAndJoin())
	}
	if !f.started || f.joined {
		f.held = errors.Join(f.held, f.closePipes())
	}
	if f.started && !f.joined {
		f.held = errors.Join(f.held, errors.New("retaining original unjoined child"))
	}
	if f.held != nil {
		finalizationCrashProcesses.Lock()
		if !f.retained {
			f.retained = true
			finalizationCrashProcesses.frames = append(finalizationCrashProcesses.frames, f)
		}
		finalizationCrashProcesses.Unlock()
		t.Errorf("retaining original interruption frame: %v", f.held)
	}
	// Both successful and failed namespaces are retained as bounded evidence.
	// No independent TempDir removal can erase a live or uncertain original.
	t.Logf("finalization interruption evidence: %s", f.directory)
}
