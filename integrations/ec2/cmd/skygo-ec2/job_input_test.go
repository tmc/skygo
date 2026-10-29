//go:build linux || darwin

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/ec2/internal/ec2job"
)

func TestTaskInputRegular(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "task.json")
	want := []byte(`{"argv":["echo","ok"]}`)
	if err := os.WriteFile(name, want, 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "task-link.json")
	if err := os.Symlink(name, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{name, link} {
		in, err := readTask(jobOptions{task: path}, nil)
		if err != nil || !bytes.Equal(in.JSON, want) {
			t.Fatalf("readTask = %v, %v", in, err)
		}
	}
	if _, err := readTask(jobOptions{task: dir}, nil); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("directory input = %v", err)
	}
}

// A no-writer FIFO must be refused locally, not wait indefinitely in Open.
// The child owns the potentially blocking original; CombinedOutput joins it,
// including when the parent's context must kill it after a regression.
func TestTaskInputFIFO(t *testing.T) {
	const childPath = "SKYGO_EC2_TASK_FIFO"
	if name := os.Getenv(childPath); name != "" {
		if _, err := readTask(jobOptions{task: name}, nil); err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("FIFO input = %v", err)
		}
		return
	}
	name := filepath.Join(t.TempDir(), "task.fifo")
	if err := syscall.Mkfifo(name, 0o600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestTaskInputFIFO$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), childPath+"="+name)
	out, err := cmd.CombinedOutput()
	if err != nil || ctx.Err() != nil {
		t.Fatalf("FIFO child = %v, context = %v\n%s", err, ctx.Err(), out)
	}
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 0 {
		t.Fatalf("FIFO child not joined successfully: %v", cmd.ProcessState)
	}
	t.Logf("FIFO child %d joined with exit status 0", cmd.Process.Pid)
}

var taskInputHeld []error

// The hook's real first Close precedes the reader's Close, whose error is
// a MODEL of uncertainty. The original file and both causes stay reachable.
func TestTaskInputRefusalKeepsClose(t *testing.T) {
	dir, err := os.MkdirTemp("", "ec2-task-kept-")
	if err != nil {
		t.Fatal(err)
	}
	var kept error
	t.Cleanup(func() { taskInputHeld = append(taskInputHeld, kept) })
	var original *os.File
	in, err := readTask(jobOptions{task: dir}, func(f *os.File) {
		original = f
		if err := f.Close(); err != nil {
			t.Errorf("model close: %v", err)
		}
	})
	kept = err
	var closeErr *taskCloseError
	if in != nil || !errors.Is(err, ec2job.ErrUnsettled) || !errors.Is(err, os.ErrClosed) || !strings.Contains(err.Error(), "regular file") || !errors.As(err, &closeErr) || closeErr.f != original {
		t.Fatalf("readTask = %v, %v; want the refusal and original failed Close", in, err)
	}
	t.Logf("keeping %s with its original failed Close", dir)
}
