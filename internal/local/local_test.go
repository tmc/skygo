package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestProgram(t *testing.T) {
	mode := os.Getenv("SKYGO_TEST_PROGRAM")
	if mode == "" {
		return
	}
	switch mode {
	case "success":
		fmt.Fprint(os.Stdout, "hello")
		fmt.Fprint(os.Stderr, " world")
	case "fail":
		os.Exit(7)
	case "tree":
		child := exec.Command(os.Args[0], "-test.run=^TestProgram$")
		child.Env = append(os.Environ(), "SKYGO_TEST_PROGRAM=tick")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
		time.Sleep(time.Minute)
	case "tick":
		for {
			if err := os.WriteFile(os.Getenv("SKYGO_TICK"), []byte(time.Now().String()), 0600); err != nil {
				os.Exit(3)
			}
			time.Sleep(10 * time.Millisecond)
		}
	case "sleep":
		time.Sleep(time.Minute)
	case "output":
		fmt.Fprint(os.Stdout, strings.Repeat("x", backend.MaxLogBytes+10000))
	}
	os.Exit(0)
}

func task(mode string) backend.Task {
	return backend.Task{Argv: []string{os.Args[0], "-test.run=^TestProgram$"}, Env: map[string]string{"SKYGO_TEST_PROGRAM": mode}}
}
func newBackend(t *testing.T) *Backend {
	t.Helper()
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}
func completed(t *testing.T, b *Backend, id string) backend.Status {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		s, err := b.Observe(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if s.State.Terminal() {
			return s
		}
		select {
		case <-deadline:
			t.Fatal("job did not finish")
		case <-tick.C:
		}
	}
}

func TestLifecycle(t *testing.T) {
	for i, tt := range []struct {
		name string
		want backend.State
		exit int32
	}{{"success", backend.Succeeded, 0}, {"fail", backend.Failed, 7}, {"output", backend.Succeeded, 0}} {
		t.Run(tt.name, func(t *testing.T) {
			b := newBackend(t)
			id := fmt.Sprintf("%032x", i)
			if err := b.Start(context.Background(), id, task(tt.name)); err != nil {
				t.Fatal(err)
			}
			s := completed(t, b, id)
			if s.State != tt.want || s.ExitCode == nil || *s.ExitCode != tt.exit {
				t.Fatalf("status = %+v", s)
			}
			var output []byte
			var offset int64
			var truncated bool
			for {
				chunk, err := b.Logs(context.Background(), id, offset, backend.MaxChunkBytes)
				if err != nil {
					t.Fatal(err)
				}
				output = append(output, chunk.Data...)
				offset = chunk.NextOffset
				truncated = chunk.Truncated
				if chunk.Complete {
					break
				}
			}
			if tt.name == "success" && string(output) != "hello world" {
				t.Fatalf("output = %q", output)
			}
			if tt.name == "output" && (len(output) != backend.MaxLogBytes || !truncated) {
				t.Fatalf("output length %d, truncated %v", len(output), truncated)
			}
			if err := b.Start(context.Background(), id, task(tt.name)); err == nil {
				t.Fatal("repeated start succeeded")
			}
			if _, err := b.Logs(context.Background(), id, offset+1, 1); !errors.Is(err, fs.ErrInvalid) {
				t.Fatalf("out-of-range read error = %v", err)
			}
		})
	}
}

func TestCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout", "close"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			id := strings.Repeat("a", 32)
			task := task("sleep")
			if mode == "timeout" {
				task.TimeoutSeconds = 1
			}
			ctx, cancel := context.WithCancel(context.Background())
			if err := b.Start(ctx, id, task); err != nil {
				t.Fatal(err)
			}
			cancel()
			s, err := b.Observe(context.Background(), id)
			if err != nil || s.State != backend.Running {
				t.Fatalf("startup context canceled workload: %+v %v", s, err)
			}
			switch mode {
			case "cancel":
				if err := b.Cancel(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			case "close":
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s = completed(t, b, id)
			want := backend.Canceled
			if mode == "timeout" {
				want = backend.Failed
			}
			if s.State != want {
				t.Fatalf("status = %+v", s)
			}
			if mode == "timeout" && s.Reason != "execution timed out" {
				t.Fatalf("reason = %q", s.Reason)
			}
		})
	}
}

func TestRestart(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("b", 32)
	if err := b.Start(context.Background(), id, task("output")); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	restarted, err := New(b.directory)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	s, err := restarted.Observe(context.Background(), id)
	if err != nil || s.State != backend.Unknown {
		t.Fatalf("restart status %+v, %v", s, err)
	}
	if err := restarted.Start(context.Background(), id, task("success")); err == nil {
		t.Fatal("restart respawned job")
	}
	chunk, err := restarted.Logs(context.Background(), id, backend.MaxLogBytes-1, 1)
	if err != nil || !chunk.Truncated || !chunk.Complete {
		t.Fatalf("restart logs %+v, %v", chunk, err)
	}
}

func TestValidation(t *testing.T) {
	b := newBackend(t)
	for i, task := range []backend.Task{{}, {Argv: []string{"x"}, Image: "x"}, {Argv: []string{"x"}, CPUMillis: 1}, {Argv: []string{"x"}, MemoryBytes: 1}, {Argv: []string{"x"}, Accelerator: "gpu"}, {Argv: []string{"x"}, TimeoutSeconds: -1}, {Argv: []string{"x"}, Env: map[string]string{"a=b": "c"}}} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			if b.Check(task) == nil {
				t.Fatal("invalid task accepted")
			}
		})
	}
	if err := b.Start(context.Background(), "../escape", task("success")); err == nil {
		t.Fatal("invalid ID accepted")
	}
}

func TestProcessGroup(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("c", 32)
	path := filepath.Join(t.TempDir(), "tick")
	task := task("tree")
	task.Env["SKYGO_TICK"] = path
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("descendant did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := b.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("descendant survived group cancellation")
	}
}

func TestConcurrentCancelAndLogs(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("d", 32)
	if err := b.Start(context.Background(), id, task("sleep")); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Cancel(context.Background(), id); err != nil {
				t.Error(err)
			}
			if _, err := b.Observe(context.Background(), id); err != nil {
				t.Error(err)
			}
			if _, err := b.Logs(context.Background(), id, 0, 1); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s := completed(t, b, id); s.State != backend.Canceled {
		t.Fatalf("status = %+v", s)
	}
}

func TestLogRanges(t *testing.T) {
	b := newBackend(t)
	id := strings.Repeat("e", 32)
	if err := b.Start(context.Background(), id, task("success")); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	for _, tt := range []struct {
		offset int64
		limit  int
	}{{-1, 1}, {0, 0}, {0, backend.MaxChunkBytes + 1}, {12, 1}} {
		if _, err := b.Logs(context.Background(), id, tt.offset, tt.limit); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("Logs(%d,%d) error = %v; want fs.ErrInvalid", tt.offset, tt.limit, err)
		}
	}
	chunk, err := b.Logs(context.Background(), id, 11, 1)
	if err != nil || !chunk.Complete || len(chunk.Data) != 0 {
		t.Fatalf("read at end = %+v, %v", chunk, err)
	}
}
