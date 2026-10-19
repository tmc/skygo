//go:build linux || darwin

package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestDefaultStoppedChild(t *testing.T) {
	for _, mode := range []string{"continue", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t)
			id := strings.Repeat("b", 32)
			dir := t.TempDir()
			tick := filepath.Join(dir, "tick")
			release := filepath.Join(dir, "release")
			task := backend.Task{Argv: []string{os.Args[0], "-test.run=^TestDefaultProgram$"}, Env: map[string]string{"SKYGO_DEFAULT_PROGRAM": "control", "SKYGO_DEFAULT_TICK": tick, "SKYGO_DEFAULT_RELEASE": release}}
			if err := b.Start(context.Background(), id, task); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(tick); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("original helper did not become live")
				}
				time.Sleep(time.Millisecond)
			}
			e := ownedExecution(t, b, id)
			send := func(sig syscall.Signal) {
				t.Helper()
				b.mu.Lock()
				defer b.mu.Unlock()
				if e.owned.signalClosed {
					t.Fatal("attempted test signal after lifecycle fence")
				}
				if err := syscall.Kill(-e.cmd.Process.Pid, sig); err != nil {
					t.Fatal(err)
				}
			}
			send(syscall.SIGSTOP)
			select {
			case <-e.done:
				t.Fatal("stopped child was treated as exited")
			case <-time.After(100 * time.Millisecond):
			}
			b.mu.Lock()
			closed := e.owned.signalClosed
			b.mu.Unlock()
			if closed {
				t.Fatal("nonterminal stop closed signal admission")
			}
			if s, err := b.Observe(context.Background(), backend.Reference{ID: id}); err != nil || s.State != backend.Running {
				t.Fatalf("stopped status=%+v %v", s, err)
			}
			if mode == "continue" {
				send(syscall.SIGCONT)
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
				t.Fatal(err)
			}
			s := completed(t, b, id)
			want := backend.Succeeded
			if mode == "cancel" {
				want = backend.Canceled
			}
			if s.State != want || s.Instance != "" || s.Termination != backend.UnknownTermination {
				t.Fatalf("status=%+v", s)
			}
		})
	}
}
