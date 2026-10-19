package local

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestRetryProgram(t *testing.T) {
	mode := os.Getenv("SKYGO_RETRY_PROGRAM")
	if mode == "" {
		return
	}
	switch mode {
	case "bounded-live":
		time.Sleep(time.Second)
	case "live":
		if err := os.WriteFile(os.Getenv("SKYGO_RETRY_MARKER"), nil, 0600); err != nil {
			os.Exit(8)
		}
		time.Sleep(time.Minute)
	case "retention-flood":
		for {
			if _, err := os.Stat(os.Getenv("SKYGO_RETRY_MARKER")); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		fmt.Fprint(os.Stdout, strings.Repeat("x", backend.MaxLogBytes+65536))
		os.Exit(7)
	case "fail-once":
		f, err := os.OpenFile(os.Getenv("SKYGO_RETRY_MARKER"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			f.Close()
			fmt.Fprint(os.Stdout, "first attempt\n")
			os.Exit(7)
		}
		if !os.IsExist(err) {
			os.Exit(8)
		}
		fmt.Fprint(os.Stdout, "second attempt\n")
	case "bounded-pipe-holder":
		time.Sleep(time.Second)
		if err := os.WriteFile(os.Getenv("SKYGO_RETRY_MARKER"), nil, 0600); err != nil {
			os.Exit(8)
		}
	}
	os.Exit(0)
}

func retryProgram(mode, marker string) backend.Task {
	return backend.Task{Argv: []string{os.Args[0], "-test.run=^TestRetryProgram$"}, Env: map[string]string{"SKYGO_RETRY_PROGRAM": mode, "SKYGO_RETRY_MARKER": marker}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}}
}

func TestRetryAttemptLogs(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	task := retryProgram("fail-once", filepath.Join(t.TempDir(), "marker"))
	var instances []string
	for i, tt := range []struct {
		code   int32
		output string
	}{{7, "first attempt\n"}, {0, "second attempt\n"}} {
		id := fmt.Sprintf("%032x", i+100)
		if err := b.Start(context.Background(), id, task); err != nil {
			t.Fatal(err)
		}
		s := completed(t, b, id)
		if s.ExitCode == nil || *s.ExitCode != tt.code || s.Termination != backend.StoppedWithExitCode {
			t.Fatalf("attempt %d status=%+v", i+1, s)
		}
		chunk, err := b.Logs(context.Background(), backend.Reference{ID: id, Instance: s.Instance}, 0, backend.MaxChunkBytes)
		if err != nil || string(chunk.Data) != tt.output || !chunk.Complete {
			t.Fatalf("attempt %d logs=%+v, %v", i+1, chunk, err)
		}
		instances = append(instances, s.Instance)
	}
	if instances[0] == instances[1] {
		t.Fatal("attempt identities reused")
	}
	// Scheduling these attempts is the controller's responsibility. This test
	// qualifies the backend's separate identities/logs and positive evidence only.
}

func TestRetryEscapedInheritedPipe(t *testing.T) {
	b := newBackend(t)
	requireRetry(t, b)
	marker := filepath.Join(t.TempDir(), "holder-done")
	var holder *exec.Cmd
	joined := false
	t.Cleanup(func() {
		if holder != nil && holder.Process != nil && !joined {
			// Only this original unreaped child handle is used; never a discovered PID.
			holder.Process.Kill()
			holder.Wait()
		}
	})
	b.retryOps.prepare = func(cmd *exec.Cmd) {
		configureRetry(cmd)
		holder = exec.Command(os.Args[0], "-test.run=^TestRetryProgram$")
		holder.Env = append(os.Environ(), "SKYGO_RETRY_PROGRAM=bounded-pipe-holder", "SKYGO_RETRY_MARKER="+marker)
		holder.Stdout, holder.Stderr = cmd.Stdout, cmd.Stderr
		configure(holder) // A different owned group deliberately retains the same pipe.
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}
	}
	id := strings.Repeat("d", 32)
	if err := b.Start(context.Background(), id, retryTask("fail")); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.Termination != backend.UnknownTermination || s.ExitCode == nil || *s.ExitCode != 7 || !strings.Contains(s.Reason, "output pipes remained open") {
		t.Fatalf("status=%+v", s)
	}
	// Join the exact pipe holder, not merely its marker. It must self-terminate;
	// the backend has no authority to kill its distinct process group.
	done := make(chan error, 1)
	go func() { done <- holder.Wait() }()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		holder.Process.Kill() // Original handle, before its single Wait completes.
		<-done
		joined = true
		t.Fatal("bounded pipe holder did not finish")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("pipe holder did not self-complete: %v", err)
	}
}
