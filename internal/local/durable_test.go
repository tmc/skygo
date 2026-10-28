//go:build linux || darwin

package local

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

// TestMain dispatches the hidden supervisor command and the API helper used
// by the restart tests. Both run only when explicitly selected. A supervisor
// launched with readyGateEnv set holds its readiness until the test releases it.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == SupervisorCommand {
		if gate := os.Getenv(readyGateEnv); gate != "" {
			os.Exit(gatedSupervisor(os.Args[2:], gate))
		}
		os.Exit(RunSupervisor(os.Args[2:]))
	}
	if os.Getenv("SKYGO_DURABLE_TEST_API") == "1" {
		os.Exit(durableTestAPI())
	}
	os.Exit(m.Run())
}

// durableTestAPI is a disposable API server: it starts one durable job,
// reports "started" and then blocks until it is killed.
func durableTestAPI() int {
	var task backend.Task
	if err := json.Unmarshal([]byte(os.Getenv("SKYGO_DURABLE_TEST_TASK")), &task); err != nil {
		fmt.Println("error:", err)
		return 1
	}
	b, err := newDurableBackend(os.Getenv("SKYGO_DURABLE_TEST_LOGS"), os.Getenv("SKYGO_DURABLE_TEST_STATE"))
	if err != nil {
		fmt.Println("error:", err)
		return 1
	}
	if err := b.Start(context.Background(), os.Getenv("SKYGO_DURABLE_TEST_ID"), task); err != nil {
		fmt.Println("error:", err)
		return 1
	}
	fmt.Println("started")
	select {}
}

func newDurableBackend(logs, state string) (*Backend, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	b, err := New(logs)
	if err != nil {
		return nil, err
	}
	if err := b.EnableDurable(DurableConfig{State: state, Executable: exe}); err != nil {
		return nil, err
	}
	return b, nil
}

type durableFixture struct {
	t                  *testing.T
	logs, state, count string
	id                 string
	task               backend.Task
}

// newDurableFixture returns a job whose workload appends one line to a
// launch counter, prints begin, sleeps and prints end.
func newDurableFixture(t *testing.T, sleep string) *durableFixture {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &durableFixture{t: t, logs: filepath.Join(dir, "logs"), state: filepath.Join(dir, "state"), count: filepath.Join(dir, "launches"), id: strings.Repeat("d", 32)}
	f.task = backend.Task{
		Argv: []string{"/bin/sh", "-c", `echo $$ >> "$COUNT"; echo begin; sleep ` + sleep + `; echo end`},
		Env:  map[string]string{"COUNT": f.count},
	}
	return f
}

func (f *durableFixture) ref(instance string) backend.Reference {
	return backend.Reference{ID: f.id, Task: backend.CloneTask(f.task), Instance: instance}
}

func (f *durableFixture) backend() *Backend {
	f.t.Helper()
	b, err := newDurableBackend(f.logs, f.state)
	if err != nil {
		f.t.Fatal(err)
	}
	return b
}

// startAPI starts the job in a helper API process. The returned channel
// receives the helper's first output line: "started" once Start returned.
func (f *durableFixture) startAPI() (*exec.Cmd, <-chan string) {
	f.t.Helper()
	exe, err := os.Executable()
	if err != nil {
		f.t.Fatal(err)
	}
	task, err := json.Marshal(f.task)
	if err != nil {
		f.t.Fatal(err)
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), "SKYGO_DURABLE_TEST_API=1", "SKYGO_DURABLE_TEST_LOGS="+f.logs,
		"SKYGO_DURABLE_TEST_STATE="+f.state, "SKYGO_DURABLE_TEST_ID="+f.id, "SKYGO_DURABLE_TEST_TASK="+string(task))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		f.t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { f.killAPI(cmd) })
	f.cleanupJob()
	started := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		started <- strings.TrimSpace(line)
	}()
	return cmd, started
}

func (f *durableFixture) awaitStarted(started <-chan string) {
	f.t.Helper()
	if line := <-started; line != "started" {
		f.t.Fatalf("API helper: %q", line)
	}
}

// killAPI SIGKILLs and reaps the helper API process, which this test owns.
func (f *durableFixture) killAPI(cmd *exec.Cmd) {
	if cmd.ProcessState != nil {
		return
	}
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
}

// cleanupJob cancels the job through its own supervisor if a test leaves it
// running. It never signals a process by PID.
func (f *durableFixture) cleanupJob() {
	f.t.Cleanup(func() {
		b, err := newDurableBackend(f.logs, f.state)
		if err != nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if s, err := b.Observe(ctx, f.ref("")); err == nil && !s.State.Terminal() {
			if err := b.Cancel(ctx, f.ref("")); err != nil {
				f.t.Errorf("cleanup cancel: %v", err)
			}
		}
	})
}

func (f *durableFixture) waitFile(name string) {
	f.t.Helper()
	path := filepath.Join(f.state, f.id, name)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Lstat(path); err == nil {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("%s did not appear", name)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// await observes until a terminal state and returns it.
func (f *durableFixture) await(b *Backend, ref backend.Reference) backend.Status {
	f.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		s, err := b.Observe(context.Background(), ref)
		if err != nil {
			f.t.Fatal(err)
		}
		if s.State.Terminal() {
			return s
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("still %s", s.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (f *durableFixture) launches() int {
	f.t.Helper()
	data, err := os.ReadFile(f.count)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.Count(string(data), "\n")
}

func (f *durableFixture) logsOf(b *Backend, ref backend.Reference) backend.LogChunk {
	f.t.Helper()
	c, err := b.Logs(context.Background(), ref, 0, backend.MaxChunkBytes)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func (f *durableFixture) running() durableRunning {
	f.t.Helper()
	var r durableRunning
	if err := readDurable(filepath.Join(f.state, f.id), durableRunningFile, maxDurableRecord, &r); err != nil {
		f.t.Fatal(err)
	}
	return r
}

func TestDurableSurvivesAPIKill(t *testing.T) {
	f := newDurableFixture(t, "2")
	api, started := f.startAPI()
	f.awaitStarted(started)
	f.killAPI(api)

	b := f.backend()
	defer b.Close()
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil {
		t.Fatal(err)
	}
	if s.State != backend.Running || s.Instance == "" {
		t.Fatalf("after API kill: %+v, want running with instance", s)
	}
	if alive, err := f.running().Supervisor.alive(); err != nil || !alive {
		t.Fatalf("supervisor alive = %v, %v", alive, err)
	}
	if err := b.Start(context.Background(), f.id, f.task); err == nil {
		t.Fatal("restarted backend started the job again")
	}
	s = f.await(b, f.ref(s.Instance))
	if s.State != backend.Succeeded || s.ExitCode == nil || *s.ExitCode != 0 {
		t.Fatalf("final status %+v", s)
	}
	c := f.logsOf(b, f.ref(s.Instance))
	if string(c.Data) != "begin\nend\n" || !c.Complete || c.Truncated {
		t.Fatalf("logs %q complete=%v truncated=%v", c.Data, c.Complete, c.Truncated)
	}
	if n := f.launches(); n != 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

func TestDurableExitWhileAPIDown(t *testing.T) {
	f := newDurableFixture(t, "0.2")
	api, _ := f.startAPI()
	f.waitFile(durableClaimFile)
	f.killAPI(api)
	f.waitFile(durableStatusFile)

	b := f.backend()
	defer b.Close()
	s := f.await(b, f.ref(""))
	if s.State != backend.Succeeded {
		t.Fatalf("status %+v", s)
	}
	if c := f.logsOf(b, f.ref(s.Instance)); string(c.Data) != "begin\nend\n" || !c.Complete {
		t.Fatalf("logs %q complete=%v", c.Data, c.Complete)
	}
	if n := f.launches(); n != 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

// TestDurableLostStartResponse kills the API as soon as the supervisor claims
// the job, before Start's result can reach any caller.
func TestDurableLostStartResponse(t *testing.T) {
	f := newDurableFixture(t, "1")
	api, _ := f.startAPI()
	f.waitFile(durableClaimFile)
	f.killAPI(api)

	b := f.backend()
	defer b.Close()
	s := f.await(b, f.ref(""))
	if s.State != backend.Succeeded || s.Instance == "" {
		t.Fatalf("status %+v", s)
	}
	if n := f.launches(); n != 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

func TestDurableCancelAfterRestart(t *testing.T) {
	f := newDurableFixture(t, "30")
	api, started := f.startAPI()
	f.awaitStarted(started)
	f.killAPI(api)
	child := f.running().Child

	b := f.backend()
	defer b.Close()
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Running {
		t.Fatalf("observe: %+v, %v", s, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Cancel(ctx, f.ref(s.Instance)); err != nil {
		t.Fatal(err)
	}
	s, err = b.Observe(ctx, f.ref(s.Instance))
	if err != nil || s.State != backend.Canceled {
		t.Fatalf("after cancel: %+v, %v", s, err)
	}
	if alive, err := child.alive(); err != nil || alive {
		t.Fatalf("workload alive = %v, %v after cancel", alive, err)
	}
	if n := f.launches(); n != 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

func TestDurableCloseDetaches(t *testing.T) {
	f := newDurableFixture(t, "0.5")
	f.cleanupJob()
	b := f.backend()
	if err := b.Start(context.Background(), f.id, f.task); err != nil {
		t.Fatal(err)
	}
	if !b.Detaches(f.id) {
		t.Fatal("durable job does not detach")
	}
	b.mu.Lock()
	reserved := b.durableReservationsLocked()
	b.mu.Unlock()
	if len(reserved) != 1 || reserved[0].ID != f.id {
		t.Fatalf("reservations %+v", reserved)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2 := f.backend()
	defer b2.Close()
	s := f.await(b2, f.ref(""))
	if s.State != backend.Succeeded {
		t.Fatalf("status after Close %+v", s)
	}
	if n := f.launches(); n != 1 {
		t.Fatalf("workload launched %d times", n)
	}
}

func TestDurableStartFailure(t *testing.T) {
	f := newDurableFixture(t, "0")
	f.task.Argv = []string{filepath.Join(f.state, "missing")}
	b := f.backend()
	defer b.Close()
	if err := b.Start(context.Background(), f.id, f.task); err == nil {
		t.Fatal("started a missing program")
	}
	s := f.await(b, f.ref(""))
	if s.State != backend.Failed || !strings.HasPrefix(s.Reason, "start: ") {
		t.Fatalf("status %+v", s)
	}
}

// writeIntent records a durable intent and log as Start would, without
// launching a supervisor.
func (f *durableFixture) writeIntent() durableIntent {
	f.t.Helper()
	for _, dir := range []string{f.logs, f.state, filepath.Join(f.state, f.id)} {
		if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			f.t.Fatal(err)
		}
	}
	in := durableIntent{Version: durableVersion, ID: f.id, Instance: "local-" + strings.Repeat("a", 32), Task: f.task, Launch: durableLaunch{Env: map[string]string{"CUDA_VISIBLE_DEVICES": "", "NVIDIA_VISIBLE_DEVICES": "void"}}, Logs: f.logs}
	if err := writeDurable(filepath.Join(f.state, f.id), durableIntentFile, in); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.logs, f.id+".log"), nil, 0600); err != nil {
		f.t.Fatal(err)
	}
	return in
}

// TestDurableUnclaimedIntentIsFenced shows that a restarted backend fences an
// intent no supervisor claimed, and a late supervisor then refuses to launch.
func TestDurableUnclaimedIntentIsFenced(t *testing.T) {
	f := newDurableFixture(t, "0")
	in := f.writeIntent()
	b := f.backend()
	defer b.Close()
	s, err := b.Observe(context.Background(), f.ref(""))
	if err != nil || s.State != backend.Failed || s.Instance != in.Instance {
		t.Fatalf("unclaimed intent: %+v, %v", s, err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.OpenFile(filepath.Join(f.logs, f.id+".log"), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cmd := exec.Command(exe, SupervisorCommand, filepath.Join(f.state, f.id))
	cmd.ExtraFiles = []*os.File{log, w}
	err = cmd.Run()
	w.Close()
	if err == nil {
		t.Fatal("late supervisor accepted a fenced job")
	}
	msg, _ := bufio.NewReader(r).ReadString('\n')
	if !strings.Contains(msg, "already claimed") {
		t.Fatalf("late supervisor said %q", msg)
	}
	if n := f.launches(); n != 0 {
		t.Fatalf("fenced workload launched %d times", n)
	}
}

func TestDurableIdentityNegatives(t *testing.T) {
	self, err := identifyProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	reused := self
	reused.Start++ // this PID, another incarnation
	rebooted := self
	rebooted.Boot = "another-boot"
	tests := []struct {
		name    string
		setup   func(f *durableFixture, in durableIntent)
		ref     func(f *durableFixture) backend.Reference
		want    backend.State
		wantErr error
	}{
		{name: "other instance", ref: func(f *durableFixture) backend.Reference { return f.ref("local-other") }, wantErr: backend.ErrIdentityMismatch},
		{name: "changed task", ref: func(f *durableFixture) backend.Reference {
			r := f.ref("")
			r.Task.Argv = []string{"/bin/true"}
			return r
		}, wantErr: backend.ErrIdentityMismatch},
		{name: "reused supervisor pid", setup: func(f *durableFixture, in durableIntent) {
			f.writeRecords(in, reused, self)
		}, want: backend.Unknown},
		{name: "rebooted", setup: func(f *durableFixture, in durableIntent) {
			f.writeRecords(in, rebooted, rebooted)
		}, want: backend.Unknown},
		{name: "success without output completion", setup: func(f *durableFixture, in durableIntent) {
			f.writeRecords(in, reused, reused)
			zero := int32(0)
			f.write(durableStatusFile, durableStatus{Version: durableVersion, Instance: in.Instance, State: backend.Succeeded, ExitCode: &zero, GroupClosed: true})
		}, want: backend.Unknown},
		{name: "symlinked status", setup: func(f *durableFixture, in durableIntent) {
			f.writeRecords(in, reused, reused)
			zero := int32(0)
			other := filepath.Join(f.state, "elsewhere.json")
			data, _ := json.Marshal(durableStatus{Version: durableVersion, Instance: in.Instance, State: backend.Succeeded, ExitCode: &zero, OutputComplete: true, GroupClosed: true})
			if err := os.WriteFile(other, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(other, filepath.Join(f.state, f.id, durableStatusFile)); err != nil {
				t.Fatal(err)
			}
		}, want: backend.Unknown},
		{name: "status for another instance", setup: func(f *durableFixture, in durableIntent) {
			f.writeRecords(in, reused, reused)
			zero := int32(0)
			f.write(durableStatusFile, durableStatus{Version: durableVersion, Instance: "local-other", State: backend.Succeeded, ExitCode: &zero, OutputComplete: true, GroupClosed: true})
		}, want: backend.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDurableFixture(t, "0")
			in := f.writeIntent()
			if tt.setup != nil {
				tt.setup(f, in)
			} else {
				f.writeRecords(in, self, self)
			}
			b := f.backend()
			defer b.Close()
			ref := f.ref("")
			if tt.ref != nil {
				ref = tt.ref(f)
			}
			s, err := b.Observe(context.Background(), ref)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Observe = %+v, %v; want %v", s, err, tt.wantErr)
				}
				if err := b.Cancel(context.Background(), ref); !errors.Is(err, tt.wantErr) {
					t.Fatalf("Cancel = %v; want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil || s.State != tt.want {
				t.Fatalf("Observe = %+v, %v; want %s", s, err, tt.want)
			}
			if err := b.Cancel(context.Background(), ref); err == nil {
				t.Fatal("Cancel of an unknown durable execution succeeded")
			}
			if _, err := os.Lstat(filepath.Join(f.state, f.id, durableCancelFile)); err == nil {
				t.Fatal("Cancel of an unknown durable execution recorded a request")
			}
		})
	}
}

// writeRecords forges a supervisor claim and running record.
func (f *durableFixture) writeRecords(in durableIntent, supervisor, child processIdentity) {
	f.t.Helper()
	f.write(durableClaimFile, durableClaim{Version: durableVersion, Instance: in.Instance, Supervisor: &supervisor})
	f.write(durableRunningFile, durableRunning{Version: durableVersion, Instance: in.Instance, Supervisor: supervisor, Child: child})
}

func (f *durableFixture) write(name string, v any) {
	f.t.Helper()
	if err := writeDurable(filepath.Join(f.state, f.id), name, v); err != nil {
		f.t.Fatal(err)
	}
}

func TestProcessIdentity(t *testing.T) {
	self, err := identifyProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if alive, err := self.alive(); err != nil || !alive {
		t.Fatalf("self alive = %v, %v", alive, err)
	}
	cmd := exec.Command("/bin/sh", "-c", "exit 0")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	child, err := identifyProcess(cmd.Process.Pid)
	if err != nil && !errors.Is(err, errProcessGone) {
		t.Fatal(err)
	}
	cmd.Wait()
	if err == nil {
		if alive, err := child.alive(); err != nil || alive {
			t.Fatalf("reaped child alive = %v, %v", alive, err)
		}
	}
}

func TestEnableDurableConfig(t *testing.T) {
	f := newDurableFixture(t, "0")
	b, err := New(f.logs)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EnableDurable(DurableConfig{State: f.logs, Executable: "/bin/true"}); err == nil {
		t.Fatal("durable state accepted the log directory")
	}
	if err := b.EnableDurable(DurableConfig{State: f.state, Executable: "relative"}); err == nil {
		t.Fatal("accepted a relative supervisor executable")
	}
}
