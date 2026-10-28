package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

const (
	gpuA = "GPU-00000000-0000-0000-0000-00000000000a"
	gpuB = "GPU-00000000-0000-0000-0000-00000000000b"
	gpuC = "GPU-00000000-0000-0000-0000-00000000000c"
)

// TestResourceProgram is a fake GPU inventory tool for queryGPUs.
func TestResourceProgram(t *testing.T) {
	switch os.Getenv("SKYGO_RESOURCE_PROGRAM") {
	case "":
		return
	case "inventory":
		fmt.Printf("%s, NVIDIA GB10\n%s, NVIDIA GB10\n", gpuB, gpuA)
	case "fail":
		fmt.Fprint(os.Stderr, "NVIDIA-SMI has failed")
		os.Exit(9)
	case "sleep":
		time.Sleep(time.Minute)
	case "flood":
		fmt.Print(strings.Repeat("x", gpuQueryMaxOutput+1))
	}
	os.Exit(0)
}

func fakeGPUQuery(t *testing.T, mode string) []string {
	t.Setenv("SKYGO_RESOURCE_PROGRAM", mode)
	return []string{os.Args[0], "-test.run=^TestResourceProgram$"}
}

func fakeHost(gpus ...hostGPU) hostProbe {
	return hostProbe{
		cpus:   func() int { return 4 },
		memory: func() (int64, error) { return 8 << 30, nil },
		gpus:   func(context.Context) ([]hostGPU, error) { return gpus, nil },
	}
}

func gb10(uuid string) hostGPU { return hostGPU{uuid: uuid, name: "NVIDIA GB10"} }

func newResourceBackend(t *testing.T, probe hostProbe) *Backend {
	b := newBackend(t)
	b.resources.probe = probe
	return b
}

func resourceID(i int) string { return fmt.Sprintf("%032x", 0xa000+i) }

func shellTask(script string) backend.Task {
	return backend.Task{Argv: []string{"/bin/sh", "-c", script}, Env: map[string]string{}}
}

func reservedGPUs(b *Backend) map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	owners := make(map[string]string)
	for id, r := range b.resources.reserved {
		for _, g := range r.gpus {
			owners[g.uuid] = id
		}
	}
	return owners
}

func reservationCount(b *Backend) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.resources.reserved)
}

func jobOutput(t *testing.T, b *Backend, id string) string {
	t.Helper()
	chunk, err := b.Logs(context.Background(), backend.Reference{ID: id}, 0, backend.MaxChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	return string(chunk.Data)
}

func TestParseGPUs(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []hostGPU
		ok    bool
	}{
		{"sorted by uuid", gpuB + ", NVIDIA GB10\n" + gpuA + ", NVIDIA GB10\n", []hostGPU{gb10(gpuA), gb10(gpuB)}, true},
		{"crlf and blank lines", "\r\n" + gpuA + ",NVIDIA GB10\r\n\n", []hostGPU{gb10(gpuA)}, true},
		{"no devices", "", nil, true},
		{"missing prefix", strings.ToUpper(gpuA[4:]) + ", NVIDIA GB10\n", nil, false},
		{"uppercase uuid", "GPU-" + strings.ToUpper(gpuA[4:]) + ", NVIDIA GB10\n", []hostGPU{gb10(gpuA)}, true},
		{"mixed case duplicate", gpuA + ", NVIDIA GB10\n" + "GPU-" + strings.ToUpper(gpuA[4:]) + ", NVIDIA GB10\n", nil, false},
		{"missing name", gpuA + "\n", nil, false},
		{"empty name", gpuA + ", \n", nil, false},
		{"invalid uuid", "GPU-1, NVIDIA GB10\n", nil, false},
		{"index instead of uuid", "0, NVIDIA GB10\n", nil, false},
		{"duplicate uuid", gpuA + ", NVIDIA GB10\n" + gpuA + ", NVIDIA GB10\n", nil, false},
		{"control character", gpuA + ", NVIDIA\x01GB10\n", nil, false},
		{"extra field", gpuA + ", NVIDIA GB10, 1\n", nil, false},
		{"not utf8", gpuA + ", \xff\n", nil, false},
		{"diagnostic text", "No devices were found\n", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseGPUs([]byte(tt.input))
			if (err == nil) != tt.ok {
				t.Fatalf("parseGPUs error = %v, want ok %v", err, tt.ok)
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("parseGPUs = %v, want %v", got, tt.want)
			}
		})
	}
	var many strings.Builder
	for i := range maxHostGPUs + 1 {
		fmt.Fprintf(&many, "GPU-00000000-0000-0000-0000-%012x, NVIDIA GB10\n", i)
	}
	if _, err := parseGPUs([]byte(many.String())); err == nil {
		t.Fatal("unbounded inventory accepted")
	}
}

func TestQueryGPUs(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name string
		mode string
		ctx  context.Context
		argv []string
		want []hostGPU
		is   error
	}{
		{name: "inventory", mode: "inventory", want: []hostGPU{gb10(gpuA), gb10(gpuB)}},
		{name: "tool fails", mode: "fail"},
		{name: "output bound", mode: "flood"},
		{name: "canceled", mode: "sleep", ctx: canceled, is: context.Canceled},
		{name: "missing tool", argv: []string{"skygo-missing-nvidia-smi"}, is: exec.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			argv := tt.argv
			if argv == nil {
				argv = fakeGPUQuery(t, tt.mode)
			}
			ctx := tt.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			got, err := queryGPUs(ctx, argv)
			if tt.want != nil {
				if err != nil || fmt.Sprint(got) != fmt.Sprint(tt.want) {
					t.Fatalf("queryGPUs = %v, %v; want %v", got, err, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("queryGPUs = %v, want error", got)
			}
			if tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("queryGPUs error = %v, want %v", err, tt.is)
			}
		})
	}
}

func TestQueryGPUsDeadline(t *testing.T) {
	argv := fakeGPUQuery(t, "sleep")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := queryGPUs(ctx, argv)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("queryGPUs = %v after %v", err, time.Since(start))
	}
}

func TestResourceCheck(t *testing.T) {
	host := fakeHost(gb10(gpuB), gb10(gpuA), hostGPU{uuid: gpuC, name: "NVIDIA A100-SXM4-80GB"})
	tests := []struct {
		name string
		edit func(*backend.Task)
		is   error
	}{
		{"no request", func(*backend.Task) {}, nil},
		{"gpu", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "GB10", 1 }, nil},
		{"gpu full name", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "NVIDIA GB10", 2 }, nil},
		{"gpu case", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "gb10", 2 }, nil},
		{"gpu excess count", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "GB10", 3 }, ErrUnsatisfiable},
		{"gpu absent", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "H100", 1 }, ErrUnsatisfiable},
		{"gpu prefix only", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "A100", 1 }, ErrUnsatisfiable},
		{"gpu count too large", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "GB10", maxHostGPUs+1 }, ErrUnsatisfiable},
		{"gpu name without count", func(t *backend.Task) { t.Accelerator = "GB10" }, ErrInvalidResources},
		{"gpu count without name", func(t *backend.Task) { t.AcceleratorCount = 1 }, ErrInvalidResources},
		{"negative gpu count", func(t *backend.Task) { t.Accelerator, t.AcceleratorCount = "GB10", -1 }, ErrInvalidResources},
		{"negative cpu", func(t *backend.Task) { t.CPUMillis = -1 }, ErrInvalidResources},
		{"negative memory", func(t *backend.Task) { t.MemoryBytes = -1 }, ErrInvalidResources},
		{"cpu capacity", func(t *backend.Task) { t.CPUMillis = 4000 }, nil},
		{"cpu excess", func(t *backend.Task) { t.CPUMillis = 4001 }, ErrUnsatisfiable},
		{"memory capacity", func(t *backend.Task) { t.MemoryBytes = 8 << 30 }, nil},
		{"memory excess", func(t *backend.Task) { t.MemoryBytes = 8<<30 + 1 }, ErrUnsatisfiable},
		{"image", func(t *backend.Task) { t.Image = "x" }, ErrUnsupported},
		{"cpu limit", func(t *backend.Task) { t.CPULimitMillis = 1 }, ErrUnsupported},
		{"memory limit", func(t *backend.Task) { t.MemoryLimitBytes = 1 }, ErrUnsupported},
		{"node selector", func(t *backend.Task) { t.NodeSelector = map[string]string{"a": "b"} }, ErrUnsupported},
	}
	b := newResourceBackend(t, host)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := shellTask("exit 0")
			tt.edit(&task)
			err := b.Check(task)
			if tt.is == nil && err != nil || tt.is != nil && !errors.Is(err, tt.is) {
				t.Fatalf("Check = %v, want %v", err, tt.is)
			}
		})
	}
}

func TestResourceCheckProbesOnlyRequested(t *testing.T) {
	var gpuCalls, memoryCalls atomic.Int32
	probe := hostProbe{
		cpus:   func() int { return 1 },
		memory: func() (int64, error) { memoryCalls.Add(1); return 0, errors.New("no memory facts") },
		gpus: func(context.Context) ([]hostGPU, error) {
			gpuCalls.Add(1)
			return nil, fmt.Errorf("nvidia-smi: %w", exec.ErrNotFound)
		},
	}
	b := newResourceBackend(t, probe)
	if err := b.Check(shellTask("exit 0")); err != nil || gpuCalls.Load() != 0 || memoryCalls.Load() != 0 {
		t.Fatalf("Check = %v, probes gpu=%d memory=%d", err, gpuCalls.Load(), memoryCalls.Load())
	}
	task := shellTask("exit 0")
	task.Accelerator, task.AcceleratorCount = "GB10", 1
	if err := b.Check(task); !errors.Is(err, ErrInventory) || !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("Check with missing tool = %v", err)
	}
	if err := b.Start(context.Background(), resourceID(1), task); !errors.Is(err, ErrInventory) {
		t.Fatalf("Start with missing tool = %v", err)
	}
	task = shellTask("exit 0")
	task.MemoryBytes = 1
	if err := b.Check(task); !errors.Is(err, ErrInventory) {
		t.Fatalf("Check with failed memory probe = %v", err)
	}
	if s, err := b.Observe(context.Background(), backend.Reference{ID: resourceID(1)}); err != nil || s.State != backend.Unknown {
		t.Fatalf("refused start installed execution: %+v %v", s, err)
	}
}

func TestResourceStartCanceledProbe(t *testing.T) {
	probe := fakeHost()
	probe.gpus = func(ctx context.Context) ([]hostGPU, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	b := newResourceBackend(t, probe)
	task := shellTask("exit 0")
	task.Accelerator, task.AcceleratorCount = "GB10", 1
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := b.Start(ctx, resourceID(1), task); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrInventory) {
		t.Fatalf("Start = %v", err)
	}
	if reservationCount(b) != 0 {
		t.Fatal("canceled probe reserved resources")
	}
}

func TestResourceSelection(t *testing.T) {
	inv := hostInventory{cpuMillis: 4000, memoryBytes: 8 << 30, gpus: []hostGPU{gb10(gpuA), gb10(gpuB), {uuid: gpuC, name: "NVIDIA H100"}}}
	tests := []struct {
		name  string
		held  []string
		count int32
		want  []string
		is    error
	}{
		{"first by uuid", nil, 1, []string{gpuA}, nil},
		{"two", nil, 2, []string{gpuA, gpuB}, nil},
		{"skips held", []string{gpuA}, 1, []string{gpuB}, nil},
		{"other model held", []string{gpuC}, 2, []string{gpuA, gpuB}, nil},
		{"all held", []string{gpuA, gpuB}, 1, nil, backend.ErrResourcesUnavailable},
		{"partly held", []string{gpuB}, 2, nil, backend.ErrResourcesUnavailable},
		{"excess", nil, 3, nil, ErrUnsatisfiable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			held := make(map[string]*resourceReservation)
			for i, uuid := range tt.held {
				held[resourceID(i)] = &resourceReservation{id: resourceID(i), gpus: []hostGPU{{uuid: uuid}}}
			}
			task := backend.Task{Accelerator: "GB10", AcceleratorCount: tt.count}
			r, err := admit(task, inv, held)
			if !errors.Is(err, tt.is) || tt.is == nil && err != nil {
				t.Fatalf("admit error = %v, want %v", err, tt.is)
			}
			var got []string
			if r != nil {
				for _, g := range r.gpus {
					got = append(got, g.uuid)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("admit selected %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResourceCPUMemoryAdmission(t *testing.T) {
	probe := fakeHost()
	probe.cpus = func() int { return 2 }
	probe.memory = func() (int64, error) { return 1 << 30, nil }
	b := newResourceBackend(t, probe)
	steps := []struct {
		cpu, memory int64
		is          error
	}{
		{1500, 0, nil},
		{1000, 0, backend.ErrResourcesUnavailable},
		{500, 1 << 30, nil},
		{1, 0, backend.ErrResourcesUnavailable},
		{0, 1, backend.ErrResourcesUnavailable},
		{0, 0, nil},
	}
	var ids []string
	for i, step := range steps {
		task := shellTask("sleep 30")
		task.CPUMillis, task.MemoryBytes = step.cpu, step.memory
		if err := b.Check(task); err != nil {
			t.Fatalf("step %d: Check = %v; reservations must not affect feasibility", i, err)
		}
		id := resourceID(i)
		err := b.Start(context.Background(), id, task)
		if !errors.Is(err, step.is) || step.is == nil && err != nil {
			t.Fatalf("step %d: Start = %v, want %v", i, err, step.is)
		}
		if err == nil {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
			t.Fatal(err)
		}
		completed(t, b, id)
	}
	if n := reservationCount(b); n != 0 {
		t.Fatalf("%d reservations after positive closure", n)
	}
	// A refused start created no output, so the same ID may start later.
	task := shellTask("exit 0")
	task.CPUMillis = 1000
	if err := b.Start(context.Background(), resourceID(1), task); err != nil {
		t.Fatal(err)
	}
	completed(t, b, resourceID(1))
}

func TestResourceVisibleDevices(t *testing.T) {
	t.Setenv("CUDA_VISIBLE_DEVICES", "ambient")
	t.Setenv("NVIDIA_VISIBLE_DEVICES", "ambient")
	b := newResourceBackend(t, fakeHost(gb10(gpuB), gb10(gpuA), hostGPU{uuid: gpuC, name: "NVIDIA GB10"}))
	tests := []struct {
		name  string
		count int32
		env   map[string]string
		want  string
	}{
		{"one", 1, nil, gpuA + "|" + gpuA},
		{"two overrides task", 2, map[string]string{"CUDA_VISIBLE_DEVICES": "task", "NVIDIA_VISIBLE_DEVICES": "all"}, gpuB + "," + gpuC + "|" + gpuB + "," + gpuC},
		{"none hides devices", 0, map[string]string{"CUDA_VISIBLE_DEVICES": "0"}, "|void"},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := shellTask(`printf '%s|%s' "$CUDA_VISIBLE_DEVICES" "$NVIDIA_VISIBLE_DEVICES"; sleep 30`)
			if tt.count != 0 {
				task.Accelerator, task.AcceleratorCount = "GB10", tt.count
			}
			for k, v := range tt.env {
				task.Env[k] = v
			}
			id := resourceID(i)
			if err := b.Start(context.Background(), id, task); err != nil {
				t.Fatal(err)
			}
			// The earlier executions keep running, so later selections skip their GPUs.
			deadline := time.Now().Add(5 * time.Second)
			for !strings.Contains(jobOutput(t, b, id), "|") {
				if time.Now().After(deadline) {
					t.Fatal("no output")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if got := jobOutput(t, b, id); got != tt.want {
				t.Fatalf("child saw %q, want %q", got, tt.want)
			}
			if task.Env["CUDA_VISIBLE_DEVICES"] != tt.env["CUDA_VISIBLE_DEVICES"] {
				t.Fatal("Start mutated the task environment")
			}
		})
	}
	for i := range tests {
		b.Cancel(context.Background(), backend.Reference{ID: resourceID(i)})
		completed(t, b, resourceID(i))
	}
	if owners := reservedGPUs(b); len(owners) != 0 {
		t.Fatalf("GPUs held after closure: %v", owners)
	}
}

func TestResourceConcurrentStarts(t *testing.T) {
	b := newResourceBackend(t, fakeHost(gb10(gpuA), gb10(gpuB)))
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := shellTask("sleep 30")
			task.Accelerator, task.AcceleratorCount = "GB10", 1
			errs[i] = b.Start(context.Background(), resourceID(i), task)
		}()
	}
	wg.Wait()
	var started []string
	for i, err := range errs {
		switch {
		case err == nil:
			started = append(started, resourceID(i))
		case !errors.Is(err, backend.ErrResourcesUnavailable):
			t.Fatalf("start %d: %v", i, err)
		}
	}
	owners := reservedGPUs(b)
	if len(started) != 2 || len(owners) != 2 || owners[gpuA] == owners[gpuB] {
		t.Fatalf("started %v, owners %v", started, owners)
	}
	for _, id := range started {
		b.Cancel(context.Background(), backend.Reference{ID: id})
		completed(t, b, id)
	}
	if owners := reservedGPUs(b); len(owners) != 0 {
		t.Fatalf("GPUs held after closure: %v", owners)
	}
}

func TestResourceHeldUntilPositiveClosure(t *testing.T) {
	b := newResourceBackend(t, fakeHost(gb10(gpuA)))
	var deliver atomic.Bool
	ops := b.defaultOps
	b.defaultOps.kill = func(cmd *exec.Cmd) error {
		if !deliver.Load() {
			return nil // reports delivery but leaves the group running
		}
		return ops.kill(cmd)
	}
	task := shellTask("sleep 30")
	task.Accelerator, task.AcceleratorCount = "GB10", 1
	id := resourceID(1)
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(context.Background(), backend.Reference{ID: id}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if owners := reservedGPUs(b); owners[gpuA] != id {
		t.Fatalf("cancellation released GPU while original ran: %v", owners)
	}
	if err := b.Start(context.Background(), resourceID(2), task); !errors.Is(err, backend.ErrResourcesUnavailable) {
		t.Fatalf("competing start = %v", err)
	}
	deliver.Store(true)
	b.mu.Lock()
	pid := b.jobs[id].cmd.Process.Pid
	b.mu.Unlock()
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if s := completed(t, b, id); s.State != backend.Canceled {
		t.Fatalf("status %+v", s)
	}
	if owners := reservedGPUs(b); len(owners) != 0 {
		t.Fatalf("GPU held after positive closure: %v", owners)
	}
	task.Argv = []string{"/bin/sh", "-c", "exit 0"}
	if err := b.Start(context.Background(), resourceID(2), task); err != nil {
		t.Fatal(err)
	}
	completed(t, b, resourceID(2))
}

// MODELED: the injected closeOutput closes the actual file and then reports
// uncertainty; no real uncertain native Close is produced.
func TestResourceHeldWhenClosureUncertain(t *testing.T) {
	b, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b.resources.probe = fakeHost(gb10(gpuA))
	b.defaultOps.closeOutput = func(o *output) error {
		o.file.Close()
		return errors.New("close uncertain")
	}
	task := shellTask("exit 0")
	task.Accelerator, task.AcceleratorCount = "GB10", 1
	id := resourceID(1)
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	completed(t, b, id)
	if owners := reservedGPUs(b); owners[gpuA] != id {
		t.Fatalf("uncertain closure released GPU: %v", owners)
	}
	if err := b.Close(); err == nil {
		t.Fatal("Close reported uncertain ownership as closed")
	}
}

func TestResourceStartFailure(t *testing.T) {
	tests := []struct {
		name     string
		argv     []string
		start    func(*exec.Cmd) error
		released bool
	}{
		{name: "preflight", argv: []string{"skygo-missing-program"}, released: true},
		{name: "after preflight", argv: []string{"/bin/sh", "-c", "exit 0"}, start: func(*exec.Cmd) error { return errors.New("start failed") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			b.resources.probe = fakeHost(gb10(gpuA))
			if tt.start != nil {
				b.defaultOps.start = tt.start
			}
			task := backend.Task{Argv: tt.argv, Accelerator: "GB10", AcceleratorCount: 1}
			if err := b.Start(context.Background(), resourceID(1), task); err == nil {
				t.Fatal("Start succeeded")
			}
			if released := len(reservedGPUs(b)) == 0; released != tt.released {
				t.Fatalf("released = %v, want %v", released, tt.released)
			}
		})
	}
}

func TestResourceRetryVisibleDevices(t *testing.T) {
	b := newResourceBackend(t, fakeHost(gb10(gpuA)))
	if !b.SupportsRetry() {
		t.Skip("retry execution requires Linux pidfds")
	}
	t.Setenv("CUDA_VISIBLE_DEVICES", "ambient")
	task := shellTask(`printf '%s|%s' "$CUDA_VISIBLE_DEVICES" "$NVIDIA_VISIBLE_DEVICES"`)
	task.Accelerator, task.AcceleratorCount = "GB10", 1
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
	id := resourceID(1)
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	if s := completed(t, b, id); s.State != backend.Succeeded {
		t.Fatalf("status %+v", s)
	}
	if got, want := jobOutput(t, b, id), gpuA+"|"+gpuA; got != want {
		t.Fatalf("child saw %q, want %q", got, want)
	}
	if owners := reservedGPUs(b); len(owners) != 0 {
		t.Fatalf("GPU held after positive closure: %v", owners)
	}
}

func TestAcceleratorNames(t *testing.T) {
	var many []hostGPU
	for i := range maxAcceleratorNames + 1 {
		many = append(many, hostGPU{uuid: fmt.Sprintf("GPU-00000000-0000-0000-0000-%012x", i), name: fmt.Sprintf("NVIDIA M%d", i)})
	}
	tests := []struct {
		name string
		gpus []hostGPU
		err  error
		want []string
		is   error
	}{
		{name: "no gpus"},
		{name: "prefix removed", gpus: []hostGPU{gb10(gpuA)}, want: []string{"GB10"}},
		{
			name: "sorted distinct",
			gpus: []hostGPU{
				{uuid: gpuC, name: "NVIDIA H100 80GB HBM3"}, gb10(gpuB), gb10(gpuA),
				{uuid: "GPU-00000000-0000-0000-0000-00000000000d", name: "Tesla T4"},
			},
			want: []string{"GB10", "H100 80GB HBM3", "Tesla T4"},
		},
		{name: "too many names", gpus: many, is: ErrInventory},
		{name: "query unavailable", err: fmt.Errorf("nvidia-smi: %w", exec.ErrNotFound), is: exec.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newResourceBackend(t, hostProbe{gpus: func(context.Context) ([]hostGPU, error) { return tt.gpus, tt.err }})
			got, err := b.AcceleratorNames(context.Background())
			if tt.is != nil {
				if !errors.Is(err, tt.is) || !errors.Is(err, ErrInventory) || got != nil {
					t.Fatalf("AcceleratorNames = %q, %v; want %v", got, err, tt.is)
				}
				return
			}
			if err != nil || fmt.Sprint(got) != fmt.Sprint(tt.want) {
				t.Fatalf("AcceleratorNames = %q, %v; want %q", got, err, tt.want)
			}
			for _, name := range got {
				task := shellTask("exit 0")
				task.Accelerator, task.AcceleratorCount = name, 1
				if err := b.Check(task); err != nil {
					t.Fatalf("advertised %q fails Check: %v", name, err)
				}
			}
		})
	}
}

func TestAcceleratorNamesBoundedQuery(t *testing.T) {
	var deadline time.Time
	b := newResourceBackend(t, hostProbe{gpus: func(ctx context.Context) ([]hostGPU, error) {
		var ok bool
		if deadline, ok = ctx.Deadline(); !ok {
			return nil, errors.New("query has no deadline")
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.AcceleratorNames(ctx); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrInventory) {
		t.Fatalf("canceled AcceleratorNames = %v", err)
	}
	if d := time.Until(deadline); d <= 0 || d > gpuQueryTimeout {
		t.Fatalf("query deadline in %v, want within %v", d, gpuQueryTimeout)
	}
}

func TestAcceleratorNamesFromTool(t *testing.T) {
	argv := fakeGPUQuery(t, "inventory")
	b := newResourceBackend(t, hostProbe{gpus: func(ctx context.Context) ([]hostGPU, error) { return queryGPUs(ctx, argv) }})
	got, err := b.AcceleratorNames(context.Background())
	if err != nil || fmt.Sprint(got) != "[GB10]" {
		t.Fatalf("AcceleratorNames = %q, %v", got, err)
	}
	b.resources.probe.gpus = func(ctx context.Context) ([]hostGPU, error) { return queryGPUs(ctx, fakeGPUQuery(t, "fail")) }
	if _, err := b.AcceleratorNames(context.Background()); !errors.Is(err, ErrInventory) {
		t.Fatalf("failed tool AcceleratorNames = %v", err)
	}
}
