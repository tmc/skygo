//go:build linux || darwin

package local

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func durableResourceTask(cpu, memory int64, count int32) backend.Task {
	t := backend.Task{Argv: []string{"true"}, CPUMillis: cpu, MemoryBytes: memory}
	if count != 0 {
		t.Accelerator, t.AcceleratorCount = "GB10", count
	}
	return t
}

func recordedResourceJob(t backend.Task, gpus ...string) *durableJob {
	return &durableJob{valid: true, task: backend.CloneTask(t), launch: durableLaunch{
		CPUMillis: t.CPUMillis, MemoryBytes: t.MemoryBytes, GPUs: append([]string(nil), gpus...),
		Env: durableResourceEnvironment(gpus),
	}}
}

func TestDurableResourceSelection(t *testing.T) {
	b := &Backend{durable: &durableState{jobs: map[string]*durableJob{
		resourceID(1): recordedResourceJob(durableResourceTask(1000, 100, 1), gpuA),
	}}}
	b.resources.reserved = map[string]*resourceReservation{
		resourceID(2): {id: resourceID(2), cpuMillis: 1000, memoryBytes: 100, gpus: []hostGPU{gb10(gpuB)}},
	}
	// The inventory was obtained before the lock. A probe here would hang or
	// fail, so the test also guards against reselecting under the mutex.
	b.resources.probe.gpus = func(context.Context) ([]hostGPU, error) {
		t.Fatal("durable admission re-probed the host")
		return nil, nil
	}
	inv := hostInventory{cpuMillis: 4000, memoryBytes: 1000, gpus: []hostGPU{gb10(gpuA), gb10(gpuB), gb10(gpuC)}}
	task := durableResourceTask(1000, 100, 1)
	task.Env = map[string]string{"CUDA_VISIBLE_DEVICES": "task", "NVIDIA_VISIBLE_DEVICES": "all"}
	original := backend.CloneTask(task)
	b.mu.Lock()
	launch, err := b.durableLaunchFor(resourceID(3), task, inv)
	b.mu.Unlock()
	if err != nil || len(launch.GPUs) != 1 || launch.GPUs[0] != gpuC {
		t.Fatalf("selected %+v, %v; want %s", launch, err, gpuC)
	}
	if !reflect.DeepEqual(task, original) || launch.Env["CUDA_VISIBLE_DEVICES"] != gpuC ||
		launch.Env["NVIDIA_VISIBLE_DEVICES"] != gpuC {
		t.Fatal("selection changed accepted task or omitted fixed visibility")
	}
	if len(b.resources.reserved) != 1 {
		t.Fatal("durable selection entered the ordinary reservation map")
	}
	// Publishing under the same lock accounts the selection before the next
	// admission, without a second ordinary-map charge.
	b.durable.jobs[resourceID(3)] = recordedResourceJob(task, launch.GPUs...)
	b.mu.Lock()
	_, err = b.durableLaunchFor(resourceID(4), task, inv)
	b.mu.Unlock()
	if !errors.Is(err, backend.ErrResourcesUnavailable) {
		t.Fatalf("competing admission = %v", err)
	}
}

func TestDurableResourceRestoreAndTerminal(t *testing.T) {
	task := durableResourceTask(1000, 100, 1)
	job := recordedResourceJob(task, gpuA)
	b := &Backend{durable: &durableState{jobs: map[string]*durableJob{resourceID(1): job}}}
	legacy := &resourceReservation{id: resourceID(2), cpuMillis: 1000, memoryBytes: 100, gpus: []hostGPU{gb10(gpuB)}}
	b.resources.reserved = map[string]*resourceReservation{legacy.id: legacy}
	b.mu.Lock()
	held, err := b.allResourceReservationsLocked()
	b.mu.Unlock()
	if err != nil || len(held) != 2 || held[resourceID(1)].gpus[0].uuid != gpuA {
		t.Fatalf("restored = %+v, %v", held, err)
	}
	// This flag is set only by the durable owner's validated terminal/closed
	// result. It must not drop a concurrent ordinary, retry or workspace owner.
	job.terminal = true
	b.mu.Lock()
	held, err = b.allResourceReservationsLocked()
	b.mu.Unlock()
	if err != nil || len(held) != 1 || held[legacy.id] != legacy || b.resources.reserved[legacy.id] != legacy {
		t.Fatalf("terminal released an unrelated owner: %+v, %v", held, err)
	}
	if job.launch.GPUs[0] != gpuA {
		t.Fatal("restoration recomputed the persisted selection")
	}
}

func TestDurableResourceValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*backend.Task, *durableLaunch)
	}{
		{"cpu mismatch", func(_ *backend.Task, l *durableLaunch) { l.CPUMillis++ }},
		{"memory mismatch", func(_ *backend.Task, l *durableLaunch) { l.MemoryBytes++ }},
		{"gpu count", func(_ *backend.Task, l *durableLaunch) { l.GPUs = nil }},
		{"index instead of uuid", func(_ *backend.Task, l *durableLaunch) { l.GPUs[0] = "0" }},
		{"uuid case alias", func(_ *backend.Task, l *durableLaunch) { l.GPUs[0] = strings.ToUpper(l.GPUs[0]) }},
		{"negative request", func(t *backend.Task, l *durableLaunch) { t.CPUMillis, l.CPUMillis = -1, -1 }},
		{"missing env", func(_ *backend.Task, l *durableLaunch) { l.Env = nil }},
		{"missing cuda key", func(_ *backend.Task, l *durableLaunch) { delete(l.Env, "CUDA_VISIBLE_DEVICES"); l.Env["OTHER"] = "x" }},
		{"wrong cuda", func(_ *backend.Task, l *durableLaunch) { l.Env["CUDA_VISIBLE_DEVICES"] = "all" }},
		{"wrong nvidia", func(_ *backend.Task, l *durableLaunch) { l.Env["NVIDIA_VISIBLE_DEVICES"] = "all" }},
		{"extra private env", func(_ *backend.Task, l *durableLaunch) { l.Env["PATH"] = "/evil" }},
		{"duplicate uuid", func(t *backend.Task, l *durableLaunch) {
			t.AcceleratorCount = 2
			l.GPUs = []string{gpuA, gpuA}
			l.Env = durableResourceEnvironment(l.GPUs)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := durableResourceTask(1000, 100, 1)
			launch := recordedResourceJob(task, gpuA).launch
			tt.edit(&task, &launch)
			if err := validateDurableResourceLaunch(task, launch); !errors.Is(err, ErrInventory) {
				t.Fatalf("validator = %v, want ErrInventory", err)
			}
		})
	}
	if err := validateDurableResourceLaunch(durableResourceTask(0, 0, 0), durableLaunch{Env: durableResourceEnvironment(nil)}); err != nil {
		t.Fatalf("CPU-only visibility = %v", err)
	}
}

func TestDurableResourceUnknownBlocksAdmission(t *testing.T) {
	for _, valid := range []bool{false, true} {
		b := &Backend{durable: &durableState{jobs: map[string]*durableJob{
			resourceID(1): {valid: valid, task: durableResourceTask(0, 0, 1), launch: durableLaunch{}},
		}}}
		b.mu.Lock()
		_, err := b.reserveResources(resourceID(2), durableResourceTask(0, 0, 0), hostInventory{})
		b.mu.Unlock()
		if !errors.Is(err, ErrInventory) {
			t.Fatalf("unknown allocation valid=%v admitted a job: %v", valid, err)
		}
	}
}

// These are real private durable records but MODELED outcomes: no workload or
// supervisor is launched. The durable reader, not a hand-set terminal flag,
// decides whether the restored allocation remains held.
func TestDurableResourceStatusKeepsAllocation(t *testing.T) {
	cases := []struct {
		name        string
		state       backend.State
		groupClosed bool
		foreign     bool
		wantHeld    bool
	}{
		{"validated completion", backend.Succeeded, true, false, false},
		{"missing group closure", backend.Succeeded, false, false, true},
		{"foreign instance", backend.Succeeded, true, true, true},
		{"unknown even with closure", backend.Unknown, true, false, true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			f := newDurableFixture(t, "0")
			f.task = durableResourceTask(1000, 100, 1)
			in := f.writeIntent()
			in.Launch = recordedResourceJob(f.task, gpuA).launch
			f.write(durableIntentFile, in)
			instance := in.Instance
			if tt.foreign {
				instance = "local-" + strings.Repeat("b", 32)
			}
			zero := int32(0)
			f.write(durableStatusFile, durableStatus{Version: durableVersion,
				Instance: instance, State: tt.state, ExitCode: &zero,
				OutputComplete: true, GroupClosed: tt.groupClosed})
			b := f.backend()
			defer b.Close()
			b.mu.Lock()
			held, err := b.allResourceReservationsLocked()
			b.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			r := held[f.id]
			if (r != nil) != tt.wantHeld || r != nil && (len(r.gpus) != 1 || r.gpus[0].uuid != gpuA) {
				t.Fatalf("restored allocation = %+v, want held %v", r, tt.wantHeld)
			}
			if _, err := b.Observe(context.Background(), f.ref("")); err != nil {
				t.Fatal(err)
			}
			b.mu.Lock()
			held, err = b.allResourceReservationsLocked()
			b.mu.Unlock()
			if err != nil || (held[f.id] != nil) != tt.wantHeld {
				t.Fatalf("observation changed allocation authority: %+v, %v", held, err)
			}
		})
	}
}

func TestDurableResourceAccountingRefusesCorruption(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		b := &Backend{durable: &durableState{jobs: make(map[string]*durableJob)}}
		first := &resourceReservation{id: resourceID(1), cpuMillis: 1<<63 - 1}
		second := &resourceReservation{id: resourceID(2), cpuMillis: 1}
		if duplicate {
			first.cpuMillis, second.cpuMillis = 0, 0
			first.gpus, second.gpus = []hostGPU{gb10(gpuA)}, []hostGPU{gb10(gpuA)}
		}
		b.resources.reserved = map[string]*resourceReservation{first.id: first, second.id: second}
		b.mu.Lock()
		_, err := b.allResourceReservationsLocked()
		b.mu.Unlock()
		if !errors.Is(err, ErrInventory) {
			t.Fatalf("corrupt accounting duplicate=%v = %v", duplicate, err)
		}
	}
}
