package local

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func TestResourceWorkspaceVisibleDevices(t *testing.T) {
	f := newWorkspaceFixture(t)
	f.backend.resources.probe = fakeHost(gb10(gpuA))
	t.Setenv("CUDA_VISIBLE_DEVICES", "ambient")
	f.binding.Task.Accelerator, f.binding.Task.AcceleratorCount = "GB10", 1
	f.binding.Task.Env["NVIDIA_VISIBLE_DEVICES"] = "task"
	var mu sync.Mutex
	var seen []string
	ops := f.backend.defaultOps
	f.backend.defaultOps.start = func(cmd *exec.Cmd) error {
		var cuda, nvidia string
		for _, kv := range cmd.Env {
			if v, ok := strings.CutPrefix(kv, "CUDA_VISIBLE_DEVICES="); ok {
				cuda = v
			}
			if v, ok := strings.CutPrefix(kv, "NVIDIA_VISIBLE_DEVICES="); ok {
				nvidia = v
			}
		}
		mu.Lock()
		seen = append(seen, cuda+"|"+nvidia)
		mu.Unlock()
		if owners := reservedGPUs(f.backend); owners[gpuA] != f.binding.AttemptID {
			return fmt.Errorf("phase started without reservation: %v", owners)
		}
		return ops.start(cmd)
	}
	o := awaitWorkspace(t, f, f.start(t))
	if !o.Ready || o.Writer == nil || o.Status.State != backend.Succeeded {
		t.Fatalf("observation %+v", o)
	}
	want := gpuA + "|" + gpuA
	if len(seen) != 2 || seen[0] != want || seen[1] != want {
		t.Fatalf("setup and task saw %q, want %q", seen, want)
	}
	if owners := reservedGPUs(f.backend); len(owners) != 0 {
		t.Fatalf("GPU held after Writer closure: %v", owners)
	}
}

func TestParseMeminfo(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  int64
		ok    bool
	}{
		{"valid", "MemFree:  1 kB\nMemTotal:       16384 kB\n", 16384 << 10, true},
		{"zero", "MemTotal: 0 kB\n", 0, false},
		{"negative", "MemTotal: -1 kB\n", 0, false},
		{"overflow", "MemTotal: 9223372036854775807 kB\n", 0, false},
		{"wrong unit", "MemTotal: 16 MB\n", 0, false},
		{"missing unit", "MemTotal: 16\n", 0, false},
		{"missing value", "MemTotal: kB\n", 0, false},
		{"missing field", "MemFree: 1 kB\n", 0, false},
		{"empty", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMeminfo(strings.NewReader(tt.input))
			if (err == nil) != tt.ok || got != tt.want {
				t.Fatalf("parseMeminfo = %d, %v; want %d ok %v", got, err, tt.want, tt.ok)
			}
		})
	}
}
