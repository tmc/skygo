package tasklower

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeEnv(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want map[string]string
	}{
		{"no resources", "run: x\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"null resources", "run: x\nresources: null\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"cpu only", "run: x\nresources: {cpus: 2}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"null envs", "run: x\nenvs: null\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"empty envs", "run: x\nenvs: {}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"num_nodes 1", "run: x\nnum_nodes: 1\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"GB10", "run: x\nresources: {accelerators: 'GB10:1'}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "1"}},
		{"L4", "run: x\nresources: {accelerators: 'L4:2'}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "2"}},
		{"H100", "run: x\nresources: {accelerators: {H100: 8}}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "8"}},
		{"generic max", "run: x\nresources: {accelerators: {A: 2147483647}}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "2147483647"}},
		{"user num nodes", "run: x\nenvs: {SKYPILOT_NUM_NODES: '7'}\n", map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"user empty num nodes", "run: x\nenvs: {SKYPILOT_NUM_NODES: ''}\n", map[string]string{"SKYPILOT_NUM_NODES": "", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"user rank and gpus", "run: x\nresources: {accelerators: 'L4:2'}\nenvs: {SKYPILOT_NODE_RANK: '3', SKYPILOT_NUM_GPUS_PER_NODE: '64'}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "2"}},
		{"user empty rank and gpus", "run: x\nenvs: {SKYPILOT_NODE_RANK: '', SKYPILOT_NUM_GPUS_PER_NODE: ''}\n", map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"unrelated envs", "run: x\nenvs: {MODE: ci, SKYPILOT_TASK_ID: t, EMPTY: ''}\n", map[string]string{"MODE": "ci", "SKYPILOT_TASK_ID": "t", "EMPTY": "", "SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
		{"envs before resources", "envs: {SKYPILOT_NUM_NODES: '7', SKYPILOT_NUM_GPUS_PER_NODE: '1'}\nresources: {accelerators: {H100: 8}}\nrun: x\n", map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "8"}},
		{"resources before envs", "run: x\nresources: {accelerators: {H100: 8}}\nenvs: {SKYPILOT_NUM_NODES: '7', SKYPILOT_NUM_GPUS_PER_NODE: '1'}\n", map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "8"}},
		{"num_nodes 1 with user value", "num_nodes: 1\nenvs: {SKYPILOT_NUM_NODES: '7'}\nrun: x\n", map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := lowerText(t, tt.yaml, Options{Shell: "/bin/sh"})
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			if !maps.Equal(got.Env, tt.want) {
				t.Fatalf("Env = %v, want %v", got.Env, tt.want)
			}
		})
	}
}

// TestRuntimeEnvAccelerators checks the upstream request classification:
// a name containing tpu, inferentia or trainium in any case requests no
// GPUs, and every other accepted name requests its count.
func TestRuntimeEnvAccelerators(t *testing.T) {
	tests := []struct {
		accel string
		text  string
		count int32
		gpus  string
	}{
		{"GB10", "1", 1, "1"},
		{"L4", "2", 2, "2"},
		{"H100", "8", 8, "8"},
		{"A", "2147483647", 2147483647, "2147483647"},
		{"ZX9000", "6", 6, "6"},
		{"future-gpu-2030", "3", 3, "3"},
		{"tpu-v5p", "4", 4, "0"},
		{"Tpu-V5P", "8", 8, "0"},
		{"TPU", "1", 1, "0"},
		{"v5ptpuslice", "4", 4, "0"},
		{"Inferentia", "2", 2, "0"},
		{"aws-INFERENTIA2-x", "1", 1, "0"},
		{"TrAiNiUm2", "16", 16, "0"},
		{"xtrainiumx", "1", 1, "0"},
		{"TP-U", "3", 3, "3"},
		{"Tpx", "2", 2, "2"},
		{"inferent", "5", 5, "5"},
		{"train", "2", 2, "2"},
		{"trainum", "7", 7, "7"},
	}
	for _, tt := range tests {
		t.Run(tt.accel, func(t *testing.T) {
			got, _, err := lowerText(t, "run: x\nresources:\n  accelerators: {"+tt.accel+": "+tt.text+"}\n", Options{Shell: "/bin/sh"})
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			want := &skyv1.Task{
				Argv:             []string{"/bin/sh", "-c", "x"},
				Env:              map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": tt.gpus},
				Accelerator:      tt.accel,
				AcceleratorCount: tt.count,
			}
			if !proto.Equal(got, want) {
				t.Fatalf("Lower =\n%v\nwant\n%v", got, want)
			}
		})
	}
}

// TestRuntimeEnvOverrides checks that taskyaml EnvOverrides replace source
// values and that the computed rank and GPU count still take precedence.
func TestRuntimeEnvOverrides(t *testing.T) {
	src := "run: x\nresources: {accelerators: 'L4:2'}\nenvs: {MODE: src, SKYPILOT_NUM_NODES: '5', SKYPILOT_NUM_GPUS_PER_NODE: '9'}\n"
	overrides := map[string]string{"MODE": "cli", "SKYPILOT_NUM_NODES": "4", "SKYPILOT_NODE_RANK": "6", "ADDED": "a"}
	d, err := taskyaml.Parse(strings.NewReader(src), taskyaml.Options{EnvOverrides: overrides})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, _, err := Lower(d, Options{Shell: "/bin/sh"})
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	want := map[string]string{"MODE": "cli", "ADDED": "a", "SKYPILOT_NUM_NODES": "4", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "2"}
	if !maps.Equal(got.Env, want) {
		t.Fatalf("Env = %v, want %v", got.Env, want)
	}
	if overrides["SKYPILOT_NODE_RANK"] != "6" || len(overrides) != 4 {
		t.Fatal("Lower changed the overrides")
	}
}

func TestRuntimeEnvWithoutLocations(t *testing.T) {
	run := "x"
	d := &taskyaml.Document{Task: taskyaml.Task{
		Run:       &run,
		Envs:      map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "2", "B": "c"},
		Resources: &taskyaml.Resources{Accelerators: []taskyaml.Accelerator{{Name: "H100", Count: taskyaml.Quantity{Value: "8"}}}},
		NumNodes:  1,
	}}
	got, notes, err := Lower(d, Options{Shell: "/bin/sh"})
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	want := &skyv1.Task{
		Argv:             []string{"/bin/sh", "-c", "x"},
		Env:              map[string]string{"B": "c", "SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "8"},
		Accelerator:      "H100",
		AcceleratorCount: 8,
	}
	if !proto.Equal(got, want) {
		t.Fatalf("Lower =\n%v\nwant\n%v", got, want)
	}
	if !maps.Equal(d.Task.Envs, map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "2", "B": "c"}) {
		t.Fatalf("Lower changed the document envs: %v", d.Task.Envs)
	}
	for _, n := range notes {
		if strings.HasPrefix(n.Path, "SKYPILOT_") {
			t.Fatalf("synthetic note %v", n)
		}
	}
}

// TestRuntimeEnvRefused checks that a refused task returns no task, notes
// or partial environment.
func TestRuntimeEnvRefused(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want taskyaml.Error
		text string
	}{
		{"two nodes", "run: x\nenvs: {SKYPILOT_NUM_NODES: '2'}\nnum_nodes: 2\n", taskyaml.Error{Code: taskyaml.Unsupported, Path: "num_nodes", Line: 3, Column: 12}, "task yaml: unsupported num_nodes at 3:12"},
		{"invalid env", "run: x\nresources: {accelerators: 'L4:2'}\nenvs: {A: \"\\0\"}\n", taskyaml.Error{Code: taskyaml.Invalid, Path: "envs", Line: 3, Column: 7}, "task yaml: invalid envs at 3:7"},
		{"unsupported after envs", "envs: {A: b}\nrun: x\nfile_mounts: {/a: b}\n", taskyaml.Error{Code: taskyaml.Unsupported, Path: "file_mounts", Line: 3, Column: 14}, "task yaml: unsupported file_mounts at 3:14"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task, notes, err := lowerText(t, tt.yaml, Options{Shell: "/bin/sh"})
			var e *taskyaml.Error
			if !errors.As(err, &e) || *e != tt.want || err.Error() != tt.text {
				t.Fatalf("Lower error = %v, want %q", err, tt.text)
			}
			if task != nil || notes != nil {
				t.Fatalf("Lower returned %v, %v with an error", task, notes)
			}
		})
	}
}

// TestRuntimeEnvIndependent checks that repeated lowering returns fresh
// tasks and maps and leaves the document unchanged.
func TestRuntimeEnvIndependent(t *testing.T) {
	d, err := taskyaml.Parse(strings.NewReader("run: x\nenvs: {MODE: ci, SKYPILOT_NUM_NODES: '7'}\nresources: {accelerators: 'L4:2'}\n"), taskyaml.Options{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	opts := Options{Shell: "/bin/sh"}
	a, notesA, err := Lower(d, opts)
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	a.Env["MODE"] = "changed"
	a.Env["SKYPILOT_NODE_RANK"] = "5"
	delete(a.Env, "SKYPILOT_NUM_GPUS_PER_NODE")
	b, notesB, err := Lower(d, opts)
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	want := map[string]string{"MODE": "ci", "SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "2"}
	if !maps.Equal(b.Env, want) {
		t.Fatalf("second Env = %v, want %v", b.Env, want)
	}
	if !maps.Equal(d.Task.Envs, map[string]string{"MODE": "ci", "SKYPILOT_NUM_NODES": "7"}) {
		t.Fatalf("Lower changed the document envs: %v", d.Task.Envs)
	}
	if !slices.Equal(notesA, notesB) {
		t.Fatalf("notes differ: %v, %v", notesA, notesB)
	}
	wantNotes := []Note{
		{"run", Lowered, "argv is shell -c text"},
		{"envs", Lowered, "environment"},
		{"resources", Default, "fields noted separately"},
		{"resources.accelerators", Lowered, "accelerator and accelerator_count"},
	}
	if !slices.Equal(notesB, wantNotes) {
		t.Fatalf("notes = %v, want %v", notesB, wantNotes)
	}
}

// TestRuntimeEnvSetup checks that setup and run share one environment and
// keep their text byte-exact.
func TestRuntimeEnvSetup(t *testing.T) {
	src := "workdir: .\nsetup: \"make  deps\\n\"\nrun: \"echo $SKYPILOT_NODE_RANK $SKYPILOT_NUM_NODES\\n\"\nenvs: {SKYPILOT_NUM_NODES: '7'}\nresources: {accelerators: {GB10: 1}}\n"
	got, _, err := lowerText(t, src, Options{Shell: "/bin/sh", Workdir: ref})
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	want := &skyv1.Task{
		Argv:             []string{"/bin/sh", "-c", "echo $SKYPILOT_NODE_RANK $SKYPILOT_NUM_NODES\n"},
		Env:              map[string]string{"SKYPILOT_NUM_NODES": "7", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "1"},
		Accelerator:      "GB10",
		AcceleratorCount: 1,
		Workdir:          &skyv1.ArtifactRef{Id: "00112233445566778899aabbccddeeff", Sha256: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", Size: 4096},
		Setup:            []string{"/bin/sh", "-c", "make  deps\n"},
	}
	if !proto.Equal(got, want) {
		t.Fatalf("Lower =\n%v\nwant\n%v", got, want)
	}
}

// TestRuntimeEnvIgnoresProcess checks that the process environment does
// not reach the task.
func TestRuntimeEnvIgnoresProcess(t *testing.T) {
	t.Setenv("SKYPILOT_NUM_NODES", "3")
	t.Setenv("SKYPILOT_NODE_RANK", "2")
	t.Setenv("SKYPILOT_NUM_GPUS_PER_NODE", "4")
	t.Setenv("CUDA_VISIBLE_DEVICES", "0,1")
	got, _, err := lowerText(t, "run: x\n", Options{Shell: "/bin/sh"})
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	want := map[string]string{"SKYPILOT_NUM_NODES": "1", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}
	if !maps.Equal(got.Env, want) {
		t.Fatalf("Env = %v, want %v", got.Env, want)
	}
}
