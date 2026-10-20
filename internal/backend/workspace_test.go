package backend_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func workspaceTask() backend.Task {
	return backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}}
}

func TestValidateWorkspace(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*backend.Task)
		ok     bool
	}{
		{"workdir only", func(*backend.Task) {}, true},
		{"setup and outputs", func(t *backend.Task) { t.Setup = []string{"prepare", ""}; t.Outputs = []string{"a", "ab/c"} }, true},
		{"setup 64 arguments", func(t *backend.Task) { t.Setup = make([]string, 64); t.Setup[0] = "run" }, true},
		{"16 outputs", func(t *backend.Task) {
			for i := 0; i < 16; i++ {
				t.Outputs = append(t.Outputs, string(rune('a'+i)))
			}
		}, true},
		{"max path", func(t *backend.Task) { t.Outputs = []string{strings.Repeat("a", 240)} }, true},
		{"empty optional slices", func(t *backend.Task) { t.Setup = []string{}; t.Outputs = []string{} }, true},
		{"directory", func(t *backend.Task) { t.Directory = "/work" }, false},
		{"bad ref", func(t *backend.Task) { t.Workdir.ID = "bad" }, false},
		{"setup without input", func(t *backend.Task) { t.Workdir = nil; t.Setup = []string{"prepare"} }, false},
		{"outputs without input", func(t *backend.Task) { t.Workdir = nil; t.Outputs = []string{"result"} }, false},
		{"setup program", func(t *backend.Task) { t.Setup = []string{""} }, false},
		{"setup nul", func(t *backend.Task) { t.Setup = []string{"a\x00b"} }, false},
		{"setup UTF8", func(t *backend.Task) { t.Setup = []string{string([]byte{255})} }, false},
		{"setup argc", func(t *backend.Task) { t.Setup = make([]string, 65); t.Setup[0] = "run" }, false},
		{"setup encoded bound", func(t *backend.Task) { t.Setup = []string{strings.Repeat("x", 16380)} }, true},
		{"setup encoded overflow", func(t *backend.Task) { t.Setup = []string{strings.Repeat("x", 16381)} }, false},
		{"setup escaping", func(t *backend.Task) { t.Setup = []string{strings.Repeat("<", 3000)} }, false},
		{"duplicate", func(t *backend.Task) { t.Outputs = []string{"out", "out"} }, false},
		{"ancestor", func(t *backend.Task) { t.Outputs = []string{"out", "out/file"} }, false},
		{"ancestor reversed", func(t *backend.Task) { t.Outputs = []string{"out/file", "out"} }, false},
		{"too many outputs", func(t *backend.Task) {
			for i := 0; i < 17; i++ {
				t.Outputs = append(t.Outputs, string(rune('a'+i)))
			}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := workspaceTask()
			test.change(&task)
			before, _ := json.Marshal(task)
			err := backend.ValidateWorkspace(task)
			after, _ := json.Marshal(task)
			if (err == nil) != test.ok {
				t.Fatalf("ValidateWorkspace() = %v; valid = %t", err, test.ok)
			}
			if string(before) != string(after) {
				t.Fatal("validation mutated task")
			}
		})
	}
}

func TestWorkspaceOutputPaths(t *testing.T) {
	for _, name := range []string{"", "/out", "C:out", "a\\b", ".", "..", "a/../b", "a/./b", "a//b", "out/", "*.txt", "a?", "a[0]", "a\x00b", string([]byte{255}), strings.Repeat("a", 241), strings.Repeat("a/", 16) + "z"} {
		t.Run(name, func(t *testing.T) {
			task := workspaceTask()
			task.Outputs = []string{name}
			if err := backend.ValidateWorkspace(task); err == nil {
				t.Fatal("accepted invalid output root")
			}
		})
	}
}

func TestWorkspaceDefaultBytes(t *testing.T) {
	// This frozen predecessor shape guards every old field/order, not just one
	// minimal task. It intentionally has no snapshot fields.
	type oldTask struct {
		Argv             []string             `json:"argv"`
		Env              map[string]string    `json:"env,omitempty"`
		Directory        string               `json:"directory,omitempty"`
		Image            string               `json:"image,omitempty"`
		CPUMillis        int64                `json:"cpu_millis,omitempty"`
		MemoryBytes      int64                `json:"memory_bytes,omitempty"`
		Accelerator      string               `json:"accelerator,omitempty"`
		AcceleratorCount int32                `json:"accelerator_count,omitempty"`
		Target           string               `json:"target,omitempty"`
		TimeoutSeconds   int64                `json:"timeout_seconds,omitempty"`
		CPULimitMillis   int64                `json:"cpu_limit_millis,omitempty"`
		MemoryLimitBytes int64                `json:"memory_limit_bytes,omitempty"`
		TargetLabels     map[string]string    `json:"target_labels,omitempty"`
		NodeSelector     map[string]string    `json:"node_selector,omitempty"`
		Retry            *backend.RetryPolicy `json:"retry,omitempty"`
	}
	for _, task := range []backend.Task{
		{}, {Argv: []string{"run"}, Setup: []string{}, Outputs: []string{}},
		{Argv: []string{"é", "<>&"}, Env: map[string]string{"Z": "last", "A": "first"}, Directory: "/work", Image: "img", CPUMillis: 1, MemoryBytes: 2, Accelerator: "gpu", AcceleratorCount: 1, Target: "local", TimeoutSeconds: 3, CPULimitMillis: 4, MemoryLimitBytes: 5, TargetLabels: map[string]string{"k": "v"}, NodeSelector: map[string]string{"n": "x"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}},
	} {
		old := oldTask{task.Argv, task.Env, task.Directory, task.Image, task.CPUMillis, task.MemoryBytes, task.Accelerator, task.AcceleratorCount, task.Target, task.TimeoutSeconds, task.CPULimitMillis, task.MemoryLimitBytes, task.TargetLabels, task.NodeSelector, task.Retry}
		want, err := json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		got, err := backend.TaskBytes(task)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("legacy bytes changed: %s; want %s", got, want)
		}
		if err := backend.ValidateWorkspace(task); err != nil {
			t.Fatalf("new validator changed default acceptance: %v", err)
		}
	}
}

func TestWorkspaceClone(t *testing.T) {
	task := workspaceTask()
	task.Setup = []string{"setup"}
	task.Outputs = []string{"out"}
	task.Env = map[string]string{"K": "V"}
	task.TargetLabels = map[string]string{"L": "V"}
	task.NodeSelector = map[string]string{"N": "V"}
	task.Retry = &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}}
	before, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	copy := backend.CloneTask(task)
	if !reflect.DeepEqual(task, copy) {
		t.Fatal("clone changed task values")
	}
	copy.Argv[0] = "changed"
	copy.Workdir.ID = "changed"
	copy.Setup[0] = "changed"
	copy.Outputs[0] = "changed"
	copy.Env["K"] = "changed"
	copy.TargetLabels["L"] = "changed"
	copy.NodeSelector["N"] = "changed"
	copy.Retry.ExitCodes[0] = 9
	copy.Retry.MaxAttempts = 3
	after, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("clone aliases original task")
	}
	if got := backend.CloneTask(backend.Task{}); got.Argv != nil || got.Env != nil || got.Workdir != nil || got.Setup != nil || got.Outputs != nil {
		t.Fatal("clone changed nil defaults")
	}
}

func TestWorkspaceIdentity(t *testing.T) {
	task := workspaceTask()
	task.Setup = []string{"prepare"}
	task.Outputs = []string{"out"}
	before, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*backend.Task){
		func(t *backend.Task) { t.Workdir.ID = strings.Repeat("c", 32) },
		func(t *backend.Task) { t.Workdir.SHA256 = strings.Repeat("c", 64) },
		func(t *backend.Task) { t.Workdir.Size++ },
		func(t *backend.Task) { t.Setup[0] = "other" },
		func(t *backend.Task) { t.Outputs[0] = "other" },
	} {
		copy := backend.CloneTask(task)
		change(&copy)
		after, err := backend.TaskBytes(copy)
		if err != nil {
			t.Fatal(err)
		}
		if string(before) == string(after) {
			t.Fatal("snapshot field missing from task identity")
		}
	}
}
