package control

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

func TestWorkspaceValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*backend.Task)
		valid  bool
	}{
		{"input only", func(*backend.Task) {}, true},
		{"setup outputs", func(t *backend.Task) { t.Setup = []string{"prepare", ""}; t.Outputs = []string{"out", "other/file"} }, true},
		{"bad input", func(t *backend.Task) { t.Workdir.Size = 0 }, false},
		{"directory conflict", func(t *backend.Task) { t.Directory = "/work" }, false},
		{"setup without input", func(t *backend.Task) { t.Workdir = nil; t.Setup = []string{"prepare"} }, false},
		{"outputs without input", func(t *backend.Task) { t.Workdir = nil; t.Outputs = []string{"out"} }, false},
		{"bad setup program", func(t *backend.Task) { t.Setup = []string{""} }, false},
		{"encoded setup overflow", func(t *backend.Task) { t.Setup = []string{strings.Repeat("x", 16381)} }, false},
		{"bad output", func(t *backend.Task) { t.Outputs = []string{"../outside"} }, false},
		{"overlapping outputs", func(t *backend.Task) { t.Outputs = []string{"a/b", "a"} }, false},
		{"old argv check", func(t *backend.Task) { t.Argv = nil }, false},
		{"old resource check", func(t *backend.Task) { t.CPUMillis = -1 }, false},
		{"old placement check", func(t *backend.Task) { t.TargetLabels = map[string]string{"": "v"} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := backend.Task{Argv: []string{"./run"}, Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}}
			test.change(&task)
			before, err := backend.TaskBytes(task)
			if err != nil {
				t.Fatal(err)
			}
			err = Validate(task)
			if test.valid && err != nil {
				t.Fatal(err)
			}
			if !test.valid && !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() = %v; want ErrInvalid", err)
			}
			after, err := backend.TaskBytes(task)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Fatal("validation rewrote accepted task")
			}
		})
	}
}

func TestCloneTaskWorkspaceIsolation(t *testing.T) {
	task := backend.Task{
		Argv: []string{"run"}, Env: map[string]string{"E": "old"},
		TargetLabels: map[string]string{"L": "old"}, NodeSelector: map[string]string{"N": "old"},
		Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024},
		Setup:   []string{"prepare"}, Outputs: []string{"out"},
		Retry: &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}},
	}
	before, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	copy := cloneTask(task)
	if !reflect.DeepEqual(copy, task) {
		t.Fatal("clone changed original values")
	}
	copy.Argv[0] = "changed"
	copy.Env["E"] = "changed"
	copy.TargetLabels["L"] = "changed"
	copy.NodeSelector["N"] = "changed"
	copy.Workdir.Size++
	copy.Workdir.ID = "changed"
	copy.Setup[0] = "changed"
	copy.Outputs[0] = "changed"
	copy.Retry.MaxAttempts = 3
	copy.Retry.ExitCodes[0] = 9
	after, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("private clone aliases original mutable fields")
	}
	zero := cloneTask(backend.Task{})
	if !reflect.DeepEqual(zero, backend.Task{}) {
		t.Fatal("clone changed zero task")
	}
}

func TestWorkspaceDefaultValidation(t *testing.T) {
	for _, task := range []backend.Task{
		{Argv: []string{"run"}},
		{Argv: []string{"run"}, Directory: "/old/meaning", Setup: []string{}, Outputs: []string{}},
		{Argv: []string{"run"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{9, 7, 9}}},
	} {
		before, err := backend.TaskBytes(task)
		if err != nil {
			t.Fatal(err)
		}
		if err := Validate(task); err != nil {
			t.Fatal(err)
		}
		after, err := backend.TaskBytes(task)
		if err != nil {
			t.Fatal(err)
		}
		if string(before) != string(after) {
			t.Fatal("default validation changed original fields or retry policy")
		}
	}
}
