package tasklower_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
)

func TestInfraBinding(t *testing.T) {
	const requested = "run: echo hello\nresources: {infra: ssh/dgx}\n"
	binding := &tasklower.InfraBinding{Infra: "ssh/dgx", Target: "dgx"}
	for _, tt := range []struct {
		name, input, target, wantTarget string
		binding                         *tasklower.InfraBinding
		refused                         bool
	}{
		{name: "matching", input: requested, binding: binding, wantTarget: "dgx"},
		{name: "matching explicit target", input: requested, binding: binding, target: "dgx", wantTarget: "dgx"},
		{name: "missing binding", input: requested, refused: true},
		{name: "target alone is not a binding", input: requested, target: "dgx", refused: true},
		{name: "wrong pool", input: requested, binding: &tasklower.InfraBinding{Infra: "ssh/other", Target: "dgx"}, refused: true},
		{name: "explicit target conflict", input: requested, binding: binding, target: "other", refused: true},
		{name: "unused binding", input: "run: echo hello\n", binding: binding},
		{name: "unused explicit target", input: "run: echo hello\n", binding: binding, target: "other", wantTarget: "other"},
		{name: "null selector", input: "run: echo hello\nresources: {infra: null}\n", binding: binding},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := taskyaml.Parse(strings.NewReader(tt.input), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			opts := tasklower.Options{Shell: "/bin/sh", Target: tt.target, InfraBinding: tt.binding}
			task, notes, err := tasklower.Lower(doc, opts)
			if tt.refused {
				var field *taskyaml.Error
				if !errors.As(err, &field) || field.Code != taskyaml.Unsupported || field.Path != "resources.infra" || field.Line != 2 || task != nil {
					t.Fatalf("Lower = %v, %v; want typed infra refusal", task, err)
				}
				return
			}
			if err != nil || task.Target != tt.wantTarget {
				t.Fatalf("Lower target = %v, %v; want %q", task, err, tt.wantTarget)
			}
			if opts.Target != tt.target {
				t.Fatal("Lower changed the explicit target option")
			}
			if doc.Task.Resources != nil && doc.Task.Resources.Infra != nil {
				if *doc.Task.Resources.Infra != "ssh/dgx" {
					t.Fatal("Lower changed the document")
				}
				found := false
				for _, n := range notes {
					if n.Path == "resources.infra" {
						found = n.Status == tasklower.Lowered && n.Detail == "target from explicit infra binding"
					}
				}
				if !found {
					t.Fatalf("no explicit lowering note: %v", notes)
				}
			}
		})
	}
}

func TestInfraBindingInvalid(t *testing.T) {
	for _, tt := range []struct {
		name    string
		binding tasklower.InfraBinding
	}{
		{"zero", tasklower.InfraBinding{}},
		{"no infra", tasklower.InfraBinding{Target: "dgx"}},
		{"no target", tasklower.InfraBinding{Infra: "ssh/dgx"}},
		{"padded infra", tasklower.InfraBinding{Infra: " ssh/dgx", Target: "dgx"}},
		{"padded target", tasklower.InfraBinding{Infra: "ssh/dgx", Target: "dgx "}},
		{"bad infra", tasklower.InfraBinding{Infra: "\xff", Target: "dgx"}},
		{"bad target", tasklower.InfraBinding{Infra: "ssh/dgx", Target: "\xff"}},
		{"nul", tasklower.InfraBinding{Infra: "ssh/\x00dgx", Target: "dgx"}},
		{"newline", tasklower.InfraBinding{Infra: "ssh/dgx", Target: "d\ngx"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := taskyaml.Parse(strings.NewReader("run: echo hello\n"), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = tasklower.Lower(doc, tasklower.Options{Shell: "/bin/sh", InfraBinding: &tt.binding})
			if err == nil || err.Error() != "tasklower: invalid infra binding" {
				t.Fatalf("Lower = %v; want fixed option error", err)
			}
		})
	}
}

func TestInfraBindingWithoutLocations(t *testing.T) {
	run, infra := "echo hello", "ssh/dgx"
	doc := &taskyaml.Document{Task: taskyaml.Task{Run: &run, Resources: &taskyaml.Resources{Infra: &infra}}}
	opts := tasklower.Options{Shell: "/bin/sh", InfraBinding: &tasklower.InfraBinding{Infra: infra, Target: "dgx"}}
	task, _, err := tasklower.Lower(doc, opts)
	if err != nil || task.Target != "dgx" {
		t.Fatalf("Lower = %v, %v", task, err)
	}
	opts.InfraBinding.Target = "changed"
	if task.Target != "dgx" {
		t.Fatal("returned task retained mutable binding")
	}
	for _, text := range []string{"autostop: false", "job_recovery: {}", "disk_size: 128"} {
		doc, err := taskyaml.Parse(strings.NewReader("run: echo hello\nresources: {infra: ssh/dgx, "+text+"}\n"), taskyaml.Options{})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = tasklower.Lower(doc, opts)
		var field *taskyaml.Error
		if !errors.As(err, &field) || field.Code != taskyaml.Unsupported || field.Path == "resources.infra" {
			t.Fatalf("binding bypassed %q: %v", text, err)
		}
	}
}

func TestInfraBindingPrimer(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "taskyaml", "testdata", "primer", "09-dgx-gpu.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := taskyaml.Parse(bytes.NewReader(data), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	opts := tasklower.Options{Shell: "/bin/sh", AllowDrop: []string{"name"}, InfraBinding: &tasklower.InfraBinding{Infra: "ssh/dgx", Target: "dgx"}}
	task, _, err := tasklower.Lower(doc, opts)
	if err != nil || task.Target != "dgx" || task.Accelerator != "GB10" || task.AcceleratorCount != 1 || len(task.Argv) != 3 || task.Argv[2] != *doc.Task.Run {
		t.Fatalf("Lower unchanged primer = %v, %v", task, err)
	}
	opts.InfraBinding.Infra = "ssh/other"
	_, _, err = tasklower.Lower(doc, opts)
	var field *taskyaml.Error
	if !errors.As(err, &field) || field.Code != taskyaml.Unsupported || field.Path != "resources.infra" {
		t.Fatalf("wrong pool = %v", err)
	}
}

func ExampleInfraBinding() {
	doc, _ := taskyaml.Parse(strings.NewReader("resources: {infra: ssh/dgx, accelerators: GB10:1}\nrun: nvidia-smi\n"), taskyaml.Options{})
	// The transport supplied this mapping after authenticating its endpoint.
	opts := tasklower.Options{Shell: "/bin/sh", InfraBinding: &tasklower.InfraBinding{Infra: "ssh/dgx", Target: "dgx"}}
	task, _, err := tasklower.Lower(doc, opts)
	fmt.Println(task.Target, task.Accelerator, task.AcceleratorCount, err)
	opts.InfraBinding.Infra = "ssh/other"
	_, _, err = tasklower.Lower(doc, opts)
	var field *taskyaml.Error
	if errors.As(err, &field) {
		fmt.Println(field.Code, field.Path)
	}
	// Output:
	// dgx GB10 1 <nil>
	// unsupported resources.infra
}
