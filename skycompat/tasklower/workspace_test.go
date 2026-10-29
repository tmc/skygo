package tasklower

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
)

func TestCheckWorkspace(t *testing.T) {
	tests := []struct {
		name string
		text string
		opts Options
		ok   bool
	}{
		{"workdir", "workdir: /does/not/exist\nrun: x\n", Options{Shell: "/bin/sh"}, true},
		{"setup and outputs", "workdir: .\nsetup: make\nrun: x\n", Options{Shell: "/bin/sh", Outputs: []string{"out", "logs"}}, true},
		{"metadata", "name: hello\nworkdir: .\nrun: x\n", Options{Shell: "/bin/sh", AllowDrop: []string{"name"}}, true},
		{"metadata refused", "name: hello\nworkdir: .\nrun: x\n", Options{Shell: "/bin/sh"}, false},
		{"run absent", "workdir: .\nsetup: make\n", Options{Shell: "/bin/sh"}, false},
		{"shell absent", "workdir: .\nrun: x\n", Options{}, false},
		{"shell relative", "workdir: .\nrun: x\n", Options{Shell: "sh"}, false},
		{"two nodes", "workdir: .\nrun: x\nnum_nodes: 2\n", Options{Shell: "/bin/sh"}, false},
		{"file mounts", "workdir: .\nrun: x\nfile_mounts: {/a: b}\n", Options{Shell: "/bin/sh"}, false},
		{"workdir substitution", "workdir: ${HOME}\nrun: x\n", Options{Shell: "/bin/sh"}, false},
		{"overlap", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Outputs: []string{"out", "out/log"}}, false},
		{"traversal", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Outputs: []string{"../out"}}, false},
		{"setup NUL", "workdir: .\nsetup: \"a\\0b\"\nrun: x\n", Options{Shell: "/bin/sh"}, false},
		{"run NUL", "workdir: .\nrun: \"a\\0b\"\n", Options{Shell: "/bin/sh"}, false},
		{"env NUL", "workdir: .\nenvs: {A: \"a\\0b\"}\nrun: x\n", Options{Shell: "/bin/sh"}, false},
		{"resource choice", "workdir: .\nrun: x\nresources: {accelerators: {L4: 1, H100: 1}}\n", Options{Shell: "/bin/sh"}, false},
		{"GPU", "workdir: .\nrun: x\nresources: {accelerators: 'L4:2'}\n", Options{Shell: "/bin/sh"}, true},
		{"infra", "workdir: .\nrun: x\nresources: {infra: k8s}\n", Options{Shell: "/bin/sh", InfraBinding: &InfraBinding{Infra: "k8s", Target: "kube"}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, err := taskyaml.Parse(strings.NewReader(tc.text), taskyaml.Options{})
			if err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			checkErr := CheckWorkspace(d, tc.opts)
			if (checkErr == nil) != tc.ok {
				t.Fatalf("CheckWorkspace = %v, want success %v", checkErr, tc.ok)
			}
			bound := tc.opts
			bound.Workdir = ref
			got, _, lowerErr := Lower(d, bound)
			if (checkErr == nil) != (lowerErr == nil) || checkErr != nil && checkErr.Error() != lowerErr.Error() {
				t.Fatalf("check %v differs from bound lowering %v", checkErr, lowerErr)
			}
			if tc.ok && !proto.Equal(got.Workdir, ref) {
				t.Fatalf("actual bound Ref changed: %v", got.Workdir)
			}
			after, err := json.Marshal(d)
			if err != nil || string(after) != string(before) || tc.opts.Workdir != nil {
				t.Fatalf("check changed document/options: %v", err)
			}
		})
	}
}

func TestCheckWorkspacePreconditions(t *testing.T) {
	d, err := taskyaml.Parse(strings.NewReader("run: x\n"), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckWorkspace(nil, Options{}); err == nil {
		t.Fatal("nil document accepted")
	}
	if err := CheckWorkspace(d, Options{Shell: "/bin/sh"}); err == nil {
		t.Fatal("document without workdir accepted")
	}
	workdir := "."
	d.Task.Workdir = &workdir
	if err := CheckWorkspace(d, Options{Shell: "/bin/sh", Workdir: ref}); err == nil {
		t.Fatal("bound reference accepted by check")
	}
	// Checking does not relax Lower: only a real caller-supplied Ref permits
	// an executable task, even after this same document passed CheckWorkspace.
	if err := CheckWorkspace(d, Options{Shell: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	if task, notes, err := Lower(d, Options{Shell: "/bin/sh"}); err == nil || task != nil || notes != nil {
		t.Fatalf("check supplied hidden authority: task=%v notes=%v err=%v", task, notes, err)
	}
}

func TestCheckWorkspaceKeepsExplicitEnvironment(t *testing.T) {
	t.Setenv("AMBIENT_ONLY", "not-a-task-input")
	d, err := taskyaml.Parse(strings.NewReader("workdir: .\nenvs: {REQUIRED: null}\nrun: x\n"), taskyaml.Options{EnvOverrides: map[string]string{"REQUIRED": "explicit"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckWorkspace(d, Options{Shell: "/bin/sh"}); err != nil {
		t.Fatal(err)
	}
	got, _, err := Lower(d, Options{Shell: "/bin/sh", Workdir: ref})
	if err != nil {
		t.Fatal(err)
	}
	if got.Env["REQUIRED"] != "explicit" {
		t.Fatalf("explicit env lost: %v", got.Env)
	}
	if _, ok := got.Env["AMBIENT_ONLY"]; ok {
		t.Fatal("checking imported process environment")
	}
}
