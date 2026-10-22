package backend_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

// workspaceFieldTests are Setup and Outputs cases shared by ValidateWorkspace
// and ValidateWorkspaceTemplate. An empty want means valid.
var workspaceFieldTests = []struct {
	name   string
	change func(*backend.Task)
	want   string
}{
	{"none", func(*backend.Task) {}, ""},
	{"setup and outputs", func(t *backend.Task) { t.Setup = []string{"prepare", ""}; t.Outputs = []string{"a", "ab/c"} }, ""},
	{"empty optional slices", func(t *backend.Task) { t.Setup = []string{}; t.Outputs = []string{} }, ""},
	{"setup 64 arguments", func(t *backend.Task) { t.Setup = make([]string, 64); t.Setup[0] = "run" }, ""},
	{"setup 65 arguments", func(t *backend.Task) { t.Setup = make([]string, 65); t.Setup[0] = "run" }, "setup exceeds 64 arguments"},
	{"setup program", func(t *backend.Task) { t.Setup = []string{""} }, "setup requires a program"},
	{"setup nul", func(t *backend.Task) { t.Setup = []string{"a\x00b"} }, "invalid setup argument"},
	{"setup UTF8", func(t *backend.Task) { t.Setup = []string{string([]byte{255})} }, "invalid setup argument"},
	{"setup encoded bound", func(t *backend.Task) { t.Setup = []string{strings.Repeat("x", 16<<10-4)} }, ""},
	{"setup encoded overflow", func(t *backend.Task) { t.Setup = []string{strings.Repeat("x", 16<<10-3)} }, "setup exceeds 16 KiB encoded"},
	{"16 outputs", func(t *backend.Task) { t.Outputs = strings.Split("abcdefghijklmnop", "") }, ""},
	{"17 outputs", func(t *backend.Task) { t.Outputs = strings.Split("abcdefghijklmnopq", "") }, "outputs exceeds 16 roots"},
	{"absolute output", func(t *backend.Task) { t.Outputs = []string{"/out"} }, "invalid output root"},
	{"output depth 16", func(t *backend.Task) { t.Outputs = []string{strings.Repeat("a/", 15) + "z"} }, ""},
	{"output depth 17", func(t *backend.Task) { t.Outputs = []string{strings.Repeat("a/", 16) + "z"} }, "output root exceeds depth 16"},
	{"output dot dot", func(t *backend.Task) { t.Outputs = []string{"a/../b"} }, "invalid output root component"},
	{"duplicate outputs", func(t *backend.Task) { t.Outputs = []string{"out", "out"} }, "overlapping output roots"},
	{"overlapping outputs", func(t *backend.Task) { t.Outputs = []string{"out/file", "out"} }, "overlapping output roots"},
}

func TestValidateWorkspaceTemplate(t *testing.T) {
	for _, test := range workspaceFieldTests {
		t.Run(test.name, func(t *testing.T) {
			task := backend.Task{Argv: []string{"./run"}}
			test.change(&task)
			checkWorkspace(t, "ValidateWorkspaceTemplate", backend.ValidateWorkspaceTemplate, task, test.want)
		})
	}
}

// TestValidateWorkspaceSharedFields shows that the Setup and Outputs checks
// shared with ValidateWorkspaceTemplate still apply to a resolved Workdir.
func TestValidateWorkspaceSharedFields(t *testing.T) {
	for _, test := range workspaceFieldTests {
		t.Run(test.name, func(t *testing.T) {
			task := workspaceTask()
			test.change(&task)
			checkWorkspace(t, "ValidateWorkspace", backend.ValidateWorkspace, task, test.want)
		})
	}
}

func TestValidateWorkspaceTemplateRefusals(t *testing.T) {
	ref := &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}
	for _, test := range []struct {
		name string
		task backend.Task
		want string
	}{
		{"workdir", backend.Task{Workdir: ref}, "workspace template requires no workdir"},
		{"invalid workdir", backend.Task{Workdir: &artifact.Ref{}}, "workspace template requires no workdir"},
		{"directory", backend.Task{Directory: "/work"}, "workspace template requires an empty directory"},
		{"directory and setup", backend.Task{Directory: "/work", Setup: []string{"prepare"}}, "workspace template requires an empty directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkWorkspace(t, "ValidateWorkspaceTemplate", backend.ValidateWorkspaceTemplate, test.task, test.want)
		})
	}
}

// TestValidateWorkspaceRefusals pins every ValidateWorkspace refusal that
// precedes the shared field checks. A template must not relax any of them.
func TestValidateWorkspaceRefusals(t *testing.T) {
	ref := func() *artifact.Ref {
		return &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}
	}
	for _, test := range []struct {
		name string
		task backend.Task
		want string
	}{
		{"no snapshot", backend.Task{Argv: []string{"run"}}, ""},
		{"no snapshot with directory", backend.Task{Directory: "/work"}, ""},
		{"setup without workdir", backend.Task{Setup: []string{"prepare"}}, "setup and outputs require workdir"},
		{"outputs without workdir", backend.Task{Outputs: []string{"result"}}, "setup and outputs require workdir"},
		{"empty setup without workdir", backend.Task{Setup: []string{""}}, "setup and outputs require workdir"},
		{"bad id", backend.Task{Workdir: &artifact.Ref{ID: "bad", SHA256: strings.Repeat("b", 64), Size: 1}}, "workdir: invalid artifact id"},
		{"bad sha256", backend.Task{Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: "bad", Size: 1}}, "workdir: invalid artifact sha256"},
		{"zero size", backend.Task{Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64)}}, "workdir: artifact size outside 1..33554432 bytes"},
		{"directory", backend.Task{Workdir: ref(), Directory: "/work"}, "workdir requires an empty directory"},
		{"directory before fields", backend.Task{Workdir: ref(), Directory: "/work", Setup: []string{""}}, "workdir requires an empty directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkWorkspace(t, "ValidateWorkspace", backend.ValidateWorkspace, test.task, test.want)
		})
	}
}

func checkWorkspace(t *testing.T, name string, validate func(backend.Task) error, task backend.Task, want string) {
	t.Helper()
	before, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	err = validate(task)
	after, _ := json.Marshal(task)
	if string(before) != string(after) {
		t.Fatalf("%s mutated task", name)
	}
	switch {
	case want == "" && err != nil:
		t.Fatalf("%s() = %v; want nil", name, err)
	case want != "" && (err == nil || err.Error() != want):
		t.Fatalf("%s() = %v; want %q", name, err, want)
	}
}
