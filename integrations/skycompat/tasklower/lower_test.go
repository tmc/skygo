package tasklower

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/integrations/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

var ref = &skyv1.ArtifactRef{Id: "00112233445566778899aabbccddeeff", Sha256: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", Size: 4096}

func lowerText(t *testing.T, text string, opts Options) (*skyv1.Task, []Note, error) {
	t.Helper()
	d, err := taskyaml.Parse(strings.NewReader(text), taskyaml.Options{})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return Lower(d, opts)
}

func TestLower(t *testing.T) {
	sh := Options{Shell: "/bin/sh"}
	tests := []struct {
		name    string
		yaml    string
		opts    Options
		want    *skyv1.Task
		wantErr string
	}{
		{"run", "run: echo hi\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "echo hi"}}, ""},
		{"run no shell", "run: echo hi\n", Options{}, nil, "task yaml: unsupported run at 1:6: requires a shell"},
		{"run absent", "envs: {A: b}\n", sh, nil, "task yaml: unsupported run at 0:0: requires run text"},
		{"run null", "run: null\n", sh, nil, "task yaml: unsupported run at 1:6: requires run text"},
		{"run nul", "run: \"a\\0b\"\n", sh, nil, "task yaml: invalid run at 1:6"},
		{"empty run", "run: ''\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", ""}}, ""},
		{"options", "run: x\n", Options{Shell: "/usr/bin/bash", Target: "kube", TimeoutSeconds: 604800}, &skyv1.Task{Argv: []string{"/usr/bin/bash", "-c", "x"}, Target: "kube", TimeoutSeconds: 604800}, ""},
		{"setup", "workdir: .\nsetup: make\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref, Outputs: []string{"out", "logs/a"}}, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Setup: []string{"/bin/sh", "-c", "make"}, Workdir: ref, Outputs: []string{"out", "logs/a"}}, ""},
		{"setup null", "setup: null\nrun: x\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"setup no workdir", "setup: make\nrun: x\n", sh, nil, "task yaml: unsupported setup at 1:8: requires workdir"},
		{"setup no shell", "workdir: .\nsetup: make\nrun: x\n", Options{Workdir: ref}, nil, "task yaml: unsupported setup at 2:8: requires a shell"},
		{"setup limit", "workdir: .\nsetup: \"" + strings.Repeat(`\x01`, 3000) + "\"\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref}, nil, "task yaml: limit setup at 2:8"},
		{"workdir no ref", "workdir: .\nrun: x\n", sh, nil, "task yaml: unsupported workdir at 1:10: requires a workdir reference"},
		{"workdir dollar", "workdir: ${HOME}\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref}, nil, "task yaml: unsupported workdir at 1:10: environment substitution"},
		{"ref without workdir", "run: x\n", Options{Shell: "/bin/sh", Workdir: ref}, nil, "tasklower: workdir reference supplied for a task without workdir"},
		{"outputs without workdir", "run: x\n", Options{Shell: "/bin/sh", Outputs: []string{"out"}}, nil, "tasklower: outputs require a task workdir"},
		{"envs", "run: x\nenvs: {B: '2', A: 'x=y'}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Env: map[string]string{"A": "x=y", "B": "2"}}, ""},
		{"envs nul", "run: x\nenvs: {A: \"\\0\"}\n", sh, nil, "task yaml: invalid envs at 2:7"},
		{"name", "name: n\nrun: x\n", sh, nil, "task yaml: unsupported name at 1:7: requires explicit drop"},
		{"name dropped", "name: n\nrun: x\n", Options{Shell: "/bin/sh", AllowDrop: []string{"name"}}, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"name null", "name: null\nrun: x\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"user yaml", "_user_specified_yaml: y\nrun: x\n", Options{Shell: "/bin/sh", AllowDrop: []string{"name"}}, nil, "task yaml: unsupported _user_specified_yaml at 1:23: requires explicit drop"},
		{"cpus", "run: x\nresources: {cpus: 0.25}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, CpuMillis: 250}, ""},
		{"cpus minimum", "run: x\nresources: {cpus: 4+}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, CpuMillis: 4000}, ""},
		{"cpus millicpu", "run: x\nresources: {cpus: 0.001}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, CpuMillis: 1}, ""},
		{"cpus fraction", "run: x\nresources: {cpus: 0.0005}\n", sh, nil, "task yaml: invalid resources.cpus at 2:19: requires whole millicpus"},
		{"cpus largest", "run: x\nresources: {cpus: 1000000000000000}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, CpuMillis: 1000000000000000000}, ""},
		{"accelerators map", "run: x\nresources: {accelerators: {H100: 8}}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Accelerator: "H100", AcceleratorCount: 8}, ""},
		{"accelerators string", "run: x\nresources: {accelerators: 'L4:2'}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Accelerator: "L4", AcceleratorCount: 2}, ""},
		{"accelerators one", "run: x\nresources: {accelerators: T4}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Accelerator: "T4", AcceleratorCount: 1}, ""},
		{"accelerators int32", "run: x\nresources: {accelerators: {A: 2147483647}}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Accelerator: "A", AcceleratorCount: math.MaxInt32}, ""},
		{"accelerators overflow", "run: x\nresources: {accelerators: {A: 2147483648}}\n", sh, nil, "task yaml: unsupported resources.accelerators at 2:27: requires a whole count"},
		{"accelerators fraction", "run: x\nresources: {accelerators: 'A:0.5'}\n", sh, nil, "task yaml: unsupported resources.accelerators at 2:27: requires a whole count"},
		{"image", "run: x\nresources: {image_id: 'docker:ghcr.io/a/b@sha256:00'}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}, Image: "ghcr.io/a/b@sha256:00"}, ""},
		{"image bare", "run: x\nresources: {image_id: ubuntu}\n", sh, nil, "task yaml: unsupported resources.image_id at 2:23: requires docker:IMAGE"},
		{"image empty", "run: x\nresources: {image_id: 'docker:'}\n", sh, nil, "task yaml: unsupported resources.image_id at 2:23: requires docker:IMAGE"},
		{"image other", "run: x\nresources: {image_id: 'gcr:docker:x'}\n", sh, nil, "task yaml: unsupported resources.image_id at 2:23: requires docker:IMAGE"},
		{"use_spot false", "run: x\nresources: {use_spot: false}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"use_spot true", "run: x\nresources: {use_spot: true}\n", sh, nil, "task yaml: unsupported resources.use_spot at 2:23"},
		{"disk default", "run: x\nresources: {disk_size: 256}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"cloud", "run: x\nresources: {cloud: kubernetes}\n", sh, nil, "task yaml: unsupported resources.cloud at 2:20: select a skygo target"},
		{"region", "run: x\nresources: {region: r}\n", sh, nil, "task yaml: unsupported resources.region at 2:21: select a skygo target"},
		{"zone", "run: x\nresources: {zone: z}\n", sh, nil, "task yaml: unsupported resources.zone at 2:19: select a skygo target"},
		{"instance type", "run: x\nresources: {instance_type: i}\n", sh, nil, "task yaml: unsupported resources.instance_type at 2:28: select a skygo target"},
		{"one node", "run: x\nnum_nodes: 1\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"two nodes", "run: x\nnum_nodes: 2\n", sh, nil, "task yaml: unsupported num_nodes at 2:12"},
		{"file mounts", "run: x\nfile_mounts: {/a: b}\n", sh, nil, "task yaml: unsupported file_mounts at 2:14"},
		{"volumes", "run: x\nvolumes: {/a: b}\n", sh, nil, "task yaml: unsupported volumes at 2:10"},
		{"empty mounts", "run: x\nfile_mounts: {}\nvolumes: {}\n", sh, &skyv1.Task{Argv: []string{"/bin/sh", "-c", "x"}}, ""},
		{"first problem wins", "num_nodes: 2\nresources: {cloud: aws}\nrun: x\n", sh, nil, "task yaml: unsupported num_nodes at 1:12"},
		{"relative shell", "run: x\n", Options{Shell: "sh"}, nil, "tasklower: shell must be a clean absolute path"},
		{"unclean shell", "run: x\n", Options{Shell: "/bin/../bin/sh"}, nil, "tasklower: shell must be a clean absolute path"},
		{"root shell", "run: x\n", Options{Shell: "/"}, nil, "tasklower: shell must be a clean absolute path"},
		{"timeout", "run: x\n", Options{Shell: "/bin/sh", TimeoutSeconds: 604801}, nil, "tasklower: timeout outside 0..604800 seconds"},
		{"negative timeout", "run: x\n", Options{Shell: "/bin/sh", TimeoutSeconds: -1}, nil, "tasklower: timeout outside 0..604800 seconds"},
		{"allow drop", "run: x\n", Options{Shell: "/bin/sh", AllowDrop: []string{"run"}}, nil, "tasklower: allow-drop accepts only name and _user_specified_yaml"},
		{"bad ref", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Workdir: &skyv1.ArtifactRef{Id: ref.Id, Sha256: ref.Sha256, Size: 32<<20 + 1}}, nil, "tasklower: invalid workdir reference"},
		{"upper ref", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Workdir: &skyv1.ArtifactRef{Id: strings.ToUpper(ref.Id), Sha256: ref.Sha256, Size: 1}}, nil, "tasklower: invalid workdir reference"},
		{"output absolute", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref, Outputs: []string{"/out"}}, nil, "tasklower: invalid output root"},
		{"output overlap", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref, Outputs: []string{"out", "out/a"}}, nil, "tasklower: overlapping output roots"},
		{"output dotdot", "workdir: .\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref, Outputs: []string{"a/../b"}}, nil, "tasklower: invalid output root component"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := lowerText(t, tt.yaml, tt.opts)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("Lower error = %v, want %q", err, tt.wantErr)
				}
				if got != nil {
					t.Fatalf("Lower returned a task with an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			if !proto.Equal(got, tt.want) {
				t.Fatalf("Lower =\n%v\nwant\n%v", got, tt.want)
			}
		})
	}
}

func TestLowerErrorType(t *testing.T) {
	_, _, err := lowerText(t, "run: x\nresources: {cloud: aws}\n", Options{Shell: "/bin/sh"})
	var e *taskyaml.Error
	if !errors.As(err, &e) || *e != (taskyaml.Error{Code: taskyaml.Unsupported, Path: "resources.cloud", Line: 2, Column: 20}) {
		t.Fatalf("got %#v", err)
	}
}

// TestMemory covers every upstream unit, unitless GiB and int64 bounds.
func TestMemory(t *testing.T) {
	tests := []struct {
		memory string
		want   int64
	}{
		{"1", 1 << 30},
		{"0.5", 1 << 29},
		{"0.0625", 67108864},
		{"1+", 1 << 30},
		{"1KB", 1 << 10},
		{"1KI", 1 << 10},
		{"1kb", 1 << 10},
		{"1Ki", 1 << 10},
		{"3MB", 3 << 20},
		{"3Mi", 3 << 20},
		{"1GB", 1 << 30},
		{"1gi", 1 << 30},
		{"2TB", 2 << 40},
		{"2Ti", 2 << 40},
		{"1PB", 1 << 50},
		{"1Pi", 1 << 50},
		{"0.5KB", 512},
		{"8191PB", 8191 << 50},
		{"0.1KB", 0},
		{"8192PB", 0},
		{"0.0000000001", 0},
		{"1000000000000000PB", 0},
	}
	for _, tt := range tests {
		t.Run(tt.memory, func(t *testing.T) {
			got, _, err := lowerText(t, "run: x\nresources:\n  memory: '"+tt.memory+"'\n", Options{Shell: "/bin/sh"})
			if tt.want == 0 {
				if err == nil || err.Error() != "task yaml: invalid resources.memory at 3:11: requires whole bytes" {
					t.Fatalf("got %v, %v", got, err)
				}
				return
			}
			if err != nil || got.MemoryBytes != tt.want {
				t.Fatalf("got %v, %v; want %d", got, err, tt.want)
			}
		})
	}
}

// TestShellExact checks that run and setup text reach argv byte-exact.
func TestShellExact(t *testing.T) {
	texts := []string{
		"echo hi\n",
		"echo 'a b' \"c d\" $(pwd) `id` \\\nnext\n\n",
		"  leading and trailing  ",
		"ünïcødé ✓ 🚀 \u00a0\u2028\u2029\ufeff\u007f\u0085\t\x01",
		"",
	}
	for _, text := range texts {
		node := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "workdir"}, {Kind: yaml.ScalarNode, Value: "."},
			{Kind: yaml.ScalarNode, Value: "setup"}, {Kind: yaml.ScalarNode, Tag: "!!str", Value: text},
			{Kind: yaml.ScalarNode, Value: "run"}, {Kind: yaml.ScalarNode, Tag: "!!str", Value: text},
		}}
		src, err := yaml.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := lowerText(t, string(src), Options{Shell: "/bin/sh", Workdir: ref})
		if err != nil {
			t.Fatalf("%q: %v", src, err)
		}
		want := []string{"/bin/sh", "-c", text}
		if !slices.Equal(got.Argv, want) || !slices.Equal(got.Setup, want) {
			t.Fatalf("%q: argv %q setup %q", text, got.Argv, got.Setup)
		}
	}
}

func TestNotes(t *testing.T) {
	_, notes, err := lowerText(t, "name: n\nrun: x\nresources:\n  cpus: 1+\n  cloud: null\nservice: null\n", Options{Shell: "/bin/sh", AllowDrop: []string{"name"}})
	if err != nil {
		t.Fatal(err)
	}
	want := []Note{
		{"name", Dropped, "not executed"},
		{"run", Lowered, "argv is shell -c text"},
		{"resources", Default, "fields noted separately"},
		{"resources.cpus", Lowered, "cpu_millis; minimum lowered to exact request"},
		{"resources.cloud", Default, "null"},
		{"service", Default, "null"},
	}
	if !slices.Equal(notes, want) {
		t.Fatalf("notes = %v, want %v", notes, want)
	}
}

// TestDocumentWithoutLocations checks that typed fields are lowered or
// refused even when a caller builds the document without positions.
func TestDocumentWithoutLocations(t *testing.T) {
	run, name, cloud := "x", "n", "aws"
	tests := []struct {
		d       taskyaml.Document
		wantErr string
	}{
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Name: &name}}, "task yaml: unsupported name at 0:0: requires explicit drop"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Resources: &taskyaml.Resources{Cloud: &cloud}}}, "task yaml: unsupported resources.cloud at 0:0: select a skygo target"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, NumNodes: 3}}, "task yaml: unsupported num_nodes at 0:0"},
		{taskyaml.Document{DAGName: &name, Task: taskyaml.Task{Run: &run}}, "task yaml: unsupported header.name at 0:0"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Resources: &taskyaml.Resources{DiskSize: &taskyaml.Quantity{Value: "100"}}}}, "task yaml: unsupported resources.disk_size at 0:0"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Resources: &taskyaml.Resources{CPUs: &taskyaml.Quantity{Value: "1/2"}}}}, "task yaml: invalid resources.cpus at 0:0: requires whole millicpus"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Resources: &taskyaml.Resources{Accelerators: map[string]taskyaml.Quantity{"A": {Value: "1"}, "B": {Value: "1"}}}}}, "task yaml: unsupported resources.accelerators at 0:0"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run}, Locations: map[string]taskyaml.Position{"future": {Line: 1, Column: 1}}}, "task yaml: unsupported future at 1:1"},
		{taskyaml.Document{Task: taskyaml.Task{Run: &run, Envs: map[string]string{"1A": "x"}}}, "task yaml: invalid envs at 0:0"},
	}
	for _, tt := range tests {
		_, _, err := Lower(&tt.d, Options{Shell: "/bin/sh"})
		if err == nil || err.Error() != tt.wantErr {
			t.Errorf("Lower(%+v) = %v, want %q", tt.d, err, tt.wantErr)
		}
	}
	if _, _, err := Lower(nil, Options{}); err == nil {
		t.Error("Lower(nil) succeeded")
	}
}

// TestMarshalRoundTrip decodes Marshal output as YAML, as skygo does, and
// as JSON, and requires both to reproduce every value exactly.
func TestMarshalRoundTrip(t *testing.T) {
	var all []rune
	for r := rune(0); r < 0x3000; r++ {
		all = append(all, r)
	}
	all = append(all, 0xfeff, 0xfffd, 0xfffe, 0xffff, 0x10000, 0x1f680, 0x10ffff)
	text := string(all)
	task := &skyv1.Task{
		Argv:             []string{"/bin/sh", "-c", text, "", " ", "yes", "null", "~", "123", "- a", "#x", "a: b"},
		Env:              map[string]string{"A": text, "ON": "on", "Z": "0x10"},
		Image:            "busybox@sha256:00",
		CpuMillis:        math.MaxInt64,
		MemoryBytes:      1,
		Accelerator:      "A100",
		AcceleratorCount: math.MaxInt32,
		Target:           "tärget",
		TimeoutSeconds:   604800,
		Workdir:          &skyv1.ArtifactRef{Id: ref.Id, Sha256: ref.Sha256, Size: 32 << 20},
		Setup:            []string{"/bin/sh", "-c", text},
		Outputs:          []string{"out", "a/b"},
	}
	b, err := Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	if !utf8Printable(b) {
		t.Fatalf("Marshal wrote a character YAML refuses")
	}
	// file mirrors the shipping internal/cli taskFile keys and types; the
	// skygo binary scripts check the actual parser.
	type file struct {
		Argv             []string          `yaml:"argv" json:"argv"`
		Env              map[string]string `yaml:"env" json:"env"`
		Image            string            `yaml:"image" json:"image"`
		CPUMillis        int64             `yaml:"cpu_millis" json:"cpu_millis"`
		MemoryBytes      int64             `yaml:"memory_bytes" json:"memory_bytes"`
		Accelerator      string            `yaml:"accelerator" json:"accelerator"`
		AcceleratorCount int32             `yaml:"accelerator_count" json:"accelerator_count"`
		Target           string            `yaml:"target" json:"target"`
		TimeoutSeconds   int64             `yaml:"timeout_seconds" json:"timeout_seconds"`
		Workdir          *struct {
			ID     string `yaml:"id" json:"id"`
			SHA256 string `yaml:"sha256" json:"sha256"`
			Size   int64  `yaml:"size" json:"size"`
		} `yaml:"workdir" json:"workdir"`
		Setup   []string `yaml:"setup" json:"setup"`
		Outputs []string `yaml:"outputs" json:"outputs"`
	}
	var y, j file
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if err := d.Decode(&y); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	jd := json.NewDecoder(bytes.NewReader(b))
	jd.DisallowUnknownFields()
	if err := jd.Decode(&j); err != nil {
		t.Fatalf("json: %v", err)
	}
	for _, f := range []file{y, j} {
		got := &skyv1.Task{Argv: f.Argv, Env: f.Env, Image: f.Image, CpuMillis: f.CPUMillis, MemoryBytes: f.MemoryBytes, Accelerator: f.Accelerator, AcceleratorCount: f.AcceleratorCount, Target: f.Target, TimeoutSeconds: f.TimeoutSeconds, Setup: f.Setup, Outputs: f.Outputs}
		if f.Workdir != nil {
			got.Workdir = &skyv1.ArtifactRef{Id: f.Workdir.ID, Sha256: f.Workdir.SHA256, Size: f.Workdir.Size}
		}
		if !proto.Equal(got, task) {
			for i := range got.Argv {
				if i < len(task.Argv) && got.Argv[i] != task.Argv[i] {
					t.Errorf("argv[%d] differs", i)
				}
			}
			t.Fatalf("round trip differs")
		}
	}
}

// utf8Printable reports whether b holds only characters YAML reads raw.
func utf8Printable(b []byte) bool {
	for _, r := range string(b) {
		if !(r == '\n' || r >= 0x20 && r <= 0x7e || r >= 0xa0 && r <= 0xd7ff || r >= 0xe000 && r <= 0xfffd && r != 0xfeff || r >= 0x10000 && r <= 0x10ffff) || r == 0x2028 || r == 0x2029 {
			return false
		}
	}
	return true
}

func TestMarshalRefuses(t *testing.T) {
	tests := []struct {
		task *skyv1.Task
		want string
	}{
		{nil, "tasklower: no task"},
		{&skyv1.Task{Argv: []string{"x"}, Directory: "/tmp"}, "tasklower: task field directory has no import encoding"},
		{&skyv1.Task{Argv: []string{"x"}, Retry: &skyv1.RetryPolicy{MaxAttempts: 2}}, "tasklower: task field retry has no import encoding"},
		{&skyv1.Task{Argv: []string{"x"}, CpuLimitMillis: 1}, "tasklower: task field cpu_limit_millis has no import encoding"},
		{&skyv1.Task{Argv: []string{"x"}, NodeSelector: map[string]string{"a": "b"}}, "tasklower: task field node_selector has no import encoding"},
		{&skyv1.Task{Argv: []string{"\xff"}}, "tasklower: task text is not UTF-8"},
		{&skyv1.Task{Argv: []string{strings.Repeat("x", 1<<20)}}, "tasklower: task file exceeds 1 MiB"},
	}
	for _, tt := range tests {
		if _, err := Marshal(tt.task); err == nil || err.Error() != tt.want {
			t.Errorf("Marshal(%v) = %v, want %q", tt.task, err, tt.want)
		}
	}
}

// fixtureManifest is testdata/import/manifest.json.
type fixtureManifest struct {
	Fixtures []struct {
		Path, SHA256, Expect, Want, Notes, Error string
		WantSHA256                               string `json:"want_sha256"`
		NotesSHA256                              string `json:"notes_sha256"`
		Options                                  struct {
			Shell, Target  string
			WorkdirRef     string `json:"workdir_ref"`
			TimeoutSeconds int64  `json:"timeout_seconds"`
			Outputs        []string
			AllowDrop      []string `json:"allow_drop"`
		}
	}
	WorkdirRef struct{ Path, SHA256 string } `json:"workdir_ref"`
	Seeds      []struct{ Path, SHA256 string }
}

func readFixture(t *testing.T, name, sum string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "import", filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	if got := sha256.Sum256(b); hex.EncodeToString(got[:]) != sum {
		t.Fatalf("%s: sha256 %x differs from manifest", name, got)
	}
	return b
}

// TestFixtures checks the immutable import corpus against its manifest.
func TestFixtures(t *testing.T) {
	var m fixtureManifest
	if err := json.Unmarshal(readFixture(t, "manifest.json", manifestSHA256), &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Fixtures) == 0 {
		t.Fatal("empty manifest")
	}
	readFixture(t, m.WorkdirRef.Path, m.WorkdirRef.SHA256)
	for _, s := range m.Seeds {
		readFixture(t, s.Path, s.SHA256)
	}
	for _, f := range m.Fixtures {
		t.Run(f.Path, func(t *testing.T) {
			d, err := taskyaml.Parse(bytes.NewReader(readFixture(t, f.Path, f.SHA256)), taskyaml.Options{})
			opts := Options{Shell: f.Options.Shell, Target: f.Options.Target, TimeoutSeconds: f.Options.TimeoutSeconds, Outputs: f.Options.Outputs, AllowDrop: f.Options.AllowDrop}
			if f.Options.WorkdirRef != "" {
				opts.Workdir = ref
			}
			var task *skyv1.Task
			var notes []Note
			if err == nil {
				task, notes, err = Lower(d, opts)
			}
			var got []byte
			if err == nil {
				got, err = Marshal(task)
			}
			switch f.Expect {
			case "refuse":
				if err == nil || err.Error() != f.Error {
					t.Fatalf("got %v, want %q", err, f.Error)
				}
			case "lower":
				if err != nil {
					t.Fatal(err)
				}
				if want := readFixture(t, f.Want, f.WantSHA256); !bytes.Equal(got, want) {
					t.Fatalf("task file:\n%s\nwant:\n%s", got, want)
				}
				var b bytes.Buffer
				for _, n := range notes {
					fmt.Fprintf(&b, "%s\t%s\t%s\n", n.Path, n.Status, n.Detail)
				}
				if want := readFixture(t, f.Notes, f.NotesSHA256); !bytes.Equal(b.Bytes(), want) {
					t.Fatalf("notes:\n%s\nwant:\n%s", b.Bytes(), want)
				}
			default:
				t.Fatalf("unknown expectation %q", f.Expect)
			}
		})
	}
}

// manifestSHA256 pins the manifest itself, so a fixture and its manifest
// row cannot change together unnoticed.
const manifestSHA256 = "30e9ffdd919a7a0683d14493bd3ff8bc9f4b43c33a7607d703a772602487b456"

// errorText matches every error Lower may return. Paths and hints come
// from fixed lists, so a source or option value cannot appear.
var errorText = regexp.MustCompile(`^(task yaml: (invalid|unsupported|limit)( (` + knownPaths() + `))? at \d+:\d+(: (requires a shell|requires run text|requires workdir|requires a workdir reference|environment substitution|requires explicit drop|requires whole millicpus|requires whole bytes|requires a whole count|select a skygo target|requires docker:IMAGE))?|tasklower: [a-z0-9 ._-]+)$`)

func knownPaths() string {
	var all []string
	for _, p := range append(append([]string{"envs", "header", "resources"}, paths...), nullOnly...) {
		all = append(all, regexp.QuoteMeta(p))
	}
	return strings.Join(all, "|")
}

// FuzzLower checks that Lower never panics, returns a task file skygo can
// read, and never places input text in an error.
func FuzzLower(f *testing.F) {
	for _, s := range []string{
		"run: echo hi\n",
		"name: s3cr3t\nrun: x\n",
		"workdir: s3cr3t$\nrun: x\n",
		"run: x\nresources: {cpus: 0.0001, memory: 3PB, accelerators: 's3cr3t:0.5', image_id: s3cr3t, cloud: s3cr3t}\n",
		"run: x\nenvs: {S3CR3T: s3cr3t}\nnum_nodes: 7\nfile_mounts: {/s3cr3t: s3cr3t}\n",
		"setup: s3cr3t\nrun: x\n",
	} {
		f.Add(s, "/bin/sh", true, true)
	}
	f.Fuzz(func(t *testing.T, text, shell string, workdir, drop bool) {
		d, err := taskyaml.Parse(strings.NewReader(text), taskyaml.Options{})
		if err != nil {
			return
		}
		opts := Options{Shell: shell}
		if workdir {
			opts.Workdir = ref
		}
		if drop {
			opts.AllowDrop = []string{"name", "_user_specified_yaml"}
		}
		task, _, err := Lower(d, opts)
		if err != nil {
			if !errorText.MatchString(err.Error()) {
				t.Fatalf("error %q is not fixed text", err)
			}
			return
		}
		if _, err := Marshal(task); err != nil && err.Error() != "tasklower: task file exceeds 1 MiB" {
			t.Fatalf("Marshal: %v", err)
		}
	})
}
