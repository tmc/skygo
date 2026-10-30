package tasklower

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
)

const workerCPU = "resources: {cpus: 0.25, memory: 1KB}\n"

func workerDocument(t *testing.T, text string) *taskyaml.Document {
	t.Helper()
	d, err := taskyaml.Parse(strings.NewReader(text), taskyaml.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func workerFieldError(t *testing.T, d *taskyaml.Document, path string) *taskyaml.Error {
	t.Helper()
	task, spec, notes, err := LowerWorkers(d, Options{Shell: "/bin/sh"}, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if err == nil || task != nil || spec != nil || notes != nil {
		t.Fatalf("refusal returned task=%v spec=%v notes=%v error=%v", task, spec, notes, err)
	}
	var field *taskyaml.Error
	if !errors.As(err, &field) || field.Path != path {
		t.Fatalf("error=%v, want typed field %q", err, path)
	}
	pos := d.Locations[path]
	if field.Line != pos.Line || field.Column != pos.Column {
		t.Fatalf("position=%d:%d, want %d:%d", field.Line, field.Column, pos.Line, pos.Column)
	}
	if strings.Contains(err.Error(), "private-") {
		t.Fatalf("input leaked in error: %v", err)
	}
	return field
}

func TestLowerWorkersSingleMatchesLower(t *testing.T) {
	sh := Options{Shell: "/bin/sh"}
	for _, tc := range []struct {
		name, text string
		opts       Options
	}{
		{"absent", "run: echo one\n", sh},
		{"null", "num_nodes: null\nrun: echo one\n", sh},
		{"explicit", "num_nodes: 1\nrun: echo one\n", sh},
		{"empty", "num_nodes: 1\nrun: ''\n", sh},
		{"gpu", "num_nodes: 1\nrun: x\nresources: {accelerators: 'L4:2'}\n", sh},
		{"tpu", "num_nodes: 1\nrun: x\nresources: {accelerators: 'tpu-v5p:4'}\n", sh},
		{"workspace", "num_nodes: 1\nworkdir: .\nsetup: make\nrun: x\n", Options{Shell: "/bin/sh", Workdir: ref, Outputs: []string{"out"}}},
		{"missing ref", "num_nodes: 1\nworkdir: .\nrun: x\n", sh},
		{"missing shell", "num_nodes: 1\nrun: x\n", Options{}},
		{"invalid shell", "num_nodes: 1\nrun: x\n", Options{Shell: "private-shell"}},
		{"outputs conflict", "num_nodes: 1\nrun: x\n", Options{Shell: "/bin/sh", Outputs: []string{"out"}}},
		{"cloud", "num_nodes: 1\nresources: {cloud: private-cloud}\nrun: x\n", sh},
		{"drop", "name: private-name\nnum_nodes: 1\nrun: x\n", Options{Shell: "/bin/sh", AllowDrop: []string{"name"}}},
		{"metadata refusal", "name: private-name\nnum_nodes: 1\nrun: x\n", sh},
		{"runtime", "num_nodes: 1\nenvs: {SKYPILOT_NUM_NODES: '7', SKYPILOT_NODE_RANK: '9', SKYPILOT_NUM_GPUS_PER_NODE: '8'}\nrun: x\n", sh},
		{"null placeholders", "num_nodes: 1\nsetup: null\nworkdir: null\nservice: null\nrun: x\n", sh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := workerDocument(t, tc.text)
			want, wantNotes, wantErr := Lower(d, tc.opts)
			got, spec, notes, err := LowerWorkers(d, tc.opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
			if spec != nil || !proto.Equal(got, want) || !reflect.DeepEqual(notes, wantNotes) {
				t.Fatalf("single worker differs: %v %v %v", got, spec, notes)
			}
			if (err == nil) != (wantErr == nil) || reflect.TypeOf(err) != reflect.TypeOf(wantErr) || err != nil && err.Error() != wantErr.Error() {
				t.Fatalf("error=%v, Lower error=%v", err, wantErr)
			}
		})
	}
	want, wantNotes, wantErr := Lower(nil, sh)
	got, spec, notes, err := LowerWorkers(nil, sh, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if !proto.Equal(got, want) || spec != nil || !reflect.DeepEqual(notes, wantNotes) || err == nil || wantErr == nil || err.Error() != wantErr.Error() {
		t.Fatalf("nil document differs: %v %v", err, wantErr)
	}
}

func TestLowerWorkersGroups(t *testing.T) {
	for _, mode := range []skyv1.WorkerMode{skyv1.WorkerMode_WORKER_MODE_INDEPENDENT, skyv1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS} {
		for _, count := range []int{2, 16} {
			t.Run(fmt.Sprintf("%d/%d", mode, count), func(t *testing.T) {
				d := workerDocument(t, fmt.Sprintf("num_nodes: %d\nrun: echo ranks\n%s", count, workerCPU))
				task, spec, notes, err := LowerWorkers(d, Options{Shell: "/bin/sh", Target: "cpu"}, mode)
				if err != nil {
					t.Fatal(err)
				}
				if task == nil || spec == nil || spec.Count != uint32(count) || spec.Mode != mode || task.CpuMillis != 250 || task.MemoryBytes != 1024 || task.Target != "cpu" || !slices.Equal(task.Argv, []string{"/bin/sh", "-c", "echo ranks"}) {
					t.Fatalf("common task/spec: %v %v", task, spec)
				}
				if task.Env["SKYPILOT_NUM_NODES"] != fmt.Sprint(count) || task.Env["SKYPILOT_NUM_GPUS_PER_NODE"] != "0" {
					t.Fatalf("environment=%v", task.Env)
				}
				if _, ok := task.Env["SKYPILOT_NODE_RANK"]; ok {
					t.Fatal("common task invented a rank")
				}
				if task.Accelerator != "" || task.AcceleratorCount != 0 || task.Workdir != nil || len(task.Outputs) != 0 || len(task.Setup) != 0 || task.Retry != nil {
					t.Fatal("CPU group acquired unsupported fields")
				}
				found := false
				for _, note := range notes {
					if note.Path == "num_nodes" {
						found = note.Status == Lowered
					}
				}
				if !found {
					t.Fatal("num_nodes lacks Lowered note")
				}
				if task.ProtoReflect().Descriptor().Fields().ByName("workers") != nil {
					t.Fatal("worker spec embedded in Task")
				}
				data, err := Marshal(task)
				if err != nil || bytes.Contains(data, []byte(`"workers"`)) {
					t.Fatalf("Task-only Marshal: %s %v", data, err)
				}
			})
		}
	}
}

func TestLowerWorkersEnvironment(t *testing.T) {
	for _, nodes := range []string{"7", ""} {
		d := workerDocument(t, "num_nodes: 2\nrun: x\n"+workerCPU+"envs: {SKYPILOT_NUM_NODES: '"+nodes+"', SKYPILOT_NODE_RANK: private-rank, SKYPILOT_NUM_GPUS_PER_NODE: '99', KEEP: private-value}\n")
		task, _, _, err := LowerWorkers(d, Options{Shell: "/bin/sh"}, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
		if err != nil {
			t.Fatal(err)
		}
		if task.Env["SKYPILOT_NUM_NODES"] != nodes || task.Env["SKYPILOT_NUM_GPUS_PER_NODE"] != "0" || task.Env["KEEP"] != "private-value" {
			t.Fatalf("environment=%v", task.Env)
		}
		if _, ok := task.Env["SKYPILOT_NODE_RANK"]; ok {
			t.Fatal("user rank escaped into common task")
		}
		if d.Task.Envs["SKYPILOT_NODE_RANK"] != "private-rank" || d.Task.Envs["SKYPILOT_NUM_GPUS_PER_NODE"] != "99" {
			t.Fatal("input environment changed")
		}
	}
}

func TestLowerWorkersModesAndCounts(t *testing.T) {
	sh := Options{Shell: "/bin/sh"}
	d := workerDocument(t, "num_nodes: 2\nrun: private-run\n"+workerCPU)
	for _, mode := range []skyv1.WorkerMode{skyv1.WorkerMode_WORKER_MODE_UNSPECIFIED, skyv1.WorkerMode(-1), skyv1.WorkerMode(99)} {
		task, spec, notes, err := LowerWorkers(d, sh, mode)
		if err == nil || task != nil || spec != nil || notes != nil || strings.Contains(err.Error(), "private-run") {
			t.Fatalf("mode refusal: %v %v %v %v", task, spec, notes, err)
		}
	}
	counts := []int{0, -1, 17, math.MaxInt32}
	var wrapsToTwo uint64 = 1<<32 + 2
	if strconv.IntSize == 64 {
		counts = append(counts, int(wrapsToTwo))
	}
	for _, count := range counts {
		d := workerDocument(t, "num_nodes: 2\nrun: x\n"+workerCPU)
		d.Task.NumNodes = count
		field := workerFieldError(t, d, "num_nodes")
		if count < 1 && field.Code != taskyaml.Invalid {
			t.Fatalf("count %d code=%s", count, field.Code)
		}
	}
	one := workerDocument(t, "run: x\nnum_nodes: 1\n")
	task, spec, notes, err := LowerWorkers(one, sh, skyv1.WorkerMode_WORKER_MODE_TCP_RENDEZVOUS)
	var field *taskyaml.Error
	if err == nil || task != nil || spec != nil || notes != nil || !errors.As(err, &field) || field.Path != "num_nodes" || field.Line != 2 || field.Column != 12 {
		t.Fatalf("TCP1 refusal: %v", err)
	}
	if task, notes, err := Lower(d, sh); err == nil || task != nil || notes != nil {
		t.Fatalf("Lower accepted group: %v %v", task, err)
	}
}

func TestLowerWorkersParserCounts(t *testing.T) {
	for _, value := range []string{"0", "-1", "1.5", "4294967298", "999999999999999999999999", "'2'"} {
		d, err := taskyaml.Parse(strings.NewReader("run: private-run\nnum_nodes: "+value+"\n"), taskyaml.Options{})
		var field *taskyaml.Error
		if d != nil || !errors.As(err, &field) || field.Code != taskyaml.Invalid || field.Path != "num_nodes" || field.Line != 2 || field.Column != 12 {
			t.Fatalf("count %q: %v %v", value, d, err)
		}
	}
}

func TestLowerWorkersPositionedRefusals(t *testing.T) {
	for _, tc := range []struct{ name, text, path string }{
		{"setup first", "setup: private-setup\nnum_nodes: 17\nrun: x\n" + workerCPU, "setup"},
		{"count first", "num_nodes: 17\nsetup: private-setup\nrun: x\n" + workerCPU, "num_nodes"},
		{"cloud first", "resources: {cloud: private-cloud, cpus: 1, memory: 1}\nnum_nodes: 17\nrun: x\n", "resources.cloud"},
		{"workdir", "num_nodes: 2\nworkdir: private-directory\nrun: x\n" + workerCPU, "workdir"},
		{"empty setup", "num_nodes: 2\nsetup: ''\nrun: x\n" + workerCPU, "setup"},
		{"gpu", "num_nodes: 2\nrun: x\nresources: {cpus: 1, memory: 1, accelerators: 'L4:2'}\n", "resources.accelerators"},
		{"tpu", "num_nodes: 2\nrun: x\nresources: {cpus: 1, memory: 1, accelerators: 'tpu-v5p:4'}\n", "resources.accelerators"},
		{"mount", "num_nodes: 2\nrun: x\n" + workerCPU + "file_mounts: {/private/path: private-source}\n", "file_mounts"},
		{"volume", "num_nodes: 2\nrun: x\n" + workerCPU + "volumes: {/private/path: private-volume}\n", "volumes"},
		{"recovery", "num_nodes: 2\nrun: x\nresources: {cpus: 1, memory: 1, job_recovery: FAILOVER}\n", "resources.job_recovery"},
		{"autostop", "num_nodes: 2\nrun: x\nresources: {cpus: 1, memory: 1, autostop: false}\n", "resources.autostop"},
		{"spot", "num_nodes: 2\nrun: x\nresources: {cpus: 1, memory: 1, use_spot: true}\n", "resources.use_spot"},
		{"missing resources", "num_nodes: 2\nrun: x\n", "resources.cpus"},
		{"missing memory", "num_nodes: 2\nrun: x\nresources: {cpus: 1}\n", "resources.memory"},
		{"fraction cpu", "num_nodes: 2\nrun: x\nresources: {cpus: 0.0005, memory: 1}\n", "resources.cpus"},
	} {
		t.Run(tc.name, func(t *testing.T) { workerFieldError(t, workerDocument(t, tc.text), tc.path) })
	}
}

func TestLowerWorkersOptionsAndParserIsolation(t *testing.T) {
	d := workerDocument(t, "num_nodes: 2\nrun: private-run\n"+workerCPU)
	for _, opts := range []Options{{Shell: "/bin/sh", Workdir: ref}, {Shell: "/bin/sh", Outputs: []string{"private-output"}}, {Shell: "private-shell"}, {Shell: "/bin/sh", AllowDrop: []string{"private-drop"}}} {
		task, spec, notes, err := LowerWorkers(d, opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
		if err == nil || task != nil || spec != nil || notes != nil || strings.Contains(err.Error(), "private-") {
			t.Fatalf("option refusal: %v %v %v %v", task, spec, notes, err)
		}
	}
	for _, path := range []string{"service", "inputs", "outputs", "job", "config", "secrets"} {
		d, err := taskyaml.Parse(strings.NewReader("num_nodes: 2\nrun: x\n"+path+": {private-key: private-value}\n"+workerCPU), taskyaml.Options{})
		var field *taskyaml.Error
		if d != nil || !errors.As(err, &field) || field.Path != path || field.Line != 3 || strings.Contains(err.Error(), "private-") {
			t.Fatalf("%s mixing: %v", path, err)
		}
	}
	if d, err := taskyaml.Parse(strings.NewReader("run: x\n---\nrun: private-workflow\n"), taskyaml.Options{}); err == nil || d != nil {
		t.Fatalf("workflow accepted: %v %v", d, err)
	}
}

func TestLowerWorkersNullDefaults(t *testing.T) {
	d := workerDocument(t, "num_nodes: 2\nrun: x\nsetup: null\nworkdir: null\nenvs: null\nfile_mounts: {}\nvolumes: {}\nservice: null\ninputs: null\noutputs: null\nresources: {cpus: 1, memory: 1, accelerators: null, cloud: null, disk_size: 256, use_spot: false, autostop: null, job_recovery: null}\n")
	_, spec, notes, err := LowerWorkers(d, Options{Shell: "/bin/sh"}, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if err != nil || spec == nil || spec.Count != 2 {
		t.Fatalf("default placeholders: %v %v", spec, err)
	}
	for _, note := range notes {
		if (note.Path == "setup" || note.Path == "workdir" || note.Path == "service") && note.Status != Default {
			t.Fatalf("placeholder note=%v", note)
		}
	}
}

func TestLowerWorkersConstructedFields(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		edit       func(*taskyaml.Document)
	}{
		{"setup", "setup", func(d *taskyaml.Document) { s := "private-setup"; d.Task.Setup = &s }},
		{"workdir", "workdir", func(d *taskyaml.Document) { s := "private-workdir"; d.Task.Workdir = &s }},
		{"mount", "file_mounts", func(d *taskyaml.Document) { d.Task.FileMounts = map[string]string{"/private": "private-source"} }},
		{"volume", "volumes", func(d *taskyaml.Document) { d.Task.Volumes = map[string]string{"/private": "private-volume"} }},
		{"zero cpu", "resources.cpus", func(d *taskyaml.Document) { d.Task.Resources.CPUs = &taskyaml.Quantity{Value: "0"} }},
		{"zero memory", "resources.memory", func(d *taskyaml.Document) { d.Task.Resources.Memory = &taskyaml.Quantity{Value: "0"} }},
		{"recovery", "resources.job_recovery", func(d *taskyaml.Document) { d.Task.Resources.JobRecovery = &taskyaml.JobRecovery{} }},
		{"accelerator", "resources.accelerators", func(d *taskyaml.Document) {
			d.Task.Resources.Accelerators = []taskyaml.Accelerator{{Name: "private-gpu", Count: taskyaml.Quantity{Value: "1"}}}
		}},
		{"header", "header.name", func(d *taskyaml.Document) { s := "private-header"; d.DAGName = &s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := workerDocument(t, "num_nodes: 2\nrun: x\n"+workerCPU)
			d.Locations = nil
			tc.edit(d)
			workerFieldError(t, d, tc.path)
		})
	}
}

func TestLowerWorkersDetachedConcurrent(t *testing.T) {
	d := workerDocument(t, "name: private-name\nnum_nodes: 2\nenvs: {A: private-value}\nrun: x\nresources: {cpus: 1+, memory: 1KB, infra: k8s}\n")
	opts := Options{Shell: "/bin/sh", AllowDrop: []string{"name"}, InfraBinding: &InfraBinding{Infra: "k8s", Target: "cpu"}}
	snapshot := func(v any) []byte {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	beforeDoc, beforeOpts := snapshot(d), snapshot(opts)
	want, wantSpec, wantNotes, err := LowerWorkers(d, opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprint(wantNotes), "private-") {
		t.Fatal("notes contain input values")
	}
	canonical, err := Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task, spec, notes, err := LowerWorkers(d, opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
			if err != nil {
				failures <- err
				return
			}
			raw, err := Marshal(task)
			if err != nil || !bytes.Equal(raw, canonical) || !proto.Equal(spec, wantSpec) || !reflect.DeepEqual(notes, wantNotes) {
				failures <- fmt.Errorf("nondeterministic result: %v", err)
				return
			}
			task.Env["A"] = "changed"
			task.Argv[0] = "/changed"
			spec.Count = 16
			notes[0].Path = "changed"
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if !bytes.Equal(beforeDoc, snapshot(d)) || !bytes.Equal(beforeOpts, snapshot(opts)) || want.Env["A"] != "private-value" || wantSpec.Count != 2 || wantNotes[0].Path == "changed" {
		t.Fatal("shared input or output mutated")
	}
	paths := make([]string, len(wantNotes))
	for i, note := range wantNotes {
		paths[i] = note.Path
	}
	if !slices.Equal(paths, []string{"name", "num_nodes", "envs", "run", "resources", "resources.cpus", "resources.memory", "resources.infra"}) {
		t.Fatalf("notes not source ordered: %v", paths)
	}
}

func TestLowerWorkersRefusalKeepsInputs(t *testing.T) {
	d := workerDocument(t, "num_nodes: 2\nrun: private-run\n"+workerCPU+"envs: {A: private-value}\n")
	opts := Options{Shell: "/bin/sh", Workdir: proto.Clone(ref).(*skyv1.ArtifactRef), Outputs: []string{"private-output"}, AllowDrop: []string{"name"}, InfraBinding: &InfraBinding{Infra: "k8s", Target: "cpu"}}
	before, err := json.Marshal(struct {
		D *taskyaml.Document
		O Options
	}{d, opts})
	if err != nil {
		t.Fatal(err)
	}
	task, spec, notes, err := LowerWorkers(d, opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if err == nil || task != nil || spec != nil || notes != nil {
		t.Fatalf("workspace conflict: %v %v %v %v", task, spec, notes, err)
	}
	after, encodeErr := json.Marshal(struct {
		D *taskyaml.Document
		O Options
	}{d, opts})
	if encodeErr != nil || !bytes.Equal(before, after) {
		t.Fatal("refusal mutated document/options")
	}
}

func TestLowerWorkersSingleDetachedWorkspace(t *testing.T) {
	d := workerDocument(t, "num_nodes: 1\nworkdir: .\nrun: x\nenvs: {A: private-value}\n")
	opts := Options{Shell: "/bin/sh", Workdir: proto.Clone(ref).(*skyv1.ArtifactRef), Outputs: []string{"out"}}
	task, spec, _, err := LowerWorkers(d, opts, skyv1.WorkerMode_WORKER_MODE_INDEPENDENT)
	if err != nil || task == nil || task.Workdir == nil || spec != nil {
		t.Fatalf("single workspace: %v %v %v", task, spec, err)
	}
	task.Workdir.Id = "changed"
	task.Outputs[0] = "changed"
	task.Env["A"] = "changed"
	if opts.Workdir.Id != ref.Id || opts.Outputs[0] != "out" || d.Task.Envs["A"] != "private-value" {
		t.Fatal("single-worker outputs alias inputs")
	}
}

func TestLowerWorkersParserZeroResources(t *testing.T) {
	for _, field := range []string{"cpus", "memory"} {
		d, err := taskyaml.Parse(strings.NewReader("num_nodes: 2\nrun: x\nresources: {"+field+": 0}\n"), taskyaml.Options{})
		var e *taskyaml.Error
		if d != nil || !errors.As(err, &e) || e.Code != taskyaml.Invalid || e.Path != "resources."+field || e.Line != 3 {
			t.Fatalf("zero %s: %v %v", field, d, err)
		}
	}
}
