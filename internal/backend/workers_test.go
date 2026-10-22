package backend_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
)

const (
	workerJob     = "0123456789abcdef0123456789abcdef"
	workerAttempt = "fedcba9876543210fedcba9876543210"
)

var (
	workerPod     = backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-0", UID: "a3219392-688c-4e60-8ce1-680a8956a01f"}
	workerService = backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210", UID: "b3219392-688c-4e60-8ce1-680a8956a01f"}
)

func workerTask() backend.Task {
	return backend.Task{Argv: []string{"python", "train.py"}, Image: "trainer:v1", Env: map[string]string{"A": "1"}}
}

// workerGroup returns a valid group of count ranks in mode.
func workerGroup(count uint8, mode backend.WorkerMode) backend.WorkerGroup {
	task := workerTask()
	raw, err := backend.TaskBytes(task)
	if err != nil {
		panic(err)
	}
	return backend.WorkerGroup{
		Execution:  backend.Reference{ID: workerJob, Task: task, Binding: "sha256:binding"},
		AttemptID:  workerAttempt,
		TaskDigest: sha256.Sum256(raw),
		Spec:       backend.WorkerSpec{Count: count, Mode: mode},
	}
}

func TestNormalizeWorkers(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   *backend.WorkerSpec
		want *backend.WorkerSpec
	}{
		{"absent", nil, nil},
		{"one independent", &backend.WorkerSpec{Count: 1, Mode: backend.WorkerIndependent}, nil},
		{"two independent", &backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent}, &backend.WorkerSpec{Count: 2, Mode: backend.WorkerIndependent}},
		{"two tcp", &backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}, &backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}},
		{"sixteen independent", &backend.WorkerSpec{Count: 16, Mode: backend.WorkerIndependent}, &backend.WorkerSpec{Count: 16, Mode: backend.WorkerIndependent}},
		{"sixteen tcp", &backend.WorkerSpec{Count: 16, Mode: backend.WorkerTCP}, &backend.WorkerSpec{Count: 16, Mode: backend.WorkerTCP}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var before backend.WorkerSpec
			if tt.in != nil {
				before = *tt.in
			}
			got, err := backend.NormalizeWorkers(tt.in)
			if err != nil || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NormalizeWorkers = %+v, %v; want %+v", got, err, tt.want)
			}
			if tt.in == nil {
				return
			}
			if *tt.in != before {
				t.Fatalf("NormalizeWorkers changed its input to %+v", *tt.in)
			}
			if got != nil && got == tt.in {
				t.Fatal("NormalizeWorkers returned its input")
			}
			if got != nil {
				tt.in.Count, tt.in.Mode = 3, backend.WorkerTCP
				if *got != *tt.want {
					t.Fatalf("result aliases input: %+v", *got)
				}
			}
		})
	}
}

func TestNormalizeWorkersRefusals(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   backend.WorkerSpec
	}{
		{"zero", backend.WorkerSpec{}},
		{"zero count", backend.WorkerSpec{Mode: backend.WorkerIndependent}},
		{"zero count tcp", backend.WorkerSpec{Mode: backend.WorkerTCP}},
		{"seventeen", backend.WorkerSpec{Count: 17, Mode: backend.WorkerIndependent}},
		{"max uint8", backend.WorkerSpec{Count: math.MaxUint8, Mode: backend.WorkerTCP}},
		{"one tcp", backend.WorkerSpec{Count: 1, Mode: backend.WorkerTCP}},
		{"no mode", backend.WorkerSpec{Count: 2}},
		{"one no mode", backend.WorkerSpec{Count: 1}},
		{"unknown mode", backend.WorkerSpec{Count: 2, Mode: "mpi"}},
		{"mode case", backend.WorkerSpec{Count: 2, Mode: "TCP"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			in := tt.in
			if got, err := backend.NormalizeWorkers(&in); err == nil || got != nil {
				t.Fatalf("NormalizeWorkers = %+v, %v; want error", got, err)
			}
			if in != tt.in {
				t.Fatalf("NormalizeWorkers changed its input to %+v", in)
			}
		})
	}
}

func TestNewWorkerSpec(t *testing.T) {
	for _, tt := range []struct {
		count uint32
		mode  backend.WorkerMode
		want  *backend.WorkerSpec
		ok    bool
	}{
		{1, backend.WorkerIndependent, nil, true},
		{2, backend.WorkerTCP, &backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP}, true},
		{16, backend.WorkerIndependent, &backend.WorkerSpec{Count: 16, Mode: backend.WorkerIndependent}, true},
		{0, backend.WorkerIndependent, nil, false},
		{1, backend.WorkerTCP, nil, false},
		{17, backend.WorkerIndependent, nil, false},
		// Narrowing first would wrap these to 0, 1, 2 and 16.
		{256, backend.WorkerIndependent, nil, false},
		{257, backend.WorkerIndependent, nil, false},
		{258, backend.WorkerTCP, nil, false},
		{272, backend.WorkerTCP, nil, false},
		{math.MaxUint32, backend.WorkerTCP, nil, false},
		{2, "", nil, false},
	} {
		t.Run(fmt.Sprintf("%d %s", tt.count, tt.mode), func(t *testing.T) {
			got, err := backend.NewWorkerSpec(tt.count, tt.mode)
			if (err == nil) != tt.ok || !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("NewWorkerSpec = %+v, %v; want %+v, ok %v", got, err, tt.want, tt.ok)
			}
		})
	}
}

func TestExecutionUnits(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   *backend.WorkerSpec
		want int
	}{
		{"absent", nil, 1},
		{"one independent", &backend.WorkerSpec{Count: 1, Mode: backend.WorkerIndependent}, 1},
		{"three tcp", &backend.WorkerSpec{Count: 3, Mode: backend.WorkerTCP}, 3},
		{"sixteen independent", &backend.WorkerSpec{Count: 16, Mode: backend.WorkerIndependent}, 16},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := backend.ExecutionUnits(tt.in); err != nil || got != tt.want {
				t.Fatalf("ExecutionUnits = %d, %v; want %d", got, err, tt.want)
			}
		})
	}
	for _, in := range []backend.WorkerSpec{
		{}, {Mode: backend.WorkerIndependent}, {Count: 1, Mode: backend.WorkerTCP},
		{Count: 17, Mode: backend.WorkerIndependent}, {Count: 2}, {Count: 2, Mode: "mpi"},
	} {
		if got, err := backend.ExecutionUnits(&in); err == nil {
			t.Errorf("ExecutionUnits(%+v) = %d, want error", in, got)
		}
	}
}

func TestWorkerSpecJSON(t *testing.T) {
	data, err := json.Marshal(backend.WorkerSpec{Count: 2, Mode: backend.WorkerTCP})
	if err != nil || string(data) != `{"count":2,"mode":"tcp"}` {
		t.Fatalf("Marshal = %s, %v", data, err)
	}
	var w backend.WorkerSpec
	if err := json.Unmarshal([]byte(`{"count":258,"mode":"tcp"}`), &w); err == nil {
		t.Fatalf("Unmarshal accepted count 258 as %+v", w)
	}
}

// TestWorkersPreserveTask freezes the P0 boundary: Task has no worker field,
// existing TaskBytes are unchanged and Backend keeps its five methods.
func TestWorkersPreserveTask(t *testing.T) {
	if _, ok := reflect.TypeFor[backend.Task]().FieldByName("Workers"); ok {
		t.Fatal("Task has a Workers field")
	}
	if got := reflect.TypeFor[backend.Backend]().NumMethod(); got != 5 {
		t.Fatalf("Backend methods = %d, want 5", got)
	}
	if got := reflect.TypeFor[backend.WorkerBackend]().NumMethod(); got != 6 {
		t.Fatalf("WorkerBackend methods = %d, want 6", got)
	}
	for _, tt := range []struct {
		task   backend.Task
		want   string
		digest string
	}{
		{backend.Task{Argv: []string{"/bin/echo", "hi"}}, `{"argv":["/bin/echo","hi"]}`,
			"5540e55137330c64cb3f38acc0975b45aa3a4ddff23dbb3cb800ecb2701b8401"},
		{dataCompleteTask(), `{"argv":["run","arg"],"env":{"A":"1","B":"2"},"directory":"/work","image":"image:v1","cpu_millis":100,"memory_bytes":1024,"accelerator":"gpu","accelerator_count":1,"target":"remote-a","timeout_seconds":30,"cpu_limit_millis":200,"memory_limit_bytes":2048,"target_labels":{"location":"lab"},"node_selector":{"pool":"data"},"retry":{"max_attempts":2,"delay_seconds":1,"exit_codes":[7]},"workdir":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":1024},"setup":["prepare"],"outputs":["out"]}`,
			"49dd5086ecd643576332ae5cf4e38c630a7f2bbb526dab345365b8e07376fe80"},
	} {
		got, err := backend.TaskBytes(tt.task)
		if err != nil || string(got) != tt.want || fmt.Sprintf("%x", sha256.Sum256(got)) != tt.digest {
			t.Errorf("TaskBytes = %s, %v; want %s", got, err, tt.want)
		}
	}
}

func TestValidateWorkerReference(t *testing.T) {
	member := backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-1", UID: "c3219392-688c-4e60-8ce1-680a8956a01f"}
	for _, tt := range []struct {
		name    string
		r       backend.WorkerReference
		cleanup bool
		ok      bool
	}{
		{"independent rank 0", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Pod: workerPod}, false, true},
		{"independent last rank", backend.WorkerReference{Group: workerGroup(16, backend.WorkerIndependent), Rank: 15, Pod: member}, false, true},
		{"tcp rank 0", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Pod: workerPod, Service: workerService}, false, true},
		{"tcp rank 1", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Pod: member, Service: workerService}, false, true},
		{"tcp rank 1 service unknown", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Pod: member}, false, true},
		{"tcp rank 0 service only cleanup", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Service: workerService}, true, true},

		{"tcp rank 0 service only", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Service: workerService}, false, false},
		{"tcp rank 1 service only cleanup", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Service: workerService}, true, false},
		{"tcp rank 0 pod without service", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Pod: workerPod}, false, false},
		{"independent service", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Pod: workerPod, Service: workerService}, false, false},
		{"missing pod", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent)}, true, false},
		{"pod name only", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Pod: backend.WorkerResource{Name: workerPod.Name}}, false, false},
		{"pod uid only", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Pod: backend.WorkerResource{UID: workerPod.UID}}, false, false},
		{"shared uid", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Pod: workerPod, Service: backend.WorkerResource{Name: workerService.Name, UID: workerPod.UID}}, false, false},
		{"rank equals count", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Rank: 2, Pod: member}, false, false},
		{"one worker", backend.WorkerReference{Group: workerGroup(1, backend.WorkerIndependent), Pod: workerPod}, false, false},
		{"unnormalized spec", backend.WorkerReference{Group: workerGroup(17, backend.WorkerIndependent), Pod: workerPod}, false, false},
		{"no mode", backend.WorkerReference{Group: workerGroup(2, ""), Pod: workerPod}, false, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerReference(tt.r, tt.cleanup); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerReference = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

// TestValidateWorkerGroup applies one change to a valid group. Rows that
// change the task's cohort also recompute its digest, so that only the cohort
// rule can refuse them.
func TestValidateWorkerGroup(t *testing.T) {
	redigest := func(g *backend.WorkerGroup) {
		raw, err := backend.TaskBytes(g.Execution.Task)
		if err != nil {
			panic(err)
		}
		g.TaskDigest = sha256.Sum256(raw)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*backend.WorkerGroup)
	}{
		{"job id", func(g *backend.WorkerGroup) { g.Execution.ID = "" }},
		{"attempt id", func(g *backend.WorkerGroup) { g.AttemptID = "" }},
		{"attempt control byte", func(g *backend.WorkerGroup) { g.AttemptID = "a\n" }},
		{"binding", func(g *backend.WorkerGroup) { g.Execution.Binding = "" }},
		{"long binding", func(g *backend.WorkerGroup) { g.Execution.Binding = strings.Repeat("b", 257) }},
		{"serialized group instance", func(g *backend.WorkerGroup) { g.Execution.Instance = "pod-a,pod-b" }},
		{"digest", func(g *backend.WorkerGroup) { g.TaskDigest[0] ^= 1 }},
		{"task changed", func(g *backend.WorkerGroup) { g.Execution.Task.Env["A"] = "2" }},
		{"retry", func(g *backend.WorkerGroup) {
			g.Execution.Task.Retry = &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{1}}
			redigest(g)
		}},
		{"workdir", func(g *backend.WorkerGroup) {
			g.Execution.Task.Workdir = &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1}
			redigest(g)
		}},
		{"setup", func(g *backend.WorkerGroup) { g.Execution.Task.Setup = []string{"make"}; redigest(g) }},
		{"outputs", func(g *backend.WorkerGroup) { g.Execution.Task.Outputs = []string{"out"}; redigest(g) }},
		{"data mounts", func(g *backend.WorkerGroup) {
			g.Execution.Task.DataMounts = []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}
			redigest(g)
		}},
		{"binding control byte", func(g *backend.WorkerGroup) { g.Execution.Binding = "sha256:\x7f" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := workerGroup(2, backend.WorkerIndependent)
			if err := backend.ValidateWorkerReference(backend.WorkerReference{Group: g, Pod: workerPod}, false); err != nil {
				t.Fatalf("control: %v", err)
			}
			tt.mutate(&g)
			if err := backend.ValidateWorkerReference(backend.WorkerReference{Group: g, Pod: workerPod}, false); err == nil {
				t.Fatal("ValidateWorkerReference accepted the group")
			}
		})
	}
}

func TestValidateWorkerCreated(t *testing.T) {
	tcp0 := backend.WorkerCreate{Group: workerGroup(3, backend.WorkerTCP)}
	tcp2 := backend.WorkerCreate{Group: workerGroup(3, backend.WorkerTCP), Rank: 2}
	ind1 := backend.WorkerCreate{Group: workerGroup(3, backend.WorkerIndependent), Rank: 1}
	for _, tt := range []struct {
		name    string
		c       backend.WorkerCreate
		got     backend.WorkerCreated
		partial bool
		ok      bool
	}{
		{"tcp rank 0 complete", tcp0, backend.WorkerCreated{Pod: workerPod, Service: workerService}, false, true},
		{"tcp rank 2 complete", tcp2, backend.WorkerCreated{Pod: workerPod}, false, true},
		{"independent complete", ind1, backend.WorkerCreated{Pod: workerPod}, false, true},
		{"tcp rank 0 service only partial", tcp0, backend.WorkerCreated{Service: workerService}, true, true},
		{"nothing partial", tcp0, backend.WorkerCreated{}, true, true},
		{"rank 2 nothing partial", tcp2, backend.WorkerCreated{}, true, true},

		{"tcp rank 0 service only", tcp0, backend.WorkerCreated{Service: workerService}, false, false},
		{"tcp rank 0 pod only", tcp0, backend.WorkerCreated{Pod: workerPod}, true, false},
		{"nothing", ind1, backend.WorkerCreated{}, false, false},
		{"tcp rank 2 service", tcp2, backend.WorkerCreated{Pod: workerPod, Service: workerService}, false, false},
		{"tcp rank 2 service partial", tcp2, backend.WorkerCreated{Service: workerService}, true, false},
		{"independent service", ind1, backend.WorkerCreated{Pod: workerPod, Service: workerService}, false, false},
		{"invented uid", ind1, backend.WorkerCreated{Pod: backend.WorkerResource{Name: workerPod.Name, UID: "\x00"}}, false, false},
		{"rank out of range", backend.WorkerCreate{Group: workerGroup(3, backend.WorkerIndependent), Rank: 3}, backend.WorkerCreated{Pod: workerPod}, false, false},
		{"rank out of range partial", backend.WorkerCreate{Group: workerGroup(3, backend.WorkerIndependent), Rank: 3}, backend.WorkerCreated{}, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerCreated(tt.c, tt.got, tt.partial); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerCreated = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

func TestValidateWorkerRelease(t *testing.T) {
	member := backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-1", UID: "c3219392-688c-4e60-8ce1-680a8956a01f"}
	for _, tt := range []struct {
		name string
		r    backend.WorkerReference
		ok   bool
	}{
		{"independent", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent), Pod: workerPod}, true},
		{"tcp rank 0", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Pod: workerPod, Service: workerService}, true},
		{"tcp rank 1", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Pod: member, Service: workerService}, true},
		{"tcp rank 1 service unknown", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Pod: member}, false},
		{"tcp rank 0 service only", backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Service: workerService}, false},
		{"missing pod", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerRelease(tt.r); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerRelease = %v, want ok %v", err, tt.ok)
			}
		})
	}
}

func TestValidateWorkerRemoval(t *testing.T) {
	member := backend.WorkerResource{Name: "skygo-fedcba9876543210fedcba9876543210-1", UID: "c3219392-688c-4e60-8ce1-680a8956a01f"}
	tcp0 := backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Pod: workerPod, Service: workerService}
	tcp0Service := backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Service: workerService}
	tcp1 := backend.WorkerReference{Group: workerGroup(2, backend.WorkerTCP), Rank: 1, Pod: member, Service: workerService}
	for _, tt := range []struct {
		name string
		r    backend.WorkerReference
		got  backend.WorkerRemoval
		ok   bool
	}{
		{"tcp rank 0 both", tcp0, backend.WorkerRemoval{Pod: workerPod, Service: workerService, PodRemoved: true, ServiceRemoved: true}, true},
		{"tcp rank 0 pod only removed", tcp0, backend.WorkerRemoval{Pod: workerPod, Service: workerService, PodRemoved: true}, true},
		{"tcp rank 0 nothing removed", tcp0, backend.WorkerRemoval{Pod: workerPod, Service: workerService}, true},
		{"tcp rank 0 service only", tcp0Service, backend.WorkerRemoval{Service: workerService, ServiceRemoved: true}, true},
		{"tcp rank 1", tcp1, backend.WorkerRemoval{Pod: member, PodRemoved: true}, true},

		{"tcp rank 1 echoes service", tcp1, backend.WorkerRemoval{Pod: member, Service: workerService, PodRemoved: true}, false},
		{"tcp rank 1 removes service", tcp1, backend.WorkerRemoval{Pod: member, PodRemoved: true, ServiceRemoved: true}, false},
		{"tcp rank 0 unknown pod removed", tcp0Service, backend.WorkerRemoval{Service: workerService, PodRemoved: true}, false},
		{"tcp rank 0 service echo missing", tcp0, backend.WorkerRemoval{Pod: workerPod, PodRemoved: true}, false},
		{"pod echo differs", tcp1, backend.WorkerRemoval{Pod: workerPod, PodRemoved: true}, false},
		{"invalid reference", backend.WorkerReference{Group: workerGroup(2, backend.WorkerIndependent)}, backend.WorkerRemoval{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := backend.ValidateWorkerRemoval(tt.r, tt.got); (err == nil) != tt.ok {
				t.Fatalf("ValidateWorkerRemoval = %v, want ok %v", err, tt.ok)
			}
		})
	}
}
