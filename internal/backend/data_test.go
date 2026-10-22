package backend_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/backend"
	"gopkg.in/yaml.v3"
)

func dataCompleteTask() backend.Task {
	return backend.Task{
		Argv: []string{"run", "arg"}, Env: map[string]string{"B": "2", "A": "1"}, Directory: "/work", Image: "image:v1",
		CPUMillis: 100, MemoryBytes: 1024, Accelerator: "gpu", AcceleratorCount: 1, Target: "remote-a", TimeoutSeconds: 30,
		CPULimitMillis: 200, MemoryLimitBytes: 2048, TargetLabels: map[string]string{"location": "lab"}, NodeSelector: map[string]string{"pool": "data"},
		Retry:   &backend.RetryPolicy{MaxAttempts: 2, DelaySeconds: 1, ExitCodes: []int32{7}},
		Workdir: &artifact.Ref{ID: strings.Repeat("a", 32), SHA256: strings.Repeat("b", 64), Size: 1024}, Setup: []string{"prepare"}, Outputs: []string{"out"},
	}
}

func TestDataBackendRemainsOptional(t *testing.T) {
	if got := reflect.TypeFor[backend.Backend]().NumMethod(); got != 5 {
		t.Fatalf("legacy Backend methods=%d, want 5", got)
	}
	remote := reflect.TypeFor[backend.DataBackend]()
	if remote.NumMethod() != 3 {
		t.Fatalf("optional DataBackend methods=%d, want 3", remote.NumMethod())
	}
	for _, name := range []string{"StartData", "ObserveData", "RemoveData"} {
		if _, ok := remote.MethodByName(name); !ok {
			t.Errorf("missing optional method %s", name)
		}
	}
}

func TestDataMountsPreserveCompleteTaskGolden(t *testing.T) {
	// This literal freezes every predecessor field, independently of Task's
	// current reflection shape. TaskBytes deliberately does not validate the
	// otherwise incompatible Directory+Workdir profile used to fill all fields.
	const want = `{"argv":["run","arg"],"env":{"A":"1","B":"2"},"directory":"/work","image":"image:v1","cpu_millis":100,"memory_bytes":1024,"accelerator":"gpu","accelerator_count":1,"target":"remote-a","timeout_seconds":30,"cpu_limit_millis":200,"memory_limit_bytes":2048,"target_labels":{"location":"lab"},"node_selector":{"pool":"data"},"retry":{"max_attempts":2,"delay_seconds":1,"exit_codes":[7]},"workdir":{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":1024},"setup":["prepare"],"outputs":["out"]}`
	const digest = "49dd5086ecd643576332ae5cf4e38c630a7f2bbb526dab345365b8e07376fe80"
	for _, mounts := range [][]backend.DataMount{nil, {}} {
		task := dataCompleteTask()
		task.DataMounts = mounts
		got, err := backend.TaskBytes(task)
		if err != nil || string(got) != want || fmt.Sprintf("%x", sha256.Sum256(got)) != digest {
			t.Fatalf("complete predecessor bytes/hash changed: %s, %v", got, err)
		}
	}
}

func TestDataMountCanonicalIdentity(t *testing.T) {
	for _, test := range []struct {
		name   string
		mounts []backend.DataMount
		want   string
	}{
		{"absent", nil, `{"argv":["run"]}`},
		{"empty", []backend.DataMount{}, `{"argv":["run"]}`},
		{"explicit false", []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}, `{"argv":["run"],"data_mounts":[{"reference":"cache","destination":"/mnt/skygo/cache","read_only":false}]}`},
		{"explicit true", []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache", ReadOnly: true}}, `{"argv":["run"],"data_mounts":[{"reference":"cache","destination":"/mnt/skygo/cache","read_only":true}]}`},
		{"preserve order", []backend.DataMount{{Reference: "z", Destination: "/mnt/skygo/z"}, {Reference: "a", Destination: "/mnt/skygo/a", ReadOnly: true}}, `{"argv":["run"],"data_mounts":[{"reference":"z","destination":"/mnt/skygo/z","read_only":false},{"reference":"a","destination":"/mnt/skygo/a","read_only":true}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := backend.TaskBytes(backend.Task{Argv: []string{"run"}, DataMounts: test.mounts})
			if err != nil || string(got) != test.want {
				t.Fatalf("canonical task=%s, %v; want %s", got, err, test.want)
			}
		})
	}
}

func TestDataMountJSONYAMLRoundTrip(t *testing.T) {
	task := dataCompleteTask()
	task.DataMounts = []backend.DataMount{{Reference: "z", Destination: "/mnt/skygo/z", ReadOnly: false}, {Reference: "a", Destination: "/mnt/skygo/a", ReadOnly: true}}
	for _, codec := range []struct {
		name      string
		marshal   func(any) ([]byte, error)
		unmarshal func([]byte, any) error
	}{{"json", json.Marshal, json.Unmarshal}, {"yaml", yaml.Marshal, yaml.Unmarshal}} {
		t.Run(codec.name, func(t *testing.T) {
			data, err := codec.marshal(task)
			if err != nil {
				t.Fatal(err)
			}
			var got backend.Task
			if err := codec.unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, task) {
				t.Fatalf("round trip changed accepted task: %+v", got)
			}
			got.DataMounts[0].Reference = "changed"
			if task.DataMounts[0].Reference != "z" {
				t.Fatal("decoded mounts share storage")
			}
		})
	}
}

func TestDataCloneIsolation(t *testing.T) {
	task := dataCompleteTask()
	task.DataMounts = []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache", ReadOnly: true}}
	before, err := backend.TaskBytes(task)
	if err != nil {
		t.Fatal(err)
	}
	copy := backend.CloneTask(task)
	if !reflect.DeepEqual(copy, task) {
		t.Fatal("clone changed accepted values")
	}
	copy.Argv[0] = "changed"
	copy.Env["A"] = "changed"
	copy.TargetLabels["location"] = "changed"
	copy.NodeSelector["pool"] = "changed"
	copy.Retry.ExitCodes[0] = 8
	copy.Retry.MaxAttempts = 3
	copy.Workdir.ID = strings.Repeat("c", 32)
	copy.Setup[0] = "changed"
	copy.Outputs[0] = "changed"
	copy.DataMounts[0].Reference = "changed"
	copy.DataMounts[0].Destination = "/mnt/skygo/changed"
	copy.DataMounts[0].ReadOnly = false
	after, err := backend.TaskBytes(task)
	if err != nil || string(after) != string(before) {
		t.Fatalf("clone aliases task: %s, %v", after, err)
	}
	if backend.CloneTask(backend.Task{}).DataMounts != nil || backend.CloneDataPolicy(nil) != nil {
		t.Fatal("clone changed nil defaults")
	}
	p := backend.DefaultDataPolicy()
	cloned := backend.CloneDataPolicy(&p)
	if cloned == &p || *cloned != p {
		t.Fatal("policy not independently copied")
	}
	cloned.StageSeconds++
	if p != backend.DefaultDataPolicy() {
		t.Fatal("policy copy aliases source")
	}
}

func TestDataPolicyExactBudgets(t *testing.T) {
	want := backend.DataPolicy{StageSeconds: 600, CollectionSeconds: 600, DefaultTaskSeconds: 86400, MaxTaskSeconds: 604800, ShutdownSeconds: 120}
	p := backend.DefaultDataPolicy()
	if p != want || p.Validate() != nil {
		t.Fatalf("default policy=%+v", p)
	}
	for _, test := range []struct{ timeout, task, total int64 }{{0, 86400, 87720}, {1, 1, 1321}, {604800, 604800, 606120}} {
		b, err := p.Budgets(test.timeout)
		if err != nil || b != (backend.DataBudgets{StageSeconds: 600, TaskSeconds: test.task, CollectionSeconds: 600, ShutdownSeconds: 120}) {
			t.Fatalf("timeout %d budgets=%+v, %v", test.timeout, b, err)
		}
		if total, err := b.TotalSeconds(); err != nil || total != test.total {
			t.Fatalf("total=%d, %v; want %d", total, err, test.total)
		}
	}
	for _, timeout := range []int64{-1, 604801, math.MaxInt64} {
		if _, err := p.Budgets(timeout); err == nil {
			t.Fatalf("accepted timeout %d", timeout)
		}
	}
	for _, edit := range []func(*backend.DataPolicy){
		func(p *backend.DataPolicy) { p.StageSeconds = 0 },
		func(p *backend.DataPolicy) { p.StageSeconds = 601 },
		func(p *backend.DataPolicy) { p.CollectionSeconds = 599 },
		func(p *backend.DataPolicy) { p.DefaultTaskSeconds = 86401 },
		func(p *backend.DataPolicy) { p.MaxTaskSeconds = 604801 },
		func(p *backend.DataPolicy) { p.ShutdownSeconds = -1 },
		func(p *backend.DataPolicy) { p.ShutdownSeconds = math.MaxInt64 },
	} {
		bad := p
		edit(&bad)
		if bad.Validate() == nil {
			t.Fatalf("accepted changed fixed policy %+v", bad)
		}
		if _, err := bad.Budgets(0); err == nil {
			t.Fatalf("budget derivation accepted changed policy %+v", bad)
		}
	}
}

func TestDataBudgetArithmeticRefusesOverflow(t *testing.T) {
	for _, b := range []backend.DataBudgets{
		{}, {StageSeconds: -1, TaskSeconds: 1, CollectionSeconds: 1, ShutdownSeconds: 1},
		{StageSeconds: 1, TaskSeconds: 0, CollectionSeconds: 1, ShutdownSeconds: 1},
		{StageSeconds: 1, TaskSeconds: 1, CollectionSeconds: 0, ShutdownSeconds: 1},
		{StageSeconds: 1, TaskSeconds: 1, CollectionSeconds: 1, ShutdownSeconds: 0},
		{StageSeconds: math.MaxInt64, TaskSeconds: 1, CollectionSeconds: 1, ShutdownSeconds: 1},
		{StageSeconds: 1, TaskSeconds: math.MaxInt64 / int64(time.Second), CollectionSeconds: 1, ShutdownSeconds: 1},
	} {
		if _, err := b.TotalSeconds(); err == nil {
			t.Fatalf("accepted invalid/overflowing budget %+v", b)
		}
	}
	limit := int64(math.MaxInt64 / int64(time.Second))
	b := backend.DataBudgets{StageSeconds: 1, TaskSeconds: limit - 3, CollectionSeconds: 1, ShutdownSeconds: 1}
	if got, err := b.TotalSeconds(); err != nil || got != limit {
		t.Fatalf("finite nanosecond boundary=%d, %v", got, err)
	}
}

func TestDataDeadlineBoundaries(t *testing.T) {
	start := time.Date(2026, 10, 20, 1, 2, 3, 4, time.FixedZone("offset", 3600))
	if got, err := backend.DataDeadline(start, 600); err != nil || !got.Equal(start.Add(600*time.Second)) {
		t.Fatalf("deadline=%v, %v", got, err)
	}
	for _, test := range []struct {
		start   time.Time
		seconds int64
	}{
		{time.Time{}, 1}, {start, 0}, {start, -1}, {start, math.MaxInt64},
		{time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC), 1},
		{time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), 1},
	} {
		if _, err := backend.DataDeadline(test.start, test.seconds); err == nil {
			t.Fatalf("accepted deadline outside finite time domain: %v + %d", test.start, test.seconds)
		}
	}
}

func TestValidateDataTaskMounts(t *testing.T) {
	base := backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
	protected := []string{"/workspace", "/skygo-helper", "/run/skygo/credentials"}
	for _, test := range []struct {
		name      string
		edit      func(*backend.Task)
		protected []string
		ok        bool
	}{
		{"mount only", func(*backend.Task) {}, protected, true},
		{"trusted nil protected list", func(*backend.Task) {}, nil, true},
		{"trusted explicit empty protected list", func(*backend.Task) {}, []string{}, true},
		{"same alias disjoint mounts", func(t *backend.Task) {
			t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "cache", Destination: "/mnt/skygo/other", ReadOnly: true})
		}, protected, true},
		{"directory disjoint", func(t *backend.Task) { t.Directory = "/mnt/skygo/cache-other" }, protected, true},
		{"directory protected equality", func(t *backend.Task) { t.Directory = "/mnt/skygo/cache" }, protected, false},
		{"directory protected child", func(t *backend.Task) { t.Directory = "/mnt/skygo/cache/work" }, protected, false},
		{"directory protected ancestor", func(t *backend.Task) { t.Directory = "/mnt/skygo" }, protected, false},
		{"directory relative", func(t *backend.Task) { t.Directory = "work" }, protected, false},
		{"protected equality", func(*backend.Task) {}, []string{"/mnt/skygo/cache"}, false},
		{"protected ancestor", func(*backend.Task) {}, []string{"/mnt/skygo"}, false},
		{"protected descendant", func(*backend.Task) {}, []string{"/mnt/skygo/cache/private"}, false},
		{"protected lexical neighbor", func(*backend.Task) {}, []string{"/mnt/skygo/cache-other"}, true},
		{"protected relative", func(*backend.Task) {}, []string{"workspace"}, false},
		{"protected noncanonical", func(*backend.Task) {}, []string{"/workspace/../private"}, false},
		{"alias63", func(t *backend.Task) { t.DataMounts[0].Reference = strings.Repeat("a", 63) }, protected, true},
		{"alias64", func(t *backend.Task) { t.DataMounts[0].Reference = strings.Repeat("a", 64) }, protected, false},
		{"path240", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/" + strings.Repeat("a", 229) }, protected, true},
		{"path241", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/" + strings.Repeat("a", 230) }, protected, false},
		{"depth16", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/" + strings.Repeat("a/", 13) + "a" }, protected, true},
		{"depth17", func(t *backend.Task) { t.DataMounts[0].Destination = "/mnt/skygo/" + strings.Repeat("a/", 14) + "a" }, protected, false},
		{"16 mounts", func(t *backend.Task) {
			t.DataMounts = nil
			for i := 0; i < 16; i++ {
				t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "cache", Destination: fmt.Sprintf("/mnt/skygo/d%d", i)})
			}
		}, protected, true},
		{"17 mounts", func(t *backend.Task) {
			t.DataMounts = nil
			for i := 0; i < 17; i++ {
				t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "cache", Destination: fmt.Sprintf("/mnt/skygo/d%d", i)})
			}
		}, protected, false},
		{"mount equality", func(t *backend.Task) { t.DataMounts = append(t.DataMounts, t.DataMounts[0]) }, protected, false},
		{"mount child", func(t *backend.Task) {
			t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "other", Destination: "/mnt/skygo/cache/child"})
		}, protected, false},
		{"mount parent", func(t *backend.Task) {
			t.DataMounts[0].Destination = "/mnt/skygo/cache/child"
			t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "other", Destination: "/mnt/skygo/cache"})
		}, protected, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := backend.CloneTask(base)
			test.edit(&task)
			before, err := backend.TaskBytes(task)
			if err != nil {
				t.Fatal(err)
			}
			paths := slices.Clone(test.protected)
			err = backend.ValidateDataTask(task, test.protected)
			if (err == nil) != test.ok {
				t.Fatalf("validation=%v; valid=%v", err, test.ok)
			}
			after, err := backend.TaskBytes(task)
			if err != nil || string(before) != string(after) || !reflect.DeepEqual(paths, test.protected) {
				t.Fatal("validation rewrote accepted values or trusted paths")
			}
		})
	}
	// Nil and empty trusted sets are caller-supplied shape inputs, never a
	// certificate that native protection is complete. Directory protection
	// remains mandatory even when the explicit protected set has no entries.
	for _, paths := range [][]string{nil, {}} {
		task := backend.CloneTask(base)
		task.Directory = "/mnt/skygo/cache"
		if backend.ValidateDataTask(task, paths) == nil {
			t.Fatal("empty protected set bypassed intrinsic Directory protection")
		}
	}
	for _, alias := range []string{"", "Cache", "-cache", "cache-", "cache.name", "cache_name", "c/cache", "é", "a\x00b"} {
		task := backend.CloneTask(base)
		task.DataMounts[0].Reference = alias
		if backend.ValidateDataTask(task, protected) == nil {
			t.Fatalf("accepted alias %q", alias)
		}
	}
	for _, path := range []string{"", "mnt/skygo/a", "/mnt/skygo", "/mnt/skygox/a", "/mnt/skygo/", "/mnt/skygo//a", "/mnt/skygo/./a", "/mnt/skygo/a/../b", "/mnt/skygo/a/", "/mnt/skygo/a\\b", "/mnt/skygo/a:b", "/mnt/skygo/a\x00b", "/mnt/skygo/*", "/mnt/skygo/a?", "/mnt/skygo/a[0]", "/mnt/skygo/" + string([]byte{255})} {
		task := backend.CloneTask(base)
		task.DataMounts[0].Destination = path
		if backend.ValidateDataTask(task, protected) == nil {
			t.Fatalf("accepted destination %q", path)
		}
	}
}

// An unresolved template gets the same mount syntax rules with no protected
// paths, and never the resolved Workdir requirement.
func TestValidateDataTemplate(t *testing.T) {
	base := backend.Task{Argv: []string{"run"}, Setup: []string{"prepare"}, Outputs: []string{"out"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
	if err := backend.ValidateDataTask(base, nil); err == nil {
		t.Fatal("unresolved template passed resolved data validation")
	}
	for _, test := range []struct {
		name string
		edit func(*backend.Task)
		ok   bool
	}{
		{"setup outputs mount", func(*backend.Task) {}, true},
		{"no mounts", func(t *backend.Task) { t.DataMounts = nil }, true},
		{"16 mounts", func(t *backend.Task) {
			t.DataMounts = nil
			for i := 0; i < 16; i++ {
				t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "cache", Destination: fmt.Sprintf("/mnt/skygo/d%d", i)})
			}
		}, true},
		{"17 mounts", func(t *backend.Task) {
			t.DataMounts = nil
			for i := 0; i < 17; i++ {
				t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "cache", Destination: fmt.Sprintf("/mnt/skygo/d%d", i)})
			}
		}, false},
		{"alias", func(t *backend.Task) { t.DataMounts[0].Reference = "BAD!" }, false},
		{"relative destination", func(t *backend.Task) { t.DataMounts[0].Destination = "relative" }, false},
		{"destination outside mount root", func(t *backend.Task) { t.DataMounts[0].Destination = "/data/cache" }, false},
		{"mount child", func(t *backend.Task) {
			t.DataMounts = append(t.DataMounts, backend.DataMount{Reference: "other", Destination: "/mnt/skygo/cache/child"})
		}, false},
		{"workdir", func(t *backend.Task) { t.Workdir = workspaceTask().Workdir }, false},
		{"directory", func(t *backend.Task) { t.Directory = "/mnt/skygo/other" }, false},
		{"setup", func(t *backend.Task) { t.Setup = []string{""} }, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := backend.CloneTask(base)
			test.edit(&task)
			before, err := backend.TaskBytes(task)
			if err != nil {
				t.Fatal(err)
			}
			if err = backend.ValidateDataTemplate(task); (err == nil) != test.ok {
				t.Fatalf("template validation=%v; valid=%v", err, test.ok)
			}
			after, err := backend.TaskBytes(task)
			if err != nil || string(before) != string(after) {
				t.Fatal("validation rewrote the template")
			}
		})
	}
}

func TestValidateDataTaskProfiles(t *testing.T) {
	for _, test := range []struct {
		name string
		task backend.Task
		ok   bool
	}{
		{"no data profile", backend.Task{Argv: []string{"run"}}, false},
		{"stage only", workspaceTask(), true},
		{"setup", func() backend.Task { t := workspaceTask(); t.Setup = []string{"prepare"}; return t }(), true},
		{"outputs", func() backend.Task { t := workspaceTask(); t.Outputs = []string{"out"}; return t }(), true},
		{"all staged", func() backend.Task {
			t := workspaceTask()
			t.Setup = []string{"prepare"}
			t.Outputs = []string{"out"}
			t.DataMounts = []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}
			return t
		}(), true},
		{"setup without workdir", backend.Task{Argv: []string{"run"}, Setup: []string{"prepare"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}, false},
		{"outputs without workdir", backend.Task{Argv: []string{"run"}, Outputs: []string{"out"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}, false},
		{"staged directory", func() backend.Task { t := workspaceTask(); t.Directory = "/work"; return t }(), false},
		{"bad workdir", func() backend.Task { t := workspaceTask(); t.Workdir.ID = "bad"; return t }(), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := backend.ValidateDataTask(test.task, []string{"/workspace", "/skygo-helper", "/run/skygo/credentials"}); (err == nil) != test.ok {
				t.Fatalf("profile validation=%v; valid=%v", err, test.ok)
			}
			if !test.ok {
				if backend.ValidateDataCreated(test.task, backend.DataCreated{}, true) == nil {
					t.Fatal("invalid profile gained partial receipt validity")
				}
				ref := backend.Reference{Task: test.task, Instance: "original-pod-uid"}
				o := backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: ref.Instance}, Phase: backend.DataHeld}
				if backend.ValidateDataObservation(ref, o) == nil {
					t.Fatal("invalid profile gained Held observation validity")
				}
			}
		})
	}
	// The explicit data validator does not alter the original local snapshot
	// validator's acceptance rules or make a legacy Submit route data-aware.
	if err := backend.ValidateWorkspace(backend.Task{Argv: []string{"run"}, Directory: "/work"}); err != nil {
		t.Fatal(err)
	}
}

// The receipt fixtures below are value/shape vectors only. They are not actual
// Pod/Secret Create returns, qualified runtime observations, or removal proofs.
func dataReceiptReference() backend.Reference {
	t := workspaceTask()
	t.Setup = []string{"prepare"}
	t.Outputs = []string{"out"}
	return backend.Reference{ID: strings.Repeat("c", 32), Task: t, Binding: "data-binding", Instance: "original-pod-uid"}
}

func dataReceiptResources() backend.DataResources {
	return backend.DataResources{Execution: dataReceiptReference(), Pod: backend.DataResource{Name: "pod", UID: "original-pod-uid"}, InputSecret: backend.DataResource{Name: "input", UID: "original-input-uid"}, OutputSecret: backend.DataResource{Name: "output", UID: "original-output-uid"}}
}

func dataReceiptExit(id string, start, finish int) backend.ContainerExit {
	return backend.ContainerExit{ContainerID: id, StartedAt: time.Date(2026, 10, 20, 0, 0, start, 0, time.UTC), FinishedAt: time.Date(2026, 10, 20, 0, 0, finish, 0, time.UTC)}
}

func dataReceiptStop() backend.DataObservation {
	stage, setup, task, collector := dataReceiptExit("stage-id", 1, 2), dataReceiptExit("setup-id", 3, 4), dataReceiptExit("task-id", 5, 10), dataReceiptExit("collector-id", 5, 12)
	zero := int32(0)
	return backend.DataObservation{Status: backend.Status{State: backend.Succeeded, Instance: "original-pod-uid", ExitCode: &zero, Termination: backend.StoppedWithExitCode}, TaskStop: &backend.DataTaskStop{Stage: &stage, Setup: &setup, Task: task}, Collector: &collector, Phase: backend.DataSettled}
}

func TestDataCreatedAndResourceShapes(t *testing.T) {
	r := dataReceiptResources()
	c := backend.DataCreated{Instance: r.Pod.UID, Pod: r.Pod, InputSecret: r.InputSecret, OutputSecret: r.OutputSecret}
	if err := backend.ValidateDataCreated(r.Execution.Task, c, false); err != nil {
		t.Fatal(err)
	}
	if err := backend.ValidateDataResources(r); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*backend.DataCreated){
		func(c *backend.DataCreated) { c.Instance = "different" },
		func(c *backend.DataCreated) { c.Pod.UID = "" },
		func(c *backend.DataCreated) { c.InputSecret = backend.DataResource{} },
		func(c *backend.DataCreated) { c.OutputSecret = backend.DataResource{} },
		func(c *backend.DataCreated) { c.Pod.Name = "bad\x00name" },
	} {
		bad := c
		edit(&bad)
		if backend.ValidateDataCreated(r.Execution.Task, bad, false) == nil {
			t.Fatalf("accepted malformed complete receipt %+v", bad)
		}
	}
	partial := backend.DataCreated{InputSecret: r.InputSecret}
	if backend.ValidateDataCreated(r.Execution.Task, partial, true) != nil || backend.ValidateDataCreated(r.Execution.Task, partial, false) == nil {
		t.Fatal("partial error-return receipt confused with complete creation")
	}
	if err := backend.ValidateDataCreated(r.Execution.Task, backend.DataCreated{}, true); err != nil {
		t.Fatalf("failed original call must preserve an empty unresolved receipt: %v", err)
	}
	r.Pod, r.OutputSecret, r.Execution.Instance = backend.DataResource{}, backend.DataResource{}, ""
	if err := backend.ValidateDataResources(r); err != nil {
		t.Fatalf("known-Secret-only partial identity: %v", err)
	}
	r.Execution.Instance = "invented-pod"
	if backend.ValidateDataResources(r) == nil {
		t.Fatal("instance adopted without original Pod")
	}
	r.Execution.Instance, r.InputSecret = "", backend.DataResource{}
	if backend.ValidateDataResources(r) == nil {
		t.Fatal("empty resources are not a known-Secret partial cleanup subject")
	}
	mountOnly := backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
	m := backend.DataCreated{Instance: c.Instance, Pod: c.Pod}
	if err := backend.ValidateDataCreated(mountOnly, m, false); err != nil {
		t.Fatal(err)
	}
	m.InputSecret = c.InputSecret
	if backend.ValidateDataCreated(mountOnly, m, false) == nil {
		t.Fatal("optional absent scope accepted a Secret")
	}
	c.OutputSecret = c.InputSecret
	if backend.ValidateDataCreated(dataReceiptReference().Task, c, false) == nil {
		t.Fatal("independent scopes share one Secret identity")
	}
	c.OutputSecret.Name = "different-name-same-uid"
	if backend.ValidateDataCreated(dataReceiptReference().Task, c, false) == nil {
		t.Fatal("independent scopes share a UID under different names")
	}
	for _, test := range []struct {
		name, uid string
		ok        bool
	}{
		{strings.Repeat("n", 63), strings.Repeat("u", 128), true},
		{strings.Repeat("n", 64), "uid", false},
		{"Pod", "uid", false},
		{"pod.name", "uid", false},
		{"-pod", "uid", false},
		{"pod-", "uid", false},
		{"pod", strings.Repeat("u", 129), false},
		{"pod", "uid\n", false},
		{"pod", "é", false},
	} {
		v := backend.DataCreated{Instance: test.uid, Pod: backend.DataResource{Name: test.name, UID: test.uid}}
		if err := backend.ValidateDataCreated(mountOnly, v, false); (err == nil) != test.ok {
			t.Fatalf("resource bounds accepted=%v, want=%v", err == nil, test.ok)
		}
	}
}

func TestDataRemovalIndependentExactEcho(t *testing.T) {
	r := dataReceiptResources()
	for mask := 0; mask < 8; mask++ {
		m := backend.DataRemoval{Pod: r.Pod, InputSecret: r.InputSecret, OutputSecret: r.OutputSecret, PodRemoved: mask&1 != 0, InputRemoved: mask&2 != 0, OutputRemoved: mask&4 != 0}
		if err := backend.ValidateDataRemoval(r, m); err != nil {
			t.Fatalf("independent acknowledgment %03b: %v", mask, err)
		}
		bad := m
		bad.Pod.UID = "same-name-replacement"
		if backend.ValidateDataRemoval(r, bad) == nil {
			t.Fatal("replacement identity accepted")
		}
		bad = m
		bad.InputSecret = backend.DataResource{}
		if backend.ValidateDataRemoval(r, bad) == nil {
			t.Fatal("known identity echo dropped")
		}
	}
	r.Pod, r.OutputSecret, r.Execution.Instance = backend.DataResource{}, backend.DataResource{}, ""
	m := backend.DataRemoval{InputSecret: r.InputSecret, InputRemoved: true}
	if err := backend.ValidateDataRemoval(r, m); err != nil {
		t.Fatalf("partial known Secret acknowledgment: %v", err)
	}
	m.PodRemoved = true
	if backend.ValidateDataRemoval(r, m) == nil {
		t.Fatal("unknown Pod acknowledged removed")
	}
	m.PodRemoved, m.OutputRemoved = false, true
	if backend.ValidateDataRemoval(r, m) == nil {
		t.Fatal("absent scope acknowledged removed")
	}
}

func TestDataObservationStopAndRunningShapes(t *testing.T) {
	ref := dataReceiptReference()
	stop := dataReceiptStop()
	if err := backend.ValidateDataObservation(ref, stop); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"mount only", "stage", "setup", "outputs", "all"} {
		p, o := ref, backend.CloneDataObservation(stop)
		p.Task = backend.CloneTask(ref.Task)
		if profile == "mount only" {
			p.Task = backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
			o.TaskStop.Stage, o.TaskStop.Setup, o.Collector = nil, nil, nil
		} else {
			if profile == "stage" || profile == "outputs" {
				p.Task.Setup, o.TaskStop.Setup = nil, nil
			}
			if profile == "stage" || profile == "setup" {
				p.Task.Outputs, o.Collector = nil, nil
			}
		}
		if err := backend.ValidateDataObservation(p, o); err != nil {
			t.Fatalf("finite stopped profile %s: %v", profile, err)
		}
		if profile == "mount only" {
			extra := dataReceiptExit("invented-init", 1, 2)
			o.TaskStop.Stage = &extra
			if backend.ValidateDataObservation(p, o) == nil {
				t.Fatal("mount-only profile accepted an init receipt")
			}
		}
	}
	// A collector may start alongside the task. Its finish, not start, must
	// follow task finish at Settled; no artificial serial sidecar is required.
	for _, test := range []struct {
		name string
		edit func(*backend.DataObservation)
	}{
		{"zero phase", func(o *backend.DataObservation) { o.Phase = 0 }},
		{"starting phase has stop", func(o *backend.DataObservation) { o.Phase = backend.DataStarting }},
		{"task phase has stop", func(o *backend.DataObservation) { o.Phase = backend.DataTask }},
		{"instance", func(o *backend.DataObservation) { o.Status.Instance = "other" }},
		{"missing stop", func(o *backend.DataObservation) { o.TaskStop = nil }},
		{"missing stage", func(o *backend.DataObservation) { o.TaskStop.Stage = nil }},
		{"missing setup", func(o *backend.DataObservation) { o.TaskStop.Setup = nil }},
		{"missing collector", func(o *backend.DataObservation) { o.Collector = nil }},
		{"init failure in stop", func(o *backend.DataObservation) { o.TaskStop.Stage.ExitCode = 1 }},
		{"overlap init task", func(o *backend.DataObservation) {
			o.TaskStop.Setup.FinishedAt = o.TaskStop.Task.StartedAt.Add(time.Second)
		}},
		{"invalid task code", func(o *backend.DataObservation) { o.TaskStop.Task.ExitCode = 256 }},
		{"code mismatch", func(o *backend.DataObservation) { *o.Status.ExitCode = 1 }},
		{"stopped without code", func(o *backend.DataObservation) { o.Status.ExitCode = nil }},
		{"zero start", func(o *backend.DataObservation) { o.TaskStop.Task.StartedAt = time.Time{} }},
		{"reverse time", func(o *backend.DataObservation) {
			o.TaskStop.Task.FinishedAt = o.TaskStop.Task.StartedAt.Add(-time.Second)
		}},
		{"empty container", func(o *backend.DataObservation) { o.TaskStop.Task.ContainerID = "" }},
		{"container129", func(o *backend.DataObservation) { o.TaskStop.Task.ContainerID = strings.Repeat("x", 129) }},
		{"container control", func(o *backend.DataObservation) { o.TaskStop.Task.ContainerID = "task\n" }},
		{"stage shares main ID", func(o *backend.DataObservation) { o.TaskStop.Stage.ContainerID = o.TaskStop.Task.ContainerID }},
		{"setup shares stage ID", func(o *backend.DataObservation) { o.TaskStop.Setup.ContainerID = o.TaskStop.Stage.ContainerID }},
		{"collector shares main ID", func(o *backend.DataObservation) { o.Collector.ContainerID = o.TaskStop.Task.ContainerID }},
		{"collector before init", func(o *backend.DataObservation) { o.Collector.StartedAt = o.TaskStop.Stage.StartedAt }},
		{"collector early finish", func(o *backend.DataObservation) {
			o.Collector.FinishedAt = o.TaskStop.Task.FinishedAt.Add(-time.Second)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := backend.CloneDataObservation(stop)
			test.edit(&o)
			if backend.ValidateDataObservation(ref, o) == nil {
				t.Fatal("accepted malformed observation")
			}
		})
	}
	start := backend.ContainerStart{ContainerID: "task-id", StartedAt: stop.TaskStop.Task.StartedAt}
	running := backend.DataObservation{Status: backend.Status{State: backend.Running, Instance: ref.Instance}, TaskStart: &start, Phase: backend.DataTask}
	if err := backend.ValidateDataObservation(ref, running); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*backend.DataObservation){
		func(o *backend.DataObservation) { o.TaskStart = nil },
		func(o *backend.DataObservation) { o.Status.State = backend.Starting },
		func(o *backend.DataObservation) { o.TaskStart.StartedAt = time.Time{} },
		func(o *backend.DataObservation) { o.TaskStart.ContainerID = "" },
		func(o *backend.DataObservation) { zero := int32(0); o.Status.ExitCode = &zero },
		func(o *backend.DataObservation) { o.Status.Termination = backend.StoppedWithExitCode },
		func(o *backend.DataObservation) { o.TaskStop = stop.TaskStop },
	} {
		o := backend.CloneDataObservation(running)
		edit(&o)
		if backend.ValidateDataObservation(ref, o) == nil {
			t.Fatal("accepted invalid current Running receipt")
		}
	}
	held := backend.DataObservation{Status: backend.Status{State: backend.Unknown, Instance: ref.Instance}, Phase: backend.DataHeld}
	if err := backend.ValidateDataObservation(ref, held); err != nil {
		t.Fatalf("missing native observation should remain Held: %v", err)
	}
	ref.Task = backend.Task{Argv: []string{"run"}, DataMounts: []backend.DataMount{{Reference: "cache", Destination: "/mnt/skygo/cache"}}}
	if err := backend.ValidateDataObservation(ref, held); err != nil {
		t.Fatalf("mount-only unknown remains Held: %v", err)
	}
	held.Phase, held.Status.State = backend.DataStage, backend.Starting
	if backend.ValidateDataObservation(ref, held) == nil {
		t.Fatal("mount-only profile cannot have a staging phase")
	}
}

func TestDataNoLaunchFailureShapes(t *testing.T) {
	ref := dataReceiptReference()
	stage := dataReceiptExit("stage-id", 1, 2)
	stage.ExitCode = 7
	blocked := func(name string) *backend.DataNotStarted {
		return &backend.DataNotStarted{Name: name, WaitingReason: "PodInitializing"}
	}
	noLaunch := backend.DataObservation{Status: backend.Status{State: backend.Failed, Instance: ref.Instance}, NoLaunch: &backend.DataNoLaunch{FailedInit: backend.StageFailure, Stage: &stage, SetupBlocked: blocked("setup"), TaskBlocked: blocked("task"), CollectorBlocked: blocked("collector")}, Phase: backend.DataSettled}
	if err := backend.ValidateDataObservation(ref, noLaunch); err != nil {
		t.Fatal(err)
	}
	setupFailure := backend.CloneDataObservation(noLaunch)
	setupFailure.NoLaunch.Stage.ExitCode = 0
	setup := dataReceiptExit("setup-id", 3, 4)
	setup.ExitCode = 8
	setupFailure.NoLaunch.FailedInit, setupFailure.NoLaunch.Setup, setupFailure.NoLaunch.SetupBlocked = backend.SetupFailure, &setup, nil
	if err := backend.ValidateDataObservation(ref, setupFailure); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*backend.DataObservation){
		func(o *backend.DataObservation) { o.NoLaunch.FailedInit = 0 },
		func(o *backend.DataObservation) { o.Phase = backend.DataCollect },
		func(o *backend.DataObservation) { o.NoLaunch.Stage.ExitCode = 0 },
		func(o *backend.DataObservation) { o.NoLaunch.SetupBlocked = nil },
		func(o *backend.DataObservation) { o.NoLaunch.TaskBlocked = nil },
		func(o *backend.DataObservation) { o.NoLaunch.CollectorBlocked = nil },
		func(o *backend.DataObservation) { o.NoLaunch.TaskBlocked.Name = "other" },
		func(o *backend.DataObservation) { o.NoLaunch.TaskBlocked.WaitingReason = "ContainerCreating" },
		func(o *backend.DataObservation) { o.Status.State = backend.Succeeded },
		func(o *backend.DataObservation) { zero := int32(0); o.Status.ExitCode = &zero },
		func(o *backend.DataObservation) { o.Status.Termination = backend.StoppedWithExitCode },
		func(o *backend.DataObservation) { exit := dataReceiptExit("collector-id", 1, 2); o.Collector = &exit },
		func(o *backend.DataObservation) { o.TaskStop = dataReceiptStop().TaskStop },
		func(o *backend.DataObservation) {
			o.TaskStart = &backend.ContainerStart{ContainerID: "task-id", StartedAt: stage.StartedAt}
		},
	} {
		o := backend.CloneDataObservation(noLaunch)
		edit(&o)
		if backend.ValidateDataObservation(ref, o) == nil {
			t.Fatal("accepted malformed no-launch union")
		}
	}
	copy := backend.CloneDataObservation(noLaunch)
	copy.NoLaunch.Stage.ContainerID = "changed"
	copy.NoLaunch.SetupBlocked.Name = "changed"
	copy.NoLaunch.TaskBlocked.Name = "changed"
	copy.NoLaunch.CollectorBlocked.Name = "changed"
	if noLaunch.NoLaunch.Stage.ContainerID != "stage-id" || noLaunch.NoLaunch.SetupBlocked.Name != "setup" || noLaunch.NoLaunch.TaskBlocked.Name != "task" || noLaunch.NoLaunch.CollectorBlocked.Name != "collector" {
		t.Fatal("no-launch clone shares pointer storage")
	}
	copy = backend.CloneDataObservation(setupFailure)
	copy.NoLaunch.Setup.ContainerID = "changed"
	if setupFailure.NoLaunch.Setup.ContainerID != "setup-id" {
		t.Fatal("setup receipt clone aliases")
	}
	stop := dataReceiptStop()
	cloned := backend.CloneDataObservation(stop)
	*cloned.Status.ExitCode = 3
	cloned.TaskStop.Stage.ContainerID = "changed"
	cloned.TaskStop.Setup.ContainerID = "changed"
	cloned.TaskStop.Task.ContainerID = "changed"
	cloned.Collector.ContainerID = "changed"
	if *stop.Status.ExitCode != 0 || stop.TaskStop.Stage.ContainerID != "stage-id" || stop.TaskStop.Setup.ContainerID != "setup-id" || stop.TaskStop.Task.ContainerID != "task-id" || stop.Collector.ContainerID != "collector-id" {
		t.Fatal("stop receipt clone aliases")
	}
	start := backend.ContainerStart{ContainerID: "task-id", StartedAt: stage.StartedAt}
	running := backend.DataObservation{TaskStart: &start}
	cloned = backend.CloneDataObservation(running)
	cloned.TaskStart.ContainerID = "changed"
	if running.TaskStart.ContainerID != "task-id" {
		t.Fatal("start receipt clone aliases")
	}
	if !reflect.DeepEqual(backend.CloneDataObservation(backend.DataObservation{}), backend.DataObservation{}) {
		t.Fatal("clone changed absent receipt defaults")
	}
}
