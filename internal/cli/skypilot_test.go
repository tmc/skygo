package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/skycompat/tasklower"
	"github.com/tmc/skygo/skycompat/taskyaml"
	"google.golang.org/protobuf/proto"
	"rsc.io/script/scripttest"
)

// skypilotService records every HTTP request and the task of every plan or
// submission so tests can check what the CLI dispatched, not only its output.
type skypilotService struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu       sync.Mutex
	requests int
	calls    []string
	tasks    []*skyv1.Task
}

func (s *skypilotService) record(call string, task *skyv1.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
	s.tasks = append(s.tasks, proto.Clone(task).(*skyv1.Task))
}

func (s *skypilotService) PlanTask(_ context.Context, r *connect.Request[skyv1.PlanTaskRequest]) (*connect.Response[skyv1.PlanTaskResponse], error) {
	s.record("plan", r.Msg.Task)
	return connect.NewResponse(&skyv1.PlanTaskResponse{Target: "local"}), nil
}

func (s *skypilotService) SubmitJob(_ context.Context, r *connect.Request[skyv1.SubmitJobRequest]) (*connect.Response[skyv1.SubmitJobResponse], error) {
	s.record("submit", r.Msg.Task)
	return connect.NewResponse(&skyv1.SubmitJobResponse{Job: &skyv1.Job{Id: r.Msg.RequestId, Target: "local", Task: r.Msg.Task, State: skyv1.JobState_JOB_STATE_SUCCEEDED}}), nil
}

func (s *skypilotService) snapshot() (int, []string, []*skyv1.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.calls, s.tasks
}

func serveSkypilot(t *testing.T, s *skypilotService) *httptest.Server {
	t.Helper()
	path, h := skyv1connect.NewControlServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests++
		s.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

const skypilotYAML = `name: hello
envs:
  GREETING: hi
resources:
  cpus: 0.5
  memory: 512MB
run: |
  echo "$GREETING"
`

func skypilotWant(shell, target string) *skyv1.Task {
	return &skyv1.Task{
		Argv:        []string{shell, "-c", "echo \"$GREETING\"\n"},
		Env:         map[string]string{"GREETING": "hi"},
		CpuMillis:   500,
		MemoryBytes: 512 << 20,
		Target:      target,
	}
}

func TestReadTaskFormat(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		lower tasklower.Options
		want  *skyv1.Task
		code  taskyaml.Code // want a *taskyaml.Error with this code and path
		path  string
		err   string // want another error containing this text
	}{
		{name: "native", input: "argv: [echo, run]\ncpu_millis: 5\ntarget: local\n", want: &skyv1.Task{Argv: []string{"echo", "run"}, CpuMillis: 5, Target: "local"}},
		{name: "skypilot", input: skypilotYAML, want: skypilotWant("/bin/sh", "")},
		{name: "skypilot shell and target", input: skypilotYAML, lower: tasklower.Options{Shell: "/bin/bash", Target: "local"}, want: skypilotWant("/bin/bash", "local")},
		{name: "mixed", input: "argv: [echo]\nrun: echo\n", err: "mixes skygo argv and SkyPilot run"},
		{name: "native with skypilot field", input: "argv: [echo]\nresources: {cpus: 1}\n", err: "field resources not found"},
		{name: "native duplicate", input: "argv: [echo]\nargv: [false]\n", err: "already defined"},
		{name: "native shell", input: "argv: [echo]\n", lower: tasklower.Options{Shell: "/bin/sh"}, err: "apply only to SkyPilot"},
		{name: "native target", input: "argv: [echo]\ntarget: local\n", lower: tasklower.Options{Target: "other"}, err: "apply only to SkyPilot"},
		{name: "native malformed", input: "argv: [echo\n", err: "decode task:"},
		{name: "skypilot with native field", input: "run: echo\ncpu_millis: 5\n", code: taskyaml.Unsupported},
		{name: "skypilot duplicate", input: "run: echo\nrun: true\n", code: taskyaml.Invalid, path: "run"},
		{name: "skypilot type", input: "run: [echo]\n", code: taskyaml.Invalid, path: "run"},
		{name: "skypilot documents", input: "run: echo\n---\nrun: echo\n", code: taskyaml.Unsupported},
		{name: "skypilot unknown", input: "run: echo\nwat: true\n", code: taskyaml.Unsupported},
		{name: "autostop", input: "run: echo\nresources:\n  autostop: 10\n", code: taskyaml.Unsupported, path: "resources.autostop"},
		{name: "infra", input: "run: echo\nresources:\n  infra: k8s\n", code: taskyaml.Unsupported, path: "resources.infra"},
		{name: "job recovery", input: "run: echo\nresources:\n  job_recovery: FAILOVER\n", code: taskyaml.Unsupported, path: "resources.job_recovery"},
		{name: "disk size", input: "run: echo\nresources:\n  disk_size: 100\n", code: taskyaml.Unsupported, path: "resources.disk_size"},
		{name: "num nodes", input: "run: echo\nnum_nodes: 2\n", code: taskyaml.Unsupported, path: "num_nodes"},
		{name: "workdir", input: "run: echo\nworkdir: .\n", code: taskyaml.Unsupported, path: "workdir"},
		{name: "cloud", input: "run: echo\nresources:\n  cloud: aws\n", code: taskyaml.Unsupported, path: "resources.cloud"},
		{name: "relative shell", input: "run: echo\n", lower: tasklower.Options{Shell: "sh"}, err: "shell must be a clean absolute path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(nil)
			var diag bytes.Buffer
			c.SetIn(strings.NewReader(tc.input))
			c.SetErr(&diag)
			task, err := readTask(c, "-", tc.lower)
			var e *taskyaml.Error
			switch {
			case tc.want != nil:
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(task, tc.want) {
					t.Fatalf("task = %v, want %v", task, tc.want)
				}
			case tc.code != "":
				if !errors.As(err, &e) || e.Code != tc.code || tc.path != "" && e.Path != tc.path {
					t.Fatalf("got %v (%#v), want taskyaml %s %s", err, e, tc.code, tc.path)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), tc.err) || errors.As(err, &e) {
					t.Fatalf("got %v, want %q", err, tc.err)
				}
			}
		})
	}
}

func TestSkypilotDroppedMetadata(t *testing.T) {
	c := New(nil)
	var diag bytes.Buffer
	c.SetIn(strings.NewReader("name: x\n_user_specified_yaml: 'run: echo'\nrun: echo\n"))
	c.SetErr(&diag)
	if _, err := readTask(c, "-", tasklower.Options{}); err != nil {
		t.Fatal(err)
	}
	want := "task yaml: dropped name: not executed\ntask yaml: dropped _user_specified_yaml: not executed\n"
	if diag.String() != want {
		t.Fatalf("stderr = %q, want %q", diag.String(), want)
	}
}

type failedMetadataWriter struct{}

func (failedMetadataWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestSkypilotMetadataWriteFailure(t *testing.T) {
	task, err := lowerTask(failedMetadataWriter{}, []byte("name: x\nrun: echo\n"), tasklower.Options{})
	if !errors.Is(err, io.ErrClosedPipe) || task != nil {
		t.Fatalf("task = %v, error = %v; want no task and closed pipe", task, err)
	}
}

func TestTaskPath(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "task.yaml")
	if err := os.WriteFile(file, []byte(skypilotYAML), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		args  []string
		stdin string
		err   string // empty means one dispatched task
	}{
		{name: "positional", args: []string{file}},
		{name: "file", args: []string{"-f", file}},
		{name: "file stdin", args: []string{"-f", "-"}, stdin: skypilotYAML},
		{name: "positional stdin", args: []string{"-"}, stdin: skypilotYAML},
		{name: "positional and file", args: []string{file, "-f", file}, err: "both as an argument and with --file"},
		{name: "positional and empty file", args: []string{file, "--file="}, err: "both as an argument and with --file"},
		{name: "two paths", args: []string{file, file}, err: "task accepts one path"},
		{name: "no path", err: "task requires --file or a path argument"},
	} {
		for _, command := range []string{"plan", "run"} {
			t.Run(tc.name+"/"+command, func(t *testing.T) {
				s := &skypilotService{}
				srv := serveSkypilot(t, s)
				c := New(nil)
				var out, diag bytes.Buffer
				c.SetOut(&out)
				c.SetErr(&diag)
				c.SetIn(strings.NewReader(tc.stdin))
				c.SetArgs(append([]string{"--endpoint", srv.URL, command}, tc.args...))
				err := c.ExecuteContext(context.Background())
				requests, _, tasks := s.snapshot()
				if tc.err != "" {
					if err == nil || !strings.Contains(err.Error(), tc.err) {
						t.Fatalf("got %v, want %q", err, tc.err)
					}
					if requests != 0 {
						t.Fatalf("%d requests before refusal", requests)
					}
					return
				}
				if err != nil {
					t.Fatalf("%v\nstderr: %s", err, &diag)
				}
				if len(tasks) != 1 || !proto.Equal(tasks[0], skypilotWant("/bin/sh", "")) {
					t.Fatalf("dispatched %v", tasks)
				}
			})
		}
	}
}

// TestSkypilotRefusalDispatch checks that a refused field stops both commands
// before any RPC, including discovery and retention lookup.
func TestSkypilotRefusalDispatch(t *testing.T) {
	for _, input := range []string{
		"run: echo\nresources:\n  autostop: 10\n",
		"run: echo\nnum_nodes: 2\n",
		"run: echo\nworkdir: .\n",
		"run: echo\nresources:\n  use_spot: true\n",
		"run: echo\nresources:\n  job_recovery: FAILOVER\n",
	} {
		for _, command := range []string{"plan", "run"} {
			s := &skypilotService{}
			srv := serveSkypilot(t, s)
			c := New(nil)
			c.SetOut(new(bytes.Buffer))
			c.SetErr(new(bytes.Buffer))
			c.SetIn(strings.NewReader(input))
			c.SetArgs([]string{"--endpoint", srv.URL, command, "-"})
			err := c.ExecuteContext(context.Background())
			var e *taskyaml.Error
			if !errors.As(err, &e) {
				t.Errorf("%s %q: got %v, want *taskyaml.Error", command, input, err)
			}
			if requests, _, _ := s.snapshot(); requests != 0 {
				t.Errorf("%s %q: %d requests after refusal", command, input, requests)
			}
		}
	}
}

// This unchanged primer task requests autostop, which no current finite-job
// target implements. Both commands must preserve that refusal before RPCs.
func TestSkypilotPrimerHelloRefusal(t *testing.T) {
	for _, command := range []string{"plan", "run"} {
		t.Run(command, func(t *testing.T) {
			s := &skypilotService{}
			srv := serveSkypilot(t, s)
			c := New(nil)
			c.SetOut(new(bytes.Buffer))
			c.SetErr(new(bytes.Buffer))
			c.SetArgs([]string{"--endpoint", srv.URL, command, "testdata/skypilot/primer01.yaml"})
			err := c.ExecuteContext(context.Background())
			var e *taskyaml.Error
			if !errors.As(err, &e) || e.Code != taskyaml.Unsupported || e.Path != "resources.autostop" {
				t.Fatalf("got %v; want typed resources.autostop refusal", err)
			}
			if requests, _, _ := s.snapshot(); requests != 0 {
				t.Fatalf("%d RPCs after refusal", requests)
			}
		})
	}
}

func TestSkypilotScripts(t *testing.T) {
	native := &skyv1.Task{Argv: []string{"echo", "run"}, Env: map[string]string{"MODE": "native"}, CpuMillis: 250, Target: "local"}
	sh, bash := skypilotWant("/bin/sh", ""), skypilotWant("/bin/bash", "local")
	for _, tc := range []struct {
		name  string
		calls []string
		tasks []*skyv1.Task
	}{
		{"skypilot", []string{"plan", "plan", "plan", "plan", "submit", "submit", "submit"}, []*skyv1.Task{sh, bash, sh, sh, sh, sh, bash}},
		{"native", []string{"plan", "submit"}, []*skyv1.Task{native, native}},
		{"refuse", nil, nil},
	} {
		s := &skypilotService{}
		srv := serveSkypilot(t, s)
		// scripttest runs parallel children; wait for them before inspecting calls.
		t.Run(tc.name, func(t *testing.T) {
			scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=skypilot-token"}, "testdata/skypilot/"+tc.name+".txt")
		})
		requests, calls, tasks := s.snapshot()
		if tc.calls == nil && requests != 0 {
			t.Errorf("%s: %d requests, want none", tc.name, requests)
		}
		if strings.Join(calls, " ") != strings.Join(tc.calls, " ") || len(tasks) != len(tc.tasks) {
			t.Errorf("%s: calls %v, want %v", tc.name, calls, tc.calls)
			continue
		}
		for i := range tasks {
			if !proto.Equal(tasks[i], tc.tasks[i]) {
				t.Errorf("%s: %s %d task %v, want %v", tc.name, calls[i], i, tasks[i], tc.tasks[i])
			}
		}
	}
}
