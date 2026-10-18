package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

func TestWorkload(t *testing.T) {
	switch os.Getenv("SKYGO_RPC_WORKLOAD") {
	case "success":
		fmt.Fprint(os.Stdout, "hello\n")
	case "fail":
		os.Exit(9)
	case "sleep":
		fmt.Fprint(os.Stdout, "started\n")
		time.Sleep(time.Minute)
	case "output":
		fmt.Fprint(os.Stdout, strings.Repeat("x", backend.MaxLogBytes+1000))
	default:
		return
	}
	os.Exit(0)
}

func workload(mode string) *v1.Task {
	return &v1.Task{Argv: []string{os.Args[0], "-test.run=^TestWorkload$"}, Env: map[string]string{"SKYGO_RPC_WORKLOAD": mode}}
}

type fixture struct {
	client  skyv1connect.ControlServiceClient
	control *control.Control
	server  *httptest.Server
	cancel  context.CancelFunc
	done    chan error
}

func setup(t *testing.T, token string, run bool) *fixture {
	t.Helper()
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := local.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := control.New(s, []backend.Target{{Name: "local", Kind: "local", Backend: b}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rpc.NewHandler(c, token))
	f := &fixture{client: skyv1connect.NewControlServiceClient(server.Client(), server.URL), control: c, server: server}
	if run {
		f.start()
	}
	t.Cleanup(func() {
		if f.cancel != nil {
			f.cancel()
			if err := <-f.done; !errors.Is(err, context.Canceled) {
				t.Errorf("reconciler: %v", err)
			}
		}
		server.Close()
		if err := b.Close(); err != nil {
			t.Error(err)
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *fixture) start() {
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.done = make(chan error, 1)
	go func() { f.done <- f.control.Run(ctx) }()
}
func submit(t *testing.T, f *fixture, request string, task *v1.Task) *v1.Job {
	t.Helper()
	r, err := f.client.SubmitJob(context.Background(), connect.NewRequest(&v1.SubmitJobRequest{RequestId: request, Task: task}))
	if err != nil {
		t.Fatal(err)
	}
	return r.Msg.Job
}
func await(t *testing.T, f *fixture, id string, terminal bool) *v1.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		r, err := f.client.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: id}))
		if err != nil {
			t.Fatal(err)
		}
		s := r.Msg.Job.State
		if terminal && (s == v1.JobState_JOB_STATE_SUCCEEDED || s == v1.JobState_JOB_STATE_FAILED || s == v1.JobState_JOB_STATE_CANCELED || s == v1.JobState_JOB_STATE_UNKNOWN) {
			return r.Msg.Job
		}
		if !terminal && s == v1.JobState_JOB_STATE_RUNNING {
			return r.Msg.Job
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
}
func code(t *testing.T, err error, want connect.Code) {
	t.Helper()
	if err == nil || connect.CodeOf(err) != want {
		t.Fatalf("error = %v, code = %v; want %v", err, connect.CodeOf(err), want)
	}
}
func logs(t *testing.T, f *fixture, id string, offset int64) ([]byte, *v1.StreamJobLogsResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := f.client.StreamJobLogs(ctx, connect.NewRequest(&v1.StreamJobLogsRequest{Id: id, Offset: offset}))
	if err != nil {
		return nil, nil, err
	}
	defer stream.Close()
	var data []byte
	var last *v1.StreamJobLogsResponse
	for stream.Receive() {
		last = stream.Msg()
		data = append(data, last.Data...)
	}
	return data, last, stream.Err()
}

func TestLifecycle(t *testing.T) {
	f := setup(t, "", true)
	targets, err := f.client.ListTargets(context.Background(), connect.NewRequest(&v1.ListTargetsRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(targets.Msg.Targets) != 1 || targets.Msg.Targets[0].Name != "local" {
		t.Fatalf("targets = %v", targets.Msg)
	}
	plan, err := f.client.PlanTask(context.Background(), connect.NewRequest(&v1.PlanTaskRequest{Task: workload("success")}))
	if err != nil || plan.Msg.Target != "local" {
		t.Fatalf("plan = %v, %v", plan, err)
	}
	j := submit(t, f, "one", workload("success"))
	again := submit(t, f, "one", workload("success"))
	if j.Id != again.Id {
		t.Fatal("retry created another job")
	}
	_, err = f.client.SubmitJob(context.Background(), connect.NewRequest(&v1.SubmitJobRequest{RequestId: "one", Task: workload("fail")}))
	code(t, err, connect.CodeAlreadyExists)
	done := await(t, f, j.Id, true)
	if done.State != v1.JobState_JOB_STATE_SUCCEEDED || done.ExitCode == nil || *done.ExitCode != 0 {
		t.Fatalf("job = %v", done)
	}
	data, last, err := logs(t, f, j.Id, 0)
	if err != nil || string(data) != "hello\n" || last == nil || !last.Complete {
		t.Fatalf("logs = %q, %v, %v", data, last, err)
	}
	canceled, err := f.client.CancelJob(context.Background(), connect.NewRequest(&v1.CancelJobRequest{Id: j.Id}))
	if err != nil || canceled.Msg.Job.State != v1.JobState_JOB_STATE_SUCCEEDED {
		t.Fatalf("cancel completed = %v, %v", canceled, err)
	}
	failed := await(t, f, submit(t, f, "failed", workload("fail")).Id, true)
	if failed.State != v1.JobState_JOB_STATE_FAILED || failed.GetExitCode() != 9 {
		t.Fatalf("failed job = %v", failed)
	}
	list, err := f.client.ListJobs(context.Background(), connect.NewRequest(&v1.ListJobsRequest{}))
	if err != nil || len(list.Msg.Jobs) != 2 {
		t.Fatalf("jobs = %v, %v", list, err)
	}
}

func TestErrors(t *testing.T) {
	f := setup(t, "", true)
	ctx := context.Background()
	for _, tt := range []struct {
		name string
		task *v1.Task
		want connect.Code
	}{{"empty", nil, connect.CodeInvalidArgument}, {"unsupported", &v1.Task{Argv: []string{"x"}, Image: "image"}, connect.CodeFailedPrecondition}, {"invalid env", &v1.Task{Argv: []string{"x"}, Env: map[string]string{"a=b": "c"}}, connect.CodeInvalidArgument}} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := f.client.PlanTask(ctx, connect.NewRequest(&v1.PlanTaskRequest{Task: tt.task}))
			code(t, err, tt.want)
		})
	}
	_, err := f.client.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: "bad"}))
	code(t, err, connect.CodeInvalidArgument)
	_, err = f.client.GetJob(ctx, connect.NewRequest(&v1.GetJobRequest{Id: strings.Repeat("f", 32)}))
	code(t, err, connect.CodeNotFound)
	for _, r := range []*v1.ListJobsRequest{{PageSize: -1}, {PageSize: 201}, {PageToken: "garbage"}} {
		_, err = f.client.ListJobs(ctx, connect.NewRequest(r))
		code(t, err, connect.CodeInvalidArgument)
	}
	_, err = f.client.SubmitJob(ctx, connect.NewRequest(&v1.SubmitJobRequest{Task: workload("success")}))
	code(t, err, connect.CodeInvalidArgument)
	j := submit(t, f, "success", workload("success"))
	await(t, f, j.Id, true)
	for _, offset := range []int64{-1, 100, backend.MaxLogBytes + 1} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) { _, _, err := logs(t, f, j.Id, offset); code(t, err, connect.CodeInvalidArgument) })
	}
}

func TestOutputCap(t *testing.T) {
	f := setup(t, "", true)
	j := submit(t, f, "output", workload("output"))
	await(t, f, j.Id, true)
	data, last, err := logs(t, f, j.Id, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != backend.MaxLogBytes || last == nil || !last.Complete || !last.Truncated || last.NextOffset != backend.MaxLogBytes {
		t.Fatalf("length %d, last %v", len(data), last)
	}
	data, last, err = logs(t, f, j.Id, backend.MaxLogBytes-7)
	if err != nil || len(data) != 7 || !last.Complete || !last.Truncated {
		t.Fatalf("resume = %d, %v, %v", len(data), last, err)
	}
}

func TestDisconnectAndCancel(t *testing.T) {
	f := setup(t, "", true)
	j := submit(t, f, "sleep", workload("sleep"))
	await(t, f, j.Id, false)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := f.client.StreamJobLogs(ctx, connect.NewRequest(&v1.StreamJobLogsRequest{Id: j.Id, Follow: true}))
	if err != nil {
		t.Fatal(err)
	}
	if !stream.Receive() {
		t.Fatalf("stream: %v", stream.Err())
	}
	cancel()
	stream.Close()
	time.Sleep(200 * time.Millisecond)
	r, err := f.client.GetJob(context.Background(), connect.NewRequest(&v1.GetJobRequest{Id: j.Id}))
	if err != nil || r.Msg.Job.State != v1.JobState_JOB_STATE_RUNNING {
		t.Fatalf("stream cancellation affected job: %v, %v", r, err)
	}
	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.client.CancelJob(context.Background(), connect.NewRequest(&v1.CancelJobRequest{Id: j.Id}))
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	done := await(t, f, j.Id, true)
	if done.State != v1.JobState_JOB_STATE_CANCELED || !done.CancelRequested {
		t.Fatalf("canceled job = %v", done)
	}
}

func TestTimeout(t *testing.T) {
	f := setup(t, "", true)
	task := workload("sleep")
	task.TimeoutSeconds = 1
	j := await(t, f, submit(t, f, "timeout", task).Id, true)
	if j.State != v1.JobState_JOB_STATE_FAILED || j.Reason != "execution timed out" {
		t.Fatalf("job = %v", j)
	}
}

func TestCancelBeforeStart(t *testing.T) {
	f := setup(t, "", false)
	j := submit(t, f, "queued", workload("success"))
	_, err := f.client.CancelJob(context.Background(), connect.NewRequest(&v1.CancelJobRequest{Id: j.Id}))
	if err != nil {
		t.Fatal(err)
	}
	f.start()
	done := await(t, f, j.Id, true)
	if done.State != v1.JobState_JOB_STATE_CANCELED {
		t.Fatalf("job = %v", done)
	}
	data, last, err := logs(t, f, j.Id, 0)
	if err != nil || len(data) != 0 || last == nil || !last.Complete {
		t.Fatalf("unstarted canceled logs = %q, %v, %v", data, last, err)
	}
}

func TestPagination(t *testing.T) {
	f := setup(t, "", false)
	var ids []string
	for i := range 3 {
		ids = append(ids, submit(t, f, fmt.Sprint(i), workload("success")).Id)
	}
	first, err := f.client.ListJobs(context.Background(), connect.NewRequest(&v1.ListJobsRequest{PageSize: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Msg.Jobs) != 1 || first.Msg.Jobs[0].Id != ids[0] || first.Msg.NextPageToken == "" {
		t.Fatalf("first = %v", first.Msg)
	}
	submit(t, f, "later", workload("success"))
	second, err := f.client.ListJobs(context.Background(), connect.NewRequest(&v1.ListJobsRequest{PageSize: 2, PageToken: first.Msg.NextPageToken}))
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Msg.Jobs) != 2 || second.Msg.Jobs[0].Id != ids[1] || second.Msg.Jobs[1].Id != ids[2] || second.Msg.NextPageToken != "" {
		t.Fatalf("second = %v", second.Msg)
	}
}

func TestAuthentication(t *testing.T) {
	f := setup(t, "secret", true)
	_, err := f.client.ListTargets(context.Background(), connect.NewRequest(&v1.ListTargetsRequest{}))
	code(t, err, connect.CodeUnauthenticated)
	request := connect.NewRequest(&v1.ListTargetsRequest{})
	request.Header().Set("Authorization", "Bearer secret")
	if _, err := f.client.ListTargets(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(f.server.URL + skyv1connect.ControlServiceListTargetsProcedure)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", response.StatusCode)
	}
}

func TestCancelCompletionRace(t *testing.T) {
	f := setup(t, "", true)
	for i := range 4 {
		j := submit(t, f, fmt.Sprintf("race-%d", i), workload("success"))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := f.client.CancelJob(context.Background(), connect.NewRequest(&v1.CancelJobRequest{Id: j.Id})); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := f.client.GetJob(context.Background(), connect.NewRequest(&v1.GetJobRequest{Id: j.Id})); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		done := await(t, f, j.Id, true)
		if done.State != v1.JobState_JOB_STATE_SUCCEEDED && done.State != v1.JobState_JOB_STATE_CANCELED {
			t.Fatalf("race result = %v", done)
		}
		again, err := f.client.CancelJob(context.Background(), connect.NewRequest(&v1.CancelJobRequest{Id: j.Id}))
		if err != nil {
			t.Fatal(err)
		}
		if again.Msg.Job.State != done.State || again.Msg.Job.Reason != done.Reason {
			t.Fatalf("terminal outcome changed: %v -> %v", done, again.Msg.Job)
		}
	}
}
