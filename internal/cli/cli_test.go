package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"rsc.io/script"
	"rsc.io/script/scripttest"
)

type testService struct {
	skyv1connect.UnimplementedControlServiceHandler
	canceled      atomic.Int32
	streamStarted chan struct{}
}

func (*testService) ListTargets(_ context.Context, r *connect.Request[skyv1.ListTargetsRequest]) (*connect.Response[skyv1.ListTargetsResponse], error) {
	if r.Header().Get("Authorization") != "Bearer test-token" {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing token"))
	}
	return connect.NewResponse(&skyv1.ListTargetsResponse{Targets: []*skyv1.Target{{Name: "local", Kind: "local"}}}), nil
}
func (*testService) SubmitJob(_ context.Context, r *connect.Request[skyv1.SubmitJobRequest]) (*connect.Response[skyv1.SubmitJobResponse], error) {
	if r.Msg.RequestId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("missing request id"))
	}
	return connect.NewResponse(&skyv1.SubmitJobResponse{Job: &skyv1.Job{Id: "j1", Target: "local", Task: r.Msg.Task, State: skyv1.JobState_JOB_STATE_FAILED, Reason: "exit status 7"}}), nil
}
func (*testService) GetJob(context.Context, *connect.Request[skyv1.GetJobRequest]) (*connect.Response[skyv1.GetJobResponse], error) {
	return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("job not found"))
}
func (s *testService) CancelJob(context.Context, *connect.Request[skyv1.CancelJobRequest]) (*connect.Response[skyv1.CancelJobResponse], error) {
	s.canceled.Add(1)
	return connect.NewResponse(&skyv1.CancelJobResponse{Job: &skyv1.Job{Id: "j1"}}), nil
}
func (s *testService) StreamJobLogs(ctx context.Context, r *connect.Request[skyv1.StreamJobLogsRequest], stream *connect.ServerStream[skyv1.StreamJobLogsResponse]) error {
	if err := stream.Send(&skyv1.StreamJobLogsResponse{Data: []byte("hello\n"), NextOffset: 6}); err != nil {
		return err
	}
	if r.Msg.Follow {
		close(s.streamStarted)
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
func serve(t *testing.T, s *testService) *httptest.Server {
	t.Helper()
	p, h := skyv1connect.NewControlServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(p, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}
func execute(ctx context.Context, args ...string) (string, string, error) {
	c := New(nil)
	var out, errout bytes.Buffer
	c.SetOut(&out)
	c.SetErr(&errout)
	c.SetArgs(args)
	err := c.ExecuteContext(ctx)
	return out.String(), errout.String(), err
}

func TestTree(t *testing.T) {
	var walk func(*cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Short == "" {
			t.Errorf("%s: missing short help", c.CommandPath())
		}
		if !c.HasSubCommands() && (c.RunE == nil || c.Args == nil) {
			t.Errorf("%s: missing execution or argument validation", c.CommandPath())
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(New(nil))
}

func TestServerOptions(t *testing.T) {
	var got ServerOptions
	c := New(func(_ context.Context, opts ServerOptions) error { got = opts; return nil })
	c.SetArgs([]string{"server", "--disable-local", "--namespace", "tasks", "--token", "secret"})
	if err := c.Execute(); err != nil {
		t.Fatal(err)
	}
	if !got.DisableLocal || got.Namespace != "tasks" || got.Token != "secret" {
		t.Fatalf("options: %+v", got)
	}
}
func TestStrictTask(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"unknown", "argv: [echo]\nwat: true\n", "field wat not found"},
		{"duplicate", "argv: [echo]\nargv: [false]\n", "already defined"},
		{"documents", "argv: [echo]\n---\nargv: [false]\n", "exactly one"},
		{"empty", "{}", "argv must not be empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(nil)
			c.SetIn(strings.NewReader(tc.input))
			_, err := readTask(c, "-")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
}
func TestFollowCancellation(t *testing.T) {
	s := &testService{streamStarted: make(chan struct{})}
	srv := serve(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := execute(ctx, "--endpoint", srv.URL, "jobs", "logs", "j1", "--follow")
		done <- err
	}()
	select {
	case <-s.streamStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stream failed to stop")
	}
	if s.canceled.Load() != 0 {
		t.Fatal("interrupt canceled workload")
	}
}
func TestWaitExitCode(t *testing.T) {
	srv := serve(t, &testService{})
	c := New(nil)
	c.SetArgs([]string{"--endpoint", srv.URL, "run", "-f", "-", "--wait"})
	c.SetIn(strings.NewReader("argv: [false]\n"))
	var out, diag bytes.Buffer
	c.SetOut(&out)
	c.SetErr(&diag)
	err := c.Execute()
	if ExitCode(err) != 2 {
		t.Fatalf("got %v, exit %d", err, ExitCode(err))
	}
	if !strings.Contains(out.String(), "failed") || !strings.Contains(diag.String(), "request-id:") {
		t.Fatalf("stdout=%s stderr=%s", &out, &diag)
	}
}

func TestCLIHelper(t *testing.T) {
	if os.Getenv("SKYGO_TEST_HELPER") != "1" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		os.Exit(99)
	}
	c := New(nil)
	c.SetArgs(args[1:])
	err := c.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(ExitCode(err))
}
func scriptEngine(t *testing.T) *script.Engine {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	e := script.NewEngine()
	e.Cmds = scripttest.DefaultCmds()
	e.Conds = scripttest.DefaultConds()
	e.Cmds["skygo"] = script.Command(script.CmdUsage{Summary: "run the CLI helper"}, func(s *script.State, args ...string) (script.WaitFunc, error) {
		cmd := exec.CommandContext(s.Context(), exe, append([]string{"-test.run=^TestCLIHelper$", "--"}, args...)...)
		cmd.Dir = s.Getwd()
		cmd.Env = append(s.Environ(), "SKYGO_TEST_HELPER=1")
		var out, diag bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diag
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return func(s *script.State) (string, string, error) {
			err := cmd.Wait()
			if setErr := s.Setenv("SKYGO_EXIT_CODE", strconv.Itoa(cmd.ProcessState.ExitCode())); setErr != nil {
				return out.String(), diag.String(), setErr
			}
			return out.String(), diag.String(), err
		}, nil
	})
	return e
}

func TestScripts(t *testing.T) {
	srv := serve(t, &testService{})
	scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=" + srv.URL, "SKYGO_TOKEN=test-token"}, "testdata/basic.txt")
}
