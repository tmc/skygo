package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
)

type ergonomicsService struct{ skypilotService }

func (s *ergonomicsService) GetServerInfo(context.Context, *connect.Request[skyv1.GetServerInfoRequest]) (*connect.Response[skyv1.GetServerInfoResponse], error) {
	return connect.NewResponse(&skyv1.GetServerInfoResponse{
		Capabilities: []skyv1.Capability{skyv1.Capability_CAPABILITY_ARTIFACT_SNAPSHOT_SERVICE_V1},
		Targets:      []*skyv1.Target{{Name: "local", Capabilities: []skyv1.Capability{skyv1.Capability_CAPABILITY_WORKDIR_SNAPSHOT_V1}}},
	}), nil
}

func (s *ergonomicsService) SubmitWorkspaceJob(_ context.Context, r *connect.Request[skyv1.SubmitWorkspaceJobRequest]) (*connect.Response[skyv1.SubmitWorkspaceJobResponse], error) {
	s.record("workspace", r.Msg.Task)
	return connect.NewResponse(&skyv1.SubmitWorkspaceJobResponse{Job: &skyv1.Job{Id: r.Msg.RequestId, Target: "local", Task: r.Msg.Task, State: skyv1.JobState_JOB_STATE_SUCCEEDED}}), nil
}

func (s *ergonomicsService) GetJob(context.Context, *connect.Request[skyv1.GetJobRequest]) (*connect.Response[skyv1.GetJobResponse], error) {
	return connect.NewResponse(&skyv1.GetJobResponse{Job: &skyv1.Job{Id: "j1", Target: "local", State: skyv1.JobState_JOB_STATE_FAILED, Reason: "bad\n\x1b[31m", ExitCode: proto.Int32(0), Attempts: []*skyv1.Attempt{{Id: "a1", Ordinal: 1, State: skyv1.JobState_JOB_STATE_FAILED}}}}), nil
}

func serveErgonomics(t *testing.T, s *ergonomicsService) *httptest.Server {
	t.Helper()
	p, h := skyv1connect.NewControlServiceHandler(s)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests++
		s.mu.Unlock()
		if !strings.HasPrefix(r.URL.Path, p) {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const ergonomicRef = `{"id":"00112233445566778899aabbccddeeff","sha256":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","size":4096}`

func TestSkypilotInputCommands(t *testing.T) {
	t.Setenv("ONLY_IN_PROCESS", "secret")
	const source = "workdir: .\nsetup: echo setup\nenvs: {REQUIRED: null, OLD: before}\nrun: echo run\n"
	for _, command := range []string{"plan", "run"} {
		for _, form := range []string{"positional", "file", "stdin"} {
			for _, quoted := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/quoted=%t", command, form, quoted), func(t *testing.T) {
					dir := t.TempDir()
					file := filepath.Join(dir, "task.yaml")
					if err := os.WriteFile(file, []byte(source), 0600); err != nil {
						t.Fatal(err)
					}
					ref := ergonomicRef
					if quoted {
						ref = strings.Replace(ref, "4096", `"4096"`, 1)
					}
					refFile := filepath.Join(dir, "ref.json")
					if err := os.WriteFile(refFile, []byte(ref), 0600); err != nil {
						t.Fatal(err)
					}
					s := new(ergonomicsService)
					srv := serveErgonomics(t, s)
					args := []string{"--endpoint", srv.URL, command, "--workdir-ref", refFile, "--output", "out", "--output", "logs", "--env", "REQUIRED=", "--env", "OLD=a,b=c", "--env", "NEW=value"}
					c := New(nil)
					switch form {
					case "positional":
						args = append(args, file)
					case "file":
						args = append(args, "-f", file)
					case "stdin":
						args = append(args, "-")
						c.SetIn(strings.NewReader(source))
					}
					c.SetArgs(args)
					c.SetOut(new(bytes.Buffer))
					c.SetErr(new(bytes.Buffer))
					if err := c.ExecuteContext(context.Background()); err != nil {
						t.Fatal(err)
					}
					_, calls, tasks := s.snapshot()
					wantCall := "plan"
					if command == "run" {
						wantCall = "workspace"
					}
					if len(calls) != 1 || calls[0] != wantCall || len(tasks) != 1 {
						t.Fatalf("calls=%v tasks=%v", calls, tasks)
					}
					want := &skyv1.Task{Argv: []string{"/bin/sh", "-c", "echo run"}, Setup: []string{"/bin/sh", "-c", "echo setup"}, Workdir: &skyv1.ArtifactRef{Id: "00112233445566778899aabbccddeeff", Sha256: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", Size: 4096}, Outputs: []string{"out", "logs"}, Env: map[string]string{"REQUIRED": "", "OLD": "a,b=c", "NEW": "value", "SKYPILOT_NODE_RANK": "0", "SKYPILOT_NUM_NODES": "1", "SKYPILOT_NUM_GPUS_PER_NODE": "0"}}
					if !proto.Equal(tasks[0], want) {
						t.Fatalf("task=%v want=%v", tasks[0], want)
					}
				})
			}
		}
	}
}

func TestSkypilotInputRefusals(t *testing.T) {
	tests := []struct {
		name, source, ref string
		flags             []string
	}{
		{"missing equals", "run: x", "", []string{"--env", "secret"}},
		{"bad key", "run: x", "", []string{"--env", "bad-key=secret"}},
		{"duplicate", "run: x", "", []string{"--env", "A=secret", "--env", "A=other"}},
		{"nul value", "run: x", "", []string{"--env", "A=secret\x00"}},
		{"long value", "run: x", "", []string{"--env", "A=" + strings.Repeat("s", (16<<10)+1)}},
		{"output without workdir", "run: x", "", []string{"--output", "out"}},
		{"traversal", "workdir: .\nrun: x", ergonomicRef, []string{"--output", "../secret"}},
		{"overlap", "workdir: .\nrun: x", ergonomicRef, []string{"--output", "out", "--output", "out/secret"}},
		{"empty output", "workdir: .\nrun: x", ergonomicRef, []string{"--output="}},
		{"setup without workdir", "setup: x\nrun: x", "", []string{"--env", "A=secret"}},
		{"empty reference", "run: x", "", []string{"--workdir-ref="}},
		{"duplicate reference", "workdir: .\nrun: x", strings.Replace(ergonomicRef, `"size":4096`, `"size":4096,"size":4096`, 1), nil},
		{"unknown reference", "workdir: .\nrun: x", strings.Replace(ergonomicRef, `"size":4096`, `"secret":4096`, 1), nil},
		{"noncanonical reference", "workdir: .\nrun: x", strings.Replace(ergonomicRef, `4096`, `4.096e3`, 1), nil},
		{"oversized reference", "workdir: .\nrun: x", strings.Repeat("secret", 700), nil},
	}
	for _, command := range []string{"plan", "run"} {
		for _, tc := range tests {
			t.Run(command+"/"+tc.name, func(t *testing.T) {
				s := new(ergonomicsService)
				srv := serveErgonomics(t, s)
				args := []string{"--endpoint", srv.URL, command, "-"}
				args = append(args, tc.flags...)
				if tc.ref != "" {
					p := filepath.Join(t.TempDir(), "ref.json")
					if err := os.WriteFile(p, []byte(tc.ref), 0600); err != nil {
						t.Fatal(err)
					}
					args = append(args, "--workdir-ref", p)
				}
				c := New(nil)
				c.SetArgs(args)
				c.SetIn(strings.NewReader(tc.source))
				var out, diag bytes.Buffer
				c.SetOut(&out)
				c.SetErr(&diag)
				err := c.ExecuteContext(context.Background())
				if err == nil {
					t.Fatal("accepted invalid inputs")
				}
				if strings.Contains(err.Error()+out.String()+diag.String(), "secret") {
					t.Fatalf("input leaked: %v %s %s", err, &out, &diag)
				}
				if n, _, _ := s.snapshot(); n != 0 {
					t.Fatalf("%d HTTP requests before refusal", n)
				}
			})
		}
		for _, flag := range []string{"--env=", "--output=", "--workdir-ref="} {
			t.Run(command+"/native/"+flag, func(t *testing.T) {
				s := new(ergonomicsService)
				srv := serveErgonomics(t, s)
				c := New(nil)
				c.SetArgs([]string{"--endpoint", srv.URL, command, "-", flag})
				c.SetIn(strings.NewReader("argv: [echo]\n"))
				c.SetOut(new(bytes.Buffer))
				c.SetErr(new(bytes.Buffer))
				if err := c.Execute(); err == nil || !strings.Contains(err.Error(), "only to SkyPilot") {
					t.Fatalf("error=%v", err)
				}
				if n, _, _ := s.snapshot(); n != 0 {
					t.Fatalf("requests=%d", n)
				}
			})
		}
	}
}
