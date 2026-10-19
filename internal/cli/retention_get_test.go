package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

// This service checks CLI decoding across a real HTTP boundary. Durable expiry
// transitions are exercised separately by the retention integration scripts.
type expiredJobService struct {
	skyv1connect.UnimplementedControlServiceHandler
	err       error
	gets      atomic.Int32
	submits   atomic.Int32
	discovery atomic.Int32
	streams   atomic.Int32
}

func (s *expiredJobService) GetRetention(context.Context, *connect.Request[v1.GetRetentionRequest]) (*connect.Response[v1.GetRetentionResponse], error) {
	s.discovery.Add(1)
	return connect.NewResponse(&v1.GetRetentionResponse{}), nil
}

func (s *expiredJobService) SubmitJob(_ context.Context, r *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.submits.Add(1)
	return connect.NewResponse(&v1.SubmitJobResponse{Job: &v1.Job{Id: strings.Repeat("b", 32), Task: r.Msg.Task, State: v1.JobState_JOB_STATE_QUEUED}}), nil
}

func (s *expiredJobService) GetJob(context.Context, *connect.Request[v1.GetJobRequest]) (*connect.Response[v1.GetJobResponse], error) {
	s.gets.Add(1)
	return nil, s.err
}

func (s *expiredJobService) StreamJobLogs(context.Context, *connect.Request[v1.StreamJobLogsRequest], *connect.ServerStream[v1.StreamJobLogsResponse]) error {
	s.streams.Add(1)
	return errors.New("unexpected log stream")
}

func TestJobReadRetentionDiagnostics(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	for _, command := range []string{"get", "wait", "logs_precheck"} {
		for _, kind := range []string{"result_expired", "untyped", "unauthenticated"} {
			t.Run(command+"/"+kind, func(t *testing.T) {
				code := connect.CodeFailedPrecondition
				message := "retention result or request expired"
				if kind == "unauthenticated" {
					code, message = connect.CodeUnauthenticated, "authentication required"
				}
				rpcErr := connect.NewError(code, errors.New(message))
				if kind == "result_expired" {
					detail, err := connect.NewErrorDetail(&v1.RetentionExpiry{Reason: v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED})
					if err != nil {
						t.Fatal(err)
					}
					rpcErr.AddDetail(detail)
				}
				service := &expiredJobService{err: rpcErr}
				path, handler := skyv1connect.NewControlServiceHandler(service)
				mux := http.NewServeMux()
				mux.Handle(path, handler)
				server := httptest.NewServer(mux)
				defer server.Close()
				id := strings.Repeat("b", 32)
				args := []string{"--endpoint", server.URL}
				submits, discovery := int32(0), int32(0)
				switch command {
				case "get":
					args = append(args, "jobs", "get", id)
				case "wait":
					file := filepath.Join(t.TempDir(), "task.yaml")
					if err := os.WriteFile(file, []byte("argv: [trainer]\n"), 0600); err != nil {
						t.Fatal(err)
					}
					args = append(args, "run", "-f", file, "--wait")
					submits, discovery = 1, 1
				case "logs_precheck":
					args = append(args, "jobs", "logs", id, "--attempt", "2")
					discovery = 1
				}
				out, _, err := execute(context.Background(), args...)
				if err == nil || connect.CodeOf(err) != code || out != "" {
					t.Fatalf("stdout=%q error=%v want code=%v", out, err, code)
				}
				want := rpcErr.Error()
				if kind == "result_expired" {
					want = "result expired: " + want
				}
				if err.Error() != want {
					t.Fatalf("diagnostic=%q want=%q", err.Error(), want)
				}
				var decoded *connect.Error
				if !errors.As(err, &decoded) || decoded.Message() != message {
					t.Fatalf("underlying RPC error changed: %v", err)
				}
				if kind == "result_expired" {
					if len(decoded.Details()) != 1 {
						t.Fatal("typed expiry detail lost")
					}
					value, decodeErr := decoded.Details()[0].Value()
					expiry, ok := value.(*v1.RetentionExpiry)
					if decodeErr != nil || !ok || expiry.Reason != v1.RetentionExpiryReason_RETENTION_EXPIRY_REASON_RESULT_EXPIRED {
						t.Fatalf("typed expiry changed: %v, %v", value, decodeErr)
					}
				}
				if service.gets.Load() != 1 || service.submits.Load() != submits || service.discovery.Load() != discovery || service.streams.Load() != 0 {
					t.Fatalf("calls get=%d submit=%d discovery=%d stream=%d", service.gets.Load(), service.submits.Load(), service.discovery.Load(), service.streams.Load())
				}
			})
		}
	}
}
