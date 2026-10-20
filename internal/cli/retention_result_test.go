package cli

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"google.golang.org/protobuf/proto"
	"rsc.io/script/scripttest"
)

// Dispatch-only peer: it records actual generated requests, never simulates
// result expiry, physical ownership, Store mutation or native completion.
type resultPruneService struct {
	skyv1connect.UnimplementedControlServiceHandler
	mu       sync.Mutex
	requests []*v1.PruneJobsRequest
	err      error
}

func (s *resultPruneService) PruneJobs(_ context.Context, r *connect.Request[v1.PruneJobsRequest]) (*connect.Response[v1.PruneJobsResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, proto.Clone(r.Msg).(*v1.PruneJobsRequest))
	if s.err != nil {
		return nil, s.err
	}
	return connect.NewResponse(&v1.PruneJobsResponse{Inspected: 1, Selected: 1}), nil
}

func TestResultOnlyPruneDispatch(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	for _, tt := range []struct {
		name       string
		args       []string
		err        error
		obligation string
		execute    bool
	}{
		{"page", []string{"--before", "2030-01-02T03:04:05Z", "--page-size", "7", "--page-token", "opaque"}, nil, "", false},
		{"execute", []string{"--before", "2030-01-02T03:04:05Z", "--yes"}, nil, "", true},
		{"obligation", []string{"--obligation", strings.Repeat("a", 32)}, nil, strings.Repeat("a", 32), false},
		{"no_fallback", []string{"--before", "2030-01-02T03:04:05Z", "--yes"}, connect.NewError(connect.CodeUnimplemented, errors.New("unsupported")), "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			peer := &resultPruneService{err: tt.err}
			_, handler := skyv1connect.NewControlServiceHandler(peer)
			server := httptest.NewServer(handler)
			defer server.Close()
			args := append([]string{"--endpoint", server.URL, "jobs", "prune", "--result-only", "--attempt", "2"}, tt.args...)
			out, _, err := execute(context.Background(), args...)
			if tt.err == nil && err != nil || tt.err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatal(err)
			}
			if tt.err != nil && out != "" {
				t.Fatalf("failed prune wrote success: %q", out)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if len(peer.requests) != 1 {
				t.Fatalf("calls=%d; no retry or mode fallback allowed", len(peer.requests))
			}
			r := peer.requests[0]
			if r.Mode != v1.PruneMode_PRUNE_MODE_RESULT_ONLY || r.Attempt != 2 || r.Execute != tt.execute || r.ObligationId != tt.obligation || r.AcknowledgeUnverifiedTerminal {
				t.Fatalf("request changed: %v", r)
			}
			if tt.obligation != "" {
				if r.Before != nil || r.PageSize != 0 || r.PageToken != "" {
					t.Fatalf("obligation acquired page selector: %v", r)
				}
			} else if r.Before == nil || r.Before.AsTime().Format(time.RFC3339) != "2030-01-02T03:04:05Z" {
				t.Fatalf("cutoff changed: %v", r)
			}
			if tt.name == "page" && (r.PageSize != 7 || r.PageToken != "opaque") {
				t.Fatalf("cursor/page changed: %v", r)
			}
		})
	}
}

func TestResultOnlyPruneScripts(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected RPC", http.StatusInternalServerError)
	}))
	t.Cleanup(func() {
		server.Close()
		if calls.Load() != 0 {
			t.Errorf("local invalid selectors made %d HTTP calls", calls.Load())
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	scripttest.Test(t, ctx, scriptEngine(t), []string{"SKYGO_ENDPOINT=" + server.URL, "SKYGO_TOKEN="}, "testdata/result-only/*.txt")
}
