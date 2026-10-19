package cli

import (
	"bytes"
	"context"
	"encoding/json"
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

type quoteService struct {
	skyv1connect.UnimplementedControlServiceHandler
	request *v1.QuoteTaskRequest
	old     bool
	calls   atomic.Int32
	other   atomic.Int32
	outcome v1.QuoteOutcome
}

func (s *quoteService) QuoteTask(_ context.Context, r *connect.Request[v1.QuoteTaskRequest]) (*connect.Response[v1.QuoteTaskResponse], error) {
	s.calls.Add(1)
	s.request = r.Msg
	if s.old {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
	}
	zero := int64(0)
	outcome := s.outcome
	if outcome == 0 {
		outcome = v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED
	}
	selected := "b"
	if outcome != v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED {
		selected = ""
	}
	return connect.NewResponse(&v1.QuoteTaskResponse{Outcome: outcome, SelectedTarget: selected, Currency: r.Msg.Currency, Unit: r.Msg.Unit, Candidates: []*v1.QuoteCandidate{{Target: "b", Eligible: true, Quote: &v1.Quote{Rate: &v1.Money{AmountMicros: &zero, Currency: "USD"}, Unit: r.Msg.Unit}, PriceStatus: v1.PriceStatus_PRICE_STATUS_COMPARABLE, CapacityWarning: true, EstimateStatus: v1.EstimateStatus_ESTIMATE_STATUS_DURATION_UNSPECIFIED}}}), nil
}
func (s *quoteService) PlanTask(context.Context, *connect.Request[v1.PlanTaskRequest]) (*connect.Response[v1.PlanTaskResponse], error) {
	s.other.Add(1)
	return connect.NewResponse(&v1.PlanTaskResponse{Target: "a"}), nil
}
func (s *quoteService) SubmitJob(context.Context, *connect.Request[v1.SubmitJobRequest]) (*connect.Response[v1.SubmitJobResponse], error) {
	s.other.Add(1)
	return nil, errors.New("unexpected submit")
}
func (s *quoteService) GetServerInfo(context.Context, *connect.Request[v1.GetServerInfoRequest]) (*connect.Response[v1.GetServerInfoResponse], error) {
	s.other.Add(1)
	return nil, errors.New("unexpected discovery")
}
func serveQuoteCLI(t *testing.T, s *quoteService) *httptest.Server {
	t.Helper()
	path, handler := skyv1connect.NewControlServiceHandler(s)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}
func quoteTaskFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "task.yaml")
	if err := os.WriteFile(path, []byte("argv: [program]\ntarget: original\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestQuoteCLIValidationBeforeRPC(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected", 500) }))
	defer server.Close()
	task := quoteTaskFile(t)
	cases := [][]string{
		{"--objective", "unknown"}, {"--currency", "USD"}, {"--unit", "target-hour"}, {"--billable-duration", "1s"},
		{"--objective", "lowest-rate"}, {"--objective", "lowest-rate", "--currency", "usd"},
		{"--objective", "lowest-rate", "--currency", "USD", "--unit", "minute"},
	}
	for _, duration := range []string{"0s", "-1s", "0.5s", "168h1s", "999999999999999999999h"} {
		cases = append(cases, []string{"--objective", "lowest-rate", "--currency", "USD", "--billable-duration", duration})
	}
	for _, flags := range cases {
		out, _, err := execute(context.Background(), append([]string{"--endpoint", server.URL, "plan", "-f", task}, flags...)...)
		if err == nil || out != "" || calls.Load() != 0 {
			t.Fatalf("flags=%v error=%v output=%q calls=%d", flags, err, out, calls.Load())
		}
	}
	if _, _, err := execute(context.Background(), "--endpoint", server.URL, "run", "-f", task, "--objective", "lowest-rate"); err == nil || calls.Load() != 0 {
		t.Fatalf("run objective accepted: %v", err)
	}
}

func TestQuoteCLIRouteAndOriginalTask(t *testing.T) {
	service := &quoteService{}
	server := serveQuoteCLI(t, service)
	task := quoteTaskFile(t)
	out, _, err := execute(context.Background(), "--endpoint", server.URL, "plan", "-f", task, "--objective", "lowest-rate", "--currency", "USD", "--billable-duration", "45m")
	if err != nil || !strings.Contains(out, "advisory target: b") || !strings.Contains(out, "0.000000 USD/target-hour") || !strings.Contains(out, "estimated compute cost: unknown") || !strings.Contains(out, "advisory only; run uses task.target or ordinary placement") {
		t.Fatalf("human: %q %v", out, err)
	}
	if service.calls.Load() != 1 || service.other.Load() != 0 || service.request.Task.Target != "original" || service.request.BillableDurationSeconds == nil || *service.request.BillableDurationSeconds != 2700 {
		t.Fatalf("routing/payload: %v", service.request)
	}
	out, _, err = execute(context.Background(), "--endpoint", server.URL, "--json", "plan", "-f", task, "--objective", "lowest-rate", "--currency", "USD")
	if err != nil || !strings.Contains(out, `"amount_micros":"0"`) || strings.Contains(out, `"billable_duration_seconds"`) {
		t.Fatalf("json presence: %q %v", out, err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(strings.TrimSuffix(out, "\n"))); err != nil || compact.String() != strings.TrimSuffix(out, "\n") {
		t.Fatalf("not compact: %q %v", out, err)
	}
	out, _, err = execute(context.Background(), "--endpoint", server.URL, "plan", "-f", task, "--objective", "first-fit")
	if err != nil || out != "a\n" || service.other.Load() != 1 {
		t.Fatalf("first-fit changed: %q %v", out, err)
	}
}

func TestQuoteCLINoFallback(t *testing.T) {
	service := &quoteService{old: true}
	server := serveQuoteCLI(t, service)
	out, _, err := execute(context.Background(), "--endpoint", server.URL, "plan", "-f", quoteTaskFile(t), "--objective", "lowest-rate", "--currency", "USD")
	if connect.CodeOf(err) != connect.CodeUnimplemented || out != "" || service.calls.Load() != 1 || service.other.Load() != 0 {
		t.Fatalf("fallback: output=%q error=%v", out, err)
	}
}

func TestQuoteCLINoSelection(t *testing.T) {
	for _, outcome := range []v1.QuoteOutcome{v1.QuoteOutcome_QUOTE_OUTCOME_NO_ELIGIBLE_TARGET, v1.QuoteOutcome_QUOTE_OUTCOME_NO_COMPARABLE_QUOTE} {
		service := &quoteService{outcome: outcome}
		server := serveQuoteCLI(t, service)
		out, _, err := execute(context.Background(), "--endpoint", server.URL, "--json", "plan", "-f", quoteTaskFile(t), "--objective", "lowest-rate", "--currency", "USD")
		if err == nil || ExitCode(err) != 1 || !strings.Contains(out, outcome.String()) {
			t.Fatalf("no selection output=%q err=%v", out, err)
		}
	}
}
