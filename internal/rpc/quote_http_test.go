package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// This backend isolates Check diagnostics; any execution method is a test error.
type quoteOnlyBackend struct{ reason string }

func (b *quoteOnlyBackend) Check(backend.Task) error {
	if b.reason != "" {
		return errors.New(b.reason)
	}
	return nil
}
func (*quoteOnlyBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected start")
}
func (*quoteOnlyBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observe")
}
func (*quoteOnlyBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancel")
}
func (*quoteOnlyBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func quoteHTTPFixture(t *testing.T, targets []backend.Target) *httptest.Server {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	engine := auth.NewLegacy("quote-secret")
	c, err := control.New(s, targets, engine)
	if err != nil {
		s.Close()
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(rpc.NewHandler(c, engine))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(func() {
		server.Close()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return server
}

func quoteHTTPAdvice(amount int64) *backend.TargetAdvice {
	now := time.Now().UTC()
	return &backend.TargetAdvice{Quote: &backend.Quote{Rate: backend.Money{AmountMicros: &amount, Currency: "USD"}, Unit: backend.TargetHour, Origin: backend.Configured, Source: "operator:rpc", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}
}

func TestQuoteHTTPProtocolsAndAuthentication(t *testing.T) {
	server := quoteHTTPFixture(t, []backend.Target{{Name: "a", Kind: "test", Binding: "a", Backend: &quoteOnlyBackend{}, Advice: quoteHTTPAdvice(2)}, {Name: "b", Kind: "test", Binding: "b", Backend: &quoteOnlyBackend{}, Advice: quoteHTTPAdvice(0)}})
	for _, opts := range [][]connect.ClientOption{nil, {connect.WithProtoJSON()}, {connect.WithGRPC()}, {connect.WithGRPCWeb()}} {
		client := skyv1connect.NewControlServiceClient(server.Client(), server.URL, opts...)
		in := &v1.QuoteTaskRequest{Task: &v1.Task{Argv: []string{"program"}}, Currency: "USD", Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR}
		if _, err := client.QuoteTask(context.Background(), connect.NewRequest(in)); connect.CodeOf(err) != connect.CodeUnauthenticated {
			t.Fatalf("missing auth: %v", err)
		}
		seconds := int64(3600)
		in.BillableDurationSeconds = &seconds
		req := connect.NewRequest(in)
		req.Header().Set("Authorization", "Bearer quote-secret")
		result, err := client.QuoteTask(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		got := result.Msg
		if got.SelectedTarget != "b" || got.ComparisonCount != 2 || got.Outcome != v1.QuoteOutcome_QUOTE_OUTCOME_SELECTED || len(got.Candidates) != 2 || len(got.ExcludedCharges) != 5 {
			t.Fatalf("quote=%v", got)
		}
		if got.Candidates[1].Quote.Rate.AmountMicros == nil || *got.Candidates[1].Quote.Rate.AmountMicros != 0 || got.Candidates[1].EstimatedComputeCost.AmountMicros == nil || *got.Candidates[1].EstimatedComputeCost.AmountMicros != 0 {
			t.Fatal("explicit free rate/estimate lost")
		}
		if in.Task.Target != "" || *in.BillableDurationSeconds != 3600 {
			t.Fatal("input changed")
		}
		in.Unit = 257 // Must not narrow to uint8 and accidentally accept target-hour.
		bad := connect.NewRequest(in)
		bad.Header().Set("Authorization", "Bearer quote-secret")
		if _, err := client.QuoteTask(context.Background(), bad); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("unknown enum: %v", err)
		}
	}
}

func TestQuoteHTTPCompactJSON(t *testing.T) {
	server := quoteHTTPFixture(t, []backend.Target{{Name: "a", Binding: "a", Backend: &quoteOnlyBackend{}, Advice: quoteHTTPAdvice(1)}})
	payload := []byte(`{"task":{"argv":["program"]},"currency":"USD","unit":"RATE_UNIT_TARGET_HOUR","unknown_field":true}`)
	for _, content := range []string{"application/json", "application/json; charset=utf-8"} {
		req, err := http.NewRequest(http.MethodPost, server.URL+skyv1connect.ControlServiceQuoteTaskProcedure, bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer quote-secret")
		req.Header.Set("Content-Type", content)
		response, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("JSON status %d: %s", response.StatusCode, data)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, data); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, compact.Bytes()) || len(data) > 65536 {
			t.Fatalf("not bounded compact JSON: %d", len(data))
		}
	}
}

func TestQuoteHTTPEncodedSizeRefusal(t *testing.T) {
	var targets []backend.Target
	for i := 0; i < 32; i++ {
		targets = append(targets, backend.Target{Name: strings.Repeat("\x01", 254) + fmt.Sprintf("%02d", i), Binding: fmt.Sprintf("route%d", i), Backend: &quoteOnlyBackend{reason: strings.Repeat("\x02", 256)}, Advice: quoteHTTPAdvice(1)})
	}
	server := quoteHTTPFixture(t, targets)
	for _, opts := range [][]connect.ClientOption{nil, {connect.WithProtoJSON()}} {
		client := skyv1connect.NewControlServiceClient(server.Client(), server.URL, opts...)
		req := connect.NewRequest(&v1.QuoteTaskRequest{Task: &v1.Task{Argv: []string{"program"}}, Currency: "USD", Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR})
		req.Header().Set("Authorization", "Bearer quote-secret")
		result, err := client.QuoteTask(context.Background(), req)
		if result != nil || connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "quote response exceeds 64 KiB") {
			t.Fatalf("oversize: %v %v", result, err)
		}
	}
}
