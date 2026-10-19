package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func quoteBoundaryFixture(t *testing.T, size int, protoNames bool) *v1.QuoteTaskResponse {
	t.Helper()
	data, err := os.ReadFile("../testdata/quote-boundary-seed.json")
	if err != nil {
		t.Fatal(err)
	}
	out := &v1.QuoteTaskResponse{}
	if err := protojson.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
	seed := out.Candidates[0]
	out.Candidates = nil
	for i := 0; i < 32; i++ {
		c := proto.Clone(seed).(*v1.QuoteCandidate)
		c.Target = strings.Repeat("x", 254) + fmt.Sprintf("%02d", i)
		out.Candidates = append(out.Candidates, c)
	}
	measure := func() int {
		b, err := (protojson.MarshalOptions{UseProtoNames: protoNames}).Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, b); err != nil {
			t.Fatal(err)
		}
		return compact.Len()
	}
	delta := size - measure()
	if delta < 0 || delta/5 > 32*254 {
		t.Fatalf("boundary outside valid fixture budget: %d", delta)
	}
	controls := delta / 5
	for i, c := range out.Candidates {
		n := min(controls, 254)
		c.Target = strings.Repeat("\x01", n) + strings.Repeat("x", 254-n) + fmt.Sprintf("%02d", i)
		controls -= n
	}
	out.Candidates[31].Quote.Source += strings.Repeat("x", delta%5)
	if got := measure(); got != size {
		t.Fatalf("fixture bytes=%d want%d", got, size)
	}
	return out
}

// Fixture budgets use only valid configured facts, names and bounded rejection
// text. No unknown fields or overlong stand-ins are used to reach the limit.
func TestQuoteFullResponseBoundaries(t *testing.T) {
	for _, protoNames := range []bool{false, true} {
		for _, size := range []int{65535, 65536, 65537} {
			t.Run(fmt.Sprintf("protoNames=%t/size=%d", protoNames, size), func(t *testing.T) {
				out := quoteBoundaryFixture(t, size, protoNames)
				p := control.QuotePlan{AsOf: out.AsOf.AsTime(), Currency: "USD", Unit: backend.TargetHour, Outcome: control.QuoteNoEligibleTarget}
				for _, c := range out.Candidates {
					q := c.Quote
					cap := c.Capacity
					advice := &backend.TargetAdvice{Quote: &backend.Quote{Rate: backend.Money{AmountMicros: q.Rate.AmountMicros, Currency: q.Rate.Currency}, Unit: backend.RateUnit(q.Unit), Origin: backend.FactOrigin(q.Origin), Source: q.Source, ObservedAt: q.ObservedAt.AsTime(), ValidUntil: q.ValidUntil.AsTime()}, Capacity: &backend.CapacitySnapshot{State: backend.CapacityState(cap.State), Origin: backend.FactOrigin(cap.Origin), Source: cap.Source, ObservedAt: cap.ObservedAt.AsTime(), ValidUntil: cap.ValidUntil.AsTime()}}
					if err := backend.ValidateAdvice(advice); err != nil {
						t.Fatal(err)
					}
					if len(c.Target) != 256 || len(c.Rejection) != 256 {
						t.Fatal("fixture violates feature bounds")
					}
					p.Candidates = append(p.Candidates, control.QuoteCandidate{Target: c.Target, Rejection: c.Rejection, Quote: advice.Quote, Capacity: advice.Capacity, PriceStatus: control.PriceComparable, CapacityFreshness: control.FreshnessFresh, CapacityWarning: true, EstimateStatus: control.EstimateQuoteNotComparable})
				}
				mapped, err := wireQuotePlan(p)
				if err != nil {
					t.Fatal(err)
				}
				if !proto.Equal(mapped, out) {
					t.Fatal("full bounded model differs from actual generated conversion")
				}
				camel, err := marshalQuoteJSON(out, false)
				if err != nil {
					t.Fatal(err)
				}
				snake, err := marshalQuoteJSON(out, true)
				if err != nil {
					t.Fatal(err)
				}
				allowed := proto.Size(out) <= 65536 && len(camel) <= 65536 && len(snake) <= 65536
				err = checkQuoteResponseSize(out)
				if allowed && err != nil || !allowed && connect.CodeOf(err) != connect.CodeFailedPrecondition {
					t.Fatalf("guard proto=%d camel=%d snake=%d: %v", proto.Size(out), len(camel), len(snake), err)
				}
				if allowed {
					emitted, err := (quoteJSONCodec{name: "json"}).Marshal(out)
					if err != nil || !bytes.Equal(emitted, camel) {
						t.Fatalf("transport codec changed measured bytes: %v", err)
					}
				}
				t.Logf("actual full message: protobuf=%d compactCamel=%d compactSnake=%d allowed=%t", proto.Size(out), len(camel), len(snake), allowed)
			})
		}
	}
}

// Configured targets have a narrower printable name bound than the pure wire
// model above. Measure the real composed response before asserting refusal.
type quoteSizeBackend struct {
	unexpected atomic.Int32
}

func (*quoteSizeBackend) Check(backend.Task) error {
	return errors.New(strings.Repeat("\x02", 256))
}
func (b *quoteSizeBackend) Start(context.Context, string, backend.Task) error {
	b.unexpected.Add(1)
	return errors.New("unexpected start")
}
func (b *quoteSizeBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	b.unexpected.Add(1)
	return backend.Status{}, errors.New("unexpected observe")
}
func (b *quoteSizeBackend) Cancel(context.Context, backend.Reference) error {
	b.unexpected.Add(1)
	return errors.New("unexpected cancel")
}
func (b *quoteSizeBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	b.unexpected.Add(1)
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func TestQuoteHTTPEncodedSizeRefusal(t *testing.T) {
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	engine := auth.NewLegacy("quote-secret")
	actor, err := engine.Authenticate("Bearer quote-secret")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	amount := int64(1)
	var targets []backend.Target
	var backends []*quoteSizeBackend
	for i := 0; i < 32; i++ {
		b := &quoteSizeBackend{}
		backends = append(backends, b)
		advice := &backend.TargetAdvice{
			Quote:    &backend.Quote{Rate: backend.Money{AmountMicros: &amount, Currency: "USD"}, Unit: backend.TargetHour, Origin: backend.Configured, Source: strings.Repeat("q", 128), ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)},
			Capacity: &backend.CapacitySnapshot{State: backend.CapacityError, Origin: backend.Configured, Source: strings.Repeat("c", 128), ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)},
		}
		if err := backend.ValidateAdvice(advice); err != nil {
			t.Fatal(err)
		}
		name := strings.Repeat("\\", 126) + fmt.Sprintf("%02d", i)
		if len(name) != 128 {
			t.Fatal("configured target name does not have its exact bound")
		}
		targets = append(targets, backend.Target{Name: name, Binding: fmt.Sprintf("route%d", i), Backend: b, Advice: advice})
	}
	c, err := control.New(db, targets, engine)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := c.QuoteTask(actor, backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Candidates) != 32 {
		t.Fatalf("actual candidate count=%d; want 32", len(plan.Candidates))
	}
	for _, candidate := range plan.Candidates {
		if candidate.Eligible || len(candidate.Rejection) != 256 {
			t.Fatal("actual candidate lost its bounded rejection")
		}
	}
	mapped, err := wireQuotePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	camel, err := marshalQuoteJSON(mapped, false)
	if err != nil {
		t.Fatal(err)
	}
	snake, err := marshalQuoteJSON(mapped, true)
	if err != nil {
		t.Fatal(err)
	}
	binarySize := proto.Size(mapped)
	t.Logf("actual configured-target message: protobuf=%d compactCamel=%d compactSnake=%d", binarySize, len(camel), len(snake))
	if binarySize <= 65536 && len(camel) <= 65536 && len(snake) <= 65536 {
		t.Fatal("actual configured response does not reach the refusal boundary")
	}
	server := httptest.NewUnstartedServer(NewHandler(c, engine))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	for _, opts := range [][]connect.ClientOption{nil, {connect.WithProtoJSON()}} {
		client := skyv1connect.NewControlServiceClient(server.Client(), server.URL, opts...)
		req := connect.NewRequest(&v1.QuoteTaskRequest{Task: &v1.Task{Argv: []string{"program"}}, Currency: "USD", Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR})
		req.Header().Set("Authorization", "Bearer quote-secret")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		result, err := client.QuoteTask(ctx, req)
		cancel()
		if result != nil || connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "quote response exceeds 64 KiB") {
			t.Fatalf("oversize: %v %v", result, err)
		}
	}
	for _, b := range backends {
		if b.unexpected.Load() != 0 {
			t.Fatal("quote fixture invoked an execution method")
		}
	}
}
