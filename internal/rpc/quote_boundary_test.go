package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
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
