package rpc

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
)

func TestQuoteConverterIndependenceAndTimestamp(t *testing.T) {
	now := time.Now().UTC()
	amount := int64(0)
	slots := uint32(2)
	duration := int64(3600)
	p := control.QuotePlan{AsOf: now, Currency: "USD", Unit: backend.TargetHour, Outcome: "selected", SelectedTarget: "a", ComparisonCount: 1, BillableDurationSeconds: &duration, Candidates: []control.QuoteCandidate{{Target: "a", Eligible: true, PriceStatus: "comparable", CapacityFreshness: "fresh", EstimateStatus: "available", Quote: &backend.Quote{Rate: backend.Money{AmountMicros: &amount, Currency: "USD"}, Unit: backend.TargetHour, Origin: backend.Configured, Source: "operator:test", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}, Capacity: &backend.CapacitySnapshot{State: backend.CapacityAvailable, AvailableSlots: &slots, Origin: backend.Configured, Source: "operator:test", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}, EstimatedComputeCost: &backend.Money{AmountMicros: &amount, Currency: "USD"}}}}
	wire, err := wireQuotePlan(p)
	if err != nil {
		t.Fatal(err)
	}
	*wire.Candidates[0].Quote.Rate.AmountMicros = 1
	*wire.Candidates[0].Capacity.AvailableSlots = 3
	*wire.BillableDurationSeconds = 1
	wire.Candidates[0].Quote.ObservedAt.Seconds++
	if amount != 0 || slots != 2 || duration != 3600 || !p.Candidates[0].Quote.ObservedAt.Equal(now.Add(-time.Minute)) {
		t.Fatal("converter retained input pointers")
	}
	*p.Candidates[0].EstimatedComputeCost.AmountMicros = 5
	if *wire.Candidates[0].EstimatedComputeCost.AmountMicros != 0 {
		t.Fatal("output estimate aliases input")
	}
	p.Candidates[0].Quote.ValidUntil = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := wireQuotePlan(p); connect.CodeOf(err) != connect.CodeInternal || err.Error() != "internal: quote response timestamps are invalid" {
		t.Fatalf("invalid timestamp: %v", err)
	}
}
