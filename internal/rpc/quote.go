package rpc

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *handler) QuoteTask(ctx context.Context, r *connect.Request[v1.QuoteTaskRequest]) (*connect.Response[v1.QuoteTaskResponse], error) {
	plan, err := h.control.QuoteTask(requestActor(ctx), task(r.Msg.Task), r.Msg.Currency, quoteRateUnit(r.Msg.Unit), r.Msg.BillableDurationSeconds)
	if err != nil {
		h.record(ctx, "plan", "", err)
		return nil, rpcError(err)
	}
	out, err := wireQuotePlan(plan)
	if err == nil {
		err = checkQuoteResponseSize(out)
	}
	h.record(ctx, "plan", "", err)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(out), nil
}

func wireMoney(m *backend.Money) *v1.Money {
	if m == nil {
		return nil
	}
	out := &v1.Money{Currency: m.Currency}
	if m.AmountMicros != nil {
		n := *m.AmountMicros
		out.AmountMicros = &n
	}
	return out
}

func wireQuote(q *backend.Quote) *v1.Quote {
	if q == nil {
		return nil
	}
	return &v1.Quote{Rate: wireMoney(&q.Rate), Unit: v1.RateUnit(q.Unit), Origin: v1.FactOrigin(q.Origin), Source: q.Source, ObservedAt: timestamppb.New(q.ObservedAt), ValidUntil: timestamppb.New(q.ValidUntil)}
}

func wireCapacity(c *backend.CapacitySnapshot) *v1.CapacitySnapshot {
	if c == nil {
		return nil
	}
	out := &v1.CapacitySnapshot{State: v1.CapacityState(c.State), Origin: v1.FactOrigin(c.Origin), Source: c.Source, ObservedAt: timestamppb.New(c.ObservedAt), ValidUntil: timestamppb.New(c.ValidUntil)}
	if c.AvailableSlots != nil {
		n := *c.AvailableSlots
		out.AvailableSlots = &n
	}
	if c.State == backend.CapacityError {
		out.FailureReason = v1.CapacityFailureReason_CAPACITY_FAILURE_REASON_SOURCE_UNAVAILABLE
	}
	return out
}

func wireQuotePlan(p control.QuotePlan) (*v1.QuoteTaskResponse, error) {
	out := &v1.QuoteTaskResponse{AsOf: timestamppb.New(p.AsOf), Currency: p.Currency, Unit: v1.RateUnit(p.Unit), SelectedTarget: p.SelectedTarget, ComparisonCount: p.ComparisonCount, UnpricedEligibleCount: p.UnpricedEligibleCount,
		Outcome:         v1.QuoteOutcome(v1.QuoteOutcome_value["QUOTE_OUTCOME_"+strings.ToUpper(string(p.Outcome))]),
		ExcludedCharges: []v1.ExcludedCharge{v1.ExcludedCharge_EXCLUDED_CHARGE_STORAGE, v1.ExcludedCharge_EXCLUDED_CHARGE_TRANSFER, v1.ExcludedCharge_EXCLUDED_CHARGE_TAX, v1.ExcludedCharge_EXCLUDED_CHARGE_LICENSES, v1.ExcludedCharge_EXCLUDED_CHARGE_PROVIDER_ADJUSTMENTS}}
	if p.BillableDurationSeconds != nil {
		n := *p.BillableDurationSeconds
		out.BillableDurationSeconds = &n
	}
	times := []*timestamppb.Timestamp{out.AsOf}
	for _, c := range p.Candidates {
		wire := &v1.QuoteCandidate{Target: c.Target, Eligible: c.Eligible, Rejection: c.Rejection, Quote: wireQuote(c.Quote), Capacity: wireCapacity(c.Capacity), CapacityWarning: c.CapacityWarning, EstimatedComputeCost: wireMoney(c.EstimatedComputeCost),
			PriceStatus:       v1.PriceStatus(v1.PriceStatus_value["PRICE_STATUS_"+strings.ToUpper(string(c.PriceStatus))]),
			CapacityFreshness: v1.FactFreshness(v1.FactFreshness_value["FACT_FRESHNESS_"+strings.ToUpper(string(c.CapacityFreshness))]),
			EstimateStatus:    v1.EstimateStatus(v1.EstimateStatus_value["ESTIMATE_STATUS_"+strings.ToUpper(string(c.EstimateStatus))])}
		if wire.Quote != nil {
			times = append(times, wire.Quote.ObservedAt, wire.Quote.ValidUntil)
		}
		if wire.Capacity != nil {
			times = append(times, wire.Capacity.ObservedAt, wire.Capacity.ValidUntil)
		}
		out.Candidates = append(out.Candidates, wire)
	}
	for _, stamp := range times {
		if stamp == nil || stamp.CheckValid() != nil {
			return nil, connect.NewError(connect.CodeInternal, errors.New("quote response timestamps are invalid"))
		}
	}
	return out, nil
}

func quoteRateUnit(unit v1.RateUnit) backend.RateUnit {
	switch unit {
	case v1.RateUnit_RATE_UNIT_TARGET_HOUR:
		return backend.TargetHour
	case v1.RateUnit_RATE_UNIT_CPU_HOUR:
		return backend.CPUHour
	case v1.RateUnit_RATE_UNIT_ACCELERATOR_HOUR:
		return backend.AcceleratorHour
	}
	return backend.RateUnitUnspecified
}
