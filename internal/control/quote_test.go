package control

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

type quoteCheckBackend struct {
	check func(backend.Task) error
	calls int
}

func (b *quoteCheckBackend) Check(t backend.Task) error {
	b.calls++
	if b.check != nil {
		return b.check(t)
	}
	return nil
}
func (*quoteCheckBackend) Start(context.Context, string, backend.Task) error {
	return errors.New("unexpected execution")
}
func (*quoteCheckBackend) Observe(context.Context, backend.Reference) (backend.Status, error) {
	return backend.Status{}, errors.New("unexpected observation")
}
func (*quoteCheckBackend) Cancel(context.Context, backend.Reference) error {
	return errors.New("unexpected cancellation")
}
func (*quoteCheckBackend) Logs(context.Context, backend.Reference, int64, int) (backend.LogChunk, error) {
	return backend.LogChunk{}, errors.New("unexpected logs")
}

func quoteTestTime() time.Time    { return time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC) }
func quoteInt(n int64) *int64     { return &n }
func quoteSlots(n uint32) *uint32 { return &n }
func quoteTestFact(amount int64) *backend.Quote {
	now := quoteTestTime()
	return &backend.Quote{Origin: backend.Configured, Rate: backend.Money{AmountMicros: quoteInt(amount), Currency: "USD"}, Unit: backend.TargetHour, Source: "operator:test", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}
}
func quoteTestTarget(name string, amount int64) backend.Target {
	return backend.Target{Name: name, Binding: "route:" + name, Backend: &quoteCheckBackend{}, Advice: &backend.TargetAdvice{Quote: quoteTestFact(amount)}}
}

func TestQuoteSelection(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		change               func([]backend.Target)
		want                 string
		outcome              QuoteOutcome
		comparable, unpriced uint32
	}{
		{"different rates", nil, "b", QuoteSelected, 2, 0},
		{"zero is present", func(ts []backend.Target) { ts[1].Advice.Quote.Rate.AmountMicros = quoteInt(0) }, "b", QuoteSelected, 2, 0},
		{"tie is lexical", func(ts []backend.Target) { ts[0].Advice.Quote.Rate.AmountMicros = quoteInt(1) }, "a", QuoteSelected, 2, 0},
		{"missing remains unknown", func(ts []backend.Target) { ts[1].Advice = nil }, "a", QuoteSelected, 1, 1},
		{"currency mismatch", func(ts []backend.Target) { ts[1].Advice.Quote.Rate.Currency = "EUR" }, "a", QuoteSelected, 1, 1},
		{"unit mismatch", func(ts []backend.Target) { ts[1].Advice.Quote.Unit = backend.CPUHour }, "a", QuoteSelected, 1, 1},
		{"stale at expiry", func(ts []backend.Target) { ts[1].Advice.Quote.ValidUntil = quoteTestTime() }, "a", QuoteSelected, 1, 1},
		{"future", func(ts []backend.Target) { ts[1].Advice.Quote.ObservedAt = quoteTestTime().Add(time.Second) }, "a", QuoteSelected, 1, 1},
		{"all missing", func(ts []backend.Target) { ts[0].Advice, ts[1].Advice = nil, nil }, "", QuoteNoComparableQuote, 0, 2},
		{"capacity error is warning", func(ts []backend.Target) {
			now := quoteTestTime()
			ts[1].Advice.Capacity = &backend.CapacitySnapshot{Origin: backend.Configured, State: backend.CapacityError, Source: "operator:failure", ObservedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Hour)}
		}, "b", QuoteSelected, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			targets := []backend.Target{quoteTestTarget("a", 2), quoteTestTarget("b", 1)}
			if tc.change != nil {
				tc.change(targets)
			}
			before := targets[0].Name
			for i := 0; i < 3; i++ {
				got, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, []backend.Target{targets[1], targets[0]}, quoteTestTime())
				if err != nil || got.SelectedTarget != tc.want || got.Outcome != tc.outcome || got.ComparisonCount != tc.comparable || got.UnpricedEligibleCount != tc.unpriced {
					t.Fatalf("plan=%+v err=%v", got, err)
				}
				if len(got.Candidates) != 2 || got.Candidates[0].Target != "a" || got.Candidates[1].Target != "b" || !got.AsOf.Equal(quoteTestTime()) {
					t.Fatalf("ordering/clock=%+v", got)
				}
				if !got.Candidates[1].CapacityWarning {
					t.Fatal("missing/error capacity did not warn")
				}
			}
			if targets[0].Name != before {
				t.Fatal("caller target order mutated")
			}
		})
	}
}

func TestQuoteComputeEstimates(t *testing.T) {
	for _, tc := range []struct{ rate, seconds, want int64 }{
		{0, 1, 0}, {1, 1, 1}, {1, 3600, 1}, {1_000_000, 2700, 750_000}, {maxQuoteRate, maxQuoteDuration, 168_000_000_000_000},
	} {
		got, err := quoteComputeEstimate(tc.rate, tc.seconds)
		if err != nil || got != tc.want {
			t.Fatalf("estimate(%d,%d)=%d,%v want%d", tc.rate, tc.seconds, got, err, tc.want)
		}
	}
	for _, input := range [][2]int64{{-1, 1}, {maxQuoteRate + 1, 1}, {1, 0}, {1, maxQuoteDuration + 1}, {math.MaxInt64, 2}} {
		if _, err := quoteComputeEstimate(input[0], input[1]); err == nil {
			t.Fatalf("accepted invalid input%v", input)
		}
	}
	target := quoteTestTarget("a", 0)
	task := backend.Task{Argv: []string{"program"}, TimeoutSeconds: 100, Retry: &backend.RetryPolicy{MaxAttempts: 10, ExitCodes: []int32{7}}}
	target.SupportsRetry = true
	got, err := quoteAt(task, "USD", backend.TargetHour, nil, []backend.Target{target}, quoteTestTime())
	if err != nil || got.Candidates[0].EstimatedComputeCost != nil || got.Candidates[0].EstimateStatus != EstimateDurationUnspecified {
		t.Fatalf("invented duration: %+v,%v", got, err)
	}
	seconds := int64(1)
	got, err = quoteAt(task, "USD", backend.TargetHour, &seconds, []backend.Target{target}, quoteTestTime())
	if err != nil || got.Candidates[0].EstimatedComputeCost == nil || *got.Candidates[0].EstimatedComputeCost.AmountMicros != 0 {
		t.Fatalf("free estimate not present: %+v,%v", got, err)
	}
	target.Advice.Quote.Unit = backend.CPUHour
	task.CPUMillis = 1000
	got, err = quoteAt(task, "USD", backend.CPUHour, &seconds, []backend.Target{target}, quoteTestTime())
	if err != nil || got.Candidates[0].EstimatedComputeCost != nil || got.Candidates[0].EstimateStatus != EstimateQuantityUnspecified {
		t.Fatalf("request invented billed quantity: %+v,%v", got, err)
	}
}

func TestQuoteFreshnessBoundaries(t *testing.T) {
	p := quoteTestFact(1)
	for _, tc := range []struct {
		now  time.Time
		want PriceStatus
	}{
		{p.ObservedAt.Add(-time.Nanosecond), PriceFuture}, {p.ObservedAt, PriceComparable}, {p.ValidUntil.Add(-time.Nanosecond), PriceComparable}, {p.ValidUntil, PriceStale},
	} {
		if got := quotePriceState(p, "USD", backend.TargetHour, tc.now); got != tc.want {
			t.Fatalf("price at%s=%s want%s", tc.now, got, tc.want)
		}
	}
	c := &backend.CapacitySnapshot{Origin: backend.Configured, State: backend.CapacityAvailable, Source: p.Source, ObservedAt: p.ObservedAt, ValidUntil: p.ValidUntil}
	if quoteCapacityFresh(nil, p.ObservedAt) != FreshnessUnknown || quoteCapacityFresh(c, p.ObservedAt.Add(-time.Nanosecond)) != FreshnessFuture || quoteCapacityFresh(c, p.ObservedAt) != FreshnessFresh || quoteCapacityFresh(c, p.ValidUntil) != FreshnessStale {
		t.Fatal("capacity freshness boundary mismatch")
	}
}

func TestQuoteAdviceValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*backend.TargetAdvice)
	}{
		{"missing amount", func(a *backend.TargetAdvice) { a.Quote.Rate.AmountMicros = nil }},
		{"negative", func(a *backend.TargetAdvice) { a.Quote.Rate.AmountMicros = quoteInt(-1) }},
		{"over rate cap", func(a *backend.TargetAdvice) { a.Quote.Rate.AmountMicros = quoteInt(maxQuoteRate + 1) }},
		{"currency", func(a *backend.TargetAdvice) { a.Quote.Rate.Currency = "usd" }},
		{"unit", func(a *backend.TargetAdvice) { a.Quote.Unit = 0 }},
		{"source", func(a *backend.TargetAdvice) { a.Quote.Source = "https://private.example" }},
		{"future year", func(a *backend.TargetAdvice) {
			a.Quote.ObservedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			a.Quote.ValidUntil = a.Quote.ObservedAt.Add(time.Hour)
		}},
		{"year zero", func(a *backend.TargetAdvice) {
			a.Quote.ObservedAt = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
			a.Quote.ValidUntil = a.Quote.ObservedAt.Add(time.Hour)
		}},
		{"negative year", func(a *backend.TargetAdvice) {
			a.Quote.ObservedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)
			a.Quote.ValidUntil = a.Quote.ObservedAt.Add(time.Hour)
		}},
		{"zero time", func(a *backend.TargetAdvice) {
			a.Quote.ObservedAt = time.Time{}
			a.Quote.ValidUntil = time.Time{}.Add(time.Hour)
		}},
		{"offset", func(a *backend.TargetAdvice) {
			a.Quote.ObservedAt = a.Quote.ObservedAt.In(time.FixedZone("offset", 3600))
			a.Quote.ValidUntil = a.Quote.ValidUntil.In(time.FixedZone("offset", 3600))
		}},
		{"window", func(a *backend.TargetAdvice) {
			a.Quote.ValidUntil = a.Quote.ObservedAt.Add(24*time.Hour + time.Nanosecond)
		}},
		{"reverse", func(a *backend.TargetAdvice) { a.Quote.ValidUntil = a.Quote.ObservedAt }},
		{"capacity count", func(a *backend.TargetAdvice) {
			now := quoteTestTime()
			a.Capacity = &backend.CapacitySnapshot{Origin: backend.Configured, State: backend.CapacityError, AvailableSlots: quoteSlots(0), Source: "operator:cap", ObservedAt: now, ValidUntil: now.Add(time.Hour)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &backend.TargetAdvice{Quote: quoteTestFact(0)}
			tc.mutate(a)
			if err := backend.ValidateAdvice(a); err == nil {
				t.Fatal("invalid advice accepted")
			}
		})
	}
	for _, start := range []time.Time{time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(9999, 12, 31, 23, 59, 58, 999999999, time.UTC)} {
		a := &backend.TargetAdvice{Quote: quoteTestFact(0)}
		a.Quote.ObservedAt = start
		a.Quote.ValidUntil = start.Add(time.Second)
		if err := backend.ValidateAdvice(a); err != nil {
			t.Fatalf("boundary rejected: %v", err)
		}
	}
	if backend.ValidateAdvice(nil) != nil || backend.ValidateAdvice(&backend.TargetAdvice{}) == nil {
		t.Fatal("nil/empty advice distinction lost")
	}
}

func TestQuoteInputAndResultIndependence(t *testing.T) {
	target := quoteTestTarget("a", 1)
	task := backend.Task{Argv: []string{"program"}, Env: map[string]string{"A": "B"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{9, 7, 9}}}
	target.SupportsRetry = true
	target.Backend = &quoteCheckBackend{check: func(got backend.Task) error {
		got.Argv[0] = "changed"
		got.Env["A"] = "changed"
		got.Retry.ExitCodes[0] = 8
		return nil
	}}
	seconds := int64(3600)
	got, err := quoteAt(task, "USD", backend.TargetHour, &seconds, []backend.Target{target}, quoteTestTime())
	if err != nil {
		t.Fatal(err)
	}
	if task.Argv[0] != "program" || task.Env["A"] != "B" || !reflect.DeepEqual(task.Retry.ExitCodes, []int32{9, 7, 9}) || task.Target != "" {
		t.Fatal("caller task was changed")
	}
	*got.Candidates[0].Quote.Rate.AmountMicros = 10
	*got.BillableDurationSeconds = 1
	if *target.Advice.Quote.Rate.AmountMicros != 1 || seconds != 3600 {
		t.Fatal("result retained input pointers")
	}
	*target.Advice.Quote.Rate.AmountMicros = 20
	if *got.Candidates[0].EstimatedComputeCost.AmountMicros != 1 {
		t.Fatal("estimate retained rate pointer")
	}
}

func TestQuoteTargetValidationAndDiagnostics(t *testing.T) {
	b := &quoteCheckBackend{}
	for _, name := range []string{strings.Repeat("x", 257), string([]byte{0xff})} {
		target := quoteTestTarget(name, 1)
		target.Backend = b
		if _, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, []backend.Target{target}, quoteTestTime()); !errors.Is(err, ErrQuoteMetadata) || b.calls != 0 {
			t.Fatalf("invalid name side effect/error: %v calls%d", err, b.calls)
		}
	}
	for _, text := range []string{"", string([]byte{0xff}), strings.Repeat("雪", 100), strings.Repeat("\x01", 256)} {
		target := quoteTestTarget("a", 1)
		target.Backend = &quoteCheckBackend{check: func(backend.Task) error { return errors.New(text) }}
		got, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, []backend.Target{target}, quoteTestTime())
		if err != nil || got.Outcome != QuoteNoEligibleTarget || got.Candidates[0].Eligible || !utf8.ValidString(got.Candidates[0].Rejection) || len(got.Candidates[0].Rejection) > 256 || got.Candidates[0].Rejection == "" {
			t.Fatalf("diagnostic=%+v,%v", got, err)
		}
	}
}

// This compares the new concrete predicate with the actual ordinary Plan
// path rather than a second expected-eligibility implementation.
func TestQuoteHardConstraintsMatchPlan(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	target := quoteTestTarget("a", 0)
	target.Labels = map[string]string{"pool": ""}
	target.Backend = &quoteCheckBackend{check: func(task backend.Task) error {
		if task.CPUMillis > 2000 {
			return errors.New("cpu quantity exceeds target policy")
		}
		return nil
	}}
	c, err := New(s, []backend.Target{target}, engine)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []backend.Task{
		{Argv: []string{"program"}},
		{Argv: []string{"program"}, Target: "other"},
		{Argv: []string{"program"}, TargetLabels: map[string]string{"missing": ""}},
		{Argv: []string{"program"}, TargetLabels: map[string]string{"pool": ""}},
		{Argv: []string{"program"}, CPUMillis: 3000},
		{Argv: []string{"program"}, CPUMillis: 1, CPULimitMillis: 2},
		{Argv: []string{"program"}, NodeSelector: map[string]string{"zone": "west"}},
		{Argv: []string{"program"}, Retry: &backend.RetryPolicy{MaxAttempts: 2, ExitCodes: []int32{7}}},
	} {
		_, _, oldErr := c.Plan(actor, task)
		got, newErr := quoteAt(task, "USD", backend.TargetHour, nil, []backend.Target{target}, quoteTestTime())
		if newErr != nil {
			t.Fatal(newErr)
		}
		if (oldErr == nil) != got.Candidates[0].Eligible {
			t.Fatalf("predicate differs for%+v: plan%v quote%+v", task, oldErr, got)
		}
	}
}

func TestQuoteRequestAndTargetBounds(t *testing.T) {
	target := quoteTestTarget("a", 1)
	for _, tc := range []struct {
		currency string
		unit     backend.RateUnit
		seconds  *int64
	}{
		{"usd", backend.TargetHour, nil}, {"US", backend.TargetHour, nil}, {"USD", 0, nil}, {"USD", 255, nil},
		{"USD", backend.TargetHour, quoteInt(0)}, {"USD", backend.TargetHour, quoteInt(-1)}, {"USD", backend.TargetHour, quoteInt(maxQuoteDuration + 1)},
	} {
		if _, err := quoteAt(backend.Task{Argv: []string{"program"}}, tc.currency, tc.unit, tc.seconds, []backend.Target{target}, quoteTestTime()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid request accepted: %+v,%v", tc, err)
		}
	}
	var targets []backend.Target
	for i := 0; i < 32; i++ {
		targets = append(targets, quoteTestTarget(fmt.Sprintf("t%02d", i), int64(32-i)))
	}
	got, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, targets, quoteTestTime())
	if err != nil || got.SelectedTarget != "t31" || got.ComparisonCount != 32 || len(got.Candidates) != 32 {
		t.Fatalf("maximum targets: %+v,%v", got, err)
	}
	targets = append(targets, quoteTestTarget("extra", 0))
	if _, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, targets, quoteTestTime()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("33 targets accepted: %v", err)
	}
}

func TestQuoteCapacityCopyAndStatus(t *testing.T) {
	now := quoteTestTime()
	target := quoteTestTarget("a", 1)
	target.Advice.Capacity = &backend.CapacitySnapshot{Origin: backend.Configured, State: backend.CapacityAvailable, AvailableSlots: quoteSlots(3), Source: "operator:cap", ObservedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Hour)}
	got, err := quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, []backend.Target{target}, now)
	if err != nil || got.Candidates[0].CapacityWarning || got.Candidates[0].CapacityFreshness != FreshnessFresh {
		t.Fatalf("capacity=%+v,%v", got, err)
	}
	*got.Candidates[0].Capacity.AvailableSlots = 4
	if *target.Advice.Capacity.AvailableSlots != 3 {
		t.Fatal("capacity pointer alias retained")
	}
	target.Advice.Capacity.ValidUntil = now
	got, err = quoteAt(backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil, []backend.Target{target}, now)
	if err != nil || got.Candidates[0].CapacityFreshness != FreshnessStale || !got.Candidates[0].CapacityWarning || got.SelectedTarget != "a" {
		t.Fatalf("stale capacity changed objective: %+v,%v", got, err)
	}
}

func TestQuoteConstructorAdviceCopy(t *testing.T) {
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	engine := auth.NewLegacy("")
	actor, err := engine.Authenticate("")
	if err != nil {
		t.Fatal(err)
	}
	target := quoteTestTarget("a", 1)
	c, err := New(s, []backend.Target{target}, engine)
	if err != nil {
		t.Fatal(err)
	}
	*target.Advice.Quote.Rate.AmountMicros = 9
	targets, err := c.Targets(actor)
	if err != nil {
		t.Fatal(err)
	}
	if *targets[0].Advice.Quote.Rate.AmountMicros != 1 {
		t.Fatal("constructor retained price pointer")
	}
	*targets[0].Advice.Quote.Rate.AmountMicros = 8
	again, err := c.Targets(actor)
	if err != nil {
		t.Fatal(err)
	}
	if *again[0].Advice.Quote.Rate.AmountMicros != 1 {
		t.Fatal("Targets exposed price pointer")
	}
	bad := quoteTestTarget("bad", 1)
	bad.Advice.Quote.Rate.AmountMicros = nil
	if _, err := New(s, []backend.Target{bad}, engine); err == nil {
		t.Fatal("constructor accepted malformed advice")
	}
	if _, err := c.QuoteTask(auth.Actor{}, backend.Task{Argv: []string{"program"}}, "USD", backend.TargetHour, nil); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("zero actor: %v", err)
	}
}
