package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/store"
)

func TestPlanQuoteRetentionEligibilityParity(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, storage := range []backend.OutputStorage{backend.UnspecifiedOutput, backend.RemoteOutput} {
			t.Run(fmt.Sprintf("enabled=%t/storage=%d", enabled, storage), func(t *testing.T) {
				var s *store.Store
				if enabled {
					s, _ = retainedStore(t)
				} else {
					s = routeStore(t)
				}
				target := quoteTestTarget("only", 1)
				target.OutputStorage = storage
				now := time.Now().UTC()
				target.Advice.Quote.ObservedAt = now.Add(-time.Second)
				target.Advice.Quote.ValidUntil = now.Add(time.Hour)
				c, err := NewWithLocalOutput(s, []backend.Target{target}, testEngine, LocalOutputConfig{})
				if err != nil {
					t.Fatal(err)
				}
				task := backend.Task{Argv: []string{"program"}}
				selected, rejected, planErr := c.Plan(testActor(), task)
				quote, err := c.QuoteTask(testActor(), task, "USD", backend.TargetHour, nil)
				if err != nil || len(quote.Candidates) != 1 {
					t.Fatal(quote, err)
				}
				want := !enabled || storage != backend.UnspecifiedOutput
				if quote.Candidates[0].Eligible != want {
					t.Fatal("quote eligibility differs from retention policy", quote)
				}
				checks := target.Backend.(*quoteCheckBackend).calls
				if want {
					if planErr != nil || selected != target.Name || len(rejected) != 0 || quote.SelectedTarget != selected || checks != 2 {
						t.Fatal("eligible Plan/Quote mismatch", selected, rejected, planErr, quote, checks)
					}
				} else {
					if !errors.Is(planErr, ErrUnsupported) || selected != "" || len(rejected) != 1 || rejected[0].Reason != "output storage is unspecified" || quote.Candidates[0].Rejection != rejected[0].Reason || quote.Outcome != QuoteNoEligibleTarget || checks != 0 {
						t.Fatal("unknown storage reached backend Check or Plan/Quote diverged", selected, rejected, planErr, quote, checks)
					}
				}
				if sequence, err := s.LastSequence(); err != nil || sequence != 0 {
					t.Fatal("advisory plan accepted work", sequence, err)
				}
			})
		}
	}
}

func TestLocalOutputConstructorValidatesAndCopiesAdvice(t *testing.T) {
	s, _ := retainedStore(t)
	policy, _, err := s.RetentionPolicy()
	if err != nil {
		t.Fatal(err)
	}
	target := quoteTestTarget("local", 1)
	target.OutputStorage = backend.LocalOutput
	target.OutputReservation = uint64(backend.MaxLogBytes) + 4*policy.OutputUnit
	target.Retention = &backend.Retention{
		CheckOutputClosed: func(context.Context, backend.OutputReference) (backend.OutputReceipt, error) {
			return backend.OutputReceipt{}, nil
		},
		RemoveOutput: func(context.Context, backend.OutputReference) error { return nil },
	}
	now := quoteTestTime()
	target.Advice.Capacity = &backend.CapacitySnapshot{State: backend.CapacityAvailable, AvailableSlots: quoteSlots(3), Origin: backend.Configured, Source: "operator:capacity", ObservedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Hour)}
	config := LocalOutputConfig{Target: target.Name, Binding: target.Binding, BindOutput: func(backend.OutputReference) error { return nil }, OutputUsage: func(context.Context) (uint64, uint64, error) { return 0, 0, nil }}
	c, err := NewWithLocalOutput(s, []backend.Target{target}, testEngine, config)
	if err != nil {
		t.Fatal(err)
	}
	*target.Advice.Quote.Rate.AmountMicros = 9
	*target.Advice.Capacity.AvailableSlots = 8
	target.Retention.CheckOutputClosed = nil
	targets, err := c.Targets(testActor())
	if err != nil {
		t.Fatal(err)
	}
	if *targets[0].Advice.Quote.Rate.AmountMicros != 1 || *targets[0].Advice.Capacity.AvailableSlots != 3 || targets[0].Retention.CheckOutputClosed == nil || targets[0].Retention.RemoveOutput == nil {
		t.Fatal("local constructor retained caller advice or callback pointers")
	}
	*targets[0].Advice.Quote.Rate.AmountMicros = 7
	*targets[0].Advice.Capacity.AvailableSlots = 6
	targets[0].Retention.RemoveOutput = nil
	again, err := c.Targets(testActor())
	if err != nil || *again[0].Advice.Quote.Rate.AmountMicros != 1 || *again[0].Advice.Capacity.AvailableSlots != 3 || again[0].Retention.RemoveOutput == nil {
		t.Fatal("discovery exposed retained advice or callback pointers", err)
	}
	bad := quoteTestTarget("local", 1)
	bad.OutputStorage, bad.OutputReservation = target.OutputStorage, target.OutputReservation
	bad.Advice.Quote.Rate.AmountMicros = nil
	if _, err := NewWithLocalOutput(s, []backend.Target{bad}, testEngine, config); err == nil || !strings.Contains(err.Error(), "invalid target facts") {
		t.Fatal("local constructor bypassed advice validation", err)
	}
	bad.Advice = backend.CloneAdvice(again[0].Advice)
	bad.Retention = &backend.Retention{RemoveOutput: func(context.Context, backend.OutputReference) error { return nil }}
	if _, err := NewWithLocalOutput(s, []backend.Target{bad}, testEngine, config); err == nil || !strings.Contains(err.Error(), "invalid retention target") {
		t.Fatal("advice validation replaced retention validation", err)
	}
}
