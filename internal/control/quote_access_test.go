package control_test

import (
	"errors"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

func TestQuoteTaskReadAuthority(t *testing.T) {
	f := newScopedControl(t)
	actor := f.actors["reader"]
	before, err := f.s.List(0, ^uint64(0), 100)
	if err != nil {
		t.Fatal(err)
	}
	task := backend.Task{Argv: []string{"work"}}
	for _, who := range []auth.Actor{actor, f.actors["alice"]} {
		p, err := f.c.QuoteTask(who, task, "USD", backend.TargetHour, nil)
		if err != nil || len(p.Candidates) != 1 || p.SelectedTarget != "" || p.UnpricedEligibleCount != 1 {
			t.Fatalf("read plan: %+v %v", p, err)
		}
	}
	after, err := f.s.List(0, ^uint64(0), 100)
	if err != nil || len(after) != len(before) {
		t.Fatalf("quote changed jobs: %v", err)
	}
	if err := auth.Revoke(f.config, actor.TokenID()); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := f.e.Check(actor, auth.Read); errors.Is(err, auth.ErrUnauthenticated) {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("revocation refresh did not arrive")
		case <-tick.C:
		}
	}
	if _, err := f.c.QuoteTask(actor, task, "USD", backend.TargetHour, nil); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("revoked quote: %v", err)
	}
}
