package main

import (
	"context"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

func TestQuoteFactsForwarded(t *testing.T) {
	calls := 0
	cmd := command(func(_ context.Context, cfg server.Config) error {
		calls++
		if cfg.TargetFacts != "facts.yaml" {
			t.Fatalf("target facts=%q", cfg.TargetFacts)
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--target-facts", "facts.yaml"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("startup calls=%d", calls)
	}
}
