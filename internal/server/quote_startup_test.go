package server_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/server"
)

func TestQuoteFactsRejectedBeforeStore(t *testing.T) {
	dir := t.TempDir()
	facts := filepath.Join(dir, "invalid.yaml")
	data := filepath.Join(dir, "uncreated-state")
	if err := os.WriteFile(facts, []byte("version: 2\ntargets: []\n"), 0600); err != nil {
		t.Fatal(err)
	}
	err := server.Run(context.Background(), server.Config{DataDir: data, Listen: "127.0.0.1:0", TargetFacts: facts})
	if err == nil {
		t.Fatal("invalid facts accepted")
	}
	if _, err := os.Stat(data); !os.IsNotExist(err) {
		t.Fatalf("store created before facts parse: %v", err)
	}
}

func TestQuoteFactsStartupSnapshot(t *testing.T) {
	dir := t.TempDir()
	facts := filepath.Join(dir, "facts.yaml")
	now := time.Now().UTC()
	text := "version: 1\ntargets:\n- name: local\n  quote:\n    amount_micros: 0\n    currency: USD\n    unit: target-hour\n    source: operator:startup\n    observed_at: \"" + now.Add(-time.Minute).Format(time.RFC3339Nano) + "\"\n    valid_until: \"" + now.Add(time.Hour).Format(time.RFC3339Nano) + "\"\n"
	if err := os.WriteFile(facts, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	running := startConfig(t, server.Config{DataDir: filepath.Join(dir, "state"), TargetFacts: facts})
	if err := os.Remove(facts); err != nil {
		t.Fatal(err)
	}
	client := skyv1connect.NewControlServiceClient(http.DefaultClient, running.url)
	result, err := client.QuoteTask(context.Background(), connect.NewRequest(&v1.QuoteTaskRequest{Task: &v1.Task{Argv: []string{"echo", "quote"}}, Currency: "USD", Unit: v1.RateUnit_RATE_UNIT_TARGET_HOUR}))
	if err != nil || result.Msg.SelectedTarget != "local" || result.Msg.Candidates[0].Quote.Rate.AmountMicros == nil || *result.Msg.Candidates[0].Quote.Rate.AmountMicros != 0 {
		t.Fatalf("startup snapshot: %v %v", result, err)
	}
}
