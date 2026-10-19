package server

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const sampleTargetFacts = `version: 1
targets:
- name: a
  quote:
    amount_micros: 0
    currency: USD
    unit: target-hour
    source: operator:test
    observed_at: "2026-10-18T12:00:00Z"
    valid_until: "2026-10-18T13:00:00Z"
  capacity:
    state: available
    source: operator:capacity
    observed_at: "2026-10-18T12:00:00Z"
    valid_until: "2026-10-18T12:05:00Z"
`

func TestParseTargetFacts(t *testing.T) {
	got, err := parseTargetFacts([]byte(sampleTargetFacts))
	if err != nil {
		t.Fatal(err)
	}
	p := got["a"].Quote
	if len(got) != 1 || p == nil || p.Rate.AmountMicros == nil || *p.Rate.AmountMicros != 0 || p.Rate.Currency != "USD" || p.Unit != backend.TargetHour || p.ObservedAt.IsZero() || p.ValidUntil.Sub(p.ObservedAt) != time.Hour {
		t.Fatalf("parsed=%+v quote=%+v", got, p)
	}
	if got["a"].Capacity == nil || got["a"].Capacity.AvailableSlots != nil {
		t.Fatal("unknown count became a zero")
	}
	if err := validateTargetFactNames(got, []backend.Target{{Name: "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := validateTargetFactNames(got, []backend.Target{{Name: "b"}}); err == nil {
		t.Fatal("unknown target accepted")
	}
	unquoted := strings.ReplaceAll(sampleTargetFacts, `"`, "")
	if _, err := parseTargetFacts([]byte(unquoted)); err != nil {
		t.Fatalf("unquoted RFC3339 timestamps: %v", err)
	}
}

func TestInvalidTargetFacts(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement, want string }{
		{"missing amount", "    amount_micros: 0\n", "", "invalid quote amount"},
		{"negative", "amount_micros: 0", "amount_micros: -1", "integer"},
		{"float", "amount_micros: 0", "amount_micros: 0.0", "integer"},
		{"exponent", "amount_micros: 0", "amount_micros: 1e2", "integer"},
		{"hex", "amount_micros: 0", "amount_micros: 0x10", "integer"},
		{"octal", "amount_micros: 0", "amount_micros: 010", "integer"},
		{"overflow", "amount_micros: 0", "amount_micros: 9223372036854775808", "unknown field or invalid value"},
		{"over rate cap", "amount_micros: 0", "amount_micros: 1000000000001", "invalid quote amount"},
		{"null amount", "amount_micros: 0", "amount_micros: null", "integer"},
		{"unknown field", "    amount_micros: 0", "    unexpected: marker-secret\n    amount_micros: 0", "unknown field"},
		{"duplicate field", "    amount_micros: 0", "    amount_micros: 1\n    amount_micros: 0", "duplicate"},
		{"coerced currency", "currency: USD", "currency: 123", "scalar"},
		{"currency", "currency: USD", "currency: usd", "metadata"},
		{"unit", "unit: target-hour", "unit: minute", "metadata"},
		{"source", "source: operator:test", "source: https://marker-secret.example", "metadata"},
		{"missing source", "    source: operator:test\n", "", "metadata"},
		{"date only", `"2026-10-18T12:00:00Z"`, `2026-10-18`, "timestamp"},
		{"offset", `2026-10-18T12:00:00Z`, `2026-10-18T12:00:00+00:00`, "timestamp"},
		{"year zero", `2026-10-18T12:00:00Z`, `0000-10-18T12:00:00Z`, "timestamp"},
		{"year overflow", `2026-10-18T12:00:00Z`, `10000-10-18T12:00:00Z`, "timestamp"},
		{"wide window", `2026-10-18T13:00:00Z`, `2026-10-19T12:00:01Z`, "validity window"},
		{"reverse window", `2026-10-18T13:00:00Z`, `2026-10-18T12:00:00Z`, "validity window"},
		{"available zero", "    state: available", "    state: available\n    available_slots: 0", "capacity count"},
		{"error count", "    state: available", "    state: error\n    available_slots: 1", "capacity count"},
		{"unavailable positive", "    state: available", "    state: unavailable\n    available_slots: 1", "capacity count"},
		{"slot maximum", "    state: available", "    state: available\n    available_slots: 1000001", "capacity count"},
		{"unknown state", "state: available", "state: ready", "capacity state"},
		{"bad version", "version: 1", "version: 2", "version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := strings.ReplaceAll(sampleTargetFacts, tc.old, tc.replacement)
			_, err := parseTargetFacts([]byte(data))
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "marker-secret") {
				t.Fatalf("error=%v; want category%s", err, tc.want)
			}
		})
	}
	for _, data := range []string{
		"version: 1\ntargets: [{name: a}]\n",
		"version: 1\ntargets: [{name: a, quote: null}]\n",
		"version: 1\ntargets: &targets [{name: a}]\n",
		"version: 1\ntargets: *targets\n",
		sampleTargetFacts + "---\nversion: 1\n",
		strings.Replace(sampleTargetFacts, "quote:", "quote: &price", 1),
		strings.Replace(sampleTargetFacts, "quote:", "quote: !custom", 1),
	} {
		if _, err := parseTargetFacts([]byte(data)); err == nil {
			t.Fatal("invalid structure accepted")
		}
	}
}

func TestTargetFactsBoundaries(t *testing.T) {
	for _, tc := range []struct{ observed, until string }{
		{"0001-01-01T00:00:00.000000001Z", "0001-01-01T00:00:01.000000001Z"},
		{"9999-12-31T23:59:58.999999999Z", "9999-12-31T23:59:59.999999999Z"},
	} {
		data := strings.ReplaceAll(sampleTargetFacts, "2026-10-18T12:00:00Z", tc.observed)
		data = strings.ReplaceAll(data, "2026-10-18T13:00:00Z", tc.until)
		data = strings.ReplaceAll(data, "2026-10-18T12:05:00Z", tc.until)
		got, err := parseTargetFacts([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		for _, instant := range []time.Time{got["a"].Quote.ObservedAt, got["a"].Quote.ValidUntil, got["a"].Capacity.ObservedAt, got["a"].Capacity.ValidUntil} {
			if err := timestamppb.New(instant).CheckValid(); err != nil {
				t.Fatalf("accepted timestamp not wire representable: %v", err)
			}
		}
	}
	atLimit := sampleTargetFacts + strings.Repeat(" ", maxTargetFactsBytes-len(sampleTargetFacts))
	if _, err := parseTargetFacts([]byte(atLimit)); err != nil {
		t.Fatalf("exact byte limit rejected: %v", err)
	}
	if _, err := parseTargetFacts([]byte(atLimit + " ")); err == nil {
		t.Fatal("byte overflow accepted")
	}
	manyNodes := "version: 1\nextra: [" + strings.Repeat("x,", 2050) + "x]\n"
	if _, err := parseTargetFacts([]byte(manyNodes)); err == nil || !strings.Contains(err.Error(), "node limit") {
		t.Fatalf("node budget: %v", err)
	}
	deep := "version: 1\nextra: " + strings.Repeat("[", 10) + "x" + strings.Repeat("]", 10) + "\n"
	if _, err := parseTargetFacts([]byte(deep)); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("depth budget: %v", err)
	}
	var doc strings.Builder
	doc.WriteString("version: 1\ntargets:\n")
	for i := 0; i < 33; i++ {
		doc.WriteString("- {name: a}\n")
	}
	if _, err := parseTargetFacts([]byte(doc.String())); err == nil || !strings.Contains(err.Error(), "1..32") {
		t.Fatalf("entry budget: %v", err)
	}
}

func TestReadTargetFactsRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "facts.yaml")
	if err := os.WriteFile(path, []byte(sampleTargetFacts), 0600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		if _, err := readTargetFacts(path); err == nil || err.Error() != "target facts files are unsupported on this platform" {
			t.Fatalf("unsupported platform error: %v", err)
		}
		return
	}
	if _, err := readTargetFacts(path); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{link, dir, filepath.Join(dir, "missing")} {
		if _, err := readTargetFacts(p); err == nil || strings.Contains(err.Error(), dir) {
			t.Fatalf("nonregular error=%v", err)
		}
	}
}

func TestTargetFactsTimestampSyntax(t *testing.T) {
	for _, value := range []string{
		"2026-10-18T12:00:00,1Z", "2026-10-18T1:00:00Z",
		"2026-10-18T1:00:00.1234567890Z", "2026-10-18T12:00:00.1234567890Z",
		"2026-10-18T12:00:00.Z", "2026-10-18t12:00:00Z", "2026-10-18T12:00:00z",
	} {
		t.Run(value, func(t *testing.T) {
			if _, err := factsTime(value); err == nil {
				t.Fatal("invalid timestamp accepted")
			}
			data := strings.ReplaceAll(sampleTargetFacts, "2026-10-18T12:00:00Z", value)
			if _, err := parseTargetFacts([]byte(data)); err == nil {
				t.Fatal("invalid quoted timestamp accepted")
			}
			if _, err := parseTargetFacts([]byte(strings.ReplaceAll(data, `"`, ""))); err == nil {
				t.Fatal("invalid unquoted timestamp accepted")
			}
		})
	}
	for digits := 1; digits <= 9; digits++ {
		value := "2026-10-18T12:00:00." + strings.Repeat("0", digits) + "Z"
		if _, err := factsTime(value); err != nil {
			t.Fatalf("valid fraction %s: %v", value, err)
		}
		data := strings.ReplaceAll(sampleTargetFacts, "2026-10-18T12:00:00Z", value)
		for _, data := range []string{data, strings.ReplaceAll(data, `"`, "")} {
			if _, err := parseTargetFacts([]byte(data)); err != nil {
				t.Fatalf("valid YAML fraction %s: %v", value, err)
			}
		}
	}
}
