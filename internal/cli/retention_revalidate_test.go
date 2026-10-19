package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/store"
)

func TestRetentionRevalidateRequiresStoppedWriters(t *testing.T) {
	for _, flags := range [][]string{nil, {"--ack-local-writers-stopped=false"}} {
		dir := filepath.Join(t.TempDir(), "absent")
		args := append([]string{"state", "retention", "revalidate", "--data-dir", dir}, flags...)
		out, _, err := execute(context.Background(), args...)
		if err == nil || !strings.Contains(err.Error(), "requires --ack-local-writers-stopped") {
			t.Fatalf("missing acknowledgment error=%v", err)
		}
		if out != "" {
			t.Fatalf("rejected acknowledgment wrote readback: %q", out)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("unacknowledged command accessed state: stat=%v", err)
		}
	}
}

func TestRetentionRevalidateRefusesDisabledState(t *testing.T) {
	dir := t.TempDir()
	s, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "jobs.db")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := execute(context.Background(), "state", "retention", "revalidate", "--data-dir", dir, "--ack-local-writers-stopped")
	if err == nil || out != "" {
		t.Fatalf("disabled revalidation stdout=%q error=%v", out, err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("disabled revalidation changed database bytes")
	}
	info, err := store.InspectRetention(dir)
	if err != nil || info.Enabled {
		t.Fatalf("disabled retention changed: enabled=%t error=%v", info.Enabled, err)
	}
}

func TestRetentionRevalidateReadback(t *testing.T) {
	dir := t.TempDir()
	before, err := store.EnableRetention(dir, store.DefaultRetentionPolicy())
	if err != nil {
		t.Fatal(err)
	}
	out, _, err := execute(context.Background(), "--json", "state", "retention", "revalidate", "--data-dir", dir, "--ack-local-writers-stopped")
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.InspectRetention(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("same-volume empty revalidation changed retained state")
	}
	var readback struct {
		Path               string `json:"path"`
		Enabled            bool   `json:"enabled"`
		RequestDomain      string `json:"request_domain"`
		RestartRequired    bool   `json:"restart_required"`
		LocalOutputEnabled bool   `json:"local_output_enabled"`
	}
	if err := json.Unmarshal([]byte(out), &readback); err != nil {
		t.Fatal(err)
	}
	if readback.Path != dir || !readback.Enabled || !readback.RestartRequired || !readback.LocalOutputEnabled || readback.RequestDomain != before.Domain.Lineage+":1" {
		t.Fatalf("incorrect revalidation readback: %s", out)
	}
}
