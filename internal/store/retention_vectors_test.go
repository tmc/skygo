package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tmc/skygo/internal/backend"
)

func TestMaximumEncodedRetentionRecords(t *testing.T) {
	task := backend.Task{Argv: []string{""}}
	b, e := backend.TaskBytes(task)
	if e != nil {
		t.Fatal(e)
	}
	task.Argv[0] = strings.Repeat("x", (256<<10)-len(b))
	b, e = backend.TaskBytes(task)
	if e != nil || len(b) != 256<<10 {
		t.Fatalf("max task %d %v", len(b), e)
	}
	ids := make([]string, 10)
	for i := range ids {
		ids[i] = fmt.Sprintf("%032x", i+1)
	}
	d, e := taskDigest(task)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 10, 19, 1, 2, 3, 999999999, time.UTC)
	h := cleanupHeader{ID: ids[0], Sequence: ^uint64(0), JobID: ids[0], Principal: strings.Repeat("a", 64), Target: strings.Repeat("\x01", 128), Binding: strings.Repeat("\x01", 256), Task: &task, TaskDigest: d, ExecutionIDs: ids, Remaining: 20, CreatedAt: now}
	raw, e := json.Marshal(h)
	if e != nil || len(raw) > cleanupHeaderLimit {
		t.Fatalf("max header %d %v", len(raw), e)
	}
	t.Logf("native header=%d cap=%d", len(raw), cleanupHeaderLimit)
	h.Task = nil
	h.NeedsManual = true
	raw, e = json.Marshal(h)
	if e != nil || len(raw) > manualHeaderLimit {
		t.Fatalf("manual header %d %v", len(raw), e)
	}
	ref := backend.OutputReference{Owner: ids[0], JobID: ids[0], AttemptID: ids[1], Binding: h.Binding, Generation: 1, ClosureID: strings.Repeat("<", 128), PayloadReleased: true}
	op := cleanupOperation{ID: ids[2], HeaderID: h.ID, Sequence: ^uint64(0), Operation: OutputCleanup, AttemptID: ids[1], ClosureID: ref.ClosureID, Generation: ref.Generation, PayloadReleased: ref.PayloadReleased, LastError: "cleanup_failed", UpdatedAt: now}
	raw, e = json.Marshal(op)
	if e != nil || len(raw) > cleanupOperationLimit {
		t.Fatalf("output operation %d %v", len(raw), e)
	}
	code := int32(255)
	proof := backend.Status{State: backend.Failed, ExitCode: &code, Termination: backend.StoppedWithExitCode, Instance: strings.Repeat("<", 128)}
	op.Operation = NativeCleanup
	op.ClosureID = ""
	op.Generation = 0
	op.PayloadReleased = false
	op.Stopped = true

	op.Proof = &proof
	raw, e = json.Marshal(op)
	if e != nil || len(raw) > cleanupOperationLimit {
		t.Fatalf("native operation %d %v", len(raw), e)
	}
	t.Logf("native operation=%d cap=%d", len(raw), cleanupOperationLimit)
}
func TestS05BinaryRefusesSchema5Unchanged(t *testing.T) {
	exe, want := os.Getenv("SKYGO_S05_BINARY"), os.Getenv("SKYGO_S05_BINARY_SHA256")
	if exe == "" && want == "" && os.Getenv("SKYGO_REQUIRE_FIXTURES") == "" {
		t.Skip("requires SKYGO_S05_BINARY and SKYGO_S05_BINARY_SHA256 (set SKYGO_REQUIRE_FIXTURES=1 to fail instead)")
	}
	if exe == "" || want == "" {
		t.Fatal("provide both SKYGO_S05_BINARY and SKYGO_S05_BINARY_SHA256")
	}
	if !filepath.IsAbs(exe) {
		t.Fatal("SKYGO_S05_BINARY must be an absolute path")
	}
	binary, e := os.ReadFile(exe)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(binary)
	if want != hex.EncodeToString(sum[:]) {
		t.Fatal("prior binary hash mismatch")
	}
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "jobs.db")
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "server", "--listen", "127.0.0.1:0", "--data-dir", dir)
	cmd.Env = append(os.Environ(), "SKYGO_TOKEN=")
	out, e := cmd.CombinedOutput()
	if e == nil || ctx.Err() != nil || !bytes.Contains(out, []byte("unsupported database schema")) {
		t.Fatalf("old binary did not promptly refuse schema5: %v %s", e, out)
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(before, after) {
		t.Fatalf("old binary modified schema5 %v", e)
	}
}

// This is a conservative schema envelope: scalar maxima need not coexist in a
// real row. It bounds the private offline corpus independently of Task size.
func TestMaximumLegacyIdentitySideRecordEnvelope(t *testing.T) {
	raw := bytes.Repeat([]byte{0xff}, 32768)
	m := retainedJob{Principal: strings.Repeat("a", 64), Domain: RequestDomain{Lineage: strings.Repeat("b", 32)}, LegacyRequestID: raw, Storage: backend.LocalOutput, OutputReserve: ^uint64(0), Overhead: ^uint64(0), Legacy: true, Started: true, OutputStart: 10, Ordinal: ^uint64(0)}
	b, e := json.Marshal(m)
	if e != nil || len(b) > 45<<10 {
		t.Fatalf("legacy side envelope %d %v", len(b), e)
	}
	r := requestRecord{Principal: m.Principal, Domain: m.Domain, LegacyRequestID: raw, JobID: strings.Repeat("c", 32), TaskDigest: [32]byte{255}, LegacyBytes: ^uint64(0)}
	b, e = json.Marshal(r)
	if e != nil || len(b) > 45<<10 {
		t.Fatalf("legacy request envelope %d %v", len(b), e)
	}
}
