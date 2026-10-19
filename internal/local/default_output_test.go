package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/backend"
)

func TestDefaultOutputFailureDrains(t *testing.T) {
	b := newBackend(t)
	marker := filepath.Join(t.TempDir(), "release")
	id := strings.Repeat("e", 32)
	task := backend.Task{
		Argv: []string{os.Args[0], "-test.run=^TestRetryProgram$"},
		Env:  map[string]string{"SKYGO_RETRY_PROGRAM": "retention-flood", "SKYGO_RETRY_MARKER": marker},
	}
	// Retry stays nil: this qualifies the default output path with a real helper
	// producing more than a pipe's capacity after its retained file has failed.
	if err := b.Start(context.Background(), id, task); err != nil {
		t.Fatal(err)
	}
	e := ownedExecution(t, b, id)
	e.output.mu.Lock()
	err := e.output.file.Close()
	e.output.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	s := completed(t, b, id)
	if s.State != backend.Failed || s.ExitCode == nil || *s.ExitCode != 7 || !strings.HasPrefix(s.Reason, "retain output: ") || s.Instance != "" || s.Termination != backend.UnknownTermination {
		t.Fatalf("status=%+v", s)
	}
	chunk, err := b.Logs(context.Background(), backend.Reference{ID: id}, 0, backend.MaxChunkBytes)
	if err != nil || len(chunk.Data) != 0 || !chunk.Complete {
		t.Fatalf("logs=%+v %v", chunk, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
}
