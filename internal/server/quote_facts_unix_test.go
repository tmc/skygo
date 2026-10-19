//go:build linux || darwin

package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReadTargetFactsRefusesFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "facts.fifo")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readTargetFacts(path); err == nil {
		t.Fatal("FIFO accepted")
	}
}

// Re-execution bounds the regression without leaving a blocked FIFO reader in
// this process if the production open accidentally loses O_NONBLOCK.
func TestReadTargetFactsReplacedFIFO(t *testing.T) {
	if path := os.Getenv("SKYGO_TEST_FACTS_REPLACEMENT"); path != "" {
		_, err := readTargetFactsWithOpen(path, func(path string) (*os.File, error) {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mkfifo(path, 0600); err != nil {
				t.Fatal(err)
			}
			return openTargetFacts(path)
		})
		if err == nil || err.Error() != "target facts file changed" {
			t.Fatalf("replacement error: %v", err)
		}
		return
	}
	path := filepath.Join(t.TempDir(), "facts.yaml")
	if err := os.WriteFile(path, []byte(sampleTargetFacts), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReadTargetFactsReplacedFIFO$")
	cmd.Env = append(os.Environ(), "SKYGO_TEST_FACTS_REPLACEMENT="+path)
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("FIFO replacement blocked: %v", ctx.Err())
	}
	if err != nil {
		t.Fatalf("FIFO replacement: %v\n%s", err, output)
	}
}
