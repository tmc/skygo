package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// UNRUN. This exercises bbolt.Open refusal before Store initialization. It
// establishes no injected kernel-close error, retained descriptor or lock fact.
func TestOpenMalformedDatabasePreservesCauseAndBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "jobs.db")
	before := []byte("not a bbolt database")
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir)
	if err == nil {
		s.Close()
		t.Fatal("malformed database opened")
	}
	if s != nil || !errors.Is(err, bolt.ErrInvalid) || !strings.HasPrefix(err.Error(), "open job database: ") {
		t.Fatalf("refusal lost dependency cause or context: store=%v error=%v", s, err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("refused open rewrote malformed database")
	}
}
