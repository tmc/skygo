//go:build darwin || linux || freebsd || openbsd || netbsd || dragonfly

package cli

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWorkspaceJournalNonregular(t *testing.T) {
	r, _ := workspaceTestPrepared(t)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(r.dir, "state.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	failed, err := openWorkspaceRun(r.dir, workspaceIntent{Endpoint: r.intent.Endpoint, CredentialSHA256: r.intent.CredentialSHA256})
	if failed != nil {
		_ = failed.Close()
	}
	if err == nil {
		t.Fatal("opened FIFO state")
	}
}
