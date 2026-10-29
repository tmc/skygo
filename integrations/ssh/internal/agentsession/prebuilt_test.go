package agentsession

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tmc/skygo/integrations/ssh/internal/agentbinary"
	"golang.org/x/crypto/ssh"
)

func realPrebuilt(t *testing.T) (agentbinary.Artifact, string) {
	t.Helper()
	s := agentbinary.Spec{Path: os.Getenv("SKYGO_SSH_AGENT_BINARY"), Commit: os.Getenv("SKYGO_SSH_AGENT_COMMIT"), Digest: os.Getenv("SKYGO_SSH_AGENT_SHA256")}
	if s == (agentbinary.Spec{}) {
		if os.Getenv("SKYGO_REQUIRE_FIXTURES") == "1" {
			t.Fatal("required prebuilt agent fixture pins missing")
		}
		t.Skip("prebuilt agent fixture not supplied")
	}
	a, err := agentbinary.Prepare(context.Background(), filepath.Join(t.TempDir(), "cache"), s)
	if err != nil {
		t.Fatal(err)
	}
	b, err := a.Build()
	if err != nil {
		t.Fatal(err)
	}
	return a, b.Path
}

func TestPrebuiltCheckedBeforeSigner(t *testing.T) {
	a, path := realPrebuilt(t)
	if err := os.Chmod(path, 0755); err != nil {
		t.Fatal(err)
	}
	calls := 0
	o := Options{Prebuilt: a, signer: func(string, string) (ssh.Signer, io.Closer, error) {
		calls++
		return nil, nil, errors.New("signer entered")
	}}
	if s, err := Open(context.Background(), o); s != nil || err == nil {
		t.Fatalf("Open=%v,%v", s, err)
	}
	if calls != 0 {
		t.Fatal("signer entered before snapshot recheck")
	}
}

func TestPrebuiltSelection(t *testing.T) {
	a, _ := realPrebuilt(t)
	sentinel := errors.New("modeled signer refusal")
	for _, tt := range []struct {
		name      string
		edit      func(*Options)
		wantCalls int
	}{
		{"prebuilt", func(*Options) {}, 1},
		{"source conflict", func(o *Options) { o.Source = "/src" }, 0},
		{"dirty conflict", func(o *Options) { o.AllowDirty = true }, 0},
		{"builder conflict", func(o *Options) { o.build = modelBuild }, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			o := Options{Prebuilt: a, signer: func(string, string) (ssh.Signer, io.Closer, error) { calls++; return nil, nil, sentinel }}
			tt.edit(&o)
			s, err := Open(context.Background(), o)
			if s != nil || err == nil || calls != tt.wantCalls {
				t.Fatalf("Open=%v,%v signer calls=%d", s, err, calls)
			}
			if tt.wantCalls == 1 && !errors.Is(err, sentinel) {
				t.Fatalf("did not reach signer after verifying real fixture: %v", err)
			}
		})
	}
}
