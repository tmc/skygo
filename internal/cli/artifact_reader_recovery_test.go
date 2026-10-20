package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"rsc.io/script"
	"rsc.io/script/scripttest"
)

// These callback checks cover CLI grammar and forwarding only. The original
// canceled context prevents advisory physical probing; no startup is emulated.
func TestServerStoppedReaderRecoveryOptions(t *testing.T) {
	for _, tt := range []struct {
		name  string
		flags []string
		want  bool
	}{
		{"default", nil, false},
		{"explicit false", []string{"--recover-stopped-artifact-readers=false"}, false},
		{"enabled", []string{"--recover-stopped-artifact-readers", "--artifact-dir", "initialized root"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			sentinel := errors.New("original startup callback")
			calls := 0
			cmd := New(func(received context.Context, got ServerOptions) error {
				calls++
				if received != ctx || !errors.Is(received.Err(), context.Canceled) {
					t.Fatal("original context replaced")
				}
				if got.RecoverStoppedArtifactReaders != tt.want {
					t.Fatalf("recovery option=%v want=%v", got.RecoverStoppedArtifactReaders, tt.want)
				}
				if tt.want && got.ArtifactDir != "initialized root" {
					t.Fatalf("artifact directory=%q", got.ArtifactDir)
				}
				return sentinel
			})
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs(append([]string{"server", "--data-dir", "unused recovery callback state"}, tt.flags...))
			if err := cmd.ExecuteContext(ctx); err != sentinel || calls != 1 {
				t.Fatalf("callback calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestServerStoppedReaderRecoveryHelp(t *testing.T) {
	calls := 0
	cmd := New(func(context.Context, ServerOptions) error { calls++; return nil })
	server, _, err := cmd.Find([]string{"server"})
	if err != nil {
		t.Fatal(err)
	}
	flag := server.Flags().Lookup("recover-stopped-artifact-readers")
	if flag == nil || flag.DefValue != "false" || flag.NoOptDefVal != "true" || flag.Value.Type() != "bool" {
		t.Fatalf("recovery flag must be an opt-in boolean: %+v", flag)
	}
	for _, premise := range []string{"ALL historical producers", "no surviving descriptor copy or use", "same-domain kernel is qualified"} {
		if !strings.Contains(flag.Usage, premise) {
			t.Errorf("recovery help missing %q: %s", premise, flag.Usage)
		}
	}
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"server", "--help"})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 || !strings.Contains(output.String(), "--recover-stopped-artifact-readers") {
		t.Fatalf("help started callback or omitted recovery flag: calls=%d output=%s", calls, output.String())
	}
}

// The script runs the source-qualified production binary, not TestCLIHelper's
// nil-startup path. No server, Local, listener or native proof is stubbed. The
// early refusal must leave its data directory absent. Default/false cases use
// an invalid listener to check normal validation without starting any service.
func TestArtifactReaderRecoveryFlagScript(t *testing.T) {
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	var binaryEnv []string
	if mode := os.Getenv("SKYGO_ARTIFACT_BINARY_COVERMODE"); mode != "" {
		if mode != testing.CoverMode() || (mode != "set" && mode != "count" && mode != "atomic") {
			t.Fatal("artifact fixture coverage mode does not match the instrumented test binary")
		}
		if dir := coverDir(t); dir != "" {
			binaryEnv = append(binaryEnv, "GOCOVERDIR="+dir)
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(home, "tmp", "skypilot", "20261020-artifact-reader-flag-tests")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	base, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("retaining failed reader flag script: %s", base)
			return
		}
		if err := os.RemoveAll(base); err != nil {
			t.Errorf("remove completed reader flag case: %v", err)
		}
	})
	file := "testdata/artifact_reader_recovery.txt"
	body, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	env := append(os.Environ(), "SKYGO_TOKEN=", "SKYGO_ENDPOINT=invalid endpoint")
	s, err := script.NewState(ctx, base, env)
	if err != nil {
		t.Fatal(err)
	}
	scripttest.Run(t, executableScriptEngine(exe, nil, binaryEnv), s, file, bytes.NewReader(body))
}
