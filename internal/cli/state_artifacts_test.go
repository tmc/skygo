package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/server"
	"rsc.io/script/scripttest"
)

func TestArtifactInitGrammarBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{"missing-root", []string{"--ack-writers-stopped"}, "required flag"},
		{"missing-ack", []string{"--artifact-dir", "not-created"}, "--ack-writers-stopped"},
		{"extra-argument", []string{"--artifact-dir", "not-created", "--ack-writers-stopped", "extra"}, "unknown command"},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := filepath.Join(t.TempDir(), "state")
			args := append([]string{"state", "artifacts", "init", "--data-dir", data}, test.args...)
			out, _, err := execute(context.Background(), args...)
			if err == nil || out != "" || !strings.Contains(err.Error(), test.want) {
				t.Fatal(out, err)
			}
			if _, err := os.Lstat(data); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("grammar failure created state", err)
			}
		})
	}
}

func TestArtifactInitReadbackContainsOnlyPolicy(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	base := t.TempDir()
	root, data := filepath.Join(base, "artifacts"), filepath.Join(base, "state")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	// State descendants bypass remote credentials; no endpoint is contacted.
	t.Setenv("SKYGO_TOKEN", "invalid bearer value")
	out, _, err := execute(context.Background(), "--json", "--endpoint", "invalid endpoint", "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got["enabled"] != true || got["policy"] != "default-v1" || got["restart_required"] != true {
		t.Fatal(got)
	}
	if strings.Contains(out, root) || strings.Contains(out, data) {
		t.Fatal("operator identity leaked", out)
	}
	_, _, err = execute(context.Background(), "state", "artifacts", "init", "--data-dir", data, "--artifact-dir", root, "--ack-writers-stopped")
	var original *server.ArtifactInitializationError
	if !errors.As(err, &original) {
		t.Fatal("CLI discarded typed initialization error", err)
	}
}

func TestArtifactInitScripts(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	scripttest.Test(t, context.Background(), scriptEngine(t), []string{"SKYGO_ENDPOINT=invalid endpoint", "SKYGO_TOKEN=invalid bearer value"}, "testdata/state_artifacts_init.txt")
}
