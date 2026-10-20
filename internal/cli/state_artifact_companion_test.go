package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"rsc.io/script/scripttest"
)

func TestArtifactCompanionGrammarBeforeMutation(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"backup extra", []string{"backup", "extra", "--data-dir", "state", "--artifact-dir", "root", "--output", "bundle", "--ack-writers-stopped"}, "unknown command"},
		{"restore extra", []string{"restore", "extra", "--input", "bundle", "--data-dir", "restored", "--ack-writers-stopped"}, "unknown command"},
		{"restore fixed root", []string{"restore", "--input", "bundle", "--data-dir", "restored", "--artifact-dir", "different", "--ack-writers-stopped"}, "unknown flag"},
		{"backup ack", []string{"backup", "--data-dir", "state", "--artifact-dir", "root", "--output", "bundle"}, "requires --ack-writers-stopped"},
		{"restore ack", []string{"restore", "--input", "bundle", "--data-dir", "restored"}, "requires --ack-writers-stopped"},
		{"backup data", []string{"backup", "--artifact-dir", "root", "--output", "bundle", "--ack-writers-stopped"}, "required flag"},
		{"backup root", []string{"backup", "--data-dir", "state", "--output", "bundle", "--ack-writers-stopped"}, "required flag"},
		{"backup output", []string{"backup", "--data-dir", "state", "--artifact-dir", "root", "--ack-writers-stopped"}, "required flag"},
		{"restore input", []string{"restore", "--data-dir", "restored", "--ack-writers-stopped"}, "required flag"},
		{"restore data", []string{"restore", "--input", "bundle", "--ack-writers-stopped"}, "required flag"},
		{"empty backup data", []string{"backup", "--data-dir", "", "--artifact-dir", "root", "--output", "bundle", "--ack-writers-stopped"}, "requires nonempty"},
		{"empty backup root", []string{"backup", "--data-dir", "state", "--artifact-dir", "", "--output", "bundle", "--ack-writers-stopped"}, "requires nonempty"},
		{"empty backup output", []string{"backup", "--data-dir", "state", "--artifact-dir", "root", "--output", "", "--ack-writers-stopped"}, "requires nonempty"},
		{"empty restore input", []string{"restore", "--input", "", "--data-dir", "restored", "--ack-writers-stopped"}, "requires nonempty"},
		{"empty restore data", []string{"restore", "--input", "bundle", "--data-dir", "", "--ack-writers-stopped"}, "requires nonempty"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base := t.TempDir()
			args := append([]string(nil), tt.args...)
			for i, arg := range args {
				if arg == "state" || arg == "root" || arg == "bundle" || arg == "restored" {
					args[i] = filepath.Join(base, arg)
				}
			}
			out, _, err := execute(context.Background(), append([]string{"state", "artifacts"}, args...)...)
			if err == nil || out != "" || !strings.Contains(err.Error(), tt.want) {
				t.Fatal(out, err)
			}
			entries, err := os.ReadDir(base)
			if err != nil || len(entries) != 0 {
				t.Fatal("grammar mutated namespace", entries, err)
			}
		})
	}
}

func TestArtifactCompanionCanceledBeforeAcquisition(t *testing.T) {
	for _, action := range []string{"backup", "restore"} {
		t.Run(action, func(t *testing.T) {
			base := t.TempDir()
			args := []string{"state", "artifacts", action, "--data-dir", filepath.Join(base, "state"), "--ack-writers-stopped"}
			if action == "backup" {
				args = append(args, "--artifact-dir", filepath.Join(base, "root"), "--output", filepath.Join(base, "bundle"))
			} else {
				args = append(args, "--input", filepath.Join(base, "bundle"))
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			out, _, err := execute(ctx, args...)
			if !errors.Is(err, context.Canceled) || out != "" {
				t.Fatal(out, err)
			}
			entries, err := os.ReadDir(base)
			if err != nil || len(entries) != 0 {
				t.Fatal("canceled command acquired output", entries, err)
			}
		})
	}
}

func TestArtifactCompanionOfflineScripts(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel) // parallel script children finish before cancellation
	scripttest.Test(t, ctx, scriptEngine(t), []string{"SKYGO_ENDPOINT=invalid endpoint", "SKYGO_TOKEN=invalid bearer value"}, "testdata/artifact-companion/*.txt")
}

// Reuses the existing real daemon/transfer harness. No fake Store writer, backend
// or native workload is introduced. The original server is stopped before each
// offline acquisition and is restarted only against its original installation.
func TestArtifactCompanionTransferScripts(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("artifact filesystem metadata unsupported")
	}
	exe := artifactScriptFixture(t, "SKYGO_ARTIFACT_BINARY")
	runArtifactScript(t, exe, exe, "testdata/artifact-companion-transfer.txt", true)
}
