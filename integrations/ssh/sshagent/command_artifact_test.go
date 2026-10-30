//go:build unix

package sshagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/internal/artifact"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	corecontrol "github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
	"google.golang.org/protobuf/encoding/protojson"
)

// The SSH enrollment edge is modeled. The canonical CLI, archive codec,
// HTTP/RPC, auth, Store, Files, Control and Local workspace are actual.
func TestCommandArtifactAndWorkspace(t *testing.T) {
	for _, workspace := range []bool{false, true} {
		t.Run(fmt.Sprint(workspace), func(t *testing.T) {
			if workspace && runtime.GOOS != "linux" {
				t.Skip("actual Local workspace writer requires Linux; no modeled replacement")
			}
			dir := t.TempDir()
			data := filepath.Join(dir, "state")
			art := filepath.Join(dir, "artifacts")
			if err := os.Mkdir(art, 0700); err != nil {
				t.Fatal(err)
			}
			if err := server.InitializeArtifacts(data, art); err != nil {
				t.Fatal(err)
			}
			s, err := store.Open(data)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			root, err := os.OpenRoot(art)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})
			info, err := root.Lstat(".")
			if err != nil {
				t.Fatal(err)
			}
			st := info.Sys().(*syscall.Stat_t)
			files, err := artifact.Open(root, artifact.FileID{Device: uint64(st.Dev), Inode: uint64(st.Ino)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := files.Close(); err != nil {
					t.Error(err)
				}
			})
			b, err := local.New(filepath.Join(dir, "logs"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := b.Close(); err != nil {
					t.Error(err)
				}
			})
			engine := auth.NewLegacy("captured-token")
			binding, err := backend.LocalBinding(s.Owner())
			if err != nil {
				t.Fatal(err)
			}
			config := corecontrol.Config{Artifacts: files, LocalBackend: b}
			if workspace {
				life, err := local.CaptureServerLifetime()
				if err != nil && !errors.Is(err, local.ErrServerLifetimeUnavailable) {
					t.Fatal(err)
				}
				var ce *local.ServerLifetimeCloseError
				if errors.As(err, &ce) {
					t.Fatal(err)
				}
				config.ServerLifetime = life
				config.LocalWorkspace = &corecontrol.LocalWorkspaceConfig{Target: "local", Binding: binding, Backend: b, Roots: root}
			}
			ctl, err := corecontrol.NewWithConfig(s, []backend.Target{{Name: "local", Kind: "local", Binding: binding, Backend: b}}, engine, config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			done := make(chan error, 1)
			go func() { done <- ctl.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			})
			srv := httptest.NewServer(rpc.NewHandler(ctl, engine))
			t.Cleanup(srv.Close)
			token := filepath.Join(dir, "token")
			if err := os.WriteFile(token, []byte("caller-file-token\n"), 0600); err != nil {
				t.Fatal(err)
			}
			run := func(args ...string) []byte {
				t.Helper()
				signer := &modelSigner{}
				var out, errout bytes.Buffer
				err := runCommand(ctx, commandAgent{client: srv.Client(), owner: signer, endpoint: srv.URL, tokenFile: token, bearer: "captured-token", host: "alias", target: "local"}, Command{Args: args, Stdout: &out, Stderr: &errout})
				if err != nil || signer.count() != 1 {
					t.Fatalf("command %q: %v; %s", args, err, errout.String())
				}
				return out.Bytes()
			}
			source := filepath.Join(dir, "source")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			const payload = "same canonical snapshot bytes\n"
			if err := os.WriteFile(filepath.Join(source, "input.txt"), []byte(payload), 0600); err != nil {
				t.Fatal(err)
			}
			var ref artifact.Ref
			if !workspace {
				if err := json.Unmarshal(run("artifacts", "upload", source), &ref); err != nil {
					t.Fatal(err)
				}
			} else {
				task := filepath.Join(dir, "task.yaml")
				text := fmt.Sprintf("workdir: %q\nrun: cat input.txt > result.txt\n", source)
				if err := os.WriteFile(task, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
				var job v1.Job
				raw := run("run", "-f", task, "--sync-workdir", "--workspace-state", filepath.Join(dir, "intent"), "--output", "result.txt", "--wait", "--json")
				if err := protojson.Unmarshal(raw, &job); err != nil {
					t.Fatal(err)
				}
				if job.State != v1.JobState_JOB_STATE_SUCCEEDED || job.Result == nil || job.Result.Reference == nil {
					t.Fatal("workspace result unavailable")
				}
				r := job.Result.Reference
				ref = artifact.Ref{ID: r.Id, SHA256: r.Sha256, Size: r.Size}
			}
			refPath := filepath.Join(dir, "ref.json")
			raw, err := json.Marshal(ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(refPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			dest := filepath.Join(dir, "download")
			run("artifacts", "download", "--ref", refPath, dest)
			name := "input.txt"
			if workspace {
				name = "result.txt"
			}
			got, err := os.ReadFile(filepath.Join(dest, name))
			if err != nil || string(got) != payload {
				t.Fatalf("artifact contents=%q err=%v", got, err)
			}
		})
	}
}
