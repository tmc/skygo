package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/internal/server"
)

// modelInstallation returns a MODEL installation layout: the private
// directories and files checkInstallation requires, with no skygo
// content. Only the agent's preflight is exercised with it; the real
// store, auth and artifact checks belong to server.Run and are exercised
// in agent_installation_linux_test.go.
func modelInstallation(t *testing.T) bootstrap.Installation {
	t.Helper()
	base := t.TempDir()
	in := bootstrap.Installation{
		DataDir:     filepath.Join(base, "data"),
		AuthFile:    filepath.Join(base, "auth.json"),
		ArtifactDir: filepath.Join(base, "artifacts"),
	}
	for _, d := range []string{in.DataDir, in.ArtifactDir} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write(t, filepath.Join(in.DataDir, "jobs.db"), "model state\n")
	write(t, in.AuthFile, "model auth\n")
	if !bootstrap.ValidPath(in.DataDir) || !bootstrap.ValidPath(in.AuthFile) || !bootstrap.ValidPath(in.ArtifactDir) {
		t.Skipf("temporary directory %s is not a valid installation path", base)
	}
	return in
}

// installationArgs returns agent arguments serving in from dir.
func installationArgs(dir, listen string, in bootstrap.Installation) []string {
	return []string{"--dir", dir, "--listen", listen, "--target", "dgx", "--data-dir", in.DataDir, "--auth-file", in.AuthFile, "--artifact-dir", in.ArtifactDir}
}

// tree describes every entry below root, with file contents as checksums,
// so a test can check that a refusal changed nothing.
func tree(t *testing.T, root string) map[string]string {
	t.Helper()
	m := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, _ := os.Readlink(p)
			m[rel] = "symlink " + target
		case info.IsDir():
			m[rel] = info.Mode().String()
		default:
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			sum := sha256.Sum256(data)
			m[rel] = info.Mode().String() + " " + hex.EncodeToString(sum[:])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// TestAgentInstallationConfig checks that an installation agent hands
// server.Run exactly the given paths, as AuthFile and ArtifactDir on the
// ExistingInstallation route with no legacy token, and otherwise the legacy
// agent's configuration.
func TestAgentInstallationConfig(t *testing.T) {
	dir, id := agentDir(t)
	remove(t, id.token)
	in := modelInstallation(t)
	var got server.Config
	served := 0
	serve := func(_ context.Context, cfg server.Config) error { got = cfg; served++; return nil }
	if err := runAgent(context.Background(), installationArgs(dir, "127.0.0.1:47080", in), io.Discard, serve); err != nil {
		t.Fatal(err)
	}
	if served != 1 {
		t.Fatalf("served %d times", served)
	}
	if got.DataDir != in.DataDir || got.AuthFile != in.AuthFile || got.ArtifactDir != in.ArtifactDir {
		t.Errorf("installation = %q %q %q; want exactly %+v", got.DataDir, got.AuthFile, got.ArtifactDir, in)
	}
	if got.Token != "" {
		t.Error("installation agent configured a legacy token")
	}
	if !got.ExistingInstallation {
		t.Error("installation agent did not select the existing installation route")
	}
	if got.Listen != "127.0.0.1:47080" || got.LocalTargetName != "dgx" || !got.DurableLocal || !filepath.IsAbs(got.LocalSupervisor) {
		t.Errorf("config = %+v", got)
	}
	if got.DisableLocal || got.Artifacts != nil || got.RecoverStoppedArtifactReaders || got.Targets != nil || got.Install != nil || got.TargetFacts != "" {
		t.Errorf("config enables more than the local target and the installation: %+v", got)
	}
	if c := got.TLSConfig; c == nil || len(c.Certificates) != 1 {
		t.Fatalf("TLS config = %+v", c)
	}
}

func TestAgentInstallationRefused(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string, in bootstrap.Installation) []string
		edit func(t *testing.T, dir string, in bootstrap.Installation)
		err  string
	}{
		{"data_only", func(dir string, in bootstrap.Installation) []string {
			return []string{"--dir", dir, "--listen", "127.0.0.1:47080", "--target", "dgx", "--data-dir", in.DataDir}
		}, nil, "clean absolute"},
		{"no_auth_file", func(dir string, in bootstrap.Installation) []string {
			return []string{"--dir", dir, "--listen", "127.0.0.1:47080", "--target", "dgx", "--data-dir", in.DataDir, "--artifact-dir", in.ArtifactDir}
		}, nil, "clean absolute"},
		{"relative_data", func(dir string, in bootstrap.Installation) []string {
			in.DataDir = "data"
			return installationArgs(dir, "127.0.0.1:47080", in)
		}, nil, "clean absolute"},
		{"unclean_artifacts", func(dir string, in bootstrap.Installation) []string {
			in.ArtifactDir += "/."
			return installationArgs(dir, "127.0.0.1:47080", in)
		}, nil, "clean absolute"},
		{"legacy_token", nil, func(t *testing.T, dir string, _ bootstrap.Installation) {
			write(t, filepath.Join(dir, "token"), "abc\n")
		}, "legacy token"},
		{"missing_data", nil, func(t *testing.T, _ string, in bootstrap.Installation) {
			if err := os.RemoveAll(in.DataDir); err != nil {
				t.Fatal(err)
			}
		}, "not initialized"},
		{"missing_state", nil, func(t *testing.T, _ string, in bootstrap.Installation) {
			remove(t, filepath.Join(in.DataDir, "jobs.db"))
		}, "not initialized"},
		{"missing_auth", nil, func(t *testing.T, _ string, in bootstrap.Installation) { remove(t, in.AuthFile) }, "not initialized"},
		{"missing_artifacts", nil, func(t *testing.T, _ string, in bootstrap.Installation) { remove(t, in.ArtifactDir) }, "not initialized"},
		{"artifacts_symlink", nil, func(t *testing.T, _ string, in bootstrap.Installation) {
			remove(t, in.ArtifactDir)
			if err := os.Symlink(t.TempDir(), in.ArtifactDir); err != nil {
				t.Fatal(err)
			}
		}, "not a directory"},
		{"auth_directory", nil, func(t *testing.T, _ string, in bootstrap.Installation) {
			remove(t, in.AuthFile)
			if err := os.Mkdir(in.AuthFile, 0o700); err != nil {
				t.Fatal(err)
			}
		}, "not a regular file"},
		{"readable_auth", nil, func(t *testing.T, _ string, in bootstrap.Installation) { chmod(t, in.AuthFile, 0o644) }, "group or others"},
		{"shared_data", nil, func(t *testing.T, _ string, in bootstrap.Installation) { chmod(t, in.DataDir, 0o755) }, "group or others"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, id := agentDir(t)
			remove(t, id.token)
			in := modelInstallation(t)
			args := installationArgs(dir, "127.0.0.1:47080", in)
			if tt.args != nil {
				args = tt.args(dir, in)
			}
			if tt.edit != nil {
				tt.edit(t, dir, in)
			}
			base := filepath.Dir(in.DataDir)
			before, beforeDir := tree(t, base), tree(t, dir)
			served := false
			serve := func(context.Context, server.Config) error { served = true; return nil }
			err := runAgent(context.Background(), args, io.Discard, serve)
			if err == nil || !strings.Contains(err.Error(), tt.err) || served {
				t.Fatalf("runAgent = %v, served %v; want %q", err, served, tt.err)
			}
			// A refusal creates no state directory, store or file.
			if after := tree(t, base); !maps.Equal(before, after) {
				t.Fatalf("refusal changed the installation from\n%v\nto\n%v", before, after)
			}
			if after := tree(t, dir); !maps.Equal(beforeDir, after) {
				t.Fatalf("refusal changed the agent directory from\n%v\nto\n%v", beforeDir, after)
			}
		})
	}
}

// TestStartAgentReadsScopedTokenFirst checks that an installation session
// refuses an unusable scoped token before resolving the host, building or
// contacting it: the error comes from the first step.
func TestStartAgentReadsScopedTokenFirst(t *testing.T) {
	in := modelInstallation(t)
	token := filepath.Join(t.TempDir(), "token")
	tests := []struct {
		name string
		edit func(t *testing.T)
		err  string
	}{
		{"missing", func(*testing.T) {}, "scoped token"},
		{"readable", func(t *testing.T) { write(t, token, "abc\n"); chmod(t, token, 0o644) }, "group or others"},
		{"empty", func(t *testing.T) { chmod(t, token, 0o600); write(t, token, "\n") }, "invalid token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.edit(t)
			f := flags{host: "dgx", knownHosts: "kh", target: "dgx", port: defaultPort, installation: in, scopedToken: token}
			cfg, err := clientConfig(context.Background(), f, io.Discard)
			if err == nil || cfg.Address != "" || !strings.Contains(err.Error(), "scoped token") || !strings.Contains(err.Error(), tt.err) {
				t.Fatalf("clientConfig resolved address %q: %v; want a scoped token refusal %q", cfg.Address, err, tt.err)
			}
		})
	}
}
