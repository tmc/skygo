//go:build linux || darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

// Source-only; NOT_RUN. These tests stop at the server.Config the command
// builds; they never call Install, which needs an open Store.

const environmentFlagImage = "registry.example.com/env/idle@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// environmentFlagFiles writes a kubeconfig, a targets file with target
// first, and a profile file for first with mode, and returns the paths of
// the targets and profile files.
func environmentFlagFiles(t *testing.T, mode os.FileMode) (targets, profiles string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kubeconfig"), []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster: {server: "https://127.0.0.1:1"}
contexts:
- name: chosen
  context: {cluster: test, user: test}
users:
- name: test
  user: {}
`), 0600); err != nil {
		t.Fatal(err)
	}
	targets = filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(targets, []byte(`version: 1
targets:
- name: first
  namespace: first
  kubeconfig: ./kubeconfig
  context: chosen
`), 0600); err != nil {
		t.Fatal(err)
	}
	profiles = filepath.Join(dir, "profiles.yaml")
	if err := os.WriteFile(profiles, []byte(`version: 1
artifacts: false
profiles:
- target: first
  revision: profile-1
  principal: alice
  storage_alias: home
  namespace_uid: namespace-uid-1
  image: `+environmentFlagImage+`
  idle_argv: [/usr/local/bin/idle]
  pvc_name: alice-home
  pvc_uid: claim-uid-1
  volume_name: pv-alice-home
  volume_uid: pv-uid-1
  access_mode: ReadWriteOncePod
  service_account_uid: service-account-uid-1
`), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(profiles, mode); err != nil {
		t.Fatal(err)
	}
	return targets, profiles
}

func TestEnvironmentProfilesFlag(t *testing.T) {
	targets, profiles := environmentFlagFiles(t, 0o600)
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.Install == nil || cfg.Targets != nil {
			t.Fatal("environment profiles did not select the environment integration")
		}
		if cfg.AuthFile != "private-auth.json" || !cfg.DisableLocal {
			t.Fatal("common options were not forwarded")
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--targets", targets, "--environment-profiles", profiles, "--auth-file", "private-auth.json", "--disable-local"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
}

func TestEnvironmentProfilesFlagRefused(t *testing.T) {
	targets, profiles := environmentFlagFiles(t, 0o600)
	_, unsafe := environmentFlagFiles(t, 0o644)
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"without targets", []string{"--namespace", "tasks", "--environment-profiles", profiles, "--auth-file", "private-auth.json"}, "requires --targets"},
		{"without auth file", []string{"--targets", targets, "--environment-profiles", profiles}, "requires --auth-file"},
		{"empty path", []string{"--targets", targets, "--environment-profiles", "", "--auth-file", "private-auth.json"}, "environment profiles"},
		{"unsafe file", []string{"--targets", targets, "--environment-profiles", unsafe, "--auth-file", "private-auth.json"}, "unsafe environment profiles file"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called for refused environment profiles")
				return nil
			})
			cmd.SetArgs(append([]string{"server"}, tt.args...))
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), filepath.Dir(profiles)) {
				t.Fatalf("error = %v, want %q without the path", err, tt.want)
			}
		})
	}
}

// TestEnvironmentProfilesDefault checks that without the flag the command
// builds the same finite targets as before, with no integration.
func TestEnvironmentProfilesDefault(t *testing.T) {
	targets, _ := environmentFlagFiles(t, 0o600)
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.Install != nil || cfg.Targets == nil {
			t.Fatal("default command installed an integration")
		}
		got, err := cfg.Targets("persistent-owner")
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Name != "first" || got[0].SupportsPersistentEnvironments || got[0].SupportsEnvironmentArtifacts {
			t.Fatalf("targets = %+v", got)
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--targets", targets, "--auth-file", "private-auth.json", "--disable-local"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
}
