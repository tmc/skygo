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

// Source-only; NOT_RUN. Like the environment flag tests these stop at the
// server.Config the command builds; they never call Install.

func serviceFlagFile(t *testing.T, mode os.FileMode) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "services.yaml")
	if err := os.WriteFile(name, []byte(`version: 1
target: first
revision: services-1
namespace_uid: namespace-uid-1
service_account_uid: service-account-uid-1
image_pull_secrets: [registry]
ceilings: {cpu_millis: 2000, memory_bytes: 4294967296}
limits: {cpu_millis: 8000, memory_bytes: 17179869184}
grants:
- principal: alice
  images: [`+environmentFlagImage+`]
`), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(name, mode); err != nil {
		t.Fatal(err)
	}
	return name
}

func TestServiceProfilesFlag(t *testing.T) {
	targets, profiles := environmentFlagFiles(t, 0o600)
	services := serviceFlagFile(t, 0o600)
	for _, args := range [][]string{
		{"--service-profiles", services},
		{"--service-profiles", services, "--environment-profiles", profiles},
	} {
		called := false
		cmd := command(func(_ context.Context, cfg server.Config) error {
			called = true
			if cfg.Install == nil || cfg.Targets != nil {
				t.Fatal("service profiles did not select the integration")
			}
			return nil
		})
		cmd.SetArgs(append([]string{"server", "--targets", targets, "--auth-file", "private-auth.json", "--disable-local"}, args...))
		if err := cmd.Execute(); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if !called {
			t.Fatalf("%v: server not called", args)
		}
	}
}

func TestServiceProfilesFlagRefused(t *testing.T) {
	targets, _ := environmentFlagFiles(t, 0o600)
	services := serviceFlagFile(t, 0o600)
	unsafe := serviceFlagFile(t, 0o644)
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"receipts", []string{"--targets", "missing-targets", "--execution-state", "missing-state", "--service-profiles", "missing-services"}, "--execution-state conflicts with --service-profiles"},
		{"receipts empty services", []string{"--targets", "missing-targets", "--execution-state", "missing-state", "--service-profiles", ""}, "--execution-state conflicts with --service-profiles"},
		{"receipts empty state", []string{"--targets", "missing-targets", "--execution-state", "", "--service-profiles", "missing-services"}, "--execution-state conflicts with --service-profiles"},
		{"receipts both empty", []string{"--targets", "missing-targets", "--execution-state", "", "--service-profiles", ""}, "--execution-state conflicts with --service-profiles"},
		{"without targets", []string{"--namespace", "tasks", "--service-profiles", services, "--auth-file", "private-auth.json"}, "--service-profiles requires --targets"},
		{"without auth file", []string{"--targets", targets, "--service-profiles", services}, "--service-profiles requires --auth-file"},
		{"unsafe file", []string{"--targets", targets, "--service-profiles", unsafe, "--auth-file", "private-auth.json"}, "service profiles"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called")
				return nil
			})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(append([]string{"server"}, tt.args...))
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDRAServiceProfilesRefused(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	l := &draLoopback{class: true}
	targets, state, dra := draCommandFixture(t, l)
	beforeState, beforeDRA := dirImage(t, state), dirImage(t, dra)
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	for _, tt := range []struct {
		name    string
		targets string
		args    []string
		want    string
	}{
		{"dra flag", filepath.Join(dir, "missing-targets"), []string{"--dra-state", ""}, "--dra-state conflicts with --service-profiles"},
		{"dra receipts", filepath.Join(dir, "missing-targets"), []string{"--execution-state", state, "--dra-state", dra}, "--execution-state conflicts with --service-profiles"},
		{"dra policy", targets, nil, "dra accelerators conflict with --service-profiles"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called")
				return nil
			})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(append([]string{"server", "--targets", tt.targets, "--service-profiles", filepath.Join(dir, "missing-services"), "--auth-file", "private-auth.json", "--data-dir", data}, tt.args...))
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want %q", err, tt.want)
			}
			if _, err := os.Stat(data); !os.IsNotExist(err) {
				t.Fatalf("Store directory was touched: %v", err)
			}
			if dirImage(t, state) != beforeState || dirImage(t, dra) != beforeDRA {
				t.Fatal("receipt state changed")
			}
		})
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.requests) != 0 {
		t.Fatal("refused configuration contacted the API")
	}
}
