package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

func TestWorkerFlagConflictsBeforeFiles(t *testing.T) {
	for _, flag := range []string{"service-profiles", "environment-profiles", "execution-state", "dra-state", "dra-log-retention", "kubeconfig", "context", "namespace"} {
		values := []string{"", "private-value"}
		if flag == "dra-log-retention" {
			values = []string{"0s", "1s"}
		}
		for _, value := range values {
			for _, profile := range []string{"absent-profile", ""} {
				t.Run(flag+"/"+value+"/profile="+profile, func(t *testing.T) {
					cmd := command(func(context.Context, server.Config) error {
						t.Fatal("server called for conflicting worker flags")
						return nil
					})
					cmd.SetArgs([]string{"server", "--worker-profile", profile, "--targets", "absent-targets", "--auth-file", "absent-auth", "--disable-local", "--" + flag, value})
					err := cmd.Execute()
					if err == nil || !strings.Contains(err.Error(), "conflict") || strings.Contains(err.Error(), "private-value") || strings.Contains(err.Error(), "open") {
						t.Fatalf("error = %v", err)
					}
				})
			}
		}
	}
}

func TestWorkerFlagRequirementsBeforeFiles(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"targets", []string{"--auth-file", "absent", "--disable-local"}, "--targets"},
		{"auth", []string{"--targets", "absent", "--disable-local"}, "--auth-file"},
		{"local", []string{"--targets", "absent", "--auth-file", "absent"}, "--disable-local"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called without worker requirements")
				return nil
			})
			cmd.SetArgs(append([]string{"server", "--worker-profile", "absent-profile"}, tt.args...))
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) || strings.Contains(err.Error(), "open") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestWorkerFlagEmptyDoesNotFallBack(t *testing.T) {
	cmd := command(func(context.Context, server.Config) error {
		t.Fatal("empty worker profile selected an ordinary target")
		return nil
	})
	cmd.SetArgs([]string{"server", "--worker-profile", "", "--targets", "absent", "--auth-file", "absent", "--disable-local"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("empty worker profile accepted")
	}
}

func TestWorkerFlagSelectsInstall(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "worker.yaml")
	if err := os.WriteFile(profile, []byte("version: 1\ntarget: cpu\nnamespace_uid: namespace-uid\nservice_account_uid: account-uid\nhelper_image: example.invalid/helper@sha256:"+strings.Repeat("a", 64)+"\nhelper_sha256: "+strings.Repeat("b", 64)+"\nbudget: {ranks: 4, cpu_millis: 4000, memory_bytes: 4294967296}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	targets := filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(targets, []byte("version: 1\ntargets:\n- name: cpu\n  namespace: jobs\n  in_cluster: true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.Install == nil || cfg.Targets != nil || !cfg.DisableLocal || cfg.AuthFile != "not-read-by-command" {
			t.Fatal("worker command did not select the exclusive integration")
		}
		// The command passes through the server's Store and Engine. These
		// absent dependencies must fail before in-cluster credentials or API.
		if in, err := cfg.Install(nil, nil); err == nil || in != nil {
			t.Fatalf("install without server dependencies = %T, %v", in, err)
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--worker-profile", profile, "--targets", targets, "--auth-file", "not-read-by-command", "--disable-local"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server configuration was not passed to the runner")
	}
}

func TestWorkerFlagHelp(t *testing.T) {
	cmd := command(nil)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"server", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "--worker-profile") {
		t.Fatal("help omits the explicit worker profile")
	}
}
