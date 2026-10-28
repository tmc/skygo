package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tmc/skygo/internal/server"
)

func TestCommand(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config")
	// An unreachable endpoint proves construction requires no API connection.
	err := os.WriteFile(config, []byte(`apiVersion: v1
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
`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.DataDir != dir || cfg.Listen != "127.0.0.1:9999" || cfg.Token != "secret" || !cfg.DisableLocal {
			t.Fatalf("configuration = %+v", cfg)
		}
		targets, err := cfg.Targets("persistent-owner")
		if err != nil {
			t.Fatal(err)
		}
		if len(targets) != 1 || targets[0].Name != "kubernetes" || targets[0].Kind != "kubernetes" || targets[0].Backend == nil || targets[0].Binding == "" {
			t.Fatalf("targets = %+v", targets)
		}
		if !targets[0].SupportsResourceLimits || targets[0].SupportsNodeSelector || len(targets[0].Labels) != 0 || len(targets[0].AcceleratorNames) != 1 || targets[0].AcceleratorNames[0] != "nvidia" {
			t.Fatal("legacy target support facts differ")
		}
		if _, err := cfg.Targets(""); err == nil {
			t.Fatal("missing owner accepted")
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--namespace", "tasks", "--kubeconfig", config, "--context", "chosen", "--data-dir", dir, "--listen", "127.0.0.1:9999", "--token", "secret", "--disable-local"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
}

func TestNamespaceRequired(t *testing.T) {
	cmd := command(func(context.Context, server.Config) error { t.Fatal("server called without namespace"); return nil })
	cmd.SetArgs([]string{"server"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "namespace is required") {
		t.Fatalf("error = %v", err)
	}
}

func TestHelp(t *testing.T) {
	cmd := command(nil)
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"server", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"skygo-kubernetes server", "--namespace", "--kubeconfig", "--context", "--targets", "--execution-state"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help missing %q", want)
		}
	}
}

func TestTargetsFlagConflicts(t *testing.T) {
	for _, flag := range []string{"--namespace", "--context", "--kubeconfig"} {
		for _, value := range []string{"", "secret-canary"} {
			t.Run(flag+"/"+value, func(t *testing.T) {
				cmd := command(func(context.Context, server.Config) error { t.Fatal("server called for conflicting flags"); return nil })
				cmd.SetArgs([]string{"server", "--targets", "absent", flag, value})
				err := cmd.Execute()
				if err == nil || !strings.Contains(err.Error(), "--targets conflicts") || strings.Contains(err.Error(), "secret-canary") {
					t.Fatalf("error = %v", err)
				}
			})
		}
	}
}

func TestTargetsValidationBeforeServer(t *testing.T) {
	for _, body := range []string{"", "version: 2\ntargets: []", "version: 1\ntargets: [{name: local, namespace: jobs, in_cluster: true}]"} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "targets.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := command(func(context.Context, server.Config) error {
				t.Fatal("server called for invalid configuration")
				return nil
			})
			cmd.SetArgs([]string{"server", "--targets", path})
			if err := cmd.Execute(); err == nil {
				t.Fatal("invalid file accepted")
			}
		})
	}
	cmd := command(func(context.Context, server.Config) error {
		t.Fatal("server called for empty explicit targets flag")
		return nil
	})
	cmd.SetArgs([]string{"server", "--targets", ""})
	if err := cmd.Execute(); err == nil {
		t.Fatal("empty explicit file fell back to legacy flags")
	}
}

func TestNamedTargetsCommand(t *testing.T) {
	dir := t.TempDir()
	kubeconfig := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
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
	path := filepath.Join(dir, "targets.yaml")
	if err := os.WriteFile(path, []byte(`version: 1
targets:
- name: first
  namespace: first
  kubeconfig: ./kubeconfig
  context: chosen
- name: second
  namespace: second
  kubeconfig: ./kubeconfig
  context: chosen
`), 0600); err != nil {
		t.Fatal(err)
	}
	called := false
	cmd := command(func(_ context.Context, cfg server.Config) error {
		called = true
		if cfg.AuthFile != "private-auth.json" || !cfg.DisableLocal {
			t.Fatal("common options were not forwarded")
		}
		// The configuration is a startup snapshot; the builder must not reread it.
		if err := os.WriteFile(path, []byte("invalid"), 0600); err != nil {
			t.Fatal(err)
		}
		targets, err := cfg.Targets("persistent-owner")
		if err != nil {
			t.Fatal(err)
		}
		if len(targets) != 2 || targets[0].Name != "first" || targets[1].Name != "second" {
			t.Fatal("named targets differ")
		}
		for _, target := range targets {
			if target.Kind != "kubernetes" || target.Backend == nil || target.Binding == "" {
				t.Fatal("target not constructed")
			}
			if !target.SupportsResourceLimits || target.SupportsNodeSelector || len(target.AcceleratorNames) != 0 || len(target.Labels) != 0 {
				t.Fatal("named target gained undeclared policy or metadata")
			}
		}
		return nil
	})
	cmd.SetArgs([]string{"server", "--targets", path, "--disable-local", "--auth-file", "private-auth.json"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("server not called")
	}
}
