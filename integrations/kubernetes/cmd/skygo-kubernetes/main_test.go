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
		if len(targets) != 1 || targets[0].Name != "kubernetes" || targets[0].Kind != "kubernetes" || targets[0].Backend == nil {
			t.Fatalf("targets = %+v", targets)
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
	for _, want := range []string{"skygo-kubernetes server", "--namespace", "--kubeconfig", "--context"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help missing %q", want)
		}
	}
}
