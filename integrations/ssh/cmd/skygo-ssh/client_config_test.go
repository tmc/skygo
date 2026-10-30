package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestClientConfigRelativeAgentSocket(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	// The actual resolver executes only this local model of ssh -G. No SSH
	// connection or user/system SSH configuration is consulted.
	fake := "#!/bin/sh\nprintf '%s\\n' 'hostname 127.0.0.1' 'port 22' 'user skygo' 'identityagent SSH_AUTH_SOCK'\n"
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(fake), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+"/usr/bin:/bin")
	t.Setenv("HOME", dir)
	t.Setenv("SSH_AUTH_SOCK", "relative-agent.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg, err := clientConfig(ctx, flags{host: "compat-alias", knownHosts: filepath.Join(dir, "known_hosts"), fingerprint: "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU", source: dir, target: "compat-target", port: defaultPort}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.Abs("relative-agent.sock")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AgentSocket != want || cfg.Address != "127.0.0.1:22" || cfg.Target != "compat-target" || !cfg.AllowDirty || cfg.Source != dir {
		t.Fatal("legacy source/alias/socket resolution changed")
	}
}
