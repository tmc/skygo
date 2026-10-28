package sshconfig

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const base = `host dgx
hostname dgx-spark.lan
user alice
port 22
identityfile ~/.ssh/id_ed25519
identityfile /keys/extra
identityagent SSH_AUTH_SOCK
forwardagent no
forwardx11 no
permitlocalcommand no
`

func TestParse(t *testing.T) {
	h, err := Parse("dgx", "/home/alice", []byte(base))
	if err != nil {
		t.Fatal(err)
	}
	want := Host{Alias: "dgx", Address: "dgx-spark.lan:22", User: "alice", IdentityFiles: []string{"/home/alice/.ssh/id_ed25519", "/keys/extra"}}
	if h.Alias != want.Alias || h.Address != want.Address || h.User != want.User || h.IdentityAgent != "" || !slices.Equal(h.IdentityFiles, want.IdentityFiles) {
		t.Fatalf("Parse = %+v, want %+v", h, want)
	}
}

func TestResolveOutputLimit(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "output")
	// A dangerous option after the limit must not disappear through truncation.
	data := base + strings.Repeat("# padding\n", maxOutput/4) + "proxycommand unsafe\n"
	if err := os.WriteFile(output, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	program := filepath.Join(dir, "ssh")
	if err := os.WriteFile(program, []byte("#!/bin/sh\ncat \"$SSH_CONFIG_OUTPUT\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SSH_CONFIG_OUTPUT", output)
	if _, err := Resolve(context.Background(), "dgx", dir, nil); err == nil || !strings.Contains(err.Error(), "output too large") {
		t.Fatalf("oversized ssh -G output = %v", err)
	}
}

func TestParseVariants(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(string) string
		address string
		agent   string
		err     string // "" if accepted
	}{
		{"other port", repl("port 22", "port 2222"), "dgx-spark.lan:2222", "", ""},
		{"ipv6", repl("hostname dgx-spark.lan", "hostname fd00::5"), "[fd00::5]:22", "", ""},
		{"agent path", repl("identityagent SSH_AUTH_SOCK", "identityagent /run/agent.sock"), "dgx-spark.lan:22", "/run/agent.sock", ""},
		{"agent none", repl("identityagent SSH_AUTH_SOCK", "identityagent none"), "dgx-spark.lan:22", "none", ""},
		{"proxy none", add("proxycommand none"), "dgx-spark.lan:22", "", ""},
		{"relative agent", repl("identityagent SSH_AUTH_SOCK", "identityagent ~/agent.sock"), "", "", "identityagent"},
		{"proxycommand", add("proxycommand nc %h %p"), "", "", "proxycommand"},
		{"proxyjump", add("proxyjump bastion"), "", "", "proxyjump"},
		{"forward agent", repl("forwardagent no", "forwardagent yes"), "", "", "forwardagent"},
		{"local forward", add("localforward 8080 [127.0.0.1]:80"), "", "", "localforward"},
		{"remote command", add("remotecommand id"), "", "", "remotecommand"},
		{"local command", repl("permitlocalcommand no", "permitlocalcommand yes"), "", "", "permitlocalcommand"},
		{"upper case host", repl("hostname dgx-spark.lan", "hostname DGX.lan"), "", "", "lowercase"},
		{"shell host", repl("hostname dgx-spark.lan", "hostname dgx;id"), "", "", "lowercase"},
		{"bad port", repl("port 22", "port 022"), "", "", "invalid port"},
		{"no port", repl("port 22\n", ""), "", "", "invalid port"},
		{"spaced user", repl("user alice", "user a b"), "", "", "invalid user"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, err := Parse("dgx", "/home/alice", []byte(tt.edit(base)))
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("Parse = %+v, %v; want error %q", h, err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if h.Address != tt.address || h.IdentityAgent != tt.agent {
				t.Fatalf("Parse = %+v; want address %q agent %q", h, tt.address, tt.agent)
			}
		})
	}
}

func repl(old, new string) func(string) string {
	return func(s string) string { return strings.Replace(s, old, new, 1) }
}

func add(line string) func(string) string {
	return func(s string) string { return s + line + "\n" }
}

func TestResolve(t *testing.T) {
	var got []string
	run := func(_ context.Context, name string, args ...string) ([]byte, error) {
		got = append([]string{name}, args...)
		return []byte(base), nil
	}
	if _, err := Resolve(context.Background(), "dgx", "/home/alice", run); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ssh", "-G", "--", "dgx"}; !slices.Equal(got, want) {
		t.Fatalf("argv = %q, want %q", got, want)
	}
	for _, alias := range []string{"", "-oProxyCommand=x", "dgx box", "dgx;id", "$(id)"} {
		got = nil
		if _, err := Resolve(context.Background(), alias, "", run); err == nil || got != nil {
			t.Errorf("Resolve(%q) = %v after running %q; want refusal before ssh", alias, err, got)
		}
	}
	failed := errors.New("exit status 255")
	if _, err := Resolve(context.Background(), "dgx", "", func(context.Context, string, ...string) ([]byte, error) { return nil, failed }); !errors.Is(err, failed) {
		t.Fatalf("Resolve = %v, want %v", err, failed)
	}
}
