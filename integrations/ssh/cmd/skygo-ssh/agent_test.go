package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/server"
)

// agentDir returns a private directory holding a new agent identity, as
// the bootstrap leaves it on the remote host.
func agentDir(t *testing.T) (string, identity) {
	t.Helper()
	id, err := loadIdentity(filepath.Join(t.TempDir(), "agent"), agentName)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(id.cert), id
}

func TestIdentityWithoutCachedLeaf(t *testing.T) {
	dir, original := agentDir(t)
	t.Setenv("GODEBUG", "x509keypairleaf=0")
	id, err := checkIdentity(dir, agentName)
	if err != nil {
		t.Fatal(err)
	}
	if id.token != original.token || id.bearer != original.bearer {
		t.Fatal("loading the same identity changed its token")
	}
}

func TestAgentConfig(t *testing.T) {
	dir, id := agentDir(t)
	var got server.Config
	serve := func(_ context.Context, cfg server.Config) error { got = cfg; return nil }
	err := runAgent(context.Background(), []string{"--dir", dir, "--listen", "127.0.0.1:47080", "--target", "dgx"}, io.Discard, serve)
	if err != nil {
		t.Fatal(err)
	}
	if got.DataDir != filepath.Join(dir, "data") || got.Listen != "127.0.0.1:47080" || got.LocalTargetName != "dgx" || got.Token != id.bearer {
		t.Errorf("config = %+v", got)
	}
	if !got.DurableLocal || !filepath.IsAbs(got.LocalSupervisor) {
		t.Fatal("agent does not configure its own durable supervisor")
	}
	if got.DisableLocal || got.AuthFile != "" || got.ArtifactDir != "" || got.Targets != nil {
		t.Errorf("config enables more than the local target: %+v", got)
	}
	c := got.TLSConfig
	if c == nil || len(c.Certificates) != 1 || c.MinVersion != tls.VersionTLS12 || !slices.Equal(c.NextProtos, []string{"http/1.1"}) {
		t.Fatalf("TLS config = %+v", c)
	}
	if !slices.Contains(c.Certificates[0].Leaf.DNSNames, agentName) {
		t.Errorf("certificate names %q", c.Certificates[0].Leaf.DNSNames)
	}
}

func TestAgentConfigRefused(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		edit func(t *testing.T, dir string)
		err  string
	}{
		{"wildcard listen", func(dir string) []string {
			return []string{"--dir", dir, "--listen", "0.0.0.0:47080", "--target", "dgx"}
		}, nil, "127.0.0.1"},
		{"host name listen", func(dir string) []string {
			return []string{"--dir", dir, "--listen", "localhost:47080", "--target", "dgx"}
		}, nil, "127.0.0.1"},
		{"relative dir", func(string) []string {
			return []string{"--dir", "agent", "--listen", "127.0.0.1:47080", "--target", "dgx"}
		}, nil, "absolute"},
		{"no target", func(dir string) []string { return []string{"--dir", dir, "--listen", "127.0.0.1:47080"} }, nil, "target"},
		{"extra argument", func(dir string) []string {
			return []string{"--dir", dir, "--listen", "127.0.0.1:47080", "--target", "dgx", "x"}
		}, nil, "unexpected"},
		{"readable key", nil, func(t *testing.T, dir string) { chmod(t, filepath.Join(dir, "key.pem"), 0o640) }, "group or others"},
		{"readable token", nil, func(t *testing.T, dir string) { chmod(t, filepath.Join(dir, "token"), 0o644) }, "group or others"},
		{"missing token", nil, func(t *testing.T, dir string) { remove(t, filepath.Join(dir, "token")) }, "token"},
		{"empty token", nil, func(t *testing.T, dir string) { write(t, filepath.Join(dir, "token"), "\n") }, "invalid token"},
		{"spaced token", nil, func(t *testing.T, dir string) { write(t, filepath.Join(dir, "token"), "a b\n") }, "invalid token"},
		{"token symlink", nil, func(t *testing.T, dir string) {
			other := filepath.Join(t.TempDir(), "token")
			write(t, other, "abc\n")
			remove(t, filepath.Join(dir, "token"))
			if err := os.Symlink(other, filepath.Join(dir, "token")); err != nil {
				t.Fatal(err)
			}
		}, "not a regular file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := agentDir(t)
			args := []string{"--dir", dir, "--listen", "127.0.0.1:47080", "--target", "dgx"}
			if tt.args != nil {
				args = tt.args(dir)
			}
			if tt.edit != nil {
				tt.edit(t, dir)
			}
			served := false
			serve := func(context.Context, server.Config) error { served = true; return nil }
			err := runAgent(context.Background(), args, io.Discard, serve)
			if err == nil || !strings.Contains(err.Error(), tt.err) || served {
				t.Fatalf("runAgent = %v, served %v; want %q", err, served, tt.err)
			}
		})
	}
}

func chmod(t *testing.T, name string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(name, mode); err != nil {
		t.Fatal(err)
	}
}

func remove(t *testing.T, name string) {
	t.Helper()
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
}

func write(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadIdentity(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "agents", "dgx", "c", "d")
	first, err := loadIdentity(dir, agentName)
	if err != nil {
		t.Fatal(err)
	}
	contents := func() []string {
		var out []string
		for _, name := range []string{first.cert, first.key, first.token} {
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, string(data))
		}
		return out
	}
	before := contents()
	again, err := loadIdentity(dir, agentName)
	if err != nil || again != first || !slices.Equal(contents(), before) {
		t.Fatalf("second load = %+v, %v; want the same identity", again, err)
	}
	for _, name := range []string{dir, first.cert, first.key, first.token} {
		info, err := os.Lstat(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s mode %v", name, info.Mode())
		}
	}
	if len(first.bearer) != 64 {
		t.Errorf("token length %d, want 64 hex digits", len(first.bearer))
	}
	entries, _ := os.ReadDir(filepath.Dir(dir))
	if len(entries) != 1 {
		t.Errorf("temporary directories remain: %v", entries)
	}

	// Each agent directory has its own identity.
	other, err := loadIdentity(filepath.Join(filepath.Dir(dir), "e"), agentName)
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(other.cert); string(data) == before[0] || other.bearer == first.bearer {
		t.Fatal("two agent directories share an identity")
	}

	// The certificate verifies only as itself, for the agent name.
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM([]byte(before[0]))
	pair, err := tls.LoadX509KeyPair(first.cert, first.key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pair.Leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: agentName}); err != nil {
		t.Fatal(err)
	}
	if _, err := pair.Leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "other.invalid"}); err == nil {
		t.Fatal("certificate verified for another name")
	}

	chmod(t, first.key, 0o644)
	if _, err := loadIdentity(dir, agentName); err == nil || !strings.Contains(err.Error(), "group or others") {
		t.Fatalf("load with a readable key = %v", err)
	}
	if _, err := loadIdentity(dir, "other.invalid"); err == nil {
		t.Fatal("identity loaded for another name")
	}
}

// A recordingTransport records each request's headers as sent.
type recordingTransport struct {
	next    http.RoundTripper
	mu      sync.Mutex
	headers []http.Header
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.headers = append(r.headers, req.Header.Clone())
	r.mu.Unlock()
	return r.next.RoundTrip(req)
}

// TestAgentCLI runs the agent in process with server.Run and drives it
// with the canonical CLI as skygo-ssh does, through a client that, like
// the SSH transport, dials one fixed address and trusts only the agent's
// certificate.
func TestAgentCLI(t *testing.T) {
	t.Setenv("SKYGO_TOKEN", "")
	dir, id := agentDir(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runAgent(ctx, []string{"--dir", dir, "--listen", address, "--target", "dgx"}, io.Discard, server.Run)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(15 * time.Second):
			t.Error("agent did not shut down")
		}
	})

	certPEM, err := os.ReadFile(id.cert)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(certPEM)
	dialer := &net.Dialer{}
	record := &recordingTransport{next: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, address)
		},
		TLSClientConfig: &tls.Config{RootCAs: roots, ServerName: agentName, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
	}}
	hc := &http.Client{Transport: identityTransport{record}}
	endpoint := "https://" + agentName

	running := func(context.Context) (bootstrap.State, error) { return bootstrap.State{Agent: "running"}, nil }
	if err := waitReady(ctx, hc, endpoint, id.bearer, "dgx", running); err != nil {
		t.Fatal(err)
	}
	if err := waitReady(ctx, hc, endpoint, id.bearer, "local", running); !errors.Is(err, errWrongAgent) {
		t.Fatalf("waitReady for another target = %v, want errWrongAgent", err)
	}
	stopped := func(context.Context) (bootstrap.State, error) { return bootstrap.State{Agent: "stopped"}, nil }
	if err := waitReady(ctx, hc, endpoint, "wrong-token", "dgx", stopped); err == nil || !strings.Contains(err.Error(), "agent stopped") {
		t.Fatalf("waitReady with a wrong token = %v", err)
	}

	skygo := func(args ...string) (string, error) {
		root := cli.NewWithClient(nil, hc, cli.ClientOptions{})
		root.SetArgs(args)
		var out, errout bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&errout)
		if err := setEndpoint(root, endpoint, id.token); err != nil {
			t.Fatal(err)
		}
		err := root.ExecuteContext(ctx)
		return out.String(), err
	}
	out, err := skygo("targets", "list")
	if err != nil || !strings.Contains(out, "dgx") || strings.Contains(out, "\nlocal") {
		t.Fatalf("targets list = %q, %v", out, err)
	}
	task := filepath.Join(t.TempDir(), "task.yaml")
	write(t, task, "argv: [/bin/echo, hello from dgx]\ntimeout_seconds: 30\n")
	out, err = skygo("run", "-f", task, "--wait", "--request-id", "ssh-alias")
	if err != nil || !strings.Contains(out, "\tdgx\tsucceeded") {
		t.Fatalf("run = %q, %v", out, err)
	}
	jobID, _, _ := strings.Cut(out, "\t")
	out, err = skygo("jobs", "logs", jobID)
	if err != nil || out != "hello from dgx\n" {
		t.Fatalf("logs = %q, %v", out, err)
	}
	write(t, task, "argv: [/bin/echo]\ntarget: local\n")
	if _, err := skygo("run", "-f", task); err == nil {
		t.Fatal("task naming the default local target was accepted")
	}

	record.mu.Lock()
	defer record.mu.Unlock()
	for _, h := range record.headers {
		if h.Get("Accept-Encoding") != "" || h.Get("Connect-Accept-Encoding") != "" {
			t.Fatalf("request offered compression: %q", h)
		}
	}
}
