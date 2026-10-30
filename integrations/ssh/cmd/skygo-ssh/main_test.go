package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/integrations/ssh/internal/agentidentity"
	"github.com/tmc/skygo/integrations/ssh/internal/agentsession"
	"github.com/tmc/skygo/integrations/ssh/internal/bootstrap"
	"github.com/tmc/skygo/internal/cli"
	"github.com/tmc/skygo/internal/local"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == local.SupervisorCommand {
		os.Exit(local.RunSupervisor(os.Args[2:]))
	}
	os.Exit(m.Run())
}

// The command's tests call the session implementation by the names it had
// in this package. Each forwarder calls the one implementation in
// agentsession or agentidentity unchanged.

// An identity is the fields of an agentidentity.Identity under their
// former names.
type identity struct {
	cert, key, token string
	bearer           string
}

func fromIdentity(id agentidentity.Identity) identity {
	return identity{cert: id.Cert, key: id.Key, token: id.Token, bearer: id.Bearer}
}

func loadIdentity(dir, name string) (identity, error) {
	id, err := agentidentity.Load(dir, name)
	return fromIdentity(id), err
}

func checkIdentity(dir, name string) (identity, error) {
	id, err := agentidentity.Check(dir, name)
	return fromIdentity(id), err
}

func readToken(name string) (string, error) { return agentidentity.ReadToken(name) }

// An identityTransport is agentsession.Transport as a positional literal.
type identityTransport struct{ next http.RoundTripper }

func (t identityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return agentsession.Transport{Next: t.next}.RoundTrip(req)
}

func agentSigner(socket, fingerprint string) (ssh.Signer, io.Closer, error) {
	return agentsession.AgentSigner(socket, fingerprint)
}

var errWrongAgent = agentsession.ErrWrongAgent

func waitReady(ctx context.Context, hc *http.Client, endpoint, token, target string, workspace bool, status func(context.Context) (bootstrap.State, error)) error {
	return agentsession.WaitReady(ctx, hc, endpoint, token, target, workspace, status)
}

func checkReady(ctx context.Context, cc skyv1connect.ControlServiceClient, token, target string, workspace bool) error {
	return agentsession.CheckReady(ctx, cc, token, target, workspace)
}

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want flags
		rest []string
		err  string // "" if accepted
	}{
		{
			name: "defaults",
			args: []string{"--host", "dgx", "--known-hosts", "kh", "run", "-f", "task.yaml", "--wait"},
			want: flags{host: "dgx", knownHosts: "kh", target: "dgx", port: defaultPort},
			rest: []string{"run", "-f", "task.yaml", "--wait"},
		},
		{
			name: "all",
			args: []string{"-host=dgx", "-known-hosts=kh", "-identity-fingerprint=SHA256:x", "-source", "/src", "-target-name", "spark", "-agent-port", "50000", "-restart-stopped", "jobs", "list"},
			want: flags{host: "dgx", knownHosts: "kh", fingerprint: "SHA256:x", source: "/src", target: "spark", port: 50000, restartStopped: true},
			rest: []string{"jobs", "list"},
		},
		{
			name: "installation",
			args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "/srv/skygo/data", "--remote-auth-file", "/srv/skygo/auth.json", "--remote-artifact-dir", "/srv/skygo/artifacts", "--scoped-token-file", "tok", "artifacts", "upload", "src"},
			want: flags{host: "dgx", knownHosts: "kh", target: "dgx", port: defaultPort,
				installation: bootstrap.Installation{DataDir: "/srv/skygo/data", AuthFile: "/srv/skygo/auth.json", ArtifactDir: "/srv/skygo/artifacts"}, scopedToken: "tok"},
			rest: []string{"artifacts", "upload", "src"},
		},
		{name: "installation without token", args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "/d", "--remote-auth-file", "/a", "--remote-artifact-dir", "/r"}, err: "go together"},
		{name: "token without installation", args: []string{"--host", "dgx", "--known-hosts", "kh", "--scoped-token-file", "tok"}, err: "go together"},
		{name: "data dir alone", args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "/d"}, err: "go together"},
		{name: "relative remote path", args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "d", "--remote-auth-file", "/a", "--remote-artifact-dir", "/r", "--scoped-token-file", "tok"}, err: "clean absolute"},
		{name: "unclean remote path", args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "/d", "--remote-auth-file", "/x/../a", "--remote-artifact-dir", "/r", "--scoped-token-file", "tok"}, err: "clean absolute"},
		{name: "shell remote path", args: []string{"--host", "dgx", "--known-hosts", "kh", "--remote-data-dir", "/d", "--remote-auth-file", "/a", "--remote-artifact-dir", "/r;id", "--scoped-token-file", "tok"}, err: "clean absolute"},
		{name: "no host", args: []string{"--known-hosts", "kh", "jobs", "list"}, err: "required"},
		{name: "no known hosts", args: []string{"--host", "dgx", "jobs", "list"}, err: "required"},
		{name: "alias is not a target name", args: []string{"--host", "DGX_1", "--known-hosts", "kh"}, err: "--target-name"},
		{name: "bad target name", args: []string{"--host", "dgx", "--known-hosts", "kh", "--target-name", "a;b"}, err: "lowercase DNS name"},
		{name: "low port", args: []string{"--host", "dgx", "--known-hosts", "kh", "--agent-port", "22"}, err: "agent-port"},
		{name: "key and fingerprint", args: []string{"--host", "dgx", "--known-hosts", "kh", "--key-file", "k", "--identity-fingerprint", "SHA256:x"}, err: "exclusive"},
		{name: "skygo flag first", args: []string{"--host", "dgx", "--known-hosts", "kh", "--json", "jobs", "list"}, err: "not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, rest, err := parseFlags(tt.args, io.Discard)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("parseFlags = %+v, %v; want %q", got, err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || !slices.Equal(rest, tt.rest) {
				t.Fatalf("parseFlags = %+v %q; want %+v %q", got, rest, tt.want, tt.rest)
			}
		})
	}
}

func TestCheckArgs(t *testing.T) {
	tests := []struct {
		args   []string
		needed bool
		err    string
	}{
		{[]string{"run", "-f", "task.yaml", "--wait"}, true, ""},
		{[]string{"jobs", "logs", "j1"}, true, ""},
		{[]string{"targets", "list", "--json"}, true, ""},
		{nil, false, ""},
		{[]string{"jobs"}, false, ""},
		{[]string{"run", "--help"}, false, ""},
		{[]string{"help", "run"}, false, ""},
		{[]string{"server"}, false, "remote host"},
		{[]string{"auth", "init", "--auth-file", "f"}, false, "remote host"},
		{[]string{"state", "inspect"}, false, "remote host"},
		{[]string{"dashboard"}, false, "not supported"},
		{[]string{"jobs", "list", "--endpoint", "http://127.0.0.1:8080"}, false, "--endpoint is set by skygo-ssh"},
		{[]string{"jobs", "list", "--endpoint=http://x"}, false, "--endpoint"},
		{[]string{"--token", "t", "jobs", "list"}, false, "--token"},
		{[]string{"jobs", "list", "--token-file=/t"}, false, "--token-file"},
		{[]string{"no-such-command"}, false, "unknown command"},
	}
	t.Setenv("SKYGO_TOKEN", "")
	for _, tt := range tests {
		needed, err := checkArgs(cli.NewWithClient(nil, nil, cli.ClientOptions{}), tt.args)
		if tt.err != "" {
			if err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("checkArgs(%q) = %v, %v; want %q", tt.args, needed, err, tt.err)
			}
			continue
		}
		if err != nil || needed != tt.needed {
			t.Errorf("checkArgs(%q) = %v, %v; want %v", tt.args, needed, err, tt.needed)
		}
	}
	t.Setenv("SKYGO_TOKEN", "elsewhere")
	if _, err := checkArgs(cli.NewWithClient(nil, nil, cli.ClientOptions{}), []string{"jobs", "list"}); err == nil || !strings.Contains(err.Error(), "SKYGO_TOKEN") {
		t.Errorf("checkArgs with SKYGO_TOKEN = %v", err)
	}
}

// A captureTransport records the request it is given.
type captureTransport struct{ got *http.Request }

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.got = req
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

func TestIdentityTransport(t *testing.T) {
	next := &captureTransport{}
	body := io.NopCloser(strings.NewReader("payload"))
	req, err := http.NewRequest(http.MethodPost, "https://agent.skygo.invalid/skygo.v1.ControlService/GetJob", body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Connect-Accept-Encoding", "gzip")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/proto")
	if _, err := (identityTransport{next}).RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	out := next.got
	if out.Header.Get("Accept-Encoding") != "" || out.Header.Get("Connect-Accept-Encoding") != "" {
		t.Errorf("sent encodings %q", out.Header)
	}
	if out.Header.Get("Authorization") != "Bearer secret" || out.Header.Get("Content-Type") != "application/proto" || out.Method != http.MethodPost || out.URL.String() != req.URL.String() {
		t.Errorf("request changed: %s %s %q", out.Method, out.URL, out.Header)
	}
	if data, _ := io.ReadAll(out.Body); string(data) != "payload" {
		t.Errorf("body = %q", data)
	}
	if req.Header.Get("Accept-Encoding") != "gzip" {
		t.Error("caller's request was modified")
	}

	// A request without an offer is passed through as is.
	plain, _ := http.NewRequest(http.MethodGet, "https://agent.skygo.invalid/", nil)
	if _, err := (identityTransport{next}).RoundTrip(plain); err != nil || next.got != plain {
		t.Errorf("plain request was copied: %v", err)
	}
}

func writePublicKey(t *testing.T, name string) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name+".pub", ssh.MarshalAuthorizedKey(key), 0o644); err != nil {
		t.Fatal(err)
	}
	return ssh.FingerprintSHA256(key)
}

func TestPublicFingerprint(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "id_a"), filepath.Join(dir, "id_b")
	missing := filepath.Join(dir, "id_missing")
	want := writePublicKey(t, a)
	if got, err := publicFingerprint([]string{missing, a, a}); err != nil || got != want {
		t.Fatalf("publicFingerprint = %q, %v; want %q", got, err, want)
	}
	if _, err := publicFingerprint([]string{missing}); err == nil || !strings.Contains(err.Error(), "--identity-fingerprint") {
		t.Fatalf("publicFingerprint without keys = %v", err)
	}
	writePublicKey(t, b)
	if _, err := publicFingerprint([]string{a, b}); err == nil || !strings.Contains(err.Error(), "2 public keys") {
		t.Fatalf("publicFingerprint with two keys = %v", err)
	}
}

func TestAgentSigner(t *testing.T) {
	keyring := agent.NewKeyring()
	var fingerprints []string
	for range 2 {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if err := keyring.Add(agent.AddedKey{PrivateKey: priv}); err != nil {
			t.Fatal(err)
		}
		signer, err := ssh.NewSignerFromKey(priv)
		if err != nil {
			t.Fatal(err)
		}
		fingerprints = append(fingerprints, ssh.FingerprintSHA256(signer.PublicKey()))
	}
	socket := filepath.Join(t.TempDir(), "a")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				agent.ServeAgent(keyring, c)
			}()
		}
	}()

	signer, closer, err := agentSigner(socket, fingerprints[1])
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	if got := ssh.FingerprintSHA256(signer.PublicKey()); got != fingerprints[1] {
		t.Fatalf("signer = %s, want %s", got, fingerprints[1])
	}
	if _, err := signer.Sign(rand.Reader, []byte("session")); err != nil {
		t.Fatalf("Sign through the agent: %v", err)
	}
	other := writePublicKey(t, filepath.Join(t.TempDir(), "other"))
	if _, _, err := agentSigner(socket, other); err == nil || !strings.Contains(err.Error(), "holds no key") {
		t.Fatalf("agentSigner for an absent key = %v", err)
	}
	if _, _, err := agentSigner("", fingerprints[0]); err == nil || !strings.Contains(err.Error(), "SSH_AUTH_SOCK") {
		t.Fatalf("agentSigner without a socket = %v", err)
	}
}

// Cleanup/error identity coverage moved to sshagent TestCommandCloseSession.
func TestRunHelp(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(t.Context(), []string{"-h"}, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stderr.String(), "--known-hosts") && !strings.Contains(stderr.String(), "-known-hosts") {
		t.Fatalf("usage = %q", stderr.String())
	}
}
