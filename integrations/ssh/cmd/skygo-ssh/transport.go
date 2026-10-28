package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"time"

	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// agentOpTimeout bounds each request to the local SSH agent.
const agentOpTimeout = 10 * time.Second

// An identityTransport removes compression negotiation from requests
// before next sends them. The generated Connect clients offer gzip, which
// the agent transport refuses; without the offer the agent answers
// uncompressed. Requests are otherwise unchanged and never retried.
type identityTransport struct{ next http.RoundTripper }

func (t identityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Accept-Encoding") == "" && req.Header.Get("Connect-Accept-Encoding") == "" {
		return t.next.RoundTrip(req)
	}
	out := req.Clone(req.Context())
	out.Header.Del("Accept-Encoding")
	out.Header.Del("Connect-Accept-Encoding")
	return t.next.RoundTrip(out)
}

// A boundedConn sets a deadline on its connection before each write, so
// every request to the SSH agent and its reply finish within timeout.
type boundedConn struct {
	net.Conn
	timeout time.Duration
}

func (c boundedConn) Write(p []byte) (int, error) {
	if err := c.SetDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

// agentSigner returns the key with the pinned fingerprint from the SSH
// agent listening on socket, and the connection to close once no client
// needs the signer. It only lists keys and signs SSH authentications.
func agentSigner(socket, fingerprint string) (ssh.Signer, io.Closer, error) {
	if socket == "" {
		return nil, nil, errors.New("SSH_AUTH_SOCK is not set; start an SSH agent holding the key or pass --key-file")
	}
	conn, err := net.DialTimeout("unix", socket, agentOpTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh agent: %w", err)
	}
	signer, err := agentssh.AgentSigner(agent.NewClient(boundedConn{conn, agentOpTimeout}), fingerprint)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return signer, conn, nil
}

// publicFingerprint returns the SHA256 fingerprint of the one public key
// found next to the identity files, as NAME.pub. It reads no private key.
func publicFingerprint(identityFiles []string) (string, error) {
	var found []string
	for _, name := range identityFiles {
		data, err := readLimited(name+".pub", 64<<10)
		if err != nil {
			continue
		}
		key, _, _, _, err := ssh.ParseAuthorizedKey(data)
		if err != nil {
			continue
		}
		if fp := ssh.FingerprintSHA256(key); !slices.Contains(found, fp) {
			found = append(found, fp)
		}
	}
	switch len(found) {
	case 0:
		return "", errors.New("no public key next to the configured identity files; pass --identity-fingerprint")
	case 1:
		return found[0], nil
	default:
		return "", fmt.Errorf("identity files hold %d public keys %v; pass --identity-fingerprint", len(found), found)
	}
}

func readLimited(name string, limit int64) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, limit))
}
