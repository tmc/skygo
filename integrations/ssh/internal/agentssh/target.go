package agentssh

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// A Target names one SSH server, the agent behind it, and the files that
// authenticate both. It is the exported form of the package's
// configuration and is validated the same way: exactly one of KeyFile and
// Signer is set, SignerKey pins Signer, and every other field is required.
type Target struct {
	SSHAddress     string     // SSH server as canonical host:port
	User           string     // SSH user
	KeyFile        string     // unencrypted private key, mode 0400 or 0600
	Signer         ssh.Signer // caller-held signer, such as from AgentSigner
	SignerKey      string     // pinned SHA256 fingerprint of Signer's public key
	KnownHostsFile string     // known_hosts file dedicated to SSHAddress
	AgentAddress   string     // agent as seen from the SSH server: 127.0.0.1:port or [::1]:port
	AgentName      string     // DNS name in the agent's TLS certificate
	AgentCAFile    string     // PEM certificates that sign the agent's certificate
}

func (t Target) config() config {
	return config{
		sshAddress:     t.SSHAddress,
		user:           t.User,
		keyFile:        t.KeyFile,
		signer:         t.Signer,
		signerKey:      t.SignerKey,
		knownHostsFile: t.KnownHostsFile,
		agentAddress:   t.AgentAddress,
		agentName:      t.AgentName,
		agentCAFile:    t.AgentCAFile,
	}
}

// Endpoint returns the base URL of the agent.
func (t Target) Endpoint() string { return t.config().endpoint() }

// NewClient returns an HTTP client whose requests reach the agent t names.
// Each request uses its own SSH connection and direct-tcpip channel; the
// client refuses redirects and never retries.
func NewClient(t Target) (*http.Client, error) { return newClient(t.config()) }

// AgentSigner returns the one key held by a whose SHA256 fingerprint is
// fingerprint. See agentSigner.
func AgentSigner(a agent.Agent, fingerprint string) (ssh.Signer, error) {
	return agentSigner(a, fingerprint)
}

// Dial returns an SSH client authenticated to the server t names, with the
// same strict host key check and client key as NewClient. Dialing and the
// handshake are bounded by ctx and the request budget. The client requests
// no agent forwarding and refuses channels the server opens. The caller
// runs its own fixed commands and closes the client.
func Dial(ctx context.Context, t Target) (*ssh.Client, error) {
	c := t.config()
	signer, hosts, err := loadSSH(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.sshAddress)
	if err != nil {
		return nil, fmt.Errorf("agentssh: dial: %w", err)
	}
	deadline, _ := ctx.Deadline()
	if err := raw.SetDeadline(deadline); err != nil {
		raw.Close()
		return nil, fmt.Errorf("agentssh: dial: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	conn, chans, reqs, err := ssh.NewClientConn(raw, c.sshAddress, &ssh.ClientConfig{
		User: c.user,
		Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: func(hostname string, _ net.Addr, key ssh.PublicKey) error {
			return hosts.check(hostname, key)
		},
		HostKeyAlgorithms: hosts.algorithms,
	})
	if !stop() {
		// ctx ended and closed raw, whatever the handshake reported.
		if err == nil {
			conn.Close()
		}
		return nil, fmt.Errorf("agentssh: ssh handshake: %w", context.Cause(ctx))
	}
	if err != nil {
		raw.Close()
		return nil, fmt.Errorf("agentssh: ssh handshake: %w", err)
	}
	if err := raw.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("agentssh: ssh handshake: %w", err)
	}
	return ssh.NewClient(conn, chans, reqs), nil
}
