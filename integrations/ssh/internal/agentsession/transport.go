package agentsession

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/tmc/skygo/integrations/ssh/internal/agentssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// agentOpTimeout bounds each request to the local SSH agent.
const agentOpTimeout = 10 * time.Second

// A Transport removes compression negotiation from requests before Next
// sends them. The generated Connect clients offer gzip, which the agent
// transport refuses; without the offer the agent answers uncompressed.
// Requests are otherwise unchanged and never retried.
type Transport struct{ Next http.RoundTripper }

func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Accept-Encoding") == "" && req.Header.Get("Connect-Accept-Encoding") == "" {
		return t.Next.RoundTrip(req)
	}
	out := req.Clone(req.Context())
	out.Header.Del("Accept-Encoding")
	out.Header.Del("Connect-Accept-Encoding")
	return t.Next.RoundTrip(out)
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

// AgentSigner returns the key with the pinned fingerprint from the SSH
// agent listening on socket, and the connection to close once no client
// needs the signer. It only lists keys and signs SSH authentications.
// On failure it has closed the connection it opened, if any.
func AgentSigner(socket, fingerprint string) (ssh.Signer, io.Closer, error) {
	if socket == "" {
		return nil, nil, errors.New("SSH_AUTH_SOCK is not set; start an SSH agent holding the key or pass --key-file")
	}
	conn, err := net.DialTimeout("unix", socket, agentOpTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh agent: %w", err)
	}
	signer, err := agentssh.AgentSigner(agent.NewClient(boundedConn{conn, agentOpTimeout}), fingerprint)
	if err != nil {
		if cerr := conn.Close(); cerr != nil {
			return nil, nil, &UnsettledError{Err: err, SignerClose: cerr, signer: conn}
		}
		return nil, nil, err
	}
	return signer, conn, nil
}
