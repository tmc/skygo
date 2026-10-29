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

// ValidAddress reports whether address is an SSH address Dial and
// NewClient accept: a canonical host:port whose host is a lowercase name
// or an IP address. It has no effects.
func ValidAddress(address string) bool {
	_, err := hostPort(address)
	return err == nil
}

// ValidFingerprint reports whether s is a pin Dial, NewClient and
// AgentSigner accept: "SHA256:" and the unpadded standard base64 of 32
// bytes, as ssh.FingerprintSHA256 returns it. It has no effects.
func ValidFingerprint(s string) bool { return validFingerprint(s) }

// OwnershipUnsettled reports whether err, or any error it wraps, is a
// receipt from this package of an original connection, file or request
// whose first Close failed or whose teardown had not finished: a
// *DialError with a Close error, a request error with a Close error, a
// held teardown, or a failed file Close. It inspects only those types,
// never error text or system error numbers.
//
// A false result means only that err holds no such receipt. It does not
// show that err came from a settled request, nor that goroutines a
// library started and does not expose have exited.
func OwnershipUnsettled(err error) bool {
	switch e := err.(type) {
	case nil:
		return false
	case *DialError:
		if e.close != nil {
			return true
		}
	case *requestError:
		if e.close != nil {
			return true
		}
	case *heldError, *closeError:
		return true
	}
	switch e := err.(type) {
	case interface{ Unwrap() error }:
		return OwnershipUnsettled(e.Unwrap())
	case interface{ Unwrap() []error }:
		for _, err := range e.Unwrap() {
			if OwnershipUnsettled(err) {
				return true
			}
		}
	}
	return false
}

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
//
// The client's TCP connection is closed once, by whichever of the
// client's Close, x/crypto's own teardown or a canceled Dial comes first;
// every Close returns that first result and never closes it again. After
// Close, the client's Wait joins its connection's reader.
//
// A failure after the TCP connection was opened is a *DialError: Dial has
// closed that connection once and returned only after its own
// cancellation callback and, for an authenticated connection, Wait.
func Dial(ctx context.Context, t Target) (*ssh.Client, error) {
	return dialer{(&net.Dialer{}).DialContext, ssh.NewClientConn}.dialSSH(ctx, t.config())
}

// A dialer opens Dial's TCP connection and runs its SSH handshake. Dial
// uses net.Dialer and ssh.NewClientConn; tests wrap them.
type dialer struct {
	dial      func(ctx context.Context, network, address string) (net.Conn, error)
	handshake func(c net.Conn, addr string, config *ssh.ClientConfig) (ssh.Conn, <-chan ssh.NewChannel, <-chan *ssh.Request, error)
}

func (d dialer) dialSSH(ctx context.Context, c config) (*ssh.Client, error) {
	signer, hosts, err := loadSSH(c)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, requestBudget)
	defer cancel()
	nc, err := d.dial(ctx, "tcp", c.sshAddress)
	if err != nil {
		return nil, fmt.Errorf("agentssh: dial: %w", err)
	}
	raw := &rawConn{Conn: nc, closed: make(chan struct{})}
	deadline, _ := ctx.Deadline()
	if err := raw.SetDeadline(deadline); err != nil {
		return nil, &DialError{op: "dial", err: err, close: raw.Close(), raw: raw}
	}
	var stopErr error // raw's Close result in the callback
	stopped := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(stopped)
		stopErr = raw.Close()
	})
	conn, chans, reqs, err := d.handshake(raw, c.sshAddress, &ssh.ClientConfig{
		User: c.user,
		Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: func(hostname string, _ net.Addr, key ssh.PublicKey) error {
			return hosts.check(hostname, key)
		},
		HostKeyAlgorithms: hosts.algorithms,
	})
	if !stop() {
		// ctx ended and the callback closed raw, whatever the handshake
		// reported. Join the callback before reading its result.
		<-stopped
		e := &DialError{op: "ssh handshake", err: context.Cause(ctx), handshake: err, close: stopErr, raw: raw}
		if err == nil {
			// The handshake finished late: settle the same connection.
			// Its Close returns the callback's result.
			conn.Close()
			e.wait = conn.Wait()
		}
		return nil, e
	}
	if err != nil {
		// x/crypto closes raw when the handshake fails; this returns that
		// first result and closes nothing again.
		return nil, &DialError{op: "ssh handshake", err: err, close: raw.Close(), raw: raw}
	}
	if err := raw.SetDeadline(time.Time{}); err != nil {
		e := &DialError{op: "ssh handshake", err: err, close: conn.Close(), raw: raw}
		e.wait = conn.Wait()
		return nil, e
	}
	return ssh.NewClient(conn, chans, reqs), nil
}

// A DialError reports a Dial that failed after its TCP connection was
// opened. Dial closed that connection once and joined its cancellation
// callback and, if the handshake had succeeded, the connection's Wait.
// It did not join the goroutines x/crypto starts inside a failed
// handshake; they hold only the closed connection.
//
// If CloseErr is not nil, the connection's state is unknown. The error
// retains the connection for as long as the caller holds the error. It is
// never closed again.
type DialError struct {
	op        string
	err       error // why Dial failed
	handshake error // the handshake's own result when ctx ended first
	close     error // the connection's first Close result
	wait      error // ssh.Conn.Wait result: a shutdown cause, not a failure
	raw       *rawConn
}

func (e *DialError) Error() string {
	s := "agentssh: " + e.op + ": " + e.err.Error()
	if e.close != nil {
		s += " (close: " + e.close.Error() + ")"
	}
	return s
}

func (e *DialError) Unwrap() []error { return nonNil(e.err, e.handshake, e.close) }

// CloseErr returns the first Close result of the TCP connection.
func (e *DialError) CloseErr() error { return e.close }
