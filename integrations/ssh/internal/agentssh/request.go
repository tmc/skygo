package agentssh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"golang.org/x/crypto/ssh"
)

const (
	maxRequest    = 1 << 20 // encoded request, headers included
	maxResponse   = 1 << 20 // response, headers included
	requestBudget = 10 * time.Second
)

var (
	errRequestTooLarge  = errors.New("agentssh: request larger than 1 MiB")
	errResponseTooLarge = errors.New("agentssh: response larger than 1 MiB")
)

// A transport sends each HTTP request over its own SSH connection and
// direct-tcpip channel to the configured agent. It never pools, retries or
// replays. Once a request's bytes are written, a failure leaves its outcome
// at the agent unknown.
type transport struct {
	config config
	signer ssh.Signer
	hosts  *hostKeys
	tls    *tls.Config

	dial        func(ctx context.Context, network, address string) (net.Conn, error)
	budget      time.Duration // lifetime of one request from dial through response body
	closeBudget time.Duration // how long a close waits before reporting a held frame
}

// RoundTrip implements http.RoundTripper. It refuses requests that are not
// GET or POST to the agent's endpoint, that ask for compression or a
// protocol upgrade, or whose encoding exceeds 1 MiB, before dialing.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	wire, err := t.encode(req)
	if err != nil {
		return nil, err
	}
	f := newFrame(req.Context(), time.Now().Add(t.budget), t.closeBudget)
	go f.watch(req.Context())
	resp, op, err := t.exchange(f, req, wire)
	if err != nil {
		return nil, f.fail(op, err)
	}
	return resp, nil
}

// encode checks req, reads and closes its body, and returns the exact
// bytes to send. It uses no network.
func (t *transport) encode(req *http.Request) ([]byte, error) {
	err := t.check(req)
	body, berr := consume(req.Body, err == nil)
	if err != nil || berr != nil {
		return nil, errors.Join(err, berr)
	}
	out := req.Clone(req.Context())
	out.Host = t.config.agentName
	out.ContentLength = int64(len(body))
	out.TransferEncoding = nil
	out.Close = true
	out.Body = nil
	out.GetBody = nil
	if len(body) > 0 {
		out.Body = io.NopCloser(bytes.NewReader(body))
	}
	var buf limitedBuffer
	if err := out.Write(&buf); err != nil {
		return nil, fmt.Errorf("agentssh: encode request: %w", err)
	}
	return buf.buf.Bytes(), nil
}

func (t *transport) check(req *http.Request) error {
	u := req.URL
	switch {
	case req.Method != http.MethodPost && req.Method != http.MethodGet:
		return fmt.Errorf("agentssh: method %q refused", req.Method)
	case u == nil || u.Scheme != "https" || u.Host != t.config.agentName || u.User != nil || u.Opaque != "":
		return fmt.Errorf("agentssh: request URL must be within %s", t.config.endpoint())
	case req.Host != "" && req.Host != t.config.agentName:
		return errors.New("agentssh: foreign Host refused")
	case len(req.TransferEncoding) > 1 || len(req.TransferEncoding) == 1 && req.TransferEncoding[0] != "chunked":
		return errors.New("agentssh: transfer encoding refused")
	case len(req.Trailer) > 0:
		return errors.New("agentssh: trailers refused")
	}
	for key, values := range req.Header {
		if !validToken(key) {
			return fmt.Errorf("agentssh: header name %q refused", key)
		}
		switch k := http.CanonicalHeaderKey(key); k {
		case "Connection", "Upgrade", "Expect", "Host", "Content-Length", "Transfer-Encoding", "Te", "Trailer":
			return fmt.Errorf("agentssh: header %s refused", k)
		case "Content-Encoding", "Accept-Encoding":
			for _, v := range values {
				for _, coding := range strings.Split(v, ",") {
					coding, _, _ = strings.Cut(coding, ";")
					if c := strings.TrimSpace(coding); c != "" && c != "identity" {
						return fmt.Errorf("agentssh: %s %q refused", k, c)
					}
				}
			}
		}
	}
	return nil
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0) {
			return false
		}
	}
	return true
}

// consume reads at most maxRequest bytes of body if read is set, and closes
// body in every case.
func consume(body io.ReadCloser, read bool) ([]byte, error) {
	if body == nil {
		return nil, nil
	}
	var data []byte
	var err error
	if read {
		data, err = io.ReadAll(io.LimitReader(body, maxRequest+1))
		if err == nil && len(data) > maxRequest {
			err = errRequestTooLarge
		}
	}
	if cerr := body.Close(); cerr != nil {
		return nil, &closeError{owner: body, err: cerr, cause: err}
	}
	return data, err
}

// A limitedBuffer is a writer that refuses to hold more than maxRequest
// bytes. It has no other methods, so Request.Write cannot bypass Write.
type limitedBuffer struct{ buf bytes.Buffer }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > maxRequest {
		return 0, errRequestTooLarge
	}
	return b.buf.Write(p)
}

// exchange sends wire to the agent and reads the response header. It
// publishes every connection it opens to f before the connection can be
// abandoned. The returned op names the step that failed.
func (t *transport) exchange(f *requestFrame, req *http.Request, wire []byte) (*http.Response, string, error) {
	c, err := t.dial(f.dialCtx, "tcp", t.config.sshAddress)
	close(f.dialed)
	if err != nil {
		return nil, "dial", err
	}
	raw, err := f.publishRaw(c)
	if err != nil {
		return nil, "dial", err
	}
	if err := raw.SetDeadline(f.deadline); err != nil {
		return nil, "dial", err
	}
	conn, chans, reqs, err := ssh.NewClientConn(raw, t.config.sshAddress, t.sshConfig(f))
	if err != nil {
		return nil, "ssh handshake", err
	}
	f.publishSSH(conn)
	// NewClient services the server's requests and refuses its channel opens.
	ch, err := f.dialAgent(ssh.NewClient(conn, chans, reqs), t.config.agentAddress, time.Until(f.deadline))
	if err != nil {
		return nil, "direct-tcpip", err
	}
	tc := tls.Client(ch, t.tls)
	if err := tc.Handshake(); err != nil {
		return nil, "tls handshake", err
	}
	if _, err := tc.Write(wire); err != nil {
		return nil, "write", err
	}
	resp, err := http.ReadResponse(bufio.NewReader(&limitedReader{r: tc, n: maxResponse}), req)
	if err != nil {
		return nil, "read", err
	}
	if err := f.publishBody(resp.Body); err != nil {
		return nil, "read", err
	}
	if resp.StatusCode < 200 || 300 <= resp.StatusCode && resp.StatusCode < 400 {
		return nil, "read", fmt.Errorf("status %q refused", resp.Status)
	}
	if e := resp.Header.Get("Content-Encoding"); e != "" && e != "identity" {
		return nil, "read", fmt.Errorf("content encoding %q refused", e)
	}
	resp.Body = &responseBody{frame: f, body: resp.Body}
	return resp, "", nil
}

func (t *transport) sshConfig(f *requestFrame) *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User: t.config.user,
		Auth: []ssh.AuthMethod{ssh.PublicKeys(t.signer)},
		HostKeyCallback: func(hostname string, _ net.Addr, key ssh.PublicKey) error {
			if err := t.hosts.check(hostname, key); err != nil {
				return err
			}
			f.mu.Lock()
			f.hostKey = ssh.FingerprintSHA256(key)
			f.mu.Unlock()
			return nil
		},
		HostKeyAlgorithms: t.hosts.algorithms,
	}
}

// A limitedReader returns errResponseTooLarge once n bytes have been read.
type limitedReader struct {
	r io.Reader
	n int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, errResponseTooLarge
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	return n, err
}

// A requestError reports a request that failed after it began to dial.
// Its frame owns every connection the request opened. If op is "write" or
// "read", the agent may have received the request.
type requestError struct {
	op    string
	err   error
	cause error // cancellation cause, if the request was canceled
	close error // the frame's close result
	frame *requestFrame
}

func (e *requestError) Error() string {
	s := "agentssh: " + e.op + ": " + e.err.Error()
	if e.cause != nil {
		s += " (canceled: " + e.cause.Error() + ")"
	}
	if e.close != nil {
		s += " (close: " + e.close.Error() + ")"
	}
	return s
}

func (e *requestError) Unwrap() []error { return nonNil(e.err, e.cause, e.close) }

// An agentIdentity is the request domain an operator enrolled for an
// installed agent.
type agentIdentity struct {
	lineage string
	epoch   uint64
}

// checkAgent confirms that the agent behind client reports want as its
// request domain and answers GetServerInfo. It sends only those read-only
// requests and GetRetention, authorized by token, which never appears in
// its errors.
func checkAgent(ctx context.Context, client *http.Client, c config, token string, want agentIdentity) error {
	if !validLineage(want.lineage) || want.epoch == 0 {
		return errors.New("agentssh: invalid enrolled request domain")
	}
	if token == "" || !validHeaderValue(token) {
		return errors.New("agentssh: invalid bearer token")
	}
	cc := skyv1connect.NewControlServiceClient(client, c.endpoint(), connect.WithAcceptCompression("gzip", nil, nil))
	retention := connect.NewRequest(&v1.GetRetentionRequest{})
	retention.Header().Set("Authorization", "Bearer "+token)
	r, err := cc.GetRetention(ctx, retention)
	if err != nil {
		return fmt.Errorf("agentssh: get retention: %w", err)
	}
	d := r.Msg.GetDomain()
	if !r.Msg.GetEnabled() || d == nil {
		return errors.New("agentssh: agent reports no request domain")
	}
	if d.Lineage != want.lineage || d.Epoch != want.epoch {
		return fmt.Errorf("agentssh: agent request domain %q:%d is not the enrolled %s:%d", d.Lineage, d.Epoch, want.lineage, want.epoch)
	}
	info := connect.NewRequest(&v1.GetServerInfoRequest{})
	info.Header().Set("Authorization", "Bearer "+token)
	if _, err := cc.GetServerInfo(ctx, info); err != nil {
		return fmt.Errorf("agentssh: get server info: %w", err)
	}
	return nil
}

func validLineage(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !('0' <= s[i] && s[i] <= '9' || 'a' <= s[i] && s[i] <= 'f') {
			return false
		}
	}
	return true
}

func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == 0x7f {
			return false
		}
	}
	return true
}
