package remotehelper

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
)

var errTransportPolicy = errors.New("remote transport policy refused")

// client owns one synchronous response at a time. Callers must inspect
// takeError after EVERY generated call, even an error, before retry or publish.
type client struct {
	remote        skyv1connect.RemoteDataServiceClient
	httpClient    *http.Client
	transport     *http.Transport
	origin, token string
	mu            sync.Mutex
	active        bool
	bodyErr       error
}

func validToken(token string) bool {
	if len(token) != 114 || !strings.HasPrefix(token, "sgd1.") || token[37] != '.' || token[70] != '.' {
		return false
	}
	for _, value := range []string{token[5:37], token[38:70]} {
		for _, c := range value {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return false
			}
		}
	}
	secret := token[71:]
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(secret)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == secret
}

func transportOrigin(value string) (string, string, bool) {
	hostport, ok := strings.CutPrefix(value, "https://")
	if !ok || hostport == "" {
		return "", "", false
	}
	host, port, hasPort := strings.Cut(hostport, ":")
	if len(host) == 0 || len(host) > 253 || net.ParseIP(host) != nil {
		return "", "", false
	}
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", "", false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", "", false
			}
		}
	}
	if last := labels[len(labels)-1]; last[0] >= '0' && last[0] <= '9' {
		return "", "", false
	}
	origin := "https://" + host
	if hasPort {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
			return "", "", false
		}
		if n != 443 {
			origin += ":" + port
		}
	}
	return origin, host, true
}

func transportRoots(value string) (*x509.CertPool, bool) {
	if len(value) == 0 || len(value) > 64<<10 {
		return nil, false
	}
	pool := x509.NewCertPool()
	rest := []byte(value)
	count := 0
	for {
		rest = bytes.TrimLeft(rest, " \t\r\n")
		if len(rest) == 0 {
			break
		}
		if !bytes.HasPrefix(rest, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, false
		}
		block, tail := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, false
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, false
		}
		pool.AddCert(certificate)
		count++
		rest = tail
	}
	return pool, count != 0
}

func newClient(origin, caPEM, token string) (*client, error) {
	canonical, host, ok := transportOrigin(origin)
	if !ok || !validToken(token) {
		return nil, errTransportPolicy
	}
	roots, ok := transportRoots(caPEM)
	if !ok {
		return nil, errTransportPolicy
	}
	transport := &http.Transport{
		Proxy:                  nil,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: host},
		DisableCompression:     true,
		MaxResponseHeaderBytes: 8 << 10,
		MaxConnsPerHost:        1,
		MaxIdleConns:           1,
		MaxIdleConnsPerHost:    1,
		IdleConnTimeout:        30 * time.Second,
	}
	c := &client{origin: canonical, token: token, transport: transport}
	c.httpClient = &http.Client{Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	c.remote = skyv1connect.NewRemoteDataServiceClient(c, canonical,
		connect.WithAcceptCompression("gzip", nil, nil), connect.WithReadMaxBytes(96<<10))
	return c, nil
}

func transportLimit(path string) int64 {
	switch path {
	case skyv1connect.RemoteDataServiceReadInputProcedure:
		return 96 << 10
	case skyv1connect.RemoteDataServicePollReleaseProcedure,
		skyv1connect.RemoteDataServiceBeginOutputProcedure,
		skyv1connect.RemoteDataServiceWriteOutputProcedure,
		skyv1connect.RemoteDataServiceFinishOutputProcedure:
		return 8 << 10
	}
	return 0
}

// Do supplies only the fixed generated routes, exact origin and one scope token.
// Policy failures use fixed diagnostics; no URL/header/body/token is formatted.
func (c *client) Do(request *http.Request) (*http.Response, error) {
	c.mu.Lock()
	if c.bodyErr != nil || c.active {
		c.mu.Unlock()
		return nil, connect.NewError(connect.CodeFailedPrecondition, errTransportPolicy)
	}
	c.active = true
	c.mu.Unlock()
	release := func() { c.mu.Lock(); c.active = false; c.mu.Unlock() }
	u := request.URL
	limit := int64(0)
	if u != nil {
		limit = transportLimit(u.Path)
	}
	if u == nil || u.Scheme+"://"+u.Host != c.origin || u.User != nil || u.Opaque != "" || request.Host != "" && request.Host != u.Host || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || request.Method != http.MethodPost || limit == 0 {
		release()
		return nil, connect.NewError(connect.CodeFailedPrecondition, errTransportPolicy)
	}
	clone := request.Clone(request.Context())
	clone.Host = u.Host
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+c.token)
	clone.Header.Set("Accept-Encoding", "identity")
	clone.Header.Del("Connect-Accept-Encoding")
	response, err := c.httpClient.Do(clone)
	if err != nil {
		if response != nil {
			// net/http may already have closed this original on its error path.
			// Retain its unresolved outcome; never attempt a replacement Close.
			c.mu.Lock()
			c.bodyErr = &responseUncertainError{response: response, cause: err}
			c.mu.Unlock()
		} else {
			release()
		}
		return nil, errors.New("remote request failed")
	}
	body := &transportBody{owner: c, original: response.Body, remaining: limit}
	response.Body = body
	if len(response.Header.Values("Content-Encoding")) != 0 || response.Uncompressed || response.StatusCode >= 300 && response.StatusCode < 400 || response.ContentLength > limit {
		if err := body.Close(); err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
		return nil, connect.NewError(connect.CodeFailedPrecondition, errTransportPolicy)
	}
	return response, nil
}

// takeError is sticky: a response policy refusal or Close uncertainty forbids
// all later requests and publication.
func (c *client) takeError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodyErr
}

// Close releases idle pool connections only. It is not an active-body join.
func (c *client) Close() { c.transport.CloseIdleConnections() }

type bodyCloseError struct {
	body  io.ReadCloser
	cause error
}

func (e *bodyCloseError) Error() string { return "remote response close uncertain" }
func (e *bodyCloseError) Unwrap() error { return e.cause }

type responseUncertainError struct {
	response *http.Response
	cause    error
}

func (e *responseUncertainError) Error() string { return "remote response uncertain" }
func (e *responseUncertainError) Unwrap() error { return e.cause }

type transportBody struct {
	owner     *client
	original  io.ReadCloser
	remaining int64
	mu        sync.Mutex
	closed    bool
	closeErr  error
	policyErr error // first read-limit refusal, latched on owner
}

func (b *transportBody) Read(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.policyErr != nil {
		return 0, b.policyErr
	}
	if int64(len(data)) > b.remaining+1 {
		data = data[:b.remaining+1]
	}
	n, err := b.original.Read(data)
	if int64(n) > b.remaining {
		n = int(b.remaining)
		b.remaining = 0
		// Latch the refusal: Connect replaces a body read error on a non-200
		// response with the status code, which the caller would then retry.
		b.policyErr = connect.NewError(connect.CodeResourceExhausted, errTransportPolicy)
		b.owner.mu.Lock()
		if b.owner.bodyErr == nil {
			b.owner.bodyErr = b.policyErr
		}
		b.owner.mu.Unlock()
		return n, b.policyErr
	}
	b.remaining -= int64(n)
	return n, err
}

func (b *transportBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return b.closeErr
	}
	b.closed = true
	if err := b.original.Close(); err != nil {
		// Close uncertainty outranks a latched policy refusal; keep both.
		cause := err
		if b.policyErr != nil {
			cause = errors.Join(err, b.policyErr)
		}
		b.closeErr = &bodyCloseError{body: b.original, cause: cause}
	}
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	if b.closeErr != nil {
		b.owner.bodyErr = b.closeErr
	} else {
		b.owner.active = false
	}
	return b.closeErr
}

var _ connect.HTTPClient = (*client)(nil)
