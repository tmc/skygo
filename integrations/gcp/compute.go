package gce

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/oauth2"
)

const (
	computeBase  = "https://compute.googleapis.com/compute/v1/"
	callTimeout  = 60 * time.Second // whole life of one original request
	tokenMargin  = 2 * time.Minute  // token validity required before each dispatch
	maxBodyBytes = 1 << 20          // largest accepted response body
	maxListPages = 4                // zoneOperations.list pages per recovery
	listPageSize = "500"            // documented maximum maxResults
	connectLimit = 10 * time.Second // dial and TLS handshake bound
	headerLimit  = 30 * time.Second // response header bound
)

// A computeClient sends single HTTPS requests to one fixed Compute Engine
// v1 endpoint. It never retries, follows redirects or uses a proxy.
type computeClient struct {
	hc   *http.Client
	base string
}

// newComputeClient returns the production client: the fixed Compute
// Engine endpoint and the system TLS roots.
func newComputeClient() *computeClient {
	return newConfiguredClient(computeBase, nil)
}

// newConfiguredClient returns the client that Create and Open use: no
// proxy, no HTTP/2 (whose transport may resend an unprocessed request),
// no connection reuse and bounded dial, handshake and header waits. base
// is the endpoint and roots the trusted TLS roots, nil meaning the
// system's. Only tests pass anything but computeBase and nil: a local
// TLS fixture's origin and its certificate. Every other choice is the
// production one.
func newConfiguredClient(base string, roots *x509.CertPool) *computeClient {
	t := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: connectLimit}).DialContext,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		TLSHandshakeTimeout:    connectLimit,
		ResponseHeaderTimeout:  headerLimit,
		MaxResponseHeaderBytes: 64 << 10,
		TLSNextProto:           map[string]func(string, *tls.Conn) http.RoundTripper{},
		ForceAttemptHTTP2:      false,
		DisableKeepAlives:      true,
	}
	return &computeClient{hc: newHTTPClient(t), base: base}
}

// newHTTPClient returns a client over rt that returns redirects to the
// caller instead of following them.
func newHTTPClient(rt http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: rt,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// A call is one original HTTP request. Its goroutine is the sole owner of
// the request, its response body and the body's first and only Close;
// done is closed after all three have returned. Nothing else closes the
// body, and neither a caller's context nor Machine.Close ends a call.
//
// A settled call keeps the actual response and its errors. If the first
// Close of the body failed, the Machine keeps the call itself as the
// owner of that failure; the body is never closed again.
type call struct {
	op       *operation // the mutation sent, or nil for a read
	what     string
	done     chan struct{}
	resp     *http.Response // the response, once headers arrived
	status   int
	body     []byte
	err      error // original request or body read error
	closeErr error // original result of the first and only body Close
}

// settled reports whether c has fully returned.
func (c *call) settled() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// start sends one request with its own fixed deadline, independent of any
// caller context, and returns its call.
func (cl *computeClient) start(what, method, path, query string, body []byte, tok *oauth2.Token) (*call, error) {
	u := cl.base + path
	if query != "" {
		u += "?" + query
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		cancel()
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	tok.SetAuthHeader(req)
	c := &call{what: what, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		defer cancel()
		resp, err := cl.hc.Do(req)
		if err != nil {
			c.err = err
			return
		}
		c.resp, c.status = resp, resp.StatusCode
		c.body, c.err = readBounded(resp.Body, maxBodyBytes)
		c.closeErr = resp.Body.Close()
	}()
	return c, nil
}

// Errors of a settled read; none carries server text or credentials.
var (
	errNotFound  = errors.New("not found")
	errAuth      = errors.New("request not authorized")
	errTransient = errors.New("no usable reply")
	errRefused   = errors.New("refused") // a mutation's definite HTTP refusal
)

// readResult classifies a settled read whose body closed cleanly and
// decodes a 200 reply into v. A transient error wraps the original one.
func (c *call) readResult(v any) error {
	if c.err != nil {
		return fmt.Errorf("%w: %w", errTransient, c.err)
	}
	switch {
	case c.status == http.StatusOK:
		if err := decodeStrict(c.body, v, false); err != nil {
			return wireErrorf(c.what, "malformed reply: %v", err)
		}
		return nil
	case c.status == http.StatusNotFound:
		return errNotFound
	case c.status == http.StatusUnauthorized || c.status == http.StatusForbidden:
		return errAuth
	case c.status == http.StatusTooManyRequests || c.status >= 500 || c.status == http.StatusRequestTimeout:
		return errTransient
	default:
		return wireErrorf(c.what, "unexpected HTTP status %d", c.status)
	}
}

// listQuery returns the zoneOperations.list query for one bounded page.
func listQuery(pageToken string) string {
	q := url.Values{"maxResults": {listPageSize}}
	if pageToken != "" {
		q.Set("pageToken", pageToken)
	}
	return q.Encode()
}
