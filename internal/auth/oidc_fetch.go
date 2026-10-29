package auth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// HTTPS key set retrieval for a jwks_uri. Keys are fetched only from the one
// URL the operator allowed when constructing the Engine, never from a URL
// found in a token, a key set or issuer metadata, and never by discovery.

const (
	jwksFetchTimeout    = 10 * time.Second
	jwksRefreshInterval = 5 * time.Second
	maxJWKSAge          = 30 * time.Second
	maxCABytes          = 64 << 10
	maxJWKSHeaderBytes  = 16 << 10
)

// jwksSource is a jwks_uri and its optional ca_file, as one OIDC file read
// named them.
type jwksSource struct {
	uri, caFile string
}

// jwksFetcher holds the last valid key set fetched from the allowed URL. The
// OIDC reader asks it for keys and never waits for a fetch; Engine.runJWKS
// fetches. Only a fetch that completed within jwksFetchTimeout of its start
// replaces the key set, which is then usable until maxJWKSAge after that
// fetch completed. Any other fetch, whether failed, late, cancelled or
// invalid, changes nothing, so it neither renews that age nor revives a
// replaced key set.
//
// A key the issuer removed therefore stops authenticating at most
// jwksFetchTimeout+maxJWKSAge (40s) after the removal: a fetch that started
// just before it may publish the old key set up to jwksFetchTimeout later.
// That bound assumes the allowed URL serves the issuer's current key set to
// every request sent after the removal; a stale cache or issuer behind the
// URL is outside it.
type jwksFetcher struct {
	allowed string
	now     func() time.Time
	wake    chan struct{} // buffered; a send asks runJWKS to fetch now

	mu      sync.Mutex
	want    jwksSource // named by the last OIDC read
	have    jwksSource // the source of data
	data    []byte     // a valid key set document, or nil
	fetched time.Time  // when the fetch that returned data completed
}

func newJWKSFetcher(allowed string, now func() time.Time) *jwksFetcher {
	return &jwksFetcher{allowed: allowed, now: now, wake: make(chan struct{}, 1)}
}

// keys returns the cached key set for src and the time the fetch that
// returned it completed. It records src as the source to fetch and wakes
// runJWKS when src changes. Any URL but the allowed one, a key set from
// another source, and a key set more than maxJWKSAge old are ErrUnavailable.
// A nil fetcher allows no URL.
func (f *jwksFetcher) keys(src jwksSource) ([]byte, time.Time, error) {
	if f == nil || src.uri != f.allowed {
		return nil, time.Time{}, ErrUnavailable
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if src != f.want {
		f.want = src
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
	if f.data == nil || f.have != src || f.now().Sub(f.fetched) > maxJWKSAge {
		return nil, time.Time{}, ErrUnavailable
	}
	return f.data, f.fetched, nil
}

// runJWKS runs e.oidcKeys until cancellation: it fetches when an OIDC read
// names a new source and jwksRefreshInterval after each fetch. It is the only
// fetcher and fetches synchronously, so it has at most one fetch outstanding.
// Cancellation, like the end of a fetch's window, cancels that fetch's
// request, but runJWKS returns only after the fetch, including its CA file
// read and response body read and Close, has returned; a fetch that returns
// after either publishes nothing. If a Close fails, the Engine retains the
// error and the original file or body, and runJWKS stops: nothing is fetched
// in its place.
func (e *Engine) runJWKS(ctx context.Context) {
	f := e.oidcKeys
	for ctx.Err() == nil && e.Err() == nil {
		if err := f.fetch(ctx); errors.Is(err, ErrCloseUncertain) {
			e.holdFetch(err)
			return
		}
		t := time.NewTimer(jwksRefreshInterval)
		select {
		case <-ctx.Done():
		case <-f.wake:
		case <-t.C:
		}
		t.Stop()
	}
}

// holdFetch retains err, from a fetch whose original Close failed, and
// notifies Changed if the Engine was not already held.
func (e *Engine) holdFetch(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.held == nil {
		close(e.changed)
		e.changed = make(chan struct{})
	}
	e.holdLocked(err)
}

// verifyJWKS parses a fetched key set. Tests replace it to model a slow
// verification; a replacement still calls parseJWKS.
var verifyJWKS = parseJWKS

// fetch fetches the key set for the source the last OIDC read named.
//
// A fetch's window is jwksFetchTimeout. It starts before the CA file read and
// covers the request, the response body read, its Close and parsing the key
// set; the request's context ends with it or with ctx. The window decides
// what is published, not how long fetch takes: a read or Close that ignores
// cancellation is waited for, never abandoned or replaced. Only a key set
// that was read in full, whose response body closed and that parses replaces
// the cache, and only if, once fetch holds f.mu to publish it, ctx has not
// ended and the window has not passed; it is dated then. Ending ctx or
// passing the window while fetch waits for f.mu publishes and redates
// nothing.
//
// Every result retains each cause: a failed fetch returns its original error
// and owner, with that error's text, joined with the cancellation or
// deadline cause if ctx ended or the window passed by the time it returned.
// A late result that otherwise succeeded is refused with fixed text and that
// cause. An uncertain Close therefore stays ErrCloseUncertain, late or not.
//
// A wake pending when fetch reads the source is for that source, so it is
// consumed.
func (f *jwksFetcher) fetch(ctx context.Context) error {
	f.mu.Lock()
	src := f.want
	select {
	case <-f.wake:
	default:
	}
	f.mu.Unlock()
	if src.uri == "" || src.uri != f.allowed {
		return ErrUnavailable
	}
	start := f.now()
	ctx, cancel := context.WithTimeout(ctx, jwksFetchTimeout)
	defer cancel()
	data, err := getJWKS(ctx, src)
	if err == nil {
		_, err = verifyJWKS(data)
	}
	if err != nil {
		return finishFetch(ctx, err, f.now().Sub(start))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	done := f.now()
	if err := finishFetch(ctx, nil, done.Sub(start)); err != nil {
		return err
	}
	f.have, f.data, f.fetched = src, data, done
	return nil
}

// finishFetch returns err, a fetch's result, with the reasons the fetch is
// too late to publish: ctx having ended, and elapsed passing
// jwksFetchTimeout, which is context.DeadlineExceeded if ctx has not
// reported it. With no such reason it returns err unchanged. Otherwise a nil
// err becomes the fixed-text refusal, and any other err keeps its text and
// unwraps to both err and the refusal, so errors.Is and errors.As still find
// err, its causes and its owner.
func finishFetch(ctx context.Context, err error, elapsed time.Duration) error {
	var late []error
	ctxErr := ctx.Err()
	if ctxErr != nil {
		late = append(late, ctxErr)
	}
	if elapsed > jwksFetchTimeout && ctxErr != context.DeadlineExceeded {
		late = append(late, context.DeadlineExceeded)
	}
	if len(late) == 0 {
		return err
	}
	tooLate := &causeError{text: "fetch JWKS: too late", err: errors.Join(late...)}
	if err == nil {
		return tooLate
	}
	return &causeError{text: err.Error(), err: errors.Join(err, tooLate)}
}

// causeError gives an original error fixed text. Unwrap returns the original,
// so errors.Is and errors.As find it, but Error never includes its text,
// which may name a URL or a path or quote a response.
type causeError struct {
	text string
	err  error
}

func (e *causeError) Error() string { return e.text }
func (e *causeError) Unwrap() error { return e.err }

// bodyCloseError retains the original key set response body whose one Close
// failed. Its text omits the URL; Unwrap exposes the original request or
// read error, if any, and the Close error.
type bodyCloseError struct {
	body io.ReadCloser
	err  error
}

func (e *bodyCloseError) Error() string        { return "close JWKS response" }
func (e *bodyCloseError) Unwrap() error        { return e.err }
func (e *bodyCloseError) Is(target error) bool { return target == ErrCloseUncertain }

// doJWKS sends the key set request. Tests replace it to model a response,
// including one returned with an error, whose body read or Close fails.
var doJWKS = (*http.Client).Do

// getJWKS performs one bounded GET of src.uri, bounded in time by ctx. It
// trusts only the CA certificates in src.caFile if set, and otherwise the
// system roots. It uses no proxy, cookies or keep-alive connections, follows
// no redirects and requires status 200. Errors have fixed text, without URL
// or response text; Unwrap exposes the original request or read error.
//
// Any response, even one returned with an error, is closed exactly once,
// after its body is read or refused, and that Close decides the result: if
// it fails, the error retains the original body and joins the request or
// read error, and no bytes are returned. Cancelling ctx cancels the request;
// it does not stand for the body read or Close having returned.
func getJWKS(ctx context.Context, src jwksSource) (data []byte, err error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if src.caFile != "" {
		pem, err := readAuthFile(src.caFile, maxCABytes, false)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(pem) {
			return nil, ErrInvalid
		}
	}
	client := &http.Client{
		// A Transport with a nil Proxy ignores proxy environment variables.
		Transport: &http.Transport{
			TLSClientConfig:        cfg,
			TLSHandshakeTimeout:    jwksFetchTimeout,
			ResponseHeaderTimeout:  jwksFetchTimeout,
			MaxResponseHeaderBytes: maxJWKSHeaderBytes,
			DisableKeepAlives:      true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.uri, nil)
	if err != nil {
		return nil, ErrInvalid
	}
	req.Header.Set("Accept", "application/jwk-set+json, application/json")
	resp, err := doJWKS(client, req)
	if resp == nil || resp.Body == nil {
		if err != nil {
			return nil, &causeError{text: "fetch JWKS", err: err}
		}
		return nil, errors.New("fetch JWKS")
	}
	body := resp.Body
	defer func() {
		if closeErr := body.Close(); closeErr != nil {
			data = nil
			err = &bodyCloseError{body: body, err: errors.Join(err, closeErr)}
		}
	}()
	if err != nil {
		return nil, &causeError{text: "fetch JWKS", err: err}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("fetch JWKS: unexpected status")
	}
	data, err = io.ReadAll(io.LimitReader(body, maxJWKSBytes+1))
	if err != nil {
		return nil, &causeError{text: "read JWKS response", err: err}
	}
	if len(data) > maxJWKSBytes {
		return nil, ErrInvalid
	}
	return data, nil
}
