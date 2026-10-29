package cli

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// OpenID Connect sign-in for the CLI: the authorization code flow with PKCE
// (RFC 7636) for a native app over a loopback redirect (RFC 8252). Every
// endpoint is configured explicitly; nothing is discovered. The ID token is
// the only credential kept, in a new private file for --token-file. Access
// and refresh tokens are discarded. No ID, access or refresh token,
// authorization code or PKCE verifier is printed or put in an error. The
// authorization URL printed with --no-browser, or when no browser opens,
// contains the state, nonce and PKCE challenge, as any browser sign-in URL
// does.

const (
	maxOIDCResponseBytes    = 64 << 10
	maxOIDCCABytes          = 64 << 10
	maxOIDCCodeBytes        = 2048
	oidcBrowserTimeout      = 10 * time.Second
	oidcExchangeTimeout     = 30 * time.Second
	defaultOIDCLoginTimeout = 5 * time.Minute
	maxOIDCLoginTimeout     = 15 * time.Minute
)

// oidcAuthorizeParams are the authorization request parameters login sets.
// A configured authorization endpoint may not already contain them.
var oidcAuthorizeParams = []string{"response_type", "client_id", "redirect_uri", "scope", "state", "nonce", "code_challenge", "code_challenge_method"}

// oidcFailure retains originals and observable causes without rendering
// a provider's error text, URL or credential bytes.
type oidcFailure struct {
	message    string
	causes     []error
	response   *http.Response
	file       *os.File
	listener   *oidcListener
	output     *tokenOutput
	descriptor *tokenDescriptor
}

func (e *oidcFailure) Error() string   { return e.message }
func (e *oidcFailure) Unwrap() []error { return e.causes }

func oidcCauses(errs ...error) []error {
	var causes []error
	for _, err := range errs {
		if err != nil {
			causes = append(causes, err)
		}
	}
	return causes
}

// tokenDescriptor is one original first-Close attempt. Its number is never
// used to retry a failed Close: the kernel may already have reused it.
type tokenDescriptor struct {
	fd  int
	err error
}

// oidcLogin is one validated sign-in configuration.
type oidcLogin struct {
	issuer, clientID string
	authorize, token *url.URL
	caFile, output   string
	replace          bool
	port             int
	timeout          time.Duration
	browser          func(ctx context.Context, url string) error // nil: print the URL only
	notice           io.Writer
	now              func() time.Time
	callbackHook     func(stage string) // tests only; see oidcCallback.at
}

func (o *options) oidcCommand() *cobra.Command {
	root := &cobra.Command{Use: "oidc", Short: "Sign in with OpenID Connect"}
	var authorize, token string
	var noBrowser bool
	l := &oidcLogin{now: time.Now}
	login := &cobra.Command{Use: "login", Short: "Sign in with a browser and write the ID token to a private file", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if err := l.parse(authorize, token); err != nil {
			return err
		}
		l.notice = cmd.ErrOrStderr()
		l.browser = openBrowser
		if noBrowser {
			l.browser = nil
		}
		expires, err := l.run(cmd.Context())
		if err != nil {
			return err
		}
		if o.json {
			return json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
				Operation string    `json:"operation"`
				ExpiresAt time.Time `json:"expires_at"`
			}{"signed in", expires})
		}
		_, err = fmt.Fprintf(cmd.OutOrStdout(), "signed in\t%s\n", expires.Format(time.RFC3339))
		return err
	}}
	f := login.Flags()
	f.StringVar(&l.issuer, "issuer", "", "Issuer URL, exactly as the server's OIDC file names it")
	f.StringVar(&l.clientID, "client-id", "", "OAuth client ID; the server's OIDC audience")
	f.StringVar(&authorize, "authorization-endpoint", "", "HTTPS authorization endpoint")
	f.StringVar(&token, "token-endpoint", "", "HTTPS token endpoint")
	f.StringVar(&l.output, "output", "", "New private file for the ID token, for use with --token-file")
	f.BoolVar(&l.replace, "replace", false, "Replace an existing private token output")
	f.StringVar(&l.caFile, "ca-file", "", "PEM CA certificates to trust instead of the system roots")
	f.IntVar(&l.port, "redirect-port", 0, "Loopback redirect port (0 chooses one)")
	f.DurationVar(&l.timeout, "login-timeout", defaultOIDCLoginTimeout, "Bound on the whole sign-in (maximum 15m)")
	f.BoolVar(&noBrowser, "no-browser", false, "Print the sign-in URL instead of opening a browser")
	for _, name := range []string{"issuer", "client-id", "authorization-endpoint", "token-endpoint", "output"} {
		_ = login.MarkFlagRequired(name)
	}
	root.AddCommand(login)
	return root
}

// parse validates the configuration. The issuer is compared exactly with the
// ID token's iss, so it is not normalized.
func (l *oidcLogin) parse(authorize, token string) error {
	if !oidcHTTPS(l.issuer) || strings.Contains(l.issuer, "?") {
		return errors.New("issuer must be an HTTPS URL without query, fragment or user information")
	}
	if l.clientID == "" || len(l.clientID) > 255 || !validToken(l.clientID) {
		return errors.New("client ID must be printable ASCII of at most 255 bytes")
	}
	var err error
	if l.authorize, err = oidcEndpoint(authorize); err != nil {
		return fmt.Errorf("authorization endpoint: %w", err)
	}
	q := l.authorize.Query()
	for _, name := range oidcAuthorizeParams {
		if q.Has(name) {
			return fmt.Errorf("authorization endpoint must not set %s", name)
		}
	}
	if l.token, err = oidcEndpoint(token); err != nil {
		return fmt.Errorf("token endpoint: %w", err)
	}
	switch {
	case l.output == "":
		return errors.New("output is required")
	case l.port < 0 || l.port > 65535:
		return errors.New("redirect port must be between 0 and 65535")
	case l.timeout <= 0 || l.timeout > maxOIDCLoginTimeout:
		return errors.New("login timeout must be positive and at most 15m")
	}
	return nil
}

// oidcHTTPS reports whether s is an absolute HTTPS URL with a host and no
// user information or fragment.
func oidcHTTPS(s string) bool {
	if len(s) > 1024 || !validToken(s) {
		return false
	}
	u, err := url.Parse(s)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Opaque == "" && !strings.Contains(s, "#")
}

func oidcEndpoint(s string) (*url.URL, error) {
	if !oidcHTTPS(s) {
		return nil, errors.New("must be an HTTPS URL without fragment or user information")
	}
	return url.Parse(s)
}

// run signs in once and writes the ID token. It returns the token's expiry.
// The output directory is verified and held before anything else happens.
// An error says whether the token was published; after publication it may
// report that syncing or cleanup failed, and the token file stays.
// The timeout starts cancellation; it is not proof that a Close or callback
// has returned. run joins the original listener and all admitted callbacks
// before publishing, and returns every observable first-Close failure.
func (l *oidcLogin) run(ctx context.Context) (expires time.Time, err error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	out, err := openTokenOutput(l.output, l.replace)
	if err != nil {
		return time.Time{}, err
	}
	defer func() { err = errors.Join(err, out.Close()) }()
	client, err := l.client()
	if err != nil {
		return time.Time{}, err
	}
	state, err := oidcRandom()
	if err != nil {
		return time.Time{}, err
	}
	nonce, err := oidcRandom()
	if err != nil {
		return time.Time{}, err
	}
	verifier, err := oidcRandom()
	if err != nil {
		return time.Time{}, err
	}

	ln, err := listenOIDC(l.port, state, l.issuer, l.callbackHook)
	if err != nil {
		return time.Time{}, err
	}
	stopped := false
	defer func() {
		if !stopped {
			err = errors.Join(err, ln.stop())
		}
	}()

	redirect := "http://" + ln.cb.host + "/callback"
	sum := sha256.Sum256([]byte(verifier))
	u := *l.authorize
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", l.clientID)
	q.Set("redirect_uri", redirect)
	q.Set("scope", "openid")
	q.Set("state", state)
	q.Set("nonce", nonce)
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(sum[:]))
	q.Set("code_challenge_method", "S256")
	u.RawQuery = q.Encode()
	if err := l.openBrowser(ctx, u.String()); err != nil {
		fmt.Fprintf(l.notice, "Open this URL in a browser to sign in:\n%s\n", u.String())
	} else {
		fmt.Fprintln(l.notice, "Waiting for the browser sign-in")
	}

	var code string
	select {
	case <-ctx.Done():
		return time.Time{}, errors.New("sign-in did not complete before the login timeout")
	case r := <-ln.cb.done:
		if r.err != nil {
			return time.Time{}, r.err
		}
		code = r.code
	}
	stopErr := ln.stop()
	stopped = true
	if stopErr != nil {
		return time.Time{}, stopErr
	}
	idToken, err := l.exchange(ctx, client, code, redirect, verifier)
	if err != nil {
		return time.Time{}, err
	}
	expires, err = l.checkIDToken(idToken, nonce)
	if err != nil {
		return time.Time{}, err
	}
	if err := out.publish(idToken); err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

// oidcListener serves the loopback redirect. stop ends it: it closes the
// listener and connections, waits for the accept loop, and then refuses new
// callbacks and waits for every admitted one to return.
type oidcListener struct {
	cb       *oidcCallback
	srv      *http.Server
	served   chan struct{}
	once     sync.Once
	ln       *oidcListenerClose
	serveErr error
	stopErr  error
}

// oidcListenerClose records the original even if Serve closes it before
// Server.Close has registered the listener. It never repeats that Close.
type oidcListenerClose struct {
	net.Listener
	once sync.Once
	err  error
}

func (l *oidcListenerClose) Close() error {
	l.once.Do(func() { l.err = l.Listener.Close() })
	return l.err
}

func listenOIDC(port int, state, issuer string, hook func(stage string)) (*oidcListener, error) {
	ln, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen for the sign-in redirect: %w", err)
	}
	return serveOIDC(ln, state, issuer, hook), nil
}

func serveOIDC(ln net.Listener, state, issuer string, hook func(string)) *oidcListener {
	cb := newOIDCCallback(ln.Addr().String(), state, issuer)
	cb.hook = hook
	s := &oidcListener{
		cb:     cb,
		srv:    &http.Server{Handler: cb, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Second, MaxHeaderBytes: 16 << 10},
		served: make(chan struct{}),
		ln:     &oidcListenerClose{Listener: ln},
	}
	go func() {
		defer close(s.served)
		s.serveErr = s.srv.Serve(s.ln)
	}()
	return s
}

// stop is idempotent; a second call waits for the first to finish.
// http.Server.Close does not wait for handlers, so the callback's own
// admission count is the join.
func (s *oidcListener) stop() error {
	s.once.Do(func() {
		closeErr := s.srv.Close()
		<-s.served
		s.cb.close()
		serveErr := s.serveErr
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		if closeErr != nil || serveErr != nil || s.ln.err != nil {
			s.stopErr = &oidcFailure{message: "close sign-in listener", listener: s, causes: oidcCauses(closeErr, serveErr, s.ln.err)}
		}
	})
	return s.stopErr
}

// openBrowser runs the configured opener for at most oidcBrowserTimeout. A
// nil opener reports an error so the caller prints the URL.
func (l *oidcLogin) openBrowser(ctx context.Context, u string) error {
	if l.browser == nil {
		return errors.New("no browser")
	}
	ctx, cancel := context.WithTimeout(ctx, oidcBrowserTimeout)
	defer cancel()
	return l.browser(ctx, u)
}

// client returns an HTTPS client that trusts only l.caFile if set and
// otherwise the system roots. It uses no proxy, cookies or keep-alive
// connections and follows no redirects.
func (l *oidcLogin) client() (*http.Client, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if l.caFile != "" {
		data, err := readOIDCCA(l.caFile, maxOIDCCABytes, (*os.File).Close)
		if err != nil {
			return nil, &oidcFailure{message: "CA file must be a regular file of at most 64 KiB", causes: []error{err}}
		}
		cfg.RootCAs = x509.NewCertPool()
		if !cfg.RootCAs.AppendCertsFromPEM(data) {
			return nil, errors.New("CA file contains no PEM certificates")
		}
	}
	return &http.Client{
		// A Transport with a nil Proxy ignores proxy environment variables.
		Transport: &http.Transport{
			TLSClientConfig:        cfg,
			TLSHandshakeTimeout:    10 * time.Second,
			ResponseHeaderTimeout:  oidcExchangeTimeout,
			MaxResponseHeaderBytes: 16 << 10,
			DisableKeepAlives:      true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, nil
}

func readOIDCCA(path string, max int64, closeFile func(*os.File) error) (data []byte, err error) {
	f, err := openOIDCCA(path)
	if err != nil {
		return nil, &oidcFailure{message: "open sign-in CA file", causes: []error{err}}
	}
	defer func() {
		if closeErr := closeFile(f); closeErr != nil {
			data = nil
			err = &oidcFailure{message: "close sign-in CA file", file: f, causes: oidcCauses(err, closeErr)}
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return nil, &oidcFailure{message: "inspect sign-in CA file", file: f, causes: []error{err}}
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	data, err = io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, &oidcFailure{message: "read sign-in CA file", file: f, causes: []error{err}}
	}
	if int64(len(data)) > max {
		return nil, errors.New("file too large")
	}
	return data, nil
}

// exchange redeems code at the token endpoint and returns the ID token. Only
// an RFC 6749 error code from a refusal is reported.
func (l *oidcLogin) exchange(ctx context.Context, client *http.Client, code, redirect, verifier string) (token string, err error) {
	ctx, cancel := context.WithTimeout(ctx, oidcExchangeTimeout)
	defer cancel()
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirect},
		"client_id":     {l.clientID},
		"code_verifier": {verifier},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.token.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("invalid token endpoint")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", &oidcFailure{message: "token request failed", response: resp, causes: []error{err}}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			token = ""
			err = &oidcFailure{message: "close token response", response: resp, causes: oidcCauses(err, closeErr)}
		}
	}()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxOIDCResponseBytes+1))
	if err != nil {
		return "", &oidcFailure{message: "invalid token response", response: resp, causes: []error{err}}
	}
	if len(data) > maxOIDCResponseBytes {
		return "", errors.New("invalid token response")
	}
	if resp.StatusCode != http.StatusOK {
		var refusal struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &refusal)
		return "", fmt.Errorf("token request refused: %s", oauthErrorCode(refusal.Error))
	}
	var r struct {
		IDToken   string `json:"id_token"`
		TokenType string `json:"token_type"`
	}
	if json.Unmarshal(data, &r) != nil || r.IDToken == "" || !strings.EqualFold(r.TokenType, "Bearer") {
		return "", errors.New("invalid token response")
	}
	return r.IDToken, nil
}

// checkIDToken checks the claims that bind an ID token to this sign-in and
// returns its expiry. The token came directly from the TLS-authenticated
// token endpoint, so its signature is left to the server, which verifies it
// with its own configured keys (OIDC Core 3.1.3.7 step 6). The CLI reads no
// identity from the token: the server maps its subject.
func (l *oidcLogin) checkIDToken(token, nonce string) (time.Time, error) {
	if !validToken(token) {
		return time.Time{}, errors.New("ID token exceeds the 4096-byte credential limit or contains invalid bytes")
	}
	bad := errors.New("invalid ID token")
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, bad
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, bad
	}
	var c struct {
		Iss   string          `json:"iss"`
		Sub   string          `json:"sub"`
		Aud   json.RawMessage `json:"aud"`
		Azp   *string         `json:"azp"`
		Nonce string          `json:"nonce"`
		Exp   int64           `json:"exp"`
	}
	if json.Unmarshal(payload, &c) != nil || c.Iss != l.issuer || c.Sub == "" || c.Exp <= 0 {
		return time.Time{}, bad
	}
	if subtle.ConstantTimeCompare([]byte(c.Nonce), []byte(nonce)) != 1 || !oidcAudience(c.Aud, c.Azp, l.clientID) {
		return time.Time{}, bad
	}
	expires := time.Unix(c.Exp, 0)
	if !l.now().Before(expires) {
		return time.Time{}, errors.New("ID token has already expired")
	}
	return expires, nil
}

// oidcAudience applies OIDC Core 3.1.3.7 steps 3 to 5.
func oidcAudience(raw json.RawMessage, azp *string, clientID string) bool {
	var auds []string
	var one string
	if json.Unmarshal(raw, &one) == nil {
		auds = []string{one}
	} else if json.Unmarshal(raw, &auds) != nil {
		return false
	}
	found := false
	for _, a := range auds {
		found = found || a == clientID
	}
	if !found || azp != nil && *azp != clientID {
		return false
	}
	return len(auds) == 1 || azp != nil
}

// oauthErrorCode returns an RFC 6749 error code if s is one, and otherwise a
// fixed text, so provider text never reaches the terminal.
func oauthErrorCode(s string) string {
	switch s {
	case "invalid_request", "unauthorized_client", "access_denied",
		"unsupported_response_type", "invalid_scope", "server_error",
		"temporarily_unavailable", "invalid_client", "invalid_grant",
		"unsupported_grant_type", "interaction_required", "login_required",
		"account_selection_required", "consent_required", "invalid_request_uri",
		"invalid_request_object", "request_not_supported", "request_uri_not_supported",
		"registration_not_supported":
		return s
	default:
		return "unrecognized error"
	}
}

func oidcRandom() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", errors.New("generate sign-in secret")
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// oidcCallback receives the one authorization response at the exact
// redirect URL. Requests for another host or path, or with another state,
// are refused and do not end the sign-in. The response page redirects
// nowhere. Each request is admitted and counted until it returns; after
// close, requests are refused before they touch the sign-in.
type oidcCallback struct {
	host, state, issuer string
	once                sync.Once
	done                chan oidcResult // buffered; receives exactly one result
	hook                func(stage string)

	mu     sync.Mutex
	closed bool
	active int
	idle   chan struct{} // closed once closed is set and active is 0
}

type oidcResult struct {
	code string
	err  error
}

func newOIDCCallback(host, state, issuer string) *oidcCallback {
	return &oidcCallback{host: host, state: state, issuer: issuer, done: make(chan oidcResult, 1), idle: make(chan struct{})}
}

// at runs the test hook for stage: "admitted" and "delivered" in an
// admitted handler, "released" as it finishes.
func (c *oidcCallback) at(stage string) {
	if c.hook != nil {
		c.hook(stage)
	}
}

func (c *oidcCallback) admit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.active++
	return true
}

func (c *oidcCallback) release() {
	c.at("released")
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	if c.closed && c.active == 0 {
		close(c.idle)
	}
}

// close stops admission and waits until every admitted request has
// returned. No request is admitted after closed is set, so the count only
// falls while close waits.
func (c *oidcCallback) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		if c.active == 0 {
			close(c.idle)
		}
	}
	waiting := c.active > 0
	c.mu.Unlock()
	if waiting {
		c.at("join-wait")
	}
	<-c.idle
}

func (c *oidcCallback) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Cache-Control", "no-store")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
	if !c.admit() {
		http.Error(w, "sign-in closed", http.StatusServiceUnavailable)
		return
	}
	defer c.release()
	c.at("admitted")
	if r.Method != http.MethodGet || r.Host != c.host || r.URL.Path != "/callback" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	q := r.URL.Query()
	if len(q["state"]) != 1 || subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(c.state)) != 1 {
		http.Error(w, "invalid sign-in response", http.StatusBadRequest)
		return
	}
	// The state matches, so this response ends the sign-in either way.
	var result oidcResult
	code := q.Get("code")
	switch {
	case q.Has("error"):
		result.err = fmt.Errorf("sign-in refused: %s", oauthErrorCode(q.Get("error")))
	case q.Has("iss") && (len(q["iss"]) != 1 || q.Get("iss") != c.issuer):
		result.err = errors.New("sign-in response names another issuer")
	case len(q["code"]) != 1 || code == "" || len(code) > maxOIDCCodeBytes || !validToken(code):
		result.err = errors.New("invalid sign-in response")
	default:
		result.code = code
	}
	first := false
	c.once.Do(func() {
		c.done <- result
		first = true
	})
	if first {
		c.at("delivered")
	}
	switch {
	case !first:
		http.Error(w, "sign-in already completed", http.StatusConflict)
	case result.err != nil:
		http.Error(w, "Sign-in failed. Return to the terminal.", http.StatusBadRequest)
	default:
		h.Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "Signed in. You may close this window.\n")
	}
}

// openBrowser asks the desktop to open u. The caller bounds its lifetime.
func openBrowser(ctx context.Context, u string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.CommandContext(ctx, "open", u)
	case "windows":
		cmd = exec.CommandContext(ctx, "rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.CommandContext(ctx, "xdg-open", u)
	}
	return cmd.Run()
}
