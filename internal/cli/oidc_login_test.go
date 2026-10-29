package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"rsc.io/script/scripttest"
)

const (
	oidcAccessSecret  = "ACCESS-TOKEN-SECRET"
	oidcRefreshSecret = "REFRESH-TOKEN-SECRET"
	oidcDescription   = "PROVIDER-DESCRIPTION-SECRET"
	oidcLowerSecret   = "authorization_code_secret"
)

// fakeIssuer MODELS token claims with a fabricated signature. HTTPS and PKCE
// requests are real; signature acceptance is covered separately.
// fakeIssuer is an HTTPS authorization server for client "skygo". Its
// /authorize answers like a completed browser sign-in, redirecting to the
// requested redirect_uri; /token redeems that code once, with PKCE.
type fakeIssuer struct {
	*httptest.Server
	t *testing.T

	mu                       sync.Mutex
	challenge, nonce, state  string
	redirect, code           string
	idToken                  string
	tokenRequests, redirects int
	page                     string // the callback's response body
	pageStatus               int

	claims      map[string]any         // changes to ID token claims; nil deletes
	callback    func(q url.Values)     // edits the callback query
	tokenStatus int                    // if not 0, refuse /token with this status
	tokenBody   string                 // the refusal body
	response    func(m map[string]any) // edits a successful token response
}

func newFakeIssuer(t *testing.T) *fakeIssuer {
	t.Helper()
	i := &fakeIssuer{t: t}
	mux := http.NewServeMux()
	mux.HandleFunc("/authorize", i.authorize)
	mux.HandleFunc("/token", i.token)
	mux.HandleFunc("/elsewhere", func(w http.ResponseWriter, r *http.Request) {
		i.mu.Lock()
		i.redirects++
		i.mu.Unlock()
	})
	// Start after setting i.Server, which the handlers read.
	i.Server = httptest.NewUnstartedServer(mux)
	i.StartTLS()
	t.Cleanup(i.Close)
	return i
}

// result returns what the sign-in left at the issuer.
func (i *fakeIssuer) result() (idToken string, tokenRequests, redirects int, page string, status int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.idToken, i.tokenRequests, i.redirects, i.page, i.pageStatus
}

var loopbackRedirect = regexp.MustCompile(`^http://127\.0\.0\.1:[0-9]+/callback$`)

func (i *fakeIssuer) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for name, want := range map[string]string{"response_type": "code", "client_id": "skygo", "scope": "openid", "code_challenge_method": "S256", "tenant": "t1"} {
		if q.Get(name) != want {
			http.Error(w, "bad "+name, http.StatusBadRequest)
			return
		}
	}
	if !loopbackRedirect.MatchString(q.Get("redirect_uri")) || q.Get("state") == "" || q.Get("nonce") == "" || len(q.Get("code_challenge")) != 43 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	i.mu.Lock()
	i.challenge, i.nonce, i.state, i.redirect = q.Get("code_challenge"), q.Get("nonce"), q.Get("state"), q.Get("redirect_uri")
	i.code = "CODE-" + i.state[:8]
	cb := url.Values{"code": {i.code}, "state": {i.state}, "iss": {i.URL}}
	edit := i.callback
	i.mu.Unlock()
	if edit != nil {
		edit(cb)
	}
	http.Redirect(w, r, q.Get("redirect_uri")+"?"+cb.Encode(), http.StatusFound)
}

func (i *fakeIssuer) token(w http.ResponseWriter, r *http.Request) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.tokenRequests++
	if i.tokenStatus != 0 {
		if i.tokenStatus == http.StatusFound {
			w.Header().Set("Location", "/elsewhere")
		}
		w.WriteHeader(i.tokenStatus)
		io.WriteString(w, i.tokenBody)
		return
	}
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_id") != "skygo" ||
		i.code == "" || r.PostForm.Get("code") != i.code || r.PostForm.Get("redirect_uri") != i.redirect ||
		base64.RawURLEncoding.EncodeToString(sum[:]) != i.challenge {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}
	i.code = "" // single use
	claims := map[string]any{"iss": i.URL, "sub": "alice-subject", "aud": "skygo", "nonce": i.nonce, "iat": time.Now().Unix() - 1, "exp": time.Now().Unix() + 600, "email": "bob@example.com"}
	for k, v := range i.claims {
		if v == nil {
			delete(claims, k)
			continue
		}
		claims[k] = v
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		i.t.Error(err)
	}
	i.idToken = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2lnbmF0dXJl"
	m := map[string]any{"id_token": i.idToken, "token_type": "Bearer", "access_token": oidcAccessSecret, "refresh_token": oidcRefreshSecret, "expires_in": 3600}
	if i.response != nil {
		i.response(m)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(m)
}

// browser follows the sign-in like a user's browser: it requests the
// authorization URL, then the redirect it returns.
func (i *fakeIssuer) browser(ctx context.Context, u string) error {
	c := &http.Client{Transport: i.Client().Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		return fmt.Errorf("authorize status %d", resp.StatusCode)
	}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, resp.Header.Get("Location"), nil)
	if err != nil {
		return err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	i.mu.Lock()
	i.page, i.pageStatus = string(body), resp.StatusCode
	i.mu.Unlock()
	return err
}

// secrets returns the values that must never be printed or stored except
// the ID token in its output file.
func (i *fakeIssuer) secrets() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := []string{oidcAccessSecret, oidcRefreshSecret, oidcDescription, oidcLowerSecret}
	for _, s := range []string{i.state, i.nonce, i.challenge, i.idToken, "CODE-"} {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// requireOutputPlatform skips where token output is unsupported.
func requireOutputPlatform(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("OIDC token output requires Linux or Darwin")
	}
}

func newTestLogin(t *testing.T, i *fakeIssuer) (*oidcLogin, *bytes.Buffer) {
	t.Helper()
	requireOutputPlatform(t)
	// The output directory must be private and reached without symlinks.
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: i.Certificate().Raw}), 0644); err != nil {
		t.Fatal(err)
	}
	notice := new(bytes.Buffer)
	l := &oidcLogin{issuer: i.URL, clientID: "skygo", caFile: ca, output: filepath.Join(dir, "id.token"), timeout: 10 * time.Second, browser: i.browser, notice: notice, now: time.Now}
	if err := l.parse(i.URL+"/authorize?tenant=t1", i.URL+"/token"); err != nil {
		t.Fatal(err)
	}
	return l, notice
}

func requireNoSecrets(t *testing.T, text string, secrets []string) {
	t.Helper()
	for _, s := range secrets {
		if strings.Contains(text, s) {
			t.Fatalf("output %q contains a secret", text)
		}
	}
}

func TestOIDCLogin(t *testing.T) {
	i := newFakeIssuer(t)
	l, notice := newTestLogin(t, i)
	expires, err := l.run(context.Background())
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	if d := time.Until(expires); d < 9*time.Minute || d > 11*time.Minute {
		t.Fatalf("expires in %v, want about 10m", d)
	}
	info, err := os.Lstat(l.output)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("token output = %v, %v; want a 0600 regular file", info, err)
	}
	first, requests, _, page, status := i.result()
	data, err := os.ReadFile(l.output)
	if err != nil || string(data) != first+"\n" {
		t.Fatalf("token output does not hold exactly the ID token: %v", err)
	}
	if token, err := readTokenFile(l.output); err != nil || token != first {
		t.Fatalf("readTokenFile(output) = %v", err)
	}
	if requests != 1 || status != http.StatusOK || !strings.HasPrefix(page, "Signed in.") {
		t.Fatalf("token requests %d, callback page %d %q", requests, status, page)
	}
	requireNoSecrets(t, notice.String(), i.secrets())
	entries, err := os.ReadDir(filepath.Dir(l.output))
	if err != nil || len(entries) != 2 {
		t.Fatalf("output directory has %d entries, want ca.pem and id.token only (%v)", len(entries), err)
	}

	// A second sign-in refuses the existing output before opening a browser.
	opened := false
	l.browser = func(ctx context.Context, u string) error { opened = true; return i.browser(ctx, u) }
	if _, err := l.run(context.Background()); err == nil || !strings.Contains(err.Error(), "already exists") || opened {
		t.Fatalf("second login = %v (browser opened %v), want refusal before the browser", err, opened)
	}
	l.replace = true
	if _, err := l.run(context.Background()); err != nil {
		t.Fatalf("login with replace: %v", err)
	}
	second, _, _, _, _ := i.result()
	data, err = os.ReadFile(l.output)
	if err != nil || string(data) != second+"\n" || second == first {
		t.Fatalf("replaced output does not hold the new ID token: %v", err)
	}
}

func TestOIDCLoginRefused(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(i *fakeIssuer, l *oidcLogin)
		want  string
	}{
		{"wrong nonce", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"nonce": "other"} }, "invalid ID token"},
		{"no nonce", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"nonce": nil} }, "invalid ID token"},
		{"wrong issuer claim", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"iss": "https://other.example"} }, "invalid ID token"},
		{"wrong audience", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"aud": "other"} }, "invalid ID token"},
		{"audiences without azp", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"aud": []string{"skygo", "other"}} }, "invalid ID token"},
		{"wrong azp", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"azp": "other"} }, "invalid ID token"},
		{"no subject", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"sub": nil} }, "invalid ID token"},
		{"expired", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"exp": time.Now().Unix() - 10} }, "already expired"},
		{"oversized", func(i *fakeIssuer, l *oidcLogin) { i.claims = map[string]any{"pad": strings.Repeat("a", 4096)} }, "4096-byte"},
		{"not bearer", func(i *fakeIssuer, l *oidcLogin) { i.response = func(m map[string]any) { m["token_type"] = "mac" } }, "invalid token response"},
		{"no id token", func(i *fakeIssuer, l *oidcLogin) { i.response = func(m map[string]any) { delete(m, "id_token") } }, "invalid token response"},
		{"provider error", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) {
				q.Del("code")
				q.Set("error", "access_denied")
				q.Set("error_description", oidcDescription)
			}
		}, "sign-in refused: access_denied"},
		{"provider error text", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) { q.Set("error", oidcDescription) }
		}, "sign-in refused: unrecognized error"},
		{"lowercase callback secret", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) { q.Set("error", oidcLowerSecret) }
		}, "sign-in refused: unrecognized error"},
		{"lowercase token secret", func(i *fakeIssuer, l *oidcLogin) {
			i.tokenStatus, i.tokenBody = http.StatusBadRequest, `{"error":"`+oidcLowerSecret+`"}`
		}, "token request refused: unrecognized error"},
		{"callback issuer", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) { q.Set("iss", "https://other.example") }
		}, "another issuer"},
		{"two codes", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) { q.Add("code", "CODE-second") }
		}, "invalid sign-in response"},
		{"forged state", func(i *fakeIssuer, l *oidcLogin) {
			i.callback = func(q url.Values) { q.Set("state", "forged") }
			l.timeout = time.Second
		}, "did not complete before the login timeout"},
		{"token refusal", func(i *fakeIssuer, l *oidcLogin) {
			i.tokenStatus, i.tokenBody = http.StatusBadRequest, `{"error":"invalid_grant","error_description":"`+oidcDescription+`"}`
		}, "token request refused: invalid_grant"},
		{"token redirect", func(i *fakeIssuer, l *oidcLogin) { i.tokenStatus = http.StatusFound }, "token request refused: unrecognized error"},
		{"untrusted certificate", func(i *fakeIssuer, l *oidcLogin) { l.caFile = "" }, "token request failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			i := newFakeIssuer(t)
			l, notice := newTestLogin(t, i)
			i.mu.Lock()
			tt.setup(i, l)
			i.mu.Unlock()
			_, err := l.run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("login = %v, want %q", err, tt.want)
			}
			requireNoSecrets(t, err.Error()+notice.String(), i.secrets())
			if _, err := os.Lstat(l.output); !os.IsNotExist(err) {
				t.Fatalf("refused login wrote its output: %v", err)
			}
			if _, _, redirects, _, _ := i.result(); redirects != 0 {
				t.Fatal("token request followed a redirect")
			}
		})
	}
}

func TestOIDCCallback(t *testing.T) {
	c := newOIDCCallback("127.0.0.1:5555", "STATE", "https://issuer.example")
	for _, tt := range []struct {
		name, method, target string
		want                 int
	}{
		{"post", http.MethodPost, "http://127.0.0.1:5555/callback?state=STATE&code=c", http.StatusNotFound},
		{"other host", http.MethodGet, "http://localhost:5555/callback?state=STATE&code=c", http.StatusNotFound},
		{"other path", http.MethodGet, "http://127.0.0.1:5555/?state=STATE&code=c", http.StatusNotFound},
		{"no state", http.MethodGet, "http://127.0.0.1:5555/callback?code=c", http.StatusBadRequest},
		{"wrong state", http.MethodGet, "http://127.0.0.1:5555/callback?state=STATF&code=c", http.StatusBadRequest},
		{"two states", http.MethodGet, "http://127.0.0.1:5555/callback?state=STATE&state=STATE&code=c", http.StatusBadRequest},
		{"accepted", http.MethodGet, "http://127.0.0.1:5555/callback?state=STATE&code=c&iss=https%3A%2F%2Fissuer.example", http.StatusOK},
		{"replayed", http.MethodGet, "http://127.0.0.1:5555/callback?state=STATE&code=d", http.StatusConflict},
	} {
		w := httptest.NewRecorder()
		c.ServeHTTP(w, httptest.NewRequest(tt.method, tt.target, nil))
		if w.Code != tt.want {
			t.Errorf("%s: status %d, want %d", tt.name, w.Code, tt.want)
		}
		if w.Header().Get("Location") != "" || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Referrer-Policy") != "no-referrer" {
			t.Errorf("%s: unsafe response headers %v", tt.name, w.Header())
		}
	}
	select {
	case r := <-c.done:
		if r.err != nil || r.code != "c" {
			t.Fatalf("result = %q, %v; want code c", r.code, r.err)
		}
	default:
		t.Fatal("no result")
	}
	select {
	case r := <-c.done:
		t.Fatalf("second result %q, %v", r.code, r.err)
	default:
	}
}

func TestOIDCLoginParse(t *testing.T) {
	const idp = "https://idp.example"
	for _, tt := range []struct {
		name                      string
		issuer, client, auth, tok string
		port                      int
		timeout                   time.Duration
		want                      string
	}{
		{"valid", idp, "skygo", idp + "/auth?tenant=t", idp + "/token", 0, time.Minute, ""},
		{"fixed port", idp + "/realms/a", "skygo", idp + "/auth", idp + "/token", 8400, time.Minute, ""},
		{"http issuer", "http://idp.example", "skygo", idp + "/auth", idp + "/token", 0, time.Minute, "issuer must be"},
		{"issuer query", idp + "?x=1", "skygo", idp + "/auth", idp + "/token", 0, time.Minute, "issuer must be"},
		{"issuer userinfo", "https://u@idp.example", "skygo", idp + "/auth", idp + "/token", 0, time.Minute, "issuer must be"},
		{"empty client", idp, "", idp + "/auth", idp + "/token", 0, time.Minute, "client ID"},
		{"client space", idp, "a b", idp + "/auth", idp + "/token", 0, time.Minute, "client ID"},
		{"http authorize", idp, "skygo", "http://idp.example/auth", idp + "/token", 0, time.Minute, "authorization endpoint"},
		{"authorize fragment", idp, "skygo", idp + "/auth#x", idp + "/token", 0, time.Minute, "authorization endpoint"},
		{"authorize state", idp, "skygo", idp + "/auth?state=x", idp + "/token", 0, time.Minute, "must not set state"},
		{"authorize redirect", idp, "skygo", idp + "/auth?redirect_uri=https://evil.example", idp + "/token", 0, time.Minute, "must not set redirect_uri"},
		{"http token", idp, "skygo", idp + "/auth", "http://idp.example/token", 0, time.Minute, "token endpoint"},
		{"bad port", idp, "skygo", idp + "/auth", idp + "/token", 70000, time.Minute, "redirect port"},
		{"zero timeout", idp, "skygo", idp + "/auth", idp + "/token", 0, 0, "login timeout"},
		{"long timeout", idp, "skygo", idp + "/auth", idp + "/token", 0, 16 * time.Minute, "login timeout"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l := &oidcLogin{issuer: tt.issuer, clientID: tt.client, output: "id.token", port: tt.port, timeout: tt.timeout}
			err := l.parse(tt.auth, tt.tok)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parse = %v, want %q", err, tt.want)
			}
		})
	}
}

// heldHook holds an admitted callback at one stage until free is called,
// and records when callbacks are admitted and return and when their joiner
// returns.
type heldHook struct {
	stage    string
	reached  chan struct{}
	release  chan struct{}
	reach    sync.Once
	freeOnce sync.Once

	mu     sync.Mutex
	events []string
}

func newHeldHook(t *testing.T, stage string) *heldHook {
	h := &heldHook{stage: stage, reached: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(h.free)
	return h
}

func (h *heldHook) hook(stage string) {
	switch stage {
	case "admitted":
		h.log("callback admitted")
	case "released":
		h.log("callback returned")
	}
	if stage == h.stage {
		h.reach.Do(func() { close(h.reached) })
		<-h.release
	}
}

func (h *heldHook) free() { h.freeOnce.Do(func() { close(h.release) }) }

func (h *heldHook) log(event string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, event)
}

func (h *heldHook) order() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.events...)
}

// joinHook reports that this callback is joining, without package hooks.
func joinHook(hook func(string)) (func(string), <-chan struct{}) {
	waiting := make(chan struct{})
	var once sync.Once
	return func(stage string) {
		if stage == "join-wait" {
			once.Do(func() { close(waiting) })
			return
		}
		hook(stage)
	}, waiting
}

// requireJoinWait requires that the joiner block on the held callback: the
// join hook fires and done stays open. A joiner that skips the join closes
// done without firing the hook. The deadline only bounds a failure.
func requireJoinWait(t *testing.T, waiting, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-waiting:
	case <-done:
		t.Fatalf("%s returned without waiting for the held callback", what)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s neither waited nor returned within 10s", what)
	}
	select {
	case <-done:
		t.Fatalf("%s returned while an admitted callback was held", what)
	default:
	}
}

// await bounds a wait for an event; the bound is a failure, never a proof.
func await(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not happen within 10s", what)
	}
}

// TestOIDCListenerJoinsAcceptedCallback holds an accepted callback after it
// has delivered its code. stop must wait for that callback to return, and
// afterwards admit no other.
func TestOIDCListenerJoinsAcceptedCallback(t *testing.T) {
	h := newHeldHook(t, "delivered")
	hook, waiting := joinHook(h.hook)
	s, err := listenOIDC(0, "STATE", "https://issuer.example", hook)
	if err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan struct{})
	t.Cleanup(func() {
		h.free()
		s.stop()
		await(t, clientDone, "the original callback client")
	})
	go func() {
		defer close(clientDone)
		if resp, err := http.Get("http://" + s.cb.host + "/callback?state=STATE&code=c"); err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case r := <-s.cb.done:
		if r.err != nil || r.code != "c" {
			t.Fatalf("result = %q, %v", r.code, r.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no callback result within 10s")
	}
	await(t, h.reached, "the held callback")
	stopped := make(chan struct{})
	go func() {
		s.stop()
		h.log("stop returned")
		close(stopped)
	}()
	requireJoinWait(t, waiting, stopped, "stop")
	h.free()
	await(t, stopped, "stop after release")
	want := []string{"callback admitted", "callback returned", "stop returned"}
	if got := h.order(); !slices.Equal(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}

	s.stop() // idempotent
	if resp, err := http.Get("http://" + s.cb.host + "/callback?state=STATE&code=c"); err == nil {
		resp.Body.Close()
		t.Fatal("listener still accepts after stop")
	}
	// A request the server had already read when it closed reaches the
	// handler afterwards. It must be refused before admission.
	w := httptest.NewRecorder()
	s.cb.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://"+s.cb.host+"/callback?state=STATE&code=d", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("callback after stop = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	if got := h.order(); !slices.Equal(got, want) {
		t.Fatalf("callback after stop ran the handler: events = %q", got)
	}
	select {
	case r := <-s.cb.done:
		t.Fatalf("callback after stop delivered %q, %v", r.code, r.err)
	default:
	}
}

// TestOIDCLoginCancelJoinsCallback cancels a sign-in while a callback is
// held before delivering anything. run must wait for that callback to
// return, and must write nothing.
func TestOIDCLoginCancelJoinsCallback(t *testing.T) {
	i := newFakeIssuer(t)
	l, _ := newTestLogin(t, i)
	h := newHeldHook(t, "admitted")
	hook, waiting := joinHook(h.hook)
	l.callbackHook = hook
	browserDone := make(chan struct{})
	browserStarted := make(chan struct{})
	l.browser = func(_ context.Context, u string) error {
		// The browser outlives the opener, as a real one does.
		close(browserStarted)
		go func() { defer close(browserDone); _ = i.browser(context.Background(), u) }()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runErr error
	returned := make(chan struct{})
	go func() {
		_, runErr = l.run(ctx)
		h.log("run returned")
		close(returned)
	}()
	t.Cleanup(func() {
		// run must be finished before the join hook is removed.
		h.free()
		await(t, returned, "run after cleanup")
		select {
		case <-browserStarted:
			await(t, browserDone, "original modeled browser")
		default:
		}
	})
	await(t, h.reached, "the held callback")
	cancel()
	requireJoinWait(t, waiting, returned, "run")
	h.free()
	await(t, returned, "run after release")
	if runErr == nil || !strings.Contains(runErr.Error(), "did not complete") {
		t.Fatalf("run = %v, want the sign-in to end incomplete", runErr)
	}
	want := []string{"callback admitted", "callback returned", "run returned"}
	if got := h.order(); !slices.Equal(got, want) {
		t.Fatalf("events = %q, want %q", got, want)
	}
	if _, err := os.Lstat(l.output); !os.IsNotExist(err) {
		t.Fatalf("canceled sign-in wrote its output: %v", err)
	}
}

// TestOIDCLoginScripts runs the oidc command in process against script
// fixtures that fail before any network use.
func TestOIDCLoginScripts(t *testing.T) {
	e := scriptEngine(t)
	scripttest.Test(t, context.Background(), e, nil, "testdata/oidc/*.txt")
}

// oidcCloseBody reports a MODELED failure after the actual HTTPS body Close.
// It is not a native TLS/kernel Close-failure witness.
type oidcCloseBody struct {
	io.ReadCloser
	cause error
	calls int
}

func (b *oidcCloseBody) Close() error {
	b.calls++
	return errors.Join(b.ReadCloser.Close(), b.cause)
}

type oidcCloseTransport struct {
	base     http.RoundTripper
	cause    error
	body     *oidcCloseBody
	response *http.Response
}

func (t *oidcCloseTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		return resp, err
	}
	t.body = &oidcCloseBody{ReadCloser: resp.Body, cause: t.cause}
	resp.Body = t.body
	t.response = resp
	return resp, nil
}

func TestOIDCTokenResponseFirstClose(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		t.Run(fmt.Sprint(truncated), func(t *testing.T) {
			const secret = "PROVIDER-CLOSE-SECRET"
			cause := errors.New(secret)
			issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if truncated {
					w.Header().Set("Content-Length", "1000")
				}
				io.WriteString(w, `{"id_token":"TOKEN-SECRET","token_type":"Bearer"}`)
			}))
			t.Cleanup(issuer.Close)
			u, err := url.Parse(issuer.URL)
			if err != nil {
				t.Fatal(err)
			}
			transport := &oidcCloseTransport{base: issuer.Client().Transport, cause: cause}
			l := &oidcLogin{token: u, clientID: "skygo"}
			token, err := l.exchange(context.Background(), &http.Client{Transport: transport}, "CODE-SECRET", "http://127.0.0.1/callback", "VERIFIER-SECRET")
			if token != "" || !errors.Is(err, cause) {
				t.Fatalf("token present=%v, err=%v", token != "", err)
			}
			if truncated && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("lost actual read failure: %v", err)
			}
			var held *oidcFailure
			if !errors.As(err, &held) || held.response != transport.response || held.response.Body != transport.body || transport.body.calls != 1 {
				t.Fatal("first Close failure lost the original response/body")
			}
			requireNoSecrets(t, err.Error(), []string{secret, "TOKEN-SECRET", "CODE-SECRET", "VERIFIER-SECRET"})
		})
	}
}

type oidcFaultListener struct {
	net.Listener
	cause error
	calls int
}

func (l *oidcFaultListener) Close() error {
	l.calls++
	return errors.Join(l.Listener.Close(), l.cause)
}
func TestOIDCListenerFirstClose(t *testing.T) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("MODELED listener Close failure")
	original := &oidcFaultListener{Listener: ln, cause: cause}
	s := serveOIDC(original, "state", "https://issuer.example", nil)
	// stop can race with Serve's registration; both paths must retain Close.
	first := s.stop()
	if !errors.Is(first, cause) {
		t.Fatalf("stop=%v", first)
	}
	if second := s.stop(); second != first || original.calls != 1 {
		t.Fatal("stop retried or lost the original Close result")
	}
	var held *oidcFailure
	if !errors.As(first, &held) || held.listener != s || s.ln.Listener != original {
		t.Fatal("listener owner was lost")
	}
}
