package cli

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	skyv1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	"github.com/tmc/skygo/internal/control"
	"github.com/tmc/skygo/internal/local"
	"github.com/tmc/skygo/internal/rpc"
	"github.com/tmc/skygo/internal/store"
)

// signedLoginIssuer serves actual TLS and RSA-signed ID tokens. The browser
// interaction is MODELED by an HTTP redirect follower; no desktop browser or
// external identity-provider account participates.
type signedLoginIssuer struct {
	server *httptest.Server
	key    *rsa.PrivateKey

	mu                         sync.Mutex
	subject, code, nonce       string
	challenge, redirect, state string
	authorizations, exchanges  int
}

func (i *signedLoginIssuer) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirect, err := url.Parse(q.Get("redirect_uri"))
	if r.Method != http.MethodGet || err != nil || redirect.Scheme != "http" || redirect.Hostname() != "127.0.0.1" || redirect.Port() == "" || redirect.Path != "/callback" || redirect.RawQuery != "" || redirect.User != nil || redirect.Fragment != "" ||
		q.Get("response_type") != "code" || q.Get("client_id") != "skygo-login" || q.Get("scope") != "openid" || q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) != 43 || q.Get("state") == "" || q.Get("nonce") == "" {
		http.Error(w, "invalid authorization request", http.StatusBadRequest)
		return
	}
	i.mu.Lock()
	i.authorizations++
	i.challenge, i.redirect, i.state, i.nonce = q.Get("code_challenge"), redirect.String(), q.Get("state"), q.Get("nonce")
	i.code = "authorization-" + i.nonce
	callback := url.Values{"code": {i.code}, "state": {i.state}, "iss": {i.server.URL}}
	i.mu.Unlock()
	http.Redirect(w, r, redirect.String()+"?"+callback.Encode(), http.StatusFound)
}

func (i *signedLoginIssuer) exchange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		http.Error(w, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.exchanges++
	challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if i.code == "" || r.PostForm.Get("code") != i.code || r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_id") != "skygo-login" || r.PostForm.Get("redirect_uri") != i.redirect || base64.RawURLEncoding.EncodeToString(challenge[:]) != i.challenge {
		http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
		return
	}
	i.code = "" // The actual exchange consumes the authorization code once.
	now := time.Now().Unix()
	payload, err := json.Marshal(map[string]any{
		"iss": i.server.URL, "sub": i.subject, "aud": "skygo-login",
		"nonce": i.nonce, "iat": now - 1, "exp": now + 600,
	})
	if err != nil {
		http.Error(w, "cannot encode ID token", http.StatusInternalServerError)
		return
	}
	input := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"login-key","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload)
	digest := sha256.Sum256([]byte(input))
	signature, err := rsa.SignPKCS1v15(rand.Reader, i.key, crypto.SHA256, digest[:])
	if err != nil {
		http.Error(w, "cannot sign ID token", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"id_token": input + "." + base64.RawURLEncoding.EncodeToString(signature), "token_type": "Bearer",
	})
}

func (i *signedLoginIssuer) browser(ctx context.Context, authorization string) error {
	client := &http.Client{Transport: i.server.Client().Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authorization, nil)
	if err != nil {
		return err
	}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if response.StatusCode != http.StatusFound {
		return errors.New("authorization did not redirect to the callback")
	}
	callbackClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer callbackClient.CloseIdleConnections()
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, response.Header.Get("Location"), nil)
	if err != nil {
		return err
	}
	response, err = callbackClient.Do(req)
	if err != nil {
		return err
	}
	_, readErr = io.Copy(io.Discard, response.Body)
	closeErr = response.Body.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("callback did not accept the authorization response")
	}
	return nil
}

// signedLoginFixture registers concrete owners before any subsequent Fatal.
// Its root has no automatic TempDir cleanup: uncertain shutdown keeps it intact.
type signedLoginFixture struct {
	root, authPath, oidcPath, caPath string
	issuer                           *signedLoginIssuer
	state                            *store.Store
	engine                           *auth.Engine
	engineCancel                     context.CancelFunc
	engineDone                       chan struct{}
	local                            *local.Backend
	control                          *control.Control
	handler                          *rpc.HTTPHandler
	api                              *httptest.Server
	held                             bool
	holdErr                          error // keeps a returned uncertain login owner reachable
}

func (f *signedLoginFixture) close(t *testing.T) {
	t.Helper()
	if f.api != nil {
		f.api.Client().CloseIdleConnections()
		f.api.Close()
		f.api = nil
	}
	if f.handler != nil {
		f.handler.WaitArtifactHTTP()
	}
	if f.control != nil {
		// No Run was started; this test accepts metadata but launches no job.
		if err := f.control.BeginShutdown(); err != nil {
			f.held = true
			t.Error(err)
		}
	}
	if f.engineCancel != nil {
		f.engineCancel()
		<-f.engineDone
		if err := f.engine.Err(); err != nil {
			f.held = true
			t.Error(err)
		}
	}
	if f.local != nil {
		if err := f.local.Close(); err != nil {
			f.held = true
			t.Error(err)
		}
	}
	if f.state != nil && !f.held {
		if err := f.state.Close(); err != nil {
			f.held = true
			t.Error(err)
		}
	}
	if f.issuer != nil && f.issuer.server != nil {
		f.issuer.server.Client().CloseIdleConnections()
		f.issuer.server.Close()
	}
	if f.held {
		t.Logf("retained signed-login fixture after uncertain shutdown: %s", f.root)
		return
	}
	if err := os.RemoveAll(f.root); err != nil {
		t.Error(err)
	}
}

func newSignedLoginFixture(t *testing.T) *signedLoginFixture {
	t.Helper()
	f := new(signedLoginFixture)
	var err error
	f.root, err = os.MkdirTemp("", "skygo-signed-login-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.close(t) })
	resolved, err := filepath.EvalSymlinks(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.root = resolved
	if err := os.Chmod(f.root, 0700); err != nil {
		t.Fatal(err)
	}
	f.authPath, f.oidcPath, f.caPath = filepath.Join(f.root, "auth.json"), filepath.Join(f.root, "oidc.json"), filepath.Join(f.root, "issuer.pem")
	f.state, err = store.Open(filepath.Join(f.root, "state"))
	if err != nil {
		f.held = errors.Is(err, store.ErrCloseUncertain)
		f.holdErr = err
		t.Fatal(err)
	}
	if _, err := auth.Initialize(f.authPath, filepath.Join(f.root, "admin.token"), f.state.Owner()); err != nil {
		f.held = errors.Is(err, auth.ErrCloseUncertain)
		f.holdErr = err
		t.Fatal(err)
	}
	if err := auth.AddPrincipal(f.authPath, "alice", auth.Operator); err != nil {
		f.held = errors.Is(err, auth.ErrCloseUncertain)
		f.holdErr = err
		t.Fatal(err)
	}
	if err := f.state.ActivateScoped(); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.issuer = &signedLoginIssuer{key: key, subject: "alice-subject"}
	mux := http.NewServeMux()
	mux.HandleFunc("/authorize", f.issuer.authorize)
	mux.HandleFunc("/token", f.issuer.exchange)
	f.issuer.server = httptest.NewUnstartedServer(mux)
	f.issuer.server.StartTLS()
	if err := os.WriteFile(f.caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.issuer.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	keys, err := json.Marshal(map[string]any{"keys": []any{map[string]string{
		"kty": "RSA", "kid": "login-key", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": "AQAB",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.root, "keys.json"), keys, 0600); err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(map[string]any{
		"version": 1, "installation": f.state.Owner(), "generation": 1,
		"issuer": f.issuer.server.URL, "audience": "skygo-login", "jwks_file": "keys.json",
		"max_token_lifetime": "1h", "clock_skew": "0s",
		"identities": []any{map[string]string{"subject": "alice-subject", "principal": "alice"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.oidcPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	f.engine, err = auth.NewFileOIDC(f.authPath, f.state.Owner(), f.oidcPath, "")
	if err != nil {
		f.held = errors.Is(err, auth.ErrCloseUncertain)
		f.holdErr = err
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.engineCancel, f.engineDone = cancel, make(chan struct{})
	go func() { defer close(f.engineDone); f.engine.Run(ctx) }()
	f.local, err = local.New(filepath.Join(f.root, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	binding, err := backend.LocalBinding(f.state.Owner())
	if err != nil {
		t.Fatal(err)
	}
	f.control, err = control.New(f.state, []backend.Target{{Name: "local", Kind: "local", Binding: binding, Backend: f.local}}, f.engine)
	if err != nil {
		t.Fatal(err)
	}
	f.handler = rpc.NewHandler(f.control, f.engine)
	f.api = httptest.NewServer(f.handler)
	return f
}

func (f *signedLoginFixture) login(t *testing.T, filename string) (string, string) {
	t.Helper()
	var notice bytes.Buffer
	path := filepath.Join(f.root, filename)
	l := &oidcLogin{issuer: f.issuer.server.URL, clientID: "skygo-login", caFile: f.caPath, output: path, timeout: 10 * time.Second, browser: f.issuer.browser, notice: &notice, now: time.Now}
	if err := l.parse(f.issuer.server.URL+"/authorize", f.issuer.server.URL+"/token"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.run(context.Background()); err != nil {
		f.held, f.holdErr = true, err
		t.Fatal(err)
	}
	token, err := readTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(notice.String(), token) {
		t.Fatal("sign-in notice exposed the ID token")
	}
	return path, token
}

// TestOIDCSignedLoginScopedRPC checks real token signatures and scoped queued
// metadata through the mounted RPC and CLI. No Control.Run or workload starts.
func TestOIDCSignedLoginScopedRPC(t *testing.T) {
	requireOutputPlatform(t)
	t.Setenv("SKYGO_TOKEN", "")
	f := newSignedLoginFixture(t)
	path, token := f.login(t, "alice.token")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		changed := f.engine.Changed()
		actor, err := f.engine.Authenticate("Bearer " + token)
		if err == nil {
			if actor.Principal() != "alice" || actor.Role() != auth.Operator {
				t.Fatal("signed ID token did not map to the configured scoped principal")
			}
			break
		}
		if !errors.Is(err, auth.ErrUnavailable) {
			t.Fatalf("authenticate signed ID token: %v", err)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("OIDC refresh did not publish its original snapshot")
		}
	}
	client := skyv1connect.NewControlServiceClient(f.api.Client(), f.api.URL)
	request := connect.NewRequest(&skyv1.SubmitJobRequest{RequestId: "signed-login-job", Task: &skyv1.Task{Argv: []string{"/bin/true"}}})
	request.Header().Set("Authorization", "Bearer "+token)
	accepted, err := client.SubmitJob(ctx, request)
	if err != nil {
		t.Fatalf("accept job with signed login token: %v", err)
	}
	if accepted.Msg.GetJob().GetId() == "" || accepted.Msg.GetJob().GetState() != skyv1.JobState_JOB_STATE_QUEUED {
		t.Fatal("accepted job has no durable queued identity")
	}
	out, diag, err := execute(ctx, "--endpoint", f.api.URL, "--token-file", path, "--json", "jobs", "list")
	if err != nil || !strings.Contains(out, accepted.Msg.GetJob().GetId()) {
		t.Fatalf("mounted CLI could not read the accepted job: %v", err)
	}
	if strings.Contains(out+diag, token) {
		t.Fatal("mounted CLI exposed the saved credential")
	}
	parts := strings.Split(token, ".")
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(signature) == 0 {
		t.Fatal("signed fixture has no decodable signature")
	}
	signature[0] ^= 1
	tampered := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(signature)
	tamperedPath := filepath.Join(f.root, "tampered.token")
	if err := os.WriteFile(tamperedPath, []byte(tampered+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f.issuer.mu.Lock()
	f.issuer.subject = "not-mapped"
	f.issuer.mu.Unlock()
	unmappedPath, unmapped := f.login(t, "unmapped.token")
	for _, tt := range []struct{ name, token, path string }{{"tampered signature", tampered, tamperedPath}, {"unmapped subject", unmapped, unmappedPath}} {
		t.Run(tt.name, func(t *testing.T) {
			req := connect.NewRequest(&skyv1.ListJobsRequest{})
			req.Header().Set("Authorization", "Bearer "+tt.token)
			if _, err := client.ListJobs(ctx, req); connect.CodeOf(err) != connect.CodeUnauthenticated {
				t.Fatalf("scoped RPC = %v, want unauthenticated", err)
			}
			out, diag, err := execute(ctx, "--endpoint", f.api.URL, "--token-file", tt.path, "--json", "jobs", "list")
			if err == nil {
				t.Fatal("mounted CLI accepted a refused token")
			}
			if strings.Contains(out+diag+err.Error(), tt.token) {
				t.Fatal("refusal exposed the credential")
			}
		})
	}
	f.issuer.mu.Lock()
	authorizations, exchanges, code := f.issuer.authorizations, f.issuer.exchanges, f.issuer.code
	f.issuer.mu.Unlock()
	if authorizations != 2 || exchanges != 2 || code != "" {
		t.Fatalf("issuer accepted %d authorizations/%d exchanges or kept an unconsumed code", authorizations, exchanges)
	}
}
