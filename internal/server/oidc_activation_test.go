package server_test

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/tmc/skygo/gen/skygo/v1"
	"github.com/tmc/skygo/gen/skygo/v1/skyv1connect"
	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/server"
	"github.com/tmc/skygo/internal/store"
)

// oidcServerFixture owns its private directory, actual server and issuer.
// A failed join retains the same frame, including its cleanup authority.
type oidcServerFixture struct {
	root, data, credentials, oidc, keys, owner, admin string
	key                                               *rsa.PrivateKey
	api                                               skyv1connect.ControlServiceClient
	cancel                                            context.CancelFunc
	done                                              chan error
	issuer                                            *httptest.Server
	mu                                                sync.Mutex
	jwks                                              []byte
	requests                                          int
	paths                                             []string
	redirect                                          bool
	startupStore                                      *store.Store
	startupError                                      error
	runError                                          error
}

var oidcHeldFixtures struct {
	sync.Mutex
	frames []*oidcServerFixture
}

func (f *oidcServerFixture) close(t *testing.T) {
	t.Helper()
	if f.startupError != nil {
		oidcHeldFixtures.Lock()
		oidcHeldFixtures.frames = append(oidcHeldFixtures.frames, f)
		oidcHeldFixtures.Unlock()
		t.Errorf("OIDC startup owner retained: %v", f.startupError)
		return
	}
	if f.cancel != nil {
		f.cancel()
	}
	if f.done != nil {
		select {
		case err := <-f.done:
			f.runError = err // Retain RunError and its original serverRun frame.
			if err != nil {
				oidcHeldFixtures.Lock()
				oidcHeldFixtures.frames = append(oidcHeldFixtures.frames, f)
				oidcHeldFixtures.Unlock()
				t.Errorf("OIDC server retained after shutdown error: %v", err)
				return
			}
		case <-time.After(15 * time.Second):
			oidcHeldFixtures.Lock()
			oidcHeldFixtures.frames = append(oidcHeldFixtures.frames, f)
			oidcHeldFixtures.Unlock()
			t.Error("OIDC server original did not join; retaining fixture")
			return
		}
	}
	if f.issuer != nil {
		f.issuer.Close()
	}
	if err := os.RemoveAll(f.root); err != nil {
		t.Error(err)
	}
}

func oidcReplace(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	temp := path + ".new"
	if err := os.WriteFile(temp, data, mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp, path); err != nil {
		t.Fatal(err)
	}
}

func (f *oidcServerFixture) writeKeys(t *testing.T, kid string, key *rsa.PublicKey) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": kid, "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": "AQAB"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.jwks = data
	f.mu.Unlock()
	oidcReplace(t, f.keys, data, 0644)
}

func (f *oidcServerFixture) writeMapping(t *testing.T, generation int, remote bool, users ...string) {
	t.Helper()
	identities := []map[string]string{}
	for _, user := range users {
		identities = append(identities, map[string]string{"subject": user + "-subject", "principal": user})
	}
	config := map[string]any{"version": 1, "installation": f.owner, "generation": generation, "issuer": "https://issuer.example", "audience": "skygo", "clock_skew": "0s", "identities": identities}
	if remote {
		config["jwks_uri"] = f.issuer.URL
		config["ca_file"] = "issuer.pem"
	} else {
		config["jwks_file"] = "keys.json"
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	oidcReplace(t, f.oidc, data, 0600)
}

func (f *oidcServerFixture) token(t *testing.T, key *rsa.PrivateKey, kid, subject string, life time.Duration) string {
	t.Helper()
	now := time.Now().Unix()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"` + kid + `","typ":"JWT"}`))
	data, err := json.Marshal(map[string]any{"iss": "https://issuer.example", "aud": "skygo", "sub": subject, "iat": now - 1, "exp": now + int64(life/time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	input := header + "." + base64.RawURLEncoding.EncodeToString(data)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func (f *oidcServerFixture) read(token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := connect.NewRequest(&v1.GetServerInfoRequest{})
	request.Header().Set("Authorization", "Bearer "+token)
	_, err := f.api.GetServerInfo(ctx, request)
	return err
}

func (f *oidcServerFixture) await(t *testing.T, token string, code connect.Code) {
	t.Helper()
	until := time.Now().Add(12 * time.Second)
	for {
		err := f.read(token)
		if code == 0 && err == nil || err != nil && connect.CodeOf(err) == code {
			return
		}
		if time.Now().After(until) {
			t.Fatalf("OIDC read = %v, want %v", err, code)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newOIDCServerFixture(t *testing.T, remote bool) *oidcServerFixture {
	t.Helper()
	root, err := os.MkdirTemp("", "skygo-oidc-")
	if err != nil {
		t.Fatal(err)
	}
	f := &oidcServerFixture{root: root, data: filepath.Join(root, "state"), credentials: filepath.Join(root, "auth.json"), oidc: filepath.Join(root, "oidc.json"), keys: filepath.Join(root, "keys.json")}
	t.Cleanup(func() { f.close(t) })
	st, err := store.Open(f.data)
	if err != nil {
		f.startupError = err
		t.Fatal(err)
	}
	f.startupStore = st
	f.owner = st.Owner()
	output := filepath.Join(root, "admin.token")
	_, initializeErr := auth.Initialize(f.credentials, output, f.owner)
	var activateErr error
	if initializeErr == nil {
		activateErr = st.ActivateScoped()
	}
	if errors.Is(initializeErr, auth.ErrCloseUncertain) {
		f.startupError = initializeErr
		t.Fatal(initializeErr)
	}
	closeErr := st.Close()
	if closeErr != nil {
		f.startupError = closeErr
	} else {
		f.startupStore = nil
	}
	if err := errors.Join(initializeErr, activateErr, closeErr); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	f.admin = strings.TrimSuffix(string(raw), "\n")
	for _, p := range []struct {
		id   string
		role auth.Role
	}{{"alice", auth.Operator}, {"bob", auth.Reader}} {
		if err := auth.AddPrincipal(f.credentials, p.id, p.role); err != nil {
			if errors.Is(err, auth.ErrCloseUncertain) {
				f.startupError = err
			}
			t.Fatal(err)
		}
	}
	f.key, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.writeKeys(t, "k1", &f.key.PublicKey)
	if remote {
		f.issuer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f.mu.Lock()
			body := append([]byte(nil), f.jwks...)
			f.requests++
			f.paths = append(f.paths, r.URL.Path)
			redirect := f.redirect
			f.mu.Unlock()
			if redirect {
				http.Redirect(w, r, f.issuer.URL+"/redirect-target", http.StatusFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}))
		ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.issuer.Certificate().Raw})
		oidcReplace(t, filepath.Join(root, "issuer.pem"), ca, 0644)
	}
	f.writeMapping(t, 1, remote, "alice", "bob")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	f.done = make(chan error, 1)
	cfg := server.Config{DataDir: f.data, Listen: address, AuthFile: f.credentials, OIDCFile: f.oidc}
	if remote {
		cfg.OIDCJWKSURL = f.issuer.URL
	}
	go func() { f.done <- server.Run(ctx, cfg) }()
	f.api = skyv1connect.NewControlServiceClient(&http.Client{}, "http://"+address)
	f.await(t, f.admin, 0)
	return f
}

func TestOIDCServerActivation(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "file"
		if remote {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			f := newOIDCServerFixture(t, remote)
			alice := f.token(t, f.key, "k1", "alice-subject", 10*time.Minute)
			bob := f.token(t, f.key, "k1", "bob-subject", 10*time.Minute)
			f.await(t, alice, 0)
			f.await(t, bob, 0)
			for _, claim := range []string{"iss", "aud"} {
				wrong := f.changedClaim(t, alice, claim, "https://wrong.example")
				f.await(t, wrong, connect.CodeUnauthenticated)
			}
			f.await(t, f.token(t, f.key, "k1", "unknown-subject", 10*time.Minute), connect.CodeUnauthenticated)
			f.writeMapping(t, 2, remote, "bob")
			f.await(t, alice, connect.CodeUnauthenticated)
			f.await(t, bob, 0)
			next, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			f.writeKeys(t, "k2", &next.PublicKey)
			f.await(t, bob, connect.CodeUnauthenticated)
			rotated := f.token(t, next, "k2", "bob-subject", 10*time.Minute)
			f.await(t, rotated, 0)
			short := f.token(t, next, "k2", "bob-subject", 2*time.Second)
			f.await(t, short, 0)
			f.await(t, short, connect.CodeUnauthenticated)
			// Invalid identity mappings make only OIDC unavailable; file credentials
			// remain authoritative and a later complete generation recovers.
			oidcReplace(t, f.oidc, []byte("{"), 0600)
			f.await(t, rotated, connect.CodeUnavailable)
			f.await(t, f.admin, 0)
			f.writeMapping(t, 3, remote, "bob")
			f.await(t, rotated, 0)
			if remote {
				f.mu.Lock()
				requests := f.requests
				f.mu.Unlock()
				if requests == 0 {
					t.Fatal("HTTPS issuer was not contacted")
				}
			}
		})
	}
}

func TestOIDCConfigurationBeforeState(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  server.Config
	}{
		{"oidc without auth", server.Config{OIDCFile: "oidc.json"}},
		{"url without oidc", server.Config{AuthFile: "auth.json", OIDCJWKSURL: "https://issuer.example/keys"}},
		{"legacy token conflict", server.Config{AuthFile: "auth.json", OIDCFile: "oidc.json", Token: "legacy"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.cfg.DataDir = filepath.Join(t.TempDir(), "must-not-exist")
			tt.cfg.Listen = "127.0.0.1:0"
			if err := server.Run(context.Background(), tt.cfg); err == nil {
				t.Fatal("invalid OIDC configuration started")
			}
			if _, err := os.Stat(tt.cfg.DataDir); !os.IsNotExist(err) {
				t.Fatalf("invalid configuration created state: %v", err)
			}
		})
	}
}

func TestOIDCStreamMappingRevocation(t *testing.T) {
	f := newOIDCServerFixture(t, false)
	token := f.token(t, f.key, "k1", "alice-subject", 10*time.Minute)
	f.await(t, token, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	submit := connect.NewRequest(&v1.SubmitJobRequest{RequestId: "oidc-stream", Task: &v1.Task{Argv: []string{"/bin/sh", "-c", "printf ready; sleep 30"}, Target: "local"}})
	submit.Header().Set("Authorization", "Bearer "+token)
	accepted, err := f.api.SubmitJob(ctx, submit)
	if err != nil {
		t.Fatal(err)
	}
	// The job is real local execution. Server shutdown owns and stops it even
	// if a test assertion ends early, independently of this stream's authority.
	request := connect.NewRequest(&v1.StreamJobLogsRequest{Id: accepted.Msg.Job.Id, Follow: true})
	request.Header().Set("Authorization", "Bearer "+token)
	stream, err := f.api.StreamJobLogs(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	}()
	var output []byte
	for stream.Receive() {
		output = append(output, stream.Msg().Data...)
		if strings.Contains(string(output), "ready") {
			break
		}
	}
	if !strings.Contains(string(output), "ready") {
		t.Fatalf("stream did not receive workload output: %v", stream.Err())
	}
	f.writeMapping(t, 2, false, "bob")
	for stream.Receive() {
	}
	if connect.CodeOf(stream.Err()) != connect.CodeUnauthenticated {
		t.Fatalf("revoked stream = %v, want unauthenticated", stream.Err())
	}
	f.await(t, f.admin, 0)
}

// changedClaim re-signs a well-formed token so the server must reject the
// claimed issuer or audience, rather than merely rejecting a bad signature.
func (f *oidcServerFixture) changedClaim(t *testing.T, token, name, value string) string {
	t.Helper()
	parts := strings.Split(token, ".")
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(data, &claims); err != nil {
		t.Fatal(err)
	}
	claims[name] = value
	data, err = json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	input := parts[0] + "." + base64.RawURLEncoding.EncodeToString(data)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestOIDCServerHTTPSPolicy(t *testing.T) {
	f := newOIDCServerFixture(t, true)
	token := f.token(t, f.key, "k1", "alice-subject", 10*time.Minute)
	f.await(t, token, 0)
	// A mapping-file edit cannot select a different URL from the operator's
	// explicit server option, even on the same issuer origin.
	raw, err := os.ReadFile(f.oidc)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	config["jwks_uri"] = f.issuer.URL + "/unapproved"
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	oidcReplace(t, f.oidc, data, 0600)
	f.await(t, token, connect.CodeUnavailable)
	f.await(t, f.admin, 0)
	f.writeMapping(t, 2, true, "alice", "bob")
	f.await(t, token, 0)
	// This is a real owned TLS issuer returning HTTP302, not a fabricated
	// response or a live external IdP. Existing eligible keys remain usable
	// until the published key set's30s age limit; failed refresh cannot renew it.
	f.mu.Lock()
	f.redirect = true
	f.mu.Unlock()
	deadline := time.Now().Add(45 * time.Second)
	for {
		err := f.read(token)
		if connect.CodeOf(err) == connect.CodeUnavailable {
			break
		}
		if err != nil {
			t.Fatalf("issuer outage = %v, want unavailable after cache expiry", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("redirecting issuer kept stale keys authoritative")
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.await(t, f.admin, 0)
	f.mu.Lock()
	paths := append([]string(nil), f.paths...)
	f.mu.Unlock()
	for _, path := range paths {
		if path == "/unapproved" || path == "/redirect-target" {
			t.Fatalf("issuer received forbidden path %q", path)
		}
	}
}
