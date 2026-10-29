package auth

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// jwksServer is a SOFTWARE HTTPS fixture (httptest) whose responses a test
// controls. It is not an issuer, and its results are not issuer evidence.
// /redirect redirects to /keys; every other path serves the current body.
type jwksServer struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	body   []byte
	block  chan struct{} // if not nil, requests wait until it is closed
	hits   map[string]int
	times  []time.Time // arrival of each /keys request
}

func newJWKSServer(t *testing.T, body []byte) *jwksServer {
	t.Helper()
	s := &jwksServer{status: http.StatusOK, body: body, hits: make(map[string]int)}
	s.Server = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

func (s *jwksServer) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.hits[r.URL.Path]++
	if r.URL.Path == "/keys" {
		s.times = append(s.times, time.Now())
	}
	status, body, block := s.status, s.body, s.block
	s.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-r.Context().Done():
			return
		}
	}
	if r.URL.Path == "/redirect" {
		http.Redirect(w, r, "/keys", http.StatusFound)
		return
	}
	w.WriteHeader(status)
	w.Write(body)
}

func (s *jwksServer) set(status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status, s.body = status, body
}

func (s *jwksServer) hit(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

func (s *jwksServer) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, h := range s.hits {
		n += h
	}
	return n
}

func (s *jwksServer) keyTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.times...)
}

// writeCA writes the server's self-signed certificate as a public PEM file.
func (s *jwksServer) writeCA(t *testing.T, path string) {
	t.Helper()
	writeFileReplace(t, path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw}), 0644)
}

// validJWKS is the fixture's full key set: rsa-1, which f.idToken signs
// with, and ec-1.
func validJWKS(t *testing.T) []byte {
	return jwksJSON(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}), ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}))
}

// newJWKSURIFixture switches the OIDC engine fixture to a jwks_uri at path on
// a new key set server trusted through ca.pem. The engine allows only
// s.URL+"/keys". No key set has been fetched yet.
func newJWKSURIFixture(t *testing.T, path string) (*oidcEngineFixture, *jwksServer) {
	t.Helper()
	f := newOIDCEngineFixture(t)
	s := newJWKSServer(t, validJWKS(t))
	s.writeCA(t, filepath.Join(f.dir, "ca.pem"))
	f.e.oidcKeys = newJWKSFetcher(s.URL+"/keys", f.clock.now)
	f.writeOIDC(t, 2, map[string]any{"jwks_file": nil, "jwks_uri": s.URL + path, "ca_file": "ca.pem"})
	return f, s
}

// modelJWKSDo replaces doJWKS with do for the test. Responses do returns are
// MODELED; they stand in for an issuer and are not issuer evidence.
func modelJWKSDo(t *testing.T, do func(*http.Client, *http.Request) (*http.Response, error)) {
	t.Helper()
	t.Cleanup(func() { doJWKS = (*http.Client).Do })
	doJWKS = do
}

// requireScoped checks that the fixture's scoped token authenticates and the
// Engine holds no error: an OIDC failure must not reach scoped tokens.
func requireScoped(t *testing.T, f *oidcEngineFixture, step string) {
	t.Helper()
	if a, err := f.e.Authenticate("Bearer " + f.alice); err != nil || a.TokenID() == "" {
		t.Fatalf("%s: scoped Authenticate = %v", step, err)
	}
	if err := f.e.Err(); err != nil || !f.e.Ready() {
		t.Fatalf("%s: Err %v, Ready %v; want scoped tokens available", step, err, f.e.Ready())
	}
}

func TestNewFileOIDCArguments(t *testing.T) {
	f := newOIDCEngineFixture(t)
	for _, tt := range []struct {
		name, oidc, url string
		ok              bool
	}{
		{"file keys", f.oidc, "", true},
		{"allowed uri", f.oidc, "https://issuer.example/keys", true},
		{"allowed uri with port and query", f.oidc, "https://issuer.example:8443/keys?v=1", true},
		{"no oidc file", "", "", false},
		{"nul in path", f.oidc + "\x00", "", false},
		{"http uri", f.oidc, "http://issuer.example/keys", false},
		{"uri with fragment", f.oidc, "https://issuer.example/keys#a", false},
		{"uri with userinfo", f.oidc, "https://u:p@issuer.example/keys", false},
		{"relative uri", f.oidc, "/keys", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e, err := NewFileOIDC(f.auth, installation, tt.oidc, tt.url)
			if !tt.ok {
				if e != nil || err != ErrInvalid {
					t.Fatalf("NewFileOIDC = %v, %v; want nil, ErrInvalid", e, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewFileOIDC: %v", err)
			}
			if (e.oidcKeys != nil) != (tt.url != "") || e.oidcPath != tt.oidc {
				t.Fatalf("NewFileOIDC configured keys %v, path %q", e.oidcKeys != nil, e.oidcPath)
			}
			if e.oidcKeys != nil && e.oidcKeys.allowed != tt.url {
				t.Fatalf("allowed URL %q, want %q", e.oidcKeys.allowed, tt.url)
			}
			// Construction reads no OIDC file and fetches nothing: OIDC waits for Run.
			if _, err := e.Authenticate("Bearer " + f.idToken(t, nil)); err != ErrUnavailable {
				t.Fatalf("Authenticate before Run = %v, want ErrUnavailable", err)
			}
			if _, err := e.Authenticate("Bearer " + f.alice); err != nil {
				t.Fatalf("scoped Authenticate: %v", err)
			}
		})
	}
	// An ordinary construction failure returns no Engine.
	if e, err := NewFileOIDC(filepath.Join(f.dir, "absent.json"), installation, f.oidc, ""); e != nil || err == nil {
		t.Fatalf("NewFileOIDC with a missing credential file = %v, %v; want nil and an error", e, err)
	}
	if e, err := NewFileOIDC(f.auth, "fedcba9876543210fedcba9876543210", f.oidc, ""); e != nil || err == nil {
		t.Fatalf("NewFileOIDC with a foreign installation = %v, %v; want nil and an error", e, err)
	}
}

// TestOIDCEngineJWKSURI walks one fetched key set through first fetch,
// outage, the exact maxJWKSAge boundary, recovery and rotation. Failed
// fetches never renew the age, and scoped tokens stay available throughout.
func TestOIDCEngineJWKSURI(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	ctx := context.Background()
	token := f.idToken(t, nil)

	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate before the first fetch = %v, want ErrUnavailable", err)
	}
	requireScoped(t, f, "before the first fetch")
	select {
	case <-f.e.oidcKeys.wake:
	default:
		t.Fatal("the OIDC read did not ask for a fetch")
	}
	fetched := f.clock.now()
	if err := f.e.oidcKeys.fetch(ctx); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	f.refresh(t)
	alice := f.authenticate(t, token)

	// An outage: every failed fetch leaves the age dating from the last
	// successful fetch.
	s.set(http.StatusServiceUnavailable, []byte(`{"keys":[]}`))
	for f.clock.now().Sub(fetched) < maxJWKSAge {
		f.clock.add(jwksRefreshInterval)
		if err := f.e.oidcKeys.fetch(ctx); err == nil || errors.Is(err, ErrCloseUncertain) {
			t.Fatalf("fetch during an outage = %v, want an ordinary error", err)
		}
		f.refresh(t)
		requireScoped(t, f, "during an outage")
	}
	if age := f.clock.now().Sub(fetched); age != maxJWKSAge {
		t.Fatalf("age %v, want exactly %v", age, maxJWKSAge)
	}
	if _, err := f.e.Check(alice, Mutate); err != nil {
		t.Fatalf("Check at exactly maxJWKSAge: %v", err)
	}
	// Just past the bound the published snapshot is refused at once, before
	// any further OIDC read: the age is checked at each Check.
	f.clock.add(time.Nanosecond)
	if _, err := f.e.Check(alice, Read); err != ErrUnavailable {
		t.Fatalf("Check past maxJWKSAge = %v, want ErrUnavailable", err)
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate past maxJWKSAge = %v, want ErrUnavailable", err)
	}
	requireScoped(t, f, "past maxJWKSAge")
	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate past maxJWKSAge after a read = %v, want ErrUnavailable", err)
	}

	// Recovery publishes the key set again; the same Actor is still valid.
	s.set(http.StatusOK, validJWKS(t))
	if err := f.e.oidcKeys.fetch(ctx); err != nil {
		t.Fatalf("fetch after the outage: %v", err)
	}
	f.refresh(t)
	if _, err := f.e.Check(alice, Mutate); err != nil {
		t.Fatalf("Check after recovery: %v", err)
	}

	// A fetched key set without the signing key revokes at the next read.
	s.set(http.StatusOK, jwksJSON(t, ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"})))
	if err := f.e.oidcKeys.fetch(ctx); err != nil {
		t.Fatalf("fetch after rotation: %v", err)
	}
	f.refresh(t)
	if _, err := f.e.Check(alice, Read); err != ErrUnauthenticated {
		t.Fatalf("Check after key removal = %v, want ErrUnauthenticated", err)
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnauthenticated {
		t.Fatalf("Authenticate after key removal = %v, want ErrUnauthenticated", err)
	}
	requireScoped(t, f, "after rotation")
}

// Fetch phases that modelFetchDelays can slow.
const (
	phaseCARead    = "ca read"
	phaseBodyRead  = "body read"
	phaseBodyClose = "body close"
	phaseVerify    = "verify"
)

// phaseBody wraps the software fixture's original response body. read runs
// once after the first Read, and close after Close, both on the original.
type phaseBody struct {
	io.ReadCloser
	read, close func()
	once        sync.Once
}

func (b *phaseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.once.Do(b.read)
	return n, err
}

func (b *phaseBody) Close() error {
	err := b.ReadCloser.Close()
	b.close()
	return err
}

// modelFetchDelays makes each later fetch of f's fetcher move f.clock by the
// delay set for each phase, after the actual CA file read, the first read of
// the actual software fixture response body, its actual Close and the actual
// parseJWKS. The delays are MODELED: no phase is slow in real time. set
// replaces the delays; nil clears them. A phase may run onPhase first.
func modelFetchDelays(t *testing.T, f *oidcEngineFixture) (set func(map[string]time.Duration), onPhase func(string, func())) {
	t.Helper()
	var mu sync.Mutex
	delays := map[string]time.Duration{}
	hooks := map[string]func(){}
	advance := func(phase string) {
		mu.Lock()
		d, hook := delays[phase], hooks[phase]
		mu.Unlock()
		if hook != nil {
			hook()
		}
		f.clock.add(d)
	}
	read, verify := readOIDCFile, verifyJWKS
	t.Cleanup(func() { readOIDCFile, verifyJWKS = read, verify })
	readOIDCFile = func(file *os.File, max int64) ([]byte, error) {
		data, err := read(file, max)
		if filepath.Base(file.Name()) == "ca.pem" {
			advance(phaseCARead)
		}
		return data, err
	}
	verifyJWKS = func(data []byte) (*jwkSet, error) {
		keys, err := verify(data)
		advance(phaseVerify)
		return keys, err
	}
	modelJWKSDo(t, func(c *http.Client, req *http.Request) (*http.Response, error) {
		resp, err := c.Do(req)
		if resp != nil && resp.Body != nil {
			resp.Body = &phaseBody{ReadCloser: resp.Body, read: func() { advance(phaseBodyRead) }, close: func() { advance(phaseBodyClose) }}
		}
		return resp, err
	})
	set = func(m map[string]time.Duration) {
		mu.Lock()
		defer mu.Unlock()
		delays = map[string]time.Duration{}
		for k, v := range m {
			delays[k] = v
		}
	}
	onPhase = func(phase string, hook func()) {
		mu.Lock()
		defer mu.Unlock()
		hooks[phase] = hook
	}
	return set, onPhase
}

// requireLate checks that err refuses a fetch that ended after its window,
// with fixed text and the deadline or cancellation cause, without holding
// the Engine.
func requireLate(t *testing.T, f *oidcEngineFixture, err, cause error) {
	t.Helper()
	if err == nil || !errors.Is(err, cause) || err.Error() != "fetch JWKS: too late" {
		t.Fatalf("late fetch = %v, want the refusal retaining %v", err, cause)
	}
	if errors.Is(err, ErrCloseUncertain) || f.e.Err() != nil {
		t.Fatalf("a late fetch with a clean Close held the Engine: %v", err)
	}
}

// TestOIDCEngineJWKSFetchWindow moves the MODELED clock by exactly
// jwksFetchTimeout, and by one nanosecond more, in each phase of one fetch:
// the CA file read, the response body read, its first Close and the key set
// verification. The whole fetch is eligible at exactly the bound and is then
// dated when it completed; one nanosecond later it publishes nothing.
func TestOIDCEngineJWKSFetchWindow(t *testing.T) {
	for _, phase := range []string{phaseCARead, phaseBodyRead, phaseBodyClose, phaseVerify} {
		for _, d := range []time.Duration{jwksFetchTimeout, jwksFetchTimeout + time.Nanosecond} {
			t.Run(fmt.Sprintf("%s/%v", phase, d), func(t *testing.T) {
				f, s := newJWKSURIFixture(t, "/keys")
				f.refresh(t)
				set, _ := modelFetchDelays(t, f)
				set(map[string]time.Duration{phase: d})
				start := f.clock.now()
				err := f.e.oidcKeys.fetch(context.Background())
				if n := s.hit("/keys"); n != 1 {
					t.Fatalf("%d requests, want 1", n)
				}
				if got := f.clock.now().Sub(start); got != d {
					t.Fatalf("modeled fetch took %v, want %v", got, d)
				}
				if d <= jwksFetchTimeout {
					if err != nil {
						t.Fatalf("fetch at exactly the window: %v", err)
					}
					if got := f.e.oidcKeys.fetched; !got.Equal(start.Add(d)) {
						t.Fatalf("key set dated %v, want its completion %v", got, start.Add(d))
					}
					f.refresh(t)
					f.authenticate(t, f.idToken(t, nil))
					return
				}
				requireLate(t, f, err, context.DeadlineExceeded)
				if f.e.oidcKeys.data != nil || !f.e.oidcKeys.fetched.IsZero() {
					t.Fatal("a late fetch published its key set")
				}
				f.refresh(t)
				if _, err := f.e.Authenticate("Bearer " + f.idToken(t, nil)); err != ErrUnavailable {
					t.Fatalf("Authenticate after a late fetch = %v, want ErrUnavailable", err)
				}
				requireScoped(t, f, "after a late fetch")
			})
		}
	}
}

// TestOIDCEngineJWKSCommitFence holds the fetcher's actual mutex from the
// verification hook, after the actual parseJWKS, so a clean verified fetch
// waits for it to publish. While the fetch waits, the control cancels its
// actual parent, or moves the MODELED clock to exactly the end of its window
// or one nanosecond past it, then releases the mutex. A cancelled or late
// fetch publishes nothing and returns its cause: the earlier key set keeps
// its bytes, source and completion time. A fetch still within its window
// publishes once, dated when it acquired the mutex.
func TestOIDCEngineJWKSCommitFence(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cancel bool
		wait   time.Duration
		cause  error
	}{
		{"within window", false, jwksFetchTimeout, nil},
		{"cancelled", true, 0, context.Canceled},
		{"past window", false, jwksFetchTimeout + time.Nanosecond, context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, s := newJWKSURIFixture(t, "/keys")
			f.refresh(t)
			k := f.e.oidcKeys
			if err := k.fetch(context.Background()); err != nil {
				t.Fatalf("first fetch: %v", err)
			}
			oldData, oldHave, oldFetched := k.data, k.have, k.fetched
			if len(oldData) == 0 {
				t.Fatal("first fetch published no key set")
			}
			f.clock.add(time.Second)
			_, onPhase := modelFetchDelays(t, f)

			// The hook takes k.mu on the fetch goroutine, at most once and
			// only before unlock; unlock releases it only if taken. So a
			// missed or late hook cannot strand the fetch or unlock an
			// unlocked mutex.
			var hmu sync.Mutex
			var taken, holding, released bool
			held := make(chan struct{})
			onPhase(phaseVerify, func() {
				hmu.Lock()
				defer hmu.Unlock()
				if taken || released {
					return
				}
				k.mu.Lock()
				taken, holding = true, true
				close(held)
			})
			unlock := func() {
				hmu.Lock()
				defer hmu.Unlock()
				released = true
				if holding {
					holding = false
					k.mu.Unlock()
				}
			}
			start := f.clock.now()
			var err error
			// startOriginal's cleanup, registered before the fetch starts,
			// releases the mutex, cancels the fetch's parent and waits for
			// that same fetch without limit before the seam resets and
			// fixture removal registered earlier.
			cancel, returned := startOriginal(t, unlock, func(ctx context.Context) {
				err = k.fetch(ctx)
			})
			select {
			case <-held:
			case <-time.After(10 * time.Second):
				t.Fatal("fetch did not verify its key set within 10s")
			}
			select {
			case <-returned:
				t.Fatal("fetch returned while the mutex was held")
			case <-time.After(200 * time.Millisecond):
			}
			if tt.cancel {
				cancel()
			}
			f.clock.add(tt.wait)
			unlock()
			select {
			case <-returned:
			case <-time.After(10 * time.Second):
				t.Fatal("fetch did not return within 10s of the mutex's release")
			}
			if n := s.hit("/keys"); n != 2 {
				t.Fatalf("%d requests, want 2", n)
			}
			if tt.cause == nil {
				if err != nil {
					t.Fatalf("fetch within its window: %v", err)
				}
				// A new slice from the new fetch, dated at the fence.
				if len(k.data) == 0 || &k.data[0] == &oldData[0] || k.have != oldHave || !k.fetched.Equal(start.Add(tt.wait)) {
					t.Fatalf("key set dated %v, want published once and dated %v", k.fetched, start.Add(tt.wait))
				}
				f.refresh(t)
				f.authenticate(t, f.idToken(t, nil))
				return
			}
			requireLate(t, f, err, tt.cause)
			other := context.Canceled
			if tt.cancel {
				other = context.DeadlineExceeded
			}
			if errors.Is(err, other) {
				t.Fatalf("fetch = %v; it also reports %v", err, other)
			}
			if len(k.data) == 0 || &k.data[0] != &oldData[0] || len(k.data) != len(oldData) || k.have != oldHave || !k.fetched.Equal(oldFetched) {
				t.Fatalf("a refused fetch replaced or redated the key set (dated %v, want %v)", k.fetched, oldFetched)
			}
		})
	}
}

// TestOIDCEngineJWKSSlowFetch checks that a key set's age dates from the
// completion of its fetch. The MODELED clock moves two seconds in each of
// the CA file read, response body read, Close and verification; the key set
// is then fresh until exactly maxJWKSAge after the fetch completed, on both
// Authenticate and Check and with the OIDC mappings and credential file
// freshly read. A later late fetch and a later failed fetch renew nothing.
func TestOIDCEngineJWKSSlowFetch(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	token := f.idToken(t, nil)
	f.refresh(t)
	set, _ := modelFetchDelays(t, f)
	set(map[string]time.Duration{phaseCARead: 2 * time.Second, phaseBodyRead: 2 * time.Second, phaseBodyClose: 2 * time.Second, phaseVerify: 2 * time.Second})
	start := f.clock.now()
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("slow fetch: %v", err)
	}
	done := f.clock.now()
	if done.Sub(start) != 8*time.Second {
		t.Fatalf("modeled fetch took %v, want 8s", done.Sub(start))
	}
	f.refresh(t)
	alice := f.authenticate(t, token)
	f.e.mu.RLock()
	got := f.e.oidc.fetched
	f.e.mu.RUnlock()
	if !got.Equal(done) {
		t.Fatalf("key set dated %v, want the fetch completion %v", got, done)
	}

	// A late refetch of the same key set renews nothing.
	f.clock.add(jwksRefreshInterval)
	set(map[string]time.Duration{phaseBodyRead: jwksFetchTimeout + time.Nanosecond})
	requireLate(t, f, f.e.oidcKeys.fetch(context.Background()), context.DeadlineExceeded)
	if !f.e.oidcKeys.fetched.Equal(done) {
		t.Fatalf("a late fetch redated the key set to %v", f.e.oidcKeys.fetched)
	}
	// Nor does a failed one.
	set(nil)
	s.set(http.StatusServiceUnavailable, validJWKS(t))
	if err := f.e.oidcKeys.fetch(context.Background()); err == nil {
		t.Fatal("fetch during an outage succeeded")
	}
	if !f.e.oidcKeys.fetched.Equal(done) {
		t.Fatalf("a failed fetch redated the key set to %v", f.e.oidcKeys.fetched)
	}

	f.clock.add(done.Add(maxJWKSAge).Sub(f.clock.now()))
	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + token); err != nil {
		t.Fatalf("Authenticate maxJWKSAge after the fetch completed: %v", err)
	}
	if _, err := f.e.Check(alice, Read); err != nil {
		t.Fatalf("Check maxJWKSAge after the fetch completed: %v", err)
	}
	// One nanosecond later the snapshot just read, with fresh mappings, is
	// refused on both paths, and so is the next read.
	f.clock.add(time.Nanosecond)
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate past maxJWKSAge from the completion = %v, want ErrUnavailable", err)
	}
	if _, err := f.e.Check(alice, Read); err != ErrUnavailable {
		t.Fatalf("Check past maxJWKSAge from the completion = %v, want ErrUnavailable", err)
	}
	f.refresh(t)
	if _, err := f.e.Check(alice, Read); err != ErrUnavailable {
		t.Fatalf("Check past maxJWKSAge after a read = %v, want ErrUnavailable", err)
	}
	requireScoped(t, f, "after a slow fetch aged out")
}

// TestOIDCEngineJWKSSnapshotAge checks that a published snapshot carries the
// age of its own key set bytes: a later fetch renews the fetcher's cache but
// not a snapshot read before it, until the next OIDC read takes the new
// fetch.
func TestOIDCEngineJWKSSnapshotAge(t *testing.T) {
	f, _ := newJWKSURIFixture(t, "/keys")
	token := f.idToken(t, nil)
	f.refresh(t)
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	first := f.clock.now()
	f.refresh(t)
	alice := f.authenticate(t, token)

	// The snapshot is read 29s after the first fetch completed.
	f.clock.add(maxJWKSAge - time.Second)
	f.refresh(t)
	// A second fetch completes 30s+1ns after the first; it is well inside
	// its own window.
	set, _ := modelFetchDelays(t, f)
	set(map[string]time.Duration{phaseBodyRead: time.Second + time.Nanosecond})
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	second := f.clock.now()
	if second.Sub(first) != maxJWKSAge+time.Nanosecond || !f.e.oidcKeys.fetched.Equal(second) {
		t.Fatalf("second fetch dated %v, %v after the first", f.e.oidcKeys.fetched, second.Sub(first))
	}
	// The OIDC read and credential file are fresh, but the snapshot's own
	// key set is too old.
	if _, err := f.e.Check(alice, Read); err != ErrUnavailable {
		t.Fatalf("Check with an old snapshot key set = %v, want ErrUnavailable", err)
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate with an old snapshot key set = %v, want ErrUnavailable", err)
	}
	f.refresh(t)
	f.e.mu.RLock()
	got := f.e.oidc.fetched
	f.e.mu.RUnlock()
	if !got.Equal(second) {
		t.Fatalf("new snapshot dated %v, want %v", got, second)
	}
	if _, err := f.e.Check(alice, Mutate); err != nil {
		t.Fatalf("Check after the next read: %v", err)
	}
}

// TestOIDCEngineJWKSKeyRemovalBound checks the honest removal bound. The
// software fixture stops serving rsa-1 just after a fetch's response was
// sent; that fetch completes, MODELED, jwksFetchTimeout later with the old
// key set. While every later fetch fails, rsa-1 still authenticates exactly
// jwksFetchTimeout+maxJWKSAge after the removal, and not one nanosecond
// later. The bound assumes the fixture, the configured endpoint, serves its
// current key set to every later request; a later successful fetch removes
// the key at its completion.
func TestOIDCEngineJWKSKeyRemovalBound(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	token := f.idToken(t, nil)
	f.refresh(t)
	set, onPhase := modelFetchDelays(t, f)
	var removed time.Time
	ecOnly := jwksJSON(t, ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}))
	onPhase(phaseBodyRead, func() {
		if removed.IsZero() {
			s.set(http.StatusOK, ecOnly)
			removed = f.clock.now()
		}
	})
	set(map[string]time.Duration{phaseBodyRead: jwksFetchTimeout})
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	onPhase(phaseBodyRead, nil)
	set(nil)
	f.refresh(t)
	alice := f.authenticate(t, token)

	s.set(http.StatusServiceUnavailable, ecOnly)
	for f.clock.now().Sub(removed) < jwksFetchTimeout+maxJWKSAge {
		f.clock.add(jwksRefreshInterval)
		if err := f.e.oidcKeys.fetch(context.Background()); err == nil {
			t.Fatal("fetch during an outage succeeded")
		}
		f.refresh(t)
	}
	if bound := f.clock.now().Sub(removed); bound != 40*time.Second {
		t.Fatalf("bound %v, want 40s", bound)
	}
	if _, err := f.e.Check(alice, Read); err != nil {
		t.Fatalf("Check exactly 40s after the removal: %v", err)
	}
	f.clock.add(time.Nanosecond)
	if _, err := f.e.Check(alice, Read); err != ErrUnavailable {
		t.Fatalf("Check 40s+1ns after the removal = %v, want ErrUnavailable", err)
	}

	// A successful fetch of the current key set removes the key at once.
	s.set(http.StatusOK, ecOnly)
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("fetch after the outage: %v", err)
	}
	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnauthenticated {
		t.Fatalf("Authenticate after the removal was fetched = %v, want ErrUnauthenticated", err)
	}
	requireScoped(t, f, "after the removal")
}

// TestOIDCEngineJWKSURIFreshness checks that fresh fetched keys do not extend
// the two-second envelope of the OIDC file, mappings and roles.
func TestOIDCEngineJWKSURIFreshness(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	f.refresh(t)
	if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
		t.Fatalf("fetch: %v", err)
	}
	f.refresh(t)
	a := f.authenticate(t, f.idToken(t, nil))

	f.clock.add(2*time.Second + time.Nanosecond)
	if err := f.e.refresh(); err != nil {
		t.Fatalf("scoped credential refresh: %v", err)
	}
	if _, err := f.e.Check(a, Read); err != ErrUnavailable {
		t.Fatalf("Check with fresh keys but a stale OIDC read = %v, want ErrUnavailable", err)
	}
	requireScoped(t, f, "stale OIDC read")
	f.refresh(t)
	if _, err := f.e.Check(a, Mutate); err != nil {
		t.Fatalf("Check after the next OIDC read: %v", err)
	}

	if err := SetRole(f.auth, "alice", Reader); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	if _, err := f.e.Check(a, Mutate); err != ErrPermissionDenied {
		t.Fatalf("Check(Mutate) after demotion = %v, want ErrPermissionDenied", err)
	}

	f.writeOIDC(t, 3, map[string]any{"jwks_file": nil, "jwks_uri": s.URL + "/keys", "ca_file": "ca.pem", "identities": []map[string]string{{"subject": "bob-subject", "principal": "bob"}}})
	f.refresh(t)
	if _, err := f.e.Check(a, Read); err != ErrUnauthenticated {
		t.Fatalf("Check after mapping removal with fresh keys = %v, want ErrUnauthenticated", err)
	}
}

// TestOIDCEngineJWKSURIRefused checks responses and configurations whose
// fetch must fail. Each case starts from a published key set. A refused
// response leaves that cache unchanged, so the same source still serves it;
// a refused new source is unavailable. No refusal holds the Engine, puts the
// URL in an error, or reaches scoped tokens.
func TestOIDCEngineJWKSURIRefused(t *testing.T) {
	writeCA := func(t *testing.T, f *oidcEngineFixture, name string, data []byte, perm os.FileMode) {
		writeFileReplace(t, filepath.Join(f.dir, name), data, perm)
	}
	for _, tt := range []struct {
		name string
		// prepare changes the server or files and returns the OIDC file's
		// key set members for the refused fetch.
		prepare func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any
		request bool  // the refused fetch reaches the server
		want    error // Authenticate after the refused fetch
	}{
		{"not found", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.set(http.StatusNotFound, validJWKS(t))
			return nil
		}, true, nil},
		{"server error", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.set(http.StatusInternalServerError, validJWKS(t))
			return nil
		}, true, nil},
		{"oversized", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.set(http.StatusOK, []byte(padJSON(string(validJWKS(t)), maxJWKSBytes+1)))
			return nil
		}, true, nil},
		{"private member", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.set(http.StatusOK, jwksJSON(t, ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"d": "AAAA"})))
			return nil
		}, true, nil},
		{"not json", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.set(http.StatusOK, []byte("<html>"))
			return nil
		}, true, nil},
		{"redirect", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			f.e.oidcKeys = newJWKSFetcher(s.URL+"/redirect", f.clock.now)
			return map[string]any{"jwks_uri": s.URL + "/redirect"}
		}, true, ErrUnavailable},
		{"untrusted certificate", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			return map[string]any{"ca_file": nil}
		}, false, ErrUnavailable},
		{"ca not pem", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			writeCA(t, f, "ca-bad.pem", []byte("not a certificate\n"), 0644)
			return map[string]any{"ca_file": "ca-bad.pem"}
		}, false, ErrUnavailable},
		{"ca oversized", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			writeCA(t, f, "ca-big.pem", []byte(strings.Repeat("\n", maxCABytes+1)), 0644)
			return map[string]any{"ca_file": "ca-big.pem"}
		}, false, ErrUnavailable},
		{"ca group writable", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			s.writeCA(t, filepath.Join(f.dir, "ca-gw.pem"))
			if err := os.Chmod(filepath.Join(f.dir, "ca-gw.pem"), 0664); err != nil {
				t.Fatal(err)
			}
			return map[string]any{"ca_file": "ca-gw.pem"}
		}, false, ErrUnavailable},
		{"ca symlink", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			if err := os.Symlink(filepath.Join(f.dir, "ca.pem"), filepath.Join(f.dir, "ca-link.pem")); err != nil {
				t.Fatal(err)
			}
			return map[string]any{"ca_file": "ca-link.pem"}
		}, false, ErrUnavailable},
		{"ca missing", func(t *testing.T, f *oidcEngineFixture, s *jwksServer) map[string]any {
			return map[string]any{"ca_file": "absent.pem"}
		}, false, ErrUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, s := newJWKSURIFixture(t, "/keys")
			token := f.idToken(t, nil)
			f.refresh(t)
			if err := f.e.oidcKeys.fetch(context.Background()); err != nil {
				t.Fatalf("first fetch: %v", err)
			}
			f.refresh(t)
			f.authenticate(t, token)
			change := map[string]any{"jwks_file": nil, "jwks_uri": s.URL + "/keys", "ca_file": "ca.pem"}
			for k, v := range tt.prepare(t, f, s) {
				change[k] = v
			}
			before := f.e.oidcKeys.data
			if tt.name == "redirect" {
				before = nil
			}
			f.writeOIDC(t, 3, change)
			f.refresh(t)
			keys, total := s.hit("/keys"), s.total()
			err := f.e.oidcKeys.fetch(context.Background())
			if err == nil {
				t.Fatal("fetch succeeded")
			}
			if errors.Is(err, ErrCloseUncertain) || f.e.Err() != nil {
				t.Fatalf("an ordinary refusal held the Engine: %v", err)
			}
			if strings.Contains(err.Error(), s.URL) || strings.Contains(err.Error(), f.dir) {
				t.Fatalf("error %q exposes the URL or a path", err.Error())
			}
			if (s.total() != total) != tt.request {
				t.Fatalf("requests %d -> %d, want a request: %v", total, s.total(), tt.request)
			}
			if s.hit("/keys") != keys && tt.name == "redirect" {
				t.Fatal("followed a redirect")
			}
			if string(f.e.oidcKeys.data) != string(before) {
				t.Fatal("a refused fetch replaced the cached key set")
			}
			f.refresh(t)
			if _, err := f.e.Authenticate("Bearer " + token); err != tt.want {
				t.Fatalf("Authenticate = %v, want %v", err, tt.want)
			}
			requireScoped(t, f, "after a refused fetch")
		})
	}
}

// TestOIDCEngineJWKSURINotAllowed checks that only the constructor's URL is
// ever fetched: an OIDC file naming any other URL is unavailable and the
// other URL is never requested.
func TestOIDCEngineJWKSURINotAllowed(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/other")
	f.refresh(t)
	if err := f.e.oidcKeys.fetch(context.Background()); err != ErrUnavailable {
		t.Fatalf("fetch = %v, want ErrUnavailable", err)
	}
	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + f.idToken(t, nil)); err != ErrUnavailable {
		t.Fatalf("Authenticate = %v, want ErrUnavailable", err)
	}
	if n := s.total(); n != 0 {
		t.Fatalf("%d key set requests, want 0", n)
	}
	requireScoped(t, f, "foreign URL")

	// Without an allowed URL, a jwks_uri is unavailable, as before.
	f.e.oidcKeys = nil
	f.refresh(t)
	if _, err := f.e.Authenticate("Bearer " + f.idToken(t, nil)); err != ErrUnavailable {
		t.Fatalf("Authenticate without an allowed URL = %v, want ErrUnavailable", err)
	}
}

// TestOIDCEngineRunFetches runs the original Run with the exported
// constructor, the real clock and the software fixture: Run fetches the
// allowed key set without any manual refresh, refetches no sooner than
// jwksRefreshInterval, and cancellation aborts a blocked fetch and joins it.
func TestOIDCEngineRunFetches(t *testing.T) {
	f, s := newJWKSURIFixture(t, "/keys")
	e, err := NewFileOIDC(f.auth, installation, f.oidc, s.URL+"/keys")
	if err != nil {
		t.Fatal(err)
	}
	token := f.idToken(t, nil)
	cancel, done := startRun(t, e, nil)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := e.Authenticate("Bearer " + token); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run did not fetch the key set within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(jwksRefreshInterval + 10*time.Second)
	for len(s.keyTimes()) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("Run did not refetch the key set")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if times := s.keyTimes(); times[1].Sub(times[0]) < jwksRefreshInterval {
		t.Fatalf("refetched after %v, want at least %v", times[1].Sub(times[0]), jwksRefreshInterval)
	}

	// Block the next fetch, which a source change starts at once.
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	s.mu.Lock()
	s.block = block
	s.mu.Unlock()
	hits := s.hit("/keys")
	s.writeCA(t, filepath.Join(f.dir, "ca2.pem"))
	f.writeOIDC(t, 3, map[string]any{"jwks_file": nil, "jwks_uri": s.URL + "/keys", "ca_file": "ca2.pem"})
	deadline = time.Now().Add(10 * time.Second)
	for s.hit("/keys") == hits {
		if time.Now().After(deadline) {
			t.Fatal("Run did not fetch after the source changed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(jwksFetchTimeout / 2):
		t.Fatal("Run did not return within half the fetch timeout of cancellation")
	}
	if e.Err() != nil {
		t.Fatalf("a cancelled fetch held the Engine: %v", e.Err())
	}
	if n := s.total(); n != hits+1 {
		t.Fatalf("%d requests after Run returned, want %d", n, hits+1)
	}
}
