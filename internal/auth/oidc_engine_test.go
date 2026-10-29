package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type oidcClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *oidcClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *oidcClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// oidcEngineFixture is a file Engine with a credential file (alice operator,
// bob reader), an OIDC file and a JWKS file in a private directory.
type oidcEngineFixture struct {
	dir, auth, oidc, keys string
	alice                 string // scoped token for alice
	base                  time.Time
	clock                 *oidcClock
	e                     *Engine
}

func newOIDCEngineFixture(t *testing.T) *oidcEngineFixture {
	t.Helper()
	oidcTestKeys(t)
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := &oidcEngineFixture{dir: dir, auth: filepath.Join(dir, "auth.json"), oidc: filepath.Join(dir, "oidc.json"), keys: filepath.Join(dir, "keys.json")}
	if _, err := Initialize(f.auth, filepath.Join(dir, "legacy.token"), installation); err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct {
		id   string
		role Role
	}{{"alice", Operator}, {"bob", Reader}} {
		if err := AddPrincipal(f.auth, p.id, p.role); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Issue(f.auth, "alice", filepath.Join(dir, "alice.token"), time.Hour); err != nil {
		t.Fatal(err)
	}
	f.alice = readToken(t, filepath.Join(dir, "alice.token"))
	// The fake clock starts after issuance so the scoped token is usable.
	f.base = time.Now().Truncate(time.Second).Add(2 * time.Second)
	f.clock = &oidcClock{t: f.base}
	f.writeKeys(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}), ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}))
	f.writeOIDC(t, 1, nil)
	e, err := newFileOIDC(f.auth, installation, f.oidc, f.clock.now)
	if err != nil {
		t.Fatal(err)
	}
	f.e = e
	f.e.refreshOIDC()
	return f
}

func writeFileReplace(t *testing.T, path string, data []byte, perm os.FileMode) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(tmp, perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// writeOIDC writes the OIDC file at generation with changes applied; a nil
// value deletes a member.
func (f *oidcEngineFixture) writeOIDC(t *testing.T, generation uint64, change map[string]any) {
	t.Helper()
	m := map[string]any{
		"version":      1,
		"installation": installation,
		"generation":   generation,
		"issuer":       oidcIssuerURL,
		"audience":     "skygo",
		"jwks_file":    "keys.json",
		"identities":   []map[string]string{{"subject": "alice-subject", "principal": "alice"}, {"subject": "bob-subject", "principal": "bob"}},
	}
	for k, v := range change {
		if v == nil {
			delete(m, k)
			continue
		}
		m[k] = v
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	writeFileReplace(t, f.oidc, data, 0600)
}

// writeKeys writes a world-readable JWKS file; public keys need no privacy.
func (f *oidcEngineFixture) writeKeys(t *testing.T, keys ...map[string]any) {
	t.Helper()
	writeFileReplace(t, f.keys, jwksJSON(t, keys...), 0644)
}

// refresh runs one credential file refresh and one OIDC refresh, as Run's two
// loops would.
func (f *oidcEngineFixture) refresh(t *testing.T) {
	t.Helper()
	if err := f.e.refresh(); err != nil {
		t.Fatalf("scoped credential refresh: %v", err)
	}
	f.e.refreshOIDC()
}

// idToken mints an RS256 ID token valid for ten minutes from the fixture base.
func (f *oidcEngineFixture) idToken(t *testing.T, change map[string]any) string {
	t.Helper()
	m := map[string]any{"exp": f.base.Unix() + 600, "iat": f.base.Unix() - 60}
	for k, v := range change {
		m[k] = v
	}
	return jws(rsaHeader, oidcClaims(t, m), func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) })
}

func (f *oidcEngineFixture) authenticate(t *testing.T, token string) Actor {
	t.Helper()
	a, err := f.e.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	return a
}

func TestOIDCEngineAuthenticate(t *testing.T) {
	f := newOIDCEngineFixture(t)
	alice := f.authenticate(t, f.idToken(t, nil))
	if alice.Principal() != "alice" || alice.Role() != Operator || alice.TokenID() != "" {
		t.Fatalf("actor = %q %q %q", alice.Principal(), alice.Role(), alice.TokenID())
	}
	if _, err := f.e.Check(alice, Mutate); err != nil {
		t.Fatalf("Check(Mutate): %v", err)
	}
	if _, err := f.e.Delegate(alice); err != ErrUnauthenticated {
		t.Fatalf("Delegate(OIDC actor) = %v, want ErrUnauthenticated", err)
	}

	es := jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, map[string]any{"sub": "bob-subject", "exp": f.base.Unix() + 600, "iat": f.base.Unix() - 60}), func(input string) []byte { return signES256(t, oidcKeys.ec, input) })
	bob := f.authenticate(t, es)
	if bob.Principal() != "bob" || bob.Role() != Reader {
		t.Fatalf("bob = %q %q", bob.Principal(), bob.Role())
	}
	if _, err := f.e.Check(bob, Mutate); err != ErrPermissionDenied {
		t.Fatalf("bob Check(Mutate) = %v", err)
	}

	// Scoped tokens are unchanged beside OIDC.
	scoped := f.authenticate(t, f.alice)
	if scoped.TokenID() == "" || scoped.Role() != Operator {
		t.Fatalf("scoped actor = %q %q", scoped.TokenID(), scoped.Role())
	}
	if _, err := f.e.Delegate(scoped); err != nil {
		t.Fatalf("Delegate(scoped) = %v", err)
	}

	for _, tt := range []struct {
		name, header string
	}{
		{"unmapped", "Bearer " + f.idToken(t, map[string]any{"sub": "mallory", "preferred_username": "alice"})},
		{"wrong audience", "Bearer " + f.idToken(t, map[string]any{"aud": "other"})},
		{"garbage jws", "Bearer a.b.c"},
		{"oversized", "Bearer " + strings.Repeat("a", 4500) + "." + strings.Repeat("b", 4500) + ".c"},
		{"no bearer", f.idToken(t, nil)},
		{"empty", ""},
	} {
		a, err := f.e.Authenticate(tt.header)
		if err != ErrUnauthenticated || a.Principal() != "" {
			t.Errorf("%s: Authenticate = %q, %v; want ErrUnauthenticated", tt.name, a.Principal(), err)
			continue
		}
		requireNoTokenText(t, err.Error(), strings.TrimPrefix(tt.header, "Bearer "))
	}
}

func TestOIDCEngineNotConfigured(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	plain, err := newFile(f.auth, installation, f.clock.now)
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]*Engine{"file without OIDC": plain, "legacy": NewLegacy("secret")} {
		if _, err := e.Authenticate("Bearer " + token); err != ErrUnauthenticated {
			t.Errorf("%s: Authenticate = %v, want ErrUnauthenticated", name, err)
		}
	}
}

// TestOIDCEngineCheck changes one input after authentication and checks the
// next Check. ErrUnavailable rows also check that scoped tokens still work.
func TestOIDCEngineCheck(t *testing.T) {
	rsa1 := func() map[string]any { return rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}) }
	ec1 := func() map[string]any { return ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}) }
	tests := []struct {
		name   string
		change func(t *testing.T, f *oidcEngineFixture)
		want   error
	}{
		{"unchanged", func(t *testing.T, f *oidcEngineFixture) {}, nil},
		{"mapping added", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"identities": []map[string]string{{"subject": "alice-subject", "principal": "alice"}, {"subject": "alice-laptop", "principal": "alice"}}})
		}, nil},
		{"key added", func(t *testing.T, f *oidcEngineFixture) {
			f.writeKeys(t, rsa1(), ec1(), rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "rsa-2"}))
		}, nil},
		{"jwks file absolute", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"jwks_file": f.keys})
		}, nil},
		{"before expiry plus skew", func(t *testing.T, f *oidcEngineFixture) { f.clock.add(629 * time.Second) }, nil},

		{"mapping removed", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"identities": []map[string]string{{"subject": "bob-subject", "principal": "bob"}}})
		}, ErrUnauthenticated},
		{"mapping moved", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"identities": []map[string]string{{"subject": "alice-subject", "principal": "bob"}}})
		}, ErrUnauthenticated},
		{"key removed", func(t *testing.T, f *oidcEngineFixture) { f.writeKeys(t, ec1()) }, ErrUnauthenticated},
		{"key replaced under same kid", func(t *testing.T, f *oidcEngineFixture) {
			f.writeKeys(t, rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "rsa-1"}), ec1())
		}, ErrUnauthenticated},
		{"principal removed", func(t *testing.T, f *oidcEngineFixture) {
			writeFixture(t, f.auth, func(c *config) {
				var ps []principal
				for _, p := range c.Principals {
					if p.ID != "alice" {
						ps = append(ps, p)
					}
				}
				var ts []credential
				for _, tok := range c.Tokens {
					if tok.Principal != "alice" {
						ts = append(ts, tok)
					}
				}
				c.Principals, c.Tokens = ps, ts
				c.Generation++
			})
		}, ErrUnauthenticated},
		{"issuer changed", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"issuer": "https://other.example"})
		}, ErrUnauthenticated},
		{"audience changed", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"audience": "other"})
		}, ErrUnauthenticated},
		{"expired", func(t *testing.T, f *oidcEngineFixture) { f.clock.add(630 * time.Second) }, ErrUnauthenticated},

		{"oidc file invalid", func(t *testing.T, f *oidcEngineFixture) { writeFileReplace(t, f.oidc, []byte("{"), 0600) }, ErrUnavailable},
		{"oidc file missing", func(t *testing.T, f *oidcEngineFixture) {
			if err := os.Remove(f.oidc); err != nil {
				t.Fatal(err)
			}
		}, ErrUnavailable},
		{"oidc file group readable", func(t *testing.T, f *oidcEngineFixture) {
			if err := os.Chmod(f.oidc, 0640); err != nil {
				t.Fatal(err)
			}
		}, ErrUnavailable},
		{"oidc generation rollback", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, nil)
			f.refresh(t)
			f.writeOIDC(t, 1, nil)
		}, ErrUnavailable},
		{"oidc same generation changed", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 1, map[string]any{"clock_skew": "10s"})
		}, ErrUnavailable},
		{"jwks uri", func(t *testing.T, f *oidcEngineFixture) {
			f.writeOIDC(t, 2, map[string]any{"jwks_file": nil, "jwks_uri": "https://issuer.example/keys"})
		}, ErrUnavailable},
		{"jwks private member", func(t *testing.T, f *oidcEngineFixture) {
			f.writeKeys(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1", "d": "AAAA"}), ec1())
		}, ErrUnavailable},
		{"jwks oversized", func(t *testing.T, f *oidcEngineFixture) {
			writeFileReplace(t, f.keys, []byte(padJSON(string(jwksJSON(t, rsa1(), ec1())), maxJWKSBytes+1)), 0644)
		}, ErrUnavailable},
		{"jwks symlink", func(t *testing.T, f *oidcEngineFixture) {
			target := filepath.Join(f.dir, "keys-real.json")
			if err := os.Rename(f.keys, target); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, f.keys); err != nil {
				t.Fatal(err)
			}
		}, ErrUnavailable},
		{"jwks missing", func(t *testing.T, f *oidcEngineFixture) {
			if err := os.Remove(f.keys); err != nil {
				t.Fatal(err)
			}
		}, ErrUnavailable},
		{"jwks group writable", func(t *testing.T, f *oidcEngineFixture) {
			writeFileReplace(t, f.keys, jwksJSON(t, rsa1(), ec1()), 0664)
		}, ErrUnavailable},
		{"jwks other writable", func(t *testing.T, f *oidcEngineFixture) {
			writeFileReplace(t, f.keys, jwksJSON(t, rsa1(), ec1()), 0646)
		}, ErrUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newOIDCEngineFixture(t)
			token := f.idToken(t, nil)
			a := f.authenticate(t, token)
			tt.change(t, f)
			f.refresh(t)
			fresh, err := f.e.Check(a, Read)
			if err != tt.want {
				t.Fatalf("Check = %v, want %v", err, tt.want)
			}
			if err == nil && (fresh.Principal() != "alice" || fresh.Role() != Operator) {
				t.Fatalf("Check actor = %q %q", fresh.Principal(), fresh.Role())
			}
			if tt.want != ErrUnavailable {
				return
			}
			if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
				t.Fatalf("Authenticate while OIDC unavailable = %v", err)
			}
			if !f.e.Ready() {
				t.Fatal("Engine not ready while only OIDC is unavailable")
			}
			if _, err := f.e.Check(f.authenticate(t, f.alice), Mutate); err != nil {
				t.Fatalf("scoped token while OIDC unavailable: %v", err)
			}
		})
	}
}

func TestOIDCEngineRoleFromCredentialFile(t *testing.T) {
	f := newOIDCEngineFixture(t)
	a := f.authenticate(t, f.idToken(t, map[string]any{"role": "admin", "groups": []string{"admin"}}))
	if err := SetRole(f.auth, "alice", Reader); err != nil {
		t.Fatal(err)
	}
	f.refresh(t)
	fresh, err := f.e.Check(a, Read)
	if err != nil || fresh.Role() != Reader {
		t.Fatalf("Check(Read) = %q, %v; want reader", fresh.Role(), err)
	}
	if _, err := f.e.Check(a, Mutate); err != ErrPermissionDenied {
		t.Fatalf("Check(Mutate) after demotion = %v", err)
	}
}

// TestOIDCEngineGeneration pins the OIDC file generation contract, which
// compares with the last accepted file: a lower generation or different bytes
// at the same generation conflicts, restoring the accepted bytes clears the
// conflict, and a higher generation is accepted.
func TestOIDCEngineGeneration(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	unavailable := func(step string) {
		t.Helper()
		if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
			t.Fatalf("%s: Authenticate = %v, want ErrUnavailable", step, err)
		}
	}
	f.writeOIDC(t, 2, nil)
	f.refresh(t)
	f.authenticate(t, token)

	f.writeOIDC(t, 1, nil)
	f.refresh(t)
	unavailable("lower generation")
	f.writeOIDC(t, 2, nil) // the accepted bytes
	f.refresh(t)
	f.authenticate(t, token)

	f.writeOIDC(t, 2, map[string]any{"clock_skew": "10s"})
	f.refresh(t)
	unavailable("same generation, different bytes")
	f.refresh(t)
	unavailable("same generation, different bytes, refreshed again")

	f.writeOIDC(t, 3, nil)
	f.refresh(t)
	f.authenticate(t, token)
	f.writeOIDC(t, 2, nil)
	f.refresh(t)
	unavailable("formerly accepted lower generation")
}

// stallOIDCRead makes e's OIDC reads after the first skip block until release
// is called. entered is closed when a read first blocks; reads counts the
// reads started. Cleanup releases any blocked read.
func stallOIDCRead(t *testing.T, e *Engine, skip int64) (entered <-chan struct{}, release func(), reads func() int64) {
	t.Helper()
	read := e.oidcRead
	in, out := make(chan struct{}), make(chan struct{})
	var enter, leave sync.Once
	var n atomic.Int64
	e.oidcRead = func() (*oidcSnapshot, error) {
		if n.Add(1) > skip {
			enter.Do(func() { close(in) })
			<-out
		}
		return read()
	}
	release = func() { leave.Do(func() { close(out) }) }
	t.Cleanup(release)
	return in, release, n.Load
}

// waitAdvance waits up to five seconds for e's credential file refresh time,
// or its OIDC read time if oidc is set, to pass prev, and returns it.
func waitAdvance(t *testing.T, e *Engine, oidc bool, prev time.Time) time.Time {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.RLock()
		got := e.lastRead
		if oidc {
			got = e.oidcLastRead
		}
		e.mu.RUnlock()
		if got.After(prev) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("Run did not refresh within 5s (OIDC: %v)", oidc)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// cleanupT is the part of testing.TB that startOriginal uses. The
// cleanup-order control replaces it to run that cleanup itself.
type cleanupT interface {
	Helper()
	Cleanup(func())
	Errorf(format string, args ...any)
}

// originalJoinDiagnostic is how long an original's cleanup waits before
// reporting the original still pending. It is only a diagnostic: the cleanup
// then keeps waiting for the same original.
var originalJoinDiagnostic = 10 * time.Second

// startOriginal runs run, the original, on a goroutine and returns the
// cancel of run's context and the original's done channel. Its cleanup is
// registered before the original starts; it calls release, if not nil,
// cancels run's context and then waits for that same original to return,
// without limit. If the original is still pending originalJoinDiagnostic
// after the cleanup began, the cleanup reports it and keeps waiting. So no
// cleanup registered earlier, such as a seam reset or the removal of a
// fixture, runs while the original lives, even after a failed assertion. An
// original that never returns leaves the test to the process timeout with
// its fixture and seams in place.
func startOriginal(t cleanupT, release func(), run func(context.Context)) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		if release != nil {
			release()
		}
		cancel()
		select {
		case <-done:
			return
		case <-time.After(originalJoinDiagnostic):
			t.Errorf("original did not return within %v of cleanup; still waiting for it", originalJoinDiagnostic)
		}
		<-done
	})
	go func() {
		defer close(done)
		run(ctx)
	}()
	return cancel, done
}

// startRun runs the original e.Run with startOriginal, whose cleanup
// releases any blocked OIDC read, cancels Run and joins that same Run before
// any earlier cleanup runs.
func startRun(t *testing.T, e *Engine, release func()) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	return startOriginal(t, release, func(ctx context.Context) { e.Run(ctx) })
}

// recordCleanup is a cleanupT that records cleanups and diagnostics, so a
// control can run the cleanups, last registered first as testing does, and
// observe them.
type recordCleanup struct {
	mu       sync.Mutex
	cleanups []func()
	errs     []string
}

func (*recordCleanup) Helper() {}

func (c *recordCleanup) Cleanup(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups = append(c.cleanups, f)
}

func (c *recordCleanup) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

func (c *recordCleanup) run() {
	c.mu.Lock()
	cleanups := c.cleanups
	c.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func (c *recordCleanup) diagnostics() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.errs)
}

// TestStartOriginalCleanupOrder runs startOriginal's cleanup on a MODELED
// original. Two cleanups registered before it stand for a seam reset and the
// removal of a fixture. In the held case, once its first hold is released
// and its context cancelled, the original waits in a second hold that only
// the control releases: past the diagnostic, which is reported once, neither
// earlier cleanup has run and the cleanup has not finished; after the
// control releases the second hold, the same original returns and each
// earlier cleanup then runs once. In the clean case the cleanup joins the
// original with no diagnostic and each earlier cleanup runs once after it.
func TestStartOriginalCleanupOrder(t *testing.T) {
	diag := originalJoinDiagnostic
	t.Cleanup(func() { originalJoinDiagnostic = diag })
	originalJoinDiagnostic = 50 * time.Millisecond
	for _, held := range []bool{false, true} {
		name := "clean"
		if held {
			name = "second hold"
		}
		t.Run(name, func(t *testing.T) {
			c := new(recordCleanup)
			var returned, reset, removed, early atomic.Int32
			c.Cleanup(func() {
				if returned.Load() == 0 {
					early.Add(1)
				}
				removed.Add(1)
			})
			c.Cleanup(func() {
				if returned.Load() == 0 {
					early.Add(1)
				}
				reset.Add(1)
			})
			first, second, inSecond := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var firstOnce, secondOnce sync.Once
			release := func() { firstOnce.Do(func() { close(first) }) }
			releaseSecond := func() { secondOnce.Do(func() { close(second) }) }
			if !held {
				releaseSecond()
			}
			// Registered before anything starts: on every path the control
			// releases both holds, then joins the cleanup it runs and the
			// same original.
			var done <-chan struct{}
			cleaned := make(chan struct{})
			started := false
			t.Cleanup(func() {
				release()
				releaseSecond()
				if started {
					<-cleaned
				}
				if done != nil {
					<-done
				}
			})
			_, done = startOriginal(c, release, func(ctx context.Context) {
				defer returned.Add(1)
				<-first
				<-ctx.Done()
				close(inSecond)
				<-second
			})
			started = true
			go func() {
				defer close(cleaned)
				c.run()
			}()
			if held {
				select {
				case <-inSecond:
				case <-time.After(10 * time.Second):
					t.Fatal("cleanup did not release the first hold and cancel the original within 10s")
				}
				time.Sleep(10 * originalJoinDiagnostic)
				select {
				case <-cleaned:
					t.Fatal("cleanup finished while the original was held")
				case <-done:
					t.Fatal("the original returned while held")
				default:
				}
				if n := c.diagnostics(); n != 1 || reset.Load() != 0 || removed.Load() != 0 {
					t.Fatalf("while held: %d diagnostics, %d seam resets, %d removals; want 1, 0, 0", n, reset.Load(), removed.Load())
				}
				releaseSecond()
			}
			select {
			case <-cleaned:
			case <-time.After(10 * time.Second):
				t.Fatal("cleanup did not finish within 10s of the original's release")
			}
			select {
			case <-done:
			default:
				t.Fatal("cleanup finished before the original returned")
			}
			want := 0
			if held {
				want = 1
			}
			if n := c.diagnostics(); n != want || returned.Load() != 1 || reset.Load() != 1 || removed.Load() != 1 || early.Load() != 0 {
				t.Fatalf("%d diagnostics, %d returns, %d seam resets, %d removals, %d before the original returned; want %d, 1, 1, 1, 0", n, returned.Load(), reset.Load(), removed.Load(), early.Load(), want)
			}
		})
	}
}

// TestOIDCEngineStalledRead blocks an OIDC read, as a FIFO or hung mount
// would, and checks that scoped tokens stay available across more than the
// two-second envelope while OIDC goes stale.
func TestOIDCEngineStalledRead(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	a := f.authenticate(t, token)
	entered, release, _ := stallOIDCRead(t, f.e, 0)
	_, stalled := startOriginal(t, release, func(context.Context) { f.e.refreshOIDC() })
	<-entered
	for i := 0; i < 3; i++ {
		f.clock.add(time.Second)
		refreshed := make(chan error, 1)
		go func() { refreshed <- f.e.refresh() }()
		select {
		case err := <-refreshed:
			if err != nil {
				t.Fatalf("scoped credential refresh: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("credential file refresh waited for a stalled OIDC read")
		}
	}
	if !f.e.Ready() {
		t.Fatal("Engine not ready while only an OIDC read is stalled")
	}
	if _, err := f.e.Check(f.authenticate(t, f.alice), Mutate); err != nil {
		t.Fatalf("scoped token while an OIDC read is stalled: %v", err)
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate with a stale OIDC snapshot = %v, want ErrUnavailable", err)
	}
	if _, err := f.e.Check(a, Read); err != ErrUnavailable {
		t.Fatalf("Check with a stale OIDC snapshot = %v, want ErrUnavailable", err)
	}
	release()
	<-stalled
	// The released read began before the clock moved, so its snapshot is
	// already stale; the next read decides.
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate after a stalled read returned = %v, want ErrUnavailable", err)
	}
	f.refresh(t)
	f.authenticate(t, token)
}

// TestOIDCEngineRunStalledRead runs the original Engine.Run on the real clock
// and never refreshes by hand. The first OIDC read succeeds and the second
// blocks, as a FIFO or hung mount would. Across at least three credential file
// refreshes and more than two seconds, scoped tokens stay available while
// OIDC goes stale. After cancellation the blocked read is retained and Run
// does not return; once the read is released, Run returns.
func TestOIDCEngineRunStalledRead(t *testing.T) {
	f := newOIDCEngineFixture(t)
	e, err := newFileOIDC(f.auth, installation, f.oidc, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	token := f.idToken(t, nil)
	entered, release, reads := stallOIDCRead(t, e, 1)
	cancel, done := startRun(t, e, release)

	var a Actor
	deadline := time.Now().Add(10 * time.Second)
	for {
		if a, err = e.Authenticate("Bearer " + token); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("OIDC not available within 10s of Run starting: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("Run's OIDC reader started no second read within 10s")
	}
	blocked := time.Now()
	e.mu.RLock()
	last := e.lastRead
	e.mu.RUnlock()
	for n := 0; n < 3 || time.Since(blocked) < 2500*time.Millisecond; n++ {
		last = waitAdvance(t, e, false, last)
	}

	if !e.Ready() {
		t.Fatal("Engine not ready while only an OIDC read is blocked")
	}
	scoped, err := e.Authenticate("Bearer " + f.alice)
	if err != nil {
		t.Fatalf("scoped Authenticate while an OIDC read is blocked: %v", err)
	}
	if _, err := e.Check(scoped, Mutate); err != nil {
		t.Fatalf("scoped Check while an OIDC read is blocked: %v", err)
	}
	if _, err := e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("OIDC Authenticate with a stale snapshot = %v, want ErrUnavailable", err)
	}
	if _, err := e.Check(a, Read); err != ErrUnavailable {
		t.Fatalf("OIDC Check with a stale snapshot = %v, want ErrUnavailable", err)
	}
	if n := reads(); n != 2 {
		t.Fatalf("%d OIDC reads started, want 2: one done and one pending", n)
	}

	cancel()
	select {
	case <-done:
		t.Fatal("Run returned while its OIDC read was still pending")
	case <-time.After(1500 * time.Millisecond):
	}
	release()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s of its pending OIDC read being released")
	}
	if n := reads(); n != 2 {
		t.Fatalf("%d OIDC reads in all, want 2: a read started after cancellation", n)
	}
}

// TestOIDCEngineRunRefreshes runs the original Engine.Run on the real clock
// and checks that its OIDC reader keeps reading after the first read, so OIDC
// stays available across at least three reads and more than two seconds.
func TestOIDCEngineRunRefreshes(t *testing.T) {
	f := newOIDCEngineFixture(t)
	e, err := newFileOIDC(f.auth, installation, f.oidc, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	token := f.idToken(t, nil)
	_, _, reads := stallOIDCRead(t, e, 1<<62)
	startRun(t, e, nil)
	start := time.Now()
	var last time.Time
	for n := 0; n < 3 || time.Since(start) < 2500*time.Millisecond; n++ {
		last = waitAdvance(t, e, true, last)
		if _, err := e.Authenticate("Bearer " + token); err != nil {
			t.Fatalf("OIDC Authenticate while Run reads: %v", err)
		}
	}
	if n := reads(); n < 3 {
		t.Fatalf("%d OIDC reads, want at least 3", n)
	}
}

// TestOIDCEngineSlowRead checks that freshness dates from the start of a
// read: a read that outlasts the envelope publishes an already stale
// snapshot, and the next read decides.
func TestOIDCEngineSlowRead(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	read := f.e.oidcRead
	f.e.oidcRead = func() (*oidcSnapshot, error) {
		s, err := read()
		f.clock.add(3 * time.Second)
		return s, err
	}
	f.e.refreshOIDC()
	if err := f.e.refresh(); err != nil {
		t.Fatalf("scoped credential refresh: %v", err)
	}
	if !f.e.Ready() {
		t.Fatal("Engine not ready after a credential file refresh")
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate after a read slower than the envelope = %v, want ErrUnavailable", err)
	}
	f.e.oidcRead = read
	f.refresh(t)
	f.authenticate(t, token)
}

// TestOIDCEngineConstructionReadsNoOIDC checks that construction does no
// OIDC file IO, so a blocked OIDC file cannot delay startup.
func TestOIDCEngineConstructionReadsNoOIDC(t *testing.T) {
	f := newOIDCEngineFixture(t)
	e, err := newFileOIDC(f.auth, installation, f.oidc, f.clock.now)
	if err != nil {
		t.Fatal(err)
	}
	token := f.idToken(t, nil)
	if _, err := e.Authenticate("Bearer " + token); err != ErrUnavailable {
		t.Fatalf("Authenticate before the first OIDC read = %v, want ErrUnavailable", err)
	}
	e.refreshOIDC()
	if _, err := e.Authenticate("Bearer " + token); err != nil {
		t.Fatalf("Authenticate after the first OIDC read: %v", err)
	}
}

// TestOIDCEngineJWKSHasNoGeneration records what the JWKS file cannot show:
// restoring an older valid key set is accepted like a rotation. Only key
// presence at each Check is enforced.
func TestOIDCEngineJWKSHasNoGeneration(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	a := f.authenticate(t, token)
	f.writeKeys(t, ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}))
	f.refresh(t)
	if _, err := f.e.Check(a, Read); err != ErrUnauthenticated {
		t.Fatalf("Check after key removal = %v, want ErrUnauthenticated", err)
	}
	if _, err := f.e.Authenticate("Bearer " + token); err != ErrUnauthenticated {
		t.Fatalf("Authenticate after key removal = %v, want ErrUnauthenticated", err)
	}
	f.writeKeys(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}), ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}))
	f.refresh(t)
	f.authenticate(t, token)
}

func TestOIDCEngineChanged(t *testing.T) {
	f := newOIDCEngineFixture(t)
	closed := func(ch <-chan struct{}) bool {
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}
	ch := f.e.Changed()
	f.refresh(t)
	if closed(ch) {
		t.Fatal("Changed closed without a change")
	}
	f.writeKeys(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}))
	f.refresh(t)
	if !closed(ch) {
		t.Fatal("Changed not closed after key rotation")
	}
	ch = f.e.Changed()
	writeFileReplace(t, f.oidc, []byte("{"), 0600)
	f.refresh(t)
	if !closed(ch) {
		t.Fatal("Changed not closed when OIDC became unavailable")
	}
}

// TestOIDCEngineStreamRevocation runs a watchStream-style loop, which rechecks
// on each Changed notification, and removes the mapping.
func TestOIDCEngineStreamRevocation(t *testing.T) {
	f := newOIDCEngineFixture(t)
	a := f.authenticate(t, f.idToken(t, nil))
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		first := true
		for {
			changed := f.e.Changed()
			if _, err := f.e.Check(a, Read); err != nil {
				done <- err
				return
			}
			if first {
				close(ready)
				first = false
			}
			select {
			case <-changed:
			case <-time.After(10 * time.Second):
				done <- errors.New("no change notification")
				return
			}
		}
	}()
	<-ready
	f.writeOIDC(t, 2, map[string]any{"identities": []map[string]string{{"subject": "bob-subject", "principal": "bob"}}})
	f.refresh(t)
	if err := <-done; err != ErrUnauthenticated {
		t.Fatalf("stream ended with %v, want ErrUnauthenticated", err)
	}
}

func TestOIDCEngineConcurrent(t *testing.T) {
	f := newOIDCEngineFixture(t)
	token := f.idToken(t, nil)
	sets := [][]map[string]any{
		{rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"})},
		{rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}), ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"})},
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				a, err := f.e.Authenticate("Bearer " + token)
				if err == nil {
					_, err = f.e.Check(a, Read)
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		f.writeKeys(t, sets[i%2]...)
		f.refresh(t)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
