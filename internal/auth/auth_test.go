package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const installation = "0123456789abcdef0123456789abcdef"

type fixture struct{ path, initial string }

func newFixture(t *testing.T) fixture {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	token := filepath.Join(dir, "legacy.token")
	if _, err := Initialize(path, token, installation); err != nil {
		t.Fatal(err)
	}
	return fixture{path, readToken(t, token)}
}
func readToken(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(string(b), "\n")
}
func (f fixture) issue(t *testing.T, id string, role Role) (string, TokenInfo) {
	t.Helper()
	if err := AddPrincipal(f.path, id, role); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(filepath.Dir(f.path), id+".token")
	info, err := Issue(f.path, id, out, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return readToken(t, out), info
}
func requireActor(t *testing.T, e *Engine, token string) Actor {
	t.Helper()
	a, err := e.Authenticate("Bearer " + token)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func writeFixture(t *testing.T, path string, change func(*config)) {
	t.Helper()
	c, _, _, err := readConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	change(c)
	data, err := encode(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyAndOpaqueAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, configured, header string
		want                     bool
	}{
		{"anonymous", "", "", true}, {"token", "secret", "Bearer secret", true}, {"missing", "secret", "", false}, {"wrong", "secret", "Bearer other", false}, {"prefix", "secret", "Basic secret", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewLegacy(tc.configured)
			a, err := e.Authenticate(tc.header)
			if (err == nil) != tc.want {
				t.Fatalf("authenticate: %v", err)
			}
			if !tc.want {
				return
			}
			if a.Principal() != "legacy" || a.Role() != Admin || a.TokenID() != "" {
				t.Fatal("legacy identity")
			}
			if _, err = e.Check(a, Mutate); err != nil {
				t.Fatal(err)
			}
			if _, err = e.Check(a, 0); !errors.Is(err, ErrPermissionDenied) {
				t.Fatal(err)
			}
			if _, err = e.Check(Actor{}, Read); !errors.Is(err, ErrUnauthenticated) {
				t.Fatal(err)
			}
			if _, err = NewLegacy(tc.configured).Check(a, Read); !errors.Is(err, ErrUnauthenticated) {
				t.Fatal("foreign engine", err)
			}
		})
	}
	var zero Engine
	if zero.Ready() {
		t.Fatal("zero engine ready")
	}
	if _, err := zero.Authenticate(""); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestScopedAuthorityRotationRevocationAndDemotion(t *testing.T) {
	f := newFixture(t)
	token, info := f.issue(t, "alice", Operator)
	reader, _ := f.issue(t, "bob", Reader)
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	a := requireActor(t, e, token)
	if a.Principal() != "alice" || a.Role() != Operator || a.TokenID() != info.ID {
		t.Fatal("identity")
	}
	if _, err = e.Check(a, Mutate); err != nil {
		t.Fatal(err)
	}
	b := requireActor(t, e, reader)
	if _, err = e.Check(b, Mutate); !errors.Is(err, ErrPermissionDenied) {
		t.Fatal(err)
	}
	out := filepath.Join(filepath.Dir(f.path), "rotated.token")
	rotated, err := Issue(f.path, "alice", out, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	changed := e.Changed()
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("missing notification")
	}
	next := requireActor(t, e, readToken(t, out))
	if next.Principal() != a.Principal() || next.TokenID() == a.TokenID() || next.TokenID() != rotated.ID {
		t.Fatal("rotation changed namespace")
	}
	if err = SetRole(f.path, "alice", Reader); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	fresh, err := e.Check(a, Read)
	if err != nil || fresh.Role() != Reader {
		t.Fatalf("fresh role: %v %v", fresh.Role(), err)
	}
	if _, err = e.Check(a, Mutate); !errors.Is(err, ErrPermissionDenied) {
		t.Fatal(err)
	}
	if err = Revoke(f.path, info.ID); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Check(a, Read); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
	if _, err = e.Authenticate("Bearer " + token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
	requireActor(t, e, readToken(t, out))
}

func TestExpiryFreshnessAndInvalidFileRecovery(t *testing.T) {
	f := newFixture(t)
	token, info := f.issue(t, "alice", Operator)
	var clock atomic.Int64
	clock.Store(info.CreatedAt.UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	e, err := newFile(f.path, installation, now)
	if err != nil {
		t.Fatal(err)
	}
	a := requireActor(t, e, token)
	clock.Add(int64(3 * time.Second))
	if e.Ready() {
		t.Fatal("stale refresh ready")
	}
	if _, err = e.Check(a, Read); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	if !e.Ready() {
		t.Fatal("refresh failed")
	}
	clock.Store(info.ExpiresAt.UnixNano())
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Check(a, Read); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("expiry", err)
	}
	old, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	changed := e.Changed()
	if err = os.WriteFile(f.path, []byte(`{"do-not-echo-secret":`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("missing failure notification")
	}
	if e.Ready() {
		t.Fatal("invalid config fallback")
	}
	if _, err = e.Authenticate("Bearer " + f.initial); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if err = os.WriteFile(f.path, old, 0600); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	if !e.Ready() {
		t.Fatal("not recovered")
	}
	if err = os.Remove(f.path); err != nil {
		t.Fatal(err)
	}
	if e.refresh() == nil || e.Ready() {
		t.Fatal("missing file fallback")
	}
}

func TestGenerationAndCredentialIdentity(t *testing.T) {
	f := newFixture(t)
	token, _ := f.issue(t, "alice", Operator)
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	a := requireActor(t, e, token)
	original, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, f.path, func(c *config) { c.Principals[0].Role = Reader })
	if err = e.refresh(); !errors.Is(err, ErrConflict) || e.Ready() {
		t.Fatalf("same generation rewrite: %v", err)
	}
	if err = os.WriteFile(f.path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	writeFixture(t, f.path, func(c *config) {
		c.Generation++
		for i := range c.Tokens {
			if c.Tokens[i].ID == a.TokenID() {
				c.Tokens[i].Hash = strings.Repeat("a", 64)
			}
		}
	})
	if err = e.refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err = e.Check(a, Read); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("replaced credential accepted", err)
	}
	if err = os.WriteFile(f.path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err = e.refresh(); !errors.Is(err, ErrConflict) {
		t.Fatal("generation rollback", err)
	}
	if _, err = NewFile(f.path, strings.Repeat("b", 32)); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong installation", err)
	}
}

func TestStrictDecodeNoSecretEcho(t *testing.T) {
	f := newFixture(t)
	good, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"unknown", append([]byte(`{"recognizable-bearer-secret":true,`), good[1:]...)},
		{"syntax", []byte(`{"recognizable-bearer-secret":`)},
		{"duplicate", append([]byte(`{"version":1,`), good[1:]...)},
		{"trailing", append(append([]byte(nil), good...), []byte(`{"recognizable-bearer-secret":1}`)...)},
		{"deep", []byte(strings.Repeat("[", 70) + "0" + strings.Repeat("]", 70))},
		{"large", []byte(strings.Repeat("x", maxConfigBytes+1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(f.path, tc.data, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := NewFile(f.path, installation)
			if !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
			if strings.Contains(err.Error(), "recognizable-bearer-secret") {
				t.Fatal("secret echo")
			}
		})
	}
	if err = os.WriteFile(f.path, good, 0600); err != nil {
		t.Fatal(err)
	}
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"recognizable-bearer-secret", "Bearer recognizable-bearer-secret", strings.Repeat("recognizable-bearer-secret", 1024)} {
		_, err = e.Authenticate(header)
		if !errors.Is(err, ErrUnauthenticated) || strings.Contains(err.Error(), "recognizable") {
			t.Fatal(err)
		}
	}
}

func TestLastUsableAdminAndOutputConflicts(t *testing.T) {
	f := newFixture(t)
	list, err := Tokens(f.path)
	if err != nil {
		t.Fatal(err)
	}
	id := list[0].ID
	if err = Revoke(f.path, id); !errors.Is(err, ErrConflict) {
		t.Fatal("last admin revoked", err)
	}
	if err = SetRole(f.path, "legacy", Reader); !errors.Is(err, ErrConflict) {
		t.Fatal("last admin demoted", err)
	}
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Issue(f.path, "legacy", f.path, time.Hour); !errors.Is(err, ErrInvalid) {
		t.Fatal("same path", err)
	}
	existing := filepath.Join(filepath.Dir(f.path), "existing")
	if err = os.WriteFile(existing, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Issue(f.path, "legacy", existing, time.Hour); !errors.Is(err, fs.ErrExist) {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(f.path)
	if string(before) != string(after) {
		t.Fatal("conflict changed config")
	}
	b, _ := os.ReadFile(existing)
	if string(b) != "keep" {
		t.Fatal("overwrote file")
	}
	// An expired admin record is not a usable fallback for revocation/demotion.
	writeFixture(t, f.path, func(c *config) {
		for i := range c.Tokens {
			c.Tokens[i].CreatedAt = time.Now().Add(-2 * time.Hour)
			c.Tokens[i].ExpiresAt = time.Now().Add(-time.Hour)
		}
	})
	if err = SetRole(f.path, "legacy", Reader); !errors.Is(err, ErrConflict) {
		t.Fatal("expired admin counted", err)
	}
	replacement := filepath.Join(filepath.Dir(f.path), "recovery.token")
	if _, err = Issue(f.path, "legacy", replacement, time.Hour); err != nil {
		t.Fatal("operator recovery", err)
	}
}

func TestTokenPropertiesLimitsAndConcurrentMutation(t *testing.T) {
	f := newFixture(t)
	if !strings.HasPrefix(f.initial, "sky_") || len(f.initial) != 80 {
		t.Fatal("token format")
	}
	data, _ := os.ReadFile(f.path)
	if strings.Contains(string(data), f.initial) {
		t.Fatal("raw token stored")
	}
	for _, name := range []string{"alice", "bob"} {
		if err := AddPrincipal(f.path, name, Operator); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := Issue(f.path, "alice", filepath.Join(filepath.Dir(f.path), fmt.Sprintf("token-%d", i)), time.Hour)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	list, err := Tokens(f.path)
	if err != nil || len(list) != 9 {
		t.Fatalf("lost update: %d %v", len(list), err)
	}
	for i := 8; i < 16; i++ {
		if _, err = Issue(f.path, "alice", filepath.Join(filepath.Dir(f.path), fmt.Sprintf("token-%d", i)), time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = Issue(f.path, "alice", filepath.Join(filepath.Dir(f.path), "too-many"), time.Hour); !errors.Is(err, ErrConflict) {
		t.Fatal("credential cap", err)
	}
	for _, d := range []time.Duration{0, -1, maxLifetime + 1} {
		if _, err = Issue(f.path, "bob", filepath.Join(filepath.Dir(f.path), "bad-lifetime"), d); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"legacy", "Bad", "-bad", "bad_secret"} {
		if err = AddPrincipal(f.path, id, Reader); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
}

func TestPrivateFilesSymlinksAndCleanupOwnership(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth")
	output := filepath.Join(dir, "token")
	cleanup, err := Initialize(path, output, installation)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{path, output} {
		s, err := os.Stat(p)
		if err != nil || s.Mode().Perm() != 0600 {
			t.Fatal("private mode", err)
		}
	}
	if err = os.Rename(output, output+".saved"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, []byte("other owner"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = cleanup(); !errors.Is(err, ErrConflict) {
		t.Fatal("replacement removed", err)
	}
	b, _ := os.ReadFile(output)
	if string(b) != "other owner" {
		t.Fatal("replaced file deleted")
	}
	f := newFixture(t)
	link := filepath.Join(filepath.Dir(f.path), "link")
	if err = os.Symlink(f.path, link); err != nil {
		t.Fatal(err)
	}
	if _, err = NewFile(link, installation); !errors.Is(err, ErrInvalid) {
		t.Fatal("config symlink", err)
	}
	if err = os.Chmod(f.path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = NewFile(f.path, installation); !errors.Is(err, ErrInvalid) {
		t.Fatal("public config", err)
	}
	missingOutput := filepath.Join(dir, "unused")
	if _, err = Initialize(output, missingOutput, installation); !errors.Is(err, fs.ErrExist) {
		t.Fatal(err)
	}
	if _, err = os.Stat(missingOutput); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("partial init leaked", err)
	}
}

func TestBoundedFileLockAndRefreshRun(t *testing.T) {
	f := newFixture(t)
	if err := os.Mkdir(f.path+".lock", 0700); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := AddPrincipal(f.path, "blocked", Reader); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("lock timeout exceeded")
	}
	if err := os.Remove(f.path + ".lock"); err != nil {
		t.Fatal(err)
	}
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); e.Run(ctx) }()
	defer func() { cancel(); <-done }()
	a := requireActor(t, e, f.initial)
	changed := e.Changed()
	out := filepath.Join(filepath.Dir(f.path), "new.token")
	if _, err = Issue(f.path, "legacy", out, time.Hour); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh did not signal")
	}
	if _, err = e.Check(a, Read); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				_, _ = e.Authenticate("Bearer " + f.initial)
				_, _ = e.Check(a, Read)
				_ = e.Ready()
				_ = e.Changed()
			}
		}()
	}
	wg.Wait()
}

func TestConfigValidation(t *testing.T) {
	f := newFixture(t)
	data, _ := os.ReadFile(f.path)
	for _, tc := range []struct {
		name   string
		change func(*config)
	}{
		{"version", func(c *config) { c.Version = 2 }}, {"duplicate principal", func(c *config) { c.Principals = append(c.Principals, c.Principals[0]) }},
		{"duplicate token", func(c *config) { c.Tokens = append(c.Tokens, c.Tokens[0]) }}, {"dangling token", func(c *config) { c.Tokens[0].Principal = "missing" }},
		{"invalid role", func(c *config) { c.Principals[0].Role = "owner" }}, {"bad hash", func(c *config) { c.Tokens[0].Hash = strings.Repeat("X", 64) }},
		{"long expiry", func(c *config) { c.Tokens[0].ExpiresAt = c.Tokens[0].CreatedAt.Add(maxLifetime + 1) }}, {"generation", func(c *config) { c.Generation = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c config
			if err := json.Unmarshal(data, &c); err != nil {
				t.Fatal(err)
			}
			tc.change(&c)
			b, _ := json.Marshal(c)
			if err := os.WriteFile(f.path, b, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewFile(f.path, installation); !errors.Is(err, ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
}
