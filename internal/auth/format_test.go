package auth

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// priorFixture copies credentials issued by the frozen pre-extension package.
// Use a private copy: checked-in fixtures are test credentials, not authority.
func priorFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile("testdata/prior-auth.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return fixture{path, readToken(t, "testdata/prior.token")}
}

func rejectCredential(t *testing.T, e *Engine, token string) {
	t.Helper()
	_, err := e.Authenticate("Bearer " + token)
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("invalid credential did not receive the generic rejection")
	}
	if strings.Contains(err.Error(), token) || len(token) >= 43 && strings.Contains(err.Error(), token[len(token)-43:]) {
		t.Fatal("credential rejection exposed token bytes")
	}
}

func TestPriorCredentialPreserved(t *testing.T) {
	f := priorFixture(t)
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	c, _, _, err := readConfig(f.path)
	if err != nil || len(c.Tokens) != 1 {
		t.Fatal("invalid prior credential fixture")
	}
	now := c.Tokens[0].CreatedAt.Add(time.Second)
	e, err := newFile(f.path, installation, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	a := requireActor(t, e, f.initial)
	if a.TokenID() != c.Tokens[0].ID || a.Principal() != "legacy" || a.Role() != Admin {
		t.Fatal("prior credential identity changed")
	}
	after, err := os.ReadFile(f.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("opening prior authority rewrote its contents")
	}
	// A valid new spelling is not an alias for the old credential's digest.
	swapped := "sky_" + strings.TrimPrefix(f.initial, "skygo_")
	rejectCredential(t, e, swapped)
}

func TestMixedCredentialFormats(t *testing.T) {
	f := priorFixture(t)
	// Keep this genuine prior credential's digest while moving its validity
	// window to the test's clock, so the test does not expire with the fixture.
	writeFixture(t, f.path, func(c *config) {
		c.Tokens[0].CreatedAt = time.Now().UTC().Add(-time.Minute)
		c.Tokens[0].ExpiresAt = c.Tokens[0].CreatedAt.Add(time.Hour)
	})
	e, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	old := requireActor(t, e, f.initial)
	prior, _, _, err := readConfig(f.path)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(filepath.Dir(f.path), "replacement.token")
	info, err := Issue(f.path, "legacy", output, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token := readToken(t, output)
	if !strings.HasPrefix(token, "sky_") || len(token) != 80 {
		t.Fatal("new issuance is not SDK-compatible")
	}
	if err := e.refresh(); err != nil {
		t.Fatal(err)
	}
	fresh := requireActor(t, e, token)
	if fresh.Principal() != old.Principal() || fresh.TokenID() == old.TokenID() || fresh.TokenID() != info.ID {
		t.Fatal("rotation changed principal or reused credential identity")
	}
	if _, err := e.Check(old, Mutate); err != nil {
		t.Fatal("issuance invalidated prior credential", err)
	}
	next, _, _, err := readConfig(f.path)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, credential := range next.Tokens {
		if credential.ID == old.TokenID() {
			found = true
			if credential.Hash != prior.Tokens[0].Hash {
				t.Fatal("issuance migrated prior digest")
			}
		}
	}
	if !found {
		t.Fatal("issuance removed prior credential")
	}
	swapped := "skygo_" + strings.TrimPrefix(token, "sky_")
	rejectCredential(t, e, swapped)
	// Restart reads the mixed authority without a format marker or migration.
	restarted, err := NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	requireActor(t, restarted, f.initial)
	requireActor(t, restarted, token)
	if err := Revoke(f.path, old.TokenID()); err != nil {
		t.Fatal(err)
	}
	if err := e.refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Check(old, Read); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("prior credential survived revocation")
	}
	if _, err := e.Check(fresh, Mutate); err != nil {
		t.Fatal("prior revocation invalidated replacement", err)
	}
	restarted, err = NewFile(f.path, installation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Authenticate("Bearer " + f.initial); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("restart revived revoked credential")
	}
	requireActor(t, restarted, token)
}

func TestCredentialFormatExpiry(t *testing.T) {
	for _, name := range []string{"prior", "new"} {
		t.Run(name, func(t *testing.T) {
			var f fixture
			if name == "prior" {
				f = priorFixture(t)
			} else {
				f = newFixture(t)
			}
			c, _, _, err := readConfig(f.path)
			if err != nil {
				t.Fatal(err)
			}
			now := c.Tokens[0].CreatedAt.Add(time.Second)
			e, err := newFile(f.path, installation, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			a := requireActor(t, e, f.initial)
			now = c.Tokens[0].ExpiresAt
			if err := e.refresh(); err != nil {
				t.Fatal(err)
			}
			if _, err := e.Check(a, Read); !errors.Is(err, ErrUnauthenticated) {
				t.Fatal("expired credential retained authority")
			}
			if _, err := e.Authenticate("Bearer " + f.initial); !errors.Is(err, ErrUnauthenticated) {
				t.Fatal("expired credential authenticated")
			}
		})
	}
}

func TestTokenFormatParsing(t *testing.T) {
	id := "0123456789abcdef0123456789abcdef"
	secret := strings.Repeat("A", 43) // canonical encoding of 32 zero test bytes
	for _, prefix := range []string{"sky_", "skygo_"} {
		t.Run(prefix, func(t *testing.T) {
			token := prefix + id + "_" + secret
			if got, ok := tokenID(token); !ok || got != id {
				t.Fatal("valid token spelling refused or ID changed")
			}
			for _, malformed := range []string{
				token[:len(token)-1], token + "A", token + "\n", " " + token,
				strings.ToUpper(prefix) + id + "_" + secret,
				prefix + strings.ToUpper(id) + "_" + secret,
				prefix + id + "-" + secret,
				prefix + id + "_" + secret[:42] + "!",
				prefix + id + "_" + secret[:42] + "B", // noncanonical trailing bits
				prefix + id + "_" + secret[:42] + "=",
				prefix + "sky_" + id + "_" + secret,
			} {
				if _, ok := tokenID(malformed); ok {
					t.Fatal("malformed credential accepted")
				}
			}
		})
	}
}
