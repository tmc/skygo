package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseOIDCConfigFile(t *testing.T) {
	data, err := os.ReadFile("testdata/oidc/config.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := parseOIDCConfig(data, installation)
	if err != nil {
		t.Fatal(err)
	}
	want := &oidcConfig{
		installation: installation,
		generation:   3,
		issuer:       "https://issuer.example/dex",
		audience:     "skygo",
		jwksURI:      "https://issuer.example/dex/keys",
		caFile:       "issuer-ca.pem",
		maxLifetime:  time.Hour,
		skew:         30 * time.Second,
		identities:   map[string]string{"alice-subject": "alice", "alice-laptop": "alice", "bob-subject": "bob", "carol-subject": "carol"},
	}
	if fmt.Sprint(c) != fmt.Sprint(want) {
		t.Fatalf("config = %+v\nwant %+v", c, want)
	}
	if _, err := parseOIDCConfig(data, "fedcba9876543210fedcba9876543210"); err != ErrInvalid {
		t.Fatalf("other installation: %v", err)
	}
	if _, err := parseOIDCConfig(data, "not-hex"); err != ErrInvalid {
		t.Fatalf("invalid installation: %v", err)
	}
}

// oidcConfigJSON renders a valid minimal configuration with changes applied;
// a nil value deletes a member.
func oidcConfigJSON(t *testing.T, change map[string]any) string {
	t.Helper()
	m := map[string]any{
		"version":      1,
		"installation": installation,
		"generation":   1,
		"issuer":       "https://issuer.example",
		"audience":     "skygo",
		"jwks_file":    "keys.json",
		"identities":   []map[string]string{{"subject": "s", "principal": "alice"}},
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
	return string(data)
}

// padJSON appends JSON whitespace to make doc exactly n bytes long, so that
// only a byte bound can reject it.
func padJSON(doc string, n int) string { return doc + strings.Repeat(" ", n-len(doc)) }

func TestParseOIDCConfig(t *testing.T) {
	identities := func(n int) []map[string]string {
		var out []map[string]string
		for i := 0; i < n; i++ {
			out = append(out, map[string]string{"subject": fmt.Sprint("s", i), "principal": "alice"})
		}
		return out
	}
	tests := []struct {
		name  string
		data  string
		valid bool
	}{
		{"minimal", oidcConfigJSON(t, nil), true},
		{"jwks uri with query", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "https://issuer.example/keys?v=1"}), true},
		{"jwks uri with port and ca", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "https://issuer.example:5556/keys", "ca_file": "/etc/ca.pem"}), true},
		{"zero skew", oidcConfigJSON(t, map[string]any{"clock_skew": "0s"}), true},
		{"max skew", oidcConfigJSON(t, map[string]any{"clock_skew": "5m"}), true},
		{"max lifetime", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "24h"}), true},
		{"max identities", oidcConfigJSON(t, map[string]any{"identities": identities(maxOIDCIdentities)}), true},
		{"unicode subject", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "ü-subject", "principal": "alice"}}}), true},

		{"empty", ``, false},
		{"not object", `[]`, false},
		{"trailing data", oidcConfigJSON(t, nil) + `{}`, false},
		{"at size cap", padJSON(oidcConfigJSON(t, nil), maxOIDCConfigBytes), true},
		{"oversized", padJSON(oidcConfigJSON(t, nil), maxOIDCConfigBytes+1), false},
		{"audience too long", oidcConfigJSON(t, map[string]any{"audience": strings.Repeat("a", maxClaimString+1)}), false},
		{"unknown member", oidcConfigJSON(t, map[string]any{"discovery": true}), false},
		{"case variant member", oidcConfigJSON(t, map[string]any{"Issuer": "https://attacker.example"}), false},
		{"duplicate member", strings.Replace(oidcConfigJSON(t, nil), `"audience":"skygo"`, `"audience":"skygo","audience":"other"`, 1), false},
		{"invalid utf8", strings.Replace(oidcConfigJSON(t, nil), `"skygo"`, "\"sky\xffgo\"", 1), false},
		{"version missing", oidcConfigJSON(t, map[string]any{"version": nil}), false},
		{"version 2", oidcConfigJSON(t, map[string]any{"version": 2}), false},
		{"version string", oidcConfigJSON(t, map[string]any{"version": "1"}), false},
		{"version fraction", strings.Replace(oidcConfigJSON(t, nil), `"version":1`, `"version":1.0`, 1), false},
		{"installation missing", oidcConfigJSON(t, map[string]any{"installation": nil}), false},
		{"installation other", oidcConfigJSON(t, map[string]any{"installation": "fedcba9876543210fedcba9876543210"}), false},
		{"generation zero", oidcConfigJSON(t, map[string]any{"generation": 0}), false},
		{"generation negative", oidcConfigJSON(t, map[string]any{"generation": -1}), false},
		{"generation missing", oidcConfigJSON(t, map[string]any{"generation": nil}), false},
		{"issuer missing", oidcConfigJSON(t, map[string]any{"issuer": nil}), false},
		{"issuer http", oidcConfigJSON(t, map[string]any{"issuer": "http://issuer.example"}), false},
		{"issuer query", oidcConfigJSON(t, map[string]any{"issuer": "https://issuer.example?x=1"}), false},
		{"issuer empty query", oidcConfigJSON(t, map[string]any{"issuer": "https://issuer.example?"}), false},
		{"issuer fragment", oidcConfigJSON(t, map[string]any{"issuer": "https://issuer.example#x"}), false},
		{"issuer userinfo", oidcConfigJSON(t, map[string]any{"issuer": "https://user@issuer.example"}), false},
		{"issuer no host", oidcConfigJSON(t, map[string]any{"issuer": "https:///dex"}), false},
		{"issuer opaque", oidcConfigJSON(t, map[string]any{"issuer": "https:issuer.example"}), false},
		{"issuer relative", oidcConfigJSON(t, map[string]any{"issuer": "issuer.example"}), false},
		{"issuer space", oidcConfigJSON(t, map[string]any{"issuer": "https://issuer.example/a b"}), false},
		{"issuer too long", oidcConfigJSON(t, map[string]any{"issuer": "https://issuer.example/" + strings.Repeat("a", maxOIDCURLBytes)}), false},
		{"audience missing", oidcConfigJSON(t, map[string]any{"audience": nil}), false},
		{"audience empty", oidcConfigJSON(t, map[string]any{"audience": ""}), false},
		{"audience array", oidcConfigJSON(t, map[string]any{"audience": []string{"skygo"}}), false},
		{"audience control", oidcConfigJSON(t, map[string]any{"audience": "sky\tgo"}), false},
		{"no key source", oidcConfigJSON(t, map[string]any{"jwks_file": nil}), false},
		{"both key sources", oidcConfigJSON(t, map[string]any{"jwks_uri": "https://issuer.example/keys"}), false},
		{"jwks uri http", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "http://issuer.example/keys"}), false},
		{"jwks uri fragment", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "https://issuer.example/keys#a"}), false},
		{"jwks uri userinfo", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "https://u:p@issuer.example/keys"}), false},
		{"jwks uri empty", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": ""}), false},
		{"jwks file empty", oidcConfigJSON(t, map[string]any{"jwks_file": ""}), false},
		{"jwks file nul", oidcConfigJSON(t, map[string]any{"jwks_file": "keys\x00.json"}), false},
		{"jwks file too long", oidcConfigJSON(t, map[string]any{"jwks_file": strings.Repeat("a", maxOIDCPathBytes+1)}), false},
		{"ca file with jwks file", oidcConfigJSON(t, map[string]any{"ca_file": "ca.pem"}), false},
		{"ca file empty", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": "https://issuer.example/keys", "ca_file": ""}), false},
		{"discovery url only", oidcConfigJSON(t, map[string]any{"jwks_file": nil, "jwks_uri": nil, "issuer": "https://issuer.example"}), false},
		{"lifetime zero", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "0s"}), false},
		{"lifetime negative", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "-1h"}), false},
		{"lifetime 25h", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "25h"}), false},
		{"lifetime above max", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "24h0m1s"}), false},
		{"lifetime subsecond", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "1.5s"}), false},
		{"lifetime number", oidcConfigJSON(t, map[string]any{"max_token_lifetime": 3600}), false},
		{"lifetime garbage", oidcConfigJSON(t, map[string]any{"max_token_lifetime": "1 hour"}), false},
		{"skew above max", oidcConfigJSON(t, map[string]any{"clock_skew": "5m1s"}), false},
		{"skew negative", oidcConfigJSON(t, map[string]any{"clock_skew": "-1s"}), false},
		{"identities missing", oidcConfigJSON(t, map[string]any{"identities": nil}), false},
		{"identities empty", oidcConfigJSON(t, map[string]any{"identities": []string{}}), false},
		{"identities object", oidcConfigJSON(t, map[string]any{"identities": map[string]string{"s": "alice"}}), false},
		{"too many identities", oidcConfigJSON(t, map[string]any{"identities": identities(maxOIDCIdentities + 1)}), false},
		{"duplicate subject", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s", "principal": "alice"}, {"subject": "s", "principal": "bob"}}}), false},
		{"identity extra member", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s", "principal": "alice", "role": "admin"}}}), false},
		{"identity missing principal", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s"}}}), false},
		{"identity case variant", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s", "Principal": "alice"}}}), false},
		{"identity duplicate member", strings.Replace(oidcConfigJSON(t, nil), `"principal":"alice"`, `"principal":"alice","principal":"bob"`, 1), false},
		{"identity email mapping", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"email": "a@example.com", "principal": "alice"}}}), false},
		{"empty subject", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "", "principal": "alice"}}}), false},
		{"long subject", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": strings.Repeat("s", 256), "principal": "alice"}}}), false},
		{"lone surrogate subject", strings.Replace(oidcConfigJSON(t, nil), `"subject":"s"`, `"subject":"s\ud800"`, 1), false},
		{"invalid principal", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s", "principal": "Alice"}}}), false},
		{"legacy principal", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "s", "principal": "legacy"}}}), false},
		{"wildcard principal", oidcConfigJSON(t, map[string]any{"identities": []map[string]string{{"subject": "*", "principal": "*"}}}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := parseOIDCConfig([]byte(tt.data), installation)
			if tt.valid {
				if err != nil || c == nil {
					t.Fatalf("parseOIDCConfig = %v, %v", c, err)
				}
				return
			}
			if err != ErrInvalid || c != nil {
				t.Fatalf("parseOIDCConfig = %+v, %v; want nil, ErrInvalid", c, err)
			}
		})
	}
}

func TestParseOIDCConfigDefaults(t *testing.T) {
	c, err := parseOIDCConfig([]byte(oidcConfigJSON(t, nil)), installation)
	if err != nil {
		t.Fatal(err)
	}
	if c.maxLifetime != defaultOIDCLifetime || c.skew != defaultOIDCSkew || c.jwksFile != "keys.json" || c.jwksURI != "" || c.caFile != "" {
		t.Fatalf("defaults = %+v", c)
	}
	c, err = parseOIDCConfig([]byte(oidcConfigJSON(t, map[string]any{"max_token_lifetime": "10m", "clock_skew": "0s"})), installation)
	if err != nil || c.maxLifetime != 10*time.Minute || c.skew != 0 {
		t.Fatalf("explicit = %+v, %v", c, err)
	}
}

// TestOIDCConfigLifetimeAndSkew checks that configured bounds, not defaults,
// govern verification.
func TestOIDCConfigLifetimeAndSkew(t *testing.T) {
	f := newOIDCFixture(t)
	f.config.maxLifetime = 10 * time.Minute
	f.config.skew = 0
	rs := func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) }
	for _, tt := range []struct {
		name   string
		claims map[string]any
		now    int64
		want   oidcReason
	}{
		{"within", map[string]any{"iat": oidcNow - 60, "exp": oidcNow + 540}, oidcNow, oidcOK},
		{"lifetime", map[string]any{"iat": oidcNow - 60, "exp": oidcNow + 541}, oidcNow, oidcLifetime},
		{"expired at exp", map[string]any{"iat": oidcNow - 60, "exp": oidcNow + 540}, oidcNow + 540, oidcExpired},
		{"iat ahead", map[string]any{"iat": oidcNow + 1, "exp": oidcNow + 540}, oidcNow, oidcNotYetValid},
	} {
		tok := jws(rsaHeader, oidcClaims(t, tt.claims), rs)
		if _, got := checkIDToken(tok, f.config, f.keys, f.principals, time.Unix(tt.now, 0)); got != tt.want {
			t.Errorf("%s: reason %d, want %d", tt.name, got, tt.want)
		}
	}
}
