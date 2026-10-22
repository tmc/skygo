package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
)

type rfcVector struct {
	JWKS       json.RawMessage `json:"jwks"`
	JWS        string          `json:"jws"`
	Thumbprint string          `json:"thumbprint"`
}

func readVector(t *testing.T, name string) rfcVector {
	t.Helper()
	data, err := os.ReadFile("testdata/oidc/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var v rfcVector
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// TestJWSVectors verifies the RFC 7515 Appendix A.2 (RS256) and A.3 (ES256)
// examples and refuses each with one signature bit flipped.
func TestJWSVectors(t *testing.T) {
	const payload = "{\"iss\":\"joe\",\r\n \"exp\":1300819380,\r\n \"http://example.com/is_root\":true}"
	for _, name := range []string{"rfc7515-a2.json", "rfc7515-a3.json"} {
		t.Run(name, func(t *testing.T) {
			v := readVector(t, name)
			keys, err := parseJWKS(v.JWKS)
			if err != nil {
				t.Fatal(err)
			}
			_, got, reason := verifyJWS(v.JWS, keys)
			if reason != oidcOK || string(got) != payload {
				t.Fatalf("verifyJWS = %q, %d", got, reason)
			}
			dot := strings.LastIndexByte(v.JWS, '.')
			sig, _ := decodeSegment(v.JWS[dot+1:])
			for _, bit := range []int{0, 8*len(sig) - 1} {
				flipped := append([]byte(nil), sig...)
				flipped[bit/8] ^= 0x80 >> (bit % 8)
				if _, _, reason := verifyJWS(v.JWS[:dot+1]+b64(flipped), keys); reason != oidcSignature {
					t.Errorf("bit %d flipped: reason %d", bit, reason)
				}
			}
			if _, _, reason := verifyJWS(v.JWS[:dot]+"X"+v.JWS[dot:], keys); reason == oidcOK {
				t.Error("altered payload verified")
			}
		})
	}
}

func TestJWKThumbprintVector(t *testing.T) {
	v := readVector(t, "rfc7638.json")
	keys, err := parseJWKS(v.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	k, ok := keys.lookup("2011-04-29")
	if !ok {
		t.Fatal("kid 2011-04-29 not found")
	}
	if got := b64(k.thumbprint[:]); got != v.Thumbprint {
		t.Fatalf("thumbprint = %s, want %s", got, v.Thumbprint)
	}
}

func TestParseJWKS(t *testing.T) {
	oidcTestKeys(t)
	rsaKey := func(extra map[string]any) map[string]any { return rsaJWKJSON(&oidcKeys.rsa.PublicKey, extra) }
	ecKey := func(extra map[string]any) map[string]any { return ecJWKJSON(&oidcKeys.ec.PublicKey, extra) }
	set := func(keys ...map[string]any) string { return string(jwksJSON(t, keys...)) }
	n := rsaKey(nil)["n"].(string)
	x := ecKey(nil)["x"].(string)
	y := ecKey(nil)["y"].(string)
	yPlusOne := b64(new(big.Int).Add(oidcKeys.ec.PublicKey.Y, big.NewInt(1)).FillBytes(make([]byte, 32)))
	nBytes, _ := decodeSegment(n)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	nNoncanonical := n[:len(n)-1] + string(alphabet[strings.IndexByte(alphabet, n[len(n)-1])|1])

	var manyEC []map[string]any
	for i := 0; i < maxJWKSKeys+1; i++ {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		manyEC = append(manyEC, ecJWKJSON(&k.PublicKey, map[string]any{"kid": fmt.Sprint("k", i)}))
	}

	tests := []struct {
		name   string
		data   string
		usable int // 0 means parseJWKS must fail
	}{
		{"rsa", set(rsaKey(nil)), 1},
		{"ec", set(ecKey(nil)), 1},
		{"members", set(rsaKey(map[string]any{"kid": "a", "alg": "RS256", "use": "sig", "key_ops": []string{"verify"}, "x5t": "AAAA", "x5c": []string{"AAAA"}, "x5t#S256": "AAAA"})), 1},
		{"ec alg", set(ecKey(map[string]any{"alg": "ES256"})), 1},
		{"mixed with skipped keys", set(
			rsaKey(map[string]any{"kid": "rs"}),
			ecKey(map[string]any{"kid": "es"}),
			rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "enc", "use": "enc"}),
			rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "ps", "alg": "PS256"}),
			rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "sign-only", "key_ops": []string{"sign"}}),
			map[string]any{"kty": "OKP", "crv": "Ed25519", "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo", "kid": "ed"},
			map[string]any{"kty": "EC", "crv": "P-384", "x": "AAAA", "y": "AAAA", "kid": "p384"},
		), 2},
		{"max keys", set(manyEC[:maxJWKSKeys]...), maxJWKSKeys},
		{"rsa max size", set(rsaKey(map[string]any{"n": b64(append(append([]byte{0x80}, make([]byte, maxRSABits/8-2)...), 1))})), 1},

		{"not object", `[]`, 0},
		{"null", `null`, 0},
		{"trailing data", set(rsaKey(nil)) + `{}`, 0},
		{"extra top member ignored", `{"keys":[` + strings.TrimSuffix(strings.TrimPrefix(set(rsaKey(nil)), `{"keys":[`), `]}`) + `],"other":1}`, 1},
		{"keys not array", `{"keys":{}}`, 0},
		{"keys empty", `{"keys":[]}`, 0},
		{"too many keys", set(manyEC...), 0},
		{"oversized", `{"keys":[` + strings.TrimSuffix(strings.TrimPrefix(set(rsaKey(map[string]any{"x5c": []string{strings.Repeat("A", maxJWKSBytes)}})), `{"keys":[`), `]}`) + `]}`, 0},
		{"duplicate member", `{"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `","y":"` + y + `"}]}`, 0},
		{"duplicate keys member", `{"keys":[],"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `"}]}`, 0},
		{"case variant member ignored", set(ecKey(map[string]any{"KTY": "RSA"})), 1},
		{"unknown member ignored", set(ecKey(map[string]any{"foo": "bar"})), 1},
		{"private d", set(ecKey(map[string]any{"d": "AAAA"})), 0},
		{"private rsa p", set(rsaKey(map[string]any{"p": "AAAA"})), 0},
		{"private member d", set(rsaKey(map[string]any{"d": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member p", set(rsaKey(map[string]any{"p": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member q", set(rsaKey(map[string]any{"q": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member dp", set(rsaKey(map[string]any{"dp": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member dq", set(rsaKey(map[string]any{"dq": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member qi", set(rsaKey(map[string]any{"qi": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member oth", set(rsaKey(map[string]any{"oth": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"private member k", set(rsaKey(map[string]any{"k": "AAAA", "issuer": "https://attacker.example/v2.0"})), 0},
		{"symmetric k", `{"keys":[{"kty":"oct","k":"AAAA"}]}`, 0},
		{"x5u", set(ecKey(map[string]any{"x5u": "https://attacker.example/x"})), 0},
		{"metadata types ignored", set(ecKey(map[string]any{"issuer": "https://attacker.example", "meta_object": map[string]any{"alg": "none", "kid": 1}, "meta_array": []any{1, "x", nil}, "meta_null": nil, "meta_number": 1.5, "meta_bool": true, "ALG": "RS256", "Alg": "none", "algorithm": "HS256"})), 1},
		{"top metadata types ignored", `{"issuer":1,"jwks_uri":"https://attacker.example/keys","meta":{"x":[null,{"y":true}]},"keys":[` + strings.TrimSuffix(strings.TrimPrefix(set(ecKey(nil)), `{"keys":[`), `]}`) + `]}`, 1},
		{"private member with metadata", set(rsaKey(map[string]any{"d": "AAAA", "issuer": "https://attacker.example/v2.0"}), ecKey(map[string]any{"kid": "ec"})), 0},
		{"private member on skipped key", set(rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"use": "enc", "d": "AAAA"}), ecKey(nil)), 0},
		{"private member null", set(ecKey(map[string]any{"d": nil})), 0},
		{"duplicate escaped member", `{"keys":[{"kty":"EC","\u006bty":"RSA","crv":"P-256","x":"` + x + `","y":"` + y + `"}]}`, 0},
		{"duplicate escaped keys", `{"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `"}],"\u006beys":[]}`, 0},
		{"duplicate in metadata", `{"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `","meta":{"a":1,"a":2}}]}`, 0},
		{"duplicate in top metadata", `{"meta":{"a":1,"a":2},"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `"}]}`, 0},
		{"metadata too deep", `{"keys":[{"kty":"EC","crv":"P-256","x":"` + x + `","y":"` + y + `","meta":` + strings.Repeat("[", 70) + strings.Repeat("]", 70) + `}]}`, 0},
		{"known member malformed with metadata", set(ecKey(map[string]any{"kid": 1, "issuer": "https://attacker.example/v2.0"})), 0},
		{"at size cap", padJSON(set(ecKey(nil)), maxJWKSBytes), 1},
		{"oversized by one byte", padJSON(set(ecKey(nil)), maxJWKSBytes+1), 0},
		{"jku", set(ecKey(map[string]any{"jku": "https://attacker.example/x"})), 0},
		{"duplicate kid", set(rsaKey(map[string]any{"kid": "a"}), ecKey(map[string]any{"kid": "a"})), 0},
		{"duplicate kid with skipped key", set(rsaKey(map[string]any{"kid": "a"}), rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "a", "use": "enc"})), 0},
		{"duplicate key", set(rsaKey(map[string]any{"kid": "a"}), rsaKey(map[string]any{"kid": "b"})), 0},
		{"kid empty", set(ecKey(map[string]any{"kid": ""})), 0},
		{"kid number", set(ecKey(map[string]any{"kid": 1})), 0},
		{"kty missing", `{"keys":[{"crv":"P-256","x":"` + x + `","y":"` + y + `"}]}`, 0},
		{"kty number", `{"keys":[{"kty":1}]}`, 0},
		{"use empty", set(ecKey(map[string]any{"use": ""})), 0},
		{"use not string", set(ecKey(map[string]any{"use": 1})), 0},
		{"key_ops duplicate", set(ecKey(map[string]any{"key_ops": []string{"verify", "verify"}})), 0},
		{"key_ops empty", set(ecKey(map[string]any{"key_ops": []string{}})), 0},
		{"key_ops not array", set(ecKey(map[string]any{"key_ops": "verify"})), 0},
		{"only encryption key", set(rsaKey(map[string]any{"use": "enc"})), 0},
		{"only sign key_ops", set(rsaKey(map[string]any{"key_ops": []string{"sign"}})), 0},
		{"alg empty", set(ecKey(map[string]any{"alg": ""})), 0},
		{"rs256 on ec", set(ecKey(map[string]any{"alg": "RS256"})), 0},
		{"es256 on rsa", set(rsaKey(map[string]any{"alg": "ES256"})), 0},
		{"es256 on p384", `{"keys":[{"kty":"EC","crv":"P-384","alg":"ES256","x":"AAAA","y":"AAAA"}]}`, 0},
		{"es256 on okp", `{"keys":[{"kty":"OKP","crv":"Ed25519","alg":"ES256","x":"AAAA"}]}`, 0},
		{"rsa with crv", set(rsaKey(map[string]any{"crv": "P-256"})), 0},
		{"ec with n", set(ecKey(map[string]any{"n": n})), 0},
		{"rsa n missing", `{"keys":[{"kty":"RSA","e":"AQAB"}]}`, 0},
		{"rsa n padded", set(rsaKey(map[string]any{"n": n + "=="})), 0},
		{"rsa n standard alphabet", set(rsaKey(map[string]any{"n": base64.RawStdEncoding.EncodeToString(append([]byte{0xfb, 0xff}, nBytes[2:]...))})), 0},
		{"rsa n noncanonical", set(rsaKey(map[string]any{"n": nNoncanonical})), 0},
		{"rsa n leading zero", set(rsaKey(map[string]any{"n": b64(append([]byte{0}, nBytes...))})), 0},
		{"rsa n even", set(rsaKey(map[string]any{"n": b64(append(append([]byte(nil), nBytes[:len(nBytes)-1]...), nBytes[len(nBytes)-1]&^1))})), 0},
		{"rsa 1024", set(rsaJWKJSON(&oidcKeys.rsaSmall.PublicKey, nil)), 0},
		{"rsa 2047", set(rsaKey(map[string]any{"n": b64(append([]byte{0x7f}, nBytes[1:]...))})), 0},
		{"rsa too large", set(rsaKey(map[string]any{"n": b64(append(append([]byte{0xff}, make([]byte, maxRSABits/8-1)...), 1))})), 0},
		{"rsa e 3", set(rsaKey(map[string]any{"e": "Aw"})), 0},
		{"rsa e leading zero", set(rsaKey(map[string]any{"e": "AAEAAQ"})), 0},
		{"rsa e number", set(rsaKey(map[string]any{"e": 65537})), 0},
		{"ec x short", set(ecKey(map[string]any{"x": b64(oidcKeys.ec.PublicKey.X.Bytes()[1:])})), 0},
		{"ec x long", set(ecKey(map[string]any{"x": b64(append([]byte{0}, oidcKeys.ec.PublicKey.X.FillBytes(make([]byte, 32))...))})), 0},
		{"ec off curve", set(ecKey(map[string]any{"y": yPlusOne})), 0},
		{"ec coordinate out of range", set(ecKey(map[string]any{"x": b64(elliptic.P256().Params().P.FillBytes(make([]byte, 32)))})), 0},
		{"ec crv missing", `{"keys":[{"kty":"EC","x":"` + x + `","y":"` + y + `"}]}`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := parseJWKS([]byte(tt.data))
			if tt.usable == 0 {
				if err != ErrInvalid || keys != nil {
					t.Fatalf("parseJWKS = %v, %v; want nil, ErrInvalid", keys, err)
				}
				if len(tt.data) > 8 && strings.Contains(err.Error(), tt.data[2:8]) {
					t.Fatalf("error %q echoes input", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(keys.keys) != tt.usable {
				t.Fatalf("usable keys = %d, want %d", len(keys.keys), tt.usable)
			}
		})
	}
}

func TestJWKSLookup(t *testing.T) {
	oidcTestKeys(t)
	keys, err := parseJWKS(jwksJSON(t,
		rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}),
		ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}),
		rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "enc", "use": "enc"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		kid, alg string
		ok       bool
	}{
		{"rsa-1", "RS256", true},
		{"ec-1", "ES256", true},
		{"enc", "", false},
		{"", "", false},
		{"rsa-2", "", false},
		{"rsa-1 ", "", false},
	} {
		k, ok := keys.lookup(tt.kid)
		if ok != tt.ok || ok && (k.kid != tt.kid || k.alg != tt.alg) {
			t.Errorf("lookup(%q) = %+v, %v", tt.kid, k, ok)
		}
	}

	sole, err := parseJWKS(jwksJSON(t, rsaJWKJSON(&oidcKeys.rsa.PublicKey, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if k, ok := sole.lookup(""); !ok || k.alg != "RS256" {
		t.Errorf("sole key without kid: %+v, %v", k, ok)
	}
	if _, ok := sole.lookup("x"); ok {
		t.Error("kid matched key without kid")
	}
}

// TestJWKThumbprintGenerated checks RFC 7638 member order and content for
// generated keys against an independent encoding/json rendering.
func TestJWKThumbprintGenerated(t *testing.T) {
	oidcTestKeys(t)
	ec := ecJWKJSON(&oidcKeys.ec.PublicKey, nil)
	rs := rsaJWKJSON(&oidcKeys.rsa.PublicKey, nil)
	for _, k := range []map[string]any{ec, rs} {
		keys, err := parseJWKS(jwksJSON(t, k))
		if err != nil {
			t.Fatal(err)
		}
		required := map[string]any{"kty": k["kty"]}
		for _, name := range []string{"crv", "x", "y", "n", "e"} {
			if v, ok := k[name]; ok {
				required[name] = v
			}
		}
		canonical, err := json.Marshal(required) // sorted keys, no whitespace
		if err != nil {
			t.Fatal(err)
		}
		if want := sha256.Sum256(canonical); keys.keys[0].thumbprint != want {
			t.Errorf("%s thumbprint mismatch", k["kty"])
		}
	}
}

func FuzzParseJWKS(f *testing.F) {
	for _, name := range []string{"rfc7515-a2.json", "rfc7515-a3.json", "rfc7638.json"} {
		data, err := os.ReadFile("testdata/oidc/" + name)
		if err != nil {
			f.Fatal(err)
		}
		var v rfcVector
		if err := json.Unmarshal(data, &v); err != nil {
			f.Fatal(err)
		}
		f.Add([]byte(v.JWKS))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := parseJWKS(data)
		if err != nil {
			if err != ErrInvalid || keys != nil {
				t.Fatalf("parseJWKS = %v, %v", keys, err)
			}
			return
		}
		if len(keys.keys) == 0 || len(keys.keys) > maxJWKSKeys {
			t.Fatalf("usable keys = %d", len(keys.keys))
		}
		for _, k := range keys.keys {
			if k.alg == "RS256" && (k.rsa == nil || k.rsa.N.BitLen() < minRSABits || k.rsa.E != 65537) || k.alg == "ES256" && k.ec == nil || k.alg != "RS256" && k.alg != "ES256" {
				t.Fatalf("unusable key accepted: %s", k.alg)
			}
		}
	})
}

// TestJWKSMetadataGrantsNoAuthority checks that an Entra-shaped set, whose
// keys carry unknown "issuer" metadata and whose top level carries extra
// members, yields the same keys as the bare set and changes no verification
// decision.
func TestJWKSMetadataGrantsNoAuthority(t *testing.T) {
	f := newOIDCFixture(t)
	const metaIssuer = "https://login.attacker.example/tenant/v2.0"
	rsa := rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1", "use": "sig", "x5t": "AAAA", "x5c": []string{"AAAA"}, "issuer": metaIssuer, "cloud_instance_name": "attacker.example"})
	ec := ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1", "issuer": metaIssuer, "alg_hint": "RS256", "role": "admin", "aud": "other"})
	data, err := json.Marshal(map[string]any{"keys": []any{rsa, ec}, "issuer": metaIssuer, "jwks_uri": "https://attacker.example/keys", "audience": "other"})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := parseJWKS(data)
	if err != nil {
		t.Fatal(err)
	}
	bare, err := parseJWKS(jwksJSON(t,
		rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}),
		ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"})))
	if err != nil {
		t.Fatal(err)
	}
	if len(keys.keys) != len(bare.keys) {
		t.Fatalf("usable keys = %d, want %d", len(keys.keys), len(bare.keys))
	}
	for i := range keys.keys {
		k, b := keys.keys[i], bare.keys[i]
		if k.kid != b.kid || k.alg != b.alg || k.thumbprint != b.thumbprint {
			t.Fatalf("key %d = %s %s, want %s %s", i, k.kid, k.alg, b.kid, b.alg)
		}
	}

	rs := func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) }
	es := func(input string) []byte { return signES256(t, oidcKeys.ec, input) }
	for _, tt := range []struct {
		name  string
		token string
		want  oidcReason
	}{
		{"configured issuer", jws(rsaHeader, oidcClaims(t, nil), rs), oidcOK},
		{"es256", jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), es), oidcOK},
		{"metadata issuer", jws(rsaHeader, oidcClaims(t, map[string]any{"iss": metaIssuer}), rs), oidcIssuer},
		{"metadata audience", jws(rsaHeader, oidcClaims(t, map[string]any{"aud": "other"}), rs), oidcAudience},
		{"metadata algorithm", jws(`{"alg":"RS256","kid":"ec-1"}`, oidcClaims(t, nil), rs), oidcAlgorithm},
	} {
		id, got := checkIDToken(tt.token, f.config, keys, f.principals, f.now)
		if got != tt.want {
			t.Errorf("%s: reason %d, want %d", tt.name, got, tt.want)
			continue
		}
		if got == oidcOK && (id.principal != "alice" || id.role != Operator) {
			t.Errorf("%s: identity %s %s, want alice operator", tt.name, id.principal, id.role)
		}
	}
}
