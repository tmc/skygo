package auth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// oidcKeys are generated once per test binary: the generated positive and
// negative oracles sign with them, and the verifier sees only public JWKs.
var oidcKeys struct {
	once           sync.Once
	rsa, rsaOther  *rsa.PrivateKey
	rsaSmall       *rsa.PrivateKey
	ec, ecOther    *ecdsa.PrivateKey
	generateFailed error
}

func oidcTestKeys(t testing.TB) {
	t.Helper()
	k := &oidcKeys
	k.once.Do(func() {
		var errs [5]error
		k.rsa, errs[0] = rsa.GenerateKey(rand.Reader, 2048)
		k.rsaOther, errs[1] = rsa.GenerateKey(rand.Reader, 2048)
		k.rsaSmall, errs[2] = rsa.GenerateKey(rand.Reader, 1024)
		k.ec, errs[3] = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		k.ecOther, errs[4] = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		k.generateFailed = errors.Join(errs[:]...)
	})
	if k.generateFailed != nil {
		t.Fatal(k.generateFailed)
	}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// rsaJWKJSON returns a public RSA JWK with optional extra members.
func rsaJWKJSON(pub *rsa.PublicKey, extra map[string]any) map[string]any {
	m := map[string]any{"kty": "RSA", "n": b64(pub.N.Bytes()), "e": b64(big.NewInt(int64(pub.E)).Bytes())}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// ecJWKJSON returns a public P-256 JWK with optional extra members.
func ecJWKJSON(pub *ecdsa.PublicKey, extra map[string]any) map[string]any {
	m := map[string]any{"kty": "EC", "crv": "P-256", "x": b64(pub.X.FillBytes(make([]byte, 32))), "y": b64(pub.Y.FillBytes(make([]byte, 32)))}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func jwksJSON(t testing.TB, keys ...map[string]any) []byte {
	t.Helper()
	data, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// signRS256 and signES256 are the test issuer; ES256 uses R||S.
func signRS256(t testing.TB, key *rsa.PrivateKey, input string) []byte {
	t.Helper()
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return sig
}

func signES256(t testing.TB, key *ecdsa.PrivateKey, input string) []byte {
	t.Helper()
	digest := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return append(r.FillBytes(make([]byte, 32)), s.FillBytes(make([]byte, 32))...)
}

const (
	oidcIssuerURL = "https://issuer.example/dex"
	oidcNow       = 1800000000
)

type oidcFixture struct {
	config     *oidcConfig
	keys       *jwkSet
	principals *config
	now        time.Time
}

func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	oidcTestKeys(t)
	data, err := os.ReadFile("testdata/oidc/config.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := parseOIDCConfig(data, installation)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := parseJWKS(jwksJSON(t,
		rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1", "alg": "RS256", "use": "sig"}),
		ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"}),
		rsaJWKJSON(&oidcKeys.rsaOther.PublicKey, map[string]any{"kid": "rsa-enc", "use": "enc"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	principals := &config{Version: 1, Installation: installation, Generation: 1, Principals: []principal{{"legacy", Admin}, {"alice", Operator}, {"bob", Reader}}}
	return &oidcFixture{config: c, keys: keys, principals: principals, now: time.Unix(oidcNow, 0)}
}

// claims returns the default valid claims for alice with changes applied. A
// nil value deletes a claim. Unused claims name bob to catch inference from
// email or preferred_username.
func oidcClaims(t testing.TB, change map[string]any) string {
	t.Helper()
	m := map[string]any{
		"iss":                oidcIssuerURL,
		"sub":                "alice-subject",
		"aud":                "skygo",
		"exp":                oidcNow + 600,
		"iat":                oidcNow - 60,
		"email":              "bob@example.com",
		"preferred_username": "bob",
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

// jws assembles a compact JWS from raw header and payload JSON.
func jws(header, payload string, sign func(string) []byte) string {
	input := b64([]byte(header)) + "." + b64([]byte(payload))
	return input + "." + b64(sign(input))
}

const rsaHeader = `{"alg":"RS256","kid":"rsa-1","typ":"JWT"}`

func TestVerifyIDToken(t *testing.T) {
	f := newOIDCFixture(t)
	rs := func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) }
	es := func(input string) []byte { return signES256(t, oidcKeys.ec, input) }
	token := func(change map[string]any) string { return jws(rsaHeader, oidcClaims(t, change), rs) }
	withHeader := func(header string) string { return jws(header, oidcClaims(t, nil), rs) }
	valid := token(nil)
	dot := strings.LastIndexByte(valid, '.')
	other := token(map[string]any{"sub": "bob-subject"})
	at := func(sec int64) time.Time { return time.Unix(sec, 0) }

	hs256 := func() string {
		der, err := x509.MarshalPKIXPublicKey(&oidcKeys.rsa.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		return jws(`{"alg":"HS256","kid":"rsa-1"}`, oidcClaims(t, nil), func(input string) []byte {
			mac := hmac.New(sha256.New, der)
			mac.Write([]byte(input))
			return mac.Sum(nil)
		})
	}
	derES256 := jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), func(input string) []byte {
		raw := signES256(t, oidcKeys.ec, input)
		der, err := asn1.Marshal(struct{ R, S *big.Int }{new(big.Int).SetBytes(raw[:32]), new(big.Int).SetBytes(raw[32:])})
		if err != nil {
			t.Fatal(err)
		}
		return der
	})
	wideES256 := jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), func(input string) []byte {
		raw := signES256(t, oidcKeys.ec, input)
		return append(append([]byte{0}, raw[:32]...), append([]byte{0}, raw[32:]...)...)
	})
	noncanonical := func(s string) string {
		const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
		i := strings.IndexByte(alphabet, s[len(s)-1])
		return s[:len(s)-1] + string(alphabet[i|1])
	}
	segments := strings.Split(valid, ".")

	tests := []struct {
		name  string
		token string
		now   time.Time
		want  oidcReason
	}{
		{name: "rs256", token: valid, want: oidcOK},
		{name: "es256 without typ", token: jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), es), want: oidcOK},
		{name: "typ JOSE", token: withHeader(`{"alg":"RS256","kid":"rsa-1","typ":"JOSE"}`), want: oidcOK},
		{name: "unknown header ignored", token: withHeader(`{"alg":"RS256","kid":"rsa-1","x5t":"AAAA","ALG":"none"}`), want: oidcOK},
		{name: "aud array with azp", token: token(map[string]any{"aud": []string{"other", "skygo"}, "azp": "skygo"}), want: oidcOK},
		{name: "single aud with azp", token: token(map[string]any{"azp": "skygo"}), want: oidcOK},
		{name: "single aud array", token: token(map[string]any{"aud": []string{"skygo"}}), want: oidcOK},
		{name: "nbf past", token: token(map[string]any{"nbf": oidcNow - 30}), want: oidcOK},
		{name: "second subject same principal", token: token(map[string]any{"sub": "alice-laptop"}), want: oidcOK},
		{name: "reader principal", token: other, want: oidcOK},
		{name: "last instant before expiry", token: valid, now: at(oidcNow + 600 + 29), want: oidcOK},
		{name: "subsecond before expiry", token: valid, now: at(oidcNow + 629).Add(999 * time.Millisecond), want: oidcOK},
		{name: "iat at skew", token: token(map[string]any{"iat": oidcNow + 30}), want: oidcOK},
		{name: "nbf at skew", token: token(map[string]any{"nbf": oidcNow + 30}), want: oidcOK},
		{name: "lifetime at cap", token: token(map[string]any{"iat": oidcNow - 60, "exp": oidcNow - 60 + 3600}), want: oidcOK},

		{name: "empty", token: "", want: oidcMalformed},
		{name: "oversized", token: token(map[string]any{"pad": strings.Repeat("a", 5800)}), want: oidcMalformed},
		{name: "oversized header", token: withHeader(`{"alg":"RS256","kid":"rsa-1","pad":"` + strings.Repeat("a", 1100) + `"}`), want: oidcMalformed},
		{name: "two segments", token: segments[0] + "." + segments[1], want: oidcMalformed},
		{name: "four segments", token: valid + "." + segments[2], want: oidcMalformed},
		{name: "jwe shape", token: segments[0] + ".." + segments[1] + "." + segments[2] + "." + segments[2], want: oidcMalformed},
		{name: "empty header", token: "." + segments[1] + "." + segments[2], want: oidcMalformed},
		{name: "empty payload", token: segments[0] + ".." + segments[2], want: oidcSignature},
		{name: "signed empty payload", token: jws(rsaHeader, "", rs), want: oidcMalformed},
		{name: "alg none unsigned", token: b64([]byte(`{"alg":"none"}`)) + "." + segments[1] + ".", want: oidcMalformed},
		{name: "padding", token: segments[0] + "." + segments[1] + "." + segments[2] + "==", want: oidcMalformed},
		{name: "padding in header", token: segments[0] + "=." + segments[1] + "." + segments[2], want: oidcMalformed},
		{name: "standard alphabet", token: strings.NewReplacer("-", "+", "_", "/").Replace(valid), want: oidcMalformed},
		{name: "line break", token: segments[0] + "." + segments[1][:10] + "\n" + segments[1][10:] + "." + segments[2], want: oidcMalformed},
		{name: "leading space", token: " " + valid, want: oidcMalformed},
		{name: "noncanonical trailing bits", token: segments[0] + "." + segments[1] + "." + noncanonical(segments[2]), want: oidcMalformed},
		{name: "duplicate header", token: withHeader(`{"alg":"RS256","alg":"RS256","kid":"rsa-1"}`), want: oidcMalformed},
		{name: "duplicate sub", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"bob-subject","sub":"alice-subject","aud":"skygo","exp":1800000600,"iat":1799999940}`, rs), want: oidcMalformed},
		{name: "duplicate exp", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","aud":"skygo","exp":1800000600,"exp":1900000000,"iat":1799999940}`, rs), want: oidcMalformed},
		{name: "duplicate sub after sub", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","sub":"bob-subject","aud":"skygo","exp":1800000600,"iat":1799999940}`, rs), want: oidcMalformed},
		{name: "duplicate by escape", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","\u0073ub":"bob-subject","aud":"skygo","exp":1800000600,"iat":1799999940}`, rs), want: oidcMalformed},
		{name: "payload array", token: jws(rsaHeader, `[`+oidcClaims(t, nil)+`]`, rs), want: oidcMalformed},
		{name: "payload trailing data", token: jws(rsaHeader, oidcClaims(t, nil)+`{}`, rs), want: oidcMalformed},
		{name: "payload invalid utf8", token: jws(rsaHeader, strings.Replace(oidcClaims(t, nil), "bob@", "b\xffb@", 1), rs), want: oidcMalformed},
		{name: "header not object", token: withHeader(`"RS256"`), want: oidcMalformed},

		{name: "alg none signed", token: withHeader(`{"alg":"none","kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "alg missing", token: withHeader(`{"kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "alg lowercase", token: withHeader(`{"alg":"rs256","kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "alg not string", token: withHeader(`{"alg":["RS256"],"kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "hs256 key confusion", token: hs256(), want: oidcAlgorithm},
		{name: "ps256", token: withHeader(`{"alg":"PS256","kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "es384", token: jws(`{"alg":"ES384","kid":"ec-1"}`, oidcClaims(t, nil), es), want: oidcAlgorithm},
		{name: "es256 header rsa key", token: withHeader(`{"alg":"ES256","kid":"rsa-1"}`), want: oidcAlgorithm},
		{name: "rs256 header ec key", token: jws(`{"alg":"RS256","kid":"ec-1"}`, oidcClaims(t, nil), es), want: oidcAlgorithm},

		{name: "unknown kid", token: withHeader(`{"alg":"RS256","kid":"rsa-2"}`), want: oidcKey},
		{name: "kid case", token: withHeader(`{"alg":"RS256","kid":"RSA-1"}`), want: oidcKey},
		{name: "missing kid several keys", token: withHeader(`{"alg":"RS256"}`), want: oidcKey},
		{name: "encryption key kid", token: jws(`{"alg":"RS256","kid":"rsa-enc"}`, oidcClaims(t, nil), func(input string) []byte { return signRS256(t, oidcKeys.rsaOther, input) }), want: oidcKey},

		{name: "crit", token: withHeader(`{"alg":"RS256","kid":"rsa-1","crit":["exp"],"exp":1}`), want: oidcHeader},
		{name: "empty crit", token: withHeader(`{"alg":"RS256","kid":"rsa-1","crit":[]}`), want: oidcHeader},
		{name: "jku", token: withHeader(`{"alg":"RS256","kid":"rsa-1","jku":"https://attacker.example/keys"}`), want: oidcHeader},
		{name: "x5u", token: withHeader(`{"alg":"RS256","kid":"rsa-1","x5u":"https://attacker.example/cert"}`), want: oidcHeader},
		{name: "jwk", token: withHeader(`{"alg":"RS256","kid":"rsa-1","jwk":{"kty":"RSA","n":"AQAB","e":"AQAB"}}`), want: oidcHeader},
		{name: "x5c", token: withHeader(`{"alg":"RS256","kid":"rsa-1","x5c":["AAAA"]}`), want: oidcHeader},
		{name: "cty", token: withHeader(`{"alg":"RS256","kid":"rsa-1","cty":"JWT"}`), want: oidcHeader},
		{name: "enc", token: withHeader(`{"alg":"RS256","kid":"rsa-1","enc":"A128GCM"}`), want: oidcHeader},
		{name: "zip", token: withHeader(`{"alg":"RS256","kid":"rsa-1","zip":"DEF"}`), want: oidcHeader},
		{name: "access token typ", token: withHeader(`{"alg":"RS256","kid":"rsa-1","typ":"at+jwt"}`), want: oidcHeader},
		{name: "access token media type", token: withHeader(`{"alg":"RS256","kid":"rsa-1","typ":"application/at+jwt"}`), want: oidcHeader},
		{name: "typ lowercase", token: withHeader(`{"alg":"RS256","kid":"rsa-1","typ":"jwt"}`), want: oidcHeader},
		{name: "typ not string", token: withHeader(`{"alg":"RS256","kid":"rsa-1","typ":1}`), want: oidcHeader},
		{name: "kid empty", token: withHeader(`{"alg":"RS256","kid":""}`), want: oidcHeader},
		{name: "kid not string", token: withHeader(`{"alg":"RS256","kid":1}`), want: oidcHeader},

		{name: "tampered payload", token: segments[0] + "." + strings.Split(other, ".")[1] + "." + segments[2], want: oidcSignature},
		{name: "wrong key same kid", token: jws(rsaHeader, oidcClaims(t, nil), func(input string) []byte { return signRS256(t, oidcKeys.rsaOther, input) }), want: oidcSignature},
		{name: "wrong ec key", token: jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), func(input string) []byte { return signES256(t, oidcKeys.ecOther, input) }), want: oidcSignature},
		{name: "rs256 signature zero prefixed", token: jws(rsaHeader, oidcClaims(t, nil), func(input string) []byte { return append([]byte{0}, rs(input)...) }), want: oidcSignature},
		{name: "rs256 signature truncated", token: jws(rsaHeader, oidcClaims(t, nil), func(input string) []byte { return rs(input)[1:] }), want: oidcSignature},
		{name: "es256 der", token: derES256, want: oidcSignature},
		{name: "es256 33-byte halves", token: wideES256, want: oidcSignature},
		{name: "es256 zero", token: jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(t, nil), func(string) []byte { return make([]byte, 64) }), want: oidcSignature},
		{name: "signature from other token", token: valid[:dot] + other[strings.LastIndexByte(other, '.'):], want: oidcSignature},

		{name: "issuer missing", token: token(map[string]any{"iss": nil}), want: oidcIssuer},
		{name: "issuer prefix", token: token(map[string]any{"iss": "https://issuer.example"}), want: oidcIssuer},
		{name: "issuer extension", token: token(map[string]any{"iss": oidcIssuerURL + "/other"}), want: oidcIssuer},
		{name: "issuer trailing slash", token: token(map[string]any{"iss": oidcIssuerURL + "/"}), want: oidcIssuer},
		{name: "issuer case", token: token(map[string]any{"iss": "https://ISSUER.example/dex"}), want: oidcIssuer},
		{name: "issuer array", token: token(map[string]any{"iss": []string{oidcIssuerURL}}), want: oidcIssuer},

		{name: "subject missing", token: token(map[string]any{"sub": nil}), want: oidcSubject},
		{name: "subject empty", token: token(map[string]any{"sub": ""}), want: oidcSubject},
		{name: "subject too long", token: token(map[string]any{"sub": strings.Repeat("s", 256)}), want: oidcSubject},
		{name: "subject number", token: token(map[string]any{"sub": 1}), want: oidcSubject},
		{name: "subject control", token: token(map[string]any{"sub": "alice-subject\n"}), want: oidcSubject},
		{name: "subject lone surrogate", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject\ud800","aud":"skygo","exp":1800000600,"iat":1799999940}`, rs), want: oidcSubject},

		{name: "audience missing", token: token(map[string]any{"aud": nil}), want: oidcAudience},
		{name: "audience other", token: token(map[string]any{"aud": "other"}), want: oidcAudience},
		{name: "audience prefix", token: token(map[string]any{"aud": "skygo-admin"}), want: oidcAudience},
		{name: "audience array without", token: token(map[string]any{"aud": []string{"a", "b"}, "azp": "skygo"}), want: oidcAudience},
		{name: "audience array without azp", token: token(map[string]any{"aud": []string{"skygo", "other"}}), want: oidcAudience},
		{name: "audience array other azp", token: token(map[string]any{"aud": []string{"skygo", "other"}, "azp": "other"}), want: oidcAudience},
		{name: "single audience other azp", token: token(map[string]any{"azp": "other"}), want: oidcAudience},
		{name: "azp not string", token: token(map[string]any{"azp": []string{"skygo"}}), want: oidcAudience},
		{name: "audience empty array", token: token(map[string]any{"aud": []string{}}), want: oidcAudience},
		{name: "audience duplicate", token: token(map[string]any{"aud": []string{"skygo", "skygo"}, "azp": "skygo"}), want: oidcAudience},
		{name: "audience number", token: token(map[string]any{"aud": 1}), want: oidcAudience},
		{name: "audience mixed array", token: token(map[string]any{"aud": []any{"skygo", 1}, "azp": "skygo"}), want: oidcAudience},
		{name: "audience too many", token: token(map[string]any{"aud": append([]string{"skygo"}, strings.Split("a b c d e f g h i j k l m n o p", " ")...), "azp": "skygo"}), want: oidcAudience},

		{name: "exp missing", token: token(map[string]any{"exp": nil}), want: oidcLifetime},
		{name: "iat missing", token: token(map[string]any{"iat": nil}), want: oidcLifetime},
		{name: "exp string", token: token(map[string]any{"exp": strconv.Itoa(oidcNow + 600)}), want: oidcLifetime},
		{name: "exp fraction", token: token(map[string]any{"exp": oidcNow + 600.5}), want: oidcLifetime},
		{name: "exp negative", token: token(map[string]any{"exp": -1}), want: oidcLifetime},
		{name: "exp exponent", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","aud":"skygo","exp":1.8e9,"iat":1799999940}`, rs), want: oidcLifetime},
		{name: "exp leading zero", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","aud":"skygo","exp":01800000600,"iat":1799999940}`, rs), want: oidcMalformed},
		{name: "exp beyond year 9999", token: token(map[string]any{"iat": int64(maxNumericDate) - 59, "exp": int64(maxNumericDate) + 1}), want: oidcLifetime},
		{name: "exp huge", token: jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","aud":"skygo","exp":99999999999999999999999,"iat":1799999940}`, rs), want: oidcLifetime},
		{name: "exp equals iat", token: token(map[string]any{"exp": oidcNow, "iat": oidcNow}), want: oidcLifetime},
		{name: "exp before iat", token: token(map[string]any{"exp": oidcNow + 10, "iat": oidcNow + 20}), want: oidcLifetime},
		{name: "lifetime above cap", token: token(map[string]any{"iat": oidcNow - 60, "exp": oidcNow - 60 + 3601}), want: oidcLifetime},
		{name: "lifetime 25h", token: token(map[string]any{"iat": oidcNow - 60, "exp": oidcNow - 60 + 25*3600}), want: oidcLifetime},
		{name: "nbf at exp", token: token(map[string]any{"nbf": oidcNow + 600}), want: oidcLifetime},
		{name: "nbf string", token: token(map[string]any{"nbf": "0"}), want: oidcLifetime},

		{name: "expired at skew", token: valid, now: at(oidcNow + 600 + 30), want: oidcExpired},
		{name: "expired just after skew", token: valid, now: at(oidcNow + 630).Add(time.Nanosecond), want: oidcExpired},
		{name: "expired long ago", token: valid, now: at(oidcNow + 86400), want: oidcExpired},
		{name: "iat future", token: token(map[string]any{"iat": oidcNow + 31}), want: oidcNotYetValid},
		{name: "nbf future", token: token(map[string]any{"nbf": oidcNow + 31}), want: oidcNotYetValid},
		{name: "before issue", token: valid, now: at(oidcNow - 91), want: oidcNotYetValid},

		{name: "unmapped subject", token: token(map[string]any{"sub": "mallory", "email": "alice@example.com", "preferred_username": "alice", "name": "alice"}), want: oidcUnmapped},
		{name: "subject case", token: token(map[string]any{"sub": "Alice-subject"}), want: oidcUnmapped},
		{name: "principal name as subject", token: token(map[string]any{"sub": "alice"}), want: oidcUnmapped},
		{name: "dangling principal", token: token(map[string]any{"sub": "carol-subject"}), want: oidcPrincipal},
	}
	seen := make(map[oidcReason]bool)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := tt.now
			if now.IsZero() {
				now = f.now
			}
			_, got := checkIDToken(tt.token, f.config, f.keys, f.principals, now)
			if got != tt.want {
				t.Fatalf("reason = %d, want %d", got, tt.want)
			}
			id, err := verifyIDToken(tt.token, f.config, f.keys, f.principals, now)
			if tt.want == oidcOK {
				if err != nil || id.principal == "" {
					t.Fatalf("verifyIDToken = %+v, %v", id, err)
				}
				return
			}
			if err != ErrUnauthenticated || id != (oidcIdentity{}) {
				t.Fatalf("verifyIDToken = %+v, %v; want zero identity, ErrUnauthenticated", id, err)
			}
			requireNoTokenText(t, err.Error(), tt.token)
		})
		seen[tt.want] = true
	}
	for r := oidcOK; r <= oidcPrincipal; r++ {
		if r != oidcUnavailable && !seen[r] {
			t.Errorf("no row for reason %d", r)
		}
	}
}

// requireNoTokenText fails if text contains the token, any of its segments,
// or any decoded claim string value.
func requireNoTokenText(t *testing.T, text, token string) {
	t.Helper()
	needles := []string{token}
	for i, seg := range strings.Split(token, ".") {
		if len(seg) >= 4 {
			needles = append(needles, seg)
		}
		if b, err := base64.RawURLEncoding.DecodeString(seg); err == nil && i == 1 {
			var claims map[string]any
			if json.Unmarshal(b, &claims) == nil {
				for _, v := range claims {
					if s, ok := v.(string); ok && len(s) >= 3 {
						needles = append(needles, s)
					}
				}
			}
		}
	}
	for _, n := range needles {
		// An empty needle is contained in every string and proves nothing.
		if n != "" && strings.Contains(text, n) {
			t.Fatalf("error text %q contains token-derived text", text)
		}
	}
}

func TestVerifyIDTokenIdentity(t *testing.T) {
	f := newOIDCFixture(t)
	tok := jws(rsaHeader, oidcClaims(t, nil), func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) })
	id, err := verifyIDToken(tok, f.config, f.keys, f.principals, f.now)
	if err != nil {
		t.Fatal(err)
	}
	pub := rsaJWKJSON(&oidcKeys.rsa.PublicKey, nil)
	want := oidcIdentity{
		principal:  "alice",
		role:       Operator,
		generation: 3,
		subject:    sha256.Sum256([]byte(oidcIssuerURL + "\x00alice-subject")),
		token:      sha256.Sum256([]byte("skygo-oidc-v1\x00" + tok)),
		kid:        "rsa-1",
		key:        sha256.Sum256([]byte(`{"e":"` + pub["e"].(string) + `","kty":"RSA","n":"` + pub["n"].(string) + `"}`)),
		expires:    time.Unix(oidcNow+600+30, 0),
	}
	if !id.expires.Equal(want.expires) {
		t.Fatalf("expires = %v, want %v", id.expires, want.expires)
	}
	id.expires = want.expires
	if id != want {
		t.Fatalf("identity = %+v\nwant %+v", id, want)
	}

	// The role comes from the credential file, not from the token.
	f.principals.Principals[2].Role = Admin
	bob := jws(rsaHeader, oidcClaims(t, map[string]any{"sub": "bob-subject", "role": "reader", "groups": []string{"reader"}}), func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) })
	id, err = verifyIDToken(bob, f.config, f.keys, f.principals, f.now)
	if err != nil || id.principal != "bob" || id.role != Admin {
		t.Fatalf("bob = %+v, %v", id, err)
	}

	// A case-variant claim name is not the claim; struct decoding would fold it.
	folded := jws(rsaHeader, `{"iss":"`+oidcIssuerURL+`","sub":"alice-subject","SUB":"bob-subject","Aud":"other","aud":"skygo","exp":1800000600,"iat":1799999940}`, func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) })
	id, err = verifyIDToken(folded, f.config, f.keys, f.principals, f.now)
	if err != nil || id.principal != "alice" {
		t.Fatalf("folded = %+v, %v", id, err)
	}
}

func TestVerifyIDTokenUnavailable(t *testing.T) {
	f := newOIDCFixture(t)
	tok := jws(rsaHeader, oidcClaims(t, nil), func(input string) []byte { return signRS256(t, oidcKeys.rsa, input) })
	other := *f.principals
	other.Installation = "fedcba9876543210fedcba9876543210"
	for _, tt := range []struct {
		name       string
		c          *oidcConfig
		keys       *jwkSet
		principals *config
	}{
		{"config", nil, f.keys, f.principals},
		{"keys", f.config, nil, f.principals},
		{"principals", f.config, f.keys, nil},
		{"installation", f.config, f.keys, &other},
	} {
		if _, err := verifyIDToken(tok, tt.c, tt.keys, tt.principals, f.now); err != ErrUnavailable {
			t.Errorf("%s: err = %v, want ErrUnavailable", tt.name, err)
		}
	}
}

func TestVerifyIDTokenSingleKeyWithoutKid(t *testing.T) {
	oidcTestKeys(t)
	f := newOIDCFixture(t)
	keys, err := parseJWKS(jwksJSON(t, ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"})))
	if err != nil {
		t.Fatal(err)
	}
	es := func(input string) []byte { return signES256(t, oidcKeys.ec, input) }
	if _, err := verifyIDToken(jws(`{"alg":"ES256"}`, oidcClaims(t, nil), es), f.config, keys, f.principals, f.now); err != nil {
		t.Fatalf("sole key without kid: %v", err)
	}
	if _, err := verifyIDToken(jws(`{"alg":"ES256","kid":"ec-2"}`, oidcClaims(t, nil), es), f.config, keys, f.principals, f.now); err != ErrUnauthenticated {
		t.Fatalf("sole key with other kid: %v", err)
	}
}

// FuzzVerifyIDToken checks that parsing is total and that only an exactly
// minted signing input verifies.
func FuzzVerifyIDToken(f *testing.F) {
	oidcTestKeys(f)
	data, err := os.ReadFile("testdata/oidc/config.json")
	if err != nil {
		f.Fatal(err)
	}
	c, err := parseOIDCConfig(data, installation)
	if err != nil {
		f.Fatal(err)
	}
	keys, err := parseJWKS(jwksJSON(f,
		rsaJWKJSON(&oidcKeys.rsa.PublicKey, map[string]any{"kid": "rsa-1"}),
		ecJWKJSON(&oidcKeys.ec.PublicKey, map[string]any{"kid": "ec-1"})))
	if err != nil {
		f.Fatal(err)
	}
	principals := &config{Version: 1, Installation: installation, Generation: 1, Principals: []principal{{"legacy", Admin}, {"alice", Operator}}}
	minted := make(map[string]bool)
	for _, tok := range []string{
		jws(rsaHeader, oidcClaims(f, nil), func(input string) []byte { return signRS256(f, oidcKeys.rsa, input) }),
		jws(`{"alg":"ES256","kid":"ec-1"}`, oidcClaims(f, map[string]any{"aud": []string{"skygo", "x"}, "azp": "skygo"}), func(input string) []byte { return signES256(f, oidcKeys.ec, input) }),
	} {
		minted[tok[:strings.LastIndexByte(tok, '.')]] = true
		f.Add(tok)
	}
	f.Fuzz(func(t *testing.T, tok string) {
		id, err := verifyIDToken(tok, c, keys, principals, time.Unix(oidcNow, 0))
		if err != nil {
			if err != ErrUnauthenticated {
				t.Fatalf("err = %v", err)
			}
			return
		}
		if !minted[tok[:strings.LastIndexByte(tok, '.')]] || id.principal != "alice" {
			t.Fatalf("accepted unminted token as %q", id.principal)
		}
	})
}
