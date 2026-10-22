package auth

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"
)

// OpenID Connect ID-token verification for one configured issuer. This file
// verifies a compact JWS against an operator-supplied key set and maps the
// verified (issuer, subject) to an existing principal. It performs no I/O.
// Tokens never select keys by URL or embedded key material, and no failure
// formats token bytes, claims or subjects.

const (
	maxIDTokenBytes    = 8 << 10
	maxJOSEHeaderBytes = 1 << 10 // the token cap bounds claims below 6 KiB
	maxClaimString     = 255
	maxAudiences       = 16
	maxNumericDate     = 253402300799 // 9999-12-31T23:59:59Z
)

// oidcReason is a bounded failure class for tests and counters. It carries no
// token-derived text.
type oidcReason uint8

const (
	oidcOK          oidcReason = iota
	oidcUnavailable            // no configuration, keys or principals
	oidcMalformed              // size, segments, base64url or JSON
	oidcHeader                 // forbidden or invalid header parameter
	oidcAlgorithm              // unsupported alg or alg differs from key
	oidcKey                    // no exactly selected key
	oidcSignature
	oidcIssuer
	oidcAudience
	oidcSubject
	oidcLifetime // exp, iat, nbf inconsistent or lifetime too long
	oidcExpired
	oidcNotYetValid // iat or nbf in the future
	oidcUnmapped    // (issuer, subject) not configured
	oidcPrincipal   // mapped principal absent from the credential file
)

// oidcIdentity is the result of a verified ID token. It holds digests, not
// token bytes or the subject.
type oidcIdentity struct {
	principal  string
	role       Role
	generation uint64   // OIDC configuration generation
	subject    [32]byte // SHA-256(iss || 0 || sub)
	token      [32]byte // SHA-256 of the domain-separated token
	kid        string
	key        [32]byte  // RFC 7638 thumbprint of the verifying key
	expires    time.Time // exp plus clock skew; verification fails from then on
}

// verifyIDToken verifies an OIDC ID token (not an access token) and maps it
// to a principal in the current credential file snapshot. Every token failure
// is ErrUnauthenticated; missing configuration is ErrUnavailable.
func verifyIDToken(token string, c *oidcConfig, keys *jwkSet, principals *config, now time.Time) (oidcIdentity, error) {
	id, reason := checkIDToken(token, c, keys, principals, now)
	switch reason {
	case oidcOK:
		return id, nil
	case oidcUnavailable:
		return oidcIdentity{}, ErrUnavailable
	default:
		return oidcIdentity{}, ErrUnauthenticated
	}
}

func checkIDToken(token string, c *oidcConfig, keys *jwkSet, principals *config, now time.Time) (oidcIdentity, oidcReason) {
	if c == nil || keys == nil || principals == nil || c.installation != principals.Installation {
		return oidcIdentity{}, oidcUnavailable
	}
	key, payload, reason := verifyJWS(token, keys)
	if reason != oidcOK {
		return oidcIdentity{}, reason
	}
	claims, ok := jsonObject(payload)
	if !ok {
		return oidcIdentity{}, oidcMalformed
	}

	iss, ok := jsonString(claims["iss"])
	if !ok || iss != c.issuer {
		return oidcIdentity{}, oidcIssuer
	}
	sub, ok := jsonString(claims["sub"])
	if !ok || !claimString(sub) {
		return oidcIdentity{}, oidcSubject
	}
	if !audienceMatches(claims, c.audience) {
		return oidcIdentity{}, oidcAudience
	}

	exp, ok1 := numericDate(claims["exp"])
	iat, ok2 := numericDate(claims["iat"])
	if !ok1 || !ok2 || exp <= iat || exp-iat > int64(c.maxLifetime/time.Second) {
		return oidcIdentity{}, oidcLifetime
	}
	nbf := iat
	if raw, present := claims["nbf"]; present {
		if nbf, ok = numericDate(raw); !ok || nbf >= exp {
			return oidcIdentity{}, oidcLifetime
		}
	}
	expires := time.Unix(exp, 0).Add(c.skew)
	if !now.Before(expires) {
		return oidcIdentity{}, oidcExpired
	}
	latest := now.Add(c.skew)
	if time.Unix(iat, 0).After(latest) || time.Unix(nbf, 0).After(latest) {
		return oidcIdentity{}, oidcNotYetValid
	}

	mapped, ok := c.identities[sub]
	if !ok {
		return oidcIdentity{}, oidcUnmapped
	}
	role := principals.role(mapped)
	if !validRole(role) {
		return oidcIdentity{}, oidcPrincipal
	}
	return oidcIdentity{
		principal:  mapped,
		role:       role,
		generation: c.generation,
		subject:    sha256.Sum256([]byte(iss + "\x00" + sub)),
		token:      sha256.Sum256([]byte("skygo-oidc-v1\x00" + token)),
		kid:        key.kid,
		key:        key.thumbprint,
		expires:    expires,
	}, oidcOK
}

// verifyJWS checks a compact JWS (RFC 7515 section 7.1) and returns its
// selected key and unparsed payload. The protected header is the only header;
// it must name RS256 or ES256, matching the selected key's algorithm.
func verifyJWS(token string, keys *jwkSet) (*jwk, []byte, oidcReason) {
	if len(token) == 0 || len(token) > maxIDTokenBytes {
		return nil, nil, oidcMalformed
	}
	first := strings.IndexByte(token, '.')
	last := strings.LastIndexByte(token, '.')
	if first <= 0 || last == first || last == len(token)-1 || strings.Count(token, ".") != 2 {
		return nil, nil, oidcMalformed
	}
	if first > base64Len(maxJOSEHeaderBytes) {
		return nil, nil, oidcMalformed
	}
	headerJSON, ok1 := decodeSegment(token[:first])
	payload, ok2 := decodeSegment(token[first+1 : last])
	sig, ok3 := decodeSegment(token[last+1:])
	if !ok1 || !ok2 || !ok3 {
		return nil, nil, oidcMalformed
	}
	header, ok := jsonObject(headerJSON)
	if !ok {
		return nil, nil, oidcMalformed
	}
	alg, kid, reason := joseHeader(header)
	if reason != oidcOK {
		return nil, nil, reason
	}
	key, ok := keys.lookup(kid)
	if !ok {
		return nil, nil, oidcKey
	}
	if key.alg != alg {
		return nil, nil, oidcAlgorithm
	}
	if !key.verify(token[:last], sig) {
		return nil, nil, oidcSignature
	}
	return key, payload, oidcOK
}

// joseHeader validates the protected header and returns alg and kid.
// Parameters that carry key material or key URLs, critical extensions,
// nested content and JWE parameters reject the token. Other unrecognized
// parameters are ignored (RFC 7515 section 4).
func joseHeader(h map[string]json.RawMessage) (alg, kid string, reason oidcReason) {
	for _, name := range []string{"crit", "jku", "jwk", "x5u", "x5c", "cty", "enc", "zip"} {
		if _, ok := h[name]; ok {
			return "", "", oidcHeader
		}
	}
	alg, ok := jsonString(h["alg"])
	if !ok {
		return "", "", oidcAlgorithm
	}
	if alg != "RS256" && alg != "ES256" {
		return "", "", oidcAlgorithm
	}
	if raw, present := h["typ"]; present {
		// ID tokens only: at+jwt and other media types are refused.
		typ, ok := jsonString(raw)
		if !ok || typ != "JWT" && typ != "JOSE" {
			return "", "", oidcHeader
		}
	}
	if raw, present := h["kid"]; present {
		if kid, ok = jsonString(raw); !ok || !claimString(kid) {
			return "", "", oidcHeader
		}
	}
	return alg, kid, oidcOK
}

// audienceMatches applies OIDC Core 3.1.3.7 steps 3 to 5: aud contains the
// audience; several audiences require azp; any azp equals the audience.
func audienceMatches(claims map[string]json.RawMessage, audience string) bool {
	raw, ok := claims["aud"]
	if !ok {
		return false
	}
	var auds []string
	if s, ok := jsonString(raw); ok {
		auds = []string{s}
	} else {
		var list []json.RawMessage
		if !jsonArray(raw, &list) || len(list) == 0 || len(list) > maxAudiences {
			return false
		}
		seen := make(map[string]bool)
		for _, raw := range list {
			s, ok := jsonString(raw)
			if !ok || seen[s] {
				return false
			}
			seen[s] = true
			auds = append(auds, s)
		}
	}
	found := false
	for _, a := range auds {
		if !claimString(a) {
			return false
		}
		found = found || a == audience
	}
	if !found {
		return false
	}
	raw, present := claims["azp"]
	if !present {
		return len(auds) == 1
	}
	azp, ok := jsonString(raw)
	return ok && azp == audience
}

// jsonObject decodes one JSON object with unique, exactly named members.
// Typed struct decoding is avoided because encoding/json matches member names
// case-insensitively, which would let "ISS" stand in for "iss".
func jsonObject(data []byte) (map[string]json.RawMessage, bool) {
	if !utf8.Valid(data) || !uniqueJSON(data) {
		return nil, false
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(data, &m) != nil || m == nil {
		return nil, false
	}
	return m, true
}

// jsonArray decodes a JSON array of raw values.
func jsonArray(raw json.RawMessage, out *[]json.RawMessage) bool {
	if len(raw) == 0 || raw[0] != '[' {
		return false
	}
	return json.Unmarshal(raw, out) == nil
}

// jsonString decodes a JSON string. U+FFFD is refused because encoding/json
// substitutes it for lone surrogate escapes, which would merge distinct
// values.
func jsonString(raw json.RawMessage) (string, bool) {
	var s string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &s) != nil || strings.ContainsRune(s, utf8.RuneError) {
		return "", false
	}
	return s, true
}

// claimString reports whether s is a nonempty identifier of at most 255 bytes
// without control characters.
func claimString(s string) bool {
	if len(s) == 0 || len(s) > maxClaimString {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// numericDate decodes a NumericDate as integer seconds in [0, year 9999].
// Fractions, exponents, signs and leading zeros are refused, so later time
// arithmetic cannot overflow.
func numericDate(raw json.RawMessage) (int64, bool) {
	n, ok := jsonUint(raw)
	if !ok || n > maxNumericDate {
		return 0, false
	}
	return int64(n), true
}

// base64Len is the unpadded base64url length of n bytes.
func base64Len(n int) int { return (n*8 + 5) / 6 }
