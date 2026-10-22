package auth

import (
	"crypto"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
)

const (
	maxJWKSBytes = 64 << 10
	maxJWKSKeys  = 32
	minRSABits   = 2048
	maxRSABits   = 8192
)

// jwk is one usable verification key from an operator-configured JWKS. Its
// algorithm is fixed when the set is parsed, never chosen by a token.
type jwk struct {
	kid        string
	alg        string // "RS256" or "ES256"
	rsa        *rsa.PublicKey
	ec         *ecdsa.PublicKey
	thumbprint [32]byte // RFC 7638 SHA-256 thumbprint
}

// jwkSet holds the usable RS256 and ES256 verification keys of one JWKS
// document. Keys for other uses or algorithms are skipped, not trusted.
type jwkSet struct {
	keys []jwk
}

// parseJWKS parses a bounded JWKS document (RFC 7517). Member names are exact
// and unique at every depth. Members other than "keys" are ignored, as RFC
// 7517 section 5 requires. Private key members, malformed known members,
// duplicate key IDs and duplicate keys reject the whole set. Errors never
// contain key text.
func parseJWKS(data []byte) (*jwkSet, error) {
	if len(data) > maxJWKSBytes {
		return nil, ErrInvalid
	}
	top, ok := jsonObject(data)
	if !ok || top["keys"] == nil {
		return nil, ErrInvalid
	}
	var raws []json.RawMessage
	if !jsonArray(top["keys"], &raws) || len(raws) == 0 || len(raws) > maxJWKSKeys {
		return nil, ErrInvalid
	}
	set := new(jwkSet)
	kids := make(map[string]bool)
	thumbprints := make(map[[32]byte]bool)
	for _, raw := range raws {
		k, usable, err := parseJWK(raw)
		if err != nil {
			return nil, err
		}
		if k.kid != "" {
			if kids[k.kid] {
				return nil, ErrInvalid
			}
			kids[k.kid] = true
		}
		if !usable {
			continue
		}
		if thumbprints[k.thumbprint] {
			return nil, ErrInvalid
		}
		thumbprints[k.thumbprint] = true
		set.keys = append(set.keys, k)
	}
	if len(set.keys) == 0 {
		return nil, ErrInvalid
	}
	return set, nil
}

// lookup selects exactly one key. A token key ID must equal a usable key's ID;
// a token without one selects only the sole usable key. There is no fallback
// to trying other keys.
func (s *jwkSet) lookup(kid string) (*jwk, bool) {
	if kid == "" {
		if len(s.keys) != 1 {
			return nil, false
		}
		return &s.keys[0], true
	}
	for i := range s.keys {
		if s.keys[i].kid == kid {
			return &s.keys[i], true
		}
	}
	return nil, false
}

// verify checks a JWS signature over input with the key's fixed algorithm.
// ES256 signatures are the 64-byte R||S form of RFC 7518 section 3.4.
func (k *jwk) verify(input string, sig []byte) bool {
	digest := sha256.Sum256([]byte(input))
	switch k.alg {
	case "RS256":
		return k.rsa != nil && len(sig) == k.rsa.Size() && rsa.VerifyPKCS1v15(k.rsa, crypto.SHA256, digest[:], sig) == nil
	case "ES256":
		if k.ec == nil || len(sig) != 64 {
			return false
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		return ecdsa.Verify(k.ec, digest[:], r, s)
	}
	return false
}

// jwkRefused are members that reject the whole set: private key parameters
// (RFC 7518 sections 6.2.2, 6.3.2 and 6.4.1) and key URLs.
var jwkRefused = []string{"d", "p", "q", "dp", "dq", "qi", "oth", "k", "x5u", "jku"}

// parseJWK parses one key. It reports usable for a verification key with a
// supported algorithm and an error for any malformed or private key. The key
// comes only from kty, n, e, crv, x and y. Other members, including
// certificates and issuer metadata, are ignored as RFC 7517 section 4
// requires, and never select a key, algorithm, issuer or URL.
func parseJWK(raw json.RawMessage) (jwk, bool, error) {
	m, ok := jsonObject(raw)
	if !ok {
		return jwk{}, false, ErrInvalid
	}
	for _, name := range jwkRefused {
		if _, ok := m[name]; ok {
			return jwk{}, false, ErrInvalid
		}
	}
	kty, ok := jsonString(m["kty"])
	if !ok {
		return jwk{}, false, ErrInvalid
	}
	var k jwk
	if raw, ok := m["kid"]; ok {
		if k.kid, ok = jsonString(raw); !ok || !claimString(k.kid) {
			return jwk{}, false, ErrInvalid
		}
	}
	usable, err := jwkVerifies(m)
	if err != nil {
		return jwk{}, false, err
	}
	alg := ""
	if raw, ok := m["alg"]; ok {
		if alg, ok = jsonString(raw); !ok || alg == "" {
			return jwk{}, false, ErrInvalid
		}
	}
	switch kty {
	case "RSA":
		if m["crv"] != nil || m["x"] != nil || m["y"] != nil {
			return jwk{}, false, ErrInvalid
		}
		switch alg {
		case "", "RS256":
		case "ES256":
			return jwk{}, false, ErrInvalid
		default:
			return k, false, nil
		}
		if !usable {
			return k, false, nil
		}
		k.alg = "RS256"
		k.rsa, k.thumbprint, ok = rsaJWK(m)
	case "EC":
		if m["n"] != nil || m["e"] != nil {
			return jwk{}, false, ErrInvalid
		}
		crv, valid := jsonString(m["crv"])
		if !valid {
			return jwk{}, false, ErrInvalid
		}
		switch {
		case alg == "RS256":
			return jwk{}, false, ErrInvalid
		case alg == "ES256" && crv != "P-256":
			return jwk{}, false, ErrInvalid
		case alg != "" && alg != "ES256", crv != "P-256", !usable:
			return k, false, nil
		}
		k.alg = "ES256"
		k.ec, k.thumbprint, ok = ecJWK(m)
	default:
		if alg == "RS256" || alg == "ES256" {
			return jwk{}, false, ErrInvalid
		}
		return k, false, nil
	}
	if !ok {
		return jwk{}, false, ErrInvalid
	}
	return k, true, nil
}

// jwkVerifies reports whether "use" and "key_ops" permit signature
// verification. Absent members permit it; malformed members are errors.
func jwkVerifies(m map[string]json.RawMessage) (bool, error) {
	ok := true
	if raw, present := m["use"]; present {
		use, valid := jsonString(raw)
		if !valid || use == "" {
			return false, ErrInvalid
		}
		ok = use == "sig"
	}
	if raw, present := m["key_ops"]; present {
		var ops []json.RawMessage
		if !jsonArray(raw, &ops) || len(ops) == 0 || len(ops) > 8 {
			return false, ErrInvalid
		}
		seen := make(map[string]bool)
		for _, raw := range ops {
			op, valid := jsonString(raw)
			if !valid || op == "" || seen[op] {
				return false, ErrInvalid
			}
			seen[op] = true
		}
		ok = ok && seen["verify"]
	}
	return ok, nil
}

// rsaJWK builds an RSA public key from canonical, minimal n and e. The
// modulus must have 2048 to 8192 bits and the exponent must be 65537.
func rsaJWK(m map[string]json.RawMessage) (*rsa.PublicKey, [32]byte, bool) {
	ns, ok1 := jsonString(m["n"])
	es, ok2 := jsonString(m["e"])
	if !ok1 || !ok2 {
		return nil, [32]byte{}, false
	}
	n, ok1 := decodeSegment(ns)
	e, ok2 := decodeSegment(es)
	if !ok1 || !ok2 || len(n) == 0 || n[0] == 0 || len(e) != 3 || e[0] != 1 || e[1] != 0 || e[2] != 1 {
		return nil, [32]byte{}, false
	}
	key := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: 65537}
	if bits := key.N.BitLen(); bits < minRSABits || bits > maxRSABits || key.N.Bit(0) == 0 {
		return nil, [32]byte{}, false
	}
	thumbprint := sha256.Sum256([]byte(`{"e":"` + es + `","kty":"RSA","n":"` + ns + `"}`))
	return key, thumbprint, true
}

// ecJWK builds a P-256 public key from 32-byte coordinates. crypto/ecdh
// rejects coordinates out of range and points not on the curve.
func ecJWK(m map[string]json.RawMessage) (*ecdsa.PublicKey, [32]byte, bool) {
	xs, ok1 := jsonString(m["x"])
	ys, ok2 := jsonString(m["y"])
	if !ok1 || !ok2 {
		return nil, [32]byte{}, false
	}
	x, ok1 := decodeSegment(xs)
	y, ok2 := decodeSegment(ys)
	if !ok1 || !ok2 || len(x) != 32 || len(y) != 32 {
		return nil, [32]byte{}, false
	}
	point := append(append([]byte{4}, x...), y...)
	if _, err := ecdh.P256().NewPublicKey(point); err != nil {
		return nil, [32]byte{}, false
	}
	key := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
	thumbprint := sha256.Sum256([]byte(`{"crv":"P-256","kty":"EC","x":"` + xs + `","y":"` + ys + `"}`))
	return key, thumbprint, true
}

// decodeSegment decodes canonical unpadded base64url (RFC 7515 section 2).
// Padding, line breaks, the standard alphabet and nonzero trailing bits fail.
func decodeSegment(s string) ([]byte, bool) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, false
	}
	return b, true
}
