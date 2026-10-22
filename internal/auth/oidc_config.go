package auth

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxOIDCConfigBytes  = 64 << 10
	maxOIDCIdentities   = 256
	maxOIDCURLBytes     = 1024
	maxOIDCPathBytes    = 4096
	defaultOIDCLifetime = time.Hour
	maxOIDCLifetime     = 24 * time.Hour
	defaultOIDCSkew     = 30 * time.Second
	maxOIDCSkew         = 5 * time.Minute
)

// oidcConfig is a validated OIDC configuration for one issuer, bound to one
// installation. Exactly one of jwksURI and jwksFile is set. Keys are never
// discovered from the issuer.
type oidcConfig struct {
	installation string
	generation   uint64
	issuer       string
	audience     string
	jwksURI      string // HTTPS only
	jwksFile     string
	caFile       string // only with jwksURI
	maxLifetime  time.Duration
	skew         time.Duration
	identities   map[string]string // subject -> principal
}

// parseOIDCConfig parses a bounded OIDC configuration document strictly.
// Member names are exact and unique and unknown members are refused. The
// result maps explicit subjects to principals; it infers nothing from email
// or other claims. Errors are ErrInvalid and never contain document text.
func parseOIDCConfig(data []byte, installation string) (*oidcConfig, error) {
	if len(data) > maxOIDCConfigBytes || !hexID(installation) {
		return nil, ErrInvalid
	}
	m, ok := jsonObject(data)
	if !ok {
		return nil, ErrInvalid
	}
	for name := range m {
		switch name {
		case "version", "installation", "generation", "issuer", "audience", "jwks_uri", "jwks_file", "ca_file", "max_token_lifetime", "clock_skew", "identities":
		default:
			return nil, ErrInvalid
		}
	}
	c := &oidcConfig{maxLifetime: defaultOIDCLifetime, skew: defaultOIDCSkew}
	if v, ok := jsonUint(m["version"]); !ok || v != 1 {
		return nil, ErrInvalid
	}
	if c.installation, ok = jsonString(m["installation"]); !ok || c.installation != installation {
		return nil, ErrInvalid
	}
	if c.generation, ok = jsonUint(m["generation"]); !ok || c.generation == 0 {
		return nil, ErrInvalid
	}
	if c.issuer, ok = jsonString(m["issuer"]); !ok || !httpsURL(c.issuer, false) {
		return nil, ErrInvalid
	}
	if c.audience, ok = jsonString(m["audience"]); !ok || !claimString(c.audience) {
		return nil, ErrInvalid
	}
	for _, f := range []struct {
		name string
		dst  *string
	}{{"jwks_uri", &c.jwksURI}, {"jwks_file", &c.jwksFile}, {"ca_file", &c.caFile}} {
		raw, present := m[f.name]
		if !present {
			continue
		}
		if *f.dst, ok = jsonString(raw); !ok || *f.dst == "" {
			return nil, ErrInvalid
		}
	}
	switch {
	case (c.jwksURI == "") == (c.jwksFile == ""):
		return nil, ErrInvalid
	case c.jwksURI != "" && !httpsURL(c.jwksURI, true):
		return nil, ErrInvalid
	case c.jwksFile != "" && (c.caFile != "" || !localPath(c.jwksFile)):
		return nil, ErrInvalid
	case c.caFile != "" && !localPath(c.caFile):
		return nil, ErrInvalid
	}
	if raw, present := m["max_token_lifetime"]; present {
		if c.maxLifetime, ok = jsonDuration(raw); !ok || c.maxLifetime <= 0 || c.maxLifetime > maxOIDCLifetime {
			return nil, ErrInvalid
		}
	}
	if raw, present := m["clock_skew"]; present {
		if c.skew, ok = jsonDuration(raw); !ok || c.skew < 0 || c.skew > maxOIDCSkew {
			return nil, ErrInvalid
		}
	}
	if c.identities, ok = oidcIdentities(m["identities"]); !ok {
		return nil, ErrInvalid
	}
	return c, nil
}

// oidcIdentities parses the explicit subject-to-principal mapping. Subjects
// are unique; several subjects may name one principal. The bootstrap legacy
// principal cannot be a target, as with AddPrincipal.
func oidcIdentities(raw json.RawMessage) (map[string]string, bool) {
	var list []json.RawMessage
	if !jsonArray(raw, &list) || len(list) == 0 || len(list) > maxOIDCIdentities {
		return nil, false
	}
	out := make(map[string]string, len(list))
	for _, raw := range list {
		m, ok := jsonObject(raw)
		if !ok || len(m) != 2 {
			return nil, false
		}
		sub, ok1 := jsonString(m["subject"])
		p, ok2 := jsonString(m["principal"])
		if !ok1 || !ok2 || !claimString(sub) || !principalID(p) || p == "legacy" {
			return nil, false
		}
		if _, dup := out[sub]; dup {
			return nil, false
		}
		out[sub] = p
	}
	return out, true
}

// httpsURL reports whether s is an absolute HTTPS URL with a host and no
// user information or fragment. An issuer also has no query (OIDC
// Discovery section 3).
func httpsURL(s string, query bool) bool {
	if len(s) > maxOIDCURLBytes || !printableASCII(s) {
		return false
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Opaque != "" || strings.Contains(s, "#") {
		return false
	}
	return query || !strings.Contains(s, "?")
}

// printableASCII reports whether s consists only of printable ASCII.
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] <= 0x20 || s[i] >= 0x7f {
			return false
		}
	}
	return s != ""
}

// localPath reports whether s is a bounded file path without NUL bytes.
func localPath(s string) bool {
	return len(s) <= maxOIDCPathBytes && !strings.ContainsRune(s, 0)
}

// jsonUint decodes a nonnegative JSON integer without sign, fraction,
// exponent or leading zeros.
func jsonUint(raw json.RawMessage) (uint64, bool) {
	s := string(raw)
	if len(s) == 0 || len(s) > 20 || len(s) > 1 && s[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(s, 10, 64)
	return n, err == nil
}

// jsonDuration decodes a Go duration string of whole seconds, such as "1h".
func jsonDuration(raw json.RawMessage) (time.Duration, bool) {
	s, ok := jsonString(raw)
	if !ok {
		return 0, false
	}
	d, err := time.ParseDuration(s)
	if err != nil || d%time.Second != 0 {
		return 0, false
	}
	return d, true
}
