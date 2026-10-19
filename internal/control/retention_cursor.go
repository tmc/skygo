package control

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/auth"
)

type retentionCursor struct {
	Version   uint8  `json:"v"`
	Kind      string `json:"k"`
	Owner     string `json:"i"`
	Lineage   string `json:"l"`
	Principal string `json:"p"`
	Global    bool   `json:"g"`
	After     uint64 `json:"a"`
	Max       uint64 `json:"m"`
	Selector  string `json:"s,omitempty"`
	Scoped    bool   `json:"h,omitempty"`
}
type logCursor struct {
	Version    uint8  `json:"v"`
	Kind       string `json:"k"`
	Owner      string `json:"i"`
	Lineage    string `json:"l"`
	Principal  string `json:"p"`
	Global     bool   `json:"g"`
	Job        string `json:"j"`
	Attempt    string `json:"a"`
	Generation uint64 `json:"n"`
	Offset     int64  `json:"o"`
	Scoped     bool   `json:"h,omitempty"`
}
type legacyCursor struct {
	After     uint64 `json:"a"`
	Max       uint64 `json:"m"`
	Principal string `json:"p,omitempty"`
	Global    bool   `json:"g,omitempty"`
}

func (c *Control) scopeCursor(actor auth.Actor, kind, lineage string) retentionCursor {
	return retentionCursor{Version: 1, Kind: kind, Owner: c.store.Owner(), Lineage: lineage, Principal: actor.Principal(), Global: actor.Role() == auth.Admin, Scoped: c.store.Scoped()}
}
func (c *Control) scopeLogCursor(actor auth.Actor, lineage string) logCursor {
	p := c.scopeCursor(actor, "logs", lineage)
	return logCursor{Version: p.Version, Kind: p.Kind, Owner: p.Owner, Lineage: p.Lineage, Principal: p.Principal, Global: p.Global, Scoped: p.Scoped}
}
func encodeCursor(value any, limit int) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > limit {
		return "", fmt.Errorf("%w: cursor exceeds limit", ErrInvalid)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func decodeCursor(token string, value any, limit int) error {
	invalid := func() error { return fmt.Errorf("%w: invalid cursor", ErrInvalid) }
	if len(token) == 0 || len(token) > base64.RawURLEncoding.EncodedLen(limit) {
		return invalid()
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) > limit || !utf8.Valid(raw) {
		return invalid()
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(value) != nil {
		return invalid()
	}
	canonical, err := encodeCursor(value, limit)
	if err != nil || canonical != token {
		return invalid()
	}
	return nil
}
func encodeRetentionCursor(cur retentionCursor) (string, error) { return encodeCursor(cur, 512) }
func decodeRetentionCursor(token string) (retentionCursor, error) {
	var cur retentionCursor
	err := decodeCursor(token, &cur, 512)
	if err == nil && (cur.Version != 1 || cur.Kind != "list" && cur.Kind != "prune" && cur.Kind != "obligations") {
		err = fmt.Errorf("%w: invalid cursor version or kind", ErrInvalid)
	}
	return cur, err
}
func encodeLogCursor(cur logCursor) (string, error) { return encodeCursor(cur, 512) }
func decodeLogCursor(token string) (logCursor, error) {
	var cur logCursor
	err := decodeCursor(token, &cur, 512)
	if err == nil && (cur.Version != 1 || cur.Kind != "logs") {
		err = fmt.Errorf("%w: invalid cursor version or kind", ErrInvalid)
	}
	return cur, err
}
