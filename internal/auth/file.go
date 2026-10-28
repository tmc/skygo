package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const maxConfigBytes = 256 << 10
const maxLifetime = 90 * 24 * time.Hour

type principal struct {
	ID   string `json:"id"`
	Role Role   `json:"role"`
}
type credential struct {
	ID        string    `json:"id"`
	Principal string    `json:"principal"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Hash      string    `json:"sha256"`
	digest    [32]byte
}
type config struct {
	Version      uint32       `json:"version"`
	Installation string       `json:"installation"`
	Generation   uint64       `json:"generation"`
	Principals   []principal  `json:"principals"`
	Tokens       []credential `json:"tokens"`
}

func (c *config) role(id string) Role {
	for _, p := range c.Principals {
		if p.ID == id {
			return p.Role
		}
	}
	return ""
}
func (t credential) usable(now time.Time) bool {
	return !now.Before(t.CreatedAt) && now.Before(t.ExpiresAt)
}
func (c *config) admin(now time.Time) bool {
	for _, t := range c.Tokens {
		if c.role(t.Principal) == Admin && t.usable(now) {
			return true
		}
	}
	return false
}

// TokenInfo contains nonsecret credential metadata, never hashes or token bytes.
type TokenInfo struct {
	ID        string    `json:"id"`
	Principal string    `json:"principal"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (t credential) info() TokenInfo { return TokenInfo{t.ID, t.Principal, t.CreatedAt, t.ExpiresAt} }

// Initialize publishes private initial config and token outputs without
// overwrite. Cleanup removes only files this call created; retain them after
// durable activation. A crash before activation can leave unused private files.
func Initialize(path, tokenOutput, installation string) (cleanup func() error, err error) {
	if !hexID(installation) {
		return nil, ErrInvalid
	}
	a, _ := filepath.Abs(path)
	b, _ := filepath.Abs(tokenOutput)
	if a == b {
		return nil, ErrInvalid
	}
	release, err := lockConfig(path)
	if err != nil {
		return nil, err
	}
	defer release()
	token, t, err := generate("legacy", 30*24*time.Hour, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	c := &config{Version: 1, Installation: installation, Generation: 1, Principals: []principal{{"legacy", Admin}}, Tokens: []credential{t}}
	data, err := encode(c)
	if err != nil {
		return nil, err
	}
	var owned []ownedFile
	cleanup = func() error {
		var result error
		for i := len(owned) - 1; i >= 0; i-- {
			result = errors.Join(result, owned[i].remove())
		}
		return result
	}
	for _, out := range []struct {
		path string
		data []byte
	}{{tokenOutput, []byte(token + "\n")}, {path, data}} {
		file, publishErr := publishNew(out.path, out.data)
		if file != nil {
			owned = append(owned, *file)
		}
		if publishErr != nil {
			_ = cleanup()
			return nil, publishErr
		}
	}
	return cleanup, nil
}

// AddPrincipal adds a named principal with a supported role.
func AddPrincipal(path, id string, role Role) error {
	if !principalID(id) || id == "legacy" || !validRole(role) {
		return ErrInvalid
	}
	return mutate(path, func(c *config) error {
		if c.role(id) != "" || len(c.Principals) >= 128 {
			return ErrConflict
		}
		c.Principals = append(c.Principals, principal{id, role})
		return nil
	})
}

// SetRole changes a role without changing ownership or token identities.
func SetRole(path, id string, role Role) error {
	if !principalID(id) || !validRole(role) {
		return ErrInvalid
	}
	return mutate(path, func(c *config) error {
		for i, p := range c.Principals {
			if p.ID == id {
				c.Principals[i].Role = role
				return nil
			}
		}
		return ErrInvalid
	})
}

// Issue writes one generated random token to a private new output file and
// commits its digest. Lifetime must be positive and at most 90 days.
func Issue(path, principal, output string, lifetime time.Duration) (TokenInfo, error) {
	if !principalID(principal) || lifetime <= 0 || lifetime > maxLifetime {
		return TokenInfo{}, ErrInvalid
	}
	a, _ := filepath.Abs(path)
	b, _ := filepath.Abs(output)
	if a == b {
		return TokenInfo{}, ErrInvalid
	}
	release, err := lockConfig(path)
	if err != nil {
		return TokenInfo{}, err
	}
	defer release()
	c, old, info, err := readConfig(path)
	if err != nil {
		return TokenInfo{}, err
	}
	if c.role(principal) == "" {
		return TokenInfo{}, ErrInvalid
	}
	now := time.Now().UTC()
	var retained []credential
	count := 0
	for _, t := range c.Tokens {
		if now.Before(t.ExpiresAt) {
			retained = append(retained, t)
			if t.Principal == principal {
				count++
			}
		}
	}
	if len(retained) >= 256 || count >= 16 {
		return TokenInfo{}, ErrConflict
	}
	c.Tokens = retained
	token, t, err := generate(principal, lifetime, now)
	if err != nil {
		return TokenInfo{}, err
	}
	for _, oldToken := range c.Tokens {
		if oldToken.ID == t.ID {
			return TokenInfo{}, ErrConflict
		}
	}
	c.Tokens = append(c.Tokens, t)
	data, err := nextConfig(c)
	if err != nil {
		return TokenInfo{}, err
	}
	file, err := publishNew(output, []byte(token+"\n"))
	if err != nil {
		if file != nil {
			_ = file.remove()
		}
		return TokenInfo{}, err
	}
	published, err := replaceConfig(path, data, old, info)
	if err != nil && !published {
		_ = file.remove()
	}
	return t.info(), err
}

// Revoke removes a token's authority. Servers refresh independently; this local
// operation is not acknowledgement of immediate daemon revocation.
func Revoke(path, tokenID string) error {
	if !hexID(tokenID) {
		return ErrInvalid
	}
	return mutate(path, func(c *config) error {
		for i, t := range c.Tokens {
			if t.ID == tokenID {
				c.Tokens = append(c.Tokens[:i], c.Tokens[i+1:]...)
				return nil
			}
		}
		return ErrInvalid
	})
}

// Tokens returns bounded nonsecret credential metadata in token-ID order.
func Tokens(path string) ([]TokenInfo, error) {
	c, _, _, err := readConfig(path)
	if err != nil {
		return nil, err
	}
	out := make([]TokenInfo, 0, len(c.Tokens))
	for _, t := range c.Tokens {
		out = append(out, t.info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func mutate(path string, change func(*config) error) error {
	release, err := lockConfig(path)
	if err != nil {
		return err
	}
	defer release()
	c, old, info, err := readConfig(path)
	if err != nil {
		return err
	}
	if err = change(c); err != nil {
		return err
	}
	data, err := nextConfig(c)
	if err != nil {
		return err
	}
	_, err = replaceConfig(path, data, old, info)
	return err
}
func nextConfig(c *config) ([]byte, error) {
	if c.Generation == ^uint64(0) || !c.admin(time.Now()) {
		return nil, ErrConflict
	}
	c.Generation++
	return encode(c)
}
func generate(principal string, lifetime time.Duration, now time.Time) (string, credential, error) {
	var id [16]byte
	var secret [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", credential{}, errors.New("generate credential")
	}
	if _, err := rand.Read(secret[:]); err != nil {
		return "", credential{}, errors.New("generate credential")
	}
	idText := hex.EncodeToString(id[:])
	token := "sky_" + idText + "_" + base64.RawURLEncoding.EncodeToString(secret[:])
	digest := tokenHash(token)
	return token, credential{ID: idText, Principal: principal, CreatedAt: now, ExpiresAt: now.Add(lifetime), Hash: hex.EncodeToString(digest[:]), digest: digest}, nil
}
func tokenID(token string) (string, bool) {
	var payload string
	switch {
	case len(token) == 80 && strings.HasPrefix(token, "sky_"):
		payload = token[4:]
	case len(token) == 82 && strings.HasPrefix(token, "skygo_"):
		payload = token[6:]
	default:
		return "", false
	}
	if payload[32] != '_' {
		return "", false
	}
	id := payload[:32]
	if !hexID(id) {
		return "", false
	}
	b, err := base64.RawURLEncoding.DecodeString(payload[33:])
	if err != nil || len(b) != 32 || base64.RawURLEncoding.EncodeToString(b) != payload[33:] {
		return "", false
	}
	return id, true
}
func hexID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func principalID(s string) bool {
	if len(s) < 1 || len(s) > 64 || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validRole(r Role) bool { return r == Reader || r == Operator || r == Admin }

// ErrCloseUncertain means a read of the credential file attempted to close its
// original descriptor and did not prove closure. No configuration from that
// read is accepted. The caller must keep the error and stop; it must not
// reopen or retry the read in its place. Use errors.Is.
var ErrCloseUncertain = errors.New("authentication file closure is uncertain")

// fileCloseError retains the original credential file whose one Close failed.
// Its text omits the path; Unwrap exposes the read and Close errors.
type fileCloseError struct {
	file *os.File
	err  error
}

func (e *fileCloseError) Error() string        { return "close authentication file" }
func (e *fileCloseError) Unwrap() error        { return e.err }
func (e *fileCloseError) Is(target error) bool { return target == ErrCloseUncertain }

// closeConfigFile closes the descriptor readConfig opened. Tests replace it
// to model an operating system Close failure of that same file.
var closeConfigFile = (*os.File).Close

func readConfig(path string) (c *config, data []byte, info os.FileInfo, err error) {
	if err := safeParent(path); err != nil {
		return nil, nil, nil, err
	}
	info, err = os.Lstat(path)
	if err != nil {
		return nil, nil, nil, fileError("read authentication file", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maxConfigBytes {
		return nil, nil, nil, ErrInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, fileError("open authentication file", err)
	}
	// The one Close decides the result: a failed Close accepts nothing.
	defer func() {
		if closeErr := closeConfigFile(f); closeErr != nil {
			c, data, info = nil, nil, nil
			err = &fileCloseError{file: f, err: errors.Join(err, closeErr)}
		}
	}()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, nil, nil, ErrInvalid
	}
	data, err = io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return nil, nil, nil, errors.New("read authentication file")
	}
	if len(data) > maxConfigBytes || !uniqueJSON(data) {
		return nil, nil, nil, ErrInvalid
	}
	var parsed config
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&parsed) != nil || d.Decode(new(any)) != io.EOF {
		return nil, nil, nil, ErrInvalid
	}
	if validate(&parsed) != nil {
		return nil, nil, nil, ErrInvalid
	}
	return &parsed, data, info, nil
}
func validate(c *config) error {
	if c.Version != 1 || !hexID(c.Installation) || c.Generation == 0 || len(c.Principals) == 0 || len(c.Principals) > 128 || len(c.Tokens) > 256 {
		return ErrInvalid
	}
	principals := make(map[string]Role)
	for _, p := range c.Principals {
		if !principalID(p.ID) || !validRole(p.Role) || principals[p.ID] != "" {
			return ErrInvalid
		}
		principals[p.ID] = p.Role
	}
	if principals["legacy"] == "" {
		return ErrInvalid
	}
	ids := make(map[string]bool)
	counts := make(map[string]int)
	for i, t := range c.Tokens {
		if !hexID(t.ID) || ids[t.ID] || principals[t.Principal] == "" || t.CreatedAt.IsZero() || !t.ExpiresAt.After(t.CreatedAt) || t.ExpiresAt.Sub(t.CreatedAt) > maxLifetime || len(t.Hash) != 64 {
			return ErrInvalid
		}
		b, err := hex.DecodeString(t.Hash)
		if err != nil || hex.EncodeToString(b) != t.Hash {
			return ErrInvalid
		}
		copy(c.Tokens[i].digest[:], b)
		ids[t.ID] = true
		counts[t.Principal]++
		if counts[t.Principal] > 16 {
			return ErrInvalid
		}
	}

	return nil
}
func encode(c *config) ([]byte, error) {
	if err := validate(c); err != nil {
		return nil, err
	}
	sort.Slice(c.Principals, func(i, j int) bool { return c.Principals[i].ID < c.Principals[j].ID })
	sort.Slice(c.Tokens, func(i, j int) bool { return c.Tokens[i].ID < c.Tokens[j].ID })
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil || len(data)+1 > maxConfigBytes {
		return nil, ErrInvalid
	}
	return append(data, '\n'), nil
}

// uniqueJSON rejects duplicate fields before typed decode. Parser errors never
// cross the credential boundary; field names and malformed text can be secrets.
func uniqueJSON(data []byte) bool {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 64 {
			return false
		}
		t, err := d.Token()
		if err != nil {
			return false
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return false
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !value(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		default:
			return false
		}
	}
	if !value(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}

func fileError(action string, err error) error {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%s: %w", action, fs.ErrNotExist)
	case errors.Is(err, fs.ErrExist):
		return fmt.Errorf("%s: %w", action, fs.ErrExist)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("%s: %w", action, fs.ErrPermission)
	default:
		return errors.New(action)
	}
}

type ownedFile struct {
	path string
	info os.FileInfo
}

func (f ownedFile) remove() error {
	info, err := os.Lstat(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fileError("inspect owned output", err)
	}
	if !os.SameFile(info, f.info) {
		return ErrConflict
	}
	if err = os.Remove(f.path); err != nil {
		return fileError("remove owned output", err)
	}
	return nil
}
func parent(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fileError("create authentication directory", err)
	}
	return safeParent(path)
}

func safeParent(path string) error {
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if err != nil {
		return fileError("inspect authentication directory", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0022 != 0 {
		return ErrInvalid
	}
	return nil
}
func temporary(path string, data []byte) (string, error) {
	if err := parent(path); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".skygo-auth-*")
	if err != nil {
		return "", fileError("prepare authentication output", err)
	}
	name := f.Name()
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(name)
		return "", errors.New("write authentication output")
	}
	return name, nil
}
func syncParent(path string) error {
	f, err := os.Open(filepath.Dir(path))
	if err != nil {
		return errors.New("open authentication directory")
	}
	defer f.Close()
	if f.Sync() != nil {
		return errors.New("sync authentication directory")
	}
	return nil
}
func publishNew(path string, data []byte) (*ownedFile, error) {
	tmp, err := temporary(path, data)
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmp)
	info, err := os.Stat(tmp)
	if err != nil {
		return nil, errors.New("inspect authentication output")
	}
	if err = os.Link(tmp, path); err != nil {
		return nil, fileError("publish authentication output", err)
	}
	out := &ownedFile{path, info}
	return out, syncParent(path)
}
func replaceConfig(path string, data, old []byte, info os.FileInfo) (bool, error) {
	tmp, err := temporary(path, data)
	if err != nil {
		return false, err
	}
	defer os.Remove(tmp)
	_, current, currentInfo, err := readConfig(path)
	if err != nil {
		return false, err
	}
	if !os.SameFile(info, currentInfo) || sha256.Sum256(current) != sha256.Sum256(old) {
		return false, ErrConflict
	}
	if err = os.Rename(tmp, path); err != nil {
		return false, fileError("publish authentication update", err)
	}
	return true, syncParent(path)
}
func lockConfig(path string) (func(), error) {
	if err := parent(path); err != nil {
		return nil, err
	}
	lock := path + ".lock"
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		err := os.Mkdir(lock, 0700)
		if err == nil {
			info, e := os.Lstat(lock)
			if e != nil {
				return nil, errors.New("inspect authentication lock")
			}
			return func() {
				current, e := os.Lstat(lock)
				if e == nil && os.SameFile(info, current) {
					_ = os.Remove(lock)
				}
			}, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, fileError("acquire authentication lock", err)
		}
		select {
		case <-deadline.C:
			return nil, ErrConflict
		case <-tick.C:
		}
	}
}
