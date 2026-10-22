package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

// oidcSnapshot is one immutable OIDC configuration and its key set, read
// together and published under the same Engine lock as the credential file
// snapshot, but by its own reader. subjects maps SHA-256(iss || 0 || sub), the digest that
// oidcIdentity.subject holds, to the mapped principal, so Check can recheck a
// mapping without retaining subjects.
type oidcSnapshot struct {
	cfg      *oidcConfig
	keys     *jwkSet
	subjects map[[32]byte]string
	hash     [32]byte // OIDC file bytes
	keysHash [32]byte // JWKS file bytes
}

// oidcActor is the OIDC authority behind an Actor. It holds digests and key
// identity, not token bytes or the subject. Revocation is by mapping, key or
// principal removal and by expiry, never by token digest: an ES256 signature
// has two valid encodings.
type oidcActor struct {
	audience string
	kid      string
	subject  [32]byte
	key      [32]byte
	expires  time.Time
}

// newFileOIDC is newFile with a private OIDC configuration file. It reads only
// the credential file; OIDC stays unavailable until Run's OIDC reader, or
// refreshOIDC, publishes a snapshot. OIDC failures leave OIDC unavailable but
// never make scoped tokens unavailable.
func newFileOIDC(path, installation, oidcPath string, now func() time.Time) (*Engine, error) {
	e := &Engine{path: path, installation: installation, oidcPath: oidcPath, changed: make(chan struct{}), now: now}
	e.oidcRead = func() (*oidcSnapshot, error) { return readOIDC(oidcPath, installation) }
	if err := e.refresh(); err != nil {
		return nil, err
	}
	return e, nil
}

// runOIDC refreshes the OIDC snapshot until cancellation. It is the Engine's
// only OIDC reader and reads synchronously, so it never starts a read while
// another is pending: a blocked read holds the loop, and the OIDC snapshot
// goes stale, until the read returns. It starts no read after it observes
// cancellation. It runs apart from the credential file refresh, which a
// blocked OIDC read therefore never delays.
func (e *Engine) runOIDC(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for ctx.Err() == nil {
		e.refreshOIDC()
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
}

// refreshOIDC reads the OIDC files without holding e.mu, then publishes the
// result and notifies Changed if it changed anything. Freshness dates from
// the start of the read, so a read slower than the envelope publishes a
// snapshot that is already stale.
func (e *Engine) refreshOIDC() {
	start := e.now()
	next, err := e.oidcRead()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.publishOIDCLocked(next, err, start) {
		close(e.changed)
		e.changed = make(chan struct{})
	}
}

// oidcReadyLocked reports whether the OIDC snapshot is valid and its read
// began within the same two-second envelope as the credential file.
func (e *Engine) oidcReadyLocked() bool {
	return e.oidcAvailable && e.now().Sub(e.oidcLastRead) <= 2*time.Second
}

// readOIDC reads the OIDC file and its JWKS file. A relative jwks_file is
// relative to the OIDC file's directory. A jwks_uri configuration is
// unavailable: keys are read only from files here, and nothing is fetched.
func readOIDC(path, installation string) (*oidcSnapshot, error) {
	data, err := readAuthFile(path, maxOIDCConfigBytes, true)
	if err != nil {
		return nil, err
	}
	cfg, err := parseOIDCConfig(data, installation)
	if err != nil {
		return nil, err
	}
	if cfg.jwksURI != "" {
		return nil, ErrUnavailable
	}
	jwksPath := cfg.jwksFile
	if !filepath.IsAbs(jwksPath) {
		jwksPath = filepath.Join(filepath.Dir(path), jwksPath)
	}
	keysData, err := readAuthFile(jwksPath, maxJWKSBytes, false)
	if err != nil {
		return nil, err
	}
	keys, err := parseJWKS(keysData)
	if err != nil {
		return nil, err
	}
	subjects := make(map[[32]byte]string, len(cfg.identities))
	for sub, p := range cfg.identities {
		subjects[sha256.Sum256([]byte(cfg.issuer+"\x00"+sub))] = p
	}
	return &oidcSnapshot{cfg: cfg, keys: keys, subjects: subjects, hash: sha256.Sum256(data), keysHash: sha256.Sum256(keysData)}, nil
}

// publishOIDCLocked installs a newly read snapshot. As with the credential
// file, the OIDC file is compared with the last published snapshot: a lower
// generation, or different bytes at the same generation, is a conflict, and
// restoring the published bytes clears it. The JWKS file has no generation:
// any valid key set is published, so replacing it with an older key set is
// indistinguishable from a rotation. Check still requires each Actor's key in
// the current set. Any failure makes OIDC unavailable at once; there is no
// last-good window for files. A published snapshot is fresh as of start, when
// its read began. It reports whether availability or content changed.
func (e *Engine) publishOIDCLocked(next *oidcSnapshot, err error, start time.Time) bool {
	prev := e.oidc
	if err == nil && prev != nil && (next.cfg.generation < prev.cfg.generation || next.cfg.generation == prev.cfg.generation && next.hash != prev.hash) {
		err = ErrConflict
	}
	if err != nil {
		changed := e.oidcAvailable
		e.oidcAvailable = false
		return changed
	}
	changed := !e.oidcAvailable || prev == nil || next.hash != prev.hash || next.keysHash != prev.keysHash
	e.oidcAvailable, e.oidcLastRead = true, start
	if changed {
		e.oidc = next
	}
	return changed
}

// authenticateOIDCLocked verifies an ID token against the current OIDC and
// credential file snapshots.
func (e *Engine) authenticateOIDCLocked(token string) (Actor, error) {
	if !e.oidcReadyLocked() {
		return Actor{}, ErrUnavailable
	}
	s := e.oidc
	id, err := verifyIDToken(token, s.cfg, s.keys, e.snapshot, e.now())
	if err != nil {
		return Actor{}, err
	}
	return Actor{engine: e, principal: id.principal, role: id.role, oidc: &oidcActor{
		audience: s.cfg.audience,
		kid:      id.kid,
		subject:  id.subject,
		key:      id.key,
		expires:  id.expires,
	}}, nil
}

// checkOIDCLocked rechecks an OIDC Actor against the current snapshots and
// returns the principal's current role. The issuer is bound through the
// subject digest, which includes it.
func (e *Engine) checkOIDCLocked(actor Actor) (Role, error) {
	if !e.oidcReadyLocked() {
		return "", ErrUnavailable
	}
	s, a := e.oidc, actor.oidc
	if s.cfg.audience != a.audience || s.subjects[a.subject] != actor.principal || !e.now().Before(a.expires) {
		return "", ErrUnauthenticated
	}
	if k, ok := s.keys.lookup(a.kid); !ok || k.thumbprint != a.key {
		return "", ErrUnauthenticated
	}
	role := e.snapshot.role(actor.principal)
	if !validRole(role) {
		return "", ErrUnauthenticated
	}
	return role, nil
}

// readAuthFile reads a bounded regular file, not a symlink, whose parent
// passes safeParent. A private file must grant no group or other permissions;
// a public file, such as a JWKS file, may be readable but not group or other
// writable. It follows readConfig's custody checks and never changes modes.
func readAuthFile(path string, max int64, private bool) ([]byte, error) {
	if err := safeParent(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fileError("read OIDC file", err)
	}
	deny := os.FileMode(0022)
	if private {
		deny = 0077
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&deny != 0 || info.Size() > max {
		return nil, ErrInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fileError("open OIDC file", err)
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, errors.New("read OIDC file")
	}
	if int64(len(data)) > max {
		return nil, ErrInvalid
	}
	return data, nil
}
