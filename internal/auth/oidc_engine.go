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
	hash     [32]byte  // OIDC file bytes
	keysHash [32]byte  // key set bytes
	fetched  time.Time // completion of the fetch that returned keys; zero for a JWKS file
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

// NewFileOIDC is NewFile with OpenID Connect ID-token authentication
// configured by the private OIDC file at oidcPath. If allowedJWKSURL is not
// empty, it is the one HTTPS key set URL the OIDC file may name as jwks_uri,
// and Run fetches keys from it; otherwise the OIDC file must name a jwks_file.
// Construction validates its arguments and reads only the credential file:
// OIDC is unavailable until Run has read the OIDC file and its keys.
//
// If the credential file's Close fails, NewFileOIDC returns the Engine, which
// retains the original file, with an error satisfying errors.Is(err,
// ErrCloseUncertain); the caller must keep both and stop, as for Err. Any
// other failure returns no Engine.
func NewFileOIDC(path, installation, oidcPath, allowedJWKSURL string) (*Engine, error) {
	if oidcPath == "" || !localPath(oidcPath) || allowedJWKSURL != "" && !httpsURL(allowedJWKSURL, true) {
		return nil, ErrInvalid
	}
	e, err := newFileOIDC(path, installation, oidcPath, time.Now)
	if err == nil && allowedJWKSURL != "" {
		e.oidcKeys = newJWKSFetcher(allowedJWKSURL, e.now)
	}
	return e, err
}

// newFileOIDC is newFile with a private OIDC configuration file. It reads only
// the credential file; OIDC stays unavailable until Run's OIDC reader, or
// refreshOIDC, publishes a snapshot. Ordinary OIDC failures leave OIDC
// unavailable but never make scoped tokens unavailable. A jwks_uri is
// unavailable unless e.oidcKeys is set before the first OIDC read. After an
// uncertain credential file Close it returns the partial Engine, which holds
// the original file, with that error.
func newFileOIDC(path, installation, oidcPath string, now func() time.Time) (*Engine, error) {
	e := &Engine{path: path, installation: installation, oidcPath: oidcPath, changed: make(chan struct{}), now: now}
	e.oidcRead = func() (*oidcSnapshot, error) { return readOIDC(oidcPath, installation, e.oidcKeys) }
	if err := e.refresh(); err != nil {
		if errors.Is(err, ErrCloseUncertain) {
			return e, err
		}
		return nil, err
	}
	return e, nil
}

// runOIDC refreshes the OIDC snapshot until cancellation. It is the Engine's
// only OIDC reader and reads synchronously, so it never starts a read while
// another is pending: a blocked read holds the loop, and the OIDC snapshot
// goes stale, until the read returns. It starts no read after it observes
// cancellation. It runs apart from the credential file refresh, which a
// blocked OIDC read therefore never delays. With a jwks_uri allowed, it also
// runs the key set fetcher on its own goroutine and returns only after the
// fetcher has stopped, so a slow fetch never delays OIDC file reads.
func (e *Engine) runOIDC(ctx context.Context) {
	if e.oidcKeys != nil {
		done := make(chan struct{})
		go func() {
			defer close(done)
			e.runJWKS(ctx)
		}()
		defer func() { <-done }()
	}
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
// snapshot that is already stale. A read whose original Close failed is
// retained by the Engine, as for the credential file, and no read follows.
func (e *Engine) refreshOIDC() {
	if e.Err() != nil {
		return
	}
	start := e.now()
	next, err := e.oidcRead()
	e.mu.Lock()
	defer e.mu.Unlock()
	changed := e.publishOIDCLocked(next, err, start)
	if errors.Is(err, ErrCloseUncertain) {
		changed = changed || e.held == nil
		e.holdLocked(err)
	}
	if changed {
		close(e.changed)
		e.changed = make(chan struct{})
	}
}

// oidcReadyLocked reports whether the OIDC snapshot is valid, its read began
// within the same two-second envelope as the credential file, and a fetched
// key set's fetch completed at most maxJWKSAge ago. The key set age is
// checked here, at each Authenticate and Check, not only when a snapshot is
// read, and it is the age of the snapshot's own key set: a later fetch never
// renews it.
func (e *Engine) oidcReadyLocked() bool {
	if !e.oidcAvailable {
		return false
	}
	now := e.now()
	return now.Sub(e.oidcLastRead) <= 2*time.Second && (e.oidc.fetched.IsZero() || now.Sub(e.oidc.fetched) <= maxJWKSAge)
}

// readOIDC reads the OIDC file and its key set. A relative jwks_file or
// ca_file is relative to the OIDC file's directory. A jwks_uri key set comes
// only from remote's cache, never from a fetch here; without remote, or for
// any URL but the allowed one, it is unavailable.
func readOIDC(path, installation string, remote *jwksFetcher) (*oidcSnapshot, error) {
	data, err := readAuthFile(path, maxOIDCConfigBytes, true)
	if err != nil {
		return nil, err
	}
	cfg, err := parseOIDCConfig(data, installation)
	if err != nil {
		return nil, err
	}
	var keysData []byte
	var fetched time.Time
	if cfg.jwksURI != "" {
		keysData, fetched, err = remote.keys(jwksSource{uri: cfg.jwksURI, caFile: oidcRelative(path, cfg.caFile)})
	} else {
		keysData, err = readAuthFile(oidcRelative(path, cfg.jwksFile), maxJWKSBytes, false)
	}
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
	return &oidcSnapshot{cfg: cfg, keys: keys, subjects: subjects, hash: sha256.Sum256(data), keysHash: sha256.Sum256(keysData), fetched: fetched}, nil
}

// oidcRelative resolves a relative file name from the OIDC file at path.
// An empty name stays empty.
func oidcRelative(path, name string) string {
	if name == "" || filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(filepath.Dir(path), name)
}

// publishOIDCLocked installs a newly read snapshot. As with the credential
// file, the OIDC file is compared with the last published snapshot: a lower
// generation, or different bytes at the same generation, is a conflict, and
// restoring the published bytes clears it. The key set has no generation:
// any valid key set is published, so replacing it with an older key set is
// indistinguishable from a rotation. Check still requires each Actor's key in
// the current set. Any failure makes OIDC unavailable at once; there is no
// last-good window for files. A published snapshot is fresh as of start, when
// its read began, and its fetched key set as of its fetch's completion; it
// always replaces the last one, so an unchanged key set renews its age only
// through a new fetch that the fetcher published. It reports whether
// availability or content changed.
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
	e.oidc, e.oidcAvailable, e.oidcLastRead = next, true, start
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

// statOIDCFile, readOIDCFile and closeOIDCFile stat, read and close the
// descriptor readAuthFile opened. Tests replace them to model a slow or
// failing operation on that same file.
var (
	statOIDCFile = (*os.File).Stat
	readOIDCFile = func(f *os.File, max int64) ([]byte, error) {
		return io.ReadAll(io.LimitReader(f, max+1))
	}
	closeOIDCFile = (*os.File).Close
)

// oidcFileError is fileError retaining the original error behind its text.
func oidcFileError(action string, err error) error {
	return &causeError{text: fileError(action, err).Error(), err: err}
}

// readAuthFile reads a bounded regular file, not a symlink, whose parent
// passes safeParent. A private file must grant no group or other permissions;
// a public file, such as a JWKS or CA file, may be readable but not group or
// other writable. It follows readConfig's custody checks and never changes
// modes. Errors have fixed text, without the path; Unwrap exposes the
// original stat, open or read error. As in readConfig, the one Close decides
// the result: if it fails, the error retains the original file and joins any
// earlier error, and no bytes are returned.
func readAuthFile(path string, max int64, private bool) (data []byte, err error) {
	if err := safeParent(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, oidcFileError("read OIDC file", err)
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
		return nil, oidcFileError("open OIDC file", err)
	}
	defer func() {
		if closeErr := closeOIDCFile(f); closeErr != nil {
			data = nil
			err = &fileCloseError{file: f, err: errors.Join(err, closeErr)}
		}
	}()
	opened, err := statOIDCFile(f)
	if err != nil {
		return nil, &causeError{text: "read OIDC file", err: err}
	}
	if !os.SameFile(info, opened) {
		return nil, ErrInvalid
	}
	data, err = readOIDCFile(f, max)
	if err != nil {
		return nil, &causeError{text: "read OIDC file", err: err}
	}
	if int64(len(data)) > max {
		return nil, ErrInvalid
	}
	return data, nil
}
