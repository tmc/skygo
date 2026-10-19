package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
	"sync"
	"time"
)

// Role is a principal's fixed permission level. Its zero value is invalid.
type Role string

const (
	// Reader permits access to a principal's own work.
	Reader Role = "reader"
	// Operator also permits submission and cancellation of owned work.
	Operator Role = "operator"
	// Admin permits read and cancellation across principals.
	Admin Role = "admin"
)

// Action selects read-only access or mutation. Its zero value is invalid.
type Action uint8

const (
	// Read permits authorized nonmutating operations.
	Read Action = iota + 1
	// Mutate requires Operator or Admin authority.
	Mutate
)

var (
	ErrUnauthenticated  = errors.New("unauthenticated")
	ErrPermissionDenied = errors.New("permission denied")
	ErrUnavailable      = errors.New("authentication unavailable")
	ErrInvalid          = errors.New("invalid authentication configuration")
	ErrConflict         = errors.New("authentication configuration conflict")
)

// Actor is authenticated authority minted by one Engine. The zero value is
// invalid. Its identity is not a caller-selected owner or a persistent record.
type Actor struct {
	engine             *Engine
	principal, tokenID string
	role               Role
	digest             [32]byte
}

// Principal returns the authenticated principal ID, or empty for a zero Actor.
func (a Actor) Principal() string { return a.principal }

// TokenID returns a nonsecret credential identifier, or empty in legacy mode.
func (a Actor) TokenID() string { return a.tokenID }

// Role returns the role at the Actor's most recent authentication or Check.
func (a Actor) Role() Role { return a.role }

// Engine authenticates one explicit legacy authority or private credential file.
// Construct it with NewLegacy or NewFile; the zero value is not usable.
type Engine struct {
	mu                 sync.RWMutex
	legacy             bool
	anonymous          bool
	legacyDigest       [32]byte
	path, installation string
	snapshot           *config
	hash               [32]byte
	lastRead           time.Time
	available          bool
	changed            chan struct{}
	now                func() time.Time
}

// NewLegacy creates a single-operator authority. Empty token permits anonymous
// access. Legacy actors are admins; they do not provide principal isolation.
func NewLegacy(token string) *Engine {
	return &Engine{legacy: true, anonymous: token == "", legacyDigest: tokenHash(token), available: true, changed: make(chan struct{}), now: time.Now}
}

// NewFile loads a private credential file bound to the installation ID.
// Run must refresh it while the server is serving requests.
func NewFile(path, installation string) (*Engine, error) {
	return newFile(path, installation, time.Now)
}

func newFile(path, installation string, now func() time.Time) (*Engine, error) {
	e := &Engine{path: path, installation: installation, changed: make(chan struct{}), now: now}
	if err := e.refresh(); err != nil {
		return nil, err
	}
	return e, nil
}

// Authenticate validates a complete Authorization header and mints an Actor.
// Only explicitly anonymous legacy mode accepts an empty header.
func (e *Engine) Authenticate(header string) (Actor, error) {
	if e == nil {
		return Actor{}, ErrUnavailable
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.readyLocked() {
		return Actor{}, ErrUnavailable
	}
	if e.legacy {
		if !e.anonymous && (len(header) > 4096 || !strings.HasPrefix(header, "Bearer ") || !equalDigest(tokenHash(strings.TrimPrefix(header, "Bearer ")), e.legacyDigest)) {
			return Actor{}, ErrUnauthenticated
		}
		return Actor{engine: e, principal: "legacy", role: Admin}, nil
	}
	if !strings.HasPrefix(header, "Bearer ") {
		return Actor{}, ErrUnauthenticated
	}
	token := strings.TrimPrefix(header, "Bearer ")
	id, ok := tokenID(token)
	if !ok {
		return Actor{}, ErrUnauthenticated
	}
	digest := tokenHash(token)
	for _, t := range e.snapshot.Tokens {
		if t.ID == id && equalDigest(t.digest, digest) && t.usable(e.now()) {
			return Actor{engine: e, principal: t.Principal, tokenID: t.ID, role: e.snapshot.role(t.Principal), digest: digest}, nil
		}
	}
	return Actor{}, ErrUnauthenticated
}

// Check refreshes an Actor's credential, role and authority for action. It
// rejects zero Actors, foreign engines, revoked credentials and invalid actions.
func (e *Engine) Check(actor Actor, action Action) (Actor, error) {
	if e == nil || actor.engine != e || actor.principal == "" {
		return Actor{}, ErrUnauthenticated
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.readyLocked() {
		return Actor{}, ErrUnavailable
	}
	if !e.legacy {
		found := false
		for _, t := range e.snapshot.Tokens {
			if t.ID == actor.tokenID && t.Principal == actor.principal && equalDigest(t.digest, actor.digest) && t.usable(e.now()) {
				actor.role = e.snapshot.role(t.Principal)
				found = true
				break
			}
		}
		if !found {
			return Actor{}, ErrUnauthenticated
		}
	}
	if action != Read && action != Mutate || action == Mutate && actor.role != Operator && actor.role != Admin {
		return Actor{}, ErrPermissionDenied
	}
	return actor, nil
}

// Ready reports whether authentication can currently serve requests. It does
// not describe execution capacity or durable reconciliation health.
func (e *Engine) Ready() bool {
	if e == nil {
		return false
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.readyLocked()
}
func (e *Engine) readyLocked() bool {
	return e.available && e.now != nil && (e.legacy || e.snapshot != nil && e.now().Sub(e.lastRead) <= 2*time.Second)
}

// Changed returns a notification closed on snapshot or availability change.
// Call it again after a notification; credential expiry also needs a timer.
func (e *Engine) Changed() <-chan struct{} { e.mu.RLock(); defer e.mu.RUnlock(); return e.changed }

// Run refreshes a file authority until cancellation. Invalid snapshots deny
// requests while the loop keeps attempting recovery. Legacy engines need no Run.
func (e *Engine) Run(ctx context.Context) {
	if e == nil || e.legacy {
		return
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			_ = e.refresh()
		}
	}
}

func (e *Engine) refresh() error {
	c, data, _, err := readConfig(e.path)
	if err == nil && c.Installation != e.installation {
		err = ErrInvalid
	}
	hash := sha256.Sum256(data)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err == nil && e.snapshot != nil && (c.Generation < e.snapshot.Generation || c.Generation == e.snapshot.Generation && hash != e.hash) {
		err = ErrConflict
	}
	changed := e.available != (err == nil) || err == nil && e.hash != hash
	e.available = err == nil
	if err == nil {
		e.snapshot, e.hash, e.lastRead = c, hash, e.now()
	}
	if changed {
		close(e.changed)
		e.changed = make(chan struct{})
	}
	return err
}

func tokenHash(token string) [32]byte { return sha256.Sum256([]byte("skygo-auth-v1\x00" + token)) }

func equalDigest(a, b [32]byte) bool { return subtle.ConstantTimeCompare(a[:], b[:]) == 1 }
