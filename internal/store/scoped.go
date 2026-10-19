package store

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

var jobPrincipalsBucket = []byte("job_principals")
var principalRequestsBucket = []byte("principal_requests")
var principalIndexBucket = []byte("principal_index")
var scopedKey = []byte("scoped_auth")

// Scoped reports whether this installation has durably activated scoped auth.
func (s *Store) Scoped() bool { return s.scoped }

func isScoped(tx *bolt.Tx) bool {
	m := tx.Bucket(metadataBucket)
	return m != nil && bytes.Equal(m.Get(scopedKey), []byte{1})
}

// ActivateScoped atomically assigns historical work to legacy and activates
// scoped authentication without downgrading the store format. Call only while
// stopped, under this Store's exclusive writer lock,
// after preparing the separately backed-up installation-bound auth file.
func (s *Store) ActivateScoped() error {
	err := s.db.Update(activateScoped)

	if err == nil {
		s.scoped = true
	}
	return err
}

func activateScoped(tx *bolt.Tx) error {
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if isScoped(tx) {
		return errors.New("scoped authentication already active")
	}
	if err := initialize(tx, string(tx.Bucket(metadataBucket).Get([]byte("owner")))); err != nil {
		return err
	}
	for _, name := range [][]byte{jobPrincipalsBucket, principalRequestsBucket, principalIndexBucket, auditBucket} {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	owners := tx.Bucket(jobPrincipalsBucket)
	requests, err := tx.Bucket(principalRequestsBucket).CreateBucket([]byte("legacy"))
	if err != nil {
		return err
	}
	if err := tx.Bucket(requestsBucket).ForEach(func(k, v []byte) error { return requests.Put(k, v) }); err != nil {
		return err
	}
	index, err := tx.Bucket(principalIndexBucket).CreateBucket([]byte("legacy"))
	if err != nil {
		return err
	}
	if err := tx.Bucket(sequenceBucket).ForEach(func(_, id []byte) error {
		if err := owners.Put(id, []byte("legacy")); err != nil {
			return err
		}
		n, err := index.NextSequence()
		if err != nil {
			return err
		}
		return index.Put(sequenceKey(n), id)
	}); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	if err := m.Put(scopedKey, []byte{1}); err != nil {
		return err
	}
	if err := m.Put(schemaKey, sequenceKey(schemaVersion)); err != nil {
		return err
	}
	return validateScoped(tx)
}

func (s *Store) authorize(actor auth.Actor, action auth.Action) error {
	if actor.Principal() == "" || (actor.Role() != auth.Reader && actor.Role() != auth.Operator && actor.Role() != auth.Admin) {
		return auth.ErrUnauthenticated
	}
	if !s.scoped && actor.Principal() != "legacy" {
		return auth.ErrPermissionDenied
	}
	if action == auth.Mutate && actor.Role() == auth.Reader {
		return auth.ErrPermissionDenied
	}
	return nil
}

func requestNamespace(tx *bolt.Tx, principal string) *bolt.Bucket {
	if !isScoped(tx) {
		return tx.Bucket(requestsBucket)
	}
	return tx.Bucket(principalRequestsBucket).Bucket([]byte(principal))
}
func bindRequest(tx *bolt.Tx, principal, request, id string) error {
	if !isScoped(tx) {
		return tx.Bucket(requestsBucket).Put([]byte(request), []byte(id))
	}
	b, err := tx.Bucket(principalRequestsBucket).CreateBucketIfNotExists([]byte(principal))
	if err != nil {
		return err
	}
	return b.Put([]byte(request), []byte(id))
}
func visible(tx *bolt.Tx, actor auth.Actor, id string) bool {
	return !isScoped(tx) || actor.Role() == auth.Admin || string(tx.Bucket(jobPrincipalsBucket).Get([]byte(id))) == actor.Principal()
}

// GetFor reads an authorized job. Foreign and missing identifiers both return
// ErrNotFound before any foreign task data is decoded.
func (s *Store) GetFor(actor auth.Actor, id string) (j backend.Job, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if !visible(tx, actor, id) {
			return ErrNotFound
		}
		var e error
		j, e = get(tx, id)
		return e
	})
	return
}

func scopeIndex(tx *bolt.Tx, actor auth.Actor) *bolt.Bucket {
	if !isScoped(tx) || actor.Role() == auth.Admin {
		return tx.Bucket(sequenceBucket)
	}
	return tx.Bucket(principalIndexBucket).Bucket([]byte(actor.Principal()))
}

// LastSequenceFor returns the authenticated listing scope's high-water mark.
// Nonadmins receive their own dense ordinal, never the global acceptance count.
func (s *Store) LastSequenceFor(actor auth.Actor) (n uint64, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		if b := scopeIndex(tx, actor); b != nil {
			n = b.Sequence()
		}
		return nil
	})
	return
}

// ListFor lists only the authenticated scope, using its own acceptance index.
// after and max use LastSequenceFor's scope; accepted Job.Sequence is unchanged.
func (s *Store) ListFor(actor auth.Actor, after, max uint64, limit int) (out []backend.Job, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if limit <= 0 {
		return nil, errors.New("list limit must be positive")
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := scopeIndex(tx, actor)
		if b == nil || after >= max {
			return nil
		}
		c := b.Cursor()
		for k, id := c.Seek(sequenceKey(after + 1)); k != nil && len(out) < limit; k, id = c.Next() {
			if len(k) != 8 {
				return errors.New("invalid principal index")
			}
			if binary.BigEndian.Uint64(k) > max {
				break
			}
			if !visible(tx, actor, string(id)) {
				return errors.New("invalid principal ownership index")
			}
			j, e := get(tx, string(id))
			if e != nil {
				return e
			}
			out = append(out, j)
		}
		return nil
	})
	return
}

// UpdateFor checks authority and ownership in the transaction that changes the
// mutable record. Its callback must not perform external I/O or change identity.
func (s *Store) UpdateFor(actor auth.Actor, id string, fn func(*backend.Job) error) (j backend.Job, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if !visible(tx, actor, id) {
			return ErrNotFound
		}
		var e error
		j, e = update(tx, id, fn)
		return e
	})
	return
}

func validateScoped(tx *bolt.Tx) error {
	m := tx.Bucket(metadataBucket)
	n := uint64(0)
	if v := m.Get(schemaKey); len(v) == 8 {
		n = binary.BigEndian.Uint64(v)
	}
	active := isScoped(tx)
	flag := m.Get(scopedKey)
	if m.Bucket(scopedKey) != nil || flag != nil && !active {
		return errors.New("invalid scoped mode flag")
	}
	if !active {
		if n == 2 {
			return errors.New("schema 2 requires scoped mode")
		}
		for _, name := range [][]byte{jobPrincipalsBucket, principalRequestsBucket, principalIndexBucket, auditBucket} {
			if tx.Bucket(name) != nil {
				return errors.New("unexpected scoped relations")
			}
		}
		return nil
	}
	if n != 2 && n != 3 && n != schemaVersion {
		return errors.New("scoped mode requires a supported format")
	}
	owners, requests, indexes := tx.Bucket(jobPrincipalsBucket), tx.Bucket(principalRequestsBucket), tx.Bucket(principalIndexBucket)
	if owners == nil || requests == nil || indexes == nil {
		return errors.New("missing scoped relations")
	}
	indexed := make(map[string]bool)
	if err := indexes.ForEach(func(principal, value []byte) error {
		if value != nil || !validPrincipal(principal) {
			return errors.New("invalid principal index bucket")
		}
		b := indexes.Bucket(principal)
		var count uint64
		if err := b.ForEach(func(k, id []byte) error {
			count++
			if len(k) != 8 || binary.BigEndian.Uint64(k) != count || tx.Bucket(jobsBucket).Get(id) == nil || !bytes.Equal(owners.Get(id), principal) || indexed[string(id)] {
				return errors.New("invalid scoped job index")
			}
			indexed[string(id)] = true
			return nil
		}); err != nil {
			return err
		}
		if count != b.Sequence() {
			return errors.New("invalid scoped index sequence")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := tx.Bucket(jobsBucket).ForEach(func(id, _ []byte) error {
		if !indexed[string(id)] {
			return errors.New("missing job principal relation")
		}
		return nil
	}); err != nil {
		return err
	}
	if err := owners.ForEach(func(id, principal []byte) error {
		if !indexed[string(id)] || len(principal) == 0 {
			return errors.New("orphan job principal relation")
		}
		return nil
	}); err != nil {
		return err
	}
	bound := make(map[string]bool)
	if err := requests.ForEach(func(principal, value []byte) error {
		if value != nil || !validPrincipal(principal) {
			return errors.New("invalid principal request bucket")
		}
		return requests.Bucket(principal).ForEach(func(key, id []byte) error {
			if len(key) == 0 || !indexed[string(id)] || !bytes.Equal(owners.Get(id), principal) {
				return errors.New("invalid scoped request binding")
			}
			bound[string(id)] = true
			return nil
		})
	}); err != nil {
		return err
	}
	for id := range indexed {
		if !bound[id] {
			return errors.New("missing scoped request binding")
		}
	}
	legacy := requests.Bucket([]byte("legacy"))
	if err := tx.Bucket(requestsBucket).ForEach(func(key, id []byte) error {
		if legacy == nil || !bytes.Equal(legacy.Get(key), id) || string(owners.Get(id)) != "legacy" {
			return errors.New("invalid preserved legacy request")
		}
		return nil
	}); err != nil {
		return err
	}
	return validateAudit(tx)
}

func validPrincipal(id []byte) bool {
	if len(id) == 0 || len(id) > 64 || id[0] < 'a' || id[0] > 'z' {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
