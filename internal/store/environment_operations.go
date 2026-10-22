package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// AcceptEnvironment atomically accepts spec under requestID in the actor's
// namespace as a Present, Creating environment at revision 1 with the given
// target binding, file delegation and initial provider checkpoint. The same
// request with identical canonical spec bytes returns the original
// environment before any other check; with different bytes it is ErrConflict.
// A new acceptance requires scoped authentication, reserves the principal's
// name and the target's storage alias, and is refused with
// ErrEnvironmentLimit beyond 32 environments, 4 per principal or 16 MiB of
// environment metadata. The caller must check the delegation fresh before
// acceptance and must not cause any provider effect before this method
// returns without error.
func (s *Store) AcceptEnvironment(actor auth.Actor, requestID string, spec environment.Spec, binding string, authority auth.Delegation, checkpoint environment.Checkpoint) (out Environment, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(requestID) {
		return out, errors.New("invalid request ID")
	}
	raw, err := environment.Canonical(spec)
	if err != nil {
		return
	}
	id, err := newID()
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = acceptEnvironment(tx, actor.Principal(), requestID, raw, binding, authority, checkpoint, id, s.now().UTC())
		return e
	})
	return
}

// EnvironmentFor reads an authorized environment. Foreign and missing
// identifiers both return ErrNotFound.
func (s *Store) EnvironmentFor(actor auth.Actor, id string) (out Environment, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		var e error
		out, e = environmentFor(tx, actor, id)
		return e
	})
	return
}

// EnvironmentsFor lists the authenticated scope in acceptance order. A
// nonadmin pages by its own dense ordinal.
func (s *Store) EnvironmentsFor(actor auth.Actor, after uint64, limit int) (out EnvironmentPage, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	prefix := []byte("i/")
	if actor.Role() != auth.Admin {
		prefix = environmentScopePrefix(actor.Principal())
	}
	return s.environments(prefix, after, limit)
}

// Environment reads an environment for trusted reconciliation.
func (s *Store) Environment(id string) (out Environment, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		var e error
		out, e = environmentView(tx.Bucket(environmentsBucket), id)
		return e
	})
	return
}

// Environments lists every environment in acceptance order for trusted
// reconciliation.
func (s *Store) Environments(after uint64, limit int) (EnvironmentPage, error) {
	return s.environments([]byte("i/"), after, limit)
}

// CommitEnvironment atomically replaces the state and checkpoint of
// environment id if its revision is still revision, and advances the
// revision. A stale revision is ErrEnvironmentStale. The move must satisfy
// environment.State.Next, and the checkpoint keeps its kind and version.
// Identity, spec, binding and authority cannot change. Deletion is a commit
// of Desired Absent; it removes no row, reservation or charge. Only a nil
// error means the commit is durable; the caller must not cause a provider
// effect that depends on it otherwise.
func (s *Store) CommitEnvironment(id string, revision uint64, next EnvironmentCommit) (out Environment, err error) {
	next.Checkpoint = next.Checkpoint.Clone()
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = commitEnvironment(tx, id, revision, next, s.now().UTC())
		return e
	})
	return
}

func (s *Store) environments(prefix []byte, after uint64, limit int) (out EnvironmentPage, err error) {
	if limit < 1 || limit > 200 {
		return out, errors.New("list limit outside 1..200")
	}
	if after == math.MaxUint64 {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, v := c.Seek(append(bytes.Clone(prefix), sequenceKey(after+1)...)); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(k) != len(prefix)+8 {
				return errors.New("invalid environment index")
			}
			if len(out.Environments) == limit {
				out.More = true
				break
			}
			e, err := environmentView(b, string(v))
			if err != nil {
				return err
			}
			out.Environments = append(out.Environments, e)
			out.After = binary.BigEndian.Uint64(k[len(prefix):])
		}
		return nil
	})
	return
}

func environmentFor(tx *bolt.Tx, actor auth.Actor, id string) (Environment, error) {
	out, err := environmentView(tx.Bucket(environmentsBucket), id)
	if err == nil && actor.Role() != auth.Admin && out.Principal != actor.Principal() {
		return Environment{}, ErrNotFound
	}
	return out, err
}

// environmentReplay finds the request binding. Only identical canonical spec
// bytes replay.
func environmentReplay(b *bolt.Bucket, principal, requestID string, raw []byte) (out Environment, found bool, err error) {
	id := b.Get(environmentRequestKey(principal, requestID))
	if id == nil {
		return out, false, nil
	}
	if out, err = environmentView(b, string(id)); err != nil {
		return out, true, err
	}
	if out.Principal != principal || out.RequestID != requestID {
		return Environment{}, true, errors.New("environment request binding mismatch")
	}
	if out.SpecDigest != sha256.Sum256(raw) {
		return Environment{}, true, ErrConflict
	}
	return out, true, nil
}

// acceptEnvironment writes the record, spec, request binding, reservations
// and indexes in one transaction, or nothing. Exact replay precedes every
// other check.
func acceptEnvironment(tx *bolt.Tx, principal, requestID string, raw []byte, binding string, authority auth.Delegation, checkpoint environment.Checkpoint, id string, now time.Time) (out Environment, err error) {
	b := tx.Bucket(environmentsBucket)
	if b == nil {
		return out, errors.New("environment metadata requires schema 9")
	}
	out, found, err := environmentReplay(b, principal, requestID, raw)
	if found || err != nil {
		return
	}
	if !isScoped(tx) {
		return out, auth.ErrPermissionDenied
	}
	if !validWorkflowAuthority(authority, principal) {
		return out, auth.ErrUnauthenticated
	}
	if !validBinding(binding) {
		return out, errors.New("invalid target binding")
	}
	if err = checkpoint.Validate(); err != nil {
		return
	}
	spec, err := environment.Parse(raw)
	if err != nil {
		return
	}
	high, err := environmentHigh(b, principal)
	if err != nil {
		return
	}
	if b.Sequence() >= maxEnvironments || high >= maxPrincipalEnvironments {
		return out, ErrEnvironmentLimit
	}
	var alias []byte
	if spec.StorageAlias != "" {
		alias = environmentAliasKey(spec.StorageAlias, spec.Target)
	}
	if b.Get(environmentNameKey(principal, spec.Name)) != nil || alias != nil && b.Get(alias) != nil {
		return out, ErrEnvironmentReserved
	}
	if b.Get(environmentRecordKey(id)) != nil || b.Get(environmentSpecKey(id)) != nil {
		return out, errors.New("random environment identity collision")
	}
	ordinal := b.Sequence() + 1
	digest := sha256.Sum256(raw)
	r := environmentRecord{
		Identity:   environment.Identity{Installation: string(tx.Bucket(metadataBucket).Get([]byte("owner"))), ID: id, Generation: environment.FirstGeneration},
		Principal:  principal,
		RequestID:  requestID,
		Spec:       hex.EncodeToString(digest[:]),
		Binding:    binding,
		Authority:  authority,
		Ordinal:    ordinal,
		Scope:      high + 1,
		Created:    now,
		Updated:    now,
		Revision:   1,
		State:      environment.State{Desired: environment.Present, Phase: environment.Creating},
		Checkpoint: checkpoint.Clone(),
	}
	record, err := encodeEnvironmentRecord(r)
	if err != nil {
		return
	}
	type row struct{ k, v []byte }
	rows := []row{
		{environmentRecordKey(id), record},
		{environmentSpecKey(id), raw},
		{environmentRequestKey(principal, requestID), []byte(id)},
		{environmentNameKey(principal, spec.Name), []byte(id)},
		{environmentIndexKey(ordinal), []byte(id)},
		{environmentScopeKey(principal, r.Scope), []byte(id)},
		{environmentHighKey(principal), sequenceKey(r.Scope)},
	}
	if alias != nil {
		rows = append(rows, row{alias, []byte(id)})
	}
	usage, err := environmentUsage(b)
	if err != nil {
		return
	}
	var removed, added uint64
	if high != 0 {
		removed = uint64(len(rows[6].k) + 8)
	}
	for _, w := range rows {
		added += uint64(len(w.k) + len(w.v))
	}
	if err = environmentCharge(usage, removed, added); err != nil {
		return
	}
	if err = b.SetSequence(ordinal); err != nil {
		return
	}
	for _, w := range rows {
		if err = b.Put(w.k, w.v); err != nil {
			return
		}
	}
	return environmentView(b, id)
}

// commitEnvironment is the compare-and-swap step. It checks the revision
// before the transition and checkpoint.
func commitEnvironment(tx *bolt.Tx, id string, revision uint64, next EnvironmentCommit, now time.Time) (out Environment, err error) {
	b := tx.Bucket(environmentsBucket)
	r, err := loadEnvironmentRecord(b, id)
	if err != nil {
		return
	}
	if r.Revision != revision {
		return out, ErrEnvironmentStale
	}
	if err = r.State.Next(next.State); err != nil {
		return
	}
	if err = next.Checkpoint.Validate(); err != nil {
		return
	}
	if next.Checkpoint.Kind != r.Checkpoint.Kind || next.Checkpoint.Version != r.Checkpoint.Version {
		return out, errors.New("environment checkpoint kind or version changed")
	}
	if r.Revision == math.MaxUint64 {
		return out, errors.New("environment revision exhausted")
	}
	old := b.Get(environmentRecordKey(id))
	r.State, r.Checkpoint, r.Revision = next.State, next.Checkpoint.Clone(), r.Revision+1
	if now.After(r.Updated) {
		r.Updated = now
	}
	record, err := encodeEnvironmentRecord(r)
	if err != nil {
		return
	}
	usage, err := environmentUsage(b)
	if err != nil {
		return
	}
	if err = environmentCharge(usage, uint64(len(old)), uint64(len(record))); err != nil {
		return
	}
	if err = b.Put(environmentRecordKey(id), record); err != nil {
		return
	}
	return environmentView(b, id)
}
