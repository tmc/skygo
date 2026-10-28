package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// artifactEnvironmentPin keeps one artifact of an undeleted version 2
// environment. Mounts naming the same artifact share it. It is not read
// permission for its owner. Read, when set, is the environment read of the
// pin for committed incarnation (Seq, Incarnation): the witness that the
// tuple was admitted, kept until a later incarnation's read replaces it.
type artifactEnvironmentPin struct {
	Owner       string
	Environment string
	Ref         ArtifactRef
	Seq         uint64 `json:",omitempty"`
	Incarnation string `json:",omitempty"`
	Read        string `json:",omitempty"`
}

// maxEnvironmentPins bounds the environment_pins artifact table: every
// accepted environment with its most artifacts.
const maxEnvironmentPins = maxEnvironments * environment.MaxArtifacts

// environmentPinKey is the pin's table key, derived from its identity.
func environmentPinKey(env string, ref ArtifactRef) string {
	h := sha256.New()
	h.Write([]byte("skygo-environment-pin\x00" + env + "\x00"))
	h.Write([]byte(ref.ID + "\x00" + ref.SHA256 + "\x00"))
	h.Write(sequenceKey(ref.Size))
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// environmentPins returns the pins, without witnesses, that record r with
// spec s requires. A Deleted environment requires none.
func environmentPins(r environmentRecord, s environment.Spec) map[string]artifactEnvironmentPin {
	out := make(map[string]artifactEnvironmentPin)
	if r.State.Phase == environment.Deleted {
		return out
	}
	for _, m := range s.Artifacts {
		ref := ArtifactRef{ID: m.ID, SHA256: m.SHA256, Size: m.Size}
		out[environmentPinKey(r.Identity.ID, ref)] = artifactEnvironmentPin{Owner: r.Principal, Environment: r.Identity.ID, Ref: ref}
	}
	return out
}

// environmentRead reports whether r is an environment materialization read.
func environmentRead(r artifactRead) bool {
	return r.Environment != "" || r.Incarnation != "" || r.IncarnationSeq != 0
}

// downloadRead reports whether r is an ordinary owner download: neither a
// remote attempt input nor an environment materialization read.
func downloadRead(r artifactRead) bool {
	return r.RemoteAttemptID == "" && !environmentRead(r)
}

// environmentPinValid checks one pin row against the artifact state: its
// key, its witness form and its complete object of the same owner.
func environmentPinValid(st *artifactState, key string, p artifactEnvironmentPin) error {
	if !validPrincipal([]byte(p.Owner)) || !environment.ValidID(p.Environment) || !artifactRefValid(p.Ref) || key != environmentPinKey(p.Environment, p.Ref) {
		return artifactCorrupt()
	}
	if p.Read == "" {
		if p.Seq != 0 || p.Incarnation != "" {
			return artifactCorrupt()
		}
	} else if !artifactHex(p.Read, 32) || p.Seq == 0 || !validIncarnation(p.Incarnation) {
		return artifactCorrupt()
	}
	o, ok := st.Objects[p.Ref.ID]
	if !ok || o.State != "complete" || o.Ref != p.Ref || o.Owner != p.Owner {
		return artifactCorrupt()
	}
	return nil
}

// environmentValidatePins checks that the pin table is exactly the set the
// environment records and specs require, and the environment reads. Every
// witness names an environment read of its tuple, at most the committed
// sequence. Every unsettled read is its pin's witness for the committed
// incarnation, and a read of a Deleted environment is settled. Before
// schema11 there are no pins and no environment reads.
func environmentValidatePins(tx *bolt.Tx, st *artifactState) error {
	if schemaNumber(tx) < environmentArtifactSchema {
		if len(st.EnvironmentPins) != 0 {
			return artifactCorrupt()
		}
		for _, r := range st.Reads {
			if environmentRead(r) {
				return artifactCorrupt()
			}
		}
		return nil
	}
	b := tx.Bucket(environmentsBucket)
	if b == nil {
		return artifactCorrupt()
	}
	records := make(map[string]environmentRecord)
	n := 0
	c := b.Cursor()
	for k, v := c.Seek([]byte("e/")); k != nil && bytes.HasPrefix(k, []byte("e/")); k, v = c.Next() {
		r, err := decodeEnvironmentRecord(v)
		if err != nil || string(k) != "e/"+r.Identity.ID {
			return artifactCorrupt()
		}
		s, err := environment.Parse(b.Get(environmentSpecKey(r.Identity.ID)))
		if err != nil {
			return artifactCorrupt()
		}
		records[r.Identity.ID] = r
		for key, want := range environmentPins(r, s) {
			p, ok := st.EnvironmentPins[key]
			if !ok || p.Owner != want.Owner || p.Environment != want.Environment || p.Ref != want.Ref {
				return artifactCorrupt()
			}
			if p.Read != "" {
				rd, ok := st.Reads[p.Read]
				if !ok || p.Seq > r.IncarnationSeq || p.Seq == r.IncarnationSeq && p.Incarnation != r.Incarnation {
					return artifactCorrupt()
				}
				if rd.Environment != p.Environment || rd.Incarnation != p.Incarnation || rd.IncarnationSeq != p.Seq || rd.Ref != p.Ref || rd.Owner != p.Owner {
					return artifactCorrupt()
				}
			}
			n++
		}
	}
	if n != len(st.EnvironmentPins) || n > maxEnvironmentPins {
		return artifactCorrupt()
	}
	for id, rd := range st.Reads {
		if !environmentRead(rd) {
			continue
		}
		if rd.RemoteAttemptID != "" || !environment.ValidID(rd.Environment) || !validIncarnation(rd.Incarnation) || rd.IncarnationSeq == 0 {
			return artifactCorrupt()
		}
		r, ok := records[rd.Environment]
		if !ok || rd.Owner != r.Principal || rd.IncarnationSeq > r.IncarnationSeq {
			return artifactCorrupt()
		}
		if r.State.Phase == environment.Deleted {
			if !rd.Closed {
				return artifactCorrupt()
			}
			continue
		}
		if p, ok := st.EnvironmentPins[environmentPinKey(rd.Environment, rd.Ref)]; !ok || p.Read != id {
			return artifactCorrupt()
		}
		if !rd.Closed && (rd.IncarnationSeq != r.IncarnationSeq || rd.Incarnation != r.Incarnation) {
			return artifactCorrupt()
		}
	}
	return nil
}

// environmentPinChange is a staged pin table change. The artifact state is
// loaded and changed before the environment rows are written, and validated
// against the written rows by commit.
type environmentPinChange struct {
	before, after *artifactState
}

// environmentStagePins stages the pins of a new environment record r with
// spec s, each under the principal's actual grant. An environment without
// artifacts touches no artifact state, and the change is nil. Any refusal of
// a mount is ErrEnvironmentArtifact, without detail.
func environmentStagePins(tx *bolt.Tx, r environmentRecord, s environment.Spec, now time.Time) (*environmentPinChange, error) {
	if len(s.Artifacts) == 0 {
		return nil, nil
	}
	if tx.Bucket(artifactsBucket) == nil {
		return nil, ErrArtifactsDisabled
	}
	before, err := artifactLoad(tx)
	if err != nil {
		return nil, err
	}
	objects, size, err := artifactRequiredSize(before)
	if err != nil {
		return nil, err
	}
	after := before.clone()
	for key, p := range environmentPins(r, s) {
		if _, ok := after.EnvironmentPins[key]; ok {
			return nil, artifactCorrupt()
		}
		if err := artifactGranted(after, p.Owner, p.Ref, now); err != nil {
			return nil, ErrEnvironmentArtifact
		}
		after.EnvironmentPins[key] = p
	}
	if n := len(after.EnvironmentPins); n > maxEnvironmentPins && n > len(before.EnvironmentPins) {
		return nil, ErrEnvironmentLimit
	}
	afterObjects, afterSize, err := artifactRequiredSize(after)
	if err != nil {
		return nil, err
	}
	if afterObjects > maxRequiredObjects && afterObjects > objects || afterSize > maxRequiredBytes && afterSize > size {
		return nil, ErrEnvironmentArtifact
	}
	return &environmentPinChange{before, after}, nil
}

// environmentStageCommit checks the artifact effects of a commit of record
// r with spec s that changes the incarnation, if moved, or reaches Deleted,
// if deleted. Either is refused with ErrEnvironmentArtifactHeld while any
// read of the environment is unsettled, whatever its producer. The Deleted
// transition releases the environment's pins, and each released object gets
// the ordinary grace. Any other commit, and every commit of an environment
// without artifacts, touches no artifact state, and the change is nil.
func environmentStageCommit(tx *bolt.Tx, r environmentRecord, s environment.Spec, moved, deleted bool, now time.Time) (*environmentPinChange, error) {
	deleted = deleted && r.State.Phase != environment.Deleted
	if len(s.Artifacts) == 0 || !moved && !deleted {
		return nil, nil
	}
	if tx.Bucket(artifactsBucket) == nil {
		return nil, artifactCorrupt()
	}
	before, err := artifactLoad(tx)
	if err != nil {
		return nil, err
	}
	for _, rd := range before.Reads {
		if rd.Environment == r.Identity.ID && !rd.Closed {
			return nil, ErrEnvironmentArtifactHeld
		}
	}
	if !deleted {
		return nil, nil
	}
	after := before.clone()
	var released []string
	for key, p := range after.EnvironmentPins {
		if p.Environment == r.Identity.ID {
			delete(after.EnvironmentPins, key)
			released = append(released, p.Ref.ID)
		}
	}
	for _, id := range released {
		after.releaseGrace(id, now)
	}
	return &environmentPinChange{before, after}, nil
}

// commit validates the staged state against the written environment rows
// and applies it within budget. A nil change commits nothing.
func (c *environmentPinChange) commit(tx *bolt.Tx, budget uint64) error {
	if c == nil {
		return nil
	}
	prepared, err := prepareArtifactChange(tx, c.before, c.after, nil, budget)
	if err != nil {
		return err
	}
	return prepared.apply(tx)
}

// BeginEnvironmentMaterialization admits, or replays, the one read of pinned
// artifact ref for committed incarnation (seq, incarnation) of environment
// id. It is trusted and controller-called. seq is the Store-assigned
// IncarnationSeq the caller read with the committed Environment; zero is
// never current. A fresh read needs the environment Present at revision. A
// replay returns the original open or closing read of the same seq unchanged.
// No tuple ever has a second read.
//
// The read is owned by the environment's principal, expires five minutes
// after now and grants nothing through GetArtifactDownload. Only
// MarkArtifactDownloadClosing and FinishArtifactDownload by its original
// owner, or stopped reader recovery, settle it. A replay needs the stored and
// the requested producer both fully known and equal. Any other unsettled read
// of the tuple is ErrEnvironmentMaterializationHeld, and a settled one is
// ErrEnvironmentMaterialized; neither writes.
func (s *Store) BeginEnvironmentMaterialization(id string, revision, seq uint64, incarnation string, ref ArtifactRef, producer OriginalServerLifetime, now time.Time) (out ArtifactLease, err error) {
	if now.IsZero() {
		return out, errors.New("invalid materialization time")
	}
	p, err := artifactProducer(producer)
	if err != nil {
		return
	}
	readID, err := newID()
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = beginEnvironmentMaterialization(tx, id, revision, seq, incarnation, ref, p, readID, now.UTC())
		return e
	})
	return
}

// beginEnvironmentMaterialization checks the expected sequence before the
// pin, witness or read is looked at.
func beginEnvironmentMaterialization(tx *bolt.Tx, id string, revision, seq uint64, incarnation string, ref ArtifactRef, p *OriginalServerLifetime, readID string, now time.Time) (ArtifactLease, error) {
	r, err := loadEnvironmentRecord(tx.Bucket(environmentsBucket), id)
	if err != nil {
		return ArtifactLease{}, err
	}
	if r.State.Phase == environment.Deleted {
		return ArtifactLease{}, ErrNotFound
	}
	if seq == 0 || seq != r.IncarnationSeq || incarnation == "" || incarnation != r.Incarnation {
		return ArtifactLease{}, ErrEnvironmentIncarnation
	}
	if tx.Bucket(artifactsBucket) == nil {
		return ArtifactLease{}, ErrNotFound
	}
	before, err := artifactLoad(tx)
	if err != nil {
		return ArtifactLease{}, err
	}
	key := environmentPinKey(id, ref)
	pin, ok := before.EnvironmentPins[key]
	if !ok {
		return ArtifactLease{}, ErrNotFound
	}
	o, exists := before.Objects[ref.ID]
	if pin.Owner != r.Principal || pin.Environment != id || pin.Ref != ref || !exists || o.State != "complete" || o.Ref != ref || o.Owner != r.Principal || !artifactFileValid(o.File) {
		return ArtifactLease{}, artifactCorrupt()
	}
	if pin.Read != "" {
		rd, ok := before.Reads[pin.Read]
		if !ok || pin.Seq > seq {
			return ArtifactLease{}, artifactCorrupt()
		}
		if pin.Seq == seq {
			if pin.Incarnation != incarnation || rd.Environment != id || rd.Incarnation != incarnation || rd.IncarnationSeq != seq || rd.Ref != ref || rd.Owner != r.Principal {
				return ArtifactLease{}, ErrArtifactConflict
			}
			return environmentReplay(before, pin.Read, rd, p)
		}
		// An older incarnation's read is settled before the incarnation
		// changes, so its receipt is replaced, never held.
		if !rd.Closed {
			return ArtifactLease{}, artifactCorrupt()
		}
	}
	if r.State.Desired != environment.Present {
		return ArtifactLease{}, ErrNotFound
	}
	if r.Revision != revision {
		return ArtifactLease{}, ErrEnvironmentStale
	}
	after := before.clone()
	if _, ok := after.Reads[readID]; ok {
		return ArtifactLease{}, ErrArtifactConflict
	}
	if pin.Read != "" {
		delete(after.Reads, pin.Read)
	}
	rd := artifactRead{Owner: r.Principal, Ref: ref, Revision: 1, Expires: now.Add(5 * time.Minute), Producer: p, Environment: id, Incarnation: incarnation, IncarnationSeq: seq}
	after.Reads[readID] = rd
	pin.Seq, pin.Incarnation, pin.Read = seq, incarnation, readID
	after.EnvironmentPins[key] = pin
	prepared, err := prepareArtifactChange(tx, before, after, nil, maxChangeBytes)
	if err != nil {
		return ArtifactLease{}, err
	}
	if err := prepared.apply(tx); err != nil {
		return ArtifactLease{}, err
	}
	return artifactLeaseDTO(readID, rd, o.File, after.Root.ID), nil
}

// environmentProducerKnown reports whether p names a stable server lifetime:
// valid, with the PID namespace device and process start ticks recorded. A
// nil, zero or partial identity is not proof that two calls came from the
// same lifetime, even when the values are equal.
func environmentProducerKnown(p *OriginalServerLifetime) bool {
	return p != nil && p.valid() && p.NamespaceDevice != 0 && p.StartTicks != 0
}

// environmentReplay returns the original lease of witness read r for a
// matching tuple. It writes nothing.
func environmentReplay(st *artifactState, id string, r artifactRead, producer *OriginalServerLifetime) (ArtifactLease, error) {
	if r.Closed {
		return ArtifactLease{}, ErrEnvironmentMaterialized
	}
	if !environmentProducerKnown(r.Producer) || !environmentProducerKnown(producer) || *r.Producer != *producer {
		return ArtifactLease{}, ErrEnvironmentMaterializationHeld
	}
	return artifactLeaseDTO(id, r, st.Objects[r.Ref.ID].File, st.Root.ID), nil
}
