package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// ErrEnvironmentLegacy reports an environment accepted before schema 12,
// without a request domain. It is never reclaimed.
var ErrEnvironmentLegacy = errors.New("environment predates request domains")

// maxEnvironmentTombstones bounds retained tombstones. ReclaimEnvironment is
// refused at the bound until PruneEnvironmentRequests removes tombstones that
// no request can replay.
const maxEnvironmentTombstones = 256

// environmentTombstone is a reclaimed environment's identity. It keeps the
// spec digest, never the spec: replay of its request supplies the spec, which
// must hash exactly to the digest, and returns its retired view, never new
// work.
type environmentTombstone struct {
	Identity  environment.Identity `json:"identity"`
	Principal string               `json:"principal"`
	Domain    RequestDomain        `json:"domain"`
	RequestID string               `json:"request_id"`
	Spec      string               `json:"spec"`
	Name      string               `json:"name"`
	Target    string               `json:"target"`
	Binding   string               `json:"binding"`
	Created   time.Time            `json:"created"`
	Retired   time.Time            `json:"retired"`
	Revision  uint64               `json:"revision"`
}

// environmentWitness binds a request in a domain to the environment it
// accepted and that request's canonical spec digest.
type environmentWitness struct {
	Environment string `json:"environment"`
	Spec        string `json:"spec"`
}

func environmentTombstoneKey(id string) []byte { return []byte("t/" + id) }
func environmentReclaimedKey(principal string) []byte {
	return []byte("x/" + principal)
}
func environmentWitnessKey(principal string, d RequestDomain, request string) []byte {
	k := []byte("w/" + principal + "/" + d.Lineage + "/")
	k = append(k, sequenceKey(d.Epoch)...)
	return append(append(k, '/'), request...)
}

// parseEnvironmentWitnessKey inverts environmentWitnessKey. A principal
// never contains '/', and the lineage and epoch have fixed widths.
func parseEnvironmentWitnessKey(k []byte) (principal string, d RequestDomain, request string, ok bool) {
	rest, ok := bytes.CutPrefix(k, []byte("w/"))
	if !ok {
		return
	}
	p, rest, ok := bytes.Cut(rest, []byte("/"))
	if !ok || len(rest) < 32+1+8+1 || rest[32] != '/' || rest[41] != '/' {
		return "", d, "", false
	}
	principal, request = string(p), string(rest[42:])
	d = RequestDomain{Lineage: string(rest[:32]), Epoch: binary.BigEndian.Uint64(rest[33:41])}
	if !validPrincipal(p) || !validRequestDomain(d) || !validRetainedRequestID(request) {
		return "", d, "", false
	}
	return principal, d, request, true
}

// environmentDecode decodes exactly one JSON value with no unknown fields.
func environmentDecode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid environment row")
	}
	return nil
}

func encodeEnvironmentTombstone(t environmentTombstone) ([]byte, error) {
	if !t.Identity.Valid() || !validPrincipal([]byte(t.Principal)) || !validRequestDomain(t.Domain) || !validRetainedRequestID(t.RequestID) || !artifactHex(t.Spec, 64) || !environment.ValidName(t.Name) || !environment.ValidTarget(t.Target) || !validBinding(t.Binding) {
		return nil, errors.New("invalid environment tombstone")
	}
	if t.Created.IsZero() || t.Retired.Before(t.Created) || t.Revision == 0 {
		return nil, errors.New("invalid environment tombstone")
	}
	return json.Marshal(t)
}

func decodeEnvironmentTombstone(raw []byte) (t environmentTombstone, err error) {
	if raw == nil {
		return t, ErrNotFound
	}
	if err = environmentDecode(raw, &t); err != nil {
		return
	}
	data, err := encodeEnvironmentTombstone(t)
	if err != nil {
		return
	}
	if !bytes.Equal(data, raw) {
		return t, errors.New("invalid environment tombstone encoding")
	}
	return t, nil
}

func encodeEnvironmentWitness(w environmentWitness) ([]byte, error) {
	if !environment.ValidID(w.Environment) || !artifactHex(w.Spec, 64) {
		return nil, errors.New("invalid environment request witness")
	}
	return json.Marshal(w)
}

func decodeEnvironmentWitness(raw []byte) (w environmentWitness, err error) {
	if err = environmentDecode(raw, &w); err != nil {
		return
	}
	data, err := encodeEnvironmentWitness(w)
	if err != nil {
		return
	}
	if !bytes.Equal(data, raw) {
		return w, errors.New("invalid environment request witness encoding")
	}
	return w, nil
}

func cloneEnvironmentDomain(d *RequestDomain) *RequestDomain {
	if d == nil {
		return nil
	}
	c := *d
	return &c
}

// environmentTombstoneView returns the retired view of t with the detached
// spec raw. The caller has matched raw to the request; it must also be
// canonical spec bytes whose digest is exactly the tombstone's and whose name
// and target are the tombstone's, or the tombstone is corrupt.
func environmentTombstoneView(t environmentTombstone, raw []byte) (Environment, error) {
	digest := sha256.Sum256(raw)
	spec, err := environment.Parse(raw)
	if err != nil || hex.EncodeToString(digest[:]) != t.Spec || spec.Name != t.Name || spec.Target != t.Target {
		return Environment{}, errors.New("environment tombstone spec mismatch")
	}
	d := t.Domain
	return Environment{
		Identity:   t.Identity,
		Principal:  t.Principal,
		RequestID:  t.RequestID,
		Spec:       spec,
		SpecDigest: digest,
		Binding:    t.Binding,
		CreatedAt:  t.Created,
		UpdatedAt:  t.Retired,
		Revision:   t.Revision,
		State:      environment.State{Desired: environment.Absent, Phase: environment.Deleted},
		Domain:     &d,
		Retired:    true,
	}, nil
}

// environmentCount counts the bucket's rows with prefix.
func environmentCount(b *bolt.Bucket, prefix []byte) int {
	n := 0
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		n++
	}
	return n
}

// environmentEpochClosed reports whether d's epoch accepts no new request.
func environmentEpochClosed(st retentionState, d RequestDomain) bool {
	for _, ep := range st.Epochs {
		if ep.Number == d.Epoch && ep.AcceptClosed {
			return true
		}
	}
	return false
}

// environmentWitnessReplay finds the request witness of principal's request
// in domain d. Only identical canonical spec bytes replay, returning the
// environment or, once reclaimed, its retired view.
func environmentWitnessReplay(b *bolt.Bucket, principal string, d RequestDomain, request string, raw []byte) (out Environment, found bool, err error) {
	v := b.Get(environmentWitnessKey(principal, d, request))
	if v == nil {
		return out, false, nil
	}
	w, err := decodeEnvironmentWitness(v)
	if err != nil {
		return out, true, err
	}
	digest := sha256.Sum256(raw)
	if w.Spec != hex.EncodeToString(digest[:]) {
		return out, true, ErrConflict
	}
	if out, err = environmentView(b, w.Environment); errors.Is(err, ErrNotFound) {
		var t environmentTombstone
		if t, err = decodeEnvironmentTombstone(b.Get(environmentTombstoneKey(w.Environment))); err == nil {
			out, err = environmentTombstoneView(t, raw)
		} else if errors.Is(err, ErrNotFound) {
			err = errors.New("environment request witness names no environment")
		}
	}
	if err == nil && (out.Principal != principal || out.RequestID != request || out.Domain == nil || *out.Domain != d) {
		err = errors.New("environment request witness mismatch")
	}
	if err != nil {
		return Environment{}, true, err
	}
	return out, true, nil
}

// EnvironmentRequest reports, without writing, what AcceptEnvironment would
// return for an exact replay of requestID in domain d by actor. It checks
// the domain as AcceptEnvironment does, evaluating the retention phases at
// the current time without storing them. A witnessed request with identical
// canonical spec bytes returns the environment or its retired view and
// found; with different bytes it is ErrConflict. An unwitnessed request in an
// epoch closed to new requests is an *ExpiryError. Otherwise found is false.
// The answer is advisory: only AcceptEnvironment decides, in its own
// transaction.
func (s *Store) EnvironmentRequest(actor auth.Actor, d RequestDomain, requestID string, spec environment.Spec) (out Environment, found bool, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(requestID) {
		return out, false, errors.New("invalid request ID")
	}
	if !validRequestDomain(d) {
		return out, false, ErrInvalidRequestDomain
	}
	raw, err := environment.Canonical(spec)
	if err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(environmentsBucket)
		if b == nil {
			return errors.New("environment metadata requires schema 9")
		}
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if _, result := domainResult(&st, d, s.now()); result != nil {
			return result
		}
		out, found, e = environmentWitnessReplay(b, actor.Principal(), d, requestID, raw)
		if e != nil || found {
			return e
		}
		if environmentEpochClosed(st, d) {
			return &ExpiryError{Reason: RequestEpochExpired}
		}
		return nil
	})
	if err != nil {
		return Environment{}, false, err
	}
	return
}

// ReclaimEnvironment retires Deleted environment id at record revision
// revision and returns its retired view. It removes the record, spec, name
// and storage alias reservations and index rows, counts the reclaim against
// the principal, and writes a tombstone of the spec digest so that exact
// replay of the request returns the retired view, with the replayed spec. The name and alias are reusable afterwards. A stale revision is
// ErrEnvironmentStale. It is refused for an environment that is not Deleted,
// for a pre-12 one with ErrEnvironmentLegacy, while any artifact read of the
// environment remains with ErrEnvironmentArtifactHeld, and with
// ErrEnvironmentLimit at the tombstone bound. Nothing reclaims
// automatically.
func (s *Store) ReclaimEnvironment(id string, revision uint64) (out Environment, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = reclaimEnvironment(tx, id, revision, s.now().UTC())
		return e
	})
	return
}

func reclaimEnvironment(tx *bolt.Tx, id string, revision uint64, now time.Time) (out Environment, err error) {
	b := tx.Bucket(environmentsBucket)
	r, err := loadEnvironmentRecord(b, id)
	if err != nil {
		return
	}
	if r.Revision != revision {
		return out, ErrEnvironmentStale
	}
	if r.State.Phase != environment.Deleted {
		return out, errors.New("environment is not deleted")
	}
	if r.Domain == nil {
		return out, ErrEnvironmentLegacy
	}
	if r.Revision == math.MaxUint64 {
		return out, errors.New("environment revision exhausted")
	}
	if environmentCount(b, []byte("t/")) >= maxEnvironmentTombstones {
		return out, ErrEnvironmentLimit
	}
	if tx.Bucket(artifactsBucket) != nil {
		st, err := artifactLoad(tx)
		if err != nil {
			return out, err
		}
		for _, p := range st.EnvironmentPins {
			if p.Environment == id {
				return out, artifactCorrupt()
			}
		}
		for _, rd := range st.Reads {
			if rd.Environment == id {
				return out, ErrEnvironmentArtifactHeld
			}
		}
	}
	// The spec bytes are cloned: the retired view is rebuilt from them after
	// their row is deleted.
	raw := bytes.Clone(b.Get(environmentSpecKey(id)))
	spec, err := environment.Parse(raw)
	if err != nil {
		return
	}
	retired := r.Updated
	if now.After(retired) {
		retired = now
	}
	t := environmentTombstone{
		Identity:  r.Identity,
		Principal: r.Principal,
		Domain:    *r.Domain,
		RequestID: r.RequestID,
		Spec:      r.Spec,
		Name:      spec.Name,
		Target:    spec.Target,
		Binding:   r.Binding,
		Created:   r.Created,
		Retired:   retired,
		Revision:  r.Revision + 1,
	}
	tombstone, err := encodeEnvironmentTombstone(t)
	if err != nil {
		return
	}
	var count uint64
	if v := b.Get(environmentReclaimedKey(r.Principal)); v != nil {
		if len(v) != 8 || binary.BigEndian.Uint64(v) == 0 {
			return out, errors.New("invalid environment reclaimed count")
		}
		count = binary.BigEndian.Uint64(v)
	}
	if count == math.MaxUint64 {
		return out, errors.New("environment reclaimed count exhausted")
	}
	doomed := [][]byte{
		environmentRecordKey(id),
		environmentSpecKey(id),
		environmentNameKey(r.Principal, spec.Name),
		environmentIndexKey(r.Ordinal),
		environmentScopeKey(r.Principal, r.Scope),
	}
	if spec.StorageAlias != "" {
		doomed = append(doomed, environmentAliasKey(spec.StorageAlias, spec.Target))
	}
	usage, err := environmentUsage(b)
	if err != nil {
		return
	}
	var removed uint64
	for i, k := range doomed {
		v := b.Get(k)
		if v == nil || i > 1 && string(v) != id {
			return out, errors.New("environment reservation mismatch")
		}
		removed += uint64(len(k) + len(v))
	}
	x := environmentReclaimedKey(r.Principal)
	if count != 0 {
		removed += uint64(len(x) + 8)
	}
	added := uint64(len(environmentTombstoneKey(id))+len(tombstone)) + uint64(len(x)+8)
	if err = environmentCharge(usage, removed, added); err != nil {
		return
	}
	for _, k := range doomed {
		if err = b.Delete(k); err != nil {
			return
		}
	}
	if err = b.Put(environmentTombstoneKey(id), tombstone); err != nil {
		return
	}
	if err = b.Put(x, sequenceKey(count+1)); err != nil {
		return
	}
	return environmentTombstoneView(t, raw)
}

// PruneEnvironmentRequests removes at most limit request witnesses whose
// epoch is retired or whose lineage is no longer current, then tombstones
// that no witness names. Such a request can no longer replay: the request
// domain refuses it first. The witness of a live record is kept until the
// record is reclaimed. Reclaimed counts are never pruned.
func (s *Store) PruneEnvironmentRequests(limit int) (removed int, err error) {
	if limit < 1 || limit > maxEnvironmentTombstones+maxEnvironments {
		return 0, errors.New("prune limit outside 1..288")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		removed, e = pruneEnvironmentRequests(tx, limit)
		return e
	})
	return
}

func pruneEnvironmentRequests(tx *bolt.Tx, limit int) (int, error) {
	b := tx.Bucket(environmentsBucket)
	if b == nil {
		return 0, errors.New("environment metadata requires schema 9")
	}
	st, err := loadRetention(tx)
	if err != nil {
		return 0, err
	}
	if !st.Enabled {
		return 0, nil
	}
	var doomed [][]byte
	named := make(map[string]bool)
	c := b.Cursor()
	for k, v := c.Seek([]byte("w/")); k != nil && bytes.HasPrefix(k, []byte("w/")); k, v = c.Next() {
		_, d, _, ok := parseEnvironmentWitnessKey(k)
		if !ok {
			return 0, errors.New("invalid environment request witness")
		}
		w, err := decodeEnvironmentWitness(v)
		if err != nil {
			return 0, err
		}
		stale := d.Lineage != st.Lineage || d.Epoch <= st.RetiredThrough
		if stale && b.Get(environmentRecordKey(w.Environment)) == nil && len(doomed) < limit {
			doomed = append(doomed, bytes.Clone(k))
			continue
		}
		named[w.Environment] = true
	}
	for k, _ := c.Seek([]byte("t/")); k != nil && bytes.HasPrefix(k, []byte("t/")) && len(doomed) < limit; k, _ = c.Next() {
		if !named[string(k[2:])] {
			doomed = append(doomed, bytes.Clone(k))
		}
	}
	for _, k := range doomed {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	return len(doomed), nil
}

// migrateEnvironmentRetirement validates the genuine schema11 installation,
// including its environment storage format tag, and writes marker12, which
// admits request witnesses, tombstones and reclaimed counts. It writes no
// such row: the tombstone domain starts empty, every existing record stays a
// pre-12 record with its r/ binding, the tag is kept, and no row is
// rewritten. The caller owns the transaction.
func migrateEnvironmentRetirement(tx *bolt.Tx) error {
	if schemaNumber(tx) != environmentArtifactSchema {
		return errors.New("environment retirement requires schema 11")
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	return tx.Bucket(metadataBucket).Put(schemaKey, sequenceKey(environmentRetirementSchema))
}
