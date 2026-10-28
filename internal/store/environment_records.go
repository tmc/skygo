package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
	bolt "go.etcd.io/bbolt"
)

// The environments bucket, installed empty by the schema9 migration, holds
// flat typed rows. Ordinals are fixed eight-byte big-endian values and are
// never reused. Before schema12 rows are never removed, so the bucket
// sequence is the number of accepted environments. From schema12 an
// environment accepted in a request domain is witnessed under w/ instead of
// r/, and ReclaimEnvironment may replace its rows with a tombstone; the
// sequence is then the number of live environments plus the reclaimed
// counts.
//
//	e/<id>                   bounded environment record
//	s/<id>                   canonical immutable spec
//	r/<principal>/<request>  id; the request binding of a pre-12 record
//	n/<principal>/<name>     id; the name reservation
//	a/<alias>/<target>       id; the storage alias reservation
//	i/<ordinal>              id; the bucket sequence is the global high-water
//	h/<principal>            per-principal high-water
//	p/<principal>/<ordinal>  id
//	w/<principal>/<lineage>/<epoch>/<request>  request witness (schema12)
//	t/<id>                   tombstone of a reclaimed environment (schema12)
//	x/<principal>            count of the principal's reclaimed environments
//	                         (schema12)
var environmentsBucket = []byte("environments")

// environmentRecordLimit bounds one encoded record, which holds at most one
// maximal checkpoint in base64.
const environmentRecordLimit = 96 << 10

type environmentRecord struct {
	Identity   environment.Identity   `json:"identity"`
	Principal  string                 `json:"principal"`
	RequestID  string                 `json:"request_id"`
	Spec       string                 `json:"spec"`
	Binding    string                 `json:"binding"`
	Authority  auth.Delegation        `json:"authority"`
	Ordinal    uint64                 `json:"ordinal"`
	Scope      uint64                 `json:"scope"`
	Created    time.Time              `json:"created"`
	Updated    time.Time              `json:"updated"`
	Revision   uint64                 `json:"revision"`
	State      environment.State      `json:"state"`
	Checkpoint environment.Checkpoint `json:"checkpoint"`
	// The committed incarnation is omitted until one is committed, so
	// earlier records encode unchanged.
	Incarnation    string `json:"incarnation,omitempty"`
	IncarnationSeq uint64 `json:"incarnation_seq,omitempty"`
	// Domain is omitted for a record accepted before schema12, which has
	// an r/ binding instead and is never reclaimed.
	Domain *RequestDomain `json:"domain,omitempty"`
}

func environmentRecordKey(id string) []byte { return []byte("e/" + id) }
func environmentSpecKey(id string) []byte   { return []byte("s/" + id) }
func environmentRequestKey(principal, request string) []byte {
	return []byte("r/" + principal + "/" + request)
}
func environmentNameKey(principal, name string) []byte {
	return []byte("n/" + principal + "/" + name)
}
func environmentAliasKey(alias, target string) []byte {
	return []byte("a/" + alias + "/" + target)
}
func environmentIndexKey(n uint64) []byte { return append([]byte("i/"), sequenceKey(n)...) }
func environmentHighKey(principal string) []byte {
	return []byte("h/" + principal)
}
func environmentScopePrefix(principal string) []byte { return []byte("p/" + principal + "/") }
func environmentScopeKey(principal string, n uint64) []byte {
	return append(environmentScopePrefix(principal), sequenceKey(n)...)
}

// encodeEnvironmentRecord checks every record invariant that needs no other
// row and returns the record's only encoding.
func encodeEnvironmentRecord(r environmentRecord) ([]byte, error) {
	if !r.Identity.Valid() || !validPrincipal([]byte(r.Principal)) || !validRetainedRequestID(r.RequestID) || !artifactHex(r.Spec, 64) || !validBinding(r.Binding) || !validWorkflowAuthority(r.Authority, r.Principal) {
		return nil, errors.New("invalid environment identity")
	}
	if r.Ordinal == 0 || r.Scope == 0 || r.Revision == 0 || r.Created.IsZero() || r.Updated.Before(r.Created) {
		return nil, errors.New("invalid environment record")
	}
	if r.Domain != nil && !validRequestDomain(*r.Domain) {
		return nil, errors.New("invalid environment request domain")
	}
	if !r.State.Valid() {
		return nil, errors.New("invalid environment state")
	}
	if r.IncarnationSeq == 0 && r.Incarnation != "" || r.IncarnationSeq != 0 && !validIncarnation(r.Incarnation) {
		return nil, errors.New("invalid environment incarnation")
	}
	if err := r.Checkpoint.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) > environmentRecordLimit {
		return nil, errors.New("environment record exceeds limit")
	}
	return data, nil
}

// decodeEnvironmentRecord accepts only the encoding that
// encodeEnvironmentRecord produces.
func decodeEnvironmentRecord(raw []byte) (r environmentRecord, err error) {
	if raw == nil {
		return r, ErrNotFound
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&r) != nil || d.Decode(new(any)) != io.EOF {
		return r, errors.New("invalid environment record")
	}
	data, err := encodeEnvironmentRecord(r)
	if err != nil {
		return r, err
	}
	if !bytes.Equal(data, raw) {
		return r, errors.New("invalid environment record encoding")
	}
	return r, nil
}

func loadEnvironmentRecord(b *bolt.Bucket, id string) (environmentRecord, error) {
	if b == nil || !environment.ValidID(id) {
		return environmentRecord{}, ErrNotFound
	}
	r, err := decodeEnvironmentRecord(b.Get(environmentRecordKey(id)))
	if err == nil && r.Identity.ID != id {
		err = errors.New("environment record key mismatch")
	}
	return r, err
}

// environmentUsage is the bucket's metadata charge: every row's key and
// value bytes. It is rederived, never stored.
func environmentUsage(b *bolt.Bucket) (n uint64, err error) {
	err = b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected environment nested bucket")
		}
		var e error
		n, e = addChecked(n, uint64(len(k)+len(v)))
		return e
	})
	return
}

// environmentCharge reports whether usage can drop removed bytes and gain
// added bytes within the metadata bound.
func environmentCharge(usage, removed, added uint64) error {
	if removed > usage || added > maxEnvironmentMetadataBytes || usage-removed > maxEnvironmentMetadataBytes-added {
		return ErrEnvironmentLimit
	}
	return nil
}

func environmentHigh(b *bolt.Bucket, principal string) (uint64, error) {
	v := b.Get(environmentHighKey(principal))
	if v == nil {
		return 0, nil
	}
	if len(v) != 8 || binary.BigEndian.Uint64(v) == 0 {
		return 0, errors.New("invalid environment scope high-water")
	}
	return binary.BigEndian.Uint64(v), nil
}

// environmentView returns a copy that shares no memory with the database.
func environmentView(b *bolt.Bucket, id string) (out Environment, err error) {
	r, err := loadEnvironmentRecord(b, id)
	if err != nil {
		return
	}
	raw := b.Get(environmentSpecKey(id))
	digest := sha256.Sum256(raw)
	if raw == nil || hex.EncodeToString(digest[:]) != r.Spec {
		return out, errors.New("environment spec mismatch")
	}
	spec, err := environment.Parse(raw)
	if err != nil {
		return
	}
	return Environment{
		Identity:   r.Identity,
		Principal:  r.Principal,
		RequestID:  r.RequestID,
		Spec:       spec,
		SpecDigest: digest,
		Binding:    r.Binding,
		Authority:  r.Authority,
		CreatedAt:  r.Created,
		UpdatedAt:  r.Updated,
		Revision:   r.Revision,
		State:      r.State,
		Checkpoint: r.Checkpoint.Clone(),

		Incarnation:    r.Incarnation,
		IncarnationSeq: r.IncarnationSeq,
		Domain:         cloneEnvironmentDomain(r.Domain),
	}, nil
}

// validIncarnation reports whether s is 1..128 bytes of ASCII letters,
// digits, '.', '_', ':' or '-', none of which JSON escapes.
func validIncarnation(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
			return false
		}
	}
	return true
}
