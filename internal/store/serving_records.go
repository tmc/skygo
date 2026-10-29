package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	bolt "go.etcd.io/bbolt"
)

// The serving_services bucket, installed empty by the schema13 migration,
// holds flat typed rows. It is distinct from the foreign services bucket of
// another storage line, which stays refused. Ordinals are fixed eight-byte
// big-endian values and are never reused; the bucket sequence is the
// high-water ordinal.
//
//	e/<id>                                      bounded service record
//	n/<principal>/<name>                        id; the name reservation
//	i/<ordinal>                                 id; the listing index
//	w/<principal>/<lineage>/<epoch>/<request>   request witness
//	t/<id>                                      tombstone of a retired service
var servingServicesBucket = []byte("serving_services")

// Hard bounds of the serving family. ServiceLimits are operator policy
// within them.
const (
	maxServingRecords       = 64
	maxServingTombstones    = 256
	maxServingWitnesses     = 4096
	maxServingMetadataBytes = 48 << 20
	servingRecordLimit      = 640 << 10 // 8 revisions of at most 64 KiB, encoded
	maxServingConditions    = 16
)

// Witness kinds.
const (
	servingCreate = "create"
	servingDelete = "delete"
)

type servingRecord struct {
	ID           string               `json:"id"`
	Installation string               `json:"installation"`
	Nonce        string               `json:"nonce"`
	Principal    string               `json:"principal"`
	Domain       RequestDomain        `json:"domain"`
	RequestID    string               `json:"request_id"`
	Name         string               `json:"name"`
	Target       string               `json:"target"`
	Binding      string               `json:"binding"`
	Authority    auth.Delegation      `json:"authority"`
	Ordinal      uint64               `json:"ordinal"`
	Created      time.Time            `json:"created"`
	Updated      time.Time            `json:"updated"`
	Revision     uint64               `json:"revision"`
	Desired      serving.Desired      `json:"desired"`
	History      serving.History      `json:"history"`
	Native       serving.Native       `json:"native,omitempty"`
	Observation  *serving.Observation `json:"observation,omitempty"`
	ObservedAt   time.Time            `json:"observed_at"`
	Operation    *ServiceOperation    `json:"operation,omitempty"`
	Conditions   []ServiceCondition   `json:"conditions,omitempty"`
	Reserved     ServiceReservation   `json:"reserved"`
}

// servingWitness binds a request in a domain to the service it created or
// deleted and to that request's digest.
type servingWitness struct {
	Kind    string `json:"kind"`
	Service string `json:"service"`
	Digest  string `json:"digest"`
}

// servingTombstone is a retired service's identity. It keeps the digest of
// revision 1, never a spec.
type servingTombstone struct {
	ID           string        `json:"id"`
	Installation string        `json:"installation"`
	Principal    string        `json:"principal"`
	Domain       RequestDomain `json:"domain"`
	RequestID    string        `json:"request_id"`
	Name         string        `json:"name"`
	Target       string        `json:"target"`
	Spec         string        `json:"spec"`
	Created      time.Time     `json:"created"`
	Retired      time.Time     `json:"retired"`
	Revision     uint64        `json:"revision"`
}

func servingRecordKey(id string) []byte { return []byte("e/" + id) }
func servingNameKey(principal, name string) []byte {
	return []byte("n/" + principal + "/" + name)
}
func servingIndexKey(n uint64) []byte      { return append([]byte("i/"), sequenceKey(n)...) }
func servingTombstoneKey(id string) []byte { return []byte("t/" + id) }
func servingWitnessKey(principal string, d RequestDomain, request string) []byte {
	k := []byte("w/" + principal + "/" + d.Lineage + "/")
	k = append(k, sequenceKey(d.Epoch)...)
	return append(append(k, '/'), request...)
}

// parseServingWitnessKey inverts servingWitnessKey.
func parseServingWitnessKey(k []byte) (principal string, d RequestDomain, request string, ok bool) {
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

var servingName = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)

// ValidServiceName reports whether s is a service name: a DNS label that
// starts with a letter.
func ValidServiceName(s string) bool { return servingName.MatchString(s) }

// validServiceTarget reports whether s is a configured target name.
func validServiceTarget(s string) bool { return servingName.MatchString(s) }

// ValidServiceID reports whether s is a service ID: 32 lowercase hexadecimal
// digits.
func ValidServiceID(s string) bool { return artifactHex(s, 32) }

// servingCreateDigest is the digest a create request is replayed by.
func servingCreateDigest(name, target, spec string) string {
	sum := sha256.Sum256([]byte("skygo-serving-create-v1\x00" + name + "\x00" + target + "\x00" + spec))
	return hex.EncodeToString(sum[:])
}

// servingDeleteDigest is the digest a delete request is replayed by.
func servingDeleteDigest(id string, revision uint64) string {
	sum := sha256.Sum256([]byte("skygo-serving-delete-v1\x00" + id + "\x00" + strconv.FormatUint(revision, 10)))
	return hex.EncodeToString(sum[:])
}

// servingDecode decodes exactly one JSON value with no unknown fields.
func servingDecode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return errors.New("invalid serving row")
	}
	return nil
}

func validServiceOperation(op *ServiceOperation) bool {
	return op == nil || artifactHex(op.ID, 32) && (op.Kind == ServiceApply || op.Kind == ServiceDelete) &&
		op.Generation != 0 && op.Revision != 0 && !op.Started.IsZero()
}

func validServiceConditions(cs []ServiceCondition) bool {
	if len(cs) > maxServingConditions {
		return false
	}
	seen := map[ServiceConditionCode]bool{}
	for _, c := range cs {
		if c.Code < ServiceOperationUncertain || c.Code > ServiceCleanupPending || seen[c.Code] || c.Since.IsZero() {
			return false
		}
		seen[c.Code] = true
	}
	return true
}

// servingReservation returns the charge of the current revision of h.
func servingReservation(h serving.History) (ServiceReservation, error) {
	rev, ok := h.Revision(h.Current().Revision)
	if !ok {
		return ServiceReservation{}, errors.New("current service revision not retained")
	}
	s := rev.Spec
	n := uint64(s.Scaling.Replicas)
	return ServiceReservation{Replicas: n, CPUMillis: n * uint64(s.Resources.CPUMillis), MemoryBytes: n * uint64(s.Resources.MemoryBytes)}, nil
}

// servingRefusesData reports whether a spec needs artifact pins, which this
// storage format does not have.
func servingRefusesData(s serving.Spec) bool {
	return len(s.Mounts) != 0 || s.Workdir != nil || len(s.Setup) != 0
}

// encodeServingRecord checks every record invariant that needs no other row
// and returns the record's only encoding.
func encodeServingRecord(r servingRecord) ([]byte, error) {
	switch {
	case !artifactHex(r.ID, 32) || !artifactHex(r.Installation, 32) || !artifactHex(r.Nonce, 32),
		!validPrincipal([]byte(r.Principal)) || !validRequestDomain(r.Domain) || !validRetainedRequestID(r.RequestID),
		!ValidServiceName(r.Name) || !validServiceTarget(r.Target) || !validBinding(r.Binding),
		!validWorkflowAuthority(r.Authority, r.Principal):
		return nil, errors.New("invalid service identity")
	case r.Ordinal == 0 || r.Revision == 0 || r.Created.IsZero() || r.Updated.Before(r.Created):
		return nil, errors.New("invalid service record")
	case r.Desired != serving.Present && r.Desired != serving.Absent:
		return nil, errors.New("invalid service intent")
	}
	if err := r.History.Validate(); err != nil {
		return nil, err
	}
	for _, rev := range r.History.Revisions {
		if servingRefusesData(rev.Spec) {
			return nil, errors.New("service data requires artifact pins")
		}
	}
	if err := r.Ref().Validate(); err != nil {
		return nil, err
	}
	if r.Observation != nil {
		if err := r.Observation.Validate(); err != nil {
			return nil, err
		}
		if r.ObservedAt.IsZero() || r.Observation.ObservedGeneration > r.History.Current().Generation {
			return nil, errors.New("invalid service observation")
		}
	} else if !r.ObservedAt.IsZero() {
		return nil, errors.New("invalid service observation time")
	}
	if !validServiceOperation(r.Operation) {
		return nil, errors.New("invalid service operation")
	}
	if op := r.Operation; op != nil {
		if _, ok := r.History.Revision(op.Revision); !ok || op.Generation > r.History.Current().Generation {
			return nil, errors.New("service operation names no rollout")
		}
		if op.Kind == ServiceDelete && r.Desired != serving.Absent {
			return nil, errors.New("service delete operation while present")
		}
	}
	if !validServiceConditions(r.Conditions) {
		return nil, errors.New("invalid service conditions")
	}
	want, err := servingReservation(r.History)
	if err != nil {
		return nil, err
	}
	if r.Reserved != want && (r.Reserved != ServiceReservation{} || !r.released()) {
		return nil, errors.New("invalid service reservation")
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(data) > servingRecordLimit {
		return nil, errors.New("service record exceeds limit")
	}
	return data, nil
}

// released reports whether r's absence is established: Absent intent, an
// observed absence and no operation in flight. Only then may its
// reservation be zero.
func (r servingRecord) released() bool {
	return r.Desired == serving.Absent && r.Observation != nil && r.Observation.Absent && r.Operation == nil
}

func (r servingRecord) Ref() serving.Ref {
	return serving.Ref{Service: r.ID, Owner: r.Installation, Nonce: r.Nonce, Binding: r.Binding, Native: r.Native}
}

// decodeServingRecord accepts only the encoding that encodeServingRecord
// produces.
func decodeServingRecord(raw []byte) (r servingRecord, err error) {
	if raw == nil {
		return r, ErrNotFound
	}
	if err = servingDecode(raw, &r); err != nil {
		return r, errors.New("invalid service record")
	}
	data, err := encodeServingRecord(r)
	if err != nil {
		return r, err
	}
	if !bytes.Equal(data, raw) {
		return r, errors.New("invalid service record encoding")
	}
	return r, nil
}

func loadServingRecord(b *bolt.Bucket, id string) (servingRecord, error) {
	if b == nil || !ValidServiceID(id) {
		return servingRecord{}, ErrNotFound
	}
	r, err := decodeServingRecord(b.Get(servingRecordKey(id)))
	if err == nil && r.ID != id {
		err = errors.New("service record key mismatch")
	}
	return r, err
}

func encodeServingWitness(w servingWitness) ([]byte, error) {
	if w.Kind != servingCreate && w.Kind != servingDelete || !ValidServiceID(w.Service) || !artifactHex(w.Digest, 64) {
		return nil, errors.New("invalid service request witness")
	}
	return json.Marshal(w)
}

func decodeServingWitness(raw []byte) (w servingWitness, err error) {
	if err = servingDecode(raw, &w); err != nil {
		return w, errors.New("invalid service request witness")
	}
	data, err := encodeServingWitness(w)
	if err != nil {
		return
	}
	if !bytes.Equal(data, raw) {
		return w, errors.New("invalid service request witness encoding")
	}
	return w, nil
}

func encodeServingTombstone(t servingTombstone) ([]byte, error) {
	switch {
	case !ValidServiceID(t.ID) || !artifactHex(t.Installation, 32) || !validPrincipal([]byte(t.Principal)),
		!validRequestDomain(t.Domain) || !validRetainedRequestID(t.RequestID),
		!ValidServiceName(t.Name) || !validServiceTarget(t.Target) || !artifactHex(t.Spec, 64),
		t.Created.IsZero() || t.Retired.Before(t.Created) || t.Revision == 0:
		return nil, errors.New("invalid service tombstone")
	}
	return json.Marshal(t)
}

func decodeServingTombstone(raw []byte) (t servingTombstone, err error) {
	if raw == nil {
		return t, ErrNotFound
	}
	if err = servingDecode(raw, &t); err != nil {
		return t, errors.New("invalid service tombstone")
	}
	data, err := encodeServingTombstone(t)
	if err != nil {
		return
	}
	if !bytes.Equal(data, raw) {
		return t, errors.New("invalid service tombstone encoding")
	}
	return t, nil
}

// servingView returns r as a Service. Decoding gave it its own memory.
func servingView(r servingRecord) Service {
	return Service{
		ID:           r.ID,
		Installation: r.Installation,
		Nonce:        r.Nonce,
		Principal:    r.Principal,
		Domain:       r.Domain,
		RequestID:    r.RequestID,
		Name:         r.Name,
		Target:       r.Target,
		Binding:      r.Binding,
		Authority:    r.Authority,
		CreatedAt:    r.Created,
		UpdatedAt:    r.Updated,
		Revision:     r.Revision,
		Desired:      r.Desired,
		History:      r.History,
		Native:       r.Native,
		Observation:  r.Observation,
		ObservedAt:   r.ObservedAt,
		Operation:    r.Operation,
		Conditions:   r.Conditions,
		Reserved:     r.Reserved,
	}
}

// servingTombstoneView returns the retired view of t.
func servingTombstoneView(t servingTombstone) Service {
	return Service{
		ID:           t.ID,
		Installation: t.Installation,
		Principal:    t.Principal,
		Domain:       t.Domain,
		RequestID:    t.RequestID,
		Name:         t.Name,
		Target:       t.Target,
		CreatedAt:    t.Created,
		UpdatedAt:    t.Retired,
		Revision:     t.Revision,
		Desired:      serving.Absent,
		Retired:      true,
	}
}

// servingLoad returns the record of id or, once retired, its view.
func servingLoad(b *bolt.Bucket, id string) (Service, error) {
	r, err := loadServingRecord(b, id)
	if err == nil {
		return servingView(r), nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Service{}, err
	}
	t, err := decodeServingTombstone(b.Get(servingTombstoneKey(id)))
	if err != nil {
		return Service{}, err
	}
	if t.ID != id {
		return Service{}, errors.New("service tombstone key mismatch")
	}
	return servingTombstoneView(t), nil
}

// servingUsage is the bucket's metadata charge: every row's key and value
// bytes. It is rederived, never stored.
func servingUsage(b *bolt.Bucket) (n uint64, err error) {
	err = b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected serving nested bucket")
		}
		var e error
		n, e = addChecked(n, uint64(len(k)+len(v)))
		return e
	})
	return
}

// servingCharge refuses a change that takes usage past the metadata bound.
func servingCharge(usage, removed, added uint64) error {
	if removed > usage || added > maxServingMetadataBytes || usage-removed > maxServingMetadataBytes-added {
		return &ServiceCapacityError{Domain: ServiceRevisionsQuota, Used: usage, Requested: added, Limit: maxServingMetadataBytes}
	}
	return nil
}

// servingCount counts the bucket's rows with prefix.
func servingCount(b *bolt.Bucket, prefix []byte) int {
	n := 0
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		n++
	}
	return n
}
