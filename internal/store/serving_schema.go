package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/tmc/skygo/internal/serving"
	bolt "go.etcd.io/bbolt"
)

// checkServingFormat checks the serving facts held at fixed keys. Below
// schema13 neither the serving tag nor the serving_services family may
// exist, even empty. At schema13 the tag must be the exact scalar and the
// family a bucket. Cursor presence catches a scalar in place of the bucket.
func checkServingFormat(tx *bolt.Tx, n uint64) error {
	m := tx.Bucket(metadataBucket)
	tagged := m != nil && hasKey(m, servingStorageFormatKey)
	k, _ := tx.Cursor().Seek(servingServicesBucket)
	family := bytes.Equal(k, servingServicesBucket)
	if n < servingSchema {
		switch {
		case tagged:
			return foreignFormat("serving storage format tag before schema 13")
		case family:
			return foreignFormat("serving services before schema 13")
		}
		return nil
	}
	switch {
	case !tagged:
		return foreignFormat("missing serving storage format tag")
	case m.Bucket(servingStorageFormatKey) != nil:
		return foreignFormat("serving storage format tag is a bucket")
	case string(m.Get(servingStorageFormatKey)) != servingStorageFormat:
		return foreignFormat("unknown serving storage format tag")
	case tx.Bucket(servingServicesBucket) == nil:
		return foreignFormat("missing serving services bucket")
	}
	return nil
}

// migrateServing validates the genuine schema12 installation, including its
// environment storage format tag and artifact root floor, and installs in
// the same transaction the empty serving_services bucket, the serving
// storage format tag and marker13. It adds no artifact table and rewrites no
// row; the artifact root floor stays artifactFormat. The caller owns the
// transaction.
func migrateServing(tx *bolt.Tx) error {
	if schemaNumber(tx) != environmentRetirementSchema {
		return errors.New("serving storage requires schema 12")
	}
	if err := validateSnapshot(tx); err != nil {
		return err
	}
	if b := tx.Bucket(artifactsBucket); b != nil {
		var root artifactRoot
		if err := artifactDecode(b.Get([]byte("state")), &root); err != nil {
			return err
		}
		if root.SchemaFloor != artifactFormat {
			return artifactCorrupt()
		}
	}
	if _, err := tx.CreateBucket(servingServicesBucket); err != nil {
		return err
	}
	m := tx.Bucket(metadataBucket)
	if err := m.Put(servingStorageFormatKey, []byte(servingStorageFormat)); err != nil {
		return err
	}
	return m.Put(schemaKey, sequenceKey(servingSchema))
}

// validateServing checks every serving row: records against their name
// reservations, listing indexes and create witnesses; witnesses against a
// record or tombstone of their own principal and request; tombstones against
// the absence of a record; and the family's hard bounds. Below schema13 the
// bucket must not exist.
func validateServing(tx *bolt.Tx) error {
	b := tx.Bucket(servingServicesBucket)
	if schemaNumber(tx) < servingSchema {
		if b != nil {
			return errors.New("serving metadata requires schema 13")
		}
		return nil
	}
	if b == nil {
		return errors.New("missing serving metadata")
	}
	records := map[string]servingRecord{}
	tombstones := map[string]servingTombstone{}
	names := map[string]string{}
	index := map[uint64]string{}
	type witness struct {
		principal string
		domain    RequestDomain
		request   string
		w         servingWitness
	}
	var witnesses []witness
	var usage uint64
	if err := b.ForEach(func(k, v []byte) error {
		if v == nil {
			return errors.New("unexpected serving nested bucket")
		}
		var e error
		if usage, e = addChecked(usage, uint64(len(k)+len(v))); e != nil {
			return e
		}
		key := string(k)
		switch {
		case strings.HasPrefix(key, "e/"):
			r, e := decodeServingRecord(v)
			if e != nil || r.ID != key[2:] {
				return errors.New("invalid service record")
			}
			records[r.ID] = r
		case strings.HasPrefix(key, "n/"):
			p, name, ok := strings.Cut(key[2:], "/")
			if !ok || !validPrincipal([]byte(p)) || !ValidServiceName(name) || !ValidServiceID(string(v)) {
				return errors.New("invalid service name reservation")
			}
			names[key[2:]] = string(v)
		case strings.HasPrefix(key, "i/"):
			if len(k) != 10 || !ValidServiceID(string(v)) {
				return errors.New("invalid service index")
			}
			n := binary.BigEndian.Uint64(k[2:])
			if n == 0 || n > b.Sequence() {
				return errors.New("invalid service index ordinal")
			}
			index[n] = string(v)
		case strings.HasPrefix(key, "w/"):
			p, d, request, ok := parseServingWitnessKey(k)
			if !ok {
				return errors.New("invalid service request witness")
			}
			w, e := decodeServingWitness(v)
			if e != nil {
				return e
			}
			witnesses = append(witnesses, witness{p, d, request, w})
		case strings.HasPrefix(key, "t/"):
			t, e := decodeServingTombstone(v)
			if e != nil || t.ID != key[2:] {
				return errors.New("invalid service tombstone")
			}
			tombstones[t.ID] = t
		default:
			return errors.New("unknown serving row")
		}
		return nil
	}); err != nil {
		return err
	}
	switch {
	case usage > maxServingMetadataBytes:
		return errors.New("serving metadata exceeds limit")
	case len(records) > maxServingRecords:
		return errors.New("service records exceed limit")
	case len(tombstones) > maxServingTombstones:
		return errors.New("service tombstones exceed limit")
	case len(witnesses) > maxServingWitnesses:
		return errors.New("service request witnesses exceed limit")
	}
	owner := string(tx.Bucket(metadataBucket).Get([]byte("owner")))
	for id, r := range records {
		if r.Installation != owner {
			return errors.New("service installation mismatch")
		}
		if _, ok := tombstones[id]; ok {
			return errors.New("retired service has a record")
		}
		if names[r.Principal+"/"+r.Name] != id || index[r.Ordinal] != id {
			return errors.New("service reservation mismatch")
		}
	}
	if len(names) != len(records) || len(index) != len(records) {
		return errors.New("orphan service row")
	}
	for _, t := range tombstones {
		if t.Installation != owner {
			return errors.New("service installation mismatch")
		}
	}
	created := map[string]bool{}
	for _, x := range witnesses {
		var principal string
		var want string
		var domain RequestDomain
		var request string
		if r, ok := records[x.w.Service]; ok {
			principal, domain, request = r.Principal, r.Domain, r.RequestID
			first, _ := r.History.Revision(1)
			want = servingCreateDigest(r.Name, r.Target, first.SHA256)
		} else if t, ok := tombstones[x.w.Service]; ok {
			principal, domain, request = t.Principal, t.Domain, t.RequestID
			want = servingCreateDigest(t.Name, t.Target, t.Spec)
		} else {
			return errors.New("service request witness names no service")
		}
		if x.w.Kind == servingDelete {
			// A delete may be made by an admin of another principal, in any
			// domain; its digest binds the service and revision it deleted.
			continue
		}
		if created[x.w.Service] || principal != x.principal || domain != x.domain || request != x.request || want != x.w.Digest {
			return errors.New("service request witness mismatch")
		}
		created[x.w.Service] = true
	}
	for id, r := range records {
		if !created[id] {
			return errors.New("service request witness missing")
		}
		if first, ok := r.History.Revision(1); !ok || first.Number != 1 {
			return errors.New("service first revision not retained")
		}
	}
	return nil
}

// servingFirstDigest returns the digest of revision 1 of h. This format
// never prunes revision 1: it accepts no update.
func servingFirstDigest(h serving.History) (string, bool) {
	rev, ok := h.Revision(1)
	return rev.SHA256, ok
}
