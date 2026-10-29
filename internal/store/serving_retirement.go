package store

import (
	"bytes"
	"errors"
	"math"
	"time"

	bolt "go.etcd.io/bbolt"
)

// RetireService retires service id at record revision revision and returns
// its retired view. The service must be released: Desired Absent, an
// observed absence, no operation in flight and no reservation; otherwise it
// is ErrServiceHeld. It removes the record, name reservation and listing
// index and writes a tombstone, so that an exact replay of the service's
// create or delete request returns the retired view and never new work. The
// name is reusable afterwards. A stale revision is ErrServiceStale. At the
// tombstone bound it is refused with a *ServiceCapacityError until
// PruneServiceRequests removes tombstones that no request can replay; the
// service stays a released, deleted record meanwhile.
func (s *Store) RetireService(id string, revision uint64) (out Service, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = retireService(tx, id, revision, s.now().UTC())
		return e
	})
	return
}

func retireService(tx *bolt.Tx, id string, revision uint64, now time.Time) (out Service, err error) {
	b := tx.Bucket(servingServicesBucket)
	r, err := loadServingRecord(b, id)
	if err != nil {
		return
	}
	if r.Revision != revision {
		return out, ErrServiceStale
	}
	if !r.released() || r.Reserved != (ServiceReservation{}) {
		return out, ErrServiceHeld
	}
	if r.Revision == math.MaxUint64 {
		return out, errors.New("service revision exhausted")
	}
	if n := uint64(servingCount(b, []byte("t/"))); n >= maxServingTombstones {
		return out, &ServiceCapacityError{Domain: ServiceRequestsQuota, Used: n, Requested: 1, Limit: maxServingTombstones}
	}
	first, ok := servingFirstDigest(r.History)
	if !ok {
		return out, errors.New("service first revision not retained")
	}
	retired := r.Updated
	if now.After(retired) {
		retired = now
	}
	t := servingTombstone{
		ID:           r.ID,
		Installation: r.Installation,
		Principal:    r.Principal,
		Domain:       r.Domain,
		RequestID:    r.RequestID,
		Name:         r.Name,
		Target:       r.Target,
		Spec:         first,
		Created:      r.Created,
		Retired:      retired,
		Revision:     r.Revision + 1,
	}
	tombstone, err := encodeServingTombstone(t)
	if err != nil {
		return
	}
	doomed := [][]byte{servingRecordKey(id), servingNameKey(r.Principal, r.Name), servingIndexKey(r.Ordinal)}
	var removed uint64
	for i, k := range doomed {
		v := b.Get(k)
		if v == nil || i > 0 && string(v) != id {
			return out, errors.New("service reservation mismatch")
		}
		removed += uint64(len(k) + len(v))
	}
	usage, err := servingUsage(b)
	if err != nil {
		return
	}
	added := uint64(len(servingTombstoneKey(id)) + len(tombstone))
	if err = servingCharge(usage, removed, added); err != nil {
		return
	}
	for _, k := range doomed {
		if err = b.Delete(k); err != nil {
			return
		}
	}
	if err = b.Put(servingTombstoneKey(id), tombstone); err != nil {
		return
	}
	return servingTombstoneView(t), nil
}

// PruneServiceRequests removes at most limit request witnesses whose epoch
// is retired or whose lineage is no longer current and whose service is
// retired, then tombstones that no witness names. Such a request can no
// longer replay: the request domain refuses it first. A live service keeps
// every witness.
func (s *Store) PruneServiceRequests(limit int) (removed int, err error) {
	if limit < 1 || limit > maxServingWitnesses {
		return 0, errors.New("prune limit outside 1..4096")
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		removed, e = pruneServiceRequests(tx, limit)
		return e
	})
	return
}

func pruneServiceRequests(tx *bolt.Tx, limit int) (int, error) {
	b := tx.Bucket(servingServicesBucket)
	if b == nil {
		return 0, errors.New("serving metadata requires schema 13")
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
		_, d, _, ok := parseServingWitnessKey(k)
		if !ok {
			return 0, errors.New("invalid service request witness")
		}
		w, err := decodeServingWitness(v)
		if err != nil {
			return 0, err
		}
		stale := d.Lineage != st.Lineage || d.Epoch <= st.RetiredThrough
		if stale && b.Get(servingRecordKey(w.Service)) == nil && len(doomed) < limit {
			doomed = append(doomed, bytes.Clone(k))
			continue
		}
		named[w.Service] = true
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
