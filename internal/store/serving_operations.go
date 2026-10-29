package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/serving"
	bolt "go.etcd.io/bbolt"
)

// AcceptService atomically accepts r in the actor's namespace as a Present
// service with revision 1 of r.Spec, rollout generation 1, record revision 1
// and a fresh creation nonce, and records the generation 1 Apply operation,
// all before any runtime call. A malformed domain is
// ErrInvalidRequestDomain. The domain is checked first: a disabled store is
// ErrRetentionDisabled, another lineage or a retired epoch is an
// *ExpiryError and an epoch never issued is ErrUnknownRequestEpoch, even for
// an exact replay. Then the same request, by name, target and normalized
// spec, returns the original service or its retired view; any other request
// under the same key is ErrConflict. A new acceptance is refused with an
// *ExpiryError once the epoch is closed to new requests, requires scoped
// authentication and a valid delegation of the actor, holds the name for the
// principal until retirement (ErrServiceNameInUse), and charges limits on
// r.Binding: unretired services of the principal and installation and the
// replicas, CPU and memory of every unreleased service of the binding
// (*ServiceCapacityError). A spec with mounts, workdir or setup is refused:
// this format has no artifact pins. Every refusal writes nothing.
func (s *Store) AcceptService(actor auth.Actor, r ServiceRequest, limits ServiceLimits) (out Service, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(r.RequestID) {
		return out, errors.New("invalid request ID")
	}
	if !validRequestDomain(r.Domain) {
		return out, ErrInvalidRequestDomain
	}
	if !ValidServiceName(r.Name) || !validServiceTarget(r.Target) {
		return out, errors.New("invalid service name or target")
	}
	h, err := serving.NewHistory(r.Spec)
	if err != nil {
		return
	}
	first, _ := servingFirstDigest(h)
	id, err := newID()
	if err != nil {
		return
	}
	nonce, err := newID()
	if err != nil {
		return
	}
	op, err := newID()
	if err != nil {
		return
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = acceptService(tx, actor.Principal(), r, h, first, limits, id, nonce, op, s.now().UTC())
		return e
	})
	return
}

func acceptService(tx *bolt.Tx, principal string, r ServiceRequest, h serving.History, first string, limits ServiceLimits, id, nonce, op string, now time.Time) (out Service, err error) {
	b := tx.Bucket(servingServicesBucket)
	if b == nil {
		return out, errors.New("serving metadata requires schema 13")
	}
	st, result, err := gateDomain(tx, r.Domain, now)
	if err != nil {
		return
	}
	if result != nil {
		return out, result
	}
	digest := servingCreateDigest(r.Name, r.Target, first)
	out, found, err := servingReplay(b, principal, r.Domain, r.RequestID, servingCreate, digest)
	if found || err != nil {
		return
	}
	if environmentEpochClosed(st, r.Domain) {
		return out, &ExpiryError{Reason: RequestEpochExpired}
	}
	if !isScoped(tx) {
		return out, auth.ErrPermissionDenied
	}
	if !validWorkflowAuthority(r.Authority, principal) {
		return out, auth.ErrUnauthenticated
	}
	if !validBinding(r.Binding) {
		return out, errors.New("invalid target binding")
	}
	for _, rev := range h.Revisions {
		if servingRefusesData(rev.Spec) {
			return out, errors.New("service data requires artifact pins")
		}
	}
	limits, err = limits.normalize()
	if err != nil {
		return
	}
	if b.Get(servingNameKey(principal, r.Name)) != nil {
		return out, ErrServiceNameInUse
	}
	charge, err := servingReservation(h)
	if err != nil {
		return
	}
	if err = servingAdmit(b, principal, r.Binding, charge, limits); err != nil {
		return
	}
	if n := uint64(servingCount(b, []byte("w/"))); n >= maxServingWitnesses {
		return out, &ServiceCapacityError{Domain: ServiceRequestsQuota, Used: n, Requested: 1, Limit: maxServingWitnesses}
	}
	if b.Get(servingRecordKey(id)) != nil || b.Get(servingTombstoneKey(id)) != nil {
		return out, errors.New("random service identity collision")
	}
	if b.Sequence() == math.MaxUint64 {
		return out, errors.New("service sequence exhausted")
	}
	ordinal := b.Sequence() + 1
	cur := h.Current()
	rec := servingRecord{
		ID:           id,
		Installation: string(tx.Bucket(metadataBucket).Get([]byte("owner"))),
		Nonce:        nonce,
		Principal:    principal,
		Domain:       r.Domain,
		RequestID:    r.RequestID,
		Name:         r.Name,
		Target:       r.Target,
		Binding:      r.Binding,
		Authority:    r.Authority,
		Ordinal:      ordinal,
		Created:      now,
		Updated:      now,
		Revision:     1,
		Desired:      serving.Present,
		History:      h,
		Operation:    &ServiceOperation{ID: op, Kind: ServiceApply, Generation: cur.Generation, Revision: cur.Revision, Started: now},
		Reserved:     charge,
	}
	record, err := encodeServingRecord(rec)
	if err != nil {
		return
	}
	witness, err := encodeServingWitness(servingWitness{Kind: servingCreate, Service: id, Digest: digest})
	if err != nil {
		return
	}
	rows := []struct{ k, v []byte }{
		{servingRecordKey(id), record},
		{servingWitnessKey(principal, r.Domain, r.RequestID), witness},
		{servingNameKey(principal, r.Name), []byte(id)},
		{servingIndexKey(ordinal), []byte(id)},
	}
	usage, err := servingUsage(b)
	if err != nil {
		return
	}
	var added uint64
	for _, w := range rows {
		added += uint64(len(w.k) + len(w.v))
	}
	if err = servingCharge(usage, 0, added); err != nil {
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
	return servingView(rec), nil
}

// servingAdmit refuses charge on binding beyond limits. Every unretired
// service counts toward the service quotas, and every unreleased one toward
// the binding's replicas, CPU and memory, whatever its phase: a deleting,
// held or unknown service keeps its charge.
func servingAdmit(b *bolt.Bucket, principal, binding string, charge ServiceReservation, limits ServiceLimits) error {
	var mine, all uint64
	var used ServiceReservation
	c := b.Cursor()
	for k, v := c.Seek([]byte("e/")); k != nil && bytes.HasPrefix(k, []byte("e/")); k, v = c.Next() {
		r, err := decodeServingRecord(v)
		if err != nil {
			return err
		}
		all++
		if r.Principal == principal {
			mine++
		}
		if r.Binding == binding {
			used.Replicas += r.Reserved.Replicas
			used.CPUMillis += r.Reserved.CPUMillis
			used.MemoryBytes += r.Reserved.MemoryBytes
		}
	}
	for _, q := range []struct {
		domain           ServiceQuota
		used, req, limit uint64
	}{
		{ServicePrincipalQuota, mine, 1, limits.PrincipalServices},
		{ServiceInstallationQuota, all, 1, min(limits.InstallationServices, maxServingRecords)},
		{ServiceReplicasQuota, used.Replicas, charge.Replicas, limits.Replicas},
		{ServiceCPUQuota, used.CPUMillis, charge.CPUMillis, limits.CPUMillis},
		{ServiceMemoryQuota, used.MemoryBytes, charge.MemoryBytes, limits.MemoryBytes},
	} {
		if q.used > q.limit || q.req > q.limit-q.used {
			return &ServiceCapacityError{Domain: q.domain, Used: q.used, Requested: q.req, Limit: q.limit}
		}
	}
	return nil
}

// servingReplay finds principal's request witness in domain d. A witness of
// the same kind and digest returns the named service or its retired view and
// found; any other witness under the key is ErrConflict.
func servingReplay(b *bolt.Bucket, principal string, d RequestDomain, request, kind, digest string) (out Service, found bool, err error) {
	v := b.Get(servingWitnessKey(principal, d, request))
	if v == nil {
		return out, false, nil
	}
	w, err := decodeServingWitness(v)
	if err != nil {
		return out, true, err
	}
	if w.Kind != kind || w.Digest != digest {
		return out, true, ErrConflict
	}
	out, err = servingLoad(b, w.Service)
	if errors.Is(err, ErrNotFound) {
		err = errors.New("service request witness names no service")
	}
	if err != nil {
		return Service{}, true, err
	}
	return out, true, nil
}

// ServiceFor reads a service visible to actor: its own or, for an admin,
// any. Foreign, retired and missing identifiers are all ErrNotFound.
func (s *Store) ServiceFor(actor auth.Actor, id string) (out Service, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		r, e := loadServingRecord(tx.Bucket(servingServicesBucket), id)
		if e != nil {
			return e
		}
		if actor.Role() != auth.Admin && r.Principal != actor.Principal() {
			return ErrNotFound
		}
		out = servingView(r)
		return nil
	})
	return
}

// ServicesFor lists the services visible to actor in acceptance order, after
// listing ordinal after. Retired services are omitted.
func (s *Store) ServicesFor(actor auth.Actor, after uint64, limit int) (ServicePage, error) {
	if err := s.authorize(actor, auth.Read); err != nil {
		return ServicePage{}, err
	}
	principal := actor.Principal()
	if actor.Role() == auth.Admin {
		principal = ""
	}
	return s.services(principal, after, limit)
}

// Service reads a service for trusted reconciliation.
func (s *Store) Service(id string) (out Service, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		r, e := loadServingRecord(tx.Bucket(servingServicesBucket), id)
		out = servingView(r)
		return e
	})
	if err != nil {
		return Service{}, err
	}
	return
}

// Services lists every unretired service in acceptance order for trusted
// reconciliation.
func (s *Store) Services(after uint64, limit int) (ServicePage, error) {
	return s.services("", after, limit)
}

func (s *Store) services(principal string, after uint64, limit int) (out ServicePage, err error) {
	if limit < 1 || limit > 200 {
		return out, errors.New("list limit outside 1..200")
	}
	if after == math.MaxUint64 {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(servingServicesBucket)
		if b == nil {
			return nil
		}
		c := b.Cursor()
		prefix := []byte("i/")
		for k, v := c.Seek(servingIndexKey(after + 1)); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(k) != 10 {
				return errors.New("invalid service index")
			}
			r, e := loadServingRecord(b, string(v))
			if e != nil {
				return e
			}
			if principal != "" && r.Principal != principal {
				continue
			}
			if len(out.Services) == limit {
				out.More = true
				break
			}
			out.Services = append(out.Services, servingView(r))
			out.After = binary.BigEndian.Uint64(k[2:])
		}
		return nil
	})
	return
}

// DeleteService latches service m.ID Absent under request m.RequestID in
// domain m.Domain, if its record revision is still m.Revision, and advances
// the revision. The domain is checked first as AcceptService checks it; an
// exact replay of the same request then returns the service's current or
// retired view before the epoch, existence, staleness and deletion checks,
// but only if the actor may see it now, and another request under the key
// is ErrConflict. A service the actor may not see is ErrNotFound; only its
// owner or an admin may delete it. A stale revision is ErrServiceStale and
// an already latched one ErrServiceDeleted. Deletion keeps the operation in
// flight, the Native, the name and the reservation: only an observed absence
// with no operation in flight releases the charge, and only RetireService
// releases the name. Every refusal writes nothing.
func (s *Store) DeleteService(actor auth.Actor, m ServiceMutation) (out Service, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if !validRetainedRequestID(m.RequestID) {
		return out, errors.New("invalid request ID")
	}
	if !validRequestDomain(m.Domain) {
		return out, ErrInvalidRequestDomain
	}
	if !ValidServiceID(m.ID) || m.Revision == 0 {
		return out, ErrNotFound
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = deleteService(tx, actor, m, s.now().UTC())
		return e
	})
	return
}

func deleteService(tx *bolt.Tx, actor auth.Actor, m ServiceMutation, now time.Time) (out Service, err error) {
	b := tx.Bucket(servingServicesBucket)
	if b == nil {
		return out, errors.New("serving metadata requires schema 13")
	}
	st, result, err := gateDomain(tx, m.Domain, now)
	if err != nil {
		return
	}
	if result != nil {
		return out, result
	}
	principal := actor.Principal()
	digest := servingDeleteDigest(m.ID, m.Revision)
	out, found, err := servingReplay(b, principal, m.Domain, m.RequestID, servingDelete, digest)
	if err != nil {
		return
	}
	if found {
		// The witness records what the actor once did, not what it may
		// see now: a replay returns the service only under the actor's
		// current owner or admin visibility, as Get would.
		if actor.Role() != auth.Admin && out.Principal != principal {
			return Service{}, ErrNotFound
		}
		return
	}
	if environmentEpochClosed(st, m.Domain) {
		return out, &ExpiryError{Reason: RequestEpochExpired}
	}
	r, err := loadServingRecord(b, m.ID)
	if err != nil {
		return
	}
	if actor.Role() != auth.Admin && r.Principal != principal {
		return out, ErrNotFound
	}
	if r.Revision != m.Revision {
		return out, ErrServiceStale
	}
	if r.Desired == serving.Absent {
		return out, ErrServiceDeleted
	}
	if r.Revision == math.MaxUint64 {
		return out, errors.New("service revision exhausted")
	}
	if n := uint64(servingCount(b, []byte("w/"))); n >= maxServingWitnesses {
		return out, &ServiceCapacityError{Domain: ServiceRequestsQuota, Used: n, Requested: 1, Limit: maxServingWitnesses}
	}
	old := b.Get(servingRecordKey(m.ID))
	oldLen := uint64(len(old))
	r.Desired = serving.Absent
	r.Revision++
	if now.After(r.Updated) {
		r.Updated = now
	}
	record, err := encodeServingRecord(r)
	if err != nil {
		return
	}
	witness, err := encodeServingWitness(servingWitness{Kind: servingDelete, Service: m.ID, Digest: digest})
	if err != nil {
		return
	}
	wk := servingWitnessKey(principal, m.Domain, m.RequestID)
	usage, err := servingUsage(b)
	if err != nil {
		return
	}
	if err = servingCharge(usage, oldLen, uint64(len(record)+len(wk)+len(witness))); err != nil {
		return
	}
	if err = b.Put(servingRecordKey(m.ID), record); err != nil {
		return
	}
	if err = b.Put(wk, witness); err != nil {
		return
	}
	return servingView(r), nil
}

// CommitService atomically replaces the runtime-derived state of service id
// with next if its record revision is still revision, and advances the
// revision. A stale revision is ErrServiceStale. Identity, intent and
// acceptance never change here.
//
// next.History may differ from the record only in the outcome of the
// current rollout, as History.Settle changes it. next.Operation may record a
// new operation only when none is in flight, and must otherwise be the one
// in flight, unchanged, or nil. Clearing an operation integrates its result:
// the commit must keep the committed Native unchanged and carry an
// observation strictly newer than the recorded one, taken after that Native
// was committed. A late Native therefore always precedes the clear, and a
// recorded absence cannot outlive an operation that may still create. A
// recorded observation is never removed.
//
// The commit that establishes absence — Desired Absent, an absent
// observation and no operation — releases the reservation. Only a nil
// error means the commit is durable.
func (s *Store) CommitService(id string, revision uint64, next ServiceCommit) (out Service, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		out, e = commitService(tx, id, revision, next, s.now().UTC())
		return e
	})
	return
}

func commitService(tx *bolt.Tx, id string, revision uint64, next ServiceCommit, now time.Time) (out Service, err error) {
	b := tx.Bucket(servingServicesBucket)
	r, err := loadServingRecord(b, id)
	if err != nil {
		return
	}
	if r.Revision != revision {
		return out, ErrServiceStale
	}
	if r.Revision == math.MaxUint64 {
		return out, errors.New("service revision exhausted")
	}
	if err = servingSettled(r.History, next.History); err != nil {
		return
	}
	if !validServiceOperation(next.Operation) {
		return out, errors.New("invalid service operation")
	}
	prev := r.Operation
	switch {
	case prev == nil && next.Operation != nil:
		cur := next.History.Current()
		op := next.Operation
		if op.Kind == ServiceApply && (r.Desired != serving.Present || op.Generation != cur.Generation || op.Revision != cur.Revision) {
			return out, errors.New("service apply names no current rollout")
		}
		if op.Kind == ServiceDelete && r.Desired != serving.Absent {
			return out, errors.New("service delete while present")
		}
		if r.released() {
			return out, errors.New("service already released")
		}
	case prev != nil && next.Operation != nil && *prev != *next.Operation:
		return out, errors.New("service operation in flight changed")
	case prev != nil && next.Operation == nil:
		if next.Native != r.Native {
			return out, errors.New("service operation cleared with uncommitted native state")
		}
		if next.Observation == nil || !next.ObservedAt.After(r.ObservedAt) {
			return out, errors.New("service operation cleared without a fresh observation")
		}
	}
	if r.Observation != nil && next.Observation == nil {
		return out, errors.New("service observation removed")
	}
	if next.Observation != nil {
		if next.ObservedAt.Before(r.ObservedAt) || next.ObservedAt.After(now.Add(time.Minute)) {
			return out, errors.New("service observation time out of order")
		}
	}
	if r.released() && (next.Observation == nil || !next.Observation.Absent) {
		return out, errors.New("released service observed present")
	}
	old := b.Get(servingRecordKey(id))
	oldLen := uint64(len(old))
	wasReleased, prevNative, prevObserved := r.released(), r.Native, r.ObservedAt
	r.Native = next.Native
	r.Operation = next.Operation
	r.History = next.History
	r.Observation = next.Observation
	r.ObservedAt = next.ObservedAt
	r.Conditions = next.Conditions
	if r.released() && !wasReleased {
		// Only an absence observed in this commit, of the committed Native,
		// releases: an absence recorded earlier may predate a create that
		// the committed Native declares.
		if r.Native != prevNative || !r.ObservedAt.After(prevObserved) {
			return out, errors.New("service released without a fresh observation of absence")
		}
	}
	if r.released() {
		r.Reserved = ServiceReservation{}
	}
	r.Revision++
	if now.After(r.Updated) {
		r.Updated = now
	}
	record, err := encodeServingRecord(r)
	if err != nil {
		return
	}
	usage, err := servingUsage(b)
	if err != nil {
		return
	}
	if err = servingCharge(usage, oldLen, uint64(len(record))); err != nil {
		return
	}
	if err = b.Put(servingRecordKey(id), record); err != nil {
		return
	}
	return servingView(r), nil
}

// servingSettled refuses a next history that is not prev with, at most, the
// current rollout's outcome advanced as History.Settle advances it.
func servingSettled(prev, next serving.History) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if len(prev.Revisions) != len(next.Revisions) || len(prev.Rollouts) != len(next.Rollouts) {
		return errors.New("service history changed")
	}
	for i, r := range prev.Revisions {
		if next.Revisions[i].Number != r.Number || next.Revisions[i].SHA256 != r.SHA256 {
			return errors.New("service history changed")
		}
	}
	last := len(prev.Rollouts) - 1
	for i, r := range prev.Rollouts {
		n := next.Rollouts[i]
		if n.Generation != r.Generation || n.Revision != r.Revision || n.Kind != r.Kind {
			return errors.New("service history changed")
		}
		if n.Outcome == r.Outcome {
			continue
		}
		ok := i == last && (r.Outcome == serving.Pending && (n.Outcome == serving.Rolling || n.Outcome == serving.Complete || n.Outcome == serving.Failed) ||
			r.Outcome == serving.Rolling && (n.Outcome == serving.Complete || n.Outcome == serving.Failed))
		if !ok {
			return errors.New("service rollout outcome cannot change so")
		}
	}
	return nil
}
