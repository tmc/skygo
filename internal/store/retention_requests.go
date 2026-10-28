package store

import (
	"encoding/binary"
	"errors"
	"math"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) RetentionInfo(actor auth.Actor) (out RetentionInfo, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		out = retentionInfo(st)
		out.PendingCleanupRecords = uint64(retentionTable(tx, cleanupOperationsBucket).Stats().KeyN)
		if actor.Role() == auth.Admin {
			u, e := usage(tx)
			if e != nil {
				return e
			}
			l := limits(st.Policy)
			for q := JobsQuota; q <= PrincipalScopesQuota; q++ {
				out.Usage = append(out.Usage, Usage{q, u[q], l[q]})
			}
		}
		return nil
	})
	return
}
func retentionInfo(st retentionState) RetentionInfo {
	o := RetentionInfo{Policy: st.Policy, Enabled: st.Enabled, Domain: RequestDomain{st.Lineage, st.Current}, LegacyReplayOpen: st.LegacyReplayOpen, LegacyCutoff: st.LegacyCutoff}
	for _, e := range st.Epochs {
		if e.Number == st.Current {
			o.AcceptUntil = e.AcceptUntil
			o.RetryUntil = e.RetryUntil
			o.AcceptClosed = e.AcceptClosed
		}
	}
	return o
}
func advancePhases(st *retentionState, now time.Time) bool {
	changed := false
	if st.LegacyReplayOpen && !now.Before(st.LegacyCutoff) {
		st.LegacyReplayOpen = false
		changed = true
	}
	for i := range st.Epochs {
		e := &st.Epochs[i]
		if !e.AcceptClosed && !now.Before(e.AcceptUntil) {
			e.AcceptClosed = true
			changed = true
		}
		if !now.Before(e.RetryUntil) && e.Number > st.RetiredThrough {
			st.RetiredThrough = e.Number
			changed = true
		}
	}
	return changed
}
func gateDomain(tx *bolt.Tx, d RequestDomain, now time.Time) (retentionState, error, error) {
	st, e := loadRetention(tx)
	if e != nil {
		return st, nil, e
	}
	changed, result := domainResult(&st, d, now)
	if changed {
		if e = putJSON(retentionRoot(tx), retentionStateKey, st); e != nil {
			return st, nil, e
		}
	}
	return st, result, nil
}

// domainResult is gateDomain without the write: it advances st's phases to
// now, reporting whether they changed, and returns the refusal of d, if any.
// A disabled store or another lineage is refused before any phase advances.
func domainResult(st *retentionState, d RequestDomain, now time.Time) (bool, error) {
	if !st.Enabled {
		return false, ErrRetentionDisabled
	}
	if d.Lineage != st.Lineage {
		return false, &ExpiryError{Reason: RequestLineageChanged}
	}
	changed := advancePhases(st, now)
	if d.Epoch == 0 {
		if !st.LegacyReplayOpen {
			return changed, &ExpiryError{Reason: RequestEpochExpired}
		}
		return changed, nil
	}
	if d.Epoch <= st.RetiredThrough {
		return changed, &ExpiryError{Reason: RequestEpochExpired}
	}
	for _, ep := range st.Epochs {
		if ep.Number == d.Epoch {
			return changed, nil
		}
	}
	return changed, ErrUnknownRequestEpoch
}
func lookupRecord(tx *bolt.Tx, actor auth.Actor, d RequestDomain, id string, t backend.Task) (j backend.Job, found bool, result error) {
	var r requestRecord
	e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(actor.Principal(), d, id), &r)
	if errors.Is(e, ErrNotFound) {
		return
	}
	if e != nil {
		return j, false, e
	}
	found = true
	if r.Principal != actor.Principal() || r.Domain != d || r.requestID() != id {
		return j, true, errors.New("request identity hash collision")
	}
	if r.WorkflowID != "" {
		return j, true, ErrConflict
	}
	if workerGroupJob(tx, r.JobID) {
		return j, true, ErrConflict
	}
	digest, e := taskDigest(t)
	if e != nil {
		return j, true, e
	}
	if digest != r.TaskDigest {
		return j, true, ErrConflict
	}
	if r.Expired {
		return j, true, &ExpiryError{Reason: ResultExpired, JobID: r.JobID}
	}
	j, e = get(tx, r.JobID)
	if e != nil {
		return j, true, e
	}
	have, e := backend.TaskBytes(j.Task)
	if e != nil {
		return j, true, e
	}
	want, e := backend.TaskBytes(t)
	if e != nil {
		return j, true, e
	}
	if string(have) != string(want) {
		return backend.Job{}, true, ErrConflict
	}
	return j, true, nil
}
func legacyLookup(tx *bolt.Tx, actor auth.Actor, id string, t backend.Task, now time.Time) (j backend.Job, found bool, result, errorIO error) {
	st, e := loadRetention(tx)
	if e != nil {
		return j, false, nil, e
	}
	_, result, errorIO = gateDomain(tx, RequestDomain{st.Lineage, 0}, now)
	if result != nil || errorIO != nil {
		return
	}
	j, found, result = lookupRecord(tx, actor, RequestDomain{st.Lineage, 0}, id, t)
	if !found && result == nil {
		result = &ExpiryError{Reason: RequestDomainRequired}
	}
	return
}
func (s *Store) LookupRetained(actor auth.Actor, d RequestDomain, id string, t backend.Task) (j backend.Job, found bool, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if !validRetainedRequestID(id) {
		return j, false, errors.New("invalid request ID")
	}
	if !validRequestDomain(d) {
		return j, false, ErrInvalidRequestDomain
	}
	t.Retry, err = backend.NormalizeRetry(t.Retry)
	if err != nil {
		return
	}
	var result error
	err = s.db.Update(func(tx *bolt.Tx) error {
		st, r, e := gateDomain(tx, d, s.now())
		if e != nil {
			return e
		}
		if r != nil {
			result = r
			return nil
		}
		j, found, result = lookupRecord(tx, actor, d, id, t)
		if !found && result == nil {
			for _, ep := range st.Epochs {
				if ep.Number == d.Epoch && ep.AcceptClosed {
					result = &ExpiryError{Reason: RequestEpochExpired}
				}
			}
		}
		return nil
	})
	if err == nil {
		err = result
	}
	return
}
func trackLegacyAccepted(tx *bolt.Tx, j backend.Job, principal, id string) error {
	digest, e := taskDigest(j.Task)
	if e != nil {
		return e
	}
	r := requestRecord{Principal: principal, LegacyRequestID: []byte(id), JobID: j.ID, TaskDigest: digest, LegacyBytes: uint64(len(id) + len(j.ID))}
	r.LegacyBytes, e = legacyBindingBytes(tx, r)
	if e != nil {
		return e
	}
	return trackJob(tx, j, retainedJob{Principal: principal, LegacyRequestID: []byte(id), Legacy: true}, r)
}
func (s *Store) SubmitRetained(actor auth.Actor, d RequestDomain, id string, t backend.Task, target, binding string, storage backend.OutputStorage, reserve uint64) (j backend.Job, err error) {
	return s.submitRetained(actor, d, id, t, target, binding, storage, reserve, submissionOrdinary, nil)
}

func (s *Store) submitRetained(actor auth.Actor, d RequestDomain, id string, t backend.Task, target, binding string, storage backend.OutputStorage, reserve uint64, kind submissionKind, remote *remoteSubmission) (j backend.Job, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if err = validateSubmitWorkspace(t, kind); err != nil {
		return
	}
	if !validRetainedRequestID(id) {
		return j, errors.New("invalid request ID")
	}
	if !validRequestDomain(d) {
		return j, ErrInvalidRequestDomain
	}
	t.Retry, err = backend.NormalizeRetry(t.Retry)
	if err != nil {
		return
	}
	existing, found, e := s.LookupRetained(actor, d, id, t)
	if e != nil || found {
		return existing, e
	}
	identity, e := newID()
	if e != nil {
		return j, e
	}
	var result error
	err = s.db.Update(func(tx *bolt.Tx) error {
		var core *artifactCoreLedger
		if kind != submissionOrdinary {
			core = artifactAcceptLedger(tx, identity, actor.Principal(), id, d, false)
		}
		st, r, e := gateDomain(tx, d, s.now())
		if e != nil {
			return e
		}
		if r != nil {
			result = r
			return nil
		}
		var exists bool
		j, exists, result = lookupRecord(tx, actor, d, id, t)
		if result != nil || exists {
			return nil
		}
		if d.Epoch == 0 {
			return errors.New("zero request epoch cannot accept")
		}
		for _, ep := range st.Epochs {
			if ep.Number == d.Epoch && ep.AcceptClosed {
				result = &ExpiryError{Reason: RequestEpochExpired}
				return nil
			}
		}
		accept := func(art *artifactState) error {
			if kind == submissionRemote {
				if e := remote.validate(actor.Principal(), t); e != nil {
					return e
				}
			}
			if art != nil && t.Workdir != nil {
				ref, e := artifactTaskRef(t)
				if e != nil {
					return e
				}
				if e := artifactGranted(art, actor.Principal(), ref, s.now()); e != nil {
					return e
				}
			}
			if target == "" || len(target) > 128 || !validBinding(binding) {
				return errors.New("invalid retained target")
			}
			if storage == backend.LocalOutput && !st.Policy.LocalOutputEnabled {
				return errors.New("local output retention is disabled")
			}
			if storage != backend.LocalOutput && storage != backend.RemoteOutput || storage == backend.RemoteOutput && reserve != 0 || storage == backend.LocalOutput && reserve != uint64(backend.MaxLogBytes)+4*st.Policy.OutputUnit {
				return errors.New("invalid output placement charge")
			}
			data, e := backend.TaskBytes(t)
			if e != nil {
				return e
			}
			if len(data) > 256<<10 {
				return errors.New("task exceeds limit")
			}
			now := s.now()
			if remote != nil {
				now = remote.now
			}
			if j, e = acceptJobRow(tx, identity, actor.Principal(), t, target, binding, now); e != nil {
				return e
			}
			digest, e := taskDigest(t)
			if e != nil {
				return e
			}
			if e := trackJob(tx, j, retainedJob{Principal: actor.Principal(), Domain: d, RequestID: id, Storage: storage, OutputReserve: reserve}, requestRecord{Principal: actor.Principal(), Domain: d, RequestID: id, JobID: j.ID, TaskDigest: digest}); e != nil {
				return e
			}
			if art != nil && t.Workdir != nil {
				ref, e := artifactTaskRef(t)
				if e != nil {
					return e
				}
				if e := artifactAcceptInput(tx, art, actor.Principal(), j.ID, ref, now); e != nil {
					return e
				}
			}
			if kind == submissionRemote {
				return remoteAccept(art, j.ID, actor.Principal(), remote)
			}
			return nil
		}
		if kind != submissionOrdinary {
			_, e := artifactCoreChange(tx, core, maxChangeBytes, 100, accept)
			return e
		}
		return accept(nil)
	})
	if err == nil {
		err = result
	}
	return
}

// acceptJobRow writes a new queued job's canonical, sequence and scope rows
// and charges its task bytes to the core counter.
func acceptJobRow(tx *bolt.Tx, identity, principal string, t backend.Task, target, binding string, now time.Time) (j backend.Job, err error) {
	data, e := backend.TaskBytes(t)
	if e != nil {
		return j, e
	}
	jobs, seq := tx.Bucket(jobsBucket), tx.Bucket(sequenceBucket)
	if jobs.Get([]byte(identity)) != nil || tx.Bucket(attemptJobsBucket).Get([]byte(identity)) != nil {
		return j, errors.New("random job identity collision")
	}
	if seq.Sequence() == math.MaxUint64 {
		return j, errors.New("acceptance sequence exhausted")
	}
	n, e := seq.NextSequence()
	if e != nil {
		return j, e
	}
	j = backend.Job{ID: identity, Task: t, Target: target, TargetBinding: binding, Status: backend.Status{State: backend.Queued}, CreatedAt: now, UpdatedAt: now, Sequence: n}
	if e = acceptAttempts(tx, &j); e != nil {
		return j, e
	}
	if e = putJSON(jobs, []byte(j.ID), j); e != nil {
		return j, e
	}
	if e = seq.Put(sequenceKey(n), []byte(j.ID)); e != nil {
		return j, e
	}
	if isScoped(tx) {
		if e = tx.Bucket(jobPrincipalsBucket).Put([]byte(j.ID), []byte(principal)); e != nil {
			return j, e
		}
		index, e := tx.Bucket(principalIndexBucket).CreateBucketIfNotExists([]byte(principal))
		if e != nil {
			return j, e
		}
		if index.Sequence() == math.MaxUint64 {
			return j, errors.New("scope sequence exhausted")
		}
		ord, e := index.NextSequence()
		if e != nil {
			return j, e
		}
		if e = index.Put(sequenceKey(ord), []byte(j.ID)); e != nil {
			return j, e
		}
	}
	m := tx.Bucket(metadataBucket)
	used := binary.BigEndian.Uint64(m.Get(taskBytesKey))
	next, e := addChecked(used, uint64(len(data)))
	if e != nil {
		return j, e
	}
	return j, m.Put(taskBytesKey, sequenceKey(next))
}

// MaintainRetention advances irreversible phases and performs one bounded
// request/job retirement plan: at most 100 inspected subjects and 1 MiB of
// complete changed bytes. It never changes canonical jobs or execution facts.
func (s *Store) MaintainRetention(now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error { return maintainRetention(tx, now) })
}

func validRetainedRequestID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
