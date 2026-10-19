package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) NextCleanup(after uint64) (out CleanupRecord, found bool, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := retentionTable(tx, cleanupOperationIndexBucket)
		c := b.Cursor()
		k, v := c.Seek(sequenceKey(after + 1))
		if k == nil {
			k, v = c.First()
		}
		if k == nil {
			return nil
		}
		var id string
		if e := json.Unmarshal(v, &id); e != nil {
			return e
		}
		var op cleanupOperation
		if e := loadJSON(retentionTable(tx, cleanupOperationsBucket), []byte(id), &op); e != nil {
			return e
		}
		var h cleanupHeader
		if e := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(op.HeaderID), &h); e != nil {
			return e
		}
		if e := validateCleanupOperation(tx, h, op); e != nil {
			return e
		}
		out = CleanupRecord{ID: op.ID, HeaderID: h.ID, Target: h.Target, Binding: h.Binding, Sequence: op.Sequence, Operation: op.Operation, Digest: cleanupDigest(op), Stopped: op.Stopped}
		if op.Operation == OutputCleanup {
			out.Output = cleanupOutputReference(tx, h, op)
		} else {
			out.Execution = backend.Reference{ID: op.AttemptID, Task: *h.Task, Binding: h.Binding, Instance: op.Proof.Instance}
			out.Proof = *op.Proof
			out.TaskDigest = h.TaskDigest
		}
		found = true
		return nil
	})
	return
}
func validateCleanupOperation(tx *bolt.Tx, h cleanupHeader, op cleanupOperation) error {
	if !validAttemptID(op.ID) || !validAttemptID(h.ID) || op.HeaderID != h.ID || op.Sequence == 0 || op.AttemptID == "" || op.Sequence > retentionTable(tx, cleanupOperationIndexBucket).Sequence() {
		return errors.New("invalid cleanup identity")
	}
	member := false
	for _, id := range h.ExecutionIDs {
		if id == op.AttemptID {
			member = true
		}
	}
	if !member {
		return errors.New("cleanup attempt is not in header")
	}
	switch op.Operation {
	case OutputCleanup:
		if op.Proof != nil || op.Stopped || op.Generation != 1 || op.ClosureID == "" || !validInstance(op.ClosureID) {
			return errors.New("invalid output-only cleanup")
		}
		var r outputReservation
		if e := loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(op.AttemptID), &r); e != nil {
			return e
		}
		if !r.Expired || r.JobID != h.JobID || r.Binding != h.Binding || r.Generation != op.Generation {
			return errors.New("output cleanup reservation mismatch")
		}
		var closure outputClosure
		if e := loadJSON(retentionTable(tx, retainedClosuresBucket), []byte(op.AttemptID), &closure); e != nil {
			return e
		}
		if closure.Reference != cleanupOutputReference(tx, h, op) {
			return errors.New("output cleanup closure mismatch")
		}
	case NativeCleanup:
		if op.Proof == nil || !op.Stopped || op.Generation != 0 || op.ClosureID != "" || op.PayloadReleased || op.Proof.Instance == "" || !validInstance(op.Proof.Instance) || h.NeedsManual || h.Task == nil {
			return errors.New("native cleanup lacks positive proof and pinned identity")
		}
		if e := validStatus(*op.Proof); e != nil {
			return e
		}
		if op.Proof.Termination != backend.StoppedWithExitCode || op.Proof.ExitCode == nil || op.Proof.Reason != "" || (op.Proof.State != backend.Failed && op.Proof.State != backend.Succeeded && op.Proof.State != backend.Canceled) {
			return errors.New("invalid native stopped receipt")
		}
		d, e := taskDigest(*h.Task)
		if e != nil || d != h.TaskDigest {
			return errors.New("cleanup task identity mismatch")
		}
	default:
		return errors.New("invalid cleanup operation")
	}
	return nil
}
func (s *Store) FinishCleanup(id string, operation CleanupOperation, digest [32]byte, now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var op cleanupOperation
		e := loadJSON(retentionTable(tx, cleanupOperationsBucket), []byte(id), &op)
		if errors.Is(e, ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		if op.Operation != operation || cleanupDigest(op) != digest {
			return errors.New("cleanup result identity mismatch")
		}
		var h cleanupHeader
		if e = loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(op.HeaderID), &h); e != nil {
			return e
		}
		if e = validateCleanupOperation(tx, h, op); e != nil {
			return e
		}
		m, e := newRetentionMutation(tx)
		if e != nil {
			return e
		}
		if op.Operation == OutputCleanup {
			var r outputReservation
			if e = loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(op.AttemptID), &r); e != nil {
				return e
			}
			if !r.Expired {
				return errors.New("output cleanup is not marked")
			}
			for _, x := range []struct {
				q QuotaDomain
				n uint64
			}{{OutputBytesQuota, r.Bytes}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
				if e = m.adjust(x.q, 0, x.n); e != nil {
					return e
				}
			}
			if tx.Bucket(jobsBucket).Get([]byte(h.JobID)) != nil {
				if e = m.put(expiredOutputBucket, []byte(op.AttemptID), true); e != nil {
					return e
				}
			}
			if e = m.drop(retainedOutputsBucket, []byte(op.AttemptID)); e != nil {
				return e
			}
			if e = m.drop(retainedClosuresBucket, []byte(op.AttemptID)); e != nil {
				return e
			}
		}
		if e = m.drop(cleanupOperationIndexBucket, sequenceKey(op.Sequence)); e != nil {
			return e
		}
		if e = m.drop(cleanupOperationsBucket, []byte(id)); e != nil {
			return e
		}
		if h.Remaining == 0 {
			return errors.New("cleanup count underflow")
		}
		if op.Operation == OutputCleanup {
			if h.OutputRemaining == 0 {
				return errors.New("output cleanup count underflow")
			}
			h.OutputRemaining--
		} else {
			if h.NativeRemaining == 0 {
				return errors.New("native cleanup count underflow")
			}
			h.NativeRemaining--
		}
		h.Remaining--
		if h.Remaining == 0 && !h.NeedsManual {
			if e = m.removeHeader(h); e != nil {
				return e
			}
			return m.commit()
		}
		if e = m.put(cleanupHeadersBucket, []byte(h.ID), h); e != nil {
			return e
		}
		return m.commit()
	})
}
func (s *Store) FailCleanup(id string, digest [32]byte, reason string, now time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var op cleanupOperation
		e := loadJSON(retentionTable(tx, cleanupOperationsBucket), []byte(id), &op)
		if errors.Is(e, ErrNotFound) {
			return nil
		}
		if e != nil {
			return e
		}
		if cleanupDigest(op) != digest {
			return errors.New("cleanup failure identity mismatch")
		}
		op.LastError = "cleanup_failed"
		op.UpdatedAt = now.UTC()
		return putRetentionRecord(tx, cleanupOperationsBucket, []byte(id), op)
	})
}
func reserveOutput(tx *bolt.Tx, j backend.Job, reserve uint64) error {
	st, e := loadRetention(tx)
	if e != nil {
		return e
	}
	if !st.Enabled {
		return nil
	}
	var m retainedJob
	if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
		return e
	}
	if m.Storage == backend.LocalOutput && !st.Policy.LocalOutputEnabled {
		return errors.New("local output retention is disabled")
	}
	if m.Storage == backend.UnspecifiedOutput {
		return errors.New("output storage has no validated placement")
	}
	if reserve != m.OutputReserve {
		return errors.New("output reservation changed")
	}
	m.Started = true
	if m.Storage == backend.LocalOutput && m.OutputStart == 0 {
		m.OutputStart = uint32(len(j.Attempts))
		if m.OutputStart == 0 {
			m.OutputStart = 1
		}
	}
	if e = putRetentionRecord(tx, retainedJobsBucket, []byte(j.ID), m); e != nil {
		return e
	}
	if m.Storage == backend.RemoteOutput {
		if reserve != 0 {
			return errors.New("remote output has local reservation")
		}
		return nil
	}
	b := retentionTable(tx, retainedOutputsBucket)
	id := j.CurrentAttemptID()
	if b.Get([]byte(id)) != nil {
		return errors.New("output already reserved")
	}
	for _, x := range []struct {
		q QuotaDomain
		n uint64
	}{{OutputBytesQuota, reserve}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
		if e = adjust(tx, x.q, x.n, 0); e != nil {
			return e
		}
	}
	return putRetentionRecord(tx, retainedOutputsBucket, []byte(id), outputReservation{JobID: j.ID, AttemptID: id, Binding: j.TargetBinding, Generation: 1, Bytes: reserve})
}
func (s *Store) CheckLogDomain(actor auth.Actor, jobID, attemptID string) error {
	if e := s.authorize(actor, auth.Read); e != nil {
		return e
	}
	return s.db.View(func(tx *bolt.Tx) error {
		j, e := get(tx, jobID)
		if e == nil {
			if !visible(tx, actor, jobID) {
				return ErrNotFound
			}
			member := false
			for _, id := range executionIDs(j) {
				if id == attemptID {
					member = true
				}
			}
			if !member {
				return errors.New("attempt is not a job member")
			}
		} else if errors.Is(e, ErrNotFound) {
			var witness string
			if loadJSON(retentionTable(tx, resultWitnessBucket), []byte(jobID), &witness) == nil && (actor.Role() == auth.Admin || witness == actor.Principal()) {
				return &ExpiryError{Reason: ResultExpired, JobID: jobID}
			}
			var h cleanupHeader
			if loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(jobID), &h) == nil && (actor.Role() == auth.Admin || h.Principal == actor.Principal()) {
				return &ExpiryError{Reason: ResultExpired, JobID: jobID}
			}
			return ErrNotFound
		} else {
			return e
		}
		var r outputReservation
		e = loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(attemptID), &r)
		if e == nil && r.Expired {
			return &ExpiryError{Reason: LogsExpired}
		}
		if e != nil && !errors.Is(e, ErrNotFound) {
			return e
		}
		// A retained job needs a persistent expired marker after physical output GC.
		if marker := retentionTable(tx, expiredOutputBucket).Get([]byte(attemptID)); marker != nil {
			return &ExpiryError{Reason: LogsExpired}
		}
		return nil
	})
}

var expiredOutputBucket = []byte("expired_output")
var resultWitnessBucket = []byte("result_witnesses")

func indexID(tx *bolt.Tx, table []byte, n uint64) (string, error) {
	var id string
	e := loadJSON(retentionTable(tx, table), sequenceKey(n), &id)
	return id, e
}
func scopePosition(tx *bolt.Tx, principal, id string) (uint64, error) {
	b := tx.Bucket(principalIndexBucket).Bucket([]byte(principal))
	if b == nil {
		return 0, ErrNotFound
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if string(v) == id {
			return binary.BigEndian.Uint64(k), nil
		}
	}
	return 0, ErrNotFound
}

// ListOutputReferences is a bounded trusted startup seam. It enumerates ALL
// durable local reservations; an empty ClosureID deliberately proves no closure.
// Request handlers must not expose this internal constructor authority as an API.
func (s *Store) ListOutputReferences(after string, limit int) (out []backend.OutputReference, next string, err error) {
	if limit < 1 || limit > 100 || after != "" && !validAttemptID(after) {
		return nil, "", errors.New("invalid output reference page")
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := retentionTable(tx, retainedOutputsBucket)
		c := b.Cursor()
		for k, v := c.Seek(append([]byte(after), 0)); k != nil; k, v = c.Next() {
			if len(out) == limit {
				next = out[len(out)-1].AttemptID
				break
			}
			var reservation outputReservation
			if e := json.Unmarshal(v, &reservation); e != nil {
				return e
			}
			r := backend.OutputReference{Owner: s.owner, JobID: reservation.JobID, AttemptID: reservation.AttemptID, Binding: reservation.Binding, Generation: reservation.Generation}
			var proof outputClosure
			e := loadJSON(retentionTable(tx, retainedClosuresBucket), k, &proof)
			if e == nil {
				r = proof.Reference
				if r.Owner != s.owner || r.AttemptID != string(k) || r.JobID != reservation.JobID || r.Binding != reservation.Binding || r.Generation != reservation.Generation || r.ClosureID == "" || !validInstance(r.ClosureID) {
					return errors.New("invalid persisted output closure")
				}
			} else if !errors.Is(e, ErrNotFound) {
				return e
			}
			out = append(out, r)
		}
		return nil
	})
	return
}

// OutputReference returns exact committed reservation identity for a trusted
// coordinator to bind before local Start. No directory presence grants authority.
func (s *Store) OutputReference(jobID, attemptID string) (out backend.OutputReference, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		var r outputReservation
		if e := loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(attemptID), &r); e != nil {
			return e
		}
		if r.JobID != jobID || r.Expired {
			return ErrNotFound
		}
		out = backend.OutputReference{Owner: s.owner, JobID: r.JobID, AttemptID: r.AttemptID, Binding: r.Binding, Generation: r.Generation}
		return nil
	})
	return
}

func cleanupOutputReference(tx *bolt.Tx, h cleanupHeader, op cleanupOperation) backend.OutputReference {
	return backend.OutputReference{Owner: string(tx.Bucket(metadataBucket).Get([]byte("owner"))), JobID: h.JobID, AttemptID: op.AttemptID, Binding: h.Binding, Generation: op.Generation, ClosureID: op.ClosureID, PayloadReleased: op.PayloadReleased}
}
