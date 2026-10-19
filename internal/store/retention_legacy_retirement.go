package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// maintainRetention uses one write plan for phases, request retirement and the
// stable acceptance-index pass. Requests get at most half the bytes and fifty
// subject units; a paired live-job inspection costs a second subject unit.
func maintainRetention(tx *bolt.Tx, now time.Time) error {
	st, err := loadRetention(tx)
	if err != nil || !st.Enabled {
		return err
	}
	if !validLegacyJobsCursor(st, tx.Bucket(sequenceBucket).Sequence()) || st.RequestAfter != "" && !validRequestCursor(st.RequestAfter) {
		return errors.New("invalid retirement cursor")
	}
	advancePhases(&st, now)
	live := st.Epochs[:0]
	for _, ep := range st.Epochs {
		if ep.Number > st.RetiredThrough {
			live = append(live, ep)
		}
	}
	st.Epochs = live
	need := true
	for _, ep := range st.Epochs {
		if ep.Number == st.Current && !ep.AcceptClosed {
			need = false
		}
	}
	if need {
		if st.Current == ^uint64(0) {
			return errors.New("request epoch exhausted")
		}
		st.Current++
		st.Epochs = append(st.Epochs, requestEpoch{Number: st.Current, AcceptUntil: now.Add(st.Policy.Window), RetryUntil: now.Add(2 * st.Policy.Window)})
	}
	if len(st.Epochs) > 2 {
		return errors.New("too many live request epochs")
	}
	m, err := newRetentionMutation(tx)
	if err != nil {
		return err
	}
	if err = m.rootPut(retentionStateKey, st); err != nil {
		return err
	}
	m, inspected, err := retireRequestLane(m, &st)
	if err != nil {
		return err
	}
	m, _, err = retireLegacyJobLane(m, &st, 100-inspected)
	if err != nil {
		return err
	}
	if err = m.rootPut(retentionStateKey, st); err != nil {
		return err
	}
	return m.commit()
}

func retireRequestLane(m *retentionMutation, st *retentionState) (*retentionMutation, int, error) {
	c := retentionTable(m.tx, retainedRequestsBucket).Cursor()
	k, v := c.Seek(append([]byte(st.RequestAfter), 0))
	if k == nil {
		k, v = c.First()
	}
	inspected := 0
	for k != nil && inspected < 50 {
		inspected++ // The request visit counts even if a paired read cannot fit.
		var r requestRecord
		if err := json.Unmarshal(v, &r); err != nil {
			return nil, inspected, err
		}
		if !validRequestIdentity(r.Domain, r.RequestID, r.LegacyRequestID) || !validPrincipal([]byte(r.Principal)) || !validAttemptID(r.JobID) || !bytes.Equal(k, requestKey(r.Principal, r.Domain, r.requestID())) {
			return nil, inspected, errors.New("invalid retirement request")
		}
		closed := r.Domain.Lineage != st.Lineage || r.Domain.Epoch == 0 && !st.LegacyReplayOpen || r.Domain.Epoch > 0 && r.Domain.Epoch <= st.RetiredThrough
		// Reserve a paired read even when the canonical row is already absent.
		if closed && r.Domain.Epoch == 0 {
			if inspected == 50 {
				break
			}
			inspected++
		}
		trial := m.fork()
		if closed {
			if r.Domain.Epoch == 0 {
				if err := trial.retireLegacyRequest(*st, r); err != nil {
					return nil, inspected, err
				}
			}
			if err := trial.dropLegacy(&r); err != nil {
				return nil, inspected, err
			}
			if err := trial.drop(resultWitnessBucket, []byte(r.JobID)); err != nil {
				return nil, inspected, err
			}
			if err := trial.drop(retainedRequestsBucket, k); err != nil {
				return nil, inspected, err
			}
		}
		next := *st
		next.RequestAfter = string(k)
		if err := trial.rootPut(retentionStateKey, next); err != nil {
			return nil, inspected, err
		}
		n, err := trial.changedBytes()
		if err != nil {
			return nil, inspected, err
		}
		if n > maxChangeBytes/2 {
			break
		}
		m, *st = trial, next
		k, v = c.Next()
	}
	if k == nil {
		st.RequestAfter = ""
	}
	if err := m.rootPut(retentionStateKey, *st); err != nil {
		return nil, inspected, err
	}
	return m, inspected, nil
}

func (m *retentionMutation) load(name, key []byte, out any) error {
	data := m.get(retentionKey{true, string(name), string(key)})
	if data == nil {
		return ErrNotFound
	}
	return json.Unmarshal(data, out)
}

func (m *retentionMutation) retireLegacyRequest(st retentionState, r requestRecord) error {
	if !legacyClosed(st, r.Domain) {
		return errors.New("legacy request is not irreversibly closed")
	}
	n, err := legacyBindingBytes(m.tx, r)
	if err != nil || n != r.LegacyBytes {
		return errors.New("legacy retirement binding charge mismatch")
	}
	j, err := get(m.tx, r.JobID)
	if errors.Is(err, ErrNotFound) && r.Expired {
		return nil
	}
	if err != nil {
		return err
	}
	if j.ID != r.JobID {
		return errors.New("legacy retirement canonical identity mismatch")
	}
	var meta retainedJob
	if err := m.load(retainedJobsBucket, []byte(j.ID), &meta); err != nil {
		return err
	}
	if err := checkRetirementJob(m.tx, st, j, meta); err != nil {
		return err
	}
	d, err := taskDigest(j.Task)
	if err != nil || d != r.TaskDigest || r.Expired || r.LegacyBytes == 0 || !validJobRequestIdentity(st, meta) || meta.LegacyRequestRetired || meta.Principal != r.Principal || meta.Domain != r.Domain || meta.RequestID != "" || !bytes.Equal(meta.LegacyRequestID, r.LegacyRequestID) {
		return errors.New("legacy retirement job identity mismatch")
	}
	meta.LegacyRequestID = nil
	meta.LegacyRequestRetired = true
	return m.put(retainedJobsBucket, []byte(j.ID), meta)
}

func retireLegacyJobLane(m *retentionMutation, st *retentionState, budget int) (*retentionMutation, int, error) {
	b := m.tx.Bucket(sequenceBucket)
	if st.LegacyJobsMax == 0 {
		st.LegacyJobsMax = b.Sequence()
	}
	inspected := 0
	if st.LegacyJobsMax == 0 || st.LegacyJobsAfter == st.LegacyJobsMax {
		st.LegacyJobsAfter, st.LegacyJobsMax = 0, 0
		return m, inspected, m.rootPut(retentionStateKey, *st)
	}
	c := b.Cursor()
	k, id := c.Seek(sequenceKey(st.LegacyJobsAfter + 1))
	for k != nil && inspected < budget {
		if len(k) != 8 {
			return nil, inspected, errors.New("invalid legacy retirement index")
		}
		n := binary.BigEndian.Uint64(k)
		if n > st.LegacyJobsMax {
			break
		}
		inspected++
		j, err := get(m.tx, string(id))
		if err != nil {
			return nil, inspected, err
		}
		if j.ID != string(id) || j.Sequence != n {
			return nil, inspected, errors.New("legacy retirement sequence mismatch")
		}
		trial := m.fork()
		if err := trial.retireLegacyJob(*st, j); err != nil {
			return nil, inspected, err
		}
		next := *st
		next.LegacyJobsAfter = n
		if err := trial.rootPut(retentionStateKey, next); err != nil {
			return nil, inspected, err
		}
		size, err := trial.changedBytes()
		if err != nil {
			return nil, inspected, err
		}
		if size > maxChangeBytes {
			break
		}
		m, *st = trial, next
		k, id = c.Next()
	}
	if k == nil || len(k) == 8 && binary.BigEndian.Uint64(k) > st.LegacyJobsMax {
		st.LegacyJobsAfter, st.LegacyJobsMax = 0, 0
	}
	if err := m.rootPut(retentionStateKey, *st); err != nil {
		return nil, inspected, err
	}
	return m, inspected, nil
}

func (m *retentionMutation) retireLegacyJob(st retentionState, j backend.Job) error {
	var meta retainedJob
	if err := m.load(retainedJobsBucket, []byte(j.ID), &meta); err != nil {
		return err
	}
	if err := checkRetirementJob(m.tx, st, j, meta); err != nil {
		return err
	}
	if !meta.Legacy || meta.LegacyRequestRetired || !legacyClosed(st, meta.Domain) {
		return nil
	}
	key := requestKey(meta.Principal, meta.Domain, meta.requestID())
	var r requestRecord
	if err := m.load(retainedRequestsBucket, key, &r); err == nil {
		d, e := taskDigest(j.Task)
		if e != nil || r.Expired || r.JobID != j.ID || r.Principal != meta.Principal || r.Domain != meta.Domain || r.RequestID != "" || !bytes.Equal(r.LegacyRequestID, meta.LegacyRequestID) || r.TaskDigest != d {
			return errors.New("conflicting legacy retirement request")
		}
		n, e := legacyBindingBytes(m.tx, r)
		if e != nil || n != r.LegacyBytes {
			return errors.New("legacy retirement binding charge mismatch")
		}
		return nil // Coupled retirement owns every extant matching request.
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	name := requestsBucket
	if isScoped(m.tx) {
		name = []byte(string(principalRequestsBucket) + "/" + meta.Principal)
	}
	if m.coreGet(name, meta.LegacyRequestID) != nil || isScoped(m.tx) && meta.Principal == "legacy" && m.coreGet(requestsBucket, meta.LegacyRequestID) != nil {
		return errors.New("retired legacy request still has raw binding")
	}
	meta.LegacyRequestID = nil
	meta.LegacyRequestRetired = true
	return m.put(retainedJobsBucket, []byte(j.ID), meta)
}

func checkRetirementJob(tx *bolt.Tx, st retentionState, j backend.Job, meta retainedJob) error {
	if !validAttemptID(j.ID) || !validPrincipal([]byte(meta.Principal)) || !validJobRequestIdentity(st, meta) {
		return errors.New("invalid legacy retirement metadata")
	}
	if isScoped(tx) {
		index := tx.Bucket(principalIndexBucket).Bucket([]byte(meta.Principal))
		if string(tx.Bucket(jobPrincipalsBucket).Get([]byte(j.ID))) != meta.Principal || index == nil || string(index.Get(sequenceKey(meta.Ordinal))) != j.ID {
			return errors.New("legacy retirement owner mismatch")
		}
	}
	overhead, err := jobOverhead(j)
	if err != nil || overhead != meta.Overhead {
		return errors.New("legacy retirement overhead mismatch")
	}
	if meta.Storage > backend.RemoteOutput || meta.Storage == backend.RemoteOutput && meta.OutputReserve != 0 ||
		st.Enabled && meta.Storage == backend.LocalOutput && (!st.Policy.LocalOutputEnabled || meta.OutputReserve != uint64(backend.MaxLogBytes)+4*st.Policy.OutputUnit) {
		return errors.New("legacy retirement output placement mismatch")
	}
	return nil
}
