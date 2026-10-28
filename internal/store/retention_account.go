package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

type retentionUsage [13]uint64

func usage(tx *bolt.Tx) (retentionUsage, error) {
	var u retentionUsage
	e := loadJSON(retentionRoot(tx), retentionUsageKey, &u)
	return u, e
}
func limits(p RetentionPolicy) retentionUsage {
	return retentionUsage{0, p.Jobs, p.TaskBytes, p.RetryBytes, p.MetadataBytes, p.RequestRecords, p.RequestBytes, p.CleanupRecords, p.CleanupBytes, p.OutputBytes, p.OutputDomains, p.OutputFiles, p.PrincipalScopes}
}
func addChecked(a, b uint64) (uint64, error) {
	if b > math.MaxUint64-a {
		return 0, errors.New("retention counter overflow")
	}
	return a + b, nil
}
func adjust(tx *bolt.Tx, q QuotaDomain, add, remove uint64) error {
	u, e := usage(tx)
	if e != nil {
		return e
	}
	if q < JobsQuota || q > PrincipalScopesQuota {
		return errors.New("invalid retention quota")
	}
	if remove > u[q] {
		return errors.New("retention counter underflow")
	}
	old := u[q]
	n, e := addChecked(old-remove, add)
	if e != nil {
		return e
	}
	s, e := loadRetention(tx)
	if e != nil {
		return e
	}
	if s.Enabled {
		if e = capacityError(s.Policy, q, old, n); e != nil {
			return e
		}
	}
	u[q] = n
	return putJSON(retentionRoot(tx), retentionUsageKey, u)
}

// capacityError refuses growth of q from before to after past its policy cap.
// Shrinking, or staying at an existing overage, is always allowed.
func capacityError(p RetentionPolicy, q QuotaDomain, before, after uint64) error {
	if cap := limits(p)[q]; after > before && after > cap {
		return &CapacityError{Domain: q, Used: before, Limit: cap}
	}
	return nil
}
func tableDomains(name []byte) (QuotaDomain, QuotaDomain) {
	switch {
	case bytes.Equal(name, retainedRequestsBucket):
		return RequestBytesQuota, RequestRecordsQuota
	case bytes.Equal(name, cleanupHeadersBucket), bytes.Equal(name, cleanupOperationsBucket):
		return CleanupBytesQuota, CleanupRecordsQuota
	case bytes.Equal(name, retainedScopesBucket):
		return MetadataBytesQuota, PrincipalScopesQuota
	default:
		return MetadataBytesQuota, 0
	}
}
func putRetentionRecord(tx *bolt.Tx, name, key []byte, value any) error {
	b := retentionTable(tx, name)
	if b == nil {
		return errors.New("missing retention table")
	}
	data, e := encodeRetentionRecord(name, value)
	if e != nil {
		return e
	}
	if bytes.Equal(name, retainedRequestsBucket) && b.Get(key) != nil {
		var before, after requestRecord
		if e = json.Unmarshal(b.Get(key), &before); e != nil {
			return e
		}
		if e = json.Unmarshal(data, &after); e != nil {
			return e
		}
		if before.Principal != after.Principal || before.Domain != after.Domain || before.requestID() != after.requestID() || before.JobID != after.JobID || before.TaskDigest != after.TaskDigest || before.WorkflowID != after.WorkflowID || before.GraphDigest != after.GraphDigest {
			return errors.New("request identity hash collision")
		}
	}
	old := b.Get(key)
	q, count := tableDomains(name)
	if e = adjust(tx, q, recordCharge(name, key, data), recordCharge(name, key, old)); e != nil {
		return e
	}
	if old == nil && count != 0 {
		if e = adjust(tx, count, 1, 0); e != nil {
			return e
		}
	}
	return b.Put(key, data)
}
func deleteRetentionRecord(tx *bolt.Tx, name, key []byte) error {
	b := retentionTable(tx, name)
	data := b.Get(key)
	if data == nil {
		return nil
	}
	q, count := tableDomains(name)
	if e := adjust(tx, q, 0, recordCharge(name, key, data)); e != nil {
		return e
	}
	if count != 0 {
		if e := adjust(tx, count, 0, 1); e != nil {
			return e
		}
	}
	return b.Delete(key)
}

func jobOverhead(j backend.Job) (uint64, error) {
	raw, e := json.Marshal(j)
	if e != nil {
		return 0, e
	}
	task, e := backend.TaskBytes(j.Task)
	if e != nil {
		return 0, e
	}
	n := len(raw) - len(task) + len(j.ID)
	for _, a := range j.Attempts {
		b, e := json.Marshal(a)
		if e != nil {
			return 0, e
		}
		n -= len(b)
	}
	if n < 0 {
		return 0, errors.New("invalid job metadata charge")
	}
	if n < jobOverheadReserve {
		n = jobOverheadReserve
	}
	return uint64(n), nil
}
func jobRelationBytes(j backend.Job, principal string, scoped bool) uint64 {
	n := uint64(40 + len(j.Attempts)*64)
	if scoped {
		n += uint64(32 + len(principal) + 40)
	}
	return n
}
func retryCharge(j backend.Job) uint64 {
	if j.Task.Retry == nil {
		return 0
	}
	return uint64(j.Task.Retry.MaxAttempts) * maxAttemptBytes
}
func putTrackedJob(tx *bolt.Tx, j backend.Job) error {
	var m retainedJob
	if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
		return e
	}
	n, e := jobOverhead(j)
	if e != nil {
		return e
	}
	if n > m.Overhead {
		return errors.New("job metadata exceeds reserved overhead")
	}
	if n < m.Overhead {
		if e = adjust(tx, MetadataBytesQuota, 0, m.Overhead-n); e != nil {
			return e
		}
		m.Overhead = n
		if e = putRetentionRecord(tx, retainedJobsBucket, []byte(j.ID), m); e != nil {
			return e
		}
	}
	return putJSON(tx.Bucket(jobsBucket), []byte(j.ID), j)
}
func principalScope(tx *bolt.Tx, principal string) error {
	b := retentionTable(tx, retainedScopesBucket)
	if b.Get([]byte(principal)) != nil {
		return nil
	}
	return putRetentionRecord(tx, retainedScopesBucket, []byte(principal), uint64(0))
}
func trackJob(tx *bolt.Tx, j backend.Job, m retainedJob, req requestRecord) error {
	key := requestKey(req.Principal, req.Domain, req.requestID())
	if v := retentionTable(tx, retainedRequestsBucket).Get(key); v != nil {
		if workflowRequest(v) {
			return ErrConflict
		}
		return errors.New("request identity key already exists")
	}
	if e := trackJobMetadata(tx, j, m); e != nil {
		return e
	}
	if e := putRetentionRecord(tx, retainedRequestsBucket, key, req); e != nil {
		return e
	}
	if req.LegacyBytes > 0 {
		return adjust(tx, RequestBytesQuota, req.LegacyBytes, 0)
	}
	return nil
}

// trackJobMetadata charges and records a new job's retained metadata. A
// workflow child has no request record; its workflow's request covers it.
func trackJobMetadata(tx *bolt.Tx, j backend.Job, m retainedJob) error {
	if e := principalScope(tx, m.Principal); e != nil {
		return e
	}
	var e error
	m.Overhead, e = jobOverhead(j)
	if e != nil {
		return e
	}
	if isScoped(tx) {
		m.Ordinal, e = scopePosition(tx, m.Principal, j.ID)
		if e != nil {
			return e
		}
	}
	scopeHigh := tx.Bucket(sequenceBucket).Sequence()
	if isScoped(tx) {
		scopeHigh = tx.Bucket(principalIndexBucket).Bucket([]byte(m.Principal)).Sequence()
	}
	if e = putRetentionRecord(tx, retainedScopesBucket, []byte(m.Principal), scopeHigh); e != nil {
		return e
	}
	if !m.Legacy && m.Overhead > jobOverheadReserve {
		return errors.New("new job metadata exceeds reserve")
	}
	t, e := backend.TaskBytes(j.Task)
	if e != nil {
		return e
	}
	for _, c := range []struct {
		q QuotaDomain
		n uint64
	}{{JobsQuota, 1}, {TaskBytesQuota, uint64(len(t))}, {RetryBytesQuota, retryCharge(j)}, {MetadataBytesQuota, m.Overhead}, {MetadataBytesQuota, jobRelationBytes(j, m.Principal, isScoped(tx))}} {
		if e = adjust(tx, c.q, c.n, 0); e != nil {
			return e
		}
	}
	return putRetentionRecord(tx, retainedJobsBucket, []byte(j.ID), m)
}
func initializeRetention(tx *bolt.Tx) error {
	if retentionRoot(tx) != nil {
		return validateRetention(tx)
	}
	r, e := tx.CreateBucket(retentionBucket)
	if e != nil {
		return e
	}
	for _, name := range [][]byte{retainedJobsBucket, retainedRequestsBucket, retainedOutputsBucket, retainedClosuresBucket, expiredOutputBucket, resultWitnessBucket, cleanupHeadersBucket, cleanupHeaderIndexBucket, cleanupOperationsBucket, cleanupOperationIndexBucket, retainedScopesBucket} {
		if _, e = r.CreateBucket(name); e != nil {
			return e
		}
	}
	st := retentionState{Policy: DefaultRetentionPolicy()}
	if e = putJSON(r, retentionStateKey, st); e != nil {
		return e
	}
	// A fixed installation reserve covers state, usage and bounded scalar counters.
	if e = putJSON(r, retentionUsageKey, retentionUsage{MetadataBytesQuota: 2048}); e != nil {
		return e
	}
	reverse := map[string]requestRecord{}
	record := func(principal string, b *bolt.Bucket) error {
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, id []byte) error {
			if reverse[string(id)].JobID != "" {
				return errors.New("job has multiple request identities")
			}
			j, e := get(tx, string(id))
			if e != nil {
				return e
			}
			d, e := taskDigest(j.Task)
			if e != nil {
				return e
			}
			r := requestRecord{Principal: principal, LegacyRequestID: bytes.Clone(k), JobID: j.ID, TaskDigest: d, LegacyBytes: uint64(len(k) + len(id))}
			n, e := legacyBindingBytes(tx, r)
			if e != nil {
				return e
			}
			r.LegacyBytes = n
			reverse[j.ID] = r
			return nil
		})
	}
	if isScoped(tx) {
		e = tx.Bucket(principalRequestsBucket).ForEach(func(p, v []byte) error {
			if v != nil {
				return errors.New("invalid request namespace")
			}
			return record(string(p), tx.Bucket(principalRequestsBucket).Bucket(p))
		})
	} else {
		e = record("legacy", tx.Bucket(requestsBucket))
	}
	if e != nil {
		return e
	}
	if e = tx.Bucket(jobsBucket).ForEach(func(id, data []byte) error {
		var j backend.Job
		if e := decodeStoredJob(data, &j); e != nil {
			return e
		}
		req, ok := reverse[string(id)]
		if !ok {
			return errors.New("missing request identity")
		}
		return trackJob(tx, j, retainedJob{Principal: req.Principal, LegacyRequestID: bytes.Clone(req.LegacyRequestID), Legacy: true}, req)
	}); e != nil {
		return e
	}
	if b := tx.Bucket(auditBucket); b != nil {
		if e = b.ForEach(func(k, v []byte) error { return adjust(tx, MetadataBytesQuota, uint64(len(k)+len(v)), 0) }); e != nil {
			return e
		}
	}
	u, e := usage(tx)
	if e != nil {
		return e
	}
	core := binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey))
	if core >= u[TaskBytesQuota] {
		st.TaskCounterOffset = core - u[TaskBytesQuota]
	} else {
		st.TaskCounterDeficit = u[TaskBytesQuota] - core
	}
	return putJSON(r, retentionStateKey, st)
}
func policyValid(p RetentionPolicy) bool {
	d := DefaultRetentionPolicy()
	a, b := limits(p), limits(d)
	for i := 1; i < len(a); i++ {
		if a[i] == 0 || a[i] > b[i] {
			return false
		}
	}
	return p.Window >= timeHour && p.Window <= 90*24*timeHour
}

const timeHour = 3600000000000

// sumUsage is only for migration/validation; hot-path mutations adjust counters.
func sumUsage(tx *bolt.Tx) (retentionUsage, error) {
	u := retentionUsage{MetadataBytesQuota: 2048}
	add := func(q QuotaDomain, n uint64) error { v, e := addChecked(u[q], n); u[q] = v; return e }
	for _, name := range [][]byte{retainedJobsBucket, retainedRequestsBucket, retainedOutputsBucket, retainedClosuresBucket, expiredOutputBucket, resultWitnessBucket, cleanupHeadersBucket, cleanupHeaderIndexBucket, cleanupOperationsBucket, cleanupOperationIndexBucket, retainedScopesBucket} {
		b := retentionTable(tx, name)
		if b == nil {
			return u, errors.New("missing retention table")
		}
		if e := b.ForEach(func(k, v []byte) error {
			if v == nil {
				return errors.New("unexpected retention nested bucket")
			}
			if bytes.Equal(name, cleanupHeadersBucket) {
				// recordCharge refuses an undecodable header only by an
				// overflowing charge; report the header's own refusal.
				var h cleanupHeader
				if e := decodeCleanupHeader(v, &h); e != nil {
					return e
				}
			}
			q, c := tableDomains(name)
			if e := add(q, recordCharge(name, k, v)); e != nil {
				return e
			}
			if c != 0 {
				if e := add(c, 1); e != nil {
					return e
				}
			}
			switch {
			case bytes.Equal(name, retainedJobsBucket):
				var m retainedJob
				if e := json.Unmarshal(v, &m); e != nil {
					return e
				}
				j, e := get(tx, string(k))
				if e != nil {
					return e
				}
				actual, e := jobOverhead(j)
				if e != nil {
					return e
				}
				if actual != m.Overhead {
					return errors.New("invalid retained overhead charge")
				}
				t, e := backend.TaskBytes(j.Task)
				if e != nil {
					return e
				}
				for _, x := range []struct {
					q QuotaDomain
					n uint64
				}{{JobsQuota, 1}, {TaskBytesQuota, uint64(len(t))}, {RetryBytesQuota, retryCharge(j)}, {MetadataBytesQuota, m.Overhead}, {MetadataBytesQuota, jobRelationBytes(j, m.Principal, isScoped(tx))}} {
					if e := add(x.q, x.n); e != nil {
						return e
					}
				}
			case bytes.Equal(name, retainedRequestsBucket):
				var x requestRecord
				if e := json.Unmarshal(v, &x); e != nil {
					return e
				}
				if x.WorkflowID != "" {
					return nil
				}
				n, e := legacyBindingBytes(tx, x)
				if e != nil {
					return e
				}
				if n != x.LegacyBytes {
					return errors.New("legacy request binding byte mismatch")
				}
				return add(RequestBytesQuota, n)
			case bytes.Equal(name, retainedOutputsBucket):
				var x outputReservation
				if e := json.Unmarshal(v, &x); e != nil {
					return e
				}
				for _, x := range []struct {
					q QuotaDomain
					n uint64
				}{{OutputBytesQuota, x.Bytes}, {OutputDomainsQuota, 1}, {OutputFilesQuota, 2}} {
					if e := add(x.q, x.n); e != nil {
						return e
					}
				}
			}
			return nil
		}); e != nil {
			return u, e
		}
	}
	if b := tx.Bucket(auditBucket); b != nil {
		if e := b.ForEach(func(k, v []byte) error { return add(MetadataBytesQuota, uint64(len(k)+len(v))) }); e != nil {
			return u, e
		}
	}
	// Graph bytes are retained task bytes but are not in the core job counter.
	graphs, metadata, e := workflowUsage(tx)
	if e != nil {
		return u, e
	}
	if e = add(TaskBytesQuota, graphs); e != nil {
		return u, e
	}
	if e = add(MetadataBytesQuota, metadata); e != nil {
		return u, e
	}
	groups, e := workerGroupUsage(tx)
	if e != nil {
		return u, e
	}
	if e = add(MetadataBytesQuota, groups); e != nil {
		return u, e
	}
	st, e := loadRetention(tx)
	if e != nil {
		return u, e
	}
	if st.TaskCounterOffset != 0 && st.TaskCounterDeficit != 0 {
		return u, errors.New("invalid legacy task counter adjustment")
	}
	expected, e := addChecked(u[TaskBytesQuota]-graphs, st.TaskCounterOffset)
	if e != nil {
		return u, e
	}
	if expected < st.TaskCounterDeficit {
		return u, errors.New("legacy task counter adjustment underflow")
	}
	expected -= st.TaskCounterDeficit
	if n := binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(taskBytesKey)); n != expected {
		return u, errors.New("task accounting mismatch")
	}

	if n := binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(retryBytesKey)); n != u[RetryBytesQuota] {
		return u, errors.New("retry accounting mismatch")
	}
	return u, nil
}

func reindexRetentionScopes(tx *bolt.Tx) error {
	if retentionRoot(tx) == nil {
		return nil
	}
	if e := tx.Bucket(requestsBucket).ForEach(func(id, jobID []byte) error {
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), jobID, &m); e != nil {
			return e
		}
		if m.LegacyRequestRetired || !validRequestIdentity(m.Domain, m.RequestID, m.LegacyRequestID) {
			return errors.New("legacy root binding targets retired identity")
		}
		key := requestKey(m.Principal, m.Domain, m.requestID())
		var r requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), key, &r); e != nil {
			return e
		}
		if r.Domain.Epoch != 0 || r.requestID() != string(id) || r.JobID != string(jobID) || r.Principal != "legacy" {
			return errors.New("invalid legacy scope activation binding")
		}
		n, e := legacyBindingBytes(tx, r)
		if e != nil {
			return e
		}
		if n < r.LegacyBytes {
			return errors.New("legacy activation charge decreased")
		}
		if e = adjust(tx, RequestBytesQuota, n, r.LegacyBytes); e != nil {
			return e
		}
		r.LegacyBytes = n
		return putRetentionRecord(tx, retainedRequestsBucket, key, r)
	}); e != nil {
		return e
	}
	return tx.Bucket(jobsBucket).ForEach(func(id, raw []byte) error {
		var j backend.Job
		if e := decodeStoredJob(raw, &j); e != nil {
			return e
		}
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), id, &m); e != nil {
			return e
		}
		m.Ordinal = 0
		var e error
		m.Ordinal, e = scopePosition(tx, m.Principal, j.ID)
		if e != nil {
			return e
		}
		if e = adjust(tx, MetadataBytesQuota, uint64(32+len(m.Principal)+40), 0); e != nil {
			return e
		}
		if e = putRetentionRecord(tx, retainedScopesBucket, []byte(m.Principal), tx.Bucket(principalIndexBucket).Bucket([]byte(m.Principal)).Sequence()); e != nil {
			return e
		}
		return putRetentionRecord(tx, retainedJobsBucket, id, m)
	})
}

// legacyBindingBytes verifies the original raw binding relation and returns its
// actual key/value charge, including both preserved legacy copies in scoped mode.
func legacyBindingBytes(tx *bolt.Tx, r requestRecord) (uint64, error) {
	if r.Domain.Epoch != 0 {
		if r.LegacyBytes != 0 {
			return 0, errors.New("modern request has legacy bytes")
		}
		return 0, nil
	}
	raw := []byte(r.requestID())
	b := requestNamespace(tx, r.Principal)
	if r.LegacyBytes == 0 {
		if b != nil && b.Get(raw) != nil || isScoped(tx) && r.Principal == "legacy" && tx.Bucket(requestsBucket).Get(raw) != nil {
			return 0, errors.New("uncharged legacy request binding")
		}
		return 0, nil
	}
	if b == nil || !bytes.Equal(b.Get(raw), []byte(r.JobID)) {
		return 0, errors.New("missing charged legacy request binding")
	}
	n := uint64(len(raw) + len(r.JobID))
	if isScoped(tx) && r.Principal == "legacy" {
		// Older scoped stores preserve a root mirror only for entries copied
		// during activation; later scoped legacy entries have no root copy.
		if mirror := tx.Bucket(requestsBucket).Get(raw); mirror != nil {
			if !bytes.Equal(mirror, []byte(r.JobID)) {
				return 0, errors.New("invalid preserved legacy request binding")
			}
			return addChecked(n, n)
		}
	}
	return n, nil
}
