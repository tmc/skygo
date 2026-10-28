package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// retentionMutation is a transaction-local GC write plan. It is used ONLY by
// prune/finalize/request retirement: admission keeps its ordinary incremental checks. No write is
// applied until final quotas and actual encoded change bytes have been checked.
type retentionMutation struct {
	tx            *bolt.Tx
	initial, used retentionUsage
	changes       map[retentionKey][]byte
	sequences     map[string]uint64
}
type retentionKey struct {
	retention  bool
	table, key string
}

func newRetentionMutation(tx *bolt.Tx) (*retentionMutation, error) {
	u, e := usage(tx)
	if e != nil {
		return nil, e
	}
	return &retentionMutation{tx: tx, initial: u, used: u, changes: map[retentionKey][]byte{}, sequences: map[string]uint64{}}, nil
}
func (m *retentionMutation) fork() *retentionMutation {
	n := *m
	n.changes = make(map[retentionKey][]byte, len(m.changes))
	for k, v := range m.changes {
		n.changes[k] = v
	}
	n.sequences = make(map[string]uint64, len(m.sequences))
	for k, v := range m.sequences {
		n.sequences[k] = v
	}
	return &n
}
func (m *retentionMutation) bucket(k retentionKey) *bolt.Bucket {
	if k.retention {
		if k.table == "" {
			return retentionRoot(m.tx)
		}
		return retentionTable(m.tx, []byte(k.table))
	}
	if parts := strings.SplitN(k.table, "/", 2); len(parts) == 2 {
		return m.tx.Bucket([]byte(parts[0])).Bucket([]byte(parts[1]))
	}
	return m.tx.Bucket([]byte(k.table))
}
func (m *retentionMutation) get(k retentionKey) []byte {
	if v, ok := m.changes[k]; ok {
		return v
	}
	return m.bucket(k).Get([]byte(k.key))
}
func (m *retentionMutation) adjust(q QuotaDomain, add, remove uint64) error {
	if q < JobsQuota || q > PrincipalScopesQuota || remove > m.used[q] {
		return errors.New("invalid GC accounting subtraction")
	}
	n, e := addChecked(m.used[q]-remove, add)
	if e != nil {
		return e
	}
	m.used[q] = n
	return nil
}
func (m *retentionMutation) put(name, key []byte, value any) error {
	data, e := encodeRetentionRecord(name, value)
	if e != nil {
		return e
	}
	k := retentionKey{true, string(name), string(key)}
	old := m.get(k)
	q, count := tableDomains(name)
	if e = m.adjust(q, recordCharge(name, key, data), recordCharge(name, key, old)); e != nil {
		return e
	}
	if old == nil && count != 0 {
		if e = m.adjust(count, 1, 0); e != nil {
			return e
		}
	}
	m.changes[k] = data
	return nil
}
func (m *retentionMutation) drop(name, key []byte) error {
	k := retentionKey{true, string(name), string(key)}
	old := m.get(k)
	if old == nil {
		return nil
	}
	q, count := tableDomains(name)
	if e := m.adjust(q, 0, recordCharge(name, key, old)); e != nil {
		return e
	}
	if count != 0 {
		if e := m.adjust(count, 0, 1); e != nil {
			return e
		}
	}
	m.changes[k] = nil
	return nil
}
func (m *retentionMutation) corePut(name, key, value []byte) {
	m.changes[retentionKey{false, string(name), string(key)}] = bytes.Clone(value)
}
func (m *retentionMutation) coreGet(name, key []byte) []byte {
	k := retentionKey{false, string(name), string(key)}
	if value, ok := m.changes[k]; ok {
		return value
	}
	if b := m.bucket(k); b != nil {
		return b.Get(key)
	}
	return nil
}
func (m *retentionMutation) next(name []byte) (uint64, error) {
	n, ok := m.sequences[string(name)]
	if !ok {
		n = retentionTable(m.tx, name).Sequence()
	}
	if n == ^uint64(0) {
		return 0, errors.New("cleanup sequence exhausted")
	}
	n++
	m.sequences[string(name)] = n
	return n, nil
}
func (m *retentionMutation) validate() error {
	st, e := loadRetention(m.tx)
	if e != nil {
		return e
	}
	for q := JobsQuota; q <= PrincipalScopesQuota; q++ {
		if e := capacityError(st.Policy, q, m.initial[q], m.used[q]); e != nil {
			return e
		}
	}
	return nil
}
func (m *retentionMutation) changedBytes() (uint64, error) {
	var n uint64
	add := func(v uint64) error { var e error; n, e = addChecked(n, v); return e }
	for k, v := range m.changes {
		old := m.bucket(k).Get([]byte(k.key))
		if bytes.Equal(old, v) {
			continue
		}
		if e := add(uint64(len(k.key) + len(old) + len(v))); e != nil {
			return 0, e
		}
	}
	for range m.sequences {
		if e := add(16); e != nil {
			return 0, e
		}
	}
	next, e := json.Marshal(m.used)
	if e != nil {
		return 0, e
	}
	old := retentionRoot(m.tx).Get(retentionUsageKey)
	if e = add(uint64(len(retentionUsageKey) + len(old) + len(next))); e != nil {
		return 0, e
	}
	return n, nil
}
func (m *retentionMutation) commit() error {
	if e := m.validate(); e != nil {
		return e
	}
	n, e := m.changedBytes()
	if e != nil {
		return e
	}
	if n > maxChangeBytes {
		return errors.New("retention mutation exceeds change budget")
	}
	keys := make([]retentionKey, 0, len(m.changes))
	for k := range m.changes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.retention != b.retention {
			return !a.retention
		}
		if a.table != b.table {
			return a.table < b.table
		}
		return a.key < b.key
	})
	for _, k := range keys {
		b := m.bucket(k)
		v := m.changes[k]
		if v == nil {
			e = b.Delete([]byte(k.key))
		} else {
			e = b.Put([]byte(k.key), v)
		}
		if e != nil {
			return e
		}
	}
	for name, n := range m.sequences {
		if e = retentionTable(m.tx, []byte(name)).SetSequence(n); e != nil {
			return e
		}
	}
	return putJSON(retentionRoot(m.tx), retentionUsageKey, m.used)
}
func encodeRetentionRecord(name []byte, value any) ([]byte, error) {
	data, e := json.Marshal(value)
	if e != nil {
		return nil, e
	}
	if bytes.Equal(name, cleanupHeadersBucket) {
		var h cleanupHeader
		if e = json.Unmarshal(data, &h); e != nil {
			return nil, e
		}
		limit := cleanupHeaderLimit
		if h.NeedsManual {
			limit = manualHeaderLimit
		}
		if len(data) > limit {
			return nil, errors.New("cleanup header exceeds limit")
		}
	}
	if bytes.Equal(name, cleanupOperationsBucket) && len(data) > cleanupOperationLimit {
		return nil, errors.New("cleanup operation exceeds limit")
	}
	if bytes.Equal(name, retainedRequestsBucket) && workflowRequest(data) && len(data) > workflowRequestLimit {
		return nil, errors.New("workflow request exceeds limit")
	}
	return data, nil
}

// workflowRequest reports whether a request record is the typed workflow
// alternative. Its fixed reservation keeps expiry from changing its charge.
func workflowRequest(data []byte) bool {
	var r requestRecord
	return json.Unmarshal(data, &r) == nil && r.WorkflowID != ""
}

// Mutable operation fields have a declared value reservation. Header count
// fields similarly reserve their full declared representation independent of
// current digit widths; their immutable payload is charged once.
func recordCharge(name, key, data []byte) uint64 {
	if data == nil {
		return 0
	}
	if bytes.Equal(name, cleanupOperationsBucket) {
		return uint64(len(key) + cleanupOperationLimit)
	}
	if bytes.Equal(name, cleanupHeadersBucket) {
		var h cleanupHeader
		if decodeCleanupHeader(data, &h) != nil {
			return ^uint64(0)
		}
		h.Remaining = ^uint32(0)
		h.OutputRemaining = ^uint32(0)
		h.NativeRemaining = ^uint32(0)
		v, e := json.Marshal(h)
		if e != nil {
			return ^uint64(0)
		}
		return uint64(len(key) + len(v))
	}
	if bytes.Equal(name, retainedRequestsBucket) && workflowRequest(data) {
		return uint64(len(key) + workflowRequestLimit)
	}
	return uint64(len(key) + len(data))
}

func (m *retentionMutation) coreDelete(name, key []byte) error { m.corePut(name, key, nil); return nil }
func (m *retentionMutation) removeHeader(h cleanupHeader) error {
	if e := m.drop(cleanupHeaderIndexBucket, sequenceKey(h.Sequence)); e != nil {
		return e
	}
	if e := m.drop(cleanupHeadersBucket, []byte(h.ID)); e != nil {
		return e
	}
	return m.retireWorkflowChild(h)
}
func (m *retentionMutation) dropLegacy(r *requestRecord) error {
	if r.LegacyBytes == 0 {
		return nil
	}
	name := requestsBucket
	if isScoped(m.tx) {
		name = []byte(string(principalRequestsBucket) + "/" + r.Principal)
	}
	m.corePut(name, []byte(r.requestID()), nil)
	if isScoped(m.tx) && r.Principal == "legacy" {
		m.corePut(requestsBucket, []byte(r.requestID()), nil)
	}
	if e := m.adjust(RequestBytesQuota, 0, r.LegacyBytes); e != nil {
		return e
	}
	r.LegacyBytes = 0
	return nil
}

func (m *retentionMutation) rootPut(key []byte, value any) error {
	data, e := json.Marshal(value)
	if e != nil {
		return e
	}
	m.changes[retentionKey{true, "", string(key)}] = data
	return nil
}
