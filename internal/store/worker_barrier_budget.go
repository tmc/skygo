package store

import (
	"bytes"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// workerBarrierBudget captures the exact cells acceptance and reconciliation
// may change. It includes job, request, scope, quota and sequence facts rather
// than treating the worker row reservation as a transaction byte budget.
// The returned check runs in the same transaction after writes; refusal rolls
// the entire transaction back. No unrelated rows or physical files are scanned.
func workerBarrierBudget(tx *bolt.Tx, id, principal string, domain RequestDomain, requestID string) func() error {
	type cell struct {
		bucket   *bolt.Bucket
		key, old []byte
	}
	var cells []cell
	add := func(b *bolt.Bucket, k []byte) {
		if b != nil {
			cells = append(cells, cell{b, bytes.Clone(k), bytes.Clone(b.Get(k))})
		}
	}
	add(tx.Bucket(jobsBucket), []byte(id))
	add(tx.Bucket(jobPrincipalsBucket), []byte(id))
	for _, k := range [][]byte{taskBytesKey, retryBytesKey} {
		add(tx.Bucket(metadataBucket), k)
	}
	add(retentionRoot(tx), retentionStateKey)
	add(retentionRoot(tx), retentionUsageKey)
	add(retentionTable(tx, retainedJobsBucket), []byte(id))
	add(retentionTable(tx, retainedRequestsBucket), requestKey(principal, domain, requestID))
	add(retentionTable(tx, retainedScopesBucket), []byte(principal))
	add(tx.Bucket(workerGroupsBucket), workerGroupKey(id))
	for rank := uint8(0); rank < 16; rank++ {
		add(tx.Bucket(workerGroupsBucket), workerMemberKey(id, rank))
	}
	seq := tx.Bucket(sequenceBucket)
	add(seq, sequenceKey(seq.Sequence()+1))
	scopes := tx.Bucket(principalIndexBucket)
	var index *bolt.Bucket
	if scopes != nil {
		index = scopes.Bucket([]byte(principal))
		if index != nil {
			add(index, sequenceKey(index.Sequence()+1))
		}
	}
	sequenceBuckets := []*bolt.Bucket{seq, tx.Bucket(workerGroupsBucket), index}
	oldSeq := make([]uint64, len(sequenceBuckets))
	for i, b := range sequenceBuckets {
		if b != nil {
			oldSeq[i] = b.Sequence()
		}
	}
	return func() error {
		var used uint64
		for _, c := range cells {
			now := c.bucket.Get(c.key)
			if !bytes.Equal(c.old, now) {
				used += uint64(len(c.key) + len(c.old) + len(now))
			}
		}
		for i, b := range sequenceBuckets {
			if b != nil && b.Sequence() != oldSeq[i] {
				used += 16
			}
		}
		// Acceptance may create a previously absent principal index. Count its
		// actual first key, value and sequence; subsequent acceptances use cells.
		if scopes != nil && index == nil {
			if b := scopes.Bucket([]byte(principal)); b != nil {
				used += uint64(len(principal)) + 16
				if e := b.ForEach(func(k, v []byte) error { used += uint64(len(k) + len(v)); return nil }); e != nil {
					return e
				}
			}
		}
		if used > maxChangeBytes {
			return errors.New("worker barrier mutation exceeds byte limit")
		}
		return nil
	}
}
