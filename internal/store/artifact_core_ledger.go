package store

import (
	"bytes"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// artifactCoreLedger observes ONLY the concrete rows/scalars touched by these
// accept/attempt callers. It never copies a database/table or changes admission.
// Rows are compared after prospective core writes but before artifact apply;
// any refusal rolls back the enclosing Bolt transaction, including sequences.
type artifactCoreLedger struct {
	rows    []artifactCoreRow
	buckets []artifactCoreBucket
}
type artifactCorePath struct{ root, child string }
type artifactCoreRow struct {
	path   artifactCorePath
	key    string
	before []byte
}
type artifactCoreBucket struct {
	path     artifactCorePath
	existed  bool
	sequence uint64
}
type artifactCoreUsage struct {
	bytes   uint64
	created int
}

func (p artifactCorePath) bucket(tx *bolt.Tx) *bolt.Bucket {
	b := tx.Bucket([]byte(p.root))
	if b != nil && p.child != "" {
		b = b.Bucket([]byte(p.child))
	}
	return b
}
func (p artifactCorePath) value(tx *bolt.Tx, key string) []byte {
	if b := p.bucket(tx); b != nil {
		return b.Get([]byte(key))
	}
	return nil
}
func (l *artifactCoreLedger) row(tx *bolt.Tx, p artifactCorePath, key []byte) {
	for _, r := range l.rows {
		if r.path == p && r.key == string(key) {
			return
		}
	}
	l.rows = append(l.rows, artifactCoreRow{p, string(key), bytes.Clone(p.value(tx, string(key)))})
}
func (l *artifactCoreLedger) bucket(tx *bolt.Tx, p artifactCorePath) {
	for _, b := range l.buckets {
		if b.path == p {
			return
		}
	}
	b := p.bucket(tx)
	before := artifactCoreBucket{path: p, existed: b != nil}
	if b != nil {
		before.sequence = b.Sequence()
	}
	l.buckets = append(l.buckets, before)
}
func (l *artifactCoreLedger) measure(tx *bolt.Tx) (out artifactCoreUsage, err error) {
	if len(l.rows) > 32 || len(l.buckets) > 4 {
		return out, errors.New("artifact core ledger exceeds scope")
	}
	for _, r := range l.rows {
		after := r.path.value(tx, r.key)
		if bytes.Equal(r.before, after) {
			continue
		}
		if err := artifactAdd(&out.bytes, uint64(len(r.key)+len(r.before)+len(after))); err != nil {
			return out, err
		}
		if r.before == nil && after != nil {
			out.created++
		}
	}
	for _, old := range l.buckets {
		b := old.path.bucket(tx)
		if b == nil {
			if old.existed {
				return out, ErrArtifactConflict
			}
			continue
		}
		if !old.existed {
			// bbolt1.3.11 bucket records have a16-byte root+sequence header.
			// This is logical metadata, not spill/page/physical accounting.
			if err := artifactAdd(&out.bytes, uint64(len(old.path.child)+16)); err != nil {
				return out, err
			}
			out.created++
		} else if old.sequence != b.Sequence() {
			// Same scalar convention as retentionMutation: old+new uint64.
			if err := artifactAdd(&out.bytes, 16); err != nil {
				return out, err
			}
		}
	}
	return
}

func artifactAttemptLedger(tx *bolt.Tx, job, candidate string) *artifactCoreLedger {
	l := new(artifactCoreLedger)
	l.row(tx, artifactCorePath{root: string(jobsBucket)}, []byte(job))
	l.row(tx, artifactCorePath{root: string(metadataBucket)}, taskBytesKey)
	l.row(tx, artifactCorePath{root: string(metadataBucket)}, retryBytesKey)
	retention := func(table []byte, key []byte) {
		l.row(tx, artifactCorePath{string(retentionBucket), string(table)}, key)
	}
	retention(nil, retentionUsageKey)
	retention(retainedJobsBucket, []byte(job))
	j, e := get(tx, job)
	if e == nil {
		retention(retainedOutputsBucket, []byte(j.CurrentAttemptID()))
	}
	if candidate != "" {
		l.row(tx, artifactCorePath{root: string(attemptJobsBucket)}, []byte(candidate))
		retention(retainedOutputsBucket, []byte(candidate))
	}
	// A workflow child's publication advances its workflow's revision.
	var c workflowChild
	if workflowDecode(workflowRow(tx, workflowChildKey(job)), &c) == nil {
		l.row(tx, artifactCorePath{root: string(workflowsBucket)}, workflowStateKey(c.Workflow))
	}
	return l
}

// artifactJobLedger observes the rows a new job's acceptance writes, except
// any request binding. A workflow child is accepted with exactly these.
func artifactJobLedger(tx *bolt.Tx, job, principal string) *artifactCoreLedger {
	l := artifactAttemptLedger(tx, job, job)
	retention := artifactCorePath{root: string(retentionBucket)}
	l.row(tx, retention, retentionStateKey)
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedScopesBucket)}, []byte(principal))
	seq := artifactCorePath{root: string(sequenceBucket)}
	l.bucket(tx, seq)
	l.row(tx, seq, sequenceKey(tx.Bucket(sequenceBucket).Sequence()+1))
	if isScoped(tx) {
		l.row(tx, artifactCorePath{root: string(jobPrincipalsBucket)}, []byte(job))
		index := artifactCorePath{string(principalIndexBucket), principal}
		l.bucket(tx, index)
		var next uint64 = 1
		if b := index.bucket(tx); b != nil {
			next = b.Sequence() + 1
		}
		l.row(tx, index, sequenceKey(next))
	}
	return l
}

func artifactAcceptLedger(tx *bolt.Tx, job, principal, request string, domain RequestDomain, legacy bool) *artifactCoreLedger {
	l := artifactJobLedger(tx, job, principal)
	l.row(tx, artifactCorePath{string(retentionBucket), string(retainedRequestsBucket)}, requestKey(principal, domain, request))
	switch {
	case legacy && isScoped(tx):
		requests := artifactCorePath{string(principalRequestsBucket), principal}
		l.bucket(tx, requests)
		l.row(tx, requests, []byte(request))
	case legacy:
		l.row(tx, artifactCorePath{root: string(requestsBucket)}, []byte(request))
	}
	return l
}

func artifactCoreChange(tx *bolt.Tx, l *artifactCoreLedger, budget uint64, records int, change func(*artifactState) error) (artifactCoreUsage, error) {
	before, err := artifactLoad(tx)
	if err != nil {
		return artifactCoreUsage{}, err
	}
	after := before.clone()
	if err := change(after); err != nil {
		return artifactCoreUsage{}, err
	}
	core, err := l.measure(tx)
	if err != nil {
		return core, err
	}
	if budget > maxChangeBytes || budget < core.bytes || records < 0 || records > 100 || core.created > records {
		return core, errArtifactMutationBudget
	}
	prepared, err := prepareArtifactChange(tx, before, after, nil, budget-core.bytes)
	if err != nil {
		return core, err
	}
	if prepared.newRecords > records-core.created {
		return core, errArtifactMutationBudget
	}
	if err := prepared.apply(tx); err != nil {
		return core, err
	}
	return core, nil
}
