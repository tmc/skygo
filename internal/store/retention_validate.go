package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func validateRetention(tx *bolt.Tx) error {
	st, e := loadRetention(tx)
	if e != nil {
		return e
	}
	if len(retentionStateKey)+len(retentionRoot(tx).Get(retentionStateKey))+len(retentionUsageKey)+len(retentionRoot(tx).Get(retentionUsageKey)) > 2048 {
		return errors.New("retention scalar reservation exceeded")
	}
	if st.RequestAfter != "" && !validRequestCursor(st.RequestAfter) {
		return errors.New("invalid retirement cursor")
	}
	if !validLegacyJobsCursor(st, tx.Bucket(sequenceBucket).Sequence()) {
		return errors.New("invalid legacy job retirement cursor")
	}
	if !policyValid(st.Policy) {
		return errors.New("invalid retention policy")
	}
	if st.Enabled {
		if !st.Policy.LocalOutputEnabled {
			if st.Policy.OutputUnit != 0 || st.Policy.Volume != 0 || retentionTable(tx, retainedOutputsBucket).Stats().KeyN != 0 {
				return errors.New("disabled local output has reservations or volume")
			}
		} else {
			a := st.Policy.OutputUnit
			if a < 4096 || a > 65536 || a&(a-1) != 0 {
				return errors.New("invalid local output allocation unit")
			}
		}
		if !validAttemptID(st.Lineage) || st.LineageSerial == 0 || st.Current == 0 || len(st.Epochs) > 2 || len(st.Epochs) == 0 || st.Restored && st.LegacyReplayOpen {
			return errors.New("invalid retention lineage")
		}
		var prev uint64
		for _, ep := range st.Epochs {
			if ep.Number == 0 || ep.Number <= prev || ep.Number > st.Current || ep.AcceptUntil.IsZero() || !ep.RetryUntil.Equal(ep.AcceptUntil.Add(st.Policy.Window)) || ep.Number <= st.RetiredThrough && !ep.AcceptClosed {
				return errors.New("invalid request epoch")
			}
			prev = ep.Number
		}
	} else if st.Lineage != "" || st.Current != 0 || st.LegacyReplayOpen || len(st.Epochs) != 0 {
		return errors.New("disabled retention contains request epochs")
	}
	if st.Enabled && (st.TaskCounterOffset != 0 || st.TaskCounterDeficit != 0) {
		return errors.New("enabled retention has legacy counter adjustment")
	}
	if e = retentionTable(tx, retainedScopesBucket).ForEach(func(k, v []byte) error {
		if !validPrincipal(k) {
			return errors.New("invalid scope witness principal")
		}
		var n uint64
		if e := json.Unmarshal(v, &n); e != nil {
			return e
		}
		want := tx.Bucket(sequenceBucket).Sequence()
		if isScoped(tx) {
			b := tx.Bucket(principalIndexBucket).Bucket(k)
			if b == nil {
				if n != 0 {
					return errors.New("scope witness has no index")
				}
				return nil
			}
			want = b.Sequence()
		}
		if n != want {
			return errors.New("scope high-water witness mismatch")
		}
		return nil
	}); e != nil {
		return e
	}
	used, e := usage(tx)
	if e != nil {
		return e
	}
	want, e := sumUsage(tx)
	if e != nil {
		return e
	}
	if used != want {
		return errors.New("retention accounting mismatch")
	}
	if e = tx.Bucket(jobsBucket).ForEach(func(id, raw []byte) error {
		var j backend.Job
		if e := decodeStoredJob(raw, &j); e != nil {
			return e
		}
		var m retainedJob
		if e := loadJSON(retentionTable(tx, retainedJobsBucket), id, &m); e != nil {
			return e
		}
		if !validPrincipal([]byte(m.Principal)) || isScoped(tx) && string(tx.Bucket(jobPrincipalsBucket).Get(id)) != m.Principal {
			return errors.New("invalid retained principal")
		}
		if m.WorkflowID != "" {
			if e := validateWorkflowChild(tx, j, m); e != nil {
				return e
			}
		} else if !validJobRequestIdentity(st, m) || !m.Legacy && !validBinding(j.TargetBinding) {
			return errors.New("invalid retained job request identity")
		}
		requestLive := !st.Enabled || m.Domain.Lineage == st.Lineage && (m.Domain.Epoch == 0 && st.LegacyReplayOpen || m.Domain.Epoch > st.RetiredThrough)
		if requestLive && m.WorkflowID == "" {
			if m.LegacyRequestRetired {
				return errors.New("live request has retired legacy metadata")
			}
			var r requestRecord
			if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(m.Principal, m.Domain, m.requestID()), &r); e != nil {
				return errors.New("missing live request identity")
			}
			if r.Expired || r.JobID != j.ID {
				return errors.New("invalid live request identity")
			}
		}
		if m.OutputStart > 10 || m.OutputStart > uint32(max(1, len(j.Attempts))) || m.OutputStart != 0 && (m.Storage != backend.LocalOutput || !m.Started) || !m.Legacy && m.Started && m.Storage == backend.LocalOutput && m.OutputStart != 1 {
			return errors.New("invalid output tracking boundary")
		}
		if m.OutputStart != 0 {
			if len(j.Attempts) > 0 && j.Attempts[m.OutputStart-1].StartingAt == nil {
				return errors.New("output boundary was never started")
			}
			if !m.Legacy {
				for i, a := range j.Attempts {
					if a.StartingAt != nil && uint32(i+1) < m.OutputStart {
						return errors.New("output boundary skips new attempt")
					}
				}
			}
			for i, aid := range executionIDs(j) {
				started := len(j.Attempts) == 0 || j.Attempts[i].StartingAt != nil
				if uint32(i+1) >= m.OutputStart && started && retentionTable(tx, retainedOutputsBucket).Get([]byte(aid)) == nil && retentionTable(tx, expiredOutputBucket).Get([]byte(aid)) == nil {
					return errors.New("missing started output domain")
				}
			}
		}
		over, e := jobOverhead(j)
		if e != nil || over != m.Overhead {
			return errors.New("job overhead reservation mismatch")
		}
		if isScoped(tx) && string(tx.Bucket(principalIndexBucket).Bucket([]byte(m.Principal)).Get(sequenceKey(m.Ordinal))) != j.ID {
			return errors.New("scope ordinal mismatch")
		}
		if st.Enabled && m.Storage == backend.LocalOutput && (!st.Policy.LocalOutputEnabled || m.OutputReserve != uint64(backend.MaxLogBytes)+4*st.Policy.OutputUnit) {
			return errors.New("invalid accepted local output charge")
		}
		if m.Storage > backend.RemoteOutput || m.Storage == backend.RemoteOutput && m.OutputReserve != 0 {
			return errors.New("invalid retained output placement")
		}
		return nil
	}); e != nil {
		return e
	}
	if e = retentionTable(tx, retainedRequestsBucket).ForEach(func(k, v []byte) error {
		var r requestRecord
		if e := json.Unmarshal(v, &r); e != nil {
			return e
		}
		if r.WorkflowID != "" {
			// The workflow alternative: live while its state exists, then an
			// expired typed witness. validateWorkflows checks the live relation.
			if !validWorkflowRequest(r) || !bytes.Equal(k, requestKey(r.Principal, r.Domain, r.RequestID)) || !st.Enabled && r.Domain != (RequestDomain{}) || st.Enabled && r.Domain.Lineage == "" {
				return errors.New("invalid workflow request")
			}
			if live := workflowRow(tx, workflowStateKey(r.WorkflowID)) != nil; live == r.Expired {
				return errors.New("workflow request state mismatch")
			}
			return nil
		}
		if !validPrincipal([]byte(r.Principal)) || !bytes.Equal(k, requestKey(r.Principal, r.Domain, r.requestID())) || !validRequestIdentity(r.Domain, r.RequestID, r.LegacyRequestID) || !validAttemptID(r.JobID) {
			return errors.New("invalid retained request")
		}
		if !r.Expired {
			j, e := get(tx, r.JobID)
			if e != nil {
				return e
			}
			d, e := taskDigest(j.Task)
			if e != nil || d != r.TaskDigest {
				return errors.New("request task digest mismatch")
			}
			var m retainedJob
			if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m); e != nil {
				return e
			}
			if m.LegacyRequestRetired || m.Principal != r.Principal || m.Domain != r.Domain || m.requestID() != r.requestID() {
				return errors.New("request identity mismatch")
			}
		} else if tx.Bucket(jobsBucket).Get([]byte(r.JobID)) != nil {
			return errors.New("expired request still has live result")
		}
		return nil
	}); e != nil {
		return e
	}
	counts := map[string]uint32{}
	outputCounts := map[string]uint32{}
	nativeCounts := map[string]uint32{}
	children := map[string]bool{}
	pendingOutputs := map[string]bool{}
	if e = retentionTable(tx, cleanupOperationsBucket).ForEach(func(k, v []byte) error {
		var op cleanupOperation
		if e := json.Unmarshal(v, &op); e != nil {
			return e
		}
		if string(k) != op.ID || len(v) > cleanupOperationLimit {
			return errors.New("invalid cleanup operation encoding")
		}
		var h cleanupHeader
		if e := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(op.HeaderID), &h); e != nil {
			return e
		}
		if e := validateCleanupOperation(tx, h, op); e != nil {
			return e
		}
		id, e := indexID(tx, cleanupOperationIndexBucket, op.Sequence)
		if e != nil || id != op.ID {
			return errors.New("cleanup operation index mismatch")
		}
		child := fmt.Sprintf("%s/%d/%s", h.ID, op.Operation, op.AttemptID)
		if children[child] {
			return errors.New("duplicate cleanup attempt operation")
		}
		children[child] = true
		if op.Operation == OutputCleanup {
			pendingOutputs[op.AttemptID] = true
		}
		counts[h.ID]++
		if op.Operation == OutputCleanup {
			outputCounts[h.ID]++
		} else {
			nativeCounts[h.ID]++
		}
		return nil
	}); e != nil {
		return e
	}
	if e = retentionTable(tx, cleanupHeadersBucket).ForEach(func(k, v []byte) error {
		var h cleanupHeader
		if e := decodeCleanupHeader(v, &h); e != nil {
			return e
		}
		limit := cleanupHeaderLimit
		if h.NeedsManual {
			limit = manualHeaderLimit
		}
		if len(v) > limit || string(k) != h.ID || h.ID != h.JobID || len(h.ExecutionIDs) == 0 || len(h.ExecutionIDs) > 10 || h.Remaining != counts[h.ID] || h.OutputRemaining != outputCounts[h.ID] || h.NativeRemaining != nativeCounts[h.ID] || h.CreatedAt.IsZero() || h.NeedsManual && h.Task != nil {
			return errors.New("invalid cleanup header")
		}
		if !validAttemptID(h.ID) || !validPrincipal([]byte(h.Principal)) || (h.Binding != "" && !validBinding(h.Binding) || h.Binding == "" && !h.NeedsManual) || (h.Target == "" || len(h.Target) > 128) || h.TaskDigest == ([32]byte{}) || h.Remaining > 20 {
			return errors.New("invalid cleanup header identity")
		}
		seen := map[string]bool{}
		for _, id := range h.ExecutionIDs {
			if !validAttemptID(id) || seen[id] {
				return errors.New("invalid cleanup execution identity")
			}
			seen[id] = true
		}
		if h.ExecutionIDs[0] != h.JobID {
			return errors.New("cleanup first execution identity mismatch")
		}
		id, e := indexID(tx, cleanupHeaderIndexBucket, h.Sequence)
		if e != nil || id != h.ID {
			return errors.New("cleanup header index mismatch")
		}
		if h.Task != nil {
			// A header can outlive its job; decodeCleanupHeader has refused
			// a Task that requests workers.
			d, e := taskDigest(*h.Task)
			if e != nil || d != h.TaskDigest {
				return errors.New("cleanup header task mismatch")
			}
		}
		return nil
	}); e != nil {
		return e
	}
	for _, pair := range []struct{ idx, rows []byte }{{cleanupHeaderIndexBucket, cleanupHeadersBucket}, {cleanupOperationIndexBucket, cleanupOperationsBucket}} {
		if e = retentionTable(tx, pair.idx).ForEach(func(k, v []byte) error {
			if len(k) != 8 || binary.BigEndian.Uint64(k) == 0 || binary.BigEndian.Uint64(k) > retentionTable(tx, pair.idx).Sequence() {
				return errors.New("invalid cleanup sequence")
			}
			var id string
			if e := json.Unmarshal(v, &id); e != nil {
				return e
			}
			if retentionTable(tx, pair.rows).Get([]byte(id)) == nil {
				return errors.New("orphan cleanup index")
			}
			return nil
		}); e != nil {
			return e
		}
	}
	if e = retentionTable(tx, retainedOutputsBucket).ForEach(func(k, v []byte) error {
		var r outputReservation
		if e := json.Unmarshal(v, &r); e != nil {
			return e
		}
		if string(k) != r.AttemptID || r.Generation != 1 || !validAttemptID(r.JobID) || !validAttemptID(r.AttemptID) || !validBinding(r.Binding) || r.Bytes == 0 {
			return errors.New("invalid output reservation")
		}
		if !st.Enabled || !st.Policy.LocalOutputEnabled || r.Bytes != uint64(backend.MaxLogBytes)+4*st.Policy.OutputUnit {
			return errors.New("invalid output reservation byte charge")
		}
		if r.Expired != pendingOutputs[r.AttemptID] {
			return errors.New("output pending cleanup mismatch")
		}
		j, e := get(tx, r.JobID)
		if e == nil {
			found := false
			for _, id := range executionIDs(j) {
				if id == r.AttemptID {
					found = true
				}
			}
			if !found || j.TargetBinding != r.Binding {
				return errors.New("output ownership mismatch")
			}
		} else {
			var h cleanupHeader
			if e = loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(r.JobID), &h); e != nil {
				return errors.New("orphan output reservation")
			}
			found := false
			for _, id := range h.ExecutionIDs {
				if id == r.AttemptID {
					found = true
				}
			}
			if !found || h.Binding != r.Binding {
				return errors.New("cleanup output mismatch")
			}
		}
		return nil
	}); e != nil {
		return e
	}
	return validateRetentionSideRecords(tx, st)
}
func validateScopedSparse(tx *bolt.Tx) error {
	m := tx.Bucket(metadataBucket)
	if !isScoped(tx) {
		if m.Get(scopedKey) != nil {
			return errors.New("invalid scoped mode flag")
		}
		for _, name := range [][]byte{jobPrincipalsBucket, principalRequestsBucket, principalIndexBucket, auditBucket} {
			if tx.Bucket(name) != nil {
				return errors.New("unexpected scoped relations")
			}
		}
		return nil
	}
	if !bytes.Equal(m.Get(scopedKey), []byte{1}) {
		return errors.New("invalid scoped flag")
	}
	owners, requests, indexes := tx.Bucket(jobPrincipalsBucket), tx.Bucket(principalRequestsBucket), tx.Bucket(principalIndexBucket)
	if owners == nil || requests == nil || indexes == nil {
		return errors.New("missing scoped relations")
	}
	indexed := map[string]bool{}
	if e := indexes.ForEach(func(principal, value []byte) error {
		if value != nil || !validPrincipal(principal) {
			return errors.New("invalid scope index")
		}
		b := indexes.Bucket(principal)
		return b.ForEach(func(k, id []byte) error {
			if len(k) != 8 || binary.BigEndian.Uint64(k) == 0 || binary.BigEndian.Uint64(k) > b.Sequence() || indexed[string(id)] || tx.Bucket(jobsBucket).Get(id) == nil || !bytes.Equal(owners.Get(id), principal) {
				return errors.New("invalid sparse scope relation")
			}
			indexed[string(id)] = true
			return nil
		})
	}); e != nil {
		return e
	}
	if e := tx.Bucket(jobsBucket).ForEach(func(id, _ []byte) error {
		if !indexed[string(id)] {
			return errors.New("missing scoped job index")
		}
		return nil
	}); e != nil {
		return e
	}
	if e := owners.ForEach(func(id, _ []byte) error {
		if !indexed[string(id)] {
			return errors.New("orphan principal relation")
		}
		return nil
	}); e != nil {
		return e
	}
	if e := requests.ForEach(func(principal, value []byte) error {
		if value != nil || !validPrincipal(principal) {
			return errors.New("invalid request namespace")
		}
		return requests.Bucket(principal).ForEach(func(k, id []byte) error {
			if len(k) == 0 || !indexed[string(id)] || !bytes.Equal(owners.Get(id), principal) {
				return errors.New("invalid legacy request binding")
			}
			return nil
		})
	}); e != nil {
		return e
	}
	if e := tx.Bucket(requestsBucket).ForEach(func(k, id []byte) error {
		b := requests.Bucket([]byte("legacy"))
		if b == nil || !bytes.Equal(b.Get(k), id) {
			return errors.New("invalid preserved legacy request")
		}
		return nil
	}); e != nil {
		return e
	}
	return validateAudit(tx)
}

func validateRetentionSideRecords(tx *bolt.Tx, st retentionState) error {
	if e := validateLegacyBindings(tx); e != nil {
		return e
	}
	expired := map[string]string{}
	if e := retentionTable(tx, retainedRequestsBucket).ForEach(func(_, v []byte) error {
		var r requestRecord
		if e := json.Unmarshal(v, &r); e != nil {
			return e
		}
		if r.Expired && r.WorkflowID == "" {
			if expired[r.JobID] != "" {
				return errors.New("duplicate expired result binding")
			}
			expired[r.JobID] = r.Principal
		}
		return nil
	}); e != nil {
		return e
	}
	if e := retentionTable(tx, resultWitnessBucket).ForEach(func(k, v []byte) error {
		var principal string
		if e := json.Unmarshal(v, &principal); e != nil {
			return e
		}
		if !validAttemptID(string(k)) || !validPrincipal([]byte(principal)) || expired[string(k)] != principal || tx.Bucket(jobsBucket).Get(k) != nil {
			return errors.New("invalid expired result witness")
		}
		delete(expired, string(k))
		return nil
	}); e != nil {
		return e
	}
	if len(expired) != 0 {
		return errors.New("missing expired result witness")
	}
	if e := retentionTable(tx, retainedClosuresBucket).ForEach(func(k, v []byte) error {
		var c outputClosure
		if e := json.Unmarshal(v, &c); e != nil {
			return e
		}
		ref := c.Reference
		var r outputReservation
		if e := loadJSON(retentionTable(tx, retainedOutputsBucket), k, &r); e != nil {
			return errors.New("orphan output closure")
		}
		if !validAttemptID(string(k)) || ref.Owner != string(tx.Bucket(metadataBucket).Get([]byte("owner"))) || ref.AttemptID != string(k) || ref.JobID != r.JobID || ref.Binding != r.Binding || ref.Generation != r.Generation || ref.Generation != 1 || ref.ClosureID == "" || !validInstance(ref.ClosureID) || c.SubjectDigest == ([32]byte{}) {
			return errors.New("invalid persisted output closure")
		}
		return nil
	}); e != nil {
		return e
	}
	if e := retentionTable(tx, expiredOutputBucket).ForEach(func(k, v []byte) error {
		if !bytes.Equal(v, []byte("true")) || !validAttemptID(string(k)) || retentionTable(tx, retainedOutputsBucket).Get(k) != nil {
			return errors.New("invalid expired output marker")
		}
		jobID := string(k)
		if owner := tx.Bucket(attemptJobsBucket).Get(k); owner != nil {
			jobID = string(owner)
		}
		j, e := get(tx, jobID)
		if e != nil {
			return errors.New("orphan expired output marker")
		}
		found := false
		for _, id := range executionIDs(j) {
			if id == string(k) {
				found = true
			}
		}
		var m retainedJob
		if e = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(jobID), &m); e != nil {
			return e
		}
		if !found || m.Storage != backend.LocalOutput {
			return errors.New("expired output ownership mismatch")
		}
		return nil
	}); e != nil {
		return e
	}
	return nil
}

func validateLegacyBindings(tx *bolt.Tx) error {
	check := func(principal string, b *bolt.Bucket) error {
		return b.ForEach(func(id, jobID []byte) error {
			var m retainedJob
			if e := loadJSON(retentionTable(tx, retainedJobsBucket), jobID, &m); e != nil {
				return e
			}
			if m.LegacyRequestRetired || m.Principal != principal || m.Domain.Epoch != 0 || m.requestID() != string(id) {
				return errors.New("orphan legacy request identity")
			}
			var r requestRecord
			if e := loadJSON(retentionTable(tx, retainedRequestsBucket), requestKey(principal, m.Domain, string(id)), &r); e != nil {
				return e
			}
			if r.JobID != string(jobID) || r.LegacyBytes == 0 {
				return errors.New("uncharged legacy identity relation")
			}
			return nil
		})
	}
	if e := check("legacy", tx.Bucket(requestsBucket)); e != nil {
		return e
	}
	if isScoped(tx) {
		return tx.Bucket(principalRequestsBucket).ForEach(func(p, v []byte) error {
			if v != nil {
				return errors.New("invalid legacy namespace")
			}
			return check(string(p), tx.Bucket(principalRequestsBucket).Bucket(p))
		})
	}
	return nil
}
