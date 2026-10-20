package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) admin(actor auth.Actor) error {
	if e := s.authorize(actor, auth.Mutate); e != nil {
		return e
	}
	if actor.Role() != auth.Admin {
		return auth.ErrPermissionDenied
	}
	return nil
}
func validateSelection(sel PruneSelection) error {
	if sel.Mode > ResultOnly || sel.Limit < 0 || sel.Limit > 100 {
		return errors.New("invalid prune selection")
	}
	if (sel.Mode == ResultOnly) != (sel.Attempt != 0) {
		return errors.New("result-only requires an explicit attempt ordinal")
	}
	if sel.Mode == MetadataOnly != sel.AcknowledgeUnverifiedTerminal {
		return errors.New("metadata-only requires acknowledgement")
	}
	if sel.ObligationID != "" {
		if !validAttemptID(sel.ObligationID) || (sel.Mode != OutputOnly && sel.Mode != ResultOnly) || !sel.Before.IsZero() || sel.After != 0 || sel.Max != 0 || sel.Limit != 0 {
			return errors.New("invalid obligation selector")
		}
	} else if sel.Before.IsZero() {
		return errors.New("prune requires before time")
	}
	return nil
}
func subject(tx *bolt.Tx, id string) (out PruneSubject, err error) {
	j, e := get(tx, id)
	if e == nil {
		out.Job = j
		out.Digest = rowDigest(tx.Bucket(jobsBucket).Get([]byte(id)))
		for _, aid := range executionIDs(j) {
			var r outputReservation
			if e = loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(aid), &r); e == nil && !r.Expired {
				out.Output = append(out.Output, backend.OutputReference{Owner: string(tx.Bucket(metadataBucket).Get([]byte("owner"))), JobID: id, AttemptID: aid, Binding: j.TargetBinding, Generation: r.Generation})
			} else if e != nil && !errors.Is(e, ErrNotFound) {
				return out, e
			}
		}
		return out, nil
	}
	if !errors.Is(e, ErrNotFound) {
		return out, e
	}
	var h cleanupHeader
	if e = loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(id), &h); e != nil {
		return out, e
	}
	if !h.NeedsManual {
		return out, ErrNotFound
	}
	o := obligation(tx, h)
	out.Obligation = &o
	out.Digest = rowDigest(retentionTable(tx, cleanupHeadersBucket).Get([]byte(id)))
	for _, aid := range h.ExecutionIDs {
		var r outputReservation
		if e = loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(aid), &r); e == nil && !r.Expired {
			out.Output = append(out.Output, backend.OutputReference{Owner: string(tx.Bucket(metadataBucket).Get([]byte("owner"))), JobID: h.JobID, AttemptID: aid, Binding: h.Binding, Generation: r.Generation})
		} else if e != nil && !errors.Is(e, ErrNotFound) {
			return out, e
		}
	}
	return out, nil
}
func executionIDs(j backend.Job) []string {
	if len(j.Attempts) == 0 {
		return []string{j.ID}
	}
	ids := make([]string, len(j.Attempts))
	for i, a := range j.Attempts {
		ids[i] = a.ID
	}
	return ids
}
func (s *Store) PruneSubjects(actor auth.Actor, sel PruneSelection) (out []PruneSubject, err error) {
	if err = s.admin(actor); err != nil {
		return
	}
	if err = validateSelection(sel); err != nil {
		return
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		ids, _, _, e := pruneIDs(tx, sel)
		if e != nil {
			return e
		}
		for _, id := range ids {
			x, e := artifactPruneSubject(tx, id, sel.Mode)
			if e != nil {
				return e
			}
			out = append(out, x)
		}
		return nil
	})
	return
}
func pruneIDs(tx *bolt.Tx, sel PruneSelection) (ids []string, after uint64, more bool, err error) {
	if sel.ObligationID != "" {
		return []string{sel.ObligationID}, 0, false, nil
	}
	b := tx.Bucket(sequenceBucket)
	max := sel.Max
	if max == 0 {
		max = b.Sequence()
	}
	if max > b.Sequence() {
		return nil, 0, false, errors.New("prune high-water exceeds sequence")
	}
	if sel.After >= max {
		return nil, sel.After, false, nil
	}
	limit := sel.Limit
	if limit == 0 {
		limit = 100
	}
	c := b.Cursor()
	seen := 0
	for k, id := c.Seek(sequenceKey(sel.After + 1)); k != nil; k, id = c.Next() {
		if len(k) != 8 {
			return nil, 0, false, errors.New("invalid prune index")
		}
		n := binary.BigEndian.Uint64(k)
		if n > max {
			break
		}
		if seen == 200 || len(ids) == limit {
			more = true
			break
		}
		after = n
		seen++
		ids = append(ids, string(id))
	}
	return
}
func (s *Store) RecordOutputClosure(ref backend.OutputReference, expected [32]byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if !st.Enabled {
			return ErrRetentionDisabled
		}
		x, e := subject(tx, ref.JobID)
		if e != nil {
			return e
		}
		if x.Digest != expected {
			return errors.New("retention subject changed")
		}
		if ref.Owner != s.owner || ref.ClosureID == "" || !validInstance(ref.ClosureID) {
			return errors.New("invalid output closure")
		}
		found := false
		for _, want := range x.Output {
			if want.Owner == ref.Owner && want.JobID == ref.JobID && want.AttemptID == ref.AttemptID && want.Binding == ref.Binding && want.Generation == ref.Generation {
				found = true
			}
		}
		if !found {
			return errors.New("output closure identity mismatch")
		}
		return putRetentionRecord(tx, retainedClosuresBucket, []byte(ref.AttemptID), outputClosure{Reference: ref, SubjectDigest: expected})
	})
}
func closureFor(tx *bolt.Tx, x PruneSubject, aid string, ephemeral []backend.OutputReference) (backend.OutputReference, bool) {
	var c outputClosure
	if loadJSON(retentionTable(tx, retainedClosuresBucket), []byte(aid), &c) == nil && c.SubjectDigest == x.Digest {
		return c.Reference, true
	}
	for _, r := range ephemeral {
		for _, want := range x.Output {
			if r.AttemptID == aid && r.Owner == want.Owner && r.JobID == want.JobID && r.Binding == want.Binding && r.Generation == want.Generation && r.ClosureID != "" && validInstance(r.ClosureID) {
				return r, true
			}
		}
	}
	return backend.OutputReference{}, false
}
func safeStatuses(j backend.Job) bool {
	if len(j.Attempts) == 0 {
		return j.Status.Termination == backend.StoppedWithExitCode && j.Status.Instance != ""
	}
	for _, a := range j.Attempts {
		if a.StartingAt == nil && a.Status.State == backend.Canceled {
			continue
		}
		if a.Status.Termination != backend.StoppedWithExitCode || a.Status.Instance == "" {
			return false
		}
	}
	return true
}
func classify(tx *bolt.Tx, x PruneSubject, sel PruneSelection, blocked map[string]bool, proofs []backend.OutputReference) (string, []backend.OutputReference) {
	return classifyQualified(tx, x, sel, blocked, proofs, false)
}
func classifyQualified(tx *bolt.Tx, x PruneSubject, sel PruneSelection, blocked map[string]bool, proofs []backend.OutputReference, workspaceRetired bool) (string, []backend.OutputReference) {
	if x.Obligation != nil {
		for _, id := range x.Obligation.ExecutionIDs {
			if blocked[id] {
				return "operation_pending", nil
			}
		}
		var refs []backend.OutputReference
		for _, r := range x.Output {
			c, ok := closureFor(tx, x, r.AttemptID, proofs)
			if !ok {
				return "proof_required", nil
			}
			refs = append(refs, c)
		}
		if len(refs) == 0 {
			return "already_expired", nil
		}
		return "", refs
	}
	j := x.Job
	for _, id := range executionIDs(j) {
		if blocked[id] {
			return "operation_pending", nil
		}
	}
	if !j.UpdatedAt.Before(sel.Before) {
		return "below_age", nil
	}
	if sel.Mode != OutputOnly {
		switch j.Status.State {
		case backend.Unknown:
			return "unknown", nil
		case backend.Retrying:
			return "retry_backoff", nil
		case backend.Queued, backend.Starting, backend.Running:
			return "active", nil
		}
	}
	var m retainedJob
	if loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &m) != nil {
		return "unavailable", nil
	}
	neverStarted := !m.Legacy && !m.Started
	if sel.Mode == Whole && !neverStarted && !safeStatuses(j) && !workspaceRetired {
		return "proof_required", nil
	}
	if retentionTable(tx, cleanupHeadersBucket).Get([]byte(j.ID)) != nil {
		return "operation_pending", nil
	}
	var refs []backend.OutputReference
	for _, r := range x.Output {
		if sel.Mode == OutputOnly && r.AttemptID == j.CurrentAttemptID() && (!j.Status.State.Terminal() || j.Status.State == backend.Unknown) {
			continue
		}
		c, ok := closureFor(tx, x, r.AttemptID, proofs)
		if !ok {
			return "proof_required", nil
		}
		if sel.Mode == MetadataOnly && !c.PayloadReleased {
			return "payload_in_use", nil
		}
		refs = append(refs, c)
	}
	if sel.Mode == OutputOnly && len(refs) == 0 {
		return "current_output", nil
	}
	return "", refs
}
func addSkip(p *PrunePage, reason string) {
	p.Skipped++
	for i := range p.Skips {
		if p.Skips[i].Reason == reason {
			p.Skips[i].Count++
			return
		}
	}
	p.Skips = append(p.Skips, PruneSkip{reason, 1})
}
func (s *Store) Prune(actor auth.Actor, sel PruneSelection, execute bool, blocked []string, ephemeral []backend.OutputReference, now time.Time) (out PrunePage, err error) {
	if err = s.admin(actor); err != nil {
		return
	}
	if err = validateSelection(sel); err != nil {
		return
	}
	if len(blocked) > 64 || len(ephemeral) > 100 {
		return out, errors.New("retention page input exceeds limit")
	}
	if execute && len(ephemeral) != 0 {
		return out, errors.New("execute cannot use ephemeral proof")
	}
	if now.IsZero() {
		return out, errors.New("retention time is zero")
	}
	blockedSet := map[string]bool{}
	for _, id := range blocked {
		blockedSet[id] = true
	}
	pool := make([]string, 100)
	if execute {
		for i := range pool {
			pool[i], err = newID()
			if err != nil {
				return
			}
		}
	} else {
		for i := range pool {
			pool[i] = fmt.Sprintf("%032x", i+1)
		}
	}
	apply := func(tx *bolt.Tx) error {
		st, e := loadRetention(tx)
		if e != nil {
			return e
		}
		if !st.Enabled && sel.Mode != ResultOnly {
			return ErrRetentionDisabled
		}
		if sel.Mode == ResultOnly && tx.Bucket(artifactsBucket) == nil {
			return ErrArtifactsDisabled
		}
		plan, e := newRetentionMutation(tx)
		if e != nil {
			return e
		}
		var artifactInitial, artifactProjected *artifactState
		if tx.Bucket(artifactsBucket) != nil {
			artifactInitial, e = artifactLoad(tx)
			if e != nil {
				return e
			}
			artifactProjected = artifactInitial.clone()
		}
		var artifactFinal artifactPrepared
		ids, after, more, e := pruneIDs(tx, sel)
		if e != nil {
			return e
		}
		out.After = after
		out.More = more
		count := 0
		for _, id := range ids {
			out.Inspected++
			x, e := artifactPruneSubject(tx, id, sel.Mode)
			if e != nil {
				return e
			}
			artifactTrial := artifactProjected
			if artifactProjected != nil {
				artifactTrial = artifactProjected.clone()
			}
			if sel.Mode == ResultOnly {
				reason, e := artifactResultSelection(tx, artifactTrial, x, sel, blockedSet, now)
				if e != nil {
					return e
				}
				if reason != "" {
					addSkip(&out, reason)
					continue
				}
				n, e := plan.changedBytes()
				if e != nil {
					return e
				}
				prepared, e := prepareArtifactChange(tx, artifactInitial, artifactTrial, plan, maxChangeBytes-n)
				if e != nil {
					var capacity *ArtifactCapacityError
					if errors.As(e, &capacity) || errors.Is(e, errArtifactMutationBudget) {
						addSkip(&out, "artifact_full")
						continue
					}
					return e
				}
				if count+prepared.newRecords > 100 {
					addSkip(&out, "cleanup_full")
					continue
				}
				artifactProjected = artifactTrial
				artifactFinal = prepared
				out.Selected++
				if execute {
					out.Marked++
				}
				continue
			}
			workspaceRetired := artifactWholeReady(artifactProjected, x.Job)
			if sel.Mode == Whole && x.Job.Task.Workdir != nil && !workspaceRetired {
				var tracked retainedJob
				if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(x.Job.ID), &tracked); e != nil {
					return e
				}
				if tracked.Started {
					addSkip(&out, "workspace_cleanup_pending")
					continue
				}
			}
			reason, refs := classifyQualified(tx, x, sel, blockedSet, ephemeral, workspaceRetired)
			if reason != "" {
				addSkip(&out, reason)
				continue
			}
			needed := len(refs)
			if sel.Mode == MetadataOnly {
				needed = 0
			}
			if sel.Mode == Whole && x.Job.Task.Workdir == nil {
				if len(x.Job.Attempts) == 0 {
					if safeStatuses(x.Job) {
						needed++
					}
				} else {
					for _, a := range x.Job.Attempts {
						if a.StartingAt != nil {
							needed++
						}
					}
				}
			}
			if retentionTable(tx, cleanupHeadersBucket).Get([]byte(id)) == nil {
				needed++
			}
			if count+needed > 100 {
				addSkip(&out, "cleanup_full")
				continue
			}
			h, ops, e := makeCleanup(tx, x, sel, refs, pool[count:count+needed], now)
			if e != nil {
				return e
			}
			trial := plan.fork()
			if e = markPrune(trial, x, sel, h, ops, refs); e != nil {
				return e
			}
			if e = trial.validate(); e != nil {
				var cap *CapacityError
				if errors.As(e, &cap) {
					reason := "metadata_full"
					if cap.Domain == CleanupBytesQuota || cap.Domain == CleanupRecordsQuota {
						reason = "cleanup_full"
					}
					addSkip(&out, reason)
					continue
				}
				return e
			}
			n, e := trial.changedBytes()
			if e != nil {
				return e
			}
			if n > maxChangeBytes {
				addSkip(&out, "cleanup_full")
				continue
			}
			if artifactTrial != nil {
				if sel.Mode == Whole {
					if e := artifactWholePrune(artifactTrial, x.Job, now); e != nil {
						if errors.Is(e, ErrArtifactConflict) {
							addSkip(&out, "artifact_in_use")
							continue
						}
						return e
					}
				} else if sel.Mode == MetadataOnly {
					if e := artifactTransferInputPlanned(artifactTrial, trial, x.Job.ID); e != nil {
						return e
					}
				}
				prepared, e := prepareArtifactChange(tx, artifactInitial, artifactTrial, trial, maxChangeBytes-n)
				if e != nil {
					var capacity *ArtifactCapacityError
					if errors.As(e, &capacity) || errors.Is(e, errArtifactMutationBudget) {
						addSkip(&out, "artifact_full")
						continue
					}
					return e
				}
				if count+needed+prepared.newRecords > 100 {
					addSkip(&out, "cleanup_full")
					continue
				}
				artifactProjected = artifactTrial
				artifactFinal = prepared
			}
			plan = trial
			count += needed
			out.Selected++
			if execute {
				out.Marked++
			}
		}
		for q := JobsQuota; q <= PrincipalScopesQuota; q++ {
			if plan.initial[q] != plan.used[q] {
				x := ChargeTransfer{Domain: q}
				if plan.initial[q] > plan.used[q] {
					x.Released = plan.initial[q] - plan.used[q]
				} else {
					x.Added = plan.used[q] - plan.initial[q]
				}
				out.Transfers = append(out.Transfers, x)
			}
		}
		sortedSkip(&out)
		if execute && out.Marked > 0 {
			if e := plan.commit(); e != nil {
				return e
			}
			if artifactProjected != nil {
				if e := artifactFinal.apply(tx); e != nil {
					return e
				}
			}
			return nil
		}
		return nil
	}
	if execute {
		err = s.db.Update(apply)
	} else {
		err = s.db.View(apply)
	}
	return
}

func makeCleanup(tx *bolt.Tx, x PruneSubject, sel PruneSelection, refs []backend.OutputReference, ids []string, now time.Time) (h cleanupHeader, ops []cleanupOperation, err error) {
	if x.Obligation != nil {
		if err = loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(x.Obligation.ID), &h); err != nil {
			return
		}
	} else {
		var m retainedJob
		if err = loadJSON(retentionTable(tx, retainedJobsBucket), []byte(x.Job.ID), &m); err != nil {
			return
		}
		d, e := taskDigest(x.Job.Task)
		if e != nil {
			err = e
			return
		}
		h = cleanupHeader{ID: x.Job.ID, JobID: x.Job.ID, Principal: m.Principal, Target: x.Job.Target, Binding: x.Job.TargetBinding, TaskDigest: d, ExecutionIDs: executionIDs(x.Job), NeedsManual: sel.Mode == MetadataOnly, CreatedAt: now}
	}
	if sel.Mode != MetadataOnly {
		for _, r := range refs {
			ops = append(ops, cleanupOperation{ID: ids[len(ops)], HeaderID: h.ID, Operation: OutputCleanup, AttemptID: r.AttemptID, ClosureID: r.ClosureID, Generation: r.Generation, PayloadReleased: r.PayloadReleased, UpdatedAt: now})
		}
	}
	if sel.Mode == Whole && x.Job.Task.Workdir == nil {
		add := func(id string, status backend.Status) {
			proof := status
			proof.Reason = ""
			if status.Termination != backend.StoppedWithExitCode || status.Instance == "" {
				return
			}
			ops = append(ops, cleanupOperation{ID: ids[len(ops)], HeaderID: h.ID, Operation: NativeCleanup, AttemptID: id, Stopped: true, Proof: &proof, UpdatedAt: now})
		}
		if len(x.Job.Attempts) == 0 {
			add(x.Job.ID, x.Job.Status)
		} else {
			for _, a := range x.Job.Attempts {
				if a.StartingAt != nil {
					add(a.ID, a.Status)
				}
			}
		}
		if len(ops) > len(refs) {
			task := x.Job.Task
			h.Task = &task
		}
	}
	for _, op := range ops {
		if op.Operation == OutputCleanup {
			h.OutputRemaining++
		} else {
			h.NativeRemaining++
		}
	}
	h.Remaining += uint32(len(ops))
	return
}
func markPrune(m *retentionMutation, x PruneSubject, sel PruneSelection, h cleanupHeader, ops []cleanupOperation, refs []backend.OutputReference) error {
	tx := m.tx
	headers := retentionTable(tx, cleanupHeadersBucket)
	if headers.Get([]byte(h.ID)) == nil {

		n, e := m.next(cleanupHeaderIndexBucket)
		if e != nil || n == 0 {
			return errors.New("cleanup header sequence exhausted")
		}
		h.Sequence = n
		if e = m.put(cleanupHeaderIndexBucket, sequenceKey(n), h.ID); e != nil {
			return e
		}
	}
	for i := range ops {

		n, e := m.next(cleanupOperationIndexBucket)
		if e != nil || n == 0 {
			return errors.New("cleanup operation sequence exhausted")
		}
		ops[i].Sequence = n
		if m.get(retentionKey{true, string(cleanupOperationsBucket), ops[i].ID}) != nil {
			return errors.New("cleanup identity collision")
		}
		if e = m.put(cleanupOperationsBucket, []byte(ops[i].ID), ops[i]); e != nil {
			return e
		}
		if e = m.put(cleanupOperationIndexBucket, sequenceKey(n), ops[i].ID); e != nil {
			return e
		}
	}
	if e := m.put(cleanupHeadersBucket, []byte(h.ID), h); e != nil {
		return e
	}
	if sel.Mode != MetadataOnly {
		for _, ref := range refs {
			var r outputReservation
			if e := loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(ref.AttemptID), &r); e != nil {
				return e
			}
			r.Expired = true
			if e := m.put(retainedOutputsBucket, []byte(r.AttemptID), r); e != nil {
				return e
			}
		}
	}
	if sel.Mode == OutputOnly {
		return nil
	}
	j := x.Job
	var meta retainedJob
	if e := loadJSON(retentionTable(tx, retainedJobsBucket), []byte(j.ID), &meta); e != nil {
		return e
	}
	st, e := loadRetention(tx)
	if e != nil {
		return e
	}
	if !validJobRequestIdentity(st, meta) {
		return errors.New("invalid pruned request identity")
	}
	if !meta.LegacyRequestRetired {
		key := requestKey(meta.Principal, meta.Domain, meta.requestID())
		var req requestRecord
		if e := loadJSON(retentionTable(tx, retainedRequestsBucket), key, &req); e == nil {
			if e = m.dropLegacy(&req); e != nil {
				return e
			}
			req.Expired = true
			if e = m.put(resultWitnessBucket, []byte(j.ID), req.Principal); e != nil {
				return e
			}
			if e = m.put(retainedRequestsBucket, key, req); e != nil {
				return e
			}
		} else if !errors.Is(e, ErrNotFound) {
			return e
		}
	}
	if e := m.coreDelete(sequenceBucket, sequenceKey(j.Sequence)); e != nil {
		return e
	}
	if isScoped(tx) {
		scopeName := []byte(string(principalIndexBucket) + "/" + meta.Principal)
		if e := m.coreDelete(scopeName, sequenceKey(meta.Ordinal)); e != nil {
			return e
		}
		if e := m.coreDelete(jobPrincipalsBucket, []byte(j.ID)); e != nil {
			return e
		}
	}
	for _, aid := range executionIDs(j) {
		if e := m.drop(expiredOutputBucket, []byte(aid)); e != nil {
			return e
		}

	}
	for _, a := range j.Attempts {
		if e := m.coreDelete(attemptJobsBucket, []byte(a.ID)); e != nil {
			return e
		}
	}
	task, e := backend.TaskBytes(j.Task)
	if e != nil {
		return e
	}
	for _, x := range []struct {
		q QuotaDomain
		n uint64
	}{{JobsQuota, 1}, {TaskBytesQuota, uint64(len(task))}, {RetryBytesQuota, retryCharge(j)}, {MetadataBytesQuota, meta.Overhead}, {MetadataBytesQuota, jobRelationBytes(j, meta.Principal, isScoped(tx))}} {
		if e = m.adjust(x.q, 0, x.n); e != nil {
			return e
		}
	}
	for _, x := range []struct {
		k []byte
		n uint64
	}{{taskBytesKey, uint64(len(task))}, {retryBytesKey, retryCharge(j)}} {
		old := binary.BigEndian.Uint64(m.coreGet(metadataBucket, x.k))
		if old < x.n {
			return errors.New("core byte counter underflow")
		}
		if e = func() error { m.corePut(metadataBucket, x.k, sequenceKey(old-x.n)); return nil }(); e != nil {
			return e
		}
	}
	if e = m.drop(retainedJobsBucket, []byte(j.ID)); e != nil {
		return e
	}
	if e = m.coreDelete(jobsBucket, []byte(j.ID)); e != nil {
		return e
	}
	if h.Remaining == 0 && !h.NeedsManual {
		return m.removeHeader(h)
	}
	return nil
}
func removeHeader(tx *bolt.Tx, h cleanupHeader) error {
	if e := deleteRetentionRecord(tx, cleanupHeaderIndexBucket, sequenceKey(h.Sequence)); e != nil {
		return e
	}
	return deleteRetentionRecord(tx, cleanupHeadersBucket, []byte(h.ID))
}
func obligation(tx *bolt.Tx, h cleanupHeader) Obligation {
	o := Obligation{Reason: "cleanup_pending", OutputPending: h.OutputRemaining > 0, ExecutionPending: h.NeedsManual || h.NativeRemaining > 0, ID: h.ID, JobID: h.JobID, Target: h.Target, Binding: h.Binding, ExecutionIDs: append([]string(nil), h.ExecutionIDs...), NeedsManual: h.NeedsManual, CreatedAt: h.CreatedAt}
	for _, id := range h.ExecutionIDs {
		var r outputReservation
		if loadJSON(retentionTable(tx, retainedOutputsBucket), []byte(id), &r) == nil {
			o.OutputBytes += r.Bytes
		}
	}
	if h.NeedsManual {
		o.Reason = "manual_reconciliation_required"
	}
	if o.OutputBytes > 0 {
		o.OutputPending = true
	}
	return o
}
func (s *Store) ListObligations(actor auth.Actor, after, max uint64, limit int) (out ObligationPage, err error) {
	if err = s.admin(actor); err != nil {
		return
	}
	if limit < 1 || limit > 100 {
		return out, errors.New("obligation page outside 1..100")
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		b := retentionTable(tx, cleanupHeaderIndexBucket)
		if max == 0 {
			max = b.Sequence()
		}
		if max > b.Sequence() {
			return errors.New("obligation high-water invalid")
		}
		out.Max = max
		c := b.Cursor()
		for k, v := c.Seek(sequenceKey(after + 1)); k != nil; k, v = c.Next() {
			n := binary.BigEndian.Uint64(k)
			if n > max {
				break
			}
			if len(out.Items) == limit {
				out.More = true
				break
			}
			var id string
			if e := json.Unmarshal(v, &id); e != nil {
				return e
			}
			var h cleanupHeader
			if e := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(id), &h); e != nil {
				return e
			}
			out.Items = append(out.Items, obligation(tx, h))
			out.After = n
		}
		return nil
	})
	return
}
func sortedSkip(p *PrunePage) {
	sort.Slice(p.Skips, func(i, j int) bool { return p.Skips[i].Reason < p.Skips[j].Reason })
}
func cleanupDigest(op cleanupOperation) [32]byte {
	op.LastError = ""
	op.UpdatedAt = time.Time{}
	b, _ := json.Marshal(op)
	return sha256.Sum256(b)
}
