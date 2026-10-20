package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// artifactWholeReady uses only the prior trusted completion/root retirement
// record. Terminal state or a missing workspace alone is never this proof.
func artifactWholeReady(st *artifactState, j backend.Job) bool {
	if j.Task.Workdir == nil {
		return false
	}
	if st == nil {
		return false
	}
	for _, w := range st.Workspaces {
		if w.JobID == j.ID {
			return false
		}
	}
	if len(j.Attempts) == 0 {
		c, ok := st.Completions[j.ID]
		return ok && c.JobID == j.ID && c.RootRemoved
	}
	for _, a := range j.Attempts {
		if a.StartingAt == nil {
			continue
		}
		c, ok := st.Completions[a.ID]
		if !ok || c.JobID != j.ID || !c.RootRemoved {
			return false
		}
	}
	return true
}

func artifactTransferInputPlanned(st *artifactState, plan *retentionMutation, job string) error {
	p, ok := st.JobInputs[job]
	if !ok {
		return nil
	}
	var h cleanupHeader
	raw := plan.get(retentionKey{true, string(cleanupHeadersBucket), job})
	if raw == nil {
		return ErrArtifactConflict
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		return err
	}
	if h.ID != job || h.JobID != job || h.Principal != p.Owner || !h.NeedsManual || h.Task != nil || h.TaskDigest != p.TaskDigest {
		return ErrArtifactConflict
	}
	if _, exists := st.ManualInputs[job]; exists {
		return ErrArtifactConflict
	}
	st.ManualInputs[job] = p
	delete(st.JobInputs, job)
	return nil
}

func artifactWholePrune(st *artifactState, j backend.Job, now time.Time) error {
	for _, w := range st.Workspaces {
		if w.JobID == j.ID {
			return ErrArtifactConflict
		}
	}
	// Every selected result is checked against all concrete consumers before
	// dropping input protection. A self-consumer is not silently forgiven.
	for _, r := range st.Results {
		if r.JobID == j.ID {
			if err := artifactExpireResult(st, r.Owner, j.ID, r.AttemptID, now); err != nil {
				return err
			}
		}
	}
	return artifactDropInput(st, j.ID, now)
}

func artifactResultSelection(tx *bolt.Tx, st *artifactState, x PruneSubject, sel PruneSelection, blocked map[string]bool, now time.Time) (string, error) {
	if st == nil {
		return "artifacts_disabled", nil
	}
	job := x.Job.ID
	var attempt, owner string
	if x.Obligation != nil {
		h := cleanupHeader{}
		if err := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(x.Obligation.ID), &h); err != nil {
			return "", err
		}
		job = h.JobID
		owner = h.Principal
		// The taskless header preserves the immutable execution order. Default
		// jobs have one entry, retry jobs preserve their original history order.
		if sel.Attempt == 0 || uint64(sel.Attempt) > uint64(len(h.ExecutionIDs)) {
			return "result_absent", nil
		}
		attempt = h.ExecutionIDs[sel.Attempt-1]
	} else {
		if !x.Job.UpdatedAt.Before(sel.Before) {
			return "below_age", nil
		}
		owner = "legacy"
		if isScoped(tx) {
			owner = string(tx.Bucket(jobPrincipalsBucket).Get([]byte(job)))
		}
		if len(x.Job.Attempts) == 0 {
			if sel.Attempt != 1 {
				return "result_absent", nil
			}
			attempt = job
		} else {
			for ordinal, a := range x.Job.Attempts {
				if uint32(ordinal+1) == sel.Attempt {
					attempt = a.ID
					break
				}
			}
		}
	}
	if attempt == "" {
		return "result_absent", nil
	}
	if blocked[attempt] {
		return "operation_pending", nil
	}
	if _, ok := st.Expired[attempt]; ok {
		return "already_expired", nil
	}
	r, ok := st.Results[attempt]
	if !ok || r.JobID != job || r.Owner != owner {
		return "result_absent", nil
	}
	if err := artifactExpireResult(st, owner, job, attempt, now); err != nil {
		if errors.Is(err, ErrArtifactConflict) {
			return "result_in_use", nil
		}
		return "", err
	}
	return "", nil
}

// artifactPruneNeedsSubject is deliberately separate from output-log closure
// discovery. ResultOnly never asks a backend to prove or remove log resources.
func artifactPruneSubject(tx *bolt.Tx, id string, mode PruneMode) (PruneSubject, error) {
	if mode != ResultOnly {
		return subject(tx, id)
	}
	var out PruneSubject
	j, err := get(tx, id)
	if err == nil {
		out.Job = j
		out.Digest = rowDigest(tx.Bucket(jobsBucket).Get([]byte(id)))
		return out, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return out, err
	}
	var h cleanupHeader
	if err := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(id), &h); err != nil {
		return out, err
	}
	if !h.NeedsManual || h.Task != nil || len(h.ExecutionIDs) == 0 {
		return out, ErrNotFound
	}
	o := obligation(tx, h)
	out.Obligation = &o
	out.Digest = rowDigest(retentionTable(tx, cleanupHeadersBucket).Get([]byte(id)))
	return out, nil
}
