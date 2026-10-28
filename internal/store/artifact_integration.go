package store

import (
	"errors"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// ErrWorkspaceSubmitRequired rejects snapshot fields at a legacy submit boundary.
var ErrWorkspaceSubmitRequired = errors.New("workspace task requires workspace submission")

// errWorkerRoute refuses worker execution on an ordinary route: a dedicated
// worker group's job, or a stored task that requests workers.
var errWorkerRoute = errors.New("worker task requires a worker route")

func artifactDiagnostic(s string) string {
	s = boundedReason(s)
	if len(s) > 128 {
		s = s[:128]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}

func hasWorkspaceFields(task backend.Task) bool {
	return task.Workdir != nil || len(task.Setup) != 0 || len(task.Outputs) != 0
}

func artifactTaskRef(task backend.Task) (ArtifactRef, error) {
	if task.Workdir == nil {
		return ArtifactRef{}, errors.New("workspace requires workdir")
	}
	if err := backend.ValidateWorkspace(task); err != nil {
		return ArtifactRef{}, err
	}
	// Canonical Ref uses signed wire sizes. Validate BEFORE converting; a
	// negative value must never become a large unsigned reservation.
	r := task.Workdir
	return ArtifactRef{ID: r.ID, SHA256: r.SHA256, Size: uint64(r.Size)}, nil
}

// SubmitWorkspace accepts a snapshot task using the existing request protocol.
// A nil domain selects disabled retention; a nonnil domain selects the retained
// protocol. Both paths replay before current input/target availability checks.
// Control must check real fresh authority and selected-target capability first
// on a lookup MISS. Store accepts no capability claim from a wire request.
func (s *Store) SubmitWorkspace(actor auth.Actor, domain *RequestDomain, requestID string, task backend.Task, target, binding string, storage backend.OutputStorage, reserve uint64) (backend.Job, error) {
	task = backend.CloneTask(task)
	if domain == nil {
		return s.submit(actor, requestID, task, target, binding, submissionWorkspace, nil)
	}
	return s.submitRetained(actor, *domain, requestID, task, target, binding, storage, reserve, submissionWorkspace, nil)
}

type submissionKind uint8

const (
	submissionOrdinary submissionKind = iota
	submissionWorkspace
	submissionRemote
)

func validateSubmitWorkspace(task backend.Task, kind submissionKind) error {
	if kind == submissionRemote {
		return backend.ValidateDataTask(task, nil)
	}
	if len(task.DataMounts) != 0 {
		return ErrRemoteAttemptRequired
	}
	if kind == submissionOrdinary {
		if hasWorkspaceFields(task) {
			return ErrWorkspaceSubmitRequired
		}
		return nil
	}
	_, err := artifactTaskRef(task)
	return err
}

// The reverse walk belongs to snapshot validation, not every upload chunk.
// Acceptance writes the new core row and its exact pin in one transaction.
func artifactValidateTaskRows(tx *bolt.Tx, st *artifactState) error {
	if st != nil {
		if e := remoteValidateCore(tx, st, nil); e != nil {
			return e
		}
	}
	return tx.Bucket(jobsBucket).ForEach(func(id, raw []byte) error {
		var job backend.Job
		if err := decodeStoredJob(raw, &job); err != nil {
			return err
		}
		if err := backend.ValidateWorkspace(job.Task); err != nil {
			return err
		}
		if job.Task.Workdir == nil {
			return nil
		}
		if st == nil {
			return ErrArtifactsDisabled
		}
		pin, ok := st.JobInputs[string(id)]
		ref, err := artifactTaskRef(job.Task)
		if !ok || err != nil || pin.JobID != job.ID || pin.Ref != ref {
			return artifactCorrupt()
		}
		return nil
	})
}
