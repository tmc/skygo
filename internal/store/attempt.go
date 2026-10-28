package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

const maxRetryBytes = 16 << 20
const maxAttemptBytes = 8192

var attemptJobsBucket = []byte("attempt_jobs")
var retryBytesKey = []byte("retry_bytes")

func schemaNumber(tx *bolt.Tx) uint64 {
	if m := tx.Bucket(metadataBucket); m != nil {
		if b := m.Get(schemaKey); len(b) == 8 {
			return binary.BigEndian.Uint64(b)
		}
	}
	return 0
}

func acceptAttempts(tx *bolt.Tx, j *backend.Job) error {
	if j.Task.Retry == nil {
		return nil
	}
	m := tx.Bucket(metadataBucket)
	raw := m.Get(retryBytesKey)
	if len(raw) != 8 {
		return errors.New("invalid retry byte counter")
	}
	used := binary.BigEndian.Uint64(raw)
	reserve := uint64(j.Task.Retry.MaxAttempts) * maxAttemptBytes
	if used > maxRetryBytes {
		return ErrFull
	}
	if reserve > maxRetryBytes-used {
		st, err := loadRetention(tx)
		if err != nil {
			return err
		}
		if st.Enabled {
			return &CapacityError{Domain: RetryBytesQuota, Used: used, Limit: st.Policy.RetryBytes}
		}
		return ErrFull
	}
	j.Attempts = []backend.Attempt{{ID: j.ID, Status: j.Status, CreatedAt: j.CreatedAt}}
	if err := tx.Bucket(attemptJobsBucket).Put([]byte(j.ID), []byte(j.ID)); err != nil {
		return err
	}
	return m.Put(retryBytesKey, sequenceKey(used+reserve))
}

// BeginAttempt commits launch intent once for the expected execution. A stale
// expected ID returns the current job unchanged. Cancellation of queued or
// waiting work settles before checking its retry deadline and allocates no ID.
// The caller must check placement and execution capacity before uncanceled calls.
func (s *Store) BeginAttempt(jobID, expectedAttemptID string, outputReserve uint64, now time.Time) (j backend.Job, start bool, err error) {
	candidate, err := newID()
	if err != nil {
		return j, false, err
	}
	resultCandidate, err := newID()
	if err != nil {
		return j, false, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		j, start, e = beginAttempt(tx, jobID, expectedAttemptID, candidate, resultCandidate, outputReserve, now.UTC())
		return e
	})
	if err != nil {
		start = false
	}
	return
}

func beginAttempt(tx *bolt.Tx, id, expected, candidate, resultCandidate string, outputReserve uint64, now time.Time) (j backend.Job, start bool, err error) {
	j, err = get(tx, id)
	if err == nil && remoteRoute(tx, id) {
		return j, false, ErrRemoteAttemptRequired
	}
	if err != nil || j.CurrentAttemptID() != expected {
		return
	}
	if j.Task.Workdir == nil || j.CancelRequested || now.IsZero() || j.Status.State != backend.Queued && j.Status.State != backend.Retrying || j.Status.State == backend.Retrying && (j.NextAttemptAt == nil || now.Before(*j.NextAttemptAt)) {
		return beginAttemptCore(tx, id, expected, candidate, outputReserve, now)
	}
	_, err = artifactCoreChange(tx, artifactAttemptLedger(tx, id, candidate), maxChangeBytes, 100, func(st *artifactState) error {
		if used := artifactCompletionCount(st); used >= 1024 {
			return &ArtifactCapacityError{Domain: "completion records", Used: uint64(used), Limit: 1024}
		}
		var e error
		j, start, e = beginAttemptCore(tx, id, expected, candidate, outputReserve, now)
		if e != nil || !start {
			return e
		}
		resultID := ""
		if len(j.Task.Outputs) != 0 {
			resultID = resultCandidate
		}
		return artifactBeginWorkspace(tx, st, id, j.CurrentAttemptID(), resultID, len(j.Task.Outputs) != 0, now)
	})
	if err != nil {
		start = false
	}
	return
}

func beginAttemptCore(tx *bolt.Tx, id, expected, candidate string, outputReserve uint64, now time.Time) (j backend.Job, start bool, err error) {
	j, err = get(tx, id)
	if err != nil || j.CurrentAttemptID() != expected {
		return
	}
	// get has refused a stored task that requests workers, before the
	// cancellation and terminal branches, so such a row is never changed here.
	// A worker group's task has no workers; its typed schema10 relation, not
	// the task, marks it. Only worker group transitions may start it.
	if workerGroupJob(tx, id) {
		return j, false, errWorkerRoute
	}
	if j.Status.State != backend.Queued && j.Status.State != backend.Retrying {
		return
	}
	if now.IsZero() {
		return j, false, errors.New("attempt time must be nonzero")
	}
	if j.CancelRequested {
		if j.Status.State == backend.Queued && len(j.Attempts) > 0 {
			a := &j.Attempts[len(j.Attempts)-1]
			a.Status = backend.Status{State: backend.Canceled, Reason: "canceled before execution"}
			a.FinishedAt = &now
		}
		j.Status = backend.Status{State: backend.Canceled, Reason: "canceled before execution"}
		j.NextAttemptAt = nil
	} else if j.Status.State == backend.Retrying {
		if j.NextAttemptAt == nil {
			return j, false, errors.New("retry has no deadline")
		}
		if now.Before(*j.NextAttemptAt) {
			return
		}
		if j.Task.Retry == nil || len(j.Attempts) >= int(j.Task.Retry.MaxAttempts) {
			return j, false, errors.New("attempt budget exhausted")
		}
		if !validAttemptID(candidate) || tx.Bucket(jobsBucket).Get([]byte(candidate)) != nil || tx.Bucket(attemptJobsBucket).Get([]byte(candidate)) != nil {
			return j, false, errors.New("random attempt identity collision")
		}
		j.Status = backend.Status{State: backend.Starting}
		j.Attempts = append(j.Attempts, backend.Attempt{ID: candidate, Status: j.Status, CreatedAt: now, StartingAt: &now})
		j.NextAttemptAt = nil
		if err = adjust(tx, MetadataBytesQuota, 64, 0); err != nil {
			return j, false, err
		}
		if err = tx.Bucket(attemptJobsBucket).Put([]byte(candidate), []byte(id)); err != nil {
			return j, false, err
		}
		start = true
	} else {
		j.Status = backend.Status{State: backend.Starting}
		if len(j.Attempts) > 0 {
			a := &j.Attempts[len(j.Attempts)-1]
			a.Status = j.Status
			a.StartingAt = &now
		}
		start = true
	}
	if start {
		if err = reserveOutput(tx, j, outputReserve); err != nil {
			return j, false, err
		}
	}
	j.UpdatedAt = now
	err = putAttemptJob(tx, j)
	if err == nil {
		j, err = get(tx, id)
	}
	return
}

// ObserveAttempt records an observation for exactly the expected current attempt.
// Positive stopped evidence and a selected nonzero exit code may schedule one
// fixed retry deadline. Duplicate, stale and already terminal observations do
// not change history or deadlines. A pinned instance is never erased or rebound.
func (s *Store) ObserveAttempt(jobID, expectedAttemptID string, status backend.Status, now time.Time) (j backend.Job, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		var e error
		j, e = observeAttempt(tx, jobID, expectedAttemptID, status, now.UTC())
		return e
	})
	return
}

func observeAttempt(tx *bolt.Tx, id, expected string, status backend.Status, now time.Time) (backend.Job, error) {
	j, err := get(tx, id)
	if err == nil && remoteRoute(tx, id) {
		return j, ErrRemoteAttemptRequired
	}
	if err != nil || j.CurrentAttemptID() != expected || j.Status.State.Terminal() || j.Status.State == backend.Retrying {
		return j, err
	}
	if j.Task.Workdir == nil {
		return observeAttemptCore(tx, id, expected, status, now)
	}
	if status.State.Terminal() && status.State != backend.Unknown {
		return j, errors.New("workspace terminal observation requires completion")
	}
	_, err = artifactCoreChange(tx, artifactAttemptLedger(tx, id, ""), maxChangeBytes, 100, func(st *artifactState) error {
		w, ok := st.Workspaces[expected]
		if !ok {
			return ErrArtifactConflict
		}
		if status.State == backend.Unknown {
			w.Held = true
			w.Diagnostic = artifactDiagnostic(status.Reason)
			st.Workspaces[expected] = w
			return nil
		}
		if status.Instance != "" {
			if w.Instance == "" {
				if err := artifactWorkspaceBound(st, expected, j.TargetBinding, status.Instance); err != nil {
					return err
				}
			} else if status.Instance != w.Instance {
				return ErrArtifactConflict
			}
		}
		var e error
		j, e = observeAttemptCore(tx, id, expected, status, now)
		if e != nil {
			return e
		}
		if j.Status.State == backend.Unknown {
			return ErrArtifactConflict
		}
		return nil
	})
	return j, err
}

func observeAttemptCore(tx *bolt.Tx, id, expected string, status backend.Status, now time.Time) (backend.Job, error) {
	j, err := get(tx, id)
	if err != nil {
		return j, err
	}
	if workerGroupJob(tx, id) {
		return j, errWorkerRoute
	}
	if j.CurrentAttemptID() != expected || j.Status.State.Terminal() || j.Status.State == backend.Retrying {
		return j, nil
	}
	if j.Status.State != backend.Starting && j.Status.State != backend.Running {
		return j, errors.New("attempt has not started")
	}
	if now.IsZero() {
		return j, errors.New("attempt time must be nonzero")
	}
	if !validInstance(status.Instance) {
		return j, errors.New("invalid execution instance")
	}
	if j.Status.Instance != "" {
		if status.Instance != "" && status.Instance != j.Status.Instance {
			status = backend.Status{State: backend.Unknown, Reason: "execution identity mismatch", Instance: j.Status.Instance}
		} else {
			status.Instance = j.Status.Instance
		}
	}
	status.Reason = boundedReason(status.Reason)
	if err := validStatus(status); err != nil {
		return j, err
	}
	if status.State == backend.Queued || status.State == backend.Retrying || status.State == backend.Starting && j.Status.State != backend.Starting {
		return j, errors.New("invalid attempt observation state")
	}
	j.Status = status
	if len(j.Attempts) > 0 {
		a := &j.Attempts[len(j.Attempts)-1]
		a.Status = status
		if status.State.Terminal() {
			a.FinishedAt = &now
		}
		p := j.Task.Retry
		if status.State == backend.Failed && status.Termination == backend.StoppedWithExitCode && status.ExitCode != nil && slices.Contains(p.ExitCodes, *status.ExitCode) && len(j.Attempts) < int(p.MaxAttempts) && !j.CancelRequested {
			deadline := now.Add(time.Duration(p.DelaySeconds) * time.Second)
			j.NextAttemptAt = &deadline
			j.Status.State = backend.Retrying
		}
	}
	j.UpdatedAt = now
	if err := putAttemptJob(tx, j); err != nil {
		return j, err
	}
	return get(tx, id)
}

func putAttemptJob(tx *bolt.Tx, j backend.Job) error {
	if err := validateAttemptJob(j); err != nil {
		return err
	}
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	_ = data
	return putTrackedJob(tx, j)
}

func validateUpdate(before, after backend.Job) error {
	if !reflect.DeepEqual(before.Attempts, after.Attempts) || !reflect.DeepEqual(before.NextAttemptAt, after.NextAttemptAt) || before.Status.Termination != after.Status.Termination || before.Status.Instance != after.Status.Instance {
		return errors.New("cannot change attempt history or evidence")
	}
	if before.CancelRequested && !after.CancelRequested {
		return errors.New("cannot withdraw cancellation")
	}
	if before.Task.Retry != nil && (before.Status.State != after.Status.State || !reflect.DeepEqual(before.Status.ExitCode, after.Status.ExitCode)) {
		return errors.New("retry state requires an attempt transition")
	}
	if before.Task.Workdir != nil && (before.Status.State != after.Status.State || !reflect.DeepEqual(before.Status.ExitCode, after.Status.ExitCode)) {
		return errors.New("workspace state requires an attempt transition")
	}
	if before.Task.Retry != nil {
		return validateAttemptJob(after)
	}
	return nil
}

func boundedReason(s string) string {
	s = strings.ToValidUTF8(s, "\ufffd")
	if len(s) > 1024 {
		s = s[:1024]
		for !utf8.ValidString(s) {
			s = s[:len(s)-1]
		}
	}
	return s
}

func validInstance(s string) bool {
	if len(s) > 128 {
		return false
	}
	for i := range len(s) {
		if s[i] < 32 || s[i] > 126 {
			return false
		}
	}
	return true
}
func validAttemptID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := range len(s) {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}
func validStatus(s backend.Status) error {
	switch s.State {
	case backend.Queued, backend.Starting, backend.Running, backend.Retrying, backend.Failed, backend.Succeeded, backend.Canceled, backend.Unknown:
	default:
		return errors.New("invalid attempt state")
	}
	if !validInstance(s.Instance) || !utf8.ValidString(s.Reason) || len(s.Reason) > 1024 {
		return errors.New("invalid attempt status metadata")
	}
	if s.Termination != backend.UnknownTermination && s.Termination != backend.StoppedWithExitCode {
		return errors.New("invalid termination evidence")
	}
	if s.Termination == backend.StoppedWithExitCode && (s.ExitCode == nil || *s.ExitCode < 0 || *s.ExitCode > 255 || s.State != backend.Failed && s.State != backend.Succeeded && s.State != backend.Canceled && s.State != backend.Retrying) {
		return errors.New("invalid stopped evidence")
	}
	return nil
}

func validateAttemptJob(j backend.Job) error {
	if j.Task.Retry == nil {
		if len(j.Attempts) != 0 || j.NextAttemptAt != nil || j.Status.State == backend.Retrying {
			return errors.New("implicit job contains retry metadata")
		}
		if j.Status.Termination != backend.UnknownTermination || j.Status.Instance != "" {
			return validStatus(j.Status)
		}
		return nil
	}
	p, err := backend.NormalizeRetry(j.Task.Retry)
	if err != nil || p == nil || !reflect.DeepEqual(p, j.Task.Retry) {
		return errors.New("stored retry policy is not canonical")
	}
	if len(j.Attempts) == 0 || len(j.Attempts) > int(p.MaxAttempts) || j.Attempts[0].ID != j.ID || !j.Attempts[0].CreatedAt.Equal(j.CreatedAt) {
		return errors.New("invalid attempt history length or identity")
	}
	if err := validStatus(j.Status); err != nil {
		return err
	}
	ids := make(map[string]bool)
	for i, a := range j.Attempts {
		if !validAttemptID(a.ID) || ids[a.ID] || a.CreatedAt.IsZero() {
			return errors.New("invalid attempt identity or time")
		}
		ids[a.ID] = true
		if err := validStatus(a.Status); err != nil {
			return err
		}
		if a.Status.State == backend.Retrying {
			return errors.New("attempt cannot be retrying")
		}
		if a.StartingAt != nil && (a.StartingAt.IsZero() || a.StartingAt.Before(a.CreatedAt)) {
			return errors.New("invalid attempt starting time")
		}
		if a.StartingAt == nil && (a.Status.ExitCode != nil || a.Status.Termination != backend.UnknownTermination || a.Status.Instance != "") {
			return errors.New("unstarted attempt contains execution metadata")
		}
		if a.Status.State == backend.Queued {
			if i != 0 || len(j.Attempts) != 1 || a.StartingAt != nil || a.FinishedAt != nil {
				return errors.New("invalid queued attempt")
			}
		} else if a.Status.State == backend.Canceled && a.StartingAt == nil {
			if a.FinishedAt == nil {
				return errors.New("unstarted cancellation has no finish time")
			}
		} else if a.StartingAt == nil {
			return errors.New("attempt has no starting intent")
		}
		if a.Status.State.Terminal() != (a.FinishedAt != nil) {
			return errors.New("invalid attempt completion time")
		}
		if a.FinishedAt != nil && (a.FinishedAt.Before(a.CreatedAt) || a.StartingAt != nil && a.FinishedAt.Before(*a.StartingAt)) {
			return errors.New("attempt finished before starting")
		}
		if i < len(j.Attempts)-1 {
			if !retryEligible(a.Status, p) || a.FinishedAt == nil || j.Attempts[i+1].CreatedAt.Before(a.FinishedAt.Add(time.Duration(p.DelaySeconds)*time.Second)) {
				return errors.New("invalid prior attempt")
			}
		}
		raw, err := json.Marshal(a)
		if err != nil || len(raw) > maxAttemptBytes {
			return errors.New("attempt metadata exceeds reservation")
		}
	}
	last := j.Attempts[len(j.Attempts)-1]
	if j.Status.State == backend.Retrying {
		if !retryEligible(last.Status, p) || len(j.Attempts) >= int(p.MaxAttempts) || last.FinishedAt == nil || j.NextAttemptAt == nil || !j.NextAttemptAt.Equal(last.FinishedAt.Add(time.Duration(p.DelaySeconds)*time.Second)) {
			return errors.New("invalid retry deadline or evidence")
		}
		want := last.Status
		want.State = backend.Retrying
		// Held routing diagnostics may differ from the failed attempt's reason.
		want.Reason = j.Status.Reason
		if !reflect.DeepEqual(want, j.Status) {
			return errors.New("retry status differs from failed attempt")
		}
	} else {
		if j.NextAttemptAt != nil {
			return errors.New("unexpected retry deadline")
		}
		if j.Status.State == backend.Canceled && last.Status.State == backend.Failed {
			if !j.CancelRequested || !retryEligible(last.Status, p) {
				return errors.New("invalid waiting cancellation")
			}
		} else {
			want := last.Status
			want.Reason = j.Status.Reason
			if !reflect.DeepEqual(want, j.Status) {
				return errors.New("logical status differs from current attempt")
			}
		}
	}
	return nil
}
func retryEligible(s backend.Status, p *backend.RetryPolicy) bool {
	return s.State == backend.Failed && s.Termination == backend.StoppedWithExitCode && s.ExitCode != nil && *s.ExitCode > 0 && slices.Contains(p.ExitCodes, *s.ExitCode)
}

func validateAttempts(tx *bolt.Tx) error {
	version := schemaNumber(tx)
	m := tx.Bucket(metadataBucket)
	index := tx.Bucket(attemptJobsBucket)
	raw := m.Get(retryBytesKey)
	if version < 4 {
		if index != nil || raw != nil || m.Bucket(retryBytesKey) != nil {
			return errors.New("retry metadata requires schema 4")
		}
	} else if index == nil || len(raw) != 8 || m.Bucket(retryBytesKey) != nil {
		return errors.New("missing or invalid retry metadata")
	}
	expected := make(map[string]string)
	var reserved uint64
	if err := tx.Bucket(jobsBucket).ForEach(func(id, data []byte) error {
		var j backend.Job
		if err := decodeStoredJob(data, &j); err != nil {
			return err
		}
		if version < 4 && (j.Task.Retry != nil || len(j.Attempts) > 0 || j.NextAttemptAt != nil || j.Status.Termination != 0 || j.Status.Instance != "" || j.Status.State == backend.Retrying) {
			return errors.New("retry fields require schema 4")
		}
		if err := validateAttemptJob(j); err != nil {
			return err
		}
		if j.Task.Retry == nil {
			return nil
		}
		reserved += uint64(j.Task.Retry.MaxAttempts) * maxAttemptBytes
		if reserved > maxRetryBytes {
			return errors.New("retry reservations exceed limit")
		}
		for _, a := range j.Attempts {
			if expected[a.ID] != "" || a.ID != j.ID && tx.Bucket(jobsBucket).Get([]byte(a.ID)) != nil {
				return errors.New("attempt identity collision")
			}
			expected[a.ID] = string(id)
		}
		return nil
	}); err != nil {
		return err
	}
	if version < 4 {
		return nil
	}
	if binary.BigEndian.Uint64(raw) != reserved {
		return errors.New("invalid retry reservation total")
	}
	if err := index.ForEach(func(id, owner []byte) error {
		if owner == nil || expected[string(id)] == "" || !bytes.Equal(owner, []byte(expected[string(id)])) {
			return errors.New("invalid attempt owner index")
		}
		delete(expected, string(id))
		return nil
	}); err != nil {
		return err
	}
	if len(expected) != 0 {
		return errors.New("missing attempt owner index")
	}
	return nil
}
