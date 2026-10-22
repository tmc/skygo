package store

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

// ErrRemoteAttemptRequired prevents an ordinary/local route from adopting a
// durable remote execution. A caller must use the exact remote transaction API.
var ErrRemoteAttemptRequired = errors.New("remote attempt requires remote operation")

type remoteSubmission struct {
	authority auth.Delegation
	policy    backend.DataPolicy
	now       time.Time
}

func (r *remoteSubmission) validate(owner string, t backend.Task) error {
	if r == nil || r.authority.Principal != owner || !validPrincipal([]byte(owner)) || !artifactHex(r.authority.TokenID, 32) || r.authority.Digest == ([32]byte{}) || !remoteCheckTime(r.now) {
		return ErrArtifactConflict
	}
	if _, e := r.policy.Budgets(t.TimeoutSeconds); e != nil {
		return e
	}
	return nil
}
func remoteAccept(st *artifactState, id, owner string, r *remoteSubmission) error {
	if st == nil || r == nil {
		return ErrArtifactsDisabled
	}
	if _, ok := st.Remote[id]; ok {
		return ErrArtifactConflict
	}
	if len(st.Remote) >= 1024 {
		return &ArtifactCapacityError{Domain: "remote rows", Used: uint64(len(st.Remote)), Limit: 1024}
	}
	if artifactCompletionCount(st) >= 1024 {
		return &ArtifactCapacityError{Domain: "completion records", Used: uint64(artifactCompletionCount(st)), Limit: 1024}
	}
	p := r.policy
	d := r.authority
	st.Remote[id] = remoteRecord{Job: id, Revision: 1, State: 1, Authority: &remoteAuthority{Principal: owner, TokenID: d.TokenID, Digest: d.Digest, Policy: [5]int64{p.StageSeconds, p.CollectionSeconds, p.DefaultTaskSeconds, p.MaxTaskSeconds, p.ShutdownSeconds}}}
	return nil
}

// SubmitData accepts through the existing atomic request protocol. Exact replay
// precedes new delegation, policy, placement and artifact availability checks.
// The actual Engine delegation check and protected target validation run outside
// Store transactions immediately before a new accepting call.
func (s *Store) SubmitData(actor auth.Actor, domain *RequestDomain, requestID string, task backend.Task, target, binding string, authority auth.Delegation, policy backend.DataPolicy, now time.Time) (backend.Job, error) {
	task = backend.CloneTask(task)
	r := &remoteSubmission{authority, policy, now.UTC()}
	if domain == nil {
		return s.submit(actor, requestID, task, target, binding, submissionRemote, r)
	}
	return s.submitRetained(actor, *domain, requestID, task, target, binding, backend.RemoteOutput, 0, submissionRemote, r)
}
func remoteRoute(tx *bolt.Tx, job string) bool {
	b := tx.Bucket(artifactsBucket)
	if b == nil {
		return false
	}
	t := b.Bucket([]byte("remote_attempts"))
	return t != nil && t.Get([]byte(job)) != nil
}
func remoteKey(j backend.Job) RemoteKey { return RemoteKey{j.ID, j.CurrentAttemptID()} }

// BeginRemoteAttempt freezes one original launch and pending scope lifetime.
// Random scope secrets are supplied only as purpose-separated hashes; replay
// returns the original values and never repeats an original backend call.
func (s *Store) BeginRemoteAttempt(jobID, expected string, hashes RemoteHashes, policy backend.DataPolicy, now time.Time) (out RemoteStart, start bool, err error) {
	candidate, e := newID()
	if e != nil {
		return out, false, e
	}
	result, e := newID()
	if e != nil {
		return out, false, e
	}
	now = now.UTC()
	err = s.db.Update(func(tx *bolt.Tx) error {
		j, e := get(tx, jobID)
		if e != nil {
			return e
		}
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		first, ok := st.Remote[jobID]
		if !ok {
			return ErrNotFound
		}
		out.Job = j
		if j.CurrentAttemptID() != expected {
			return nil
		}
		if j.CancelRequested && (j.Status.State == backend.Queued || j.Status.State == backend.Retrying) {
			_, e = artifactCoreChange(tx, artifactAttemptLedger(tx, jobID, ""), maxChangeBytes, 100, func(st *artifactState) error {
				r, initial := st.Remote[jobID]
				initial = initial && expected == jobID && j.Status.State == backend.Queued && r.State == 1 && r.Creation == 0 && r.Active == nil && r.Done == nil && r.Resources == (remoteResources{}) && r.Ack == 0 && remoteQueuedBefore(j)
				var e error
				j, start, e = beginAttemptCore(tx, jobID, expected, candidate, 0, now)
				if e != nil {
					return e
				}
				if start {
					return ErrArtifactConflict
				}
				if initial {
					if !remoteNeverStarted(j, jobID) {
						return ErrArtifactConflict
					}
					r.State = 4
					r.Done = &remoteDecision{Digest: remoteQueuedDigest(j), Outcome: uint8(DataCanceled)}
					if e = remoteBump(&r); e != nil {
						return e
					}
					st.Remote[jobID] = r
					out.Revision = r.Revision
				}
				out.Job = j
				return nil
			})
			return e
		}
		if current, ok := st.Remote[expected]; ok && j.Status.State != backend.Retrying && current.State != 1 {
			out.Revision = current.Revision
			if current.Active != nil {
				out.Budgets = remoteBudgets(current.Active)
			}
			return nil
		}
		if j.Status.State != backend.Queued && j.Status.State != backend.Retrying {
			return nil
		}
		if j.Status.State == backend.Retrying && (j.NextAttemptAt == nil || now.Before(*j.NextAttemptAt)) {
			return nil
		}
		if j.Status.State == backend.Retrying {
			retired, e := remoteRetryRetired(st, j, expected)
			if errors.Is(e, errRemoteRetryUnproven) {
				// Hold the job Retrying: one unprovable predecessor must not stop
				// the caller's other work. It may still be canceled.
				return nil
			}
			if e != nil || !retired {
				return e
			}
		}
		_, e = artifactCoreChange(tx, artifactAttemptLedger(tx, jobID, candidate), maxChangeBytes, 100, func(st *artifactState) error {
			var e error
			if j.CancelRequested {
				out.Job, start, e = beginAttemptCore(tx, jobID, expected, candidate, 0, now)
				return e
			}
			if first.Authority == nil || policy != remotePolicy(first.Authority) {
				return ErrArtifactConflict
			}
			budgets, e := policy.Budgets(j.Task.TimeoutSeconds)
			if e != nil {
				return e
			}
			if (hashes.Input != ([32]byte{})) != (j.Task.Workdir != nil) || (hashes.Output != ([32]byte{})) != (len(j.Task.Outputs) != 0) || hashes.Input != ([32]byte{}) && hashes.Input == hashes.Output {
				return ErrArtifactConflict
			}
			total, e := budgets.TotalSeconds()
			if e != nil {
				return e
			}
			stage, e := backend.DataDeadline(now, budgets.StageSeconds)
			if e != nil {
				return e
			}
			end, e := backend.DataDeadline(now, total)
			if e != nil {
				return e
			}
			expires, e := backend.DataDeadline(now, 120)
			if e != nil {
				return e
			}
			if j.Task.Workdir != nil {
				p, ok := st.JobInputs[j.ID]
				if !ok {
					return ErrArtifactConflict
				}
				if e := artifactGranted(st, p.Owner, p.Ref, now); e != nil {
					return e
				}
			}
			j, start, e = beginAttemptCore(tx, jobID, expected, candidate, 0, now)
			if e != nil || !start {
				return e
			}
			id := j.CurrentAttemptID()
			r, exists := st.Remote[id]
			if exists && r.State != 1 {
				return ErrArtifactConflict
			}
			if !exists {
				if len(st.Remote) >= 1024 || artifactCompletionCount(st) >= 1024 {
					return &ArtifactCapacityError{Domain: "remote rows", Used: uint64(len(st.Remote)), Limit: 1024}
				}
				r = remoteRecord{Job: jobID, Revision: 1}
			}
			r.State = 2
			r.Active = &remoteActive{Phase: uint8(backend.DataStarting), Budgets: [4]int64{budgets.StageSeconds, budgets.TaskSeconds, budgets.CollectionSeconds, budgets.ShutdownSeconds}, Began: now, StageDeadline: stage, ResourceDeadline: end}
			if j.Task.Workdir != nil {
				r.Active.Input = &remoteLease{Hash: hashes.Input, Epoch: 1, State: 1, Expires: expires, Renewed: now}
			}
			if len(j.Task.Outputs) != 0 {
				owner, e := remoteOwner(st, first)
				if e != nil {
					return e
				}
				if _, ok := st.Objects[result]; ok {
					return ErrArtifactConflict
				}
				if _, ok := st.Uploads[result]; ok {
					return ErrArtifactConflict
				}
				r.Active.Output = &remoteLease{Hash: hashes.Output, Epoch: 1, State: 1, Expires: expires, Renewed: now}
				r.Active.ResultID = result
				expires := now.Add(24 * time.Hour)
				if !remoteCheckTime(expires) {
					return ErrArtifactConflict
				}
				st.Objects[result] = artifactObject{Owner: owner, JobID: jobID, Ref: ArtifactRef{ID: result, Size: artifactMaxSize}, Origin: "remote_result", Source: id, State: "reserved", Expires: expires, DeleteAfter: expires}
			}
			if e = remoteBump(&r); e != nil {
				return e
			}
			st.Remote[id] = r
			out = RemoteStart{Job: j, Revision: r.Revision, Budgets: budgets}
			return nil
		})
		return e
	})
	if err != nil {
		start = false
	}
	return
}

// RecordRemoteCreated integrates the SAME original return once. Late success
// retains cleanup receipts but never upgrades closed or expired admission.
func (s *Store) RecordRemoteCreated(key RemoteKey, revision uint64, c RemoteCreation, now time.Time) (out RemoteAttempt, err error) {
	now = now.UTC()
	err = s.db.Update(func(tx *bolt.Tx) error {
		_, e := artifactCoreChange(tx, artifactAttemptLedger(tx, key.JobID, ""), maxChangeBytes, 100, func(st *artifactState) error {
			r, e := remoteGet(st, key)
			if e != nil {
				return e
			}
			j, e := get(tx, key.JobID)
			if e != nil {
				return e
			}
			if e := backend.ValidateDataCreated(j.Task, c.Resources, !c.Succeeded); e != nil {
				return e
			}
			resources := remoteCreatedRecord(c.Resources)
			if r.Creation != 0 {
				if !reflect.DeepEqual(r.Resources, resources) || (r.Creation == 2) == c.Succeeded {
					return ErrArtifactConflict
				}
				out, e = remoteSnapshot(st, key.AttemptID, r)
				return e
			}
			if r.State != 2 || r.Active == nil || r.Revision != revision || !remoteCheckTime(now) {
				return ErrArtifactConflict
			}
			r.Resources = resources
			r.Creation = 2
			if c.Succeeded {
				r.Creation = 1
				admit := j.CurrentAttemptID() == key.AttemptID && !j.CancelRequested && r.Active.ClosedReason == 0 && now.Before(r.Active.StageDeadline) && now.Before(r.Active.ResourceDeadline)
				for _, l := range []*remoteLease{r.Active.Input, r.Active.Output} {
					if l != nil && (l.State != 1 || !now.Before(l.Expires)) {
						admit = false
					}
				}
				if admit {
					r.Creation = 3
					for _, l := range []*remoteLease{r.Active.Input, r.Active.Output} {
						if l != nil {
							l.State = 2
						}
					}
				} else {
					remoteClose(&r, RemoteExpired)
				}
			} else {
				remoteClose(&r, RemoteIdentityMismatch)
			}
			if r.Resources.Pod != nil {
				status := backend.Status{State: backend.Starting, Instance: r.Resources.Pod.UID}
				if _, e = observeAttemptCore(tx, key.JobID, key.AttemptID, status, now); e != nil {
					return e
				}
			}
			if e = remoteBump(&r); e != nil {
				return e
			}
			st.Remote[key.AttemptID] = r
			out, e = remoteSnapshot(st, key.AttemptID, r)
			return e
		})
		return e
	})
	return
}
func remoteRenew(r *remoteRecord, now time.Time) error {
	if r.Creation != 3 || r.Active == nil || r.Active.ClosedReason != 0 || r.Active.Phase == uint8(backend.DataHeld) {
		return nil
	}
	a := r.Active
	collection, e := remoteCollectionDeadline(a)
	if e != nil {
		return e
	}
	if !now.Before(a.ResourceDeadline) || a.TaskDeadline != nil && a.Stop == nil && !now.Before(*a.TaskDeadline) || a.Stop == nil && a.Running == nil && !now.Before(a.StageDeadline) || collection != nil && !now.Before(*collection) {
		remoteClose(r, RemotePhaseTimeout)
		return nil
	}
	for _, l := range []*remoteLease{a.Input, a.Output} {
		if l == nil || l.State == 3 || l.State == 4 {
			continue
		}
		if !now.Before(l.Expires) {
			remoteClose(r, RemoteExpired)
			return nil
		}
	}
	due := false
	for _, l := range []*remoteLease{a.Input, a.Output} {
		if l != nil && l.State == 2 && !now.Before(l.Renewed.Add(30*time.Second)) {
			due = true
		}
	}
	if due {
		end, e := backend.DataDeadline(now, 120)
		if e != nil {
			remoteClose(r, RemotePhaseTimeout)
			return nil
		}
		for _, l := range []*remoteLease{a.Input, a.Output} {
			if l != nil && l.State == 2 && !now.Before(l.Renewed.Add(30*time.Second)) {
				l.Renewed, l.Expires = now, end
			}
		}
	}
	return nil
}

// ObserveRemote preserves actual start identity and qualified native receipts.
// A failed fresh delegation check must first call RevokeRemote. Cleanup-only
// observations remain integrable afterward but cannot renew or release grants.
func (s *Store) ObserveRemote(key RemoteKey, revision uint64, o backend.DataObservation, now time.Time) (out RemoteAttempt, err error) {
	o = backend.CloneDataObservation(o)
	now = now.UTC()
	err = s.db.Update(func(tx *bolt.Tx) error {
		_, e := artifactCoreChange(tx, artifactAttemptLedger(tx, key.JobID, ""), maxChangeBytes, 100, func(st *artifactState) error {
			r, e := remoteGet(st, key)
			if e != nil {
				return e
			}
			if r.State != 2 || r.Active == nil || r.Revision != revision || !remoteCheckTime(now) {
				return ErrArtifactConflict
			}
			ref, e := remoteRef(tx, key, r)
			if e != nil {
				return e
			}
			if e = backend.ValidateDataObservation(ref, o); e != nil {
				return e
			}
			a := r.Active
			if o.TaskStart != nil {
				t := remoteStartReceipt{ID: o.TaskStart.ContainerID, Started: o.TaskStart.StartedAt.UTC()}
				if a.Stop != nil && (a.Stop.Task.ID != t.ID || !a.Stop.Task.Started.Equal(t.Started)) || a.Running != nil && !reflect.DeepEqual(*a.Running, t) {
					return ErrArtifactConflict
				}
			}
			if o.TaskStop != nil {
				stop := remoteStopRecord(o.TaskStop)
				if a.NoLaunch != nil || a.Stop != nil && !reflect.DeepEqual(a.Stop, stop) || a.Running != nil && (a.Running.ID != stop.Task.ID || !a.Running.Started.Equal(stop.Task.Started)) {
					return ErrArtifactConflict
				}
			}
			if o.NoLaunch != nil && (a.Stop != nil || a.Running != nil || a.NoLaunch != nil && !reflect.DeepEqual(a.NoLaunch, remoteNoLaunchRecord(o.NoLaunch))) || o.Collector != nil && a.Collector != nil && !reflect.DeepEqual(a.Collector, remoteExitRecord(o.Collector)) {
				return ErrArtifactConflict
			}
			if !remoteObservationTimes(a, o, &now) {
				if a.ClosedReason == 0 {
					remoteClose(&r, RemoteIdentityMismatch)
				}
				if e = remoteBump(&r); e != nil {
					return e
				}
				st.Remote[key.AttemptID] = r
				out, e = remoteSnapshot(st, key.AttemptID, r)
				return e
			}
			if o.TaskStart != nil {
				t := remoteStartReceipt{ID: o.TaskStart.ContainerID, Started: o.TaskStart.StartedAt.UTC()}
				if a.Stop == nil {
					a.Running = &t
				}
				end, e := backend.DataDeadline(t.Started, a.Budgets[1])
				if e != nil {
					return e
				}
				if a.TaskDeadline != nil && !a.TaskDeadline.Equal(end) {
					return ErrArtifactConflict
				}
				a.TaskDeadline = &end
			}
			if o.TaskStop != nil {
				stop := remoteStopRecord(o.TaskStop)

				end, e := backend.DataDeadline(stop.Task.Started, a.Budgets[1])
				if e != nil {
					return e
				}
				if a.TaskDeadline != nil && !a.TaskDeadline.Equal(end) {
					return ErrArtifactConflict
				}
				a.TaskDeadline = &end
				a.Stop = stop
				a.Running = nil
			}
			if o.NoLaunch != nil {
				n := remoteNoLaunchRecord(o.NoLaunch)

				a.NoLaunch = n
				for _, l := range []*remoteLease{a.Input, a.Output} {
					if l != nil && l.State != 4 {
						l.State = 3
					}
				}
			}
			if o.Collector != nil {
				c := remoteExitRecord(o.Collector)

				a.Collector = c
			}
			if a.ClosedReason == 0 {
				a.Phase = uint8(o.Phase)
			}
			if o.Phase == backend.DataHeld && o.NoLaunch == nil {
				remoteClose(&r, RemoteIdentityMismatch)
			}
			if e = remoteRenew(&r, now); e != nil {
				return e
			}
			if a.ClosedReason == 0 && a.Stop != nil && a.Stop.Task.Code == 0 && a.Output != nil && a.Output.State == 2 && a.ReleaseRevision == 0 {
				_, e := backend.DataDeadline(now, a.Budgets[2])
				if e != nil {
					remoteClose(&r, RemotePhaseTimeout)
				} else {
					a.ReleaseAt = remoteCopy(&now)
					next, e := artifactNext(r.Revision)
					if e != nil {
						return e
					}
					a.ReleaseRevision = next
				}
			}
			if a.ClosedReason == 0 && o.TaskStart != nil {
				if _, e = observeAttemptCore(tx, key.JobID, key.AttemptID, backend.Status{State: backend.Running, Instance: ref.Instance}, now); e != nil {
					return e
				}
			}
			if e = remoteBump(&r); e != nil {
				return e
			}
			st.Remote[key.AttemptID] = r
			out, e = remoteSnapshot(st, key.AttemptID, r)
			return e
		})
		return e
	})
	return
}

// RevokeRemote closes admission irreversibly without erasing positive receipts.
func (s *Store) RevokeRemote(key RemoteKey, revision uint64, reason RemoteCloseReason, now time.Time) (out RemoteAttempt, err error) {
	now = now.UTC()
	err = s.artifactUpdate(func(st *artifactState) error {
		r, e := remoteGet(st, key)
		if e != nil {
			return e
		}
		if reason < RemoteCanceled || reason > RemoteShutdown || !remoteCheckTime(now) {
			return ErrArtifactConflict
		}
		if r.State >= 3 {
			out, e = remoteSnapshot(st, key.AttemptID, r)
			return e
		}
		if r.State != 2 || r.Active == nil {
			return ErrArtifactConflict
		}
		if r.Active.ClosedReason != 0 {
			out, e = remoteSnapshot(st, key.AttemptID, r)
			return e
		}
		if r.Revision != revision {
			return ErrArtifactConflict
		}
		remoteClose(&r, reason)
		if e = remoteBump(&r); e != nil {
			return e
		}
		st.Remote[key.AttemptID] = r
		out, e = remoteSnapshot(st, key.AttemptID, r)
		return e
	})
	return
}

func remoteSemanticDigest(j backend.Job, key RemoteKey, c RemoteCompletion) ([32]byte, error) {
	task, e := backend.TaskBytes(j.Task)
	if e != nil {
		return [32]byte{}, e
	}
	// Physical allocation identity, upload cursor and lease noise are deliberately
	// excluded. Logical content is stable across repeated sealed handoffs.
	type content struct {
		SHA256                                   string
		Size                                     uint64
		ExpandedBytes, RegularFiles, Directories uint64
	}
	var candidate *content
	if c.Outcome == DataCanceled && c.Stop == nil && c.NoLaunch == nil && c.Collector == nil && c.Candidate == nil {
		return remoteQueuedDigest(j), nil
	}
	if c.Candidate != nil {
		v := c.Candidate
		candidate = &content{v.Reference.SHA256, v.Reference.Size, v.Manifest.ExpandedBytes, v.Manifest.RegularFiles, v.Manifest.Directories}
	}
	raw, e := json.Marshal(struct {
		Purpose   string
		Key       RemoteKey
		Task      json.RawMessage
		Binding   string
		Outcome   DataOutcome
		Stop      *backend.DataTaskStop
		NoLaunch  *backend.DataNoLaunch
		Collector *backend.ContainerExit
		Content   *content
	}{"skygo.remote-completion.v1", key, task, j.TargetBinding, c.Outcome, c.Stop, c.NoLaunch, c.Collector, candidate})
	if e != nil {
		return [32]byte{}, e
	}
	return sha256.Sum256(raw), nil
}

// errRemoteRetryUnproven reports a retired predecessor whose retained facts
// cannot establish the selected retry.
var errRemoteRetryUnproven = errors.New("retired remote attempt does not prove retry")

// remoteRetryRetired reports whether the retry-waiting job j may begin the
// attempt after expected. It returns false and a nil error while that original
// attempt is still retiring, errRemoteRetryUnproven when the retired attempt
// cannot establish the selected retry, and ErrArtifactConflict for an invalid
// job or record.
func remoteRetryRetired(st *artifactState, j backend.Job, expected string) (bool, error) {
	p, n := j.Task.Retry, len(j.Attempts)
	if p == nil || n == 0 || n >= int(p.MaxAttempts) {
		return false, ErrArtifactConflict
	}
	last := j.Attempts[n-1]
	if last.ID != expected || !retryEligible(last.Status, p) || last.FinishedAt == nil {
		return false, ErrArtifactConflict
	}
	r, ok := st.Remote[expected]
	if !ok || r.Job != j.ID || r.State < 2 {
		return false, ErrArtifactConflict
	}
	if r.State != 4 || r.Active != nil {
		return false, nil
	}
	d := r.Done
	if d == nil || r.Creation != 3 || r.Resources.Pod == nil || r.Resources.Pod.UID == "" || last.Status.Instance != r.Resources.Pod.UID {
		return false, errRemoteRetryUnproven
	}
	if d.Outcome != uint8(DataMainExit) || d.OriginalExit == nil || *d.OriginalExit != *last.Status.ExitCode {
		return false, errRemoteRetryUnproven
	}
	workdir, outputs := j.Task.Workdir != nil, len(j.Task.Outputs) != 0
	if (r.Resources.Input != nil) != workdir || (r.Resources.Output != nil) != outputs || r.Ack != remoteRequiredAck(r) {
		return false, errRemoteRetryUnproven
	}
	if workdir != (d.Input != nil) || d.Input != nil && d.Input.State != 4 {
		return false, errRemoteRetryUnproven
	}
	return true, nil
}

func remoteCompletionStatus(j backend.Job, r remoteRecord, c RemoteCompletion) (backend.Status, *int32, error) {
	instance := remoteCreated(r).Instance
	status := backend.Status{State: backend.Failed, Instance: instance, Reason: "remote phase failed"}
	if c.Outcome < DataMainExit || c.Outcome >= DataUncertain || (c.Stop == nil) == (c.NoLaunch == nil) {
		return status, nil, ErrArtifactConflict
	}
	observation := backend.DataObservation{Phase: backend.DataSettled, TaskStop: c.Stop, NoLaunch: c.NoLaunch, Collector: c.Collector}
	if c.Stop != nil {
		code := c.Stop.Task.ExitCode
		status.ExitCode = &code
		status.State = backend.Failed
		if code == 0 {
			status.State = backend.Succeeded
		}
		observation.Status = status
	} else {
		observation.Status = status
	}
	ref := backend.Reference{ID: j.CurrentAttemptID(), Task: j.Task, Binding: j.TargetBinding, Instance: instance}
	if e := backend.ValidateDataObservation(ref, observation); e != nil {
		return status, nil, e
	}
	var original *int32
	switch c.Outcome {
	case DataMainExit:
		if c.Stop == nil {
			return status, nil, ErrArtifactConflict
		}
		code := c.Stop.Task.ExitCode
		original = &code
		status.Termination = backend.StoppedWithExitCode
		status.Reason = ""
	case DataCollectionFailure:
		if c.Stop == nil || c.Stop.Task.ExitCode != 0 || len(j.Task.Outputs) == 0 {
			return status, nil, ErrArtifactConflict
		}
		code := int32(0)
		original = &code
		status.State = backend.Failed
		status.Reason = "remote output collection failed"
	case DataStageFailure:
		if c.NoLaunch == nil || c.NoLaunch.FailedInit != backend.StageFailure {
			return status, nil, ErrArtifactConflict
		}
	case DataSetupFailure:
		if c.NoLaunch == nil || c.NoLaunch.FailedInit != backend.SetupFailure {
			return status, nil, ErrArtifactConflict
		}
	case DataCanceled:
		status.State = backend.Canceled
		status.Reason = "remote execution canceled"
	case DataTimeout:
		status.State = backend.Failed
		status.Reason = "remote execution timed out"
	}
	if c.Outcome != DataMainExit {
		status.ExitCode = nil
		status.Termination = backend.UnknownTermination
	}
	// Losing authority is a job-level phase failure that cannot retry. The
	// native nonzero exit stays in the completion proof as original. Completed
	// rows have no Active and keep their committed outcome.
	if c.Outcome == DataMainExit && *original != 0 && r.Active != nil && r.Active.ClosedReason == uint8(RemoteAuthorityLost) {
		status.State = backend.Failed
		status.ExitCode = nil
		status.Termination = backend.UnknownTermination
		status.Reason = "remote authority lost"
	}
	return status, original, nil
}
func remoteDiscardOutput(st *artifactState, r remoteRecord, operation string) error {
	id := remoteOutputID(r)
	if id == "" {
		return nil
	}
	u, upload := st.Uploads[id]
	if upload && u.State == "deleting" && artifactFileValid(u.Part) {
		for _, d := range st.Deletes {
			if d.ObjectID == id && d.Owner == u.Owner && d.File == u.Part && d.Revision == u.Revision && (d.Kind == "upload" || d.Kind == "finalizing") {
				return nil
			}
		}
		return ErrArtifactConflict
	}
	o, ok := st.Objects[id]
	if !ok {
		return ErrArtifactConflict
	}
	if _, ok := st.Deletes[operation]; ok {
		return ErrArtifactConflict
	}
	switch {
	case !upload && o.State == "reserved":
		delete(st.Objects, id)
		return nil
	case upload && u.State == "creating":
		return ErrArtifactConflict // UNKNOWN original create cannot become absence.
	case upload && u.State == "writable":
		if o.State != "reserved" || !artifactFileValid(u.Part) {
			return ErrArtifactConflict
		}
		delete(st.Objects, id)
		return artifactMarkUploadDelete(st, operation, u)
	case upload && u.State == "finalizing":
		return artifactMarkUploadDelete(st, operation, u)
	case upload && u.State == "complete" && o.State == "complete":
		if st.reading(id) {
			return ErrArtifactConflict
		}
		o.State = "deleting"
		st.Objects[id] = o
		delete(st.Grants, id)
		st.Deletes[operation] = artifactDelete{Owner: o.Owner, Kind: "object", ObjectID: id, File: o.File, Revision: 1}
		return nil
	default:
		return ErrArtifactConflict
	}
}

// CompleteRemoteAttempt commits one qualified semantic decision, result and
// scope revocation atomically. Exact duplicate lookup precedes current-attempt,
// grant, candidate-presence and result-GC gates; it never repeats native work.
func (s *Store) CompleteRemoteAttempt(key RemoteKey, c RemoteCompletion, now time.Time) (out backend.Job, err error) {
	c.Stop = remoteStopDTO(remoteStopRecord(c.Stop))
	c.NoLaunch = remoteNoLaunchDTO(remoteNoLaunchRecord(c.NoLaunch))
	c.Collector = remoteExitDTO(remoteExitRecord(c.Collector))
	if c.Candidate != nil {
		candidate := *c.Candidate
		c.Candidate = &candidate
	}
	operation, e := newID()
	if e != nil {
		return out, e
	}
	now = now.UTC()
	err = s.db.Update(func(tx *bolt.Tx) error {
		j, e := get(tx, key.JobID)
		if e != nil {
			return e
		}
		digest, e := remoteSemanticDigest(j, key, c)
		if e != nil {
			return e
		}
		_, e = artifactCoreChange(tx, artifactAttemptLedger(tx, key.JobID, ""), maxChangeBytes, 100, func(st *artifactState) error {
			r, e := remoteGet(st, key)
			if e != nil {
				return e
			}
			if r.Done != nil {
				if r.Done.Digest != digest {
					return ErrArtifactConflict
				}
				out = j
				return nil
			}
			if r.State != 2 || r.Active == nil || j.CurrentAttemptID() != key.AttemptID || !remoteCheckTime(now) {
				return ErrArtifactConflict
			}
			if !reflect.DeepEqual(r.Active.Stop, remoteStopRecord(c.Stop)) || !reflect.DeepEqual(r.Active.NoLaunch, remoteNoLaunchRecord(c.NoLaunch)) || !reflect.DeepEqual(r.Active.Collector, remoteExitRecord(c.Collector)) {
				return ErrArtifactConflict
			}
			status, original, e := remoteCompletionStatus(j, r, c)
			if e != nil {
				return e
			}
			if c.Outcome == DataMainExit && c.Stop != nil && c.Stop.Task.ExitCode == 0 && (r.Active.ClosedReason != 0 || j.CancelRequested) {
				return ErrArtifactConflict
			}
			// Native stop alone is not original server transfer/reader settlement.
			// Every actual scoped read must carry its real positive Close receipt.
			for _, read := range st.Reads {
				if read.RemoteAttemptID == key.AttemptID && !read.Closed {
					return ErrArtifactConflict
				}
			}
			// Only the original consumed complete transfer survives Active=nil.
			var input *remoteLease
			if l := r.Active.Input; l != nil && l.State == 4 {
				input = remoteCopy(l)
			}
			closed := r.Active.ClosedReason
			owner, e := remoteOwner(st, r)
			if e != nil {
				return e
			}
			resultID := r.Active.ResultID
			publish := c.Outcome == DataMainExit && c.Stop != nil && c.Stop.Task.ExitCode == 0 && c.Collector != nil && c.Collector.ExitCode == 0 && len(j.Task.Outputs) != 0 && r.Active.ClosedReason == 0 && r.Creation == 3 && !j.CancelRequested
			if publish {
				if _, _, e := remoteGrantRecord(st, RemoteGrant{Key: key, Kind: RemoteOutput, Epoch: r.Active.Output.Epoch, Expires: r.Active.Output.Expires}, now); e != nil {
					return e
				}
			}
			if publish {
				v := c.Candidate
				if v == nil || v.Reference.ID != resultID || !artifactFileValid(v.File) || v.File.Device != st.Root.Volume || !artifactManifestValid(v.Manifest, v.Reference) {
					return ErrArtifactConflict
				}
				o, ok := st.Objects[resultID]
				if !ok || o.State != "complete" || o.Origin != "remote_result" || o.Source != key.AttemptID || o.Owner != owner || o.Ref != v.Reference || o.File != v.File || o.Manifest != v.Manifest {
					return ErrArtifactConflict
				}
				u, ok := st.Uploads[resultID]
				if !ok || u.State != "complete" || u.RemoteAttemptID != key.AttemptID {
					return ErrArtifactConflict
				}
				st.Results[key.AttemptID] = artifactResult{Owner: owner, JobID: key.JobID, AttemptID: key.AttemptID, Ref: o.Ref, Receipt: digest}
				st.Grants[resultID] = artifactGrant{Owner: owner, Ref: o.Ref, Source: "remote_result", SourceID: key.AttemptID, Expires: o.Expires}
			} else if c.Candidate != nil {
				return ErrArtifactConflict
			}
			if !publish && len(j.Task.Outputs) != 0 && c.Outcome == DataMainExit && c.Stop != nil && c.Stop.Task.ExitCode == 0 {
				return ErrArtifactConflict
			} // requires explicit absent/phase outcome, never silent success.
			r.Done = &remoteDecision{Digest: digest, Outcome: uint8(c.Outcome), OriginalExit: remoteCopy(original), Stop: remoteStopRecord(c.Stop), NoLaunch: remoteNoLaunchRecord(c.NoLaunch), Collector: remoteExitRecord(c.Collector), ResultID: resultID, Input: input}
			r.State = 3
			r.Active = nil
			if e = remoteBump(&r); e != nil {
				return e
			}
			st.Remote[key.AttemptID] = r
			if !publish {
				if e = remoteDiscardOutput(st, r, operation); e != nil {
					return e
				}
			}
			if out, e = observeAttemptCore(tx, key.JobID, key.AttemptID, status, now); e != nil {
				return e
			}
			// Another Workdir attempt needs this attempt's complete input transfer.
			if out.Status.State == backend.Retrying && j.Task.Workdir != nil && (input == nil || closed != 0) {
				return ErrArtifactConflict
			}
			return workflowPublish(tx, st, out, key.AttemptID)
		})
		return e
	})
	return
}

// AckRemoteRemoval integrates exact original resource-retirement receipts.
// Native stop and all original call/server owners must already be joined; a
// field echo, elapsed deadline or deletion request is not such proof.
// Nil error may leave State3 with all Ack bits while a bounded closed-reader
// metadata tail remains. Keep the SAME original positive removal result and
// replay this method; query the row for State4 before releasing that frame.
// Metadata replay never authorizes another native RemoveData invocation.
func (s *Store) AckRemoteRemoval(key RemoteKey, revision uint64, removed backend.DataRemoval, now time.Time) error {
	now = now.UTC()
	return s.db.Update(func(tx *bolt.Tx) error {
		_, e := artifactChange(tx, maxChangeBytes, func(st *artifactState) error {
			r, e := remoteGet(st, key)
			if e != nil {
				return e
			}
			ref, e := remoteRef(tx, key, r)
			if e != nil {
				return e
			}
			original := backend.DataResources{Execution: ref, Pod: remoteResourceDTO(r.Resources.Pod), InputSecret: remoteResourceDTO(r.Resources.Input), OutputSecret: remoteResourceDTO(r.Resources.Output)}
			if e = backend.ValidateDataRemoval(original, removed); e != nil {
				return e
			}
			if !remoteCheckTime(now) {
				return ErrArtifactConflict
			}
			bits := uint8(0)
			if removed.PodRemoved {
				bits |= 1
			}
			if removed.InputRemoved {
				bits |= 2
			}
			if removed.OutputRemoved {
				bits |= 4
			}
			partial := r.State == 2 && r.Creation == 2 && r.Resources.Pod == nil && r.Active != nil && r.Active.ClosedReason != 0 && r.Active.Stop == nil && r.Active.NoLaunch == nil && r.Active.Running == nil && r.Active.Collector == nil
			if r.State != 3 && r.State != 4 && !partial {
				return ErrArtifactConflict
			}
			if bits == 0 {
				return nil
			}
			fresh := bits &^ r.Ack
			if fresh != 0 && r.Revision != revision {
				return ErrArtifactConflict
			}
			r.Ack |= bits
			required := uint8(1)
			if ref.Task.Workdir != nil {
				required |= 2
			}
			if len(ref.Task.Outputs) != 0 {
				required |= 4
			}
			detached := 0
			beforeState := r.State
			if r.State == 3 && r.Ack == required && r.Resources.Pod != nil && (required&2 == 0 || r.Resources.Input != nil) && (required&4 == 0 || r.Resources.Output != nil) {
				more := false
				detached, more, e = remoteDetachClosedReads(st, key.AttemptID)
				if e != nil {
					return e
				}
				if !more {
					r.State = 4
					r.Done.Stop = nil
					r.Done.NoLaunch = nil
					r.Done.Collector = nil
				}
			}
			if fresh == 0 && detached == 0 && r.State == beforeState {
				return nil
			}
			// A stale original positive result may progress ONLY this metadata tail:
			// no new native bits, calls, physical credit or reader Close are inferred.
			if e = remoteBump(&r); e != nil {
				return e
			}
			st.Remote[key.AttemptID] = r
			return nil
		})
		return e
	})
}
