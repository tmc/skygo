package store

import (
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/tmc/skygo/internal/backend"
	bolt "go.etcd.io/bbolt"
)

func artifactValidateCore(tx *bolt.Tx, st *artifactState) error {
	return artifactValidateCorePlanned(tx, st, nil)
}

func artifactValidateCorePlanned(tx *bolt.Tx, st *artifactState, plan *retentionMutation) error {
	jobRow := func(id string) (backend.Job, error) {
		if plan == nil {
			return get(tx, id)
		}
		raw := plan.coreGet(jobsBucket, []byte(id))
		if raw == nil {
			return backend.Job{}, ErrNotFound
		}
		var job backend.Job
		if err := json.Unmarshal(raw, &job); err != nil {
			return job, err
		}
		return job, nil
	}
	headerRow := func(id string) (cleanupHeader, error) {
		var h cleanupHeader
		if plan == nil {
			err := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(id), &h)
			return h, err
		}
		raw := plan.get(retentionKey{true, string(cleanupHeadersBucket), id})
		if raw == nil {
			return h, ErrNotFound
		}
		err := json.Unmarshal(raw, &h)
		return h, err
	}
	ownerOf := func(job string) string {
		if !isScoped(tx) {
			return "legacy"
		}
		if plan != nil {
			return string(plan.coreGet(jobPrincipalsBucket, []byte(job)))
		}
		return string(tx.Bucket(jobPrincipalsBucket).Get([]byte(job)))
	}
	for id, p := range st.JobInputs {
		j, err := jobRow(id)
		if err != nil || ownerOf(id) != p.Owner {
			return artifactCorrupt()
		}
		d, err := taskDigest(j.Task)
		if err != nil || d != p.TaskDigest {
			return artifactCorrupt()
		}
		ref, err := artifactTaskRef(j.Task)
		if err != nil || ref != p.Ref {
			return artifactCorrupt()
		}
	}
	for id, p := range st.ManualInputs {
		h, err := headerRow(id)
		if err != nil || h.ID != id || h.JobID != p.JobID || h.Principal != p.Owner || !h.NeedsManual || h.Task != nil || h.TaskDigest != p.TaskDigest {
			return artifactCorrupt()
		}
	}
	// Results and workspaces may outlive the job only under its existing
	// bounded manual header. They never become an unrelated global pin set.
	subject := func(owner, job, attempt string) (string, error) {
		j, err := jobRow(job)
		if err == nil {
			if ownerOf(job) != owner || !slices.Contains(executionIDs(j), attempt) {
				return "", artifactCorrupt()
			}
			return j.TargetBinding, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return "", artifactCorrupt()
		}
		h, err := headerRow(job)
		if err != nil || h.JobID != job || h.Principal != owner || !h.NeedsManual || h.Task != nil || !slices.Contains(h.ExecutionIDs, attempt) {
			return "", artifactCorrupt()
		}
		return h.Binding, nil
	}
	for _, r := range st.Results {
		if _, err := subject(r.Owner, r.JobID, r.AttemptID); err != nil {
			return err
		}
	}
	for _, r := range st.Expired {
		if r.Collected {
			if _, err := jobRow(r.JobID); !errors.Is(err, ErrNotFound) {
				return artifactCorrupt()
			}
			continue
		}
		if _, err := subject(r.Owner, r.JobID, r.AttemptID); err != nil {
			return err
		}
	}
	for _, r := range st.Completions {
		if _, err := subject(r.Owner, r.JobID, r.AttemptID); err != nil {
			return err
		}
		if j, err := jobRow(r.JobID); err == nil {
			d, err := taskDigest(j.Task)
			if err != nil || d != r.TaskDigest {
				return artifactCorrupt()
			}
		} else {
			h, err := headerRow(r.JobID)
			if err != nil || h.TaskDigest != r.TaskDigest {
				return artifactCorrupt()
			}
		}
		workspace, rootPresent := st.Workspaces[r.AttemptID]
		if rootPresent && (!workspace.WriterClosed || workspace.Held) {
			return artifactCorrupt()
		}
		if r.RootRemoved == rootPresent {
			return artifactCorrupt()
		}
	}
	for _, w := range st.Workspaces {
		binding, err := subject(w.Owner, w.JobID, w.AttemptID)
		if err != nil {
			return err
		}
		if w.Binding != binding {
			return artifactCorrupt()
		}
		p, ok := st.JobInputs[w.JobID]
		if !ok {
			p, ok = st.ManualInputs[w.JobID]
		}
		if !ok || p.Owner != w.Owner || p.Ref != w.Input {
			return artifactCorrupt()
		}
	}
	return nil
}

// artifactAcceptInput belongs at the end of the existing accepting transaction,
// after the accepted job/principal/index rows exist. The public submit replay
// branches must return before reaching it. Failure rolls back the whole job.
func artifactAcceptInput(tx *bolt.Tx, st *artifactState, owner, job string, ref ArtifactRef, now time.Time) error {
	if err := artifactGranted(st, owner, ref, now); err != nil {
		return err
	}
	j, err := get(tx, job)
	if err != nil {
		return err
	}
	if isScoped(tx) && string(tx.Bucket(jobPrincipalsBucket).Get([]byte(job))) != owner || !isScoped(tx) && owner != "legacy" {
		return ErrArtifactConflict
	}
	d, err := taskDigest(j.Task)
	if err != nil {
		return err
	}
	accepted, err := artifactTaskRef(j.Task)
	if err != nil || accepted != ref {
		return ErrArtifactConflict
	}
	if _, ok := st.JobInputs[job]; ok {
		return ErrArtifactConflict
	}
	st.JobInputs[job] = artifactPin{Owner: owner, JobID: job, Ref: ref, TaskDigest: d}
	return nil
}

// artifactTransferInput joins MetadataOnly's NET plan. It accepts ONLY an
// already-created taskless needs_manual header for this exact removed job.
// No Task is copied and no closure/native authority is minted.
func artifactTransferInput(tx *bolt.Tx, st *artifactState, job, header string) error {
	p, ok := st.JobInputs[job]
	if !ok {
		return nil
	}
	var h cleanupHeader
	if err := loadJSON(retentionTable(tx, cleanupHeadersBucket), []byte(header), &h); err != nil {
		return err
	}
	if h.ID != header || h.JobID != job || h.Principal != p.Owner || !h.NeedsManual || h.Task != nil || h.TaskDigest != p.TaskDigest {
		return ErrArtifactConflict
	}
	if _, ok := st.ManualInputs[header]; ok {
		return ErrArtifactConflict
	}
	st.ManualInputs[header] = p
	delete(st.JobInputs, job)
	return nil
}

func (st *artifactState) releaseGrace(id string, now time.Time) {
	if st.pinned(id) {
		return
	}
	o, ok := st.Objects[id]
	if !ok {
		return
	}
	if until := now.UTC().Add(time.Hour); o.DeleteAfter.Before(until) {
		o.DeleteAfter = until
	}
	if o.DeleteAfter.Before(o.Expires) {
		o.DeleteAfter = o.Expires
	}
	st.Objects[id] = o
}

// artifactDropInput is called ONLY in Whole's existing proof-authorized job
// removal transaction. Terminal status is not this authorization. The caller
// first settles all workspace/result protections or leaves the job held.
func artifactDropInput(st *artifactState, job string, now time.Time) error {
	for _, w := range st.Workspaces {
		if w.JobID == job {
			return ErrArtifactConflict
		}
	}
	for _, r := range st.Results {
		if r.JobID == job {
			return ErrArtifactConflict
		}
	}
	for id, r := range st.Expired {
		if r.JobID != job {
			continue
		}
		if _, exists := st.Objects[r.ObjectID]; exists {
			r.Collected = true
			st.Expired[id] = r
		} else {
			delete(st.Expired, id)
		}
	}
	for id, r := range st.Completions {
		if r.JobID == job {
			delete(st.Completions, id)
		}
	}
	if p, ok := st.JobInputs[job]; ok {
		delete(st.JobInputs, job)
		st.releaseGrace(p.Ref.ID, now)
	}
	return nil
}

// artifactBeginWorkspace joins the real Starting transaction AFTER its early
// cancel branch. resultID is allocated before the transaction but becomes
// durable only here. No filesystem operation runs while this transaction holds.
func artifactBeginWorkspace(tx *bolt.Tx, st *artifactState, job, attempt, resultID string, outputs bool, now time.Time) error {
	p, ok := st.JobInputs[job]
	if !ok {
		return ErrArtifactConflict
	}
	j, err := get(tx, job)
	if err != nil {
		return err
	}
	if j.CurrentAttemptID() != attempt || j.Status.State != backend.Starting || !artifactHex(attempt, 32) || j.TargetBinding == "" {
		return ErrArtifactConflict
	}
	d, err := taskDigest(j.Task)
	if err != nil || d != p.TaskDigest {
		return ErrArtifactConflict
	}
	if _, ok := st.Workspaces[attempt]; ok {
		return ErrArtifactConflict
	}
	if err := artifactGranted(st, p.Owner, p.Ref, now); err != nil {
		return err
	}
	w := artifactWorkspace{Owner: p.Owner, JobID: job, AttemptID: attempt, Binding: j.TargetBinding, Input: p.Ref}
	if outputs {
		if !artifactHex(resultID, 32) {
			return ErrArtifactConflict
		}
		if tx.Bucket(jobsBucket).Get([]byte(resultID)) != nil || tx.Bucket(attemptJobsBucket).Get([]byte(resultID)) != nil {
			return ErrArtifactConflict
		}
		if _, ok := st.Objects[resultID]; ok {
			return ErrArtifactConflict
		}
		if _, ok := st.Uploads[resultID]; ok {
			return ErrArtifactConflict
		}
		w.ResultID = resultID
		// A reservation deliberately has NO digest. It cannot become a Ref
		// or grant until the trusted sealed candidate supplies actual bytes.
		expires := now.UTC().Add(24 * time.Hour)
		st.Objects[resultID] = artifactObject{Owner: p.Owner, JobID: job, Ref: ArtifactRef{ID: resultID, Size: artifactMaxSize}, Origin: "result", Source: attempt, State: "reserved", Expires: expires, DeleteAfter: expires}
	} else if resultID != "" {
		return ErrArtifactConflict
	}
	st.Workspaces[attempt] = w
	return nil
}

// artifactWorkspaceBound consumes the actual local ownership handoff before
// launch. A replacement instance cannot adopt a prior workspace.
func artifactWorkspaceBound(st *artifactState, attempt, binding, instance string) error {
	w, ok := st.Workspaces[attempt]
	if !ok || w.Binding != binding || !validInstance(instance) || instance == "" || w.Instance != "" {
		return ErrArtifactConflict
	}
	w.Instance = instance
	st.Workspaces[attempt] = w
	return nil
}

// artifactWorkspaceQuiescent accepts the distinct trusted writer-ownership
// receipt. It is NOT an S05 stopped receipt and never authorizes task retry or
// native cleanup. Default jobs need this receipt without a synthetic Retry.
func artifactWorkspaceQuiescent(st *artifactState, attempt, instance string, receipt [32]byte) error {
	w, ok := st.Workspaces[attempt]
	if !ok || w.Instance == "" || w.Instance != instance || receipt == ([32]byte{}) {
		return ErrArtifactConflict
	}
	if w.WriterClosed && w.WriterReceipt != receipt {
		return ErrArtifactConflict
	}
	w.WriterClosed = true
	w.WriterReceipt = receipt
	st.Workspaces[attempt] = w
	return nil
}

// artifactPublishResult must join the SAME transaction as the trusted actual
// observeAttempt terminal/retry decision. It publishes no intermediate result.
// The sealed receipt implies the owned candidate's full hash/archive checks,
// exclusive publication and directory syncs, not a client-supplied assertion.
func artifactPublishResult(st *artifactState, attempt, instance string, ref ArtifactRef, file ArtifactFile, manifest ArtifactManifest, receipt [32]byte, now time.Time) error {
	w, ok := st.Workspaces[attempt]
	if !ok || !w.WriterClosed || w.Instance != instance || w.ResultID != ref.ID || !artifactRefValid(ref) || !artifactFileValid(file) || !artifactManifestValid(manifest, ref) || receipt == ([32]byte{}) {
		return ErrArtifactConflict
	}
	o, ok := st.Objects[w.ResultID]
	if !ok || o.State != "reserved" || o.Owner != w.Owner || o.Origin != "result" || o.Source != attempt || o.JobID != w.JobID {
		return ErrArtifactConflict
	}
	if _, ok := st.Results[attempt]; ok {
		return ErrArtifactConflict
	}
	if _, ok := st.Expired[attempt]; ok {
		return ErrArtifactConflict
	}
	o.Ref = ref
	o.File = file
	o.Manifest = manifest
	o.State = "complete"
	o.Expires = now.UTC().Add(24 * time.Hour)
	o.DeleteAfter = o.Expires
	st.Objects[ref.ID] = o
	st.Results[attempt] = artifactResult{Owner: w.Owner, JobID: w.JobID, AttemptID: attempt, Ref: ref, Receipt: receipt}
	st.Grants[ref.ID] = artifactGrant{Owner: w.Owner, Ref: ref, Source: "result", SourceID: attempt, Expires: o.Expires}
	return nil
}

// artifactExpireResult joins explicit ResultOnly pruning. Revoking its grant
// and relation atomically prevents a cached Ref from starting another read.
// Already active reads still prevent deletion until their real close receipt.
func artifactExpireResult(st *artifactState, owner, job, attempt string, now time.Time) error {
	r, ok := st.Results[attempt]
	if !ok || r.Owner != owner || r.JobID != job {
		return ErrNotFound
	}
	// The selected result relation is itself a pin, so generic pinned would
	// always refuse. Check every OTHER concrete durable consumer instead.
	for _, read := range st.Reads {
		if !read.Closed && read.Ref.ID == r.Ref.ID {
			return ErrArtifactConflict
		}
	}
	for _, pin := range st.JobInputs {
		if pin.Ref.ID == r.Ref.ID {
			return ErrArtifactConflict
		}
	}
	for _, pin := range st.ManualInputs {
		if pin.Ref.ID == r.Ref.ID {
			return ErrArtifactConflict
		}
	}
	for _, w := range st.Workspaces {
		if w.ResultID == r.Ref.ID || w.Input.ID == r.Ref.ID {
			return ErrArtifactConflict
		}
	}
	delete(st.Grants, r.Ref.ID)
	delete(st.Results, attempt)
	st.releaseGrace(r.Ref.ID, now)
	st.Expired[attempt] = artifactExpiredResult{Owner: owner, JobID: job, AttemptID: attempt, Expired: now.UTC(), ObjectID: r.Ref.ID}
	return nil
}

// artifactWorkspaceRemoved consumes only a committed workspace intent after
// the trusted worker settled BOTH external candidate and original root ownership.
// Native execution retirement preceded Mark; shutdown handle-only handoff is not
// this receipt. Missing paths alone never authorize release.
func artifactWorkspaceRemoved(st *artifactState, operation string, expected artifactDelete, now time.Time) error {
	d, ok := st.Deletes[operation]
	if !ok || d.Owner != expected.Owner || d.Kind != expected.Kind || d.ObjectID != expected.ObjectID || d.File != expected.File || d.Revision != expected.Revision || d.Workspace == nil || expected.Workspace == nil || !artifactSameWorkspaceDelete(d.Workspace, expected.Workspace) {
		return ErrArtifactConflict
	}
	if err := artifactWorkspaceDeleteValid(st, d); err != nil {
		return err
	}
	w := st.Workspaces[d.ObjectID]
	completed := st.Completions[d.ObjectID]
	if w.ResultID != "" {
		switch d.Workspace.Disposition {
		case WorkspaceCandidateNeverCreated, WorkspaceCandidateClosedUnpublished:
			// The caller's positive receipt covers independent two-name
			// candidate disposition, not just unlink of the workspace root.
			delete(st.Objects, w.ResultID)
		case WorkspaceResultPublishedIndependent:
			// The completed result keeps its independent File/Ref/grant.
		default:
			return ErrArtifactConflict
		}
	}
	delete(st.Workspaces, d.ObjectID)
	delete(st.Deletes, operation)
	completed.RootRemoved = true
	st.Completions[d.ObjectID] = completed
	st.releaseGrace(w.Input.ID, now)
	if w.ResultID != "" {
		st.releaseGrace(w.ResultID, now)
	}
	return nil
}
