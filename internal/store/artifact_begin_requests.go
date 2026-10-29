package store

import (
	"bytes"
	"errors"
	"sort"
	"time"

	"github.com/tmc/skygo/internal/auth"
	bolt "go.etcd.io/bbolt"
)

// A replay or an empty retirement rolls back its read-only recheck transaction,
// avoiding even a metadata-only bbolt commit after the public read-only lookup.
var errArtifactBeginNoChange = errors.New("artifact begin request unchanged")

func validArtifactBeginRequest(domain RequestDomain, requestID, sha string, size uint64) error {
	if !validRequestDomain(domain) {
		return ErrInvalidRequestDomain
	}
	if !validRetainedRequestID(requestID) || !artifactHex(sha, 64) || size == 0 || size > artifactMaxSize {
		return ErrArtifactConflict
	}
	return nil
}

// artifactBeginLookup only reads an already validated bounded artifact state.
// A retained witness whose upload was removed remains an expired request,
// never permission to reserve another upload under that same scoped key.
func artifactBeginLookup(st *artifactState, owner string, domain RequestDomain, requestID, sha string, size uint64, now time.Time) (ArtifactUpload, bool, error) {
	for _, r := range st.BeginRequests {
		if r.Owner != owner || r.Domain != domain || r.RequestID != requestID {
			continue
		}
		if r.Ref.SHA256 != sha || r.Ref.Size != size {
			return ArtifactUpload{}, true, ErrArtifactConflict
		}
		u, ok := st.Uploads[r.Ref.ID]
		if !ok || u.State == "deleting" {
			return ArtifactUpload{}, true, ErrArtifactExpired
		}
		if u.Owner != owner || u.Ref != r.Ref || u.RemoteAttemptID != "" {
			return ArtifactUpload{}, true, artifactCorrupt()
		}
		if u.State != "complete" && !now.Before(u.Expires) {
			return ArtifactUpload{}, true, ErrArtifactExpired
		}
		return artifactUploadDTO(u), true, nil
	}
	return ArtifactUpload{}, false, nil
}

func artifactBeginDomain(tx *bolt.Tx, domain RequestDomain, now time.Time) (retentionState, error, error) {
	if schemaNumber(tx) < artifactBeginRequestSchema {
		return retentionState{}, nil, ErrUnsupportedSchema
	}
	return gateDomain(tx, domain, now)
}

// Domain phase fences share the transaction's actual changed-value budget.
func artifactBeginPhaseBytes(tx *bolt.Tx, old []byte) (uint64, error) {
	current := retentionRoot(tx).Get(retentionStateKey)
	if bytes.Equal(old, current) {
		return 0, nil
	}
	used := uint64(len(retentionStateKey) + len(old) + len(current))
	if used > maxChangeBytes {
		return 0, errArtifactMutationBudget
	}
	return used, nil
}

func artifactBeginAccept(st retentionState, domain RequestDomain) (time.Time, error) {
	for _, epoch := range st.Epochs {
		if epoch.Number == domain.Epoch {
			if epoch.AcceptClosed {
				return time.Time{}, &ExpiryError{Reason: RequestEpochExpired}
			}
			return epoch.RetryUntil, nil
		}
	}
	return time.Time{}, ErrUnknownRequestEpoch
}

// LookupArtifactUploadRequest returns the principal/domain/request binding.
// It may persist existing monotonic domain closure fences, including on an
// expiry refusal, so clock rollback cannot reopen a refused epoch. Otherwise
// replay changes no metadata, quota, expiry or files. The bool reports a
// matching witness, including an expired upload or a conflicting payload.
func (s *Store) LookupArtifactUploadRequest(actor auth.Actor, domain RequestDomain, requestID, sha string, size uint64) (out ArtifactUpload, found bool, err error) {
	if err = s.authorize(actor, auth.Read); err != nil {
		return
	}
	if err = validArtifactBeginRequest(domain, requestID, sha, size); err != nil {
		return
	}
	var refusal error
	err = s.db.Update(func(tx *bolt.Tx) error {
		old := bytes.Clone(retentionRoot(tx).Get(retentionStateKey))
		now := s.now().UTC()
		retention, result, e := artifactBeginDomain(tx, domain, now)
		if e != nil {
			return e
		}
		refusal = result
		if refusal == nil {
			st, e := artifactLoad(tx)
			if e != nil {
				return e
			}
			out, found, refusal = artifactBeginLookup(st, actor.Principal(), domain, requestID, sha, size, now)
			if !found && refusal == nil {
				_, refusal = artifactBeginAccept(retention, domain)
			}
		}
		used, e := artifactBeginPhaseBytes(tx, old)
		if e != nil {
			return e
		}
		if used == 0 {
			return errArtifactBeginNoChange
		}
		return nil
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	if err == nil {
		err = refusal
	}
	if err != nil {
		out = ArtifactUpload{}
	}
	return
}

// BeginArtifactUploadRequest atomically reserves a new upload and its fixed
// request witness, or replays the original upload. Only created=true authorizes
// Control to attempt the first exclusive part creation. A replay never renews
// expiry, revises acknowledgment, adopts a part or retries an unknown creator.
func (s *Store) BeginArtifactUploadRequest(actor auth.Actor, domain RequestDomain, requestID, sha string, size uint64) (out ArtifactUpload, created bool, err error) {
	if err = s.authorize(actor, auth.Mutate); err != nil {
		return
	}
	if err = validArtifactBeginRequest(domain, requestID, sha, size); err != nil {
		return
	}
	if existing, found, e := s.LookupArtifactUploadRequest(actor, domain, requestID, sha, size); e != nil || found {
		return existing, false, e
	}
	id, err := newID()
	if err != nil {
		return ArtifactUpload{}, false, err
	}
	var refusal error
	admitted := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		old := bytes.Clone(retentionRoot(tx).Get(retentionStateKey))
		now := s.now().UTC()
		retention, result, e := artifactBeginDomain(tx, domain, now)
		if e != nil {
			return e
		}
		refusal = result
		var st *artifactState
		var found bool
		if refusal == nil {
			st, e = artifactLoad(tx)
			if e != nil {
				return e
			}
			out, found, refusal = artifactBeginLookup(st, actor.Principal(), domain, requestID, sha, size, now)
		}
		var retryUntil time.Time
		if !found && refusal == nil {
			retryUntil, refusal = artifactBeginAccept(retention, domain)
		}
		used, e := artifactBeginPhaseBytes(tx, old)
		if e != nil {
			return e
		}
		if found || refusal != nil {
			if used == 0 {
				return errArtifactBeginNoChange
			}
			return nil
		}
		_, e = artifactChange(tx, maxChangeBytes-used, func(st *artifactState) error {
			if _, ok := st.Uploads[id]; ok {
				return ErrArtifactConflict
			}
			if _, ok := st.Objects[id]; ok {
				return ErrArtifactConflict
			}
			if _, ok := st.BeginRequests[id]; ok {
				return ErrArtifactConflict
			}
			u := artifactUploadRecord{Owner: actor.Principal(), Ref: ArtifactRef{ID: id, SHA256: sha, Size: size}, Revision: 1, State: "creating", Created: now, Expires: now.Add(time.Hour)}
			st.Uploads[id] = u
			st.BeginRequests[id] = artifactBeginRequest{Owner: u.Owner, Domain: domain, RequestID: requestID, Ref: u.Ref, Created: now, RetryUntil: retryUntil}
			out = artifactUploadDTO(u)
			return nil
		})
		admitted = e == nil
		return e
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		err = nil
	}
	if err == nil {
		err = refusal
	}
	if err != nil {
		return ArtifactUpload{}, false, err
	}
	return out, admitted, nil
}

func artifactBeginRetired(st retentionState, domain RequestDomain) bool {
	return st.Enabled && (domain.Lineage != st.Lineage || domain.Epoch <= st.RetiredThrough)
}

// RetireArtifactUploadRequests removes at most 16 request witnesses after the
// existing domain gate permanently refuses their lineage/epoch. It never
// removes an upload, object, file owner or artifact byte charge. Physical owner
// settlement and artifact cleanup remain separate operations.
func (s *Store) RetireArtifactUploadRequests() (retired int, err error) {
	var candidate bool
	err = s.db.View(func(tx *bolt.Tx) error {
		if schemaNumber(tx) < artifactBeginRequestSchema {
			return nil
		}
		retention, e := loadRetention(tx)
		if e != nil {
			return e
		}
		advancePhases(&retention, s.now().UTC())
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		for _, r := range st.BeginRequests {
			candidate = candidate || artifactBeginRetired(retention, r.Domain)
		}
		return nil
	})
	if err != nil || !candidate {
		return 0, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		retention, e := loadRetention(tx)
		if e != nil {
			return e
		}
		old := bytes.Clone(retentionRoot(tx).Get(retentionStateKey))
		changed := advancePhases(&retention, s.now().UTC())
		budget := uint64(maxChangeBytes)
		if changed {
			if e := putJSON(retentionRoot(tx), retentionStateKey, retention); e != nil {
				return e
			}
			current := retentionRoot(tx).Get(retentionStateKey)
			used := uint64(len(retentionStateKey) + len(old) + len(current))
			if used > budget {
				return errArtifactMutationBudget
			}
			budget -= used
		}
		_, e = artifactChange(tx, budget, func(st *artifactState) error {
			var ids []string
			for id, r := range st.BeginRequests {
				if artifactBeginRetired(retention, r.Domain) {
					ids = append(ids, id)
				}
			}
			sort.Strings(ids)
			for _, id := range ids[:min(16, len(ids))] {
				delete(st.BeginRequests, id)
				retired++
			}
			if retired == 0 {
				return errArtifactBeginNoChange
			}
			return nil
		})
		return e
	})
	if errors.Is(err, errArtifactBeginNoChange) {
		return 0, nil
	}
	if err != nil {
		retired = 0
	}
	return
}
