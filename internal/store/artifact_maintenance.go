package store

import (
	"crypto/sha256"
	"encoding/json"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// WorkspaceRootState distinguishes an original root from positively uncreated
// storage. Neither a missing path nor an unidentified failed create is evidence.
type WorkspaceRootState uint8

const (
	WorkspaceRootNeverCreated WorkspaceRootState = iota + 1
	WorkspaceRootOwned
)

// WorkspaceCandidateDisposition records the original sequencer's candidate
// ownership. A nil candidate after EEXIST or uncertain Close is not NeverCreated.
type WorkspaceCandidateDisposition uint8

const (
	WorkspaceNoOutputs WorkspaceCandidateDisposition = iota + 1
	WorkspaceCandidateNeverCreated
	WorkspaceCandidateClosedUnpublished
	WorkspaceResultPublishedIndependent
)

// ArtifactWorkspaceCleanupSubject is a trusted, one-View completed subject.
// ResultState is none, reserved or complete. Only complete has a ResultRef/File.
// These are durable identity fences, not physical closure or native stop proof.
type ArtifactWorkspaceCleanupSubject struct {
	Owner            string
	JobID            string
	AttemptID        string
	Binding          string
	Instance         string
	RootID           string
	TaskDigest       [32]byte
	CompletionDigest [32]byte
	WriterReceipt    [32]byte
	InputRef         ArtifactRef
	ResultID         string
	ResultState      string
	ResultRef        ArtifactRef
	ResultFile       ArtifactFile
}

// WorkspaceCleanupOrigin comes ONLY from the original Local owner after actual
// native execution/phase/pipe/log/collection retirement. The independent closed
// log record and cleanup-root handles remain owned. No client, restart scan or
// copied fields can establish that event. Mark must commit before first unlink.
type WorkspaceCleanupOrigin struct {
	Subject         ArtifactWorkspaceCleanupSubject
	Parent          ArtifactFile
	Root            ArtifactFile
	RootState       WorkspaceRootState
	Disposition     WorkspaceCandidateDisposition
	CandidateID     string
	CandidateFile   ArtifactFile
	CandidateSize   int64
	CandidateSHA256 string
	Producer        OriginalServerLifetime
}

// artifactWorkspaceDelete is the finite same-gc workspace payload. Subject
// digests, Task, binding and Instance stay in their protected original rows.
type artifactWorkspaceDelete struct {
	Parent          ArtifactFile
	Root            ArtifactFile
	RootState       WorkspaceRootState
	Disposition     WorkspaceCandidateDisposition
	CandidateID     string
	CandidateFile   ArtifactFile
	CandidateSize   int64
	CandidateSHA256 string
	Producer        *OriginalServerLifetime `json:",omitempty"`
}

// WorkspaceCleanupWork is an exact committed intent snapshot, not permission
// to reconstruct a missing live owner. Handoff closes owned cleanup handles
// without returning a WorkspaceCleanupReceipt or releasing durable charges.
type WorkspaceCleanupWork struct {
	ID       string
	Revision uint64
	Digest   [32]byte
	Origin   WorkspaceCleanupOrigin
}

// ArtifactDeleteWork identifies a committed object/upload deletion. File zero
// means interrupted create ownership is unknown and must never be dispatched.
type ArtifactDeleteWork struct {
	ID       string
	Kind     string
	Owner    string
	RootID   string
	Revision uint64
	Ref      ArtifactRef
	File     ArtifactFile
	Digest   [32]byte
}

// ArtifactCleanup contains exactly one finite maintenance operation. It is a
// read-only union; finishing workspace and object work uses different methods.
type ArtifactCleanup struct {
	Delete    *ArtifactDeleteWork
	Workspace *WorkspaceCleanupWork
}

// ArtifactDeleteReceipt is supplied after original owners join and the exact
// intended names are unlinked and their original parent positively synced.
// Fields alone prove neither physical event. Finalizing covers BOTH names.
type ArtifactDeleteReceipt struct{ Work ArtifactDeleteWork }

// WorkspaceCleanupReceipt requires actual root/candidate disposition, cleanup
// descriptor closure and parent Sync. Native retirement precedes Mark already.
// A positive-close/no-credit shutdown handoff MUST NOT produce this receipt.
type WorkspaceCleanupReceipt struct{ Work WorkspaceCleanupWork }

func artifactCleanupSubject(st *artifactState, job, attempt string) (ArtifactWorkspaceCleanupSubject, error) {
	w, ok := st.Workspaces[attempt]
	if !ok || w.JobID != job {
		return ArtifactWorkspaceCleanupSubject{}, ErrNotFound
	}
	c, done := st.Completions[attempt]
	if !done || c.JobID != job || !w.WriterClosed || w.Held || w.Instance == "" || c.RootRemoved || c.Owner != w.Owner || c.AttemptID != attempt || c.TaskDigest == ([32]byte{}) || c.Digest == ([32]byte{}) || w.WriterReceipt == ([32]byte{}) {
		return ArtifactWorkspaceCleanupSubject{}, ErrArtifactConflict
	}
	out := ArtifactWorkspaceCleanupSubject{Owner: w.Owner, JobID: job, AttemptID: attempt, Binding: w.Binding, Instance: w.Instance, RootID: st.Root.ID, TaskDigest: c.TaskDigest, CompletionDigest: c.Digest, WriterReceipt: w.WriterReceipt, InputRef: w.Input, ResultID: w.ResultID, ResultState: "none"}
	if w.ResultID != "" {
		o, ok := st.Objects[w.ResultID]
		if !ok || o.Owner != w.Owner || o.JobID != job || o.Source != attempt || o.Origin != "result" {
			return out, ErrArtifactConflict
		}
		out.ResultState = o.State
		switch o.State {
		case "reserved":
		case "complete":
			out.ResultRef = o.Ref
			out.ResultFile = o.File
		default:
			return out, ErrArtifactConflict
		}
	}
	return out, nil
}

func workspaceDeletePayload(o WorkspaceCleanupOrigin) artifactWorkspaceDelete {
	var p *OriginalServerLifetime
	if o.Producer != (OriginalServerLifetime{}) {
		value := o.Producer
		p = &value
	}
	return artifactWorkspaceDelete{Parent: o.Parent, Root: o.Root, RootState: o.RootState, Disposition: o.Disposition, CandidateID: o.CandidateID, CandidateFile: o.CandidateFile, CandidateSize: o.CandidateSize, CandidateSHA256: o.CandidateSHA256, Producer: p}
}

func artifactSameWorkspaceDelete(a, b *artifactWorkspaceDelete) bool {
	if a == nil || b == nil {
		return a == b
	}
	x, y := *a, *b
	x.Producer = nil
	y.Producer = nil
	return x == y && artifactProducerValue(a.Producer) == artifactProducerValue(b.Producer)
}

func artifactWorkspaceDeleteValid(st *artifactState, d artifactDelete) error {
	p := d.Workspace
	if d.Kind != "workspace" || d.File != (ArtifactFile{}) || d.Revision == 0 || p == nil {
		return artifactCorrupt()
	}
	if !artifactProducerValid(p.Producer) {
		return artifactCorrupt()
	}
	w, ok := st.Workspaces[d.ObjectID]
	if !ok || w.Owner != d.Owner {
		return artifactCorrupt()
	}
	subject, err := artifactCleanupSubject(st, w.JobID, d.ObjectID)
	if err != nil {
		return artifactCorrupt()
	}
	if !artifactFileValid(p.Parent) || p.Parent.Device != st.Root.Volume {
		return artifactCorrupt()
	}
	switch p.RootState {
	case WorkspaceRootNeverCreated:
		if p.Root != (ArtifactFile{}) {
			return artifactCorrupt()
		}
	case WorkspaceRootOwned:
		if !artifactFileValid(p.Root) || p.Root.Device != st.Root.Volume || p.Root == p.Parent {
			return artifactCorrupt()
		}
	default:
		return artifactCorrupt()
	}
	candidateZero := p.CandidateID == "" && p.CandidateFile == (ArtifactFile{}) && p.CandidateSize == 0 && p.CandidateSHA256 == ""
	switch p.Disposition {
	case WorkspaceNoOutputs:
		if subject.ResultState != "none" || !candidateZero {
			return artifactCorrupt()
		}
	case WorkspaceCandidateNeverCreated:
		if subject.ResultState != "reserved" || !candidateZero {
			return artifactCorrupt()
		}
	case WorkspaceCandidateClosedUnpublished:
		if p.RootState != WorkspaceRootOwned || subject.ResultState != "reserved" || p.CandidateID != subject.ResultID || !artifactHex(p.CandidateID, 32) || !artifactFileValid(p.CandidateFile) || p.CandidateFile.Device != st.Root.Volume || p.CandidateFile == p.Parent || p.CandidateFile == p.Root || p.CandidateSize < 0 || p.CandidateSize > artifactMaxSize || !artifactHex(p.CandidateSHA256, 64) {
			return artifactCorrupt()
		}
	case WorkspaceResultPublishedIndependent:
		if p.RootState != WorkspaceRootOwned || subject.ResultState != "complete" || !candidateZero {
			return artifactCorrupt()
		}
	default:
		return artifactCorrupt()
	}
	return nil
}

func artifactWorkspaceWork(st *artifactState, id string, d artifactDelete) (WorkspaceCleanupWork, error) {
	if err := artifactWorkspaceDeleteValid(st, d); err != nil {
		return WorkspaceCleanupWork{}, err
	}
	w := st.Workspaces[d.ObjectID]
	subject, err := artifactCleanupSubject(st, w.JobID, d.ObjectID)
	if err != nil {
		return WorkspaceCleanupWork{}, err
	}
	p := d.Workspace
	work := WorkspaceCleanupWork{ID: id, Revision: d.Revision, Origin: WorkspaceCleanupOrigin{Subject: subject, Parent: p.Parent, Root: p.Root, RootState: p.RootState, Disposition: p.Disposition, CandidateID: p.CandidateID, CandidateFile: p.CandidateFile, CandidateSize: p.CandidateSize, CandidateSHA256: p.CandidateSHA256, Producer: artifactProducerValue(p.Producer)}}
	raw, err := json.Marshal(work)
	if err != nil {
		return WorkspaceCleanupWork{}, err
	}
	work.Digest = sha256.Sum256(raw) // identity CAS, never physical proof
	return work, nil
}

func artifactDeleteWork(st *artifactState, id string, d artifactDelete) (ArtifactDeleteWork, error) {
	if d.Kind != "object" && d.Kind != "upload" && d.Kind != "finalizing" || d.Workspace != nil {
		return ArtifactDeleteWork{}, ErrArtifactConflict
	}
	var ref ArtifactRef
	if d.Kind == "object" {
		ref = st.Objects[d.ObjectID].Ref
	} else {
		ref = st.Uploads[d.ObjectID].Ref
	}
	if !artifactRefValid(ref) || !artifactFileValid(d.File) {
		return ArtifactDeleteWork{}, ErrArtifactConflict
	}
	work := ArtifactDeleteWork{ID: id, Kind: d.Kind, Owner: d.Owner, RootID: st.Root.ID, Revision: d.Revision, Ref: ref, File: d.File}
	raw, err := json.Marshal(work)
	if err != nil {
		return work, err
	}
	work.Digest = sha256.Sum256(raw)
	return work, nil
}

// NextArtifactCleanup selects bounded validated committed work, wrapping once
// after a lower-hex operation cursor. Unknown creating File0 is held, not sent
// to Files. Selection is read-only and works with job retention disabled.
func (s *Store) NextArtifactCleanup(after string) (out ArtifactCleanup, found bool, err error) {
	if after != "" && !artifactHex(after, 32) {
		return out, false, ErrArtifactConflict
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		st, e := artifactLoad(tx)
		if e != nil {
			return e
		}
		ids := make([]string, 0, len(st.Deletes))
		for id := range st.Deletes {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		start := sort.Search(len(ids), func(i int) bool { return ids[i] > after })
		for n := 0; n < len(ids); n++ {
			id := ids[(start+n)%len(ids)]
			d := st.Deletes[id]
			if d.Kind == "workspace" {
				work, e := artifactWorkspaceWork(st, id, d)
				if e != nil {
					return e
				}
				out.Workspace = &work
			} else {
				if !artifactFileValid(d.File) {
					continue
				} // unknown original create; retain all charges
				work, e := artifactDeleteWork(st, id, d)
				if e != nil {
					return e
				}
				out.Delete = &work
			}
			found = true
			return nil
		}
		return nil
	})
	return
}

// FinishArtifactDelete consumes the exact committed deletion and actual owned
// unlink+parent-sync receipt. Nil error with retired=false means only a bounded
// <=64 closed-reader tail was collected; the sole fair worker must reselect it.
// Actual filesystem receipts are produced outside this transaction.
func (s *Store) FinishArtifactDelete(expected ArtifactDeleteWork, receipt ArtifactDeleteReceipt) (retired bool, err error) {
	if expected != receipt.Work {
		return false, ErrArtifactConflict
	}
	err = s.artifactUpdate(func(st *artifactState) error {
		d, ok := st.Deletes[expected.ID]
		if !ok {
			return ErrNotFound
		}
		actual, e := artifactDeleteWork(st, expected.ID, d)
		if e != nil {
			return e
		}
		if actual != expected {
			return ErrArtifactConflict
		}
		if e := artifactFinishDelete(st, expected.ID, d); e != nil {
			return e
		}
		_, remaining := st.Deletes[expected.ID]
		retired = !remaining
		return nil
	})
	if err != nil {
		retired = false
	}
	return
}

// FinishWorkspaceCleanup credits only actual independent candidate settlement
// AND root unlink/parent-sync/cleanup-owner retirement. A shutdown handoff that
// merely closes original root handles must not call it. Native retirement was
// a prerequisite of the original Mark, never inferred here from absent paths.
func (s *Store) FinishWorkspaceCleanup(expected WorkspaceCleanupWork, receipt WorkspaceCleanupReceipt) error {
	if expected != receipt.Work {
		return ErrArtifactConflict
	}
	return s.artifactUpdate(func(st *artifactState) error {
		d, ok := st.Deletes[expected.ID]
		if !ok {
			return ErrNotFound
		}
		actual, e := artifactWorkspaceWork(st, expected.ID, d)
		if e != nil {
			return e
		}
		if actual != expected {
			return ErrArtifactConflict
		}
		return artifactWorkspaceRemoved(st, expected.ID, d, s.now())
	})
}
