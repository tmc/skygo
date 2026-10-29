package store

import (
	"errors"
	"fmt"
	"time"
)

const artifactMaxSize = 32 << 20
const artifactChunkSize = 64 << 10

var (
	ErrArtifactsDisabled = errors.New("artifact metadata not installed")
	ErrArtifactConflict  = errors.New("artifact operation conflict")
	ErrArtifactExpired   = errors.New("artifact expired")
)

// ArtifactRef identifies one immutable object, not permission to read it.
// A digest alone never grants access or deduplicates another owner's object.
type ArtifactRef struct {
	ID     string
	SHA256 string
	Size   uint64
}

// ArtifactManifest is supplied only by the SAME trusted archive parser that
// validated and hashed Reference. Counts include implicit directories; they
// are never accepted from a client or inferred from a missing companion file.
type ArtifactManifest struct {
	Reference     ArtifactRef
	ExpandedBytes uint64
	RegularFiles  uint64
	Directories   uint64
}

// ArtifactAttemptResult distinguishes absent and expired result metadata.
// Expired has no Reference. Whole-job expiry retains its existing S03 meaning.
type ArtifactAttemptResult struct {
	State     string
	AttemptID string
	Reference *ArtifactRef
}

// ArtifactUpload is the durable, owner-scoped upload cursor.
// Revision is a compare-and-swap token; it does not authorize filesystem work.
type ArtifactUpload struct {
	Reference    ArtifactRef
	Revision     uint64
	State        string
	Acknowledged uint64
	ExpiresAt    time.Time
}

// ArtifactFile identifies an already-opened owned regular file. Only the
// trusted filesystem adapter may supply it, after checking the bound root.
// These receipts are in-process handoffs, never RPC request parameters.
type ArtifactFile struct {
	Device uint64
	Inode  uint64
}

// ArtifactCursor binds a filesystem operation to a durable upload generation.
type ArtifactCursor struct {
	ID       string
	Revision uint64
}

// ArtifactLease protects an owned object until the actual reader has closed.
// Expiration makes a lease eligible for closing, not for counter credit.
type ArtifactLease struct {
	ID        string
	Reference ArtifactRef
	Revision  uint64
	ExpiresAt time.Time
	Closing   bool
	File      ArtifactFile
	RootID    string
	Producer  OriginalServerLifetime
}

// ArtifactInstallation is persisted configuration, not a filesystem sample.
// Enabled false leaves the remaining fields unavailable, not measured zeros.
type ArtifactInstallation struct {
	Enabled bool
	ID      string
	Unit    uint64
	Volume  uint64
}

// ArtifactDownloadClosure is captured by the original admitted reader owner.
// It is cleanup-only and never populated from an RPC or client/context value.
// Finish additionally requires actual I/O+HTTP joins and FD close at its caller;
// these identity fields alone do not prove that physical event occurred.
type ArtifactDownloadClosure struct {
	Owner    string
	LeaseID  string
	Revision uint64
	Ref      ArtifactRef
	File     ArtifactFile
	RootID   string
	Producer OriginalServerLifetime
}

// ArtifactCreateAbandonment comes only from the original admitted create
// owner after its actual I/O and FD owners settle. It records cleanup identity,
// never writable bytes, a read grant or a client-selected inode adoption.
type ArtifactCreateAbandonment struct {
	Owner  string
	Cursor ArtifactCursor
	Ref    ArtifactRef
	File   ArtifactFile
	RootID string
}

// ArtifactCapacityError is a held admission, not a backend execution failure.
type ArtifactCapacityError struct {
	Domain string
	Used   uint64
	Limit  uint64
}

func (e *ArtifactCapacityError) Error() string {
	return fmt.Sprintf("artifact %s capacity: used %d, limit %d", e.Domain, e.Used, e.Limit)
}

// artifactRoot is installed only by the explicit stopped installer.
// ID binds companion bytes; Unit/Volume come from an actual root probe.
type artifactRoot struct {
	Layout      uint64
	SchemaFloor uint64
	Policy      string
	ID          string
	Unit        uint64
	Volume      uint64
}

type artifactUploadRecord struct {
	RemoteAttemptID string `json:",omitempty"`
	Owner           string
	Ref             ArtifactRef
	Revision        uint64
	State           string
	Ack             uint64
	Created         time.Time
	Expires         time.Time
	Part            ArtifactFile
	LastOffset      uint64
	LastSize        uint64
	LastSHA256      string
}

// artifactBeginRequest fixes a scoped Begin's original upload identity. The
// witness survives upload collection until its domain is permanently retired.
type artifactBeginRequest struct {
	Owner      string
	Domain     RequestDomain
	RequestID  string
	Ref        ArtifactRef
	Created    time.Time
	RetryUntil time.Time
}

type artifactObject struct {
	Owner    string
	JobID    string
	Ref      ArtifactRef
	Origin   string
	Source   string
	State    string
	File     ArtifactFile
	Manifest ArtifactManifest
	Expires  time.Time
	// DeleteAfter survives read-grant revocation. Releasing the final
	// durable consumer may extend it, but can never renew a revoked grant.
	DeleteAfter time.Time
}

type artifactGrant struct {
	Owner    string
	Ref      ArtifactRef
	Source   string
	SourceID string
	Expires  time.Time
}

type artifactRead struct {
	RemoteAttemptID string `json:",omitempty"`
	Owner           string
	Ref             ArtifactRef
	Revision        uint64
	Expires         time.Time
	Closing         bool
	Closed          bool
	ClosedAt        time.Time
	Producer        *OriginalServerLifetime `json:",omitempty"`
	// An environment materialization read names its environment and the
	// committed incarnation, string and Store-assigned sequence, it was
	// admitted for. Only its original materializer settles it; it is never a
	// download.
	Environment    string `json:",omitempty"`
	Incarnation    string `json:",omitempty"`
	IncarnationSeq uint64 `json:",omitempty"`
}

// artifactPin is private and concrete: jobInputs and manualInputs have
// different actual lifecycle callers. No public general pin API is provided.
type artifactPin struct {
	Owner      string
	JobID      string
	Ref        ArtifactRef
	TaskDigest [32]byte
}

type artifactResult struct {
	Owner     string
	JobID     string
	AttemptID string
	Ref       ArtifactRef
	Receipt   [32]byte
}

type artifactExpiredResult struct {
	Owner     string
	JobID     string
	AttemptID string
	Expired   time.Time
	ObjectID  string
	Collected bool
}

type artifactWorkspace struct {
	Owner         string
	JobID         string
	AttemptID     string
	Binding       string
	Input         ArtifactRef
	ResultID      string
	Instance      string
	WriterClosed  bool
	WriterReceipt [32]byte
	Held          bool
	Diagnostic    string
}

type artifactCompletion struct {
	Owner            string
	JobID            string
	AttemptID        string
	TaskDigest       [32]byte
	Digest           [32]byte
	Outcome          WorkspaceOutcome
	OriginalTaskExit *int32
	RootRemoved      bool
}

type artifactDelete struct {
	Owner     string
	Kind      string
	ObjectID  string
	File      ArtifactFile
	Revision  uint64
	Workspace *artifactWorkspaceDelete `json:",omitempty"`
}

func artifactHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func artifactRefValid(r ArtifactRef) bool {
	return artifactHex(r.ID, 32) && artifactHex(r.SHA256, 64) && r.Size > 0 && r.Size <= artifactMaxSize
}

func artifactManifestValid(m ArtifactManifest, r ArtifactRef) bool {
	return m.Reference == r && artifactRefValid(r) && m.ExpandedBytes <= artifactMaxSize && m.RegularFiles <= 2048 && m.Directories <= 512 && (m.RegularFiles != 0 || m.ExpandedBytes == 0)
}

func artifactFileValid(f ArtifactFile) bool { return f.Device != 0 && f.Inode != 0 }

func artifactUploadDTO(r artifactUploadRecord) ArtifactUpload {
	return ArtifactUpload{Reference: r.Ref, Revision: r.Revision, State: r.State, Acknowledged: r.Ack, ExpiresAt: r.Expires}
}

func artifactLeaseDTO(id string, r artifactRead, file ArtifactFile, root string) ArtifactLease {
	return ArtifactLease{ID: id, Reference: r.Ref, Revision: r.Revision, ExpiresAt: r.Expires, Closing: r.Closing, File: file, RootID: root, Producer: artifactProducerValue(r.Producer)}
}
