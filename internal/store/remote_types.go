package store

import (
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/backend"
)

// RemoteKey identifies one accepted remote execution, never native authority.
type RemoteKey struct{ JobID, AttemptID string }

// RemoteHashes are purpose-separated token digests; plaintext remains in memory.
type RemoteHashes struct{ Input, Output [32]byte }

// RemoteStart is the original committed start intent. Replay never starts again.
type RemoteStart struct {
	Job      backend.Job
	Revision uint64
	Budgets  backend.DataBudgets
}

// RemoteCreation describes the SAME original StartData return, including errors.
// Succeeded means that return had nil error; Store separately decides activation.
type RemoteCreation struct {
	Resources backend.DataCreated
	Succeeded bool
}

// RemoteOutputSeal binds validated local server bytes, not a remote Pod inode.
type RemoteOutputSeal struct {
	Reference ArtifactRef
	File      ArtifactFile
	Manifest  ArtifactManifest
}

// RemoteCompletion is a trusted original-owner handoff, never a wire assertion.
// Native qualification and actual transfer/reader/writer joins precede this call.
type RemoteCompletion struct {
	Outcome   DataOutcome
	Stop      *backend.DataTaskStop
	NoLaunch  *backend.DataNoLaunch
	Collector *backend.ContainerExit
	Candidate *RemoteOutputSeal
}

// RemoteGrantKind selects one purpose-separated scope. Zero is invalid.
type RemoteGrantKind uint8

const (
	RemoteInput RemoteGrantKind = iota + 1
	RemoteOutput
)

// RemoteCloseReason records irreversible admission closure, not stop proof.
type RemoteCloseReason uint8

const (
	RemoteCanceled RemoteCloseReason = iota + 1
	RemoteExpired
	RemoteAuthorityLost
	RemotePhaseTimeout
	RemoteIdentityMismatch
	RemoteShutdown
)

// DataOutcome keeps main exit distinct from failures that cannot cause retry.
type DataOutcome uint8

const (
	DataMainExit DataOutcome = iota + 1
	DataStageFailure
	DataSetupFailure
	DataCollectionFailure
	DataCanceled
	DataTimeout
	DataUncertain
)

// RemoteRelease is metadata only; abort never restores a closed byte grant.
type RemoteRelease struct {
	State           uint8
	Revision        uint64
	TaskContainerID string
	TaskExitCode    *int32
}

// RemoteGrant is a detached checked scope generation, not caller authority.
// Only the trusted data service passes it after fresh delegation verification.
type RemoteGrant struct {
	Key     RemoteKey
	Kind    RemoteGrantKind
	Epoch   uint64
	Expires time.Time
}

// RemoteScope retains a fixed hash and monotonically closed admission state.
// States are1 pending,2 active,3 revoked/closed,4 normal input complete.
// State4 is input-only and preserves identity across later attempt revocation.
type RemoteScope struct {
	Hash             [32]byte
	Epoch            uint64
	State            uint8
	Expires, Renewed time.Time
}

// RemoteActive is a detached snapshot of the original trusted attempt facts.
type RemoteActive struct {
	Phase                                  backend.DataPhase
	Budgets                                backend.DataBudgets
	Began, StageDeadline, ResourceDeadline time.Time
	TaskDeadline, CollectionDeadline       *time.Time
	Input, Output                          *RemoteScope
	Running                                *backend.ContainerStart
	Stop                                   *backend.DataTaskStop
	NoLaunch                               *backend.DataNoLaunch
	Collector                              *backend.ContainerExit
	ReleaseRevision                        uint64
	ClosedReason                           RemoteCloseReason
	ResultID                               string
}

// RemoteDecision preserves semantic replay after retry and result reclamation.
type RemoteDecision struct {
	Digest       [32]byte
	Outcome      DataOutcome
	OriginalExit *int32
	Stop         *backend.DataTaskStop
	NoLaunch     *backend.DataNoLaunch
	Collector    *backend.ContainerExit
	ResultID     string
}

// RemoteAttempt is detached trusted metadata. Creation is immutable:0 unknown,
// 1 original success without activation,2 original error,3 success activated.
// State is1 accepted,2 active,3 completed,4 positively retired. Zero is invalid.
// Ack bits Pod1/Input2/Output4 record independent positive original retirements.
type RemoteAttempt struct {
	Key             RemoteKey
	Revision        uint64
	State, Creation uint8
	Authority       auth.Delegation
	Policy          backend.DataPolicy
	Resources       backend.DataCreated
	Active          *RemoteActive
	Done            *RemoteDecision
	Ack             uint8
}
