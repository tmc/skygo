package store

import (
	"errors"
	"time"

	"github.com/tmc/skygo/internal/auth"
	"github.com/tmc/skygo/internal/environment"
)

var (
	// ErrEnvironmentStale reports a commit whose expected revision is not
	// the environment's current revision. Nothing was written.
	ErrEnvironmentStale = errors.New("environment revision changed")

	// ErrEnvironmentReserved reports a name already used by the principal,
	// or a storage alias already used on the target. Deletion releases
	// neither; only ReclaimEnvironment does.
	ErrEnvironmentReserved = errors.New("environment name or storage alias is reserved")

	// ErrEnvironmentLimit reports an admission bound. Accepted
	// environments, including deleted, unknown and legacy ones, count until
	// ReclaimEnvironment retires them, and none is evicted to make room.
	ErrEnvironmentLimit = errors.New("environment limit reached")

	// ErrEnvironmentArtifact reports an artifact mount the principal may
	// not pin: missing, foreign, incomplete, mismatched, expired or past
	// the required inventory bound. Nothing was written.
	ErrEnvironmentArtifact = errors.New("environment artifact unavailable")

	// ErrEnvironmentArtifactHeld reports an open materialization read of
	// the environment. It refuses the Deleted commit and every incarnation
	// change until the read is settled, and ReclaimEnvironment until no
	// read of the environment, settled or not, remains. Nothing was written.
	ErrEnvironmentArtifactHeld = errors.New("environment artifact read open")

	// ErrEnvironmentIncarnation reports a materialization whose expected
	// incarnation sequence or incarnation is not the committed one.
	ErrEnvironmentIncarnation = errors.New("environment incarnation is not current")

	// ErrEnvironmentMaterialized reports that the incarnation's read of
	// the artifact is already settled. A new incarnation is required to
	// read it again.
	ErrEnvironmentMaterialized = errors.New("environment artifact already materialized")

	// ErrEnvironmentMaterializationHeld reports an unsettled read of the
	// artifact that this call cannot prove its own: another server
	// lifetime's, or one whose stored or requested lifetime is not fully
	// known. It is never released by retrying.
	ErrEnvironmentMaterializationHeld = errors.New("environment artifact read held by another server lifetime")
)

// Environment admission bounds. Every environment counts until
// ReclaimEnvironment retires it.
const (
	maxEnvironments             = 32
	maxPrincipalEnvironments    = 4
	maxEnvironmentMetadataBytes = 16 << 20
)

// Environment is one accepted environment. Identity, Principal, RequestID,
// Spec, Binding and Authority are fixed at acceptance; State, Checkpoint and
// Incarnation change only by CommitEnvironment, which advances Revision. Checkpoint is server-only provider state: callers must
// never return it to clients.
type Environment struct {
	Identity   environment.Identity
	Principal  string
	RequestID  string
	Spec       environment.Spec
	SpecDigest [32]byte
	Binding    string
	Authority  auth.Delegation
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Revision   uint64
	State      environment.State
	Checkpoint environment.Checkpoint

	// Incarnation is the committed runtime incarnation, and IncarnationSeq
	// is the Store-assigned count of its changes. Both are zero until the
	// first incarnation is committed.
	Incarnation    string
	IncarnationSeq uint64

	// Domain is the request domain of acceptance, or nil for an
	// environment accepted before schema 12.
	Domain *RequestDomain

	// Retired reports the view of an environment that ReclaimEnvironment
	// retired: Desired Absent, Phase Deleted, and no Authority, Checkpoint
	// or incarnation. The Store keeps only the spec digest; Spec is the
	// replayed request's spec, whose canonical bytes hash exactly to
	// SpecDigest.
	Retired bool
}

// EnvironmentPage is one listing page. After is a listing ordinal.
type EnvironmentPage struct {
	Environments []Environment
	After        uint64
	More         bool
}

// EnvironmentCommit is the next durable state and checkpoint of an
// environment. The checkpoint's kind and version must not change.
type EnvironmentCommit struct {
	State      environment.State
	Checkpoint environment.Checkpoint
	// Incarnation, if not empty and not the committed one, is the next
	// runtime-observed incarnation: 1..128 bytes of letters, digits, '.',
	// '_', ':' or '-'. It may change only while the environment is and
	// stays Present, never while a materialization read of the environment
	// is open, and each change advances IncarnationSeq. Empty keeps the
	// committed incarnation; it is never cleared.
	Incarnation string
}
