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
	// or a storage alias already used on the target. Reservations are
	// never released by deletion.
	ErrEnvironmentReserved = errors.New("environment name or storage alias is reserved")

	// ErrEnvironmentLimit reports an admission bound. Accepted
	// environments, including deleted and unknown ones, are never evicted
	// to make room.
	ErrEnvironmentLimit = errors.New("environment limit reached")
)

// Environment admission bounds. Every accepted row counts until a separately
// qualified retention policy exists.
const (
	maxEnvironments             = 32
	maxPrincipalEnvironments    = 4
	maxEnvironmentMetadataBytes = 16 << 20
)

// Environment is one accepted environment. Identity, Principal, RequestID,
// Spec, Binding and Authority are fixed at acceptance; State and Checkpoint
// change only by CommitEnvironment, which advances Revision. Checkpoint is server-only provider state: callers must
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
}
