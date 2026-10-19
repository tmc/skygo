package backend

import "context"

// OutputStorage identifies who owns retained output. It does not describe
// workload storage or prove that an individual output can be removed.
type OutputStorage uint8

const (
	// UnspecifiedOutput is the zero value. Enabled retention refuses new starts
	// without an explicit output storage fact from the target constructor.
	UnspecifiedOutput OutputStorage = iota
	// LocalOutput uses this server's measured output volume and reservation.
	LocalOutput
	// RemoteOutput has no local output reservation or output cleanup callbacks.
	RemoteOutput
)

// OutputReference identifies one immutable output domain without Task or native
// execution cleanup authority. Owner is the installation ID; JobID is logical,
// and AttemptID is the execution ID (equal to JobID for an implicit attempt).
// Generation is one. Binding must match the accepted target. A closure query
// leaves ClosureID empty and PayloadReleased false; only the trusted adapter's
// receipt supplies them. ClosureID has 1..128 printable ASCII bytes when present.
// PayloadReleased records completed command payload release, not workload stop.
type OutputReference struct {
	Owner, JobID, AttemptID, Binding string
	Generation                       uint64
	ClosureID                        string
	PayloadReleased                  bool
}

// OutputReceipt reports independently verified writer closure. ClosureID is a
// bounded immutable adapter receipt; it is not a credential or stopped proof.
// PayloadReleased additionally reports completed command payload sanitization.
// A closed writer can have PayloadReleased false. The zero value proves nothing.
type OutputReceipt struct {
	ClosureID       string
	PayloadReleased bool
}

// Retention supplies separate output and execution cleanup operations. A nil
// pointer or nil callback makes that operation unavailable, never successful.
// CheckOutputClosed and RemoveOutput must both be present or both absent.
// RemoveExecution is independently optional. Callbacks are safe for concurrent use.
//
// CheckOutputClosed is read-only: it validates identity and returns a receipt
// without closing handles, sanitizing payload, signaling or making native calls.
// RemoveOutput rechecks that receipt, quiesces output handles, removes only the
// exact output domain and syncs its directory. It cannot retire execution state.
// RemoveExecution requires accepted stopped evidence and full Reference identity;
// it never substitutes for output cleanup. Neither operation falls back to another.
// Callers invoke callbacks outside store transactions and control-plane locks.
type Retention struct {
	CheckOutputClosed func(context.Context, OutputReference) (OutputReceipt, error)
	RemoveOutput      func(context.Context, OutputReference) error
	RemoveExecution   func(context.Context, Reference) error
}
