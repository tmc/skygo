// Package leaderlease holds one Kubernetes Lease for a skygo server process.
//
// A held Lease is a single-active guard: while one process renews it, another
// process using this package with the same Lease waits. It is not a storage
// or native-effect fence and does not make a server highly available. A
// paused or partitioned holder may still act after its Lease expired; the
// client-go leader election code this package follows makes the same
// disclaimer. Fencing the state itself is a separate precondition.
//
// Acquire blocks until the process holds the Lease. A [Held] Lease is renewed
// by one goroutine that it owns until the Lease is lost, stopped or released.
// Loss is sticky: a lost Lease is never reacquired, and a new process is
// required. The caller's context bounds only the acquisition; canceling it
// later neither stops the renewal nor releases the Lease.
//
// [Held.Valid] reports whether the last successful acquisition or renewal,
// as observed by this process on its monotonic clock when the request was
// sent, is within the renew deadline. It never uses the Lease's recorded
// times. [Held.Release] gives the Lease up and is meant to be called only
// after the state it guards was closed successfully; it refuses a Lease that
// is lost or no longer valid. Otherwise [Held.Stop] stops renewing and leaves
// the Lease to expire.
//
// The Lease record is the one client-go's tools/leaderelection writes, through
// its resourcelock conversions, so the two can contend for the same Lease. A
// contender detects renewal by the Lease's resourceVersion, not by the
// record's whole-second times, and every update is conditional on the
// resourceVersion this process last read or wrote.
package leaderlease
