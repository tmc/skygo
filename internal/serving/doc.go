// Package serving defines the portable contract for long-running services:
// a validated Spec, immutable numbered revisions, rollout generations and the
// Runtime boundary that a target integration implements.
//
// A Spec describes one digest-pinned container with one TCP port, an HTTP GET
// readiness probe, positive CPU and memory requests, read-only data aliases and
// a fixed replica count. Normalize applies defaults and orders set-valued
// fields; Digest binds the normalized fields' canonical bytes, never map
// iteration order, time or process state.
//
// History holds a service's revisions and rollouts. Update adds a revision,
// Rollback points a new generation at a retained one, and Settle advances the
// current rollout only from a runtime Observation. Desired state, observed
// state and readiness stay distinct: a stored spec or a successful Apply is
// never availability. Prune never removes the current target, the last
// complete or the newest revision.
//
// The package contains no provider implementation. It grants no authority,
// reserves no quota and performs no I/O.
package serving
