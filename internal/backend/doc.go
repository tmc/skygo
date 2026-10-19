// Package backend defines tasks, durable job records, and the portable execution
// boundary shared by local processes and Kubernetes Jobs.
//
// Start is called at most once per accepted execution. Observe distinguishes
// missing execution from temporary observation errors. Reference carries the
// accepted task and immutable routing binding so a recovering backend can reject
// a conflicting execution. TaskBytes preserves the canonical JSON task encoding;
// accepted target bindings are separate from caller task equality. Target facts
// explicitly describe supported semantics without claiming available capacity.
package backend
