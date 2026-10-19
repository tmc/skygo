// Package control validates tasks, selects configured targets, and reconciles
// durable jobs independently of the transport and execution backend.
//
// Submit persists acceptance before returning. Run owns one reconciliation loop for the lifetime of its Control;
// cancellation records intent separately from the observed outcome. An unknown
// outcome never authorizes another execution of the same job. Targets are frozen
// at construction. Every backend call requires the job's original routing binding;
// missing or changed bindings hold execution without implying termination.
// Request replay returns its original placement despite configuration changes.
// Each target has one bounded I/O worker; unresponsive targets cannot occupy
// another target's worker. Canceling Run does not force a context-ignoring call
// to return, and a second Run on the same Control is refused.
//
// Each public request takes an auth.Actor and revalidates its current role and
// credential with the configured auth.Engine. Scoped stores enforce ownership
// atomically with cancellation; cursors bind to the principal's current listing
// scope. Run is the trusted reconciler and intentionally observes all owners.
// Audit failures have a separate diagnostic and never grant or revoke access.
// RecordAccess queues bounded best-effort diagnostics without database I/O on
// the request path. Run attempts persistence, but pending events can be lost at
// overflow, shutdown or crash; no shutdown flush or durable audit receipt is
// promised. Stored timestamps describe persistence attempts. Disk stalls can
// still delay Run's durable writes independently of request completion.
package control
