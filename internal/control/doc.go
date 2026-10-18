// Package control validates tasks, selects configured targets, and reconciles
// durable jobs independently of the transport and execution backend.
//
// Submit persists acceptance before returning. Run owns one reconciliation loop;
// cancellation records intent separately from the observed outcome. An unknown
// outcome never authorizes another execution of the same job.
package control
