// Package backend defines tasks, durable job records, and the portable execution
// boundary shared by local processes and Kubernetes Jobs.
//
// Start is called at most once per accepted execution. Observe distinguishes
// missing execution from temporary observation errors. Reference carries the
// accepted task so a recovering backend can reject a conflicting execution.
package backend
