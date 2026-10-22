// Package kube executes finite workloads as native Kubernetes Jobs or Pods.
//
// FromTarget constructs a named target with explicit connection settings and
// resource policy. FromConfig preserves the legacy single-target loading rules.
// Both freeze routing, server trust and policy into an opaque target binding.
// Observe, Cancel and Logs reject a different accepted binding before API calls;
// the coordinator must also check the binding before calling Start.
//
// Jobs use restartPolicy Never and backoffLimit zero. Kubernetes may still
// create more than one Pod for a Job; execution is not exactly once. Logs reject
// ambiguous multiple-Pod output and expose only the first MiB of available
// container logs. Node log rotation or Pod deletion can remove that output;
// this backend does not provide a durable log archive.
//
// Cancellation uses UID-preconditioned foreground deletion. This backend reports
// Canceled after an observed Job's successful deletion request and disappearance.
// That knowledge is in memory: a missing Job after restart remains Unknown even
// if cancellation is replayed. Absence alone does not establish termination.
// Ownership uses stable operator/id labels, task and target-binding annotations,
// and controller Job UID references. Other namespace writers must be trusted.
//
// Tasks with an enabled retry policy instead create one directly owned Pod per
// attempt. A successful HTTP201 response pins its UID in a bounded private
// receipt; subsequent references require that receipt or the durable Instance.
// Ambiguous creation never authorizes adoption, replacement or native effects.
// Retry evidence requires the same UID and current single-container runtime
// identity and ordered timestamps on two reads, with a selected reported exit
// code. This trusts the API/kubelet report for that container, and does not claim
// signal provenance, whole-process-tree termination or native qualification.
// Cancellation and Pod disappearance do not establish retry eligibility.
//
// Pod log reads have no atomic UID precondition; metadata checks do not eliminate
// the race with a privileged writer replacing a Pod before its log request.
//
// Retention identifies remote output with no local output callbacks or charge.
// Execution cleanup supports only directly owned retry Pods. Its trusted caller
// must first validate a persisted NativeExecution intent containing accepted
// stopped evidence, full task and pinned UID; no RPC supplies that authority.
// A present Pod must retain the same complete stopped identity. UID-preconditioned
// deletion and confirmed absence permit receipt retirement. Missing after the
// durable intent is idempotent cleanup, never new stopped proof. Default Jobs and
// success without stopped proof require manual reconciliation. Native cleanup
// qualification is separate from these enforced constructor mechanics.
//
// A target with an optional data block also implements backend.DataBackend.
// Tasks with a workdir, setup, outputs or data mounts never use the Job or
// retry lanes: StartData creates scoped immutable token Secrets and one direct
// Pod with the fixed skygo-data-layout-v1 profile, each posted once, and returns
// their original Create-return UIDs even alongside an error. ObserveData reads
// only that UID and the complete expected profile; termination, no-launch and
// collector receipts require two identical reads in one call. RemoveData deletes
// each recorded resource under its original UID and acknowledges only absence
// it observed itself. Because client-go discards response body Close errors,
// data requests carry an operation frame that records each original body's
// single Close; an operation with an uncertain closure returns no observation
// or acknowledgment, sends nothing further, and keeps its ownership.
//
// Check refuses every data task unless the target installed a native
// qualification. FromTarget installs one only from a configured
// DataQualification: an operator-reviewed record, validated against the
// actual executable digest, its build receipt, the target binding and the
// record's image, path, mount and pool coverage, and then compared with
// read-only startup observations of the namespace, server version, pool nodes
// and recorded PVCs. Every covered task must name the record's pool in its own
// node selector; it is never added. The final binding includes the record
// identity. StartData repeats the observations in its own frame before any
// create. The policy revisions are operator assertions, and equal API strings
// do not prove unchanged runtime, admission or storage behavior. This package
// consumes a record; it neither produces one nor qualifies a cluster.
package kube
