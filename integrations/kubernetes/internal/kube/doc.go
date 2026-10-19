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
package kube
