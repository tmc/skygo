// Package kube executes finite workloads as native Kubernetes Jobs.
//
// Jobs use restartPolicy Never and backoffLimit zero. Kubernetes may still
// create more than one Pod for a Job; execution is not exactly once. Logs reject
// ambiguous multiple-Pod output and expose only the first MiB of available
// container logs. Node log rotation or Pod deletion can remove that output;
// this backend does not provide a durable log archive.
//
// Cancellation uses foreground deletion and is confirmed when the Job disappears.
// Cancellation knowledge is in memory: after a server restart, the coordinator
// must replay its durable cancellation intent before observing a missing Job.
// Otherwise the missing Job is Unknown. Ownership uses a stable operator label
// and Job UID references; it assumes other namespace writers are trusted.
package kube
