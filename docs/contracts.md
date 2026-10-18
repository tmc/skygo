# Task and API contracts

The CLI accepts exactly one YAML document. Unknown fields, duplicate keys,
empty argv, and documents larger than 1 MiB fail before submission. The server
applies additional task and RPC size limits. YAML is not SkyPilot-compatible.

| Field | Meaning |
| --- | --- |
| `argv` | Program and arguments; no implicit shell |
| `env` | String environment overrides |
| `directory` | Absolute server-host directory locally; absolute image directory on Kubernetes |
| `image` | Required for Kubernetes; rejected locally |
| `cpu_millis` | Kubernetes CPU request in millicores |
| `memory_bytes` | Kubernetes memory request in bytes |
| `accelerator` | `nvidia` on Kubernetes; requires a positive count |
| `accelerator_count` | Requested `nvidia.com/gpu` units |
| `target` | Optional configured target name: `local` or `kubernetes` |
| `timeout_seconds` | Positive execution timeout; zero leaves it unspecified |

CPU and memory requests do not impose workload limits. GPU requests also set the
matching limit. Actual placement depends on cluster configuration and available
resources. No hardware discovery, quota check, VM provisioning, or price model
runs during planning. A shell is used only when explicitly present in argv.

The public protocol is `skygo.v1.ControlService` in
[`control.proto`](../proto/skygo/v1/control.proto). The seven methods are
PlanTask, SubmitJob, GetJob, ListJobs, CancelJob, StreamJobLogs, and ListTargets.
Generated clients live in `gen/skygo/v1/skyv1connect`. Both executable
entrypoints use this protocol; a client does not need Kubernetes dependencies
to submit tasks to a server configured with the Kubernetes integration.

A submission requires a request ID. The server persists the ID, normalized task,
and resulting job atomically before acknowledging acceptance. Identical retries
return the same job; different tasks with the same ID return `already_exists`.
Acceptance does not guarantee execution. There is no automatic workload retry.

The observed states are queued, starting, running, succeeded, failed, canceled,
and unknown. Unknown means execution cannot be established; it does not prove
termination. CancelJob records desired cancellation; `cancel_requested` does not
mean the backend has stopped. Inspect observed state afterward. Completion can
win a race with cancellation.

ListJobs returns a bounded page and an opaque continuation token. Pass that token
unchanged. Records and request bindings persist until the operator removes the
store; removing it destroys retry protection and ownership identity.

Log offsets count bytes in a bounded combined output prefix. The JSON stream
reports `next_offset`, `complete`, and `truncated`. Local logs survive server
restart. Kubernetes logs are read from Kubernetes, not copied into durable
storage; rotation can invalidate offsets without making a stable archive.
Kubernetes log polling re-reads this bounded prefix; it is not an incremental
log transport and can generate repeated API traffic for quiet workloads.
A disconnected client can retry from an offset only while that prefix remains
available. Streams also end on server shutdown or its HTTP write timeout.

There is one durable writer. The database lock rejects a second server sharing
its directory. A PVC is persistence, not replication. Restoring or cloning state
must not produce two active installations controlling the same Kubernetes Jobs.

Accepted task payloads are limited to 16 MiB in aggregate per data directory,
in addition to the 10,000-job limit. Existing request IDs remain retryable at
capacity. The controller starts at most 32 executions at a time.

A missing configured target preserves the last observed state with an explicit
reason. Restore that target to resume reconciliation. Kubernetes operations
verify the stored task identity as well as ownership; a conflicting native Job
is left untouched and the accepted job becomes unknown.
