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
[`control.proto`](../proto/skygo/v1/control.proto). The methods are
PlanTask, SubmitJob, GetJob, ListJobs, CancelJob, StreamJobLogs, ListTargets, and
GetServerInfo. Discovery reports typed capabilities for each configured target.
The current
capability is `FINITE_JOBS_V1`; it describes finite-job semantics, not available
capacity. A client requiring a capability must check the actual selected target
before performing the associated operation. Missing discovery is unsupported,
not implicit support. Existing servers cannot be made to reject unknown newer
protobuf fields retroactively.
Generated clients live in `gen/skygo/v1/skyv1connect`. Both executable
entrypoints use this protocol; a client does not need Kubernetes dependencies
to submit tasks to a server configured with the Kubernetes integration.

A submission requires a request ID. Scoped mode namespaces IDs by the
authenticated principal; legacy mode has one global namespace. The server
persists the ID, normalized task,
and resulting job atomically before acknowledging acceptance. Identical retries
return the same job; different tasks with the same ID return `already_exists`.
Acceptance does not guarantee execution. There is no automatic workload retry.

The observed states are queued, starting, running, succeeded, failed, canceled,
and unknown. Unknown means execution cannot be established; it does not prove
termination. CancelJob records desired cancellation; `cancel_requested` does not
mean the backend has stopped. Inspect observed state afterward. Completion can
win a race with cancellation.

ListJobs returns a bounded page and an opaque continuation token. Pass that token
unchanged under the same principal/read scope. Foreign and missing records are
indistinguishable in scoped mode. Records and request bindings persist until the
operator removes the
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

## State backup and rollback

The store checks its schema before migration. Missing schema markers identify
legacy state; supported legacy state migrates transactionally without changing
accepted task bytes, installation identity or request bindings. Newer schemas
and malformed markers refuse without modifying the existing database.

`state backup --data-dir DIR --output FILE` creates a consistent offline database
snapshot. Stop the writable server first; its database lock blocks backup.
`state restore --input FILE --data-dir NEW_DIR` validates and publishes state in
a new directory. Neither operation runs, cancels or rewrites workloads, and
neither overwrites an existing destination.

Snapshots contain only the database. Copy local retained logs separately if
needed; workload files, programs and images also need independent preservation.
Scoped authentication also requires its latest matching private auth file and
client credential files; snapshots do not contain them. See
[authentication recovery](authentication.md#recovery).
Restoring job metadata alone does not restore their output. Active job intent
is preserved: queued jobs may start when the restored server runs, local work
from another server lifetime becomes unknown, and Kubernetes work is checked
against its stored ownership and task identity. Unknown does not authorize retry.

Only one copy of an installation identity may reconcile external work. Keep the
original stopped before starting the restored copy. For rollback, stop the newer
server, restore a matching pre-upgrade snapshot into a new directory, and reconcile
external workloads before starting the prior binary. Binaries predating the
schema fence cannot safely be opened on newer state. A snapshot may omit work
accepted afterward; account for those external workloads before resubmitting.
