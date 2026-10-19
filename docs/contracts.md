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
| `accelerator` | Configured accelerator name; requires a positive count |
| `accelerator_count` | Requested units of the configured accelerator resource |
| `target` | Optional hard configured target name; never falls back to another target |
| `timeout_seconds` | Positive execution timeout; zero leaves it unspecified |
| `cpu_limit_millis` | CPU limit in millicores; zero leaves it unspecified |
| `memory_limit_bytes` | Memory limit in bytes; zero leaves it unspecified |
| `target_labels` | Exact string matches against configured target labels |
| `node_selector` | Exact Kubernetes node label matches, subject to target policy |
| `retry` | Optional finite retry policy; see below |

A nonzero CPU or memory limit requires a positive corresponding request and
must be at least that request. Requests alone do not impose limits. Accelerator
requests also set the matching limit. Local execution rejects resource requests,
limits, images, accelerators and node selectors; it does not silently ignore them.
Target labels constrain core target selection, including a hard `target` when
present. A missing label differs from a label whose value is empty. Node selectors
constrain native placement within the chosen target; they do not choose a target.

Configured target labels are limited to 32 entries, and each task placement map
to 16. Keys are nonempty and at most 256 bytes; values are at most 4096 bytes.
Both must be valid UTF-8 without NUL. An integration can impose further native
syntax and policy restrictions. Actual placement depends on cluster configuration
and available resources. No hardware discovery, quota check, VM provisioning, or
price model runs during planning. A shell is used only when explicitly in argv.

The public protocol is `skygo.v1.ControlService` in
[`control.proto`](../proto/skygo/v1/control.proto). The methods are
PlanTask, SubmitJob, SubmitRetryJob, GetJob, ListJobs, CancelJob, StreamJobLogs, ListTargets, and
GetServerInfo. Discovery reports typed capabilities for each configured target.
The capabilities are `FINITE_JOBS_V1`, `RESOURCE_LIMITS_V1`, `NODE_SELECTOR_V1`
and `TARGET_LABELS_V1`, plus `FINITE_JOB_RETRY_V1` for targets implementing
the enforced retry path. Finite jobs and target-label matching are core semantics;
resource limits and node selectors are advertised only by targets configured to
support them. `Target.labels` and `Target.accelerator_names` report declared
metadata. Kind/name does not imply support, and discovery does not report capacity
or expose execution bindings, kubeconfigs or credentials.

For a task using limits or placement maps without enabled retry, CLI `plan` and `run` first call
PlanTask with the original task, then check required capabilities on that exact
chosen target through GetServerInfo. A failed check produces no successful plan
and prevents submission. `run` then submits the original task and request ID,
without inserting the chosen target into the payload. Default tasks need no new
capability check and remain usable on old servers. Explicit inspection uses, for
example, `info --target batch --require resource-limits-v1,node-selector-v1`.
Missing discovery is unsupported, not implicit support. Existing servers cannot
be made to reject unknown newer protobuf fields retroactively.

Target configuration is immutable for a server lifetime. The preflight sequence
is not a reservation or an atomic handshake across replacement of an endpoint
with a different server. CLI preflight can refuse a retry when today's route no
longer qualifies; the SubmitJob API still replays an already accepted identical
request before considering today's route. Do not edit a task to force a retry
onto a new target.
Generated clients live in `gen/skygo/v1/skyv1connect`. Both executable
entrypoints use this protocol; a client does not need Kubernetes dependencies
to submit tasks to a server configured with the Kubernetes integration.

A submission requires a request ID. Scoped mode namespaces IDs by the
authenticated principal; legacy mode has one global namespace. The server
persists the ID, original task, selected target binding,
and resulting job atomically before acknowledging acceptance. Identical retries
return the same job; different tasks with the same ID return `already_exists`.
Acceptance does not guarantee execution. Default tasks have one implicit attempt.

## Finite retries

An enabled `retry` policy requires `max_attempts` from 2 through 10 and 1 through
32 input `exit_codes`, each from 1 through 255. Codes are sorted and deduplicated
before acceptance. `delay_seconds` is 1 through 3600; omitted or zero becomes 1.
Omitted retry, `{}`, or `max_attempts: 1` without delay/codes means the unchanged
single-attempt path. Delay/codes with a maximum of zero or one are invalid.
Maximum attempts includes the first execution.

The normalized policy is part of accepted task identity. Every attempt reuses
that task and the original target binding; only its execution ID changes. Retry
CLI submission calls the additive SubmitRetryJob RPC directly with the original
wire task, without placement preflight or fallback to SubmitJob. An old server
returns unimplemented. An accepted request replays before current route,
capability or capacity checks. Changing a policy under the same request ID is a
conflict. Explicit `plan` still describes current placement, not a replay.

Only a failed attempt with positive backend stopped evidence, an explicitly
selected exit code, remaining budget and no cancellation schedules another
attempt. Its deadline is committed once. `retrying` is nonterminal and releases
its starting/running slot. At the deadline, the same target binding and actual
retry support must still hold, with a free slot. Otherwise the job stays held;
there is no failover. Canceling queued/backoff work settles without waiting for
the deadline, route or slot and allocates no new attempt. Canceling already
starting/running work records intent and awaits observation. Success may win a
late cancellation. Unknown, missing workloads, Start timeouts and lost replies
never authorize replacement.

Local retry requires Linux pidfd_open/pidfd_send_signal support (Linux 5.3 or
newer, subject to runtime availability). Darwin rejects enabled retries. The
local proof retains the unreaped leader while stopping its process group, then
requires reaping, group absence and combined-output EOF. Cancellation, timeout,
output failure and uncertain cleanup suppress positive evidence. Programs that
escape the group are outside this cooperative boundary; this is not whole-tree
or exactly-once execution.

Kubernetes retry creates one directly owned restartNever Pod per attempt;
default tasks retain native Jobs. HTTP201 and its returned UID establish the
initial identity; later operations require the same pinned UID. Lost Create
replies are not adopted or replaced. Retry eligibility requires the sole owned
container's current matching runtime identity and positive ordered termination
timestamps, confirmed by another GET. Reported exit codes have no signal
provenance: selected values such as 137 are not described as ordinary exits.
Deletion acknowledgement, NotFound, node loss and phase alone do not prove
stopped. Missing cancellation evidence can remain unknown.

`--json jobs get ID` includes bounded history and `next_attempt_at` for enabled
retry jobs. History timestamps describe durable decisions, not exact OS times.
Default jobs keep their implicit original ID without new stored history.
`run --wait` includes backoff and returns the final logical outcome.


The observed states are queued, starting, running, retrying, succeeded, failed,
canceled, and unknown. Unknown means execution cannot be established; it does not prove
termination. CancelJob records desired cancellation; `cancel_requested` does not
mean the backend has stopped. Inspect observed state afterward. Completion can
win a race with cancellation.

ListJobs returns a bounded page and an opaque continuation token. Pass that token
unchanged under the same principal/read scope. Foreign and missing records are
indistinguishable in scoped mode. Records and request bindings persist until the
operator removes the
store; removing it destroys retry protection and ownership identity.

Log offsets count bytes in a bounded combined output prefix. The JSON stream
reports `next_offset`, `complete`, and `truncated` in protobuf JSON. For retry
jobs or explicit new selection it also includes `attempt_id`. `jobs logs ID
--attempt N` selects a one-based accepted attempt. Without a selector, current
is resolved once when the stream opens; reads never switch to a later attempt.
Every read checks current authorization and selected-attempt membership. Complete
refers to that selected output, not the logical job's completion. Each execution
retains or reads at most 1 MiB, so an enabled job permits at most 10 MiB of log
prefixes. There is no aggregate physical log-disk reservation. Local logs survive server
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
capacity. Enabled retry jobs additionally reserve max_attempts × 8192 bytes
against a 16 MiB aggregate history-accounting budget at acceptance. Each encoded
attempt is bounded by that reservation; it is not reserved bbolt pages or disk
space. Completion does not release the reservation. Histories have at most ten
entries; later attempts do not consume new logical/request/sequence bindings.
The controller admits at most 32 starting/running executions at a time; unknown
remote executions may remain outside that accounting.

Each accepted job carries an immutable execution binding separate from Task.
Reconciliation and backend log reads require the configured name and binding to
match. Missing, changed or unbound legacy routes retain their last observed state
with an explicit held reason and cause no backend calls. Restore the exact route
to resume; reusing a name for another cluster does not rebind old jobs. Queued
cancellation can settle without a route because execution never started.
Kubernetes operations also verify task identity and ownership; a conflicting
native Job remains untouched and the accepted job becomes unknown.

Only the server's actual built-in local backend authorizes automatic binding of
historical unbound `local` rows. This runs after store/auth/target validation and
before listening or reconciliation, and is skipped with `--disable-local`.
A factory target's name or kind is not proof of that construction. Local binding
identifies logical installation semantics, not a physical machine or surviving
PID. Legacy Kubernetes/unknown routes remain held; there is no automatic remote
binding migration.
## State backup and rollback

The store checks its schema before migration. Format 4 adds attempt history,
identity indexes and reservation accounting independently of legacy/scoped
authentication mode. Opening supported historical
state upgrades the format transactionally without changing accepted task bytes,
installation identity, request bindings, ownership or accounting. It leaves old
rows byte-identical and does not activate scoped auth. Opening can upgrade format even if later server configuration
fails. Take a stopped pre-upgrade snapshot before using the new binary. Newer
schemas and malformed state refuse before writes. Historical remote rows without
a binding remain readable and replayable but held from execution and backend logs.

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
Stored backoff deadlines and attempt history survive backup/restore; restore
does not restart a deadline or allocate an attempt.

Only one copy of an installation identity may reconcile external work. Keep the
original stopped before starting the restored copy. For rollback, stop the newer
server, restore a matching pre-upgrade snapshot into a new directory, and reconcile
external workloads before starting the prior binary. Binaries predating the
schema fence cannot safely be opened on newer state. A snapshot may omit work
accepted afterward; account for those external workloads before resubmitting.
