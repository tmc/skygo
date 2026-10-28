# Dynamic resource allocation

DRA support is planned, not implemented. Its first prerequisite, durable
execution receipts for ordinary Jobs and retry Pods, is implemented behind an
explicit constructor, which the server selects only with `--execution-state`;
see [Durable execution receipts](#durable-execution-receipts).
No target accepts DRA tasks. The Kubernetes target currently maps
accelerator names to extended resources, such as `nvidia.com/gpu`. This design
adds a second, explicit allocation mode inside the Kubernetes integration module.
It does not add Kubernetes dependencies to the core or change local and SSH
execution.

## Device requests and target policy

Tasks continue to request a portable accelerator name and positive integer count.
The operator maps each name to exactly one allocation mode:

- An extended-resource name and the existing node-selector policy.
- A DRA `DeviceClass`, bounded CEL selectors, and an exact device count supplied
  by the task.

The two modes are mutually exclusive. A missing DRA API, class, driver, or
permission never selects the extended-resource mode instead. A target can expose
separate accelerator names when the operator deliberately offers both modes.

DRA policy, canonical selectors, allocation mode, and count rules belong in the
target binding. Changing them changes the binding. Targets without DRA keep
their existing binding bytes and behavior. User tasks cannot supply Kubernetes
objects, arbitrary CEL, driver configuration, or administrative device access.

`Check` remains a local policy check. Construction of a DRA-enabled target adds
bounded API discovery and class validation under the caller's context; the
legacy constructor path remains unchanged. Planning reports compatibility, not
a reservation or a promise that devices are currently free. Kubernetes and its
DRA driver perform allocation and scheduling.

## Claims and ownership

The first implementation uses namespaced `ResourceClaimTemplate` objects and
independent generated claims for each Pod. The Pod references its template and
the task container references the corresponding claim. This follows the
[Kubernetes workload model](https://kubernetes.io/docs/tasks/configure-pod-container/assign-resources/allocate-devices-dra/).

Templates are immutable execution objects. Their names derive from the target
binding, accepted task, and attempt identity; their metadata records those
values. A template is owned by its exact Job UID, or by the exact directly
managed Pod UID for retry execution. A missing template must not permit the
workload to run. Create the parent referencing the expected template, obtain
its UID, then create the template with that owner reference. Validate this
ordering against a real API server before enabling it.

Kubernetes generates a separate claim for each Pod and ties that claim's
lifetime to the Pod. See the upstream
[claim lifecycle](https://kubernetes.io/docs/concepts/resource-management/dynamic-resource-allocation/dra-api/).
A completed Job may retain its Pod for logs, so job completion alone is not a
Skygo guarantee that a device has been released. DRA cleanup must provide a
separate, observed release result.

Native receipts must survive both a server restart and deletion of the native
parent. Pin the parent UID through the existing opaque execution identity.
Keep bounded template and generated-claim names, UIDs, and specification digests
until cleanup completes; parent annotations alone cannot preserve this receipt
after parent deletion. The durable receipt format below reserves these typed
rows; DRA must use it. Recovery validates the parent UID,
namespace, task digest, binding, owner references, and expected specifications.
A name or label match alone does not authorize adoption or deletion. The
ordinary Job path needs this stronger UID pin; the existing retry path already
uses a pinned Pod instance.

A lost create response remains uncertain until the permitted identity checks
resolve it. It does not authorize a second allocation. Retry attempts receive
new templates and generated claims; they never reuse the preceding Pod's
claim. Cancellation deletes only the pinned workload. Garbage collection
handles owned dependents; any direct deletion additionally requires the
recorded UID. Allocation uncertainty, finalizers, and cleanup timeouts remain
visible and prevent a successful resource-release result. Skygo never removes
Kubernetes or driver finalizers to force progress.

Caller-supplied or shared claims, fractional capacity, prioritized device
alternatives, and administrative access are outside this first phase. They
need separate authorization and lifetime rules.

## Durable execution receipts

`FromTargetWithState(ctx, config, owner, stateDir)` is `FromTarget` with
durable custody of each attempt's native parent: a Job, or a directly managed
Pod for retry. `FromTarget` and `FromConfig` are unchanged: they keep volatile
receipts, adopt an existing ordinary Job by owner, task and binding, and create
objects without the receipt annotations.

`skygo-kubernetes server --targets ./targets.yaml --execution-state DIR`
builds every named target with `FromTargetWithState` and `DIR`; without the
flag the server builds them with `FromTarget` as before. An explicit flag,
even an empty one, never falls back. The flag requires `--targets` and
conflicts with `--environment-profiles`; both are refused before any
configuration file is read. `DIR` is validated, and data targets refused,
when the server builds its targets, before any request. The operator creates
`DIR`; the server does not create, migrate or adopt state. Attempts started
without receipts, including those of a server run without the flag, are
refused, not adopted: drain or cancel them before restarting with it. The
mode leaves DRA, environments, remote data and ordinary Job cleanup
unavailable. Durability assumes `DIR` stays on one local disk that honors
`fsync` ordering and `flock`; receipts are separate files and are not written
atomically together, and tombstones are kept as described below. The mode's
authored tests use a loopback API with MODELED status and have NOT_RUN; no
native cluster, process restart or SIGKILL has exercised it, and it has no
runtime or native cluster qualification.

`stateDir` is server-owned private state: an existing absolute directory
without symlinks, owned by the server user with mode `0700`. It is never the
working directory, a shared cache, a temporary directory, a task-controlled
path, or an environment volume. Receipts live in a subdirectory derived from
the owner, namespace and target binding, as one canonical JSON file of at most
16 KiB per attempt and at most 2048 files. Receipts require Linux or Darwin.
Targets with a data block are refused. Every construction creates the
subdirectory if needed and then syncs the state directory itself, through a
descriptor checked to be the directory found at construction, even when an
earlier or concurrent construction created the subdirectory; a failed sync
refuses construction, so no create can be recorded under an entry that was
never confirmed durable.

For each attempt, `Start`:

1. records the attempt, task digest, expected specification digest and a
   random creation nonce in phase `intent`, and syncs the file and directory;
2. records phase `sent`, consuming the only create grant;
3. sends one create without transport retries, carrying the nonce and
   specification digest as annotations;
4. validates the response as strictly as a later read, and records the UID in
   phase `pinned`.

A definite `AlreadyExists`, `Invalid` or `Forbidden` response records
`rejected`: the API server proved nothing was persisted. A complete success
response that is not `201 Created`, or does not decode strictly, or does not
match the receipt, records `refused` before `Start` returns; a `200` whose
body is exactly the receipted object is refused as well. The witness records
the actual HTTP status, the SHA-256 and size of the exact body, whether it
decoded, and, only from a decoded body and only where well formed, its
reported UID, nonce and spec annotations and canonical spec digest. Absent and
malformed values are both omitted, never synthesized, so the witness cannot
tell them apart; no body bytes are stored. The object the response reports
may exist; `refused` is terminal and no later read, scheduling status or
restart adopts, reads, cancels or cleans it up, even if a concurrent recovery
read pinned the object first. If writing `refused` fails or is uncertain,
`Start` returns that storage error; the Backend then authorizes nothing for
the attempt, but other processes see only the last durable record and are not
notified. A lost connection or a body that never completes is not a known
response: the receipt stays `sent`, as after any other failure, which never
permits another request in this or any later process. `Start` refuses an
attempt that already has a receipt; this mode never adopts.

Receipted operations use their own HTTP client, a copy of the one client-go
builds for the frozen configuration; the shared, default and `FromTarget`
clients are not changed. It never follows a redirect. A complete `3xx`
response to the create is a known response that does not report this
request's creation: it records `refused` with a witness of its status and
exact body, even if the body is a Status client-go would decode as
`AlreadyExists`. A `3xx` whose body does not complete leaves the receipt
`sent`. The create body is sent at most once: the request reaching the
transport has no `GetBody`, so neither HTTP/2 (after `REFUSED_STREAM`,
`GOAWAY` or a peer `PROTOCOL_ERROR` once the body was written) nor a redirect
can resend it, and the receipt stays `sent`. Its JSON encoding, `apiVersion`,
`kind`, content type and HTTP/2 are those of client-go. An HTTP/1 request that
failed before anything was written is not resent either; that costs the
attempt, not custody. Reads and UID-preconditioned deletions keep the
transport's retries: a repeated read changes nothing, and a repeated delete
can only delete the recorded UID or fail, after the intent it repeats was
recorded.

A refusal does not revoke operations already in progress. An operation that
loaded the receipt before the refusal reloads it after each read of the parent
(match, NotFound or uncertain alike) and before returning anything; before a
log request that follows other requests; before releasing log bytes; and
before reporting a Job status after listing its Pods. Cancellation and cleanup
record their intent before each deletion, which a refused record refuses. A
refused, retired or otherwise changed record then returns its error, so
nothing is pinned, reported or released after the refusal is visible. A
recorded cancellation step advances the generation without changing the
identity, phase or UID; the read is then reported under the current record. A
request already sent completes under the receipt it was admitted with and is
not recalled: a delete or log request sent just before the refusal still
reaches the API server, and the bytes or status of an operation that returned
before the refusal remain returned. These checks are local reads of the
record, without holding the journal lock across a request.

`Observe`, `Cancel`, `Logs` and retry cleanup use only the recorded UID and
report it as `Status.Instance`. A supplied instance must equal it. Every read
of a pinned Job or retry Pod, including each recheck before releasing log
bytes, reporting a termination or deleting, is matched by itself against the
receipt by the rules below. Before the
UID is pinned, a lost create response is recovered only from the object with
the recorded nonce, expected specification, owner, binding and namespace. If
no such object is visible, `Observe` reports `Starting` with the reason
`kubernetes create outcome uncertain` (or `pending` before the grant) and
nothing is resent. Labels, name, task digest or the existence of a UID never
authorize adoption. A different UID is an identity mismatch.

The expected specification digest covers the parent's kind, name, namespace,
generate name, labels, annotations (except the nonce and digest themselves),
owner references and finalizers, and the complete typed `JobSpec` or
`PodSpec`, including every container, init and ephemeral container, volume,
security context, host namespace flag, service account, scheduling input,
resource name and claim. Responses and reads are decoded strictly: a field
the vendored API types do not represent is refused, not dropped. The
expected object and every observed one are canonicalized by the same rules,
and only these:

- unset fields receive the Kubernetes 1.32 API defaults: for Jobs
  `completionMode: NonIndexed`, `suspend: false`,
  `podReplacementPolicy: TerminatingOrFailed` (the default, and the behavior
  without the feature) and `manualSelector: false` (equivalent to unset); for
  Pod specs and Job templates `dnsPolicy`, `restartPolicy`, an empty
  `securityContext`, `terminationGracePeriodSeconds: 30`, `schedulerName`,
  and per container the termination message path and policy and the image
  pull policy derived from the image tag; for direct Pods only, requests
  equal to limits for limited resources without a request and
  `enableServiceLinks: true`;
- for direct Pods only, the output of the default ServiceAccount, Priority
  and DefaultTolerationSeconds admission plugins where the field is unset:
  service account `default`, priority 0 with `PreemptLowerPriority`, and the
  not-ready and unreachable `NoExecute` tolerations of 300 seconds;
- a Job's generated selector and its four controller labels are removed only
  in the exact shape the API server generates for that Job's own UID and
  name;
- a direct Pod's node name is removed only in a read, never a create
  response, and only with a true `PodScheduled` condition;
- a Job's `foregroundDeletion` finalizer is removed only while the Job is
  being deleted, as `Cancel` requests;
- quantities are compared by value, never by format, and no resource name is
  removed.

Everything else is refused, including image pull secrets copied from the
service account, LimitRange defaults, `AlwaysPullImages`, a global default
PriorityClass, other toleration durations, Pod security or webhook mutation,
Job-level suspension, failure, success and TTL policies, and manual or
foreign selectors. A namespace that applies such changes cannot use
receipts. A create response carrying a node name is recorded `refused`.
After a lost response, a node name with `PodScheduled` is accepted as
trusted placement output of the API server: it does not prove that the
scheduler made the original choice, that the node or its devices qualify,
or that a cluster administrator with the same access did not set both.
Defaults added by Kubernetes releases after 1.32, such as
container resize policies, are refused until the module stage is upgraded.

Cancellation records `intent` before the UID-preconditioned deletion and
`delivered` after its successful response. A NotFound response proves neither.
Retry cleanup records intent before deletion and, only after the existing
stopped-evidence contract confirms the Pod is gone, replaces the receipt with
a `retired` tombstone. The tombstone keeps the identity, nonce and UID and its
counted slot; it authorizes no create, read, cancellation or further cleanup,
in this or any later process. Tombstones are never reclaimed: a store whose
slots are all used refuses new attempts until an operator acts. Ordinary Job
cleanup remains unavailable.

Native operations are serialized against cleanup by an operation lock file in
the scope directory, separate from the short journal lock that orders receipt
writes; the journal lock is never held across a request. `Start`, `Observe`,
`Cancel` and `Logs` hold a shared permit until their last response, log body
and receipt write are finished. Nested calls share it. A shared request waits
for cleanup only until its context is done. Cleanup takes the permit
exclusively without waiting and fails as pending while any operation of the
scope runs in any Backend or process sharing the state directory.

`Logs` sends one log request and closes the original response body, once,
before its final receipt check; no byte is returned before that `Close`
returned without error. If the `Close` fails or its outcome is unknown, `Logs`
returns no bytes and an error, and the process keeps the body and the shared
permit until it exits. Cleanup of that scope then fails as pending in every
Backend and process, and further `Logs` of the attempt fail in that process;
other operations proceed. At most 16 log bodies are reserved or kept per
process, after which receipted `Logs` fail; a kept body is never dropped to
make room. Nothing is closed twice or canceled to settle a body, and a
`Close` that never returns keeps its `Logs` call waiting. Stopping the process
is the only release.

Each lock acquisition reopens the scope directory through the state directory
without following symlinks and requires both to be the same private
directories found at construction; record files are then opened, renamed,
removed and synced relative to that open directory. Files are opened
nonblocking and checked before use, so a FIFO or device fails instead of
blocking. A same-user administrator replacing an ancestor of the state
directory between the check and a file operation is outside this check.

A missing, corrupt, foreign, rolled-back or oversized receipt, an unexpected
directory entry, wrong ownership or mode, a symlink, a replaced directory, or
a full store refuses the operation before any API request. A failed or
uncertain receipt write is reported as an error and grants nothing. Recovery
from such a refusal is an operator action; Skygo does not rebuild receipts
from cluster objects.

Every backend method refuses a receipt carrying dependent rows. Those typed
rows record `ResourceClaimTemplate` objects owned by the parent UID, each
Job-owned Pod, and each generated `ResourceClaim` owned by one exact Pod: a
recorded Pod row of a Job, never the Job, or the parent UID of a directly
managed Pod, which has no Pod rows. They exist for the DRA lifecycle and are
exercised only by storage tests.

Limits of this stage: rollback of a receipt file to an older valid copy is
detected within one process, and across restarts only through a pinned
`Reference.Instance`. Recovery cannot defend against a privileged namespace
writer that copies a nonce.

## Module and cluster requirements

The integration currently pins Kubernetes Go modules at `v0.32.3` and declares
Go `1.24.1`; it cannot use the stable typed `resource.k8s.io/v1` client as-is.
Upgrade `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go` together
inside this module. Do not hand-copy Kubernetes API types into Skygo.

Select and test an explicit supported client/cluster combination before
implementation. The upstream `v0.34.0` API module declares
[Go 1.24](https://github.com/kubernetes/api/blob/v0.34.0/go.mod), while
`v0.35.0` declares
[Go 1.25](https://github.com/kubernetes/api/blob/v0.35.0/go.mod). A newer optional
module toolchain requirement does not require raising the core's requirement.
API discovery must confirm `resource.k8s.io/v1`; an installed driver and
operator-provisioned `DeviceClass` are separate requirements.

Use namespace-scoped permissions for the managed templates and claim
observations, plus read access to the configured cluster-scoped classes.
Skygo does not install drivers, modify `DeviceClass` or `ResourceSlice`
objects, update allocation status, or request administrative device access.
Unneeded list, mutation, and cluster-wide privileges stay absent.

## Implementation order and acceptance

1. Extend target configuration, validation, cloning, and canonical binding for
   the mutually exclusive modes. Preserve legacy bindings byte for byte.
2. Extend the shared Pod renderer and add template construction. Wire both
   ordinary Jobs and directly managed retry Pods; retain CPU and memory
   requests independently. Durable UID receipts and recovery exist for the
   parent; DRA tasks additionally require only receipted targets and must
   record each template, Pod and generated claim before acting on it.
   Core stops observing terminal jobs, so DRA must either keep a joined
   cleanup owner after completion or withhold terminal status until claim
   accounting settles, retaining exit evidence and logs. Data tasks, workers
   and persistent environments refuse DRA until they have their own receipts.
3. Implement observed allocation diagnostics, cancellation, retention, and
   cleanup. Report unsatisfied claims as scheduling state; do not infer
   running or released devices from successful object creation.
4. Apply the same rules to each worker rank when the Kubernetes worker
   backend is integrated. Every rank gets an independent claim. Gate worker
   release on the existing group protocol, and join every rank's claim
   cleanup before reporting group removal. Do not advertise DRA for workers
   based on ordinary-job tests.
5. Qualify the complete lifecycle on an authorized cluster with a real DRA
   driver. Until then, unit and fake-client tests establish rendering and
   controller behavior only.

Required tests cover strict policy rejection, unchanged legacy bindings, exact
Pod and template fields, count and selector changes, ownership conflicts, lost
responses, server restart, retry separation, cancellation before allocation,
delayed driver cleanup, and replacement UIDs. Real-cluster acceptance must
show the requested device inside the container and prove eventual claim
cleanup after cancellation and retry. A missing driver or unavailable test
cluster is an explicit validation hold, not a passing allocation test.
