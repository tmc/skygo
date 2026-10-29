# Dynamic resource allocation

An experimental first DRA mode is implemented. It is disabled unless the
operator configures it, and it is not native qualification: its tests use a
loopback API whose controller, scheduler and kubelet behavior is MODELED and
have not been run here. Automatic retry, native Jobs, templates, failed or
canceled cleanup and device-level recovery remain later work.

## Configuration

Tasks keep requesting a portable accelerator name and a positive count. The
operator maps each name to exactly one mode: an extended resource
(`resource_name`) or DRA:

```yaml
resources:
  max_accelerators: 8
  accelerators:
    h100:
      dra:
        device_class: gpu.example.com      # DNS subdomain
        selectors:                         # 0..8 CEL expressions, 1..1024 bytes each
        - 'device.attributes["gpu.example.com"].model == "H100"'
```

`dra` conflicts with `resource_name`, `required_node_selector` and a target
`data` block. A missing API, class or driver never falls back to an extended
resource. The class, selectors (in order) and the fixed claim shape below are
part of the target binding (`skygo-kubernetes-dra-target-v1`); targets without
DRA keep their existing binding bytes. Tasks cannot supply classes, CEL,
driver configuration, admin access or Kubernetes objects.

The server enables the mode only with all of:

    skygo-kubernetes server --targets targets.yaml \
      --execution-state /srv/skygo/exec --dra-state /srv/skygo/dra \
      [--dra-log-retention 24h]

`--dra-state` requires `--execution-state` (and so `--targets`); a targets file
with a DRA accelerator is refused without it. With it, the server installs one
integration (`NewDRAIntegration`) that constructs every configured target once,
as `FromTargetWithState` does, and owns DRA custody and maintenance. Only those
targets advertise DRA accelerators. DRA tasks with retry, data, workdir, setup
or outputs are refused by `Check`, before any effect. Workers and persistent
environments are unavailable on this route.

## Operating contract

Skygo assumes, and does not attest:

- The namespace is dedicated to this installation and its admission is
  trusted. No other writer creates, replaces, mutates or consumes its claims or
  Pods, binds Pods directly to nodes, or force-deletes them. `ExactCount` asks
  for exclusive devices; it does not limit a claim to one Pod, and the
  namespace UID is not an access control list.
- One deployed server writes the two state roots and holds the namespace
  credentials. The operator stops older executables and withholds the new
  roots and namespace credentials from them. An older executable refuses the
  new targets file (unknown field `dra`) and the `--dra-state` flag before it
  writes anything, but it never reads the DRA state marker: nothing here
  protects against an arbitrary old or hostile program with the same access.
- The kubelet, container runtime and DRA driver are standard and trusted. In
  v1.35.9 the kubelet stops containers, then unprepares DRA resources, and only
  then publishes the terminal phase (`pkg/kubelet/kubelet.go:2224-2289`,
  `status/status_manager.go:1281-1295`, `cm/dra/manager.go:648-688`).

[`examples/dra-rbac.yaml`](../examples/dra-rbac.yaml) shows the narrow
permissions the mode uses.

## One claim, then one Pod

Each attempt has exactly two objects, both declared durably before either is
created:

- a standalone `resource.k8s.io/v1` ResourceClaim `skygo-<attempt>-<nonce>`
  with one request `accelerator`: `exactly` the policy's class and selectors,
  `allocationMode: ExactCount` and the task's count as an int64. It has no
  owner reference, finalizer, admin access, alternatives, constraints,
  configuration, tolerations or capacity request;
- one directly managed Pod `skygo-<attempt>` (the ordinary retry Pod, without
  an extended resource) whose `spec.resourceClaims` has the single entry
  `accelerator` with `resourceClaimName`, and whose task container lists that
  entry and request in `resources.claims`.

No Job, template, generated claim, helper container or shared claim is used.
The record pins the namespace UID and the class UID and complete spec digest
read at construction; both are read again before every native mutation, each
create, the cancellation deletion and each API cleanup deletion, and any
change refuses that send and is a HOLD. A read that fails or whose response
is incomplete or not cleanly closed is not drift: it defers the send. Start
records both identities with independent nonces and spec digests, consumes
the claim's send grant, sends one create, and pins the UID of an exact `201`;
only then does it consume the Pod's separate grant and send the Pod once.

A response is complete only when its body was read to its end; headers or a
status alone are not. A complete response other than an exact `201` is
terminal: a definite rejection (`AlreadyExists`, `Invalid`, `Forbidden`) is
recorded as rejected, and any other status, `500`, `429` and `409 Conflict`
included, is recorded refused with a witness, even if the API stored the
object, and no later read adopts it. Complete `201` responses are matched
strictly: unknown fields, status in a claim create response, admission
mutation (other than the v1.35.9 Pod defaults listed below), `200`, a
redirect or another nonce are refused with a witness. A lost or incomplete
reply leaves its grant consumed: only a read of the exact declared object
pins it, and a claim whose reply was lost never gets a Pod. A grant that was
never consumed when Start ends is withdrawn. Nothing is resent, in this or
any later process, and network requests never run under the journal lock.

client-go reads and closes every response body but discards the error of its
`Close`. The DRA mode therefore owns each native request's original response:
it records what was read and runs the original `Close` once, and a request is
settled only when that `Close` returned without error. An exact `201` whose
`Close` is still running or failed is kept as the seen UID, truth without
authority: nothing is pinned from it and no Pod is sent after such a claim.
A failed `Close` of any request of an attempt, a read, create or deletion,
is a sticky HOLD of the whole attempt and keeps the integration's `Close`
failing. The original first publishes the failure in its process, and then
records that HOLD in the attempt's record before its operation releases any
permit, even after its caller returned, so no later pin, create,
cancellation, cleanup deletion or retirement follows in this or a later
process; truths already recorded, a seen or pinned UID, a delivered
deletion or a witnessed success, stay. An attempt already in HOLD keeps its
first reason: every HOLD is the same fence (below). If the HOLD cannot be
written, the operation keeps its permits for the process lifetime and the integration's
`Close` fails. A `Close` still running, or a caller's timeout, is not a
failure. A caller's cancellation or deadline ends its wait, not the
original: its operation keeps every permit until the original returns, and
the integration's `Close` joins it. Construction reads, discovery included,
are context-aware requests on the same transport, bounded by the caller and
30 seconds; if one's original `Close` is still held or failed, construction
returns its error with a partial Integration, which the server closes before
anything it depends on.

The Pod is canonicalized with the same Pod defaults and admission outputs as
receipts (below); the v1.35.9 sources add none for these Pods. A claim is
compared exactly, except that the built-in `resource.kubernetes.io/delete-protection`
finalizer is ignored when it is the only one.

## Execution, HOLD and API cleanup

The execution outcome, a sticky HOLD reason and API cleanup are recorded
separately. A witnessed ordinary success of the exact pinned Pod (phase
`Succeeded`, the single task container terminated with code 0, no deletion in
progress) is reported as succeeded. Generic `Failed` (which PodGC can write
without the kubelet), cancellation, a missing or replaced Pod or claim, a
foreign reservation on the claim, or any other unusual state is a HOLD: the
claim, its record and the attempt stay for the operator. A 404, an empty
reservation list, a cleared allocation, a missing finalizer, a node `Ready`
snapshot, a timeout or a later allocation never repair it. Keeping the claim
object does not keep its allocation: the claim controller can deallocate it
independently.

In this first mode every HOLD, whatever its reason, including one recorded
by an earlier version, is the same fence and is handled by the operator: no
recovery pin, create, cancellation, cleanup deletion or retirement follows
it, in any process, and its first reason is kept, never overwritten or
reclassified. Reads and the witness of an execution outcome continue. Each
native mutation is admitted only after its namespace and class rechecks, in
one step under the journal lock that checks the current record and a
published failed `Close` and registers the request; a HOLD or failure before
that admits nothing, and a request admitted before it may still deliver,
which is recorded and never recalled. Automatic handling of failed or
canceled attempts, retries and Jobs remains later work.

The integration's maintenance loop resumes every open record after a restart
and after the core stops observing the job. After a witnessed success, with no
HOLD or cancellation, once `--dra-log-retention` has passed (zero keeps
everything) and while no other operation of the scope runs in any process,
it records intent and sends one graceful, UID-preconditioned deletion of the
Pod, waits for its absence, then does the same for the claim, and retires the
record as a tombstone. That absence is API cleanup only; it is not proof that
a device was physically released, that a driver RPC ran, or native
qualification. Skygo never removes finalizers, clears status, forces deletion
or creates anything in cleanup. Cancellation deletes only the pinned Pod,
gracefully and UID-preconditioned; it keeps the claim in HOLD. A delivered
cancellation is recorded as a fact beside the execution, not as its outcome:
a Pod that then disappears without a witnessed terminal state is reported
Unknown, never Canceled or stopped; only a terminal state witnessed from the
exact Pod is reported, as Canceled.

The integration's `Close` first refuses every further operation of every
target it owns, cached target references included, and cancels its own
`Run`; only then does it join `Run`, every operation and every original
response `Close`. A `Close` that returns an error, after its deadline or a
failed original, never readmits anything; the server then keeps the Store and
state the integration depends on.

## DRA state

`--dra-state` names a separate, private directory: absolute, without symlinks,
owned by the server user with mode `0700`, neither the execution state nor
inside or around it. It holds a marker (`skygo-dra-state.json`: format and
owner) and one scope per DRA target binding with the same lock, permit,
private-file and fsync rules as receipts, but its own record format
(`skygo-kubernetes-dra-record-v1`). Before any lock, scope, record or API write
the server reads the whole root and refuses anything but a fresh empty root or
the exact same-version root of this installation: unknown or legacy entries,
a scope of no configured target (as after a binding change), another marker,
a lock that is not an empty private regular file, or a record that does not
check exactly. A temporary file of the marker or of a record is left only by
an interrupted write; whatever it holds, it is refused for the operator and
never removed, replaced or adopted. Nothing is migrated or adopted. To
change a DRA target's binding, drain it with the old configuration and start
with a fresh root. Execution receipts and their format are unchanged.

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
managed Pod, which has no Pod rows. They are exercised only by storage tests;
the DRA mode does not use them and keeps its own records in the DRA state.

Limits of this stage: rollback of a receipt file to an older valid copy is
detected within one process, and across restarts only through a pinned
`Reference.Instance`. Recovery cannot defend against a privileged namespace
writer that copies a nonce.

## Module and cluster requirements

The integration pins `k8s.io/api`, `k8s.io/apimachinery` and `k8s.io/client-go`
at `v0.35.9` and declares Go `1.25.0`; the core's requirements are unchanged.
The server confirms `resource.k8s.io/v1` by discovery; an installed driver and
an operator-provisioned `DeviceClass` are separate requirements. Skygo does not
install drivers, modify `DeviceClass` or `ResourceSlice` objects, update
allocation status or request administrative device access.

## Remaining work

Each item below is required before a broader DRA claim:

1. Qualify this mode on an authorized cluster with a real DRA driver: the
   device inside the container, normal completion and API cleanup, driver
   downtime, controller restart, cancellation and node loss staying in HOLD.
   The available development cluster runs v1.36.5 without device classes,
   which differs from the pinned v1.35.9 sources.
2. Failed and canceled cleanup, and device-level recovery, under a documented
   release contract.
3. Automatic retry, gated on release of the previous attempt.
4. Native Job execution with DRA, if required, with every replacement Pod's
   consumers tracked; templates later still.
5. Workers and persistent environments with their own claims.

A missing driver or unavailable cluster is a validation hold, not a passing
allocation test.

The mode's authored tests have NOT_RUN. They use loopback APIs with MODELED
status: `TestDRAServerRun` runs the production command with the actual
`server.Run`, a real Store and the generated Connect client;
`TestDRATwoOSProcesses` starts two processes of the test binary over the same
roots; `TestDRATwoSequentialBackends` is one process. The old-binary refusal
gate needs an executable built from the signed base commit and its build
receipt's SHA-256, and reports NOT_RUN without them.
