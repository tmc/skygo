# skygo-gce

Module `github.com/tmc/skygo/integrations/gcp`, package `gce` and command
`skygo-gce`: lifecycle of one Compute Engine VM and its one owned boot disk.

    skygo-gce {start|stop|restart|down|status} -config FILE -state DIR [-credentials FILE] [-refresh] [-timeout D] [-steps N] [-interval D] [-json]

This is lifecycle only. It runs no skygo job, artifact, log, cancel or SSH
session and is not a skygo execution target. Full X4 acceptance (actual
SSH/S11 composition, canonical job/artifact/log/cancel, native GCP
campaign) is OPEN.

## Verification

Tests use a local Compute model with real HTTPS transport and private
state files. They do not qualify a real Compute Engine deployment, Linux
image, SSH host identity, or job execution target. Run the normal and race
tests and `go vet ./...` before using changes to the lifecycle code.

## Configuration

A private (mode 0600, owned, one link) JSON file with exactly these fields:
`project`, `compute_project_id` (numeric Compute project id), `zone`,
`machine_type` (`zones/ZONE/machineTypes/T`), `subnet`
(`projects/P/regions/R/subnetworks/S`), `image` (`project`, exact `name`,
numeric `id`; no families), `disk_type` (`zones/ZONE/diskTypes/T`),
`disk_gib` (10 to 65536), `ssh_user`, `authorized_key` (one `ssh-ed25519`
public key: the complete key wire with exactly 32 key bytes; no other key
type is supported). Keys are matched after JSON unescaping and case
folding, so no field may appear twice under any spelling.

## Authentication

`-credentials` names a private JSON file with `access_token`,
`token_type` (Bearer) and `expiry`, each exactly once under its exact
spelling (escaped aliases count). Nothing else is accepted: no refresh
token, service account key, application default credentials, metadata
server, gcloud or environment. The token is never written to state,
status or errors. Each request needs at least two minutes of validity;
renewal is the operator's job between runs.

## Behavior

- ARM64 only. Before anything that charges, one `images.get` of the
  configured image and one `machineTypes.get` of exactly
  `projects/PROJECT/zones/ZONE/machineTypes/T` must report their exact
  name, id, canonical `selfLink` and (bare) zone, the image `READY`, and
  `architecture` exactly `ARM64`; an unspecified, `X86_64`, missing or
  unknown architecture, or any identity mismatch, is refused with nothing
  sent. A private versioned admission witness of both replies is
  committed before the first mutation. A state without it (for example,
  from before admission existed) never starts, stops or qualifies a
  machine; it can only tear down what it exactly owns. The configured
  image id is the operator's enrollment of a trusted Linux image: Compute
  Engine attests no operating system, and the command never boots,
  connects to or inspects the VM.
- The disk is created explicitly from the exact image and must report the
  pinned `sourceImageId` before any VM is inserted. The VM attaches only
  that disk with `autoDelete: false`, no service account, no external
  address and metadata limited to the single `ssh-keys` entry,
  `block-project-ssh-keys=true` and an ownership witness. A project with
  OS Login enabled is refused.
- Each mutation has its own random request id, is committed before its
  single dispatch and is never resent. A lost reply is recovered only from
  the configured zone's operation list; missing, truncated or expired
  evidence holds (exit 3).
- `down` is latched. It deletes the VM, verifies it gone, deletes the
  unused owned disk and verifies it gone; an owned disk found holding
  another image is still deleted. A numeric id is pinned only with a
  durable witness of the settled successful create that reported it,
  independent of the bounded operation history; a stored id without one is
  refused before any request. A resource counts as gone only if its id is
  so pinned and a fresh lookup finds nothing with no request unresolved:
  `ABSENT` after the command's own delete, `REMOVED-EXTERNALLY` otherwise.
  A lost, failed or unpinned create holds.
- Start, stop, restart and `status -refresh` qualify a machine only if its
  disk reports exactly the pinned image id.
- `status` reads the stored state and calls no service. `status -refresh
  -credentials FILE` opens the state and observes the resources with GET
  requests only, recording what it saw; it never sends, resends or
  prepares a mutation, and never clears an unresolved request or fault.
- A pending original request keeps the state lock. `-timeout` sets the
  operation deadline; closing must end by a cleanup deadline fixed 90
  seconds later, longer than one request's fixed 60 second deadline. The
  command runs its steps one at a time on one machine and only observes
  these deadlines; no deadline bounds the state's file system calls. After
  the operation deadline nothing new starts, and the state is closed only
  after the running step returns. If a step or close is still running at
  the cleanup deadline, the command reports HOLD and exits 1: the process
  keeps that step, the machine and the lock until the step returns or the
  process exits, a later return changes nothing, the state records any
  request as possibly sent, and a later run recovers it without resending.
  Neither a deadline nor process exit is a successful close.
- If a response body or state file fails its first close, or a state
  commit is not confirmed, the command stops, keeps that owner and the
  lock, and exits 1. An unconfirmed commit is retried in full only when
  closing and never rolled back.

## Trust boundary

Compute Engine stop, start and delete name resources and accept no id
precondition. A writer that deletes and recreates the VM or disk under the
same name between this command's check and its mutation causes the
mutation to act on the replacement; the operation's target id reveals it
only afterward. Use an operator-controlled project and zone, an external
lease on the reserved names and no other writer that reuses them. If that
boundary is unacceptable, do not use this command for mutation.

## State versions

The private state writer uses version 2. Its intent counter distinguishes a
new goal from an unresolved earlier request; the completed-intent counter
survives a later observation of guest shutdown. Repeating an unfinished goal
continues its original, without resending it. Repeating a completed goal can
prepare a fresh operation with a new request ID.

The reader validates version 1 before upgrading it in memory. It treats all
legacy operations as one intent and marks that intent completed only when
the legacy Reached bit records completion. History alone never proves that
a goal was observed reached. A legacy unreached goal may therefore remain
held instead of issuing a duplicate request. Direct Drive also begins a fresh unfinished intent when a verified power
state requires new work after a completed goal. A pending original or a
DONE operation whose resulting state has not been observed remains in that
unfinished intent; repeating the same goal does not send it again.

A later save writes version 2;
old readers refuse it. Missing or mixed-version intent fields are refused.
