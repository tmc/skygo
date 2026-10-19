# skygo

skygo runs finite, single-node jobs on local Linux/macOS processes or Kubernetes
Jobs, or directly owned Pods for opt-in retries. One Go server owns durable job
records; the CLI uses its generated Connect API. The root Go module and `skygo`
binary have no Kubernetes dependencies.
The separate `integrations/kubernetes` module builds `skygo-kubernetes`, which
adds the Kubernetes backend. No Python, Ray, external database, or `kubectl` is
required by either application.

This is an independent, small implementation of selected SkyPilot concerns:
task validation, target selection, submission, status, logs, and cancellation.
The CLI uses its own task format. The optional `integrations/skycompat` module
parses selected upstream task YAML and provides wire codecs and an authenticated
HTTP handler for health and syntax validation. Imported tasks remain inert. See
its [supported subset](integrations/skycompat/README.md).

## Run locally

Build from this checkout with Go 1.24.1 or newer:

```sh
GOWORK=off CGO_ENABLED=0 go build -o bin/skygo ./cmd/skygo
./bin/skygo server --data-dir ./state
```

In another terminal:

```sh
./bin/skygo targets list
./bin/skygo plan -f examples/local.yaml
./bin/skygo run -f examples/local.yaml --wait --request-id hello-1
./bin/skygo jobs list
./bin/skygo jobs logs JOB_ID
```

The example runs `/bin/echo` on the **server host**, which must have that program.
The application build is pure Go; workload programs remain external inputs.
Successful `--wait` returns exit 0. A failed, canceled, or unknown job returns 2;
usage and API failures return 1.

A timeout does not mean the job was rejected. With retention disabled, retry
with the printed request ID and the same task. With retention enabled, preserve
the exact printed request domain, request ID and original task together; ID alone
is insufficient. A changed task conflicts with a live binding. Restart does not
extend an enabled request's replay window or restore expired results.

## Commands

```text
skygo server --data-dir DIR
skygo server --data-dir DIR --auth-file FILE
skygo-kubernetes server --data-dir DIR --namespace NS [--disable-local]
skygo-kubernetes server --data-dir DIR --targets FILE [--disable-local]
skygo targets list
skygo info [--target NAME --require finite-jobs-v1]
skygo state backup --data-dir DIR --output FILE
skygo state restore --input FILE --data-dir NEW_DIR
skygo state retention inspect --data-dir DIR
skygo state retention enable --data-dir DIR
skygo state retention revalidate --data-dir DIR --ack-local-writers-stopped
skygo state compact --data-dir DIR --output FILE
skygo plan -f task.yaml
skygo run -f task.yaml [--request-domain LINEAGE:EPOCH] [--request-id ID] [--wait]
skygo jobs list [--page-size N] [--page-token TOKEN]
skygo jobs retention [--page-size N] [--page-token TOKEN]
skygo jobs prune --before RFC3339 [--yes]
skygo jobs get ID
skygo jobs logs ID [--follow] [--offset BYTES] [--attempt N] [--cursor TOKEN]
skygo jobs cancel ID --yes
```

`--endpoint` defaults to `http://127.0.0.1:8080`; `SKYGO_ENDPOINT` changes that
default. Choose one credential source: `--token-file`, deprecated `--token`, or
`SKYGO_TOKEN`. Conflicting sources fail. `--timeout` bounds individual RPCs
(default 30 seconds), not the lifetime of `--wait` or log follow. Interrupting a
client command does not cancel its job. Cancellation requires `jobs cancel`.

Output is plain text without color, prompts, or pagers. `--json` writes protobuf
JSON for API commands, command-specific JSON for local state commands, and public
metadata for local auth commands. Log streams
write one JSON object per chunk, with base64 data and byte
offsets. Diagnostics and retry IDs go to stderr. Use `-f -` to read YAML on stdin.

`info` reports capabilities for each configured target. A requirement applies
only to the named target and fails when discovery is unavailable. Discovery is
not a capacity reservation. Ordinary `plan` checks required capabilities against
its selected target; `plan --objective lowest-rate` uses the separate quote path.
With retention disabled or unavailable on an older server,
`run` preflights task extensions only on the default, non-retry path. A selected
retention domain instead uses `SubmitRetainedJob` directly, preserving replay
before current placement; the original task remains unchanged. Legacy commands
on older servers do not acquire newer task semantics.

Stop the server before `state backup` or `state restore`. Backup preserves the
database's installation identity, job records and request bindings. Restore keeps
the owner and accepted job records, but enabled retention receives a fresh
request lineage and closed legacy replay; old request domains become invalid.
Snapshots exclude local logs, workload files and images. Restore requires a new
data directory; neither command overwrites existing output. Keep the original
installation stopped before starting a restored copy. See the
[backup and rollback contract](docs/contracts.md#state-backup-and-rollback) and
[retention guide](docs/retention.md).

For small-team use, [activate scoped authentication](docs/authentication.md).
Readers and operators see their own work; admins have global read/cancel access.
Legacy single-operator mode remains explicit. Local execution still trusts
workloads as the server's OS identity.

## Finite retries

Tasks default to one attempt. An enabled policy selects failed exit codes and
bounds total attempts, including the first. This illustrative YAML fragment is
not a complete task:

```yaml
retry:
  max_attempts: 3
  delay_seconds: 2
  exit_codes: [7]
```

The server requires positive stopped evidence before another attempt. Unknown
execution, an ambiguous Start reply, cancellation, timeout or missing evidence
does not authorize replacement. Local retry requires Linux pidfd support;
Darwin rejects enabled retry policies. Kubernetes retry uses one directly owned
Pod per attempt; tasks without a policy retain the Job path. These guarantees
cover the owned process group or Pod/container, not arbitrary side effects or
exactly-once execution.

`run --wait` includes retry backoff. While the result is retained,
`--json jobs get ID` exposes attempt history and the next deadline;
`jobs logs ID --attempt N` selects a one-based attempt while its output remains
available.
A log stream opened without this flag fixes the current attempt at open and
never follows a later attempt. See the [retry contract](docs/contracts.md#finite-retries).

## Kubernetes

Build the integration from this checkout, then provide an explicit context and
namespace to run its server outside a cluster:

```sh
(cd integrations/kubernetes && GOWORK=off CGO_ENABLED=0 go build \
  -o ../../bin/skygo-kubernetes ./cmd/skygo-kubernetes)
./bin/skygo-kubernetes server --data-dir ./state-kube --disable-local \
  --kubeconfig "$HOME/.kube/config" --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
./bin/skygo run -f examples/kubernetes.yaml --wait
```

For one target, `--namespace` is required; with no kubeconfig/context, the server
uses in-cluster credentials. For named targets, use `--targets FILE` instead of
those three flags. The [configuration example](integrations/kubernetes/examples/targets.yaml)
shows separate namespaces, public target labels, resource maxima and selector
allowlists. File-based targets require an explicit kubeconfig and context, or
`in_cluster: true`; relative paths resolve against the configuration file.

CPU/memory requests and limits, configured accelerator mappings and allowed node
selectors are enforced by the Kubernetes backend. Changing a target's route,
trust or policy holds its previously accepted work until the original binding
is restored. Take a stopped pre-upgrade snapshot before opening existing state;
legacy remote rows without a binding remain held. See the
[binding and rollback contracts](docs/contracts.md#state-backup-and-rollback).

Both binaries provide the same client commands and can connect to either server.
The root binary has no Kubernetes server flags.
[Deployment instructions](docs/kubernetes.md) describe the singleton PVC-backed
installation, authentication, and rollback.

## Boundaries

- Local execution trusts workloads as the server's OS identity and inherits its
  environment. It is not a sandbox. Images, CPU/memory requests and limits,
  accelerators, and node selectors are rejected locally. The Kubernetes deployment disables this target.
- After abrupt local server death, unfinished jobs become `unknown`; they are
  never automatically relaunched or killed using an old PID. Orphan processes
  can remain. Process-group cancellation cannot contain programs that escape
  their group. Normal shutdown stops owned local processes.
- Default Kubernetes tasks use native Jobs without application retries. Enabled
  retry policies use directly owned Pods with UID fences. A stable default Job
  name and ownership checks support adoption, but execution is not exactly once.
  Unschedulable jobs remain starting. Placement validates capabilities, not
  live capacity or price.
- Local logs retain at most the first 1 MiB per execution attempt, subject to
  enabled retention cleanup. Kubernetes logs read
  at most the
  first 1 MiB currently available from the sole owned Pod; kubelet rotation,
  deletion, and eviction can remove or change that prefix. Offsets are not an
  archival cursor. Truncation is explicit; multiple owned Pods are ambiguous.
- Retention is disabled by default. Historical admission keeps the 10,000-job
  and 16 MiB aggregate task limits; known requests remain replayable at capacity.
  Explicit stopped-state activation enables request epochs, logical quotas and
  paged pruning. Local output reservations share a bounded pool; configured
  domain and file maxima are not simultaneous capacity promises. Physical
  observations and watermarks do not reserve disk or guarantee a maximum file
  size. See [retention and storage maintenance](docs/retention.md).
- The controller admits up to 32 starting/running jobs; further jobs remain
  queued. Unknown executions may still exist outside that accounting. Log
  streaming permits 32 concurrent streams, each lasting at most five minutes.
- No VM provisioning, uploads, SSH, interactive clusters, DAGs, distributed
  training, failover to another target, model serving, HA, or rolling overlap.
  Scoped control-plane access does not isolate workloads from one another.

See [task and API contracts](docs/contracts.md) and the
[manual verification plan](docs/testing.md).

## Development

```sh
make test
make race
make check
make build
```

These targets cover all three Go modules with `GOWORK=off`. `make check` also needs
Buf; `make tools` installs the pinned development tools. Running `go test ./...`
from the root alone skips both integration modules.

The integration uses a local `replace` directive for the unpublished root module.
It builds from this checkout; independent module releases need a published root
version and removal of that local replacement. See the
[integration README](integrations/kubernetes/README.md).

Default tests use local fixtures and HTTP test servers; no cloud or Kubernetes
credentials are needed. Live Kubernetes tests are explicitly gated. Protobuf
sources are in `proto/`; Buf configuration generates messages and Connect
clients/handlers under `gen/`. Generated files must be regenerated, not edited.

