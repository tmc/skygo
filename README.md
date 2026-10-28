# skygo

skygo runs finite, single-node jobs on local processes or Kubernetes. A Go server
keeps durable job records; the CLI uses its generated Connect API. The root
module builds `skygo` without Kubernetes dependencies. The separate
[`integrations/kubernetes`](integrations/kubernetes/README.md) module builds
`skygo-kubernetes`, with native Jobs and directly owned Pods for finite retries.

This independent implementation covers selected SkyPilot concerns: task
validation, target selection, submission, status, logs, cancellation, finite
retries, and bounded directory snapshots. It has its own task format. Packages
`skycompat/taskyaml` and `skycompat/tasklower` parse a selected upstream YAML
subset into typed values and lower it to that format, refusing what it cannot
represent. The optional [`skycompat`](integrations/skycompat/README.md) module
provides health/validation compatibility. `skygo run task.yaml` and
`skygo plan task.yaml` accept native task files or supported SkyPilot YAML
directly; unsupported execution requirements are refused before submission.
`skycompat import
--shell /bin/sh` lowers a supported task to a skygo task file that `skygo run -f`
executes. The parser preserves `resources.autostop` and
`resources.job_recovery`; lowering refuses either field when non-null.

## Run locally

Build from this checkout with Go 1.24.1 or newer:

```sh
GOWORK=off CGO_ENABLED=0 go build -o bin/skygo ./cmd/skygo
./bin/skygo server --data-dir ./state
```

In another terminal, from the checkout root:

```sh
./bin/skygo targets list
./bin/skygo plan -f examples/local.yaml
./bin/skygo run -f examples/local.yaml --wait --request-id hello-1
./bin/skygo jobs list
```

The example executes `/bin/echo` on the server host. Workload programs are
external inputs; the application build is pure Go. Successful `--wait` returns
exit 0; unsuccessful waited jobs return 2, and usage or API failures return 1.

`--endpoint` defaults to `http://127.0.0.1:8080`; `SKYGO_ENDPOINT` changes that
default. Select one credential source: `--token-file`, deprecated `--token`, or
`SKYGO_TOKEN`. See [scoped authentication](docs/authentication.md).

## Commands

Use `skygo --help` and each command's `--help` for flags. The main commands are
`server`, `targets list`, `info`, `plan`, `run`, `jobs`, `workflow`, `artifacts`,
`state`, `auth` and `dashboard`. `--json` selects JSON output. Job logs and
cancellation use `jobs logs JOB_ID` and `jobs cancel JOB_ID --yes`.

Keep a submitted task, request ID, and any printed request domain together for
recovery. A timeout does not establish whether submission succeeded. Retrying
the exact accepted request replays its job; changing the task conflicts.
Interrupting a client does not cancel the workload.

## Directory snapshots

Explicitly install an artifact directory before starting transfers. Upload a
quiescent input directory, reference it in a task's `workdir`, run optional setup,
and collect declared outputs. Local workspace execution requires Linux process
ownership support; transfer and target capabilities are discovered separately.
The [artifact guide](docs/artifacts.md) includes a complete upload/run/download
example, resume commands, limits, expiry, and storage cautions.

## Finite retries

Tasks default to one attempt. An opt-in policy bounds attempts and selects failed
exit codes. Another attempt requires positive stopped evidence; an unknown
execution does not authorize replacement. Local retry requires Linux pidfd
support. See the [retry contract](docs/contracts.md#finite-retries).

## Kubernetes

The optional integration keeps Kubernetes dependencies out of the core module.
Both binaries provide the same client commands. See the
[integration README](integrations/kubernetes/README.md) for builds and target
configuration, and the [deployment guide](docs/kubernetes.md) for the singleton
PVC-backed server.

## Local resources

CPU and memory requests use host inventory and admission accounting. NVIDIA GPU
requests use `nvidia-smi` inventory and reserve matching device UUIDs within the
backend. Each local job receives `CUDA_VISIBLE_DEVICES` and
`NVIDIA_VISIBLE_DEVICES` for its selected devices. GPU visibility relies on
cooperating workload programs and provides no device isolation.

## Boundaries

Not yet supported:

- Cloud provisioning. The [`ec2`](integrations/ec2/README.md) module has no
  command or target and has not been run against AWS.
- SSH targets. The [`ssh`](integrations/ssh/README.md) module provides a
  transport to an operator-installed agent; it has no bootstrap command.
- Multi-node jobs. The worker-group API is defined but not served.
- Model serving. Only portable serving types exist.
- Pools, autostop and autodown.
- `exec`, `stop`, `start` and `down` commands.
- Enforced CPU or memory limits, container images, and node selectors on the
  local target.
- TLS for the standalone `skygo server` command, which listens on plain HTTP.

Plain local jobs (no workdir, no retry policy) run under a detached supervisor,
the `skygo` binary re-executed, which owns the workload, its log and its final
status. Stopping or killing the server leaves the job running; a restarted
server with the same data directory reattaches after checking the supervisor's
PID, kernel start time and boot identity, and reports the real result and logs.
Success also requires the original leader's exit, drained output, and observed
absence of its cooperative process group. Uncertain group closure stays unknown
and keeps its resource reservation. It never runs the job twice.
A job whose supervisor vanished without a final
status becomes unknown and is not re-run. The server's service manager must not
kill the server's whole process tree, or the supervisors die with it.

Local jobs with a workdir or retry policy, and all local jobs when output
retention is enabled, are still owned by the server: SIGINT or SIGTERM cancels
them, and after SIGKILL they become unknown and may keep running untracked. An
old PID does not authorize relaunch or termination, so restart does neither.
A workflow step left unknown blocks its dependents, and that workflow never
finishes.

For plain jobs without output retention, a resource-capacity refusal proven to
precede execution leaves the same job queued for later admission. Other startup
errors never authorize another launch. Retained, workspace and retry attempts
do not yet have this deferral transition.

Local execution trusts workloads as the server's OS user and inherits its
environment. It is not a sandbox. Kubernetes ownership checks do not provide
exactly-once execution.

`state backup` copies only the database; `state restore` refuses artifact-enabled
snapshots. The separate stopped companion commands copy and restore an eligible
database/artifact pair. Logs, credentials, workload programs and external resources
are excluded. Keep the original installation stopped when using its restored
copy. See
[artifact storage](docs/artifacts.md#storage-and-backup),
[backup contracts](docs/contracts.md#state-backup-and-rollback), and
[retention](docs/retention.md).

## Development

```sh
make test
make race
make check
make build
```

These targets cover the root, `ssh`, `ec2`, `kubernetes` and `skycompat`
modules with `GOWORK=off`.

Release-oracle tests in `internal/cli`, `internal/rpc` and `internal/store` need
prebuilt executables by absolute path with their SHA-256 digests:
`SKYGO_ARTIFACT_BINARY` and `SKYGO_ARTIFACT_BINARY_SHA256` name a `skygo` built
from this checkout, and `SKYGO_S05_BINARY` and `SKYGO_S05_BINARY_SHA256` name a
`skygo` built from predecessor commit `db1c8bbf`. `make test` skips these tests
when both variables of a pair are unset; a partial or mismatched pair fails.
`make test-fixtures` sets `SKYGO_REQUIRE_FIXTURES=1`, which makes a missing pair
fail too:

```sh
SKYGO_ARTIFACT_BINARY=... SKYGO_ARTIFACT_BINARY_SHA256=... \
SKYGO_S05_BINARY=... SKYGO_S05_BINARY_SHA256=... make test-fixtures
```

`make check` needs Buf; `make tools` installs pinned tools. Protobuf sources live
in `proto/`; Buf generates messages and Connect clients/handlers in `gen/`.
The integration modules use local replacements for the unpublished root module.
See [task/API contracts](docs/contracts.md) and [testing](docs/testing.md).
