# skygo

skygo runs finite, single-node jobs on local processes or Kubernetes. A Go server
keeps durable job records; the CLI uses its generated Connect API. The root
module builds `skygo` without Kubernetes dependencies. The separate
[`integrations/kubernetes`](integrations/kubernetes/README.md) module builds
`skygo-kubernetes`, with native Jobs and directly owned Pods for finite retries.

This independent implementation covers selected SkyPilot concerns: task
validation, target selection, submission, status, logs, cancellation, finite
retries, and bounded directory snapshots. It has its own task format. The optional
[`skycompat`](integrations/skycompat/README.md) module parses a selected upstream
YAML subset and provides health/validation compatibility. `skycompat import
--shell /bin/sh` lowers a supported task to a skygo task file that `skygo run -f`
executes. The subset is narrow: none of SkyPilot 0.14.0's ten bundled examples
import as written; each stops at its first unsupported field, such as
`autostop`, `accelerators`, `infra`, `job_recovery`, `network_tier`, `service`,
`pool` or a multi-document job group.

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

## Boundaries

Not yet supported:

- Cloud provisioning. The [`ec2`](integrations/ec2/README.md) module has no
  command or target and has not been run against AWS.
- SSH targets. The [`ssh`](integrations/ssh/README.md) module is a transport to
  a pre-installed agent, with no command.
- Multi-node jobs. The worker-group API is defined but not served.
- Model serving. Only portable serving types exist.
- Pools, autostop and autodown.
- `exec`, `stop`, `start` and `down` commands.
- GPUs on the local target, which refuses accelerator and resource requests.
  Kubernetes accelerator requests are tested only against fakes.
- TLS. The server listens on plain HTTP.

SIGINT or SIGTERM stops the server and cancels its running local jobs. After an
abrupt death such as SIGKILL, a running local job becomes unknown and may keep
running untracked; an old PID does not authorize relaunch or termination, so
restart does neither. A workflow step left unknown this way blocks its
dependents, and that workflow never finishes.

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
