# skygo

skygo runs finite, single-node jobs on local Linux/macOS processes or Kubernetes
Jobs. One Go server owns durable job records; the CLI uses its generated Connect
API. Kubernetes is optional. No Python, Ray, external database, or `kubectl` is
required by the application.

This is an independent, small implementation of selected SkyPilot concerns:
task validation, target selection, submission, status, logs, and cancellation.
It does not implement SkyPilot's API or YAML format.

## Run locally

Build from this checkout with Go 1.24.1 or newer:

```sh
CGO_ENABLED=0 go build -o bin/skygo ./cmd/skygo
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

Use the printed request ID with the same task when retrying a submission whose
response was lost. A timeout does not mean the job was rejected. Reusing an ID
with a different task fails. Request bindings survive server restart.

## Commands

```text
skygo server --data-dir DIR [--namespace NS] [--disable-local]
skygo targets list
skygo plan -f task.yaml
skygo run -f task.yaml [--request-id ID] [--wait]
skygo jobs list [--page-size N] [--page-token TOKEN]
skygo jobs get ID
skygo jobs logs ID [--follow] [--offset BYTES]
skygo jobs cancel ID --yes
```

`--endpoint` defaults to `http://127.0.0.1:8080`; `SKYGO_ENDPOINT` changes that
default. `--token` overrides `SKYGO_TOKEN`. `--timeout` bounds individual RPCs
(default 30 seconds), not the lifetime of `--wait` or log follow. Interrupting a
client command does not cancel its job. Cancellation requires `jobs cancel`.

Output is plain text without color, prompts, or pagers. `--json` writes protobuf
JSON; log streams write one JSON object per chunk, with base64 data and byte
offsets. Diagnostics and retry IDs go to stderr. Use `-f -` to read YAML on stdin.

## Kubernetes

To run the server outside a cluster, provide an explicit context and namespace:

```sh
./bin/skygo server --data-dir ./state-kube --disable-local \
  --kubeconfig "$HOME/.kube/config" --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
./bin/skygo run -f examples/kubernetes.yaml --wait
```

With `--namespace` and no kubeconfig/context, the server uses in-cluster
credentials. [Deployment instructions](docs/kubernetes.md) describe the
singleton PVC-backed installation, authentication, and rollback.

## Boundaries

- Local execution trusts workloads as the server's OS identity and inherits its
  environment. It is not a sandbox. Images, CPU/memory requests, and accelerators
  are rejected locally. The Kubernetes deployment disables this target.
- After abrupt local server death, unfinished jobs become `unknown`; they are
  never automatically relaunched or killed using an old PID. Orphan processes
  can remain. Process-group cancellation cannot contain programs that escape
  their group. Normal shutdown stops owned local processes.
- Kubernetes uses native Jobs without application retries. A stable Job name
  and ownership checks support adoption, but execution is not exactly once.
  Unschedulable jobs remain starting. Placement validates capabilities, not
  live capacity or price.
- Local logs retain the first 1 MiB per job. Kubernetes logs read at most the
  first 1 MiB currently available from the sole owned Pod; kubelet rotation,
  deletion, and eviction can remove or change that prefix. Offsets are not an
  archival cursor. Truncation is explicit; multiple owned Pods are ambiguous.
- A single locked store retains at most 10,000 job/request bindings, without
  automatic pruning, with accepted task payloads capped at 16 MiB in aggregate.
  Existing request IDs remain retryable at capacity. Budget local log space up to
  roughly 10 GiB plus metadata. There is no retention management API.
- The controller admits up to 32 starting/running jobs; further jobs remain
  queued. Unknown executions may still exist outside that accounting. Log
  streaming permits 32 concurrent streams, each lasting at most five minutes.
- No VM provisioning, uploads, SSH, interactive clusters, DAGs, distributed
  training, automatic rescheduling, model serving, multi-user authorization,
  HA, or rolling overlap. The single bearer token authorizes all operations.

See [task and API contracts](docs/contracts.md) and the
[manual verification plan](docs/testing.md).

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
CGO_ENABLED=0 go build ./cmd/skygo
```

Default tests use local fixtures and HTTP test servers; no cloud or Kubernetes
credentials are needed. Live Kubernetes tests are explicitly gated. Protobuf
sources are in `proto/`; Buf configuration generates messages and Connect
clients/handlers under `gen/`. Generated files must be regenerated, not edited.

