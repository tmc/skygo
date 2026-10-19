# Manual verification

Run the default suite without cluster credentials:

```sh
make test
make race
make check
make build
```

These targets test both modules with `GOWORK=off`; root `go test ./...` alone
does not visit the nested integration module. `make check` also needs Buf
(install the pinned tools with `make tools`).

The suite includes real local subprocesses and generated Connect clients over
HTTP test servers. Fake Kubernetes clients do not qualify a live cluster.
Keep command output and the tested source commit outside the module.

## CLI workflow contracts

Run `GOWORK=off go test ./internal/cli -run '^TestCLIParity$' -count=1`
for the source-derived upstream workflow checks in `internal/cli/testdata/parity`.
They build the production CLI and use a fresh real local control plane per script
for planning, job lifecycle, logs, cancellation, JSON, offline help and input
validation.
Comments pin the upstream source used for each analogue and identify Go-specific
contracts. These tests do not execute upstream SkyPilot or establish drop-in CLI
compatibility; command names, task YAML, defaults and exit codes differ.

`GOWORK=off go test ./internal/cli -run '^TestCoverage$' -count=1`
runs separate production server and CLI processes for interrupted log follow,
graceful restart/request replay, writer exclusion, and responses lost after
submission/cancellation was processed. Coverage-mode builds collect helper
subprocess data; the parent test's displayed percentage alone does not include
all subprocess coverage. Parent `-race` does not instrument those separately
built executables.

## Local lifecycle

Use a new private task directory. In terminal A:

```sh
mkdir -p "$HOME/tmp/skygo/manual-test"
./bin/skygo server --data-dir "$HOME/tmp/skygo/manual-test/state"
```

In terminal B:

```sh
./bin/skygo plan -f examples/local.yaml
./bin/skygo run -f examples/local.yaml --wait --request-id manual-hello
./bin/skygo run -f examples/local.yaml --wait --request-id manual-hello
./bin/skygo --json jobs list
```

Expect `local`, then the same successful job ID on both submissions. Copy that
ID and check `jobs logs ID` prints `hello from skygo`. Restart terminal A using
the same data directory and repeat the submission: expect the same ID and logs.
Starting another server with that directory must fail the writer lock.

`./bin/skygo info --target local --require finite-jobs-v1` must report the local
finite-job capability. An unknown target or required capability must fail.
Use `--json` to inspect the typed response; `targets list` retains its plain table.

Stop terminal A, then make a database-only snapshot and restore into a fresh
directory:

```sh
./bin/skygo state backup --data-dir "$HOME/tmp/skygo/manual-test/state" \
  --output "$HOME/tmp/skygo/manual-test/state.db"
./bin/skygo state restore --input "$HOME/tmp/skygo/manual-test/state.db" \
  --data-dir "$HOME/tmp/skygo/manual-test/restored"
./bin/skygo server --data-dir "$HOME/tmp/skygo/manual-test/restored"
```

Repeat the original completed submission from terminal B: expect the same ID
and status. Logs are excluded from the snapshot and are unavailable in this
fresh restore. Stop this server before returning to the original directory;
the two copies share an installation identity. Repeating backup to the existing
file or restore to the existing directory must fail without overwriting either.
Backup while the original writable server is running must fail its database lock.
Do not point an old unfenced binary at migrated state as a rollback test.

```sh
./bin/skygo run -f examples/local-sleep.yaml --request-id manual-sleep
./bin/skygo jobs logs SLEEP_JOB_ID --follow
```

Interrupt log follow with Ctrl-C. `jobs get SLEEP_JOB_ID` must still show a live
job. `jobs cancel SLEEP_JOB_ID` must reject the missing `--yes`; retry with
`--yes` and poll get until canceled. Use a new request ID for each new execution.

```sh
printf 'argv: [/bin/sh, -c, "exit 7"]\ntarget: local\n' | \
  ./bin/skygo run -f - --wait --request-id manual-failure
printf 'exit=%s\n' "$?"
```

Expect failed state and CLI exit 2. `jobs get missing` should fail with exit 1.
An unknown YAML key must fail before an RPC. Set `image: busybox:1.37.0` and
`target: local` in a task: planning must reject that combination.

For crash recovery, submit a short-lived sleep, SIGKILL only the known test
server PID, and restart it with the same directory. The unfinished job must
become unknown, never succeeded or relaunched. Wait for the short workload to
exit; do not signal a PID recovered from durable records. After normal shutdown,
remove only the disposable directory when its evidence is no longer needed.

## Scoped authentication

Follow [the scoped setup](authentication.md#activate) in a fresh private task
directory. Use two operator principals and one reader; never print their token
files. With each operator's `--token-file`, submit the same request ID and expect
different jobs. A retry using a second token for the same principal must return
the original job. Foreign and valid-but-missing IDs must have identical errors.
The reader must be unable to submit or cancel; the admin must see both operators.

Start an authenticated log follow, then revoke its public token ID using the
local auth command. The follow must end while the workload remains running.
Demote an admin following another principal's job; that follow must also end.
New reads of the demoted principal's own work remain allowed. Invalid auth
configuration must withdraw readiness and refuse authentication without stopping
durable reconciliation. Restore a valid newer configuration before continuing.

Restart the scoped server with the same state/auth file, then retry a completed
request. Take an offline database snapshot and restore to a fresh directory with
the latest matching auth file. Scope and request identity must survive; local
logs remain excluded. Starting protected state without its auth file must fail.

`GOWORK=off go test ./internal/cli -run '^TestAuthScripts$' -count=1` runs the
actual daemon workflows. The optional historical-binary fixture lane additionally
checks that the exact pre-auth binary refuses protected state byte-identically;
provide its verified executable with `SKYGO_S01_BINARY` and expected digest with
`SKYGO_S01_SHA256`. An ordinary skipped
historical fixture is not that acceptance receipt.

## Disposable Kubernetes backend

These commands create and delete resources. Use a disposable cluster that you
explicitly own. A container runtime and `kind` are developer tools here, not
application dependencies. Pin a kind node image supported by your local setup.

```sh
kind create cluster --name skygo-test --image YOUR_PINNED_KIND_NODE_IMAGE
kubectl --context kind-skygo-test create namespace skygo-backend-test
(cd integrations/kubernetes && GOWORK=off \
SKYGO_KUBE_TEST=1 \
SKYGO_KUBE_CONTEXT=kind-skygo-test \
SKYGO_KUBE_NAMESPACE=skygo-backend-test \
SKYGO_KUBE_IMAGE=YOUR_APPROVED_BUSYBOX_IMAGE \
go test -count=1 -v ./internal/kube -run '^TestLive$')
```

Expect success/log read, failed workload, cancellation, and stable-identity
adoption checks to pass. Tests clean their own Jobs. These checks do not prove
server Pod lifecycle, Service routing, RBAC, storage mounts, or rollout behavior.

## In-cluster server

Follow [the deployment instructions](kubernetes.md) in the disposable cluster.
Use the exact tested image digest and rendered manifest. Then verify:

1. The Pod runs non-root, mounts state read/write and rootfs read-only, and the
   Service has type ClusterIP. `targets list` reports Kubernetes only.
2. Without `SKYGO_TOKEN`, `targets list` fails authentication; with the chosen
   token it succeeds through port-forward. `/startup`, `/live`, and `/ready`
   return 200 once initialized.
3. `run -f examples/kubernetes.yaml --wait` succeeds and its logs contain the
   expected text. Repeat with the same request ID: no second native Job appears.
4. Run a sleeping task. Restart the Deployment while following logs. The stream
   disconnects, the task Job remains, and the restarted server observes that
   same task without creating another Job. Check state and retry identity after
   the PVC remounts.
5. Cancel a sleeping task and confirm both observed cancellation and foreground
   deletion of its native Job/Pod. Delete an uncanceled test Job externally:
   its control-plane state must become unknown.
6. Request an unavailable resource in a disposable task. It remains starting
   with a scheduling reason rather than reporting success. Remove it afterward.
7. Inspect `kubectl --context kind-skygo-test -n skygo get events` and server
   logs for mount, scheduling, probe, RBAC, and termination failures. Test
   direct ingress denial from another Pod when NetworkPolicy is enforced.
8. Exercise SIGTERM during active streams and submissions. Check readiness
   withdrawal and bounded termination; measure peak CPU/memory and startup/drain
   time under representative load before replacing illustrative YAML budgets.
9. Practice backup, upgrade, and rollback from the deployment guide. Confirm
   restored metadata, identity, and workload ownership before accepting traffic.

Do not claim production readiness from rendering manifests or passing unit
fixtures. A live run must record context, Kubernetes version, source commit,
image digest, rendered configuration, observed outcomes, and cleanup.

Remove the disposable cluster when finished:

```sh
kind delete cluster --name skygo-test
```
