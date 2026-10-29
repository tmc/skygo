# Kubernetes integration

This Go module builds `skygo-kubernetes`, the skygo CLI and server with a native
Kubernetes backend: native Jobs by default and directly owned Pods for enabled
finite retry policies. The root `skygo` binary remains a portable client and
local server without Kubernetes dependencies. Both binaries speak the same
Connect API and provide the same client commands.

Build from the repository root:

```sh
(cd integrations/kubernetes && GOWORK=off CGO_ENABLED=0 go build \
  -o ../../bin/skygo-kubernetes ./cmd/skygo-kubernetes)
./bin/skygo-kubernetes server --data-dir ./state-kube --disable-local \
  --kubeconfig "$HOME/.kube/config" --context YOUR_CONTEXT --namespace YOUR_NAMESPACE
./bin/skygo-kubernetes run -f examples/kubernetes.yaml --wait
```

With the single-target flags, the server requires `--namespace` and names its
Kubernetes target `kubernetes`. Without `--kubeconfig` or `--context`, it uses
in-cluster credentials. Local execution is also enabled unless `--disable-local`
is set; the deployment disables it. The root executable does not expose these
Kubernetes server flags.

For named targets, provide one strict version-1 YAML file:

```sh
./bin/skygo-kubernetes server --data-dir ./state-kube --disable-local \
  --targets ./targets.yaml
```

See [the target configuration example](examples/targets.yaml). Each file target
requires a name, namespace, and either `in_cluster: true` or both an explicit
`kubeconfig` and `context`. Relative kubeconfig paths resolve against the target
file's directory. Environment variables are not expanded. The file is loaded
once at startup; restart to change it. `--targets` conflicts with explicitly
supplied `--kubeconfig`, `--context`, and `--namespace`, including empty values.
Local execution remains controlled by `--disable-local`.

The file is limited to 1 MiB, one YAML document, and 32 total targets including
local. Names must be unique Kubernetes DNS labels; `local` is reserved. Unknown
fields, duplicate keys, anchors, aliases, merge keys, and nesting beyond 16 are
rejected. Keys are limited to 256 bytes and string values to 4096 bytes (context
names to 1024); Kubernetes names and label values have their narrower syntax
limits. Each labels, selector, accelerator, or required-selector map has at most
32 entries, and a selector allowlist has at most 32 distinct values. Invalid
configuration fails before the server listens; diagnostics omit configuration
values and private connection settings.

Target `labels` are explicitly public placement metadata. Do not put secrets in
them. Kubeconfig paths, context names, API URLs, CA data, and credentials are not
copied into public metadata. Resource maxima of zero add no policy bound; they
do not request or reserve capacity. Selector allowlists do not supply defaults.
A model alias such as `A100` needs `required_node_selector` entries that appear
in its target's allowlist; tasks must request those exact selectors. The generic
`nvidia` name retains its vendor resource meaning.

Targets implementing retry advertise `finite-job-retry-v1`; enabled tasks use
one restartNever Pod
per attempt. Each attempt pins its Create response UID and requires matching
ownership/task/binding identity on later calls. Missing or ambiguous Create does
not authorize adoption or replacement. Another attempt needs positive stopped
container evidence and an explicitly selected reported exit code. Kubernetes
exit codes do not establish ordinary-exit or signal provenance. Cancellation,
deadline, disappearance and incomplete evidence do not authorize retry.

This path needs namespaced Pod `create`, `get`, and `delete` permissions in
addition to existing Job `create/get/delete`, Pod `list`, and Pod log `get`.
Keep the Role scoped to each configured workload namespace. No cluster-wide
permission or new target configuration key is required. Default jobs retain the
Job backend and its existing adoption behavior. See the
[retry contract](../../docs/contracts.md#finite-retries) and the native
checks in the [verification plan](../../docs/testing.md#finite-retry-checks).

Named targets can keep durable execution receipts for ordinary Jobs and retry
Pods:

```sh
install -d -m 0700 /var/lib/skygo/kube-execution
./bin/skygo-kubernetes server --data-dir ./state-kube --disable-local \
  --targets ./targets.yaml --execution-state /var/lib/skygo/kube-execution
```

The operator prepares the directory: an existing absolute path without
symlinks, owned by the server user, mode `0700`, on the same local disk for
its lifetime. The server never creates, guesses or migrates it and never
falls back to the working directory. An unusable directory, including an
explicitly empty value, fails when the server builds its targets.
`--execution-state` requires `--targets` and conflicts with
`--environment-profiles`; targets with a `data` block are refused. Without
the flag, targets behave as before, without receipts.

Each target records, before its single create request, a random nonce in a
private receipt and pins the returned UID; a restarted server recovers only
the object carrying that receipt and never resends a create. Targets sharing
the directory use separate scopes per owner, namespace and binding. Attempts
created without a receipt, including those of a server run without the flag,
are refused rather than adopted: drain or cancel them before enabling the
flag. Durability relies on ordered `fsync` of each receipt file and its
directory and on `flock`; it does not make writes to different files atomic
together. Retired receipts are tombstones that are never reclaimed, and a
store full of them refuses new attempts until an operator acts. See
[durable execution receipts](docs/dra.md#durable-execution-receipts). This
mode does not enable DRA, environment or remote data support, or ordinary
Job cleanup, and has not been qualified against a native cluster.

A named target may also have a `data` block, which fixes its helper image,
approved task images, worker origin and PVC or Secret references. A data block
alone advertises no remote data support: every data task is refused until the
target installs a native qualification through `data.qualification`:

```yaml
    data:
      # helper_image, helper_sha256, origin, ca_pem, task_images, references
      qualification:
        record: ./qualification.json      # operator-reviewed record
        build_receipt: ./build-receipt.json
        policy_revision: cluster-policy-7 # current operator policy
        storage_policy:
          datasets: csi-policy-3          # per PVC reference
```

Both files must be regular files of mode 0400 or 0600 owned by the server user;
relative paths resolve against the target file's directory. The build receipt
is one canonical JSON object, written by the release process, with no
surrounding whitespace:

```json
{"version":"skygo-kubernetes-build-receipt-v1","executable_sha256":"…","helper_image":"…","helper_sha256":"…","helper_platform_digest":"…","helper_os":"linux","helper_architecture":"arm64"}
```

At startup the server hashes its own executable, which must match the
receipt, and refuses the target unless the record matches that executable,
the receipt, the target configuration and its coverage rules. It then reads
the namespace, `/version`, the nodes matching the record's pool selector and
each recorded PVC and its StorageClass, each response limited to 4 MiB, and
refuses on any difference, including an added pool node. Every data start
repeats those reads before its first create. The server needs `get` on its
namespace, `list` on nodes, `get` on PVCs and StorageClasses for these reads;
it never reads Secret data. The record identity becomes part of the target
binding; file names do not. Every qualified task must name the record's pool
in its own node selector, which the target's `node_selectors` must allow.

The policy revisions are the operator's assertions, not facts the API can
attest; runtime, admission and storage behavior can change behind unchanged
API strings. Quiesce and requalify before changing them. A record is trusted
operator configuration: this server consumes it but neither produces one nor
qualifies a cluster.

## Fixed services

`--service-profiles FILE` serves fixed one-replica services on one of the
`--targets`; it requires `--targets` and `--auth-file` and may be combined with
`--environment-profiles`. See [the service profile example](examples/services.yaml).
The file is strict version-1 YAML of mode 0400 or 0600. It names the target,
the UIDs of its namespace and `default` service account, the image pull
secrets that account uses, per-service CPU and memory ceilings, admission
limits for the target binding, and the digest-pinned images each principal may
run. At startup the server reads the namespace, service account and secrets
and refuses any difference; it never creates, modifies or deletes them.

Each service is one owned Deployment (one replica, `maxSurge: 0`) and one
ClusterIP Service in the target namespace. Each create is declared in the
service's committed record before it is sent, and is sent once, without client
retries. An object is the service's only by its recorded UID, or by the
declaration it carries; names and labels never confer ownership. A create
whose outcome cannot be resolved holds the service rather than being retried
or reported absent. Readiness requires the recorded Deployment's observed
generation, owned ready Pods and ready endpoints of the owned Service.

```sh
skygo serve up examples/service.yaml --name web --target kube
skygo serve status
skygo serve forward ID --listen 127.0.0.1:8081
skygo serve down ID
```

`serve forward` relays a loopback listener to
`/services/ID/forward/` on the server, which serves only the service's owner,
only while its current generation is ready, and removes the bearer token and
hop-by-hop headers before the request reaches the application. Upgrades and
CONNECT are refused. Update, rollback, data mounts, workdir, setup and CPU
scaling are not served.

This path needs namespaced `create`, `get`, `list` and `delete` on
Deployments, `get` and `list` on ReplicaSets and Pods, `create`, `get` and
`delete` on Services, `list` on EndpointSlices, `get` on the namespace, the
`default` ServiceAccount and the listed Secrets, and `create` on
`pods/portforward`.

The local `replace github.com/tmc/skygo => ../..` supports development from this
checkout. This is not an independently released module: publishing it requires
a published root-module version and removal of the local replacement.

Run this module's default tests without cluster credentials:

```sh
cd integrations/kubernetes
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

Root `make test`, `make race`, `make check`, and `make build` cover both modules.
Root `go test ./...` does not recurse into this module. See the
[verification plan](../../docs/testing.md) for opt-in live tests.

Build its container from the repository root, using the root build context:

```sh
docker build -f integrations/kubernetes/Dockerfile -t skygo:local .
```

The root Dockerfile builds the local-only executable. See the
[deployment guide](../../docs/kubernetes.md) for singleton storage, credentials,
workload boundaries, and upgrade procedures.

An experimental first DRA mode runs a task that names a `dra` accelerator as
one standalone ResourceClaim and then one Pod, with
`--execution-state DIR --dra-state DIR2` and a dedicated trusted namespace. It
refuses retry, data, workers, environments and service profiles, and keeps every
unusual outcome in HOLD, including a complete non-`201` create reply, a failed
response `Close` and a canceled Pod that vanished without a witnessed terminal
state. It is not natively qualified. See [DRA](docs/dra.md) for policy, ownership,
cleanup and remaining qualification, and
[`examples/dra-rbac.yaml`](examples/dra-rbac.yaml) for its permissions.
