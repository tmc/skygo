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
