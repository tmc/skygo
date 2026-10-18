# Kubernetes integration

This Go module builds `skygo-kubernetes`, the skygo CLI and server with a native
Kubernetes Job backend. The root `skygo` binary remains a portable client and
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

The server requires `--namespace`. Without `--kubeconfig` or `--context`, it uses
in-cluster credentials. Local execution is also enabled unless `--disable-local`
is set; the deployment disables it. The root executable does not expose these
Kubernetes server flags.

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
