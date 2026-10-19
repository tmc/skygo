# Kubernetes deployment

The supported topology is one server with one exclusively locked database on a
PVC. `deploy/` uses a one-replica Deployment with `Recreate`. Upgrades interrupt
API and log connections. Do not add replicas, rolling overlap, an HPA, or a PDB
as a substitute for state replication. Native workload Jobs continue during
server downtime; local execution is disabled in this deployment.

## Prepare and install

Use a dedicated namespace and a context you have explicitly selected. The
commands below mutate that cluster. They are an installation recipe, not a
statement that a deployment has been tested there.

Build an image from the reviewed checkout, load it into a disposable cluster or
publish it to your own registry, and record its digest. Build with the integration
Dockerfile and repository root as the context; the root Dockerfile builds the
local-only server. The integration Dockerfile accepts
`--build-arg GO_IMAGE=golang@sha256:...` for a pinned builder. Release deployments
must replace the illustrative image tag with a tested digest.

```sh
docker build -f integrations/kubernetes/Dockerfile -t skygo:local .
# For a disposable kind cluster only:
kind load docker-image skygo:local --name skygo-test
```

Create a working copy of `deploy/` outside the checkout. Update its image using
`kustomize edit set image skygo=YOUR_REGISTRY/skygo@sha256:YOUR_DIGEST`, or edit
`server.yaml`. Select a StorageClass and size, and replace the illustrative
resource/probe budgets with measurements from representative load. The fsGroup
requires a volume driver that provides the expected write access.

```sh
kubectl --context YOUR_CONTEXT apply -f deploy/namespace.yaml
# TOKEN_FILE is a private file containing your chosen random bearer token.
kubectl --context YOUR_CONTEXT -n skygo create secret generic skygo-auth \
  --from-file=token="$TOKEN_FILE"
kubectl kustomize YOUR_DEPLOY_DIRECTORY
kubectl --context YOUR_CONTEXT apply --dry-run=server -k YOUR_DEPLOY_DIRECTORY
kubectl --context YOUR_CONTEXT apply -k YOUR_DEPLOY_DIRECTORY
kubectl --context YOUR_CONTEXT -n skygo rollout status deployment/skygo
kubectl --context YOUR_CONTEXT -n skygo port-forward service/skygo 8080:8080
```

The private token file must contain no trailing newline (for example, write it
using `printf '%s'` with restrictive file permissions). The secret template is
deliberately excluded from Kustomize. Do not commit a token. The server reads it
at startup, so token rotation requires a restart.
In another terminal, set `SKYGO_TOKEN` from the same private file and run:

```sh
export SKYGO_TOKEN="$(cat "$TOKEN_FILE")"
./bin/skygo targets list
./bin/skygo run -f examples/kubernetes.yaml --wait
```

Only the `kubernetes` target should appear. The Service is ClusterIP; there is no
Ingress or LoadBalancer. The default NetworkPolicy denies direct ingress to the
server Pod. It requires a network plugin that enforces NetworkPolicy; verify
port-forward and probe behavior on your cluster. Egress is not restricted,
because Kubernetes API and DNS destinations depend on the installation.
[NetworkPolicy behavior](https://kubernetes.io/docs/concepts/services-networking/network-policies/)
is a cluster-level prerequisite, not a guarantee supplied by these manifests.

For direct clients, deliberately configure authenticated TLS termination and a
narrow ingress policy first. The binary serves HTTP, not TLS. A bearer token
alone does not encrypt traffic. Health endpoints share this private listener,
are unauthenticated, and expose no job data. There is no metrics or pprof route.

## Identity and workload isolation

The server ServiceAccount may create/get/delete Jobs, list Pods, and read Pod
logs in this namespace. It has no permission to read Secrets. The server needs
its mounted Kubernetes token; workload Pods do not. Users of the single API
token are trusted operators with permission to run images in the namespace.
This is not a multi-tenant boundary or a hardened untrusted-code service.

The server runs non-root with a read-only root filesystem, dropped capabilities,
RuntimeDefault seccomp, and a writable state volume. These settings constrain
the server container; they are not a promise that arbitrary task images satisfy
restricted Pod Security policy. Apply workload admission/resource policy for
your environment and test image compatibility.

## Lifecycle and storage

`/startup` answers only after state and target initialization and listener bind.
`/live` reflects the controller's durable reconciliation health; remote API
outages alone must not restart it. `/ready` additionally becomes false during
shutdown. These distinct probe roles follow the
[Kubernetes probe contract](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/).
The values in YAML are illustrative budgets and require measurement.

On SIGTERM the server withdraws readiness, cancels RPC/log contexts and its
reconciliation loop, and bounds HTTP shutdown. Active log streams disconnect;
clients can reconnect subject to log retention. Kubernetes Jobs continue.
The 30-second Pod termination grace is an example budget; verify actual drain
under load before treating it as sufficient.

Metadata and retry identities persist on the PVC. The store rejects a concurrent
writer, but requires a filesystem supporting its locking and mmap semantics.
Do not use independent copied PVCs for simultaneous servers. Records are not
pruned automatically. Kubernetes owns workload logs and completed Job objects;
operator cleanup loses those logs. Do not remove Jobs whose status must still
be reconciled.

## Upgrade and rollback

Before an upgrade, record the current image digest and retain the prior manifest.
Scale the server to zero, wait for its Pod to terminate, and take a consistent
volume backup using your storage system. Native workload Jobs remain running.
Never copy the live database as a substitute for a consistent backup.
With the server stopped and its volume mounted exclusively for maintenance,
`skygo state backup --data-dir DIR --output FILE` makes a database-only snapshot.
Use storage-level backups for other retained files. Restore with
`skygo state restore --input FILE --data-dir NEW_DIR`; it refuses an existing
directory. Preserve exactly one active copy of the installation identity.

Apply the new digest and run the manual verification plan. If it fails, scale
to zero again. Restore the pre-upgrade state backup before starting the prior
image when storage compatibility has not been proved. This may lose jobs and
request IDs accepted after the backup; reconcile their Kubernetes Jobs manually
before retrying submissions. Do not run both versions concurrently.
The schema fence refuses newer state in supported binaries; older unfenced
binaries offer no such protection. Always use their matching pre-upgrade snapshot.

A disposable test installation can be removed with:

```sh
kubectl --context YOUR_DISPOSABLE_CONTEXT delete namespace skygo
```

That deletes its Jobs, Secret, and PVC; storage-provider reclaim policy determines
whether underlying data is retained. It is not a routine rollback command.
