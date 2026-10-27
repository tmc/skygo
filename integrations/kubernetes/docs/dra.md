# Dynamic resource allocation

DRA support is planned, not implemented. The Kubernetes target currently maps
accelerator names to extended resources, such as `nvidia.com/gpu`. This design
adds a second, explicit allocation mode inside the Kubernetes integration module.
It does not add Kubernetes dependencies to the core or change local and SSH
execution.

## Device requests and target policy

Tasks continue to request a portable accelerator name and positive integer count.
The operator maps each name to exactly one allocation mode:

- An extended-resource name and the existing node-selector policy.
- A DRA `DeviceClass`, bounded CEL selectors, and an exact device count supplied
  by the task.

The two modes are mutually exclusive. A missing DRA API, class, driver, or
permission never selects the extended-resource mode instead. A target can expose
separate accelerator names when the operator deliberately offers both modes.

DRA policy, canonical selectors, allocation mode, and count rules belong in the
target binding. Changing them changes the binding. Targets without DRA keep
their existing binding bytes and behavior. User tasks cannot supply Kubernetes
objects, arbitrary CEL, driver configuration, or administrative device access.

`Check` remains a local policy check. Construction of a DRA-enabled target adds
bounded API discovery and class validation under the caller's context; the
legacy constructor path remains unchanged. Planning reports compatibility, not
a reservation or a promise that devices are currently free. Kubernetes and its
DRA driver perform allocation and scheduling.

## Claims and ownership

The first implementation uses namespaced `ResourceClaimTemplate` objects and
independent generated claims for each Pod. The Pod references its template and
the task container references the corresponding claim. This follows the
[Kubernetes workload model](https://kubernetes.io/docs/tasks/configure-pod-container/assign-resources/allocate-devices-dra/).

Templates are immutable execution objects. Their names derive from the target
binding, accepted task, and attempt identity; their metadata records those
values. A template is owned by its exact Job UID, or by the exact directly
managed Pod UID for retry execution. A missing template must not permit the
workload to run. Create the parent referencing the expected template, obtain
its UID, then create the template with that owner reference. Validate this
ordering against a real API server before enabling it.

Kubernetes generates a separate claim for each Pod and ties that claim's
lifetime to the Pod. See the upstream
[claim lifecycle](https://kubernetes.io/docs/concepts/resource-management/dynamic-resource-allocation/dra-api/).
A completed Job may retain its Pod for logs, so job completion alone is not a
Skygo guarantee that a device has been released. DRA cleanup must provide a
separate, observed release result.

Native receipts must survive both a server restart and deletion of the native
parent. Pin the parent UID through the existing opaque execution identity.
Keep bounded template and generated-claim names, UIDs, and specification digests
until cleanup completes; parent annotations alone cannot preserve this receipt
after parent deletion. The implementation must settle that durable receipt
representation before enabling DRA. Recovery validates the parent UID,
namespace, task digest, binding, owner references, and expected specifications.
A name or label match alone does not authorize adoption or deletion. The
ordinary Job path needs this stronger UID pin; the existing retry path already
uses a pinned Pod instance.

A lost create response remains uncertain until the permitted identity checks
resolve it. It does not authorize a second allocation. Retry attempts receive
new templates and generated claims; they never reuse the preceding Pod's
claim. Cancellation deletes only the pinned workload. Garbage collection
handles owned dependents; any direct deletion additionally requires the
recorded UID. Allocation uncertainty, finalizers, and cleanup timeouts remain
visible and prevent a successful resource-release result. Skygo never removes
Kubernetes or driver finalizers to force progress.

Caller-supplied or shared claims, fractional capacity, prioritized device
alternatives, and administrative access are outside this first phase. They
need separate authorization and lifetime rules.

## Module and cluster requirements

The integration currently pins Kubernetes Go modules at `v0.32.3` and declares
Go `1.24.1`; it cannot use the stable typed `resource.k8s.io/v1` client as-is.
Upgrade `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go` together
inside this module. Do not hand-copy Kubernetes API types into Skygo.

Select and test an explicit supported client/cluster combination before
implementation. The upstream `v0.34.0` API module declares
[Go 1.24](https://github.com/kubernetes/api/blob/v0.34.0/go.mod), while
`v0.35.0` declares
[Go 1.25](https://github.com/kubernetes/api/blob/v0.35.0/go.mod). A newer optional
module toolchain requirement does not require raising the core's requirement.
API discovery must confirm `resource.k8s.io/v1`; an installed driver and
operator-provisioned `DeviceClass` are separate requirements.

Use namespace-scoped permissions for the managed templates and claim
observations, plus read access to the configured cluster-scoped classes.
Skygo does not install drivers, modify `DeviceClass` or `ResourceSlice`
objects, update allocation status, or request administrative device access.
Unneeded list, mutation, and cluster-wide privileges stay absent.

## Implementation order and acceptance

1. Extend target configuration, validation, cloning, and canonical binding for
   the mutually exclusive modes. Preserve legacy bindings byte for byte.
2. Extend the shared Pod renderer and add template construction. Wire both
   ordinary Jobs and directly managed retry Pods; retain CPU and memory
   requests independently. Add durable UID receipts and recovery before
   accepting DRA tasks.
3. Implement observed allocation diagnostics, cancellation, retention, and
   cleanup. Report unsatisfied claims as scheduling state; do not infer
   running or released devices from successful object creation.
4. Apply the same rules to each worker rank when the Kubernetes worker
   backend is integrated. Every rank gets an independent claim. Gate worker
   release on the existing group protocol, and join every rank's claim
   cleanup before reporting group removal. Do not advertise DRA for workers
   based on ordinary-job tests.
5. Qualify the complete lifecycle on an authorized cluster with a real DRA
   driver. Until then, unit and fake-client tests establish rendering and
   controller behavior only.

Required tests cover strict policy rejection, unchanged legacy bindings, exact
Pod and template fields, count and selector changes, ownership conflicts, lost
responses, server restart, retry separation, cancellation before allocation,
delayed driver cleanup, and replacement UIDs. Real-cluster acceptance must
show the requested device inside the container and prove eventual claim
cleanup after cancellation and retry. A missing driver or unavailable test
cluster is an explicit validation hold, not a passing allocation test.
