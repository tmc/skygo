# skygo-data helper image

This directory builds the image named by a Kubernetes target's
`data.helper_image`. The image holds one file, the static Linux `skygo-data`
helper (cmd/skygo-data), at `/skygo-data`, mode 0555, run as 65532:65532. The
data profile runs it as `/skygo-data stage` and `/skygo-data collect -- ...`
and copies it to `/opt/skygo/bin/skygo-data` for setup and task containers.
There is no shell, CA bundle, server, client, credential or source in the
image; the helper receives its origin CA through the profile environment.

## Interface

    GO=/usr/local/go/bin/go integrations/kubernetes/helper/build.sh ARCH OUT

- ARCH is `amd64` or `arm64`: the Linux architecture of the target's nodes.
- OUT is a new directory outside the source tree; its parent must exist.
  build.sh never overwrites or removes an existing path. If a step fails
  after OUT is created, OUT is left as is for inspection and has no
  receipt.txt (it is written last); remove it before retrying.
- GO selects the Go command (default `go`). Its version must equal the `go`
  line of the repository go.mod. Modules must already be in the module cache:
  the build runs with GOENV=off, GOPROXY=off, GOFLAGS=-mod=readonly and
  GOTOOLCHAIN=local, so GOMODCACHE and GOCACHE come from the environment.
  GOROOT is the Go command's own (recorded in the receipt), not the caller's.
  It targets the baseline instruction level (GOAMD64=v1, GOARM64=v8.0) with
  GOFIPS140=off and no GOEXPERIMENT. The build fails if the helper's build
  info shows a different CGO_ENABLED, GOOS, GOARCH, level or any GOEXPERIMENT.

build.sh writes:

- `OUT/context/skygo-data` and `OUT/context/Dockerfile`: the image build context.
- `OUT/receipt.txt`: a `skygo-helper-build-v1` record of the source, Go
  toolchain, host, flags and helper digest. Image fields are `UNRUN` until an
  operator runs the commands below; the receipt is build input, not a runtime
  attestation.
- `OUT/buildinfo.txt`: `go version -m` of the helper (module versions and sums).
- `OUT/commands.txt`: the exact image build and inspection commands.

## Build and publish

Build one image per architecture. A target's `helper_sha256` names one
executable, so a multi-architecture index cannot serve a single target.

    out=$HOME/tmp/skygo-helper/amd64-$(date +%Y%m%d%H%M%S)
    GO=/usr/local/go/bin/go integrations/kubernetes/helper/build.sh amd64 "$out"
    cat "$out/commands.txt"

`commands.txt` contains, for a lowercase repository REPO with an explicit
registry host and no tag (such as docker.io/NAME/skygo-data):

    docker build --platform linux/amd64 --build-arg HELPER_SHA256=<sha> -t REPO:<sha> OUT/context
    docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}}' REPO:<sha>
    docker push REPO:<sha>
    docker buildx imagetools inspect REPO:<sha>

The image inspect line reports `image_id` and `image_platform`, and imagetools
reports the manifest digest for `image_digest`.

The build uses only `FROM scratch`; it pulls no base image.

## Target configuration

Set the target's data block from actual results, never from examples:

    data:
      helper_image: REPO@sha256:<manifest digest reported by imagetools inspect>
      helper_sha256: <helper_sha256 from OUT/receipt.txt>

The manifest digest must be the one the registry reports for the pushed
single-platform image. The target refuses a helper_image whose first component
is not a registry host. A configured helper image is not native qualification:
image, ABI, nonroot, fsGroup, path and cluster qualification remain separate.

## Limits

Linux amd64 and arm64 only, CGO disabled. Build time is one `go build` of the
helper; the image is the size of the helper binary.

## Worker verbs

The worker profile uses `/skygo-data install` to verify
`SKYGO_DATA_HELPER_SHA256` and copy the helper into
`/opt/skygo/bin/skygo-data`. The task container runs that copy as
`worker -- argv...`; `worker-ready` checks its prepared marker.
These verbs do not contact the API server or use credentials.

`worker` requires one each of `SKYGO_JOB_ID`, `SKYGO_ATTEMPT_ID`,
`SKYGO_WORKER_MODE`, `RANK`, `WORLD_SIZE`, `SKYGO_POD_UID`, and
`SKYGO_TASK_SHA256`. It writes its Pod UID to
`/run/skygo/worker-state/prepared` and waits for
`/run/skygo/worker/start`. The start file must be empty or the exact
lowercase token from the current backend worker codec. Projected regular
files may be symlinks. These fixed paths are trusted profile mounts;
opening an arbitrary FIFO is not a bounded operation.

A prepared marker proves preparation only. Readiness, token presence and a
container exit do not certify that task argv executed. The caller must persist
its all-prepared decision before delivering tokens. Cancellation after the
helper's final check can race execution and requires native termination.
Pre-execution refusals are silent on stdout and stderr; diagnostics go to the
termination file. Original file Close failures are retained without retry.
The command currently observes interrupt cancellation, not a dedicated SIGTERM
handler; signal and actual kernel execution qualification remain separate.

Historical helper digests built before these verbs were added cannot serve
worker profiles. Rebuild from the intended source and retain its build receipt.
A source or modeled-dispatch test does not qualify an image, Pod, or task exec.

Immediately before task dispatch, the helper sets `SKYPILOT_NODE_RANK` from
its validated original rank, replacing user overrides. Other variables are
preserved, including an intentional `SKYPILOT_NUM_NODES` override. The worker
profile is CPU-only; these compatibility variables do not imply GPU support.
