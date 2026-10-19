# skycompat

This optional module parses a selected subset of upstream SkyPilot YAML and
implements selected wire codecs and an authenticated health/validation HTTP facade.
The reference is SkyPilot commit
`922a9f046c4a2714edf2b76fec3d1862dcd56ae3`, API 67.

`taskyaml.Parse` reads a single task. `taskyaml.ParseDAG` reads the SDK's name
header followed by one task. Both are pure syntax parsers: scripts, paths,
provider names and resource constraints are preserved, not executed, resolved,
validated against providers or submitted. Unknown semantics are refused.
No shell permission or backend capability follows from successful parsing.

The parser accepts selected name, run/setup, environment, workdir, resource,
node count and string mount reference forms. Rich service, workflow, storage,
recovery and provider configuration remain unsupported. Environment nulls need
explicit overrides. Memory numbers mean upstream GiB; source units and `+`
constraints are preserved. The legacy `disk_size: 256` value is preserved as an
upstream unspecified default, never a disk allocation guarantee.

Input is bounded to 1 MiB, 16 KiB per scalar, depth 32 and 8192 nodes. Aliases,
anchors, merge keys, custom tags, duplicate keys and unexpected documents fail.
Errors contain known field paths and positions, excluding user values.

Run independently:

```
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off go vet ./...
```

`facade.NewHandler(facade.Options{Endpoint: canonicalURL})` serves only
`GET /api/health` and `POST /validate`. It uses the generated canonical Connect
client and forwards each request's original bearer credential. No task, file,
artifact, cluster or asynchronous request is created. API67 identifies the
selected wire profile, not full SkyPilot API compatibility.

The HTTP validation profile accepts the SDK's header plus one task: name, run,
resolved environment values and benign empty/default fields. Nondefault setup,
workdir, mounts, resources/providers and additional workers are refused. The
broader parser syntax above does not imply HTTP or runtime support. Shell text
is inert. Health reports the implementation version `0.0.0+skycompat.api67`.

For the pinned SDK, configure a canonical-issued `sky_` credential using its
private `api_server.service_account_token` configuration. The canonical server
also accepts previously issued `skygo_` credentials, but the pinned SDK requires
`sky_`. Environment-token mode is unsupported: upstream copies that token into
the request body, which this profile refuses. Known config-path provenance is
bounded text that is discarded; it is never opened or used as configuration.

The embedding HTTP server must set header and connection limits. The handler
bounds supported operations to 32, canonical calls to two seconds, and body/write
processing to five seconds using response-controller deadlines. See
[facade package documentation](facade/doc.go) and
[qualification boundaries](docs/conformance.md).

This development checkout resolves its generated canonical client through the
root module replacement `../..`. Publishing this module requires a separately
reviewed root version and dependency resolution; the local replacement is not
release qualification. Python is development oracle tooling, not a product
dependency. Real scoped HTTP and unmodified pinned SDK health/validation tests
passed separately from the parser/codec corpus; workload and native backend
compatibility remain outside this HTTP profile.
