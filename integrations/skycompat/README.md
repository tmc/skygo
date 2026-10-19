# skycompat

This optional module parses a selected subset of upstream SkyPilot YAML and
implements selected wire codecs. The reference is SkyPilot commit
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

Python may provide a pinned development oracle; it is not a product dependency.
No root module replacement is needed by these standalone parsers/codecs. Future
canonical clients must resolve a reviewed root version independently.
