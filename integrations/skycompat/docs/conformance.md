# Compatibility conformance corpus

This module's selected parser/codec profile targets upstream SkyPilot source
`922a9f046c4a2714edf2b76fec3d1862dcd56ae3`, API 67. The profile implements a
bounded subset; API67 is not a claim of complete upstream behavior.

`testdata/yaml/manifest.json` and `testdata/wire/manifest.json` identify every
input by SHA-256 and record source path, relevant lines, whole-file SHA-256,
license, expected selected-profile result and qualification layer. Fixtures are
small original source-derived examples unless marked captured oracle output.
The exact upstream root license is retained in [testdata/LICENSE](../testdata/LICENSE), including its existing attribution appendix. The pinned upstream root has no NOTICE file. This accompanies source-derived and captured fixture data; the module license for new Go code remains separate. No upstream implementation code or secrets are embedded in fixtures. `SOURCE_ONLY` means a source-grounded expectation, not an
executed upstream result.

Parser tests consume the YAML manifest. `entrypoint=task` means one task document;
`entrypoint=dag` means the separately bounded SDK name-header + one-task envelope.
The upstream dumper emits that envelope even for one task. Genuine multi-task
workflows are not qualified by accepting a single-task SDK envelope.

An `expect=accept` row asserts syntax and preserved typed data only. Its
`runtime_status=DEFERRED` is deliberate: parsing shell text, resource constraints,
workdir or mount references neither executes them nor promises a concrete backend
can satisfy them. `values` supplies meaningful semantic checks, such as exact
multiline text, Python boolean strings, minimum/resource units and worker count.
Error paths contain only fixed recognized schema paths or their nearest safe parent; source positions identify unknown/dynamic keys without echoing attacker-controlled text. A rejected row records a selected-profile refusal; it does not imply upstream
also rejects that valid feature. Unknown/service/config/alternative fields, unsafe YAML and unsupported resource effects must never disappear into an ignored metadata bag. Known cloud/region/zone/instance_type/image_id references are typed and preserved as DEFERRED, without provider-name validation or runtime support; canonical lowering must refuse unsupported effects before work.

The initial profile preserves Python-style canonical scalar coercion where
supported: boolean `true` becomes environment string `True`, while quoted
`'yes'` remains `yes`. Plain YAML1.1 `yes` is a different case. Null environment
values without explicit caller overrides fail; numeric formats whose exact
coercion is not supported fail clearly. CPU plus/minimum suffix, upstream binary
GB semantics and accelerator model constraints remain explicit until lowering.
A generic GPU count cannot silently satisfy an A100 model request.

Wire fixtures distinguish synchronous `/validate` HTTP400 detail from async
`/api/get` HTTP500 detail carrying a request envelope. `return_value` and `error`
are JSON strings inside outer JSON. Upstream's harmless None pickle literal for
entrypoint/request_body is tested as a restricted bookkeeping representation;
it does not qualify payload introspection, Python callable reconstruction or
request reexecution. The error fixture's pickle contains only primitive data.
No product Go code evaluates incoming arbitrary pickle. Outbound primitive bytes
may differ from the fixture while decoding to the same strictly allowed value.

C02 owns the actual unmodified pinned SDK oracle environment and codec tests.
The request-success, request-error and validation-error fixtures now have an actual unmodified pinned SDK decode receipt: success yields tuple (1, None), and both error forms yield ValueError with the fixture message. Their manifest rows are SDK_DECODE_PASS. Exact Task/DAG outputs, including empty resource/mount/volume maps and num_nodes1, are SDK_DUMP_CAPTURED. The remote ValidateBody row is SDK_PAYLOAD_CAPTURED with only listed nonsemantic identity/diagnostic normalizations, preserving actual DAG/default fields and explicit disable-usage context; its constructor made no HTTP request. Other rows remain SOURCE_ONLY. These are codec/capture qualifications, not HTTP facade or domain execution evidence. An absent handle and illustrative job ID have no
meaning as a successful launched cluster or managed job. Async request identity,
numeric domain ID and canonical execution identity remain distinct and scoped.

Runtime, SDK, CLI, local process and native Kubernetes qualification are separate
ledger columns. Future actual-oracle captures must retain exact command, source
pin, test environment and normalization rules. Normalization may remove unstable
IDs/timestamps/paths, never errors, execution state, output loss, auth behavior or
request lifecycle differences. Python remains development oracle tooling; the
shipping product must build and execute with CGO disabled and without Python.

## Selected-profile matrix

| Area | Parser/codec expectation | Runtime/wire gate |
| --- | --- | --- |
| name, run/setup text, env canonical scalars | Preserve exact selected values; explicit env overrides resolve null | Shell lowering/auth pending; parsing never executes |
| CPU/memory/model constraints | Preserve minimum/unit/model forms; invalid/unsupported forms refuse | S04/S05 matching capabilities required |
| workdir, string mounts/volumes | Typed references, no path reads | S06/S07 staging/approval required |
| cloud/region/zone/instance/image references | Typed accepted syntax, DEFERRED | Provider semantic validation and executor unavailable until matching slice |
| single-task SDK DAG envelope | Header + one task, including actual empty/default fields | Not multi-task workflow support |
| multi-task/service/config/alternatives | Explicit selected unsupported, no dropped effects | S08/S09/S14 and typed contract extension |
| duplicate/unsafe tags/anchors/deep documents | Invalid/limit refusal with safe source location | No execution |
| request/error envelopes | Three actual pinned-SDK decoder checks pass | No HTTP handler/request-lifecycle or real launch proof |
| real SDK HTTP + canonical execution | Not qualified by corpus | C03/C04/C05 and underlying accepted domains |


## E0 HTTP health and validation qualification

The facade has a separate real HTTP qualification, not inferred from the corpus
above. Against a real canonical scoped server with accepted credential format,
the unmodified pinned SDK performed health/version discovery, public
`sky.client.sdk.validate` for its ordinary one-task DAG, and a resource-negative
validation that reconstructed ValueError. The honest implementation version
`0.0.0+skycompat.api67` was accepted. Config-only credentials were absent from
transmitted bodies. The separate environment-token negative case was refused;
no body-token allowlist or SDK monkeypatch was used.

The current profile serves GET `/api/health` and POST `/validate` only. Health
returns public version fields and user null; successful validation returns JSON
null. Validation forwards the original bearer credential on a fresh canonical
GetServerInfo and accepts only the narrow inert task subset documented in the
README. It does not plan or submit work. Known nonnull
`override_skypilot_config_path` is accepted only as null or bounded4096-byte text,
discarded without inner JSON interpretation, path access or config merging.
Nondefault config content, identity authority, uploads and environment secrets
remain refused.

Actual-process HTTP tests covered two principals including a reader, missing/bad
credentials, revocation, expiry, auth-unavailable recovery, duplicate headers,
negative/default fields and zero jobs created. Real TCP tests bounded incomplete
bodies and disconnects, including unknown-route rejection. A stalled-response
proxy covered the two-second canonical deadline and admission cap. A separate
controlled response writer verified permits remain held during frontend flush
and release on completion/deadline; that test is not TCP backpressure evidence.

Receipts for this E0 candidate are retained in the coordinator's task evidence,
`20261018-gap-closure-execution/skycompat/E0/facade-work`, including source hashes,
normal/race/vet results and redacted SDK HTTP traces. Child canonical processes
were CGO-disabled production builds, not race-instrumented merely because their
Go parent test ran with -race. Tests requiring the external canonical binary or
pinned Python oracle skip when those explicit test inputs are absent; a default
standalone unit run alone does not repeat the HTTP/SDK qualification.

| E0 surface | Qualification | Remaining limit |
| --- | --- | --- |
| Health/version | Real scoped canonical process and unmodified pinned SDK | Selected implementation/profile identity only |
| One-task validation | Real SDK positive and expected ValueError negatives | Narrow inert subset; no lowering/execution |
| Config credentials/provenance | Real header forwarding; body-secret negative refused | No environment-token mode or configuration override |
| Bounded body/admission | Real TCP and separately labeled controlled flush tests | Embedding server owns header/connection limits |
| Async requests, jobs, environments, services | Not implemented by this facade | Corresponding canonical domain and E1 identity gates remain open |

These E0 receipts do not promote the earlier illustrative async codec fixtures
to real request-lifecycle support, nor qualify Kubernetes/SSH/cloud execution,
full API67, or a published standalone module.
