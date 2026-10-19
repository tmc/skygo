# Scoped authentication

The default mode is single operator: a configured bearer token, or anonymous
loopback access, grants global control. Scoped mode assigns work to principals.
Readers inspect their own work; operators also submit and cancel their own work;
admins inspect and cancel all work. An admin submits as its own principal.
Target facts and planning are shared. This is control-plane access control;
execution-authorized workloads remain trusted within their execution environment.

## Activate

Stop the server and take a consistent database snapshot. Use private paths for
the auth file and token files; the commands refuse existing outputs.

```sh
skygo auth init --data-dir state --auth-file auth.json --token-output admin.token
skygo auth principal add alice --role operator --auth-file auth.json
skygo auth token issue alice --auth-file auth.json --output alice.token --expires-in 24h
skygo server --data-dir state --auth-file auth.json
```

Existing work belongs to reserved principal `legacy`; the initial token is its
admin credential. Activation preserves installation identity and accepted tasks.
A scoped store requires its matching auth file and cannot fall back to legacy
token or anonymous mode. Stop supplying `SKYGO_TOKEN` or `--token` when starting
this server. The Kubernetes entrypoint accepts the same `--auth-file` flag; its
file must be provisioned privately in the server's environment.

Clients select one credential source:

```sh
skygo --token-file alice.token run -f examples/local.yaml --request-id example
skygo --token-file alice.token jobs list
```

`--token-file`, literal `--token`, and nonempty `SKYGO_TOKEN` conflict rather than
silently overriding one another. Token files must be private regular files
containing one token, optionally followed by a newline. Literal `--token` is
deprecated. Help does not display the environment token. API commands use
protobuf JSON; local auth commands return public metadata with `--json`.

Request IDs belong to principals. Different principals may reuse an ID without
sharing a job; tokens for the same principal share retry identity. Foreign and
missing job IDs are indistinguishable. Pagination tokens are bound to the
current principal and read scope; restart pagination after changing scope.

## Rotate and revoke

```sh
skygo auth token issue alice --auth-file auth.json --output replacement.token --expires-in 24h
skygo auth token list --auth-file auth.json
skygo auth token revoke OLD_TOKEN_ID --auth-file auth.json
skygo auth principal set-role alice --role reader --auth-file auth.json
```

Verify the replacement credential before revoking the old public token ID.
The tools prevent removing the last usable admin credential. Tokens are random
bearer credentials, expire after 30 days by default, and may last at most 90 days.
Raw credentials are written once to private files; the auth file stores hashes.
Keep credentials and auth files out of workload artifacts and logs.

The server polls one local auth file and revalidates active streams. Updates
affect new requests and protected streams within the tested local filesystem
and transport envelope; they do not recall bytes already sent or undo authorized
operations. Revocation and role changes stop protected follows, including an
admin's foreign-job follow after demotion. They do not cancel accepted jobs.
Invalid or missing configuration denies authentication and withdraws readiness
while durable reconciliation continues.

Backend cancellation is cooperative. A backend call that ignores context retains
its stream slot until it returns; replacing disconnected clients cannot create
unbounded abandoned calls. The stream slot also covers the HTTP handler's final
response writes.

Access diagnostics use a nonblocking queue of at most 256 metadata records.
Reconciliation attempts to persist at most four per pass, retaining the latest
4096 in a ring. Pending records can be lost on overflow, shutdown, crash or write
failure; successful enqueue does not acknowledge persistence. Stored timestamps
describe persistence attempts, not the original access time. The readiness
header `Skygo-Audit: unavailable` reports diagnostic faults without changing
authorization or liveness. An overflow warning remains until server restart.

## Recovery

Database snapshots exclude the auth file and raw token files. Preserve the latest
matching auth authority separately; restoring an older credential file can revive
revoked access. Never run two restored copies of one installation identity.
The pre-activation binary requires its matching pre-activation snapshot for
rollback. A fenced older binary refuses a newer protected schema.

If activation fails before its database transaction commits, it removes only
files it created. A crash can leave private orphan outputs while the database
is still unactivated; inspect and archive only those known outputs before retry.
After activation, restore the matching authority rather than disabling auth.
Local file mutations use a sibling `.lock` directory. After a crashed writer,
remove that exact lock only after all credential writers are stopped.

Filesystem administrators control the auth file. This does not provide SSO,
workload sandboxing, namespace isolation, or protection against a machine admin.
Protect bearer traffic with the deployment's trusted TLS/network boundary.
