# skygo SSH integration

`skygo-ssh` runs skygo commands against a Linux arm64 host, such as a DGX
Spark, that you reach with SSH. It builds the skygo agent from your local
checkout, copies it to the host, starts it there, and runs the canonical
skygo command tree against it through SSH. You do not copy any binary.

```
skygo-ssh --host dgx --known-hosts ~/.config/skygo-ssh/dgx.known_hosts \
    run -f task.yaml --wait
```

Everything after skygo-ssh's own flags is passed unchanged to the skygo
command tree, so `run`, `plan`, `jobs list`, `jobs get`, `jobs logs`,
`jobs cancel`, `targets list`, `info` and `workflow` work as they do
against a local server, and so will any command later added to that tree.
skygo-ssh's flags must come first; skygo flags such as `--json` go after
the subcommand.

## Install

From the checkout, with Go 1.24 or later:

```
cd integrations/ssh
go install ./cmd/skygo-ssh
```

Run `skygo-ssh` from inside a skygo checkout, or pass `--source DIR`. Each
command builds `integrations/ssh/cmd/skygo-ssh` from that checkout with
`GOOS=linux GOARCH=arm64 CGO_ENABLED=0 GOWORK=off GOTOOLCHAIN=local`, so
in source mode the agent is your current source. Without `--source` the checkout
must have no uncommitted changes; with it, uncommitted changes are built
too. Builds are cached under the user cache directory (`~/Library/Caches`
on macOS) in `skygo-ssh/build/COMMIT/SHA256/`.

## Use a prebuilt agent

The installed client also works outside a checkout, without Go or Git on
its PATH. Build the Linux arm64 agent separately from the intended source:

```
make agent-linux-arm64
shasum -a 256 bin/skygo-ssh-linux-arm64
```

Record the actual clean source commit and the binary's SHA-256, then run:

```
skygo-ssh --host dgx --known-hosts KNOWN_HOSTS \
    --agent-binary BINARY --agent-commit COMMIT --agent-digest SHA256 targets list
```

All three binary flags are required together and are exclusive with
`--source`. Relative binary paths are accepted by the CLI; Go callers use
an absolute path in `Config.Prebuilt`.

The binary must be a regular, read-only executable, at most 256 MiB,
containing a static Linux arm64 Go build of this module's `skygo-ssh`
command with `CGO_ENABLED=0`. PIE and dynamic executables are refused.
The client copies and verifies the bytes, then atomically publishes a
read-only snapshot under `skygo-ssh/build/prebuilt/COMMIT/SHA256/`.
An existing snapshot must match; it is never replaced. The cache must be
private and trusted. Another process of the same user can change its
permissions: mode 0555 is not hostile-process isolation.

The commit is your assertion of the clean source used to build the agent.
The digest verifies the selected bytes, and the ELF and Go metadata check
the format and target; none proves source provenance. Missing embedded
VCS metadata is accepted, but contradictory metadata is refused. This
mode preserves the existing remote identity, record and bootstrap checks.

## Enroll the host

skygo-ssh resolves `--host` with `ssh -G`, which applies your OpenSSH
configuration without connecting, and uses its `HostName`, `Port`, `User`,
`IdentityFile` and `IdentityAgent`. Configurations with `ProxyCommand`,
`ProxyJump`, `ForwardAgent`, port forwarding, `RemoteCommand` or
`PermitLocalCommand yes` are refused. The resolved host name must be a
lowercase name or an IP address.

The host key must be enrolled in a known_hosts file used only for this
host, listing the host by literal name. Hashed, wildcard, negated and
`@cert-authority` entries are refused, so your ordinary
`~/.ssh/known_hosts` usually cannot be used directly. To copy a key you
already trust from it, with HOST the resolved host name (`[HOST]:PORT` if
the port is not 22):

```
mkdir -p ~/.config/skygo-ssh && chmod 700 ~/.config/skygo-ssh
ssh -G dgx | grep -E '^(hostname|port) '
ssh-keygen -F HOST | grep -v '^#' | awk -v h=HOST '{print h, $2, $3}' \
    > ~/.config/skygo-ssh/dgx.known_hosts
ssh-keygen -lf ~/.config/skygo-ssh/dgx.known_hosts
```

Check the printed fingerprints against the host before use. Unknown,
mismatched and `@revoked` keys are refused.

The client key comes from your SSH agent (`SSH_AUTH_SOCK`, or the
configured `IdentityAgent`), so encrypted keys work once added with
`ssh-add`. Exactly one agent key is used: the one whose SHA256 fingerprint
is `--identity-fingerprint`, or by default the one public key found as
`IdentityFile.pub` for the host. `ssh-add -l -E sha256` lists the
fingerprints. No other key is offered, and the agent is never forwarded.
Alternatively, `--key-file` names an unencrypted private key, mode 0600.

## What happens on the host

skygo-ssh runs one fixed shell script over SSH, with every operand
validated and quoted. It needs a POSIX login shell, `sh`, `uname`,
`sha256sum` or `shasum`, `head`, `tail` and `nohup`, and uses `setsid` and
`/proc` when present. It writes only below `~/skygo-test`:

```
~/skygo-test/COMMIT/SHA256/      mode 0700, one directory per agent build
    skygo-ssh                    the agent binary, SHA-256 is SHA256
    cert.pem key.pem token       its TLS identity and bearer token, mode 0400
    data/                        its job state
    agent.config                 its port and target, mode 0400
    agent.pid agent.log          its process record and output
```

Files are sent on standard input, never on a command line, and stored only
if absent. An existing file must match the local one; a mismatch or a
symlink is refused, never replaced. The binary's checksum is checked
again before every start.

The agent, `skygo-ssh agent`, is the canonical skygo server
(`server.Run`) with:

- its built-in local target named after the host (`--target-name`,
  default the alias), so tasks run as your user on the host itself;
- HTTPS on `127.0.0.1:47080` only (`--agent-port`), with a self-signed
  certificate for `agent.skygo.invalid` made for that agent directory;
- a legacy bearer token and an unscoped durable store in `data/`.

It runs detached with `setsid nohup` and outlives the SSH session, and its
jobs persist. Each command reuses a running agent recorded in `agent.pid`.
If the recorded process has exited, skygo-ssh refuses to start another
unless you pass `--restart-stopped`. If the record cannot be verified, for
example an empty `agent.pid` left by an interrupted start, it is never
replayed: inspect `agent.log`, stop any agent process, and remove
`agent.pid`.

The first start records `--agent-port` and `--target-name` in
`agent.config`, exactly `port PORT` and `target NAME` on two lines, and
the record is never rewritten. A directory's configuration cannot change
in place: a later command with another port or target name is refused
before anything is started, stopped or renamed, even with
`--restart-stopped`. Use the original values; a new build has its own directory. Do not
remove the record to change settings while keeping the old job store.
A record that is not a regular file holding exactly that text, such as
a symlink or a truncated or extended file, is
refused as unverifiable. The remote script reads the record with one
exact parser, at most 1024 bytes, for both its status report and every
start; a read that fails is unsafe, whatever it delivered. The status report says only whether the record is absent, unsafe,
or a legacy or installation record that is or is not the requested one,
never its contents. skygo-ssh refuses a record of another configuration
or form before copying any file. A running agent without a record, started by an
earlier skygo-ssh, is refused because its configuration cannot be
verified; once it has stopped, `--restart-stopped` starts it again and
records the configuration.

After starting, skygo-ssh reports the agent ready only when an authorized
`GetServerInfo`, sent through SSH to the fixed loopback address and
verified against that directory's certificate, names the expected target.
On failure it prints the end of `agent.log`.

The certificate, key and token are kept locally under the user config
directory (`~/Library/Application Support` on macOS) in
`skygo-ssh/agents/ALIAS/COMMIT/SHA256/`, mode 0600, and are reused. The
token is passed to skygo as `--token-file` and is never printed.

## Serving an initialized installation

By default the agent keeps its own unscoped store and legacy token. To
use scoped credentials, artifacts and workspace jobs instead, initialize
an installation on the host yourself, with the canonical `skygo` binary
and the server stopped:

```
skygo auth init --data-dir /srv/skygo/data --auth-file /srv/skygo/auth.json \
    --token-output ~/admin.token
skygo state artifacts init --data-dir /srv/skygo/data \
    --artifact-dir /srv/skygo/artifacts --ack-writers-stopped
```

Then copy a token issued by that auth file to a private local file and
name all four paths:

```
skygo-ssh --host dgx --known-hosts ~/.config/skygo-ssh/dgx.known_hosts \
    --remote-data-dir /srv/skygo/data --remote-auth-file /srv/skygo/auth.json \
    --remote-artifact-dir /srv/skygo/artifacts --scoped-token-file ~/.config/skygo-ssh/dgx.token \
    artifacts upload ./src
```

The agent is then `server.Run` with that state directory, `AuthFile` and
`ArtifactDir`, `ExistingInstallation` set, and no legacy token;
`artifacts upload`, `artifacts download` and tasks with `workdir`, `setup`
and `outputs` work as against a local server. Neither skygo-ssh nor the
server creates, initializes or migrates the installation. Before starting
the agent skygo-ssh refuses an incomplete set of flags, paths that are not
clean and absolute (letters, digits, `.`, `_`, `-` and `/`, at most 200
bytes), a state directory without `jobs.db`, an absent auth file or
artifact directory, any of them a symlink, and an agent directory holding
a legacy token. The agent repeats these checks. The server then decides
the rest before it listens: it reads the store without writing it and
refuses one that is not an existing, current, scoped installation (an
empty `jobs.db` included), an auth file of another installation, and an
artifact root whose marker does not match the store; only then does it
take the database's writer lock, and it refuses if the store changed in
between. Other writers must be stopped: edits that bypass the database's
lock are not detected.

The agent directory then holds no `token`, and `agent.config` adds
`data DIR`, `auth FILE` and `artifacts DIR` lines. A directory started for
one installation, or for none, refuses a command naming another before
anything is copied or started: a command without installation flags
refuses an installation's directory, and a command with them refuses a
legacy record even if the directory holds no token. A two-line record is
always legacy; a missing token never makes it an installation's. In
either mode, a directory whose `agent.config` is truncated, extended, of
unknown form or not a regular file is refused before anything is copied
into it. Nothing adopts, copies or rewrites a mismatched record. The agent's
paths come only from that record and its
command line; credentials stay in files. Readiness requires the scoped
token to be accepted and the artifact service and the target's workdir
snapshots to be advertised. The server does not report its installation's
identity, so the exact paths are bound by the record, not checked over RPC.

Interrupting skygo-ssh, or losing the SSH connection, does not cancel a
job; `jobs cancel ID --yes` does. Workspace jobs are not covered by the
durable supervisor: a job running when the agent stops is not reattached.
Uploaded snapshots and results follow the installation's own artifact
lifecycle; skygo-ssh adds no retention or cleanup of its own.

## Tasks

A task without `target` runs on the host. `target: dgx` (the target name)
also works; `target: local` does not, since the host's target is renamed.
skygo-ssh does not rewrite a task's target. It supplies the canonical
CLI with the exact infrastructure binding `ssh/ALIAS` to the target it
verified through the authenticated connection. Plain HTTP skygo commands
have no such binding and cannot infer an SSH pool from a target name.

For example, from the source checkout, the original DGX primer needs no
manual binary transfer or YAML edits:

```
skygo-ssh --host dgx --known-hosts ~/.config/skygo-ssh/dgx.known_hosts \
    --source . run -f skycompat/taskyaml/testdata/primer/09-dgx-gpu.yaml --wait
```

The agent uses the canonical built-in execution backend. Resource requests
are checked there; the transport does not discard them or substitute an
untargeted task. Host GPU names come from a bounded inventory query, and
per-request admission remains authoritative. Tasks run as the SSH user;
this command does not install k3s, use sudo, or provision a machine.

## Limits

- Every RPC opens its own SSH connection and is limited to 10 seconds and
  1 MiB each way. `run --wait` polls and is unaffected, but
  `jobs logs --follow` stops after 10 seconds; jobs keep running, and
  `jobs logs ID` reads the retained log again.
- A new build gets a new agent directory and a new agent on the same port.
  While the previous agent runs, the new one cannot listen and skygo-ssh
  reports it. skygo-ssh cannot stop the previous agent: `agent.pid` holds
  only a process ID, which does not identify the process, so do not
  signal by it. Stopping is HOLD until the process's original custodian
  can stop it (see "Native installation check (HOLD)").
- There is no stop command and no dashboard. Without an initialized
  installation there is no scoped authentication, retention domain,
  artifacts or workspaces. `server`, `auth` and `state` work on local files
  and are refused; run them on the host.
- Artifact transfers use the canonical 64 KiB chunks, each one RPC within
  the limits above. There are no persistent environments, sessions or
  interactive terminals.
- Removing the local identity while the remote copy exists makes the next
  command refuse the mismatch; remove the remote agent directory too.
- The agent is built for linux/arm64 and installs only where `uname -m`
  reports `aarch64`.

## Packages

- `sshagent` opens the agent from Go as skygo-ssh does, from an explicit
  `Config` (SSH address and user, known_hosts file, key file or pinned SSH
  agent key, checkout, local identity and build directories, target, port
  and optional installation) instead of an `ssh -G` alias. A `Session`
  offers only `Control`, an authenticated control client, `Binding`, the
  facts Open verified, and `Close`.
- `internal/agentsession` is the one implementation of opening a session,
  used by both skygo-ssh and `sshagent`: it builds, installs, starts and
  waits for the agent.
- `internal/agentidentity` creates and reads the agent's private TLS
  identity and token files, for both the agent and its clients.
- `internal/agentssh` sends each HTTP request over its own SSH connection
  and `direct-tcpip` channel to the agent's loopback port, verifying the
  SSH host key against the dedicated known_hosts file and the agent with
  TLS. It never retries, replays, pools or follows redirects, and refuses
  compression and protocol upgrades. `Dial` returns an SSH client with the
  same checks for the bootstrap's commands.
- `internal/sshconfig` resolves the host alias with `ssh -G`.
- `internal/agentbuild` locates the checkout and builds the agent.
- `internal/bootstrap` holds the remote script and runs it.

Opening a session acquires two local connections: the connection to the
SSH agent, held until `Session.Close`, and the SSH connection that runs the
bootstrap commands, closed and joined before Open returns. Each is closed
once; a later or concurrent Close returns the first result and never
closes it again. Each readiness request must also show that its
connections were closed: the first Close of its response body, which
Connect may discard, is recorded and must return nil, and Open sends no
further request or command after one that did not. If any of these did
not settle, Open returns an `OpenError` that references the originals for
as long as the caller holds the error, and no session; its text names only
the parts that did not settle, and `errors.Is` and `errors.As` reach the
original errors. Requests made later through `Control` own their
connections; `Session.Close` does not wait for them. Nothing stops the
remote agent or its jobs.

## Tests

```
(cd integrations/ssh && GOWORK=off go test ./...)
GOWORK=off go test ./internal/server ./internal/cli
```

The bootstrap tests run the real script through an in-process SSH server
with a temporary `HOME`; the test that checks process reuse needs `/proc`.
`TestStartCommand` checks the operands Start sends through a MODEL
Runner that runs nothing; it does not show the record the script writes
or the argv it starts the agent with. The record tests start nothing;
they check that Status and refused installs and starts leave the agent
directory, the installation and a bystander file identical, inode and
modification time included. `TestRecordReadFailure` replaces `head`
with a MODEL that delivers a whole valid record and then fails, and
checks that the script's parser reports it unsafe.
`cmd/skygo-ssh` runs the agent in process with `server.Run` and drives it
with the canonical CLI; on Linux, `TestAgentInstallationCLI` and the
script `cmd/skygo-ssh/testdata/installation.txt` do so for a real
installation initialized with `skygo auth init` and `skygo state
artifacts init`. The CLI test's refusals, including a MODELED zero-byte
`jobs.db`, must leave both installations' state, auth files, artifact
roots and a bystander identical, device, inode, mode and bytes included.
Their roots live in one private directory under
`~/tmp/skypilot/20261028-s11-ssh-installation-tests/`, removed only after
every in-process `server.Run` has returned without a `*server.RunError`
and the test passed; otherwise the test reports the kept path. No test
connects to a real host.

The `Dial` ownership tests in `internal/agentssh` wrap the actual TCP
connection to an in-process SSH server and count the Closes that reach it;
failed Closes and deadlines, and a server that never answers, are MODELS.
The runner ownership tests use the in-process exec server, a MODEL client
whose session open stays pending and a MODEL stdin that holds a started
command. The `internal/agentsession` readiness tests use MODEL transports
and response bodies, whose Close failures hold nothing. Its
`TestOpenModelHost` tests run Open through an in-process SSH server with
actual SSH, host key, client key, channels, TLS and typed GetServerInfo,
but the agent build, the bootstrap script and the agent are MODELS: a
fixed file is hashed and transferred, the script's operands are
interpreted in memory, and an in-process Connect server stands in for the
agent. None builds or starts an agent or runs the script. The `sshagent`
tests use a MODEL SSH agent connection and a MODEL transport.

Prebuilt tests consume a separately compiled real agent; no test builds
one implicitly. Set `SKYGO_SSH_AGENT_BINARY`, `SKYGO_SSH_AGENT_COMMIT` and
`SKYGO_SSH_AGENT_SHA256` to its absolute path, asserted clean source commit
and actual digest. With `SKYGO_REQUIRE_FIXTURES=1`, missing pins fail; any
partial or incorrect pins always fail. Without the required-fixture mode,
missing all three pins skips only the positive prebuilt tests. Qualification
supplies every pin and requires zero skips. Corrupted executable fixtures
and the refusing `ssh -G` sentinel are modeled faults, not native runs.

## Native installation check (HOLD)

`testdata/native-installation.sh` refuses unconditionally, before any
network access or effect. A native check needs two things this module
does not have:

- A stop by the original process custodian. `agent.pid` records only a
  process ID; signaling by that number, or by a process name or a
  `kill -0` probe, may reach an unrelated process that reused it, and is
  not a substitute. The agent is started detached by a remote shell that
  exits, so nothing holds the started process and none of skygo-ssh, its
  tests or this check can stop it. There is no stop command, and none is
  described here; native work stays HOLD until a stop exists that is
  held by the process's original starter and waits for that same process
  to exit.
- An admitted binary. Running whatever `skygo-ssh` is first on `PATH`
  proves nothing about the source. The client binary, the agent it builds
  and the operator's `skygo` must be bound by SHA-256 to a separately
  admitted, real Git checkout (not a copied tree without Git metadata).

Until both exist, the native work is NOT_RUN. The intended cases are kept
here as a recipe. It is not executable as written: `ALIAS`, `KH`, `PIN`,
`SKYGO_SSH` (the admitted client binary), `NS` (`~/skygo-test/PIN/s11` on
the host) and `WORK` (a new private local evidence directory) are
placeholders, and every stop step below is the missing contract.

```
# Host floors, checked before any effect and again before each start:
# uname -m = aarch64; >= 100 GiB free in the remote HOME; >= 16 GiB
# MemAvailable; >= 4 CPUs; GPU utilization <= 10% and no compute apps.
# Run CPU-only jobs; no sudo, installation or system change.

# Operator, on the host, server stopped, for installations a and b:
skygo auth init --data-dir NS/a/data --auth-file NS/a/auth.json --token-output NS/a/admin.token
skygo state artifacts init --data-dir NS/a/data --artifact-dir NS/a/artifacts --ack-writers-stopped
# Copy NS/a/admin.token to WORK/a.token by a private file transfer (never
# through a captured stdout or a log), mode 0600.

# Client, locally (I is the four installation flags for a):
SKYGO_SSH --host ALIAS --known-hosts KH I artifacts upload WORK/source
SKYGO_SSH ... I run -f WORK/task.json --wait --request-id s11-native-PIN
SKYGO_SSH ... I artifacts download --ref WORK/result.json WORK/result
cmp WORK/expected.txt WORK/result/out/result.txt
SKYGO_SSH ... I run -f WORK/long.json --request-id s11-native-cancel-PIN
SKYGO_SSH ... I jobs logs --follow ID      # interrupted: job stays RUNNING
SKYGO_SSH ... I jobs cancel ID --yes       # job becomes CANCELED
# STOP the exact agent process (missing contract), then:
SKYGO_SSH ... --restart-stopped I jobs get JOB --json   # same result
SKYGO_SSH ... (installation b flags) targets list       # refused: mismatch
SKYGO_SSH ... targets list                              # refused: legacy into installation
# Both installation trees unchanged; no token in the agent directory.
# On any failure, signal nothing (the stop is the missing contract) and
# keep NS, the agent directory and WORK for inspection.
```

Tasks are files written locally (`task.json`, `long.json`); no task
argument is interpolated into a remote command line.

The `sshagent.RunCommand` API runs the same canonical command tree as
`skygo-ssh`. `CheckCommand` checks arguments without opening an agent;
help also opens nothing. `Command.Host` is an optional placement label,
not a connection or authorization selector. The connection, enrolled host
key, target and captured bearer come from `Config` and its verified session.
Caller Authorization headers cannot replace that bearer.

Each command uses the existing request/body ledger. It retains at most 64
response-body, transport-error and reserved response records, reserving two
slots before each dispatch (at most 32 simultaneously pending dispatches).
Clean bodies retire only after their first Close succeeds and every original
Read/Close returns without a non-EOF read error. Reads after Close begins are
refused; Close is never repeated. This bounds ledger records, not command
output, snapshots, caller buffers or server-side logs. A failed read, transport
or first Close fences the command and retains its original causes and owners.
Retain an unsettled error and do not reopen the host. Typed `Session.Control`
calls keep their existing semantics.

The first local admission refusal fences and cancels that command before
closing a refused request body. Cancellation requests a stop; it does not
join a handler or Close. Every already accepted refused request whose
first Close is pending or fails remains owned separately. The 64-record
bound above excludes those refusal originals and borrowed caller buffers.
Their count depends on handlers already accepted by the loopback relay;
that relay has no fixed handler-count limit. Clean first Closes retire their
own refusal bookkeeping, while every failed or pending original and cause
stays reachable through the command error. No 64-plus-one total memory bound
is claimed.

For loopback forwarding, the command's request-body guard is installed
before `ReverseProxy.ServeHTTP` captures cleanup. The transport and proxy
therefore share the same original first Close and stored result, including
when admission refuses a nonempty request. Request Reads and first Closes
remain counted until their actual return; only clean closed guards retire.
Their separate owner frontier includes admitted and refused forward bodies.
The HTTP server also has its own native request-body cleanup; this wrapper
does not replace that private standard-library owner or attest to independent
borrowed aliases.
