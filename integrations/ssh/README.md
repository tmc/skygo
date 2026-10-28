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
the agent is always your current source. Without `--source` the checkout
must have no uncommitted changes; with it, uncommitted changes are built
too. Builds are cached under the user cache directory (`~/Library/Caches`
on macOS) in `skygo-ssh/build/COMMIT/SHA256/`.

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
refused as unverifiable. A running agent without a record, started by an
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
  Stop the previous agent first (`kill $(cat ~/skygo-test/C/D/agent.pid)`
  on the host); otherwise the new agent cannot listen and skygo-ssh
  reports it.
- There is no stop command, no scoped authentication, no retention domain,
  no artifacts or workspaces, and no dashboard. `server`, `auth` and
  `state` work on local files and are refused; run them on the host.
- Removing the local identity while the remote copy exists makes the next
  command refuse the mismatch; remove the remote agent directory too.
- The agent is built for linux/arm64 and installs only where `uname -m`
  reports `aarch64`.

## Packages

- `internal/agentssh` sends each HTTP request over its own SSH connection
  and `direct-tcpip` channel to the agent's loopback port, verifying the
  SSH host key against the dedicated known_hosts file and the agent with
  TLS. It never retries, replays, pools or follows redirects, and refuses
  compression and protocol upgrades. `Dial` returns an SSH client with the
  same checks for the bootstrap's commands.
- `internal/sshconfig` resolves the host alias with `ssh -G`.
- `internal/agentbuild` locates the checkout and builds the agent.
- `internal/bootstrap` holds the remote script and runs it.

## Tests

```
(cd integrations/ssh && GOWORK=off go test ./...)
GOWORK=off go test ./internal/server ./internal/cli
```

The bootstrap tests run the real script through an in-process SSH server
with a temporary `HOME`; the test that checks process reuse needs `/proc`.
`cmd/skygo-ssh` runs the agent in process with `server.Run` and drives it
with the canonical CLI. No test connects to a real host.
