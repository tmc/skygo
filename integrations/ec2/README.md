# EC2 integration

This optional Go module manages the lifecycle of one owned EC2 virtual
machine for skygo. It provides `skygo-ec2`, a command that creates, stops,
restarts and terminates one VM and keeps its lifecycle in a private state
directory.

`skygo-ec2` is a VM lifecycle tool. It is **not** a skygo execution target:
it runs no job, installs no agent, collects no artifacts or logs, and
nothing in skygo schedules work onto the VM. It has not been run against
AWS.

Packages:

- `internal/ec2api`: a small EC2 and STS Query client. It builds an
  immutable RunInstances request whose ClientToken is derived from the
  environment identity, tags the instance, root volume and network
  interface at creation, signs with the AWS SDK for Go v2 SigV4 signer,
  makes exactly one HTTP attempt per call and never retries. Before every
  create, stop, start and terminate it checks that the credentials belong to
  the profile's account; before every stop, start, terminate and read of an
  instance it describes the exact instance ID and refuses it unless the
  account, region, ClientToken, every tag, image, type, subnet, key pair and
  security groups match.
- `internal/ec2vm`: the lifecycle state machine. `Step` makes at most one
  EC2 call for a record.
- `internal/ec2ctl`: the durable caller. It stores every record field in a
  locked private directory and drives `ec2vm.Step` with a real
  `ec2api.Client`.
- `internal/ec2test`: a local TLS model of EC2 and STS, used only by tests.
- `cmd/skygo-ec2`: the command.

## Build and run

From the repository root:

```sh
(cd integrations/ec2 && GOWORK=off go build -o skygo-ec2 ./cmd/skygo-ec2)
```

```
skygo-ec2 command -config FILE -state DIR [flags]

  start     create the VM if needed and wait until it runs
  stop      stop the VM and wait until it is stopped
  restart   stop the running VM, then start it again
  down      terminate the VM; latched: it is never started again
  status    print the stored state; with -refresh, also describe the VM

  -credentials FILE  needed except by status without -refresh
  -timeout D         bound on the command (default 10m, at most 2h)
  -steps N           most lifecycle steps (default 120, at most 1000)
  -interval D        wait between observations (default 5s, at most 5m)
  -json              print the status as JSON
  -refresh           status: describe the owned VM now
```

Exit status: 0 goal reached (or status printed), 1 error, 2 usage error,
3 goal not reached because the record is held or the step bound was hit.
Status goes to standard output, errors to standard error.

For example, with fixture identifiers:

```sh
skygo-ec2 start  -config vm.json -state ~/.local/state/skygo-ec2/env-0123 -credentials creds.json
skygo-ec2 status -config vm.json -state ~/.local/state/skygo-ec2/env-0123 -json
skygo-ec2 down   -config vm.json -state ~/.local/state/skygo-ec2/env-0123 -credentials creds.json
```

## Configuration

`-config` names a version 1 JSON file of at most 64 KiB, a private regular
file like the credentials file below (mode 0600). Unknown fields and
trailing data are refused. Every field is fixed for the life of the state
directory: a state whose profile, identity or user data differ is refused.

```json
{
  "version": 1,
  "profile": {
    "account": "123456789012",
    "region": "us-west-2",
    "subnet_id": "subnet-0123456789abcdef0",
    "security_group_ids": ["sg-0aaaaaaaaaaaaaaaa"],
    "image_id": "ami-0123456789abcdef0",
    "instance_type": "t3.micro",
    "cpu_credits": "standard",
    "key_name": "",
    "root_device": "/dev/xvda",
    "root_volume_gib": 16,
    "root_volume_type": "gp3",
    "encrypt_root": true
  },
  "identity": {
    "owner": "install-7f3a",
    "environment": "env-0123456789abcdef",
    "generation": 1,
    "lease": ""
  },
  "user_data": "#cloud-config\n"
}
```

The subnet, security groups, image and key pair must already exist.
`skygo-ec2` creates no IAM, network or image resources. A new generation
is a new VM and needs a new state directory.

## Credentials

`-credentials` names a JSON file of at most 8 KiB, a regular file owned by
the user, not a symbolic link, with one link, owner read and write and no
group or other permission (mode 0600). A directory, FIFO, device or socket
is refused before anything is read; opening a FIFO does not wait for a
writer. `-config` and the files in the state directory are checked the
same way. The file holds:

```json
{"access_key_id": "AKIA...", "secret_access_key": "...", "session_token": "..."}
```

`session_token` is optional. `skygo-ec2` reads credentials only from this
file. It reads no AWS environment variables or shared configuration, uses
no instance metadata service or proxy, and has no endpoint option. The
credentials are kept in memory for the one command; they are never written
to the state directory and never printed. Errors from EC2 and STS print
the action, error class, provider code, HTTP status and request ID, never
the provider's message.

## State directory and file format

The state directory must be owned by the user with no group or other
permission (`start` creates it with mode 0700 if it does not exist). It
holds:

- `lock`: a nonblocking exclusive `flock` taken by every writer and held
  across every EC2 call until the command ends. A second writer fails at
  once. The kernel releases the lock when its holder exits; nothing
  deletes it or steals it by age or process ID.
- `vm.json`: the state, mode 0600.
- `vm.json.tmp`: an uncommitted save, removed when the lock is taken.

Every writer syncs the state directory's parent before it takes the lock,
so the directory's entry is durable before anything is stored in it or
any EC2 call is made; if that sync fails, the command stops there. A
directory left by a failed sync holds no state, so `start` refuses it
rather than delete it. A save writes the temporary file, syncs it,
renames it over `vm.json` and syncs the directory. A symbolic link in place of the directory or any of
these files is refused, as is a file with more than one link. Directories
above the state directory are not checked. Locking and the permission
checks need Linux or macOS.

`vm.json` is format `skygo-ec2-vm-state` version 1: indented JSON with the
profile, a restart flag and every field of the lifecycle record (identity,
profile hash, exact RunInstances request bytes, ClientToken, desired state,
phase, hold, instance ID, reported unverified instance IDs, create-sent
flag and time, credit verification and the last failure's fixed
diagnostic). Another format or version, an unknown field, trailing data, a
non-canonical encoding, an inconsistent record or more than 256 KiB is
refused and left untouched; there is no migration. Do not edit or delete
it: `start` refuses an existing directory without a state file rather than
begin a second record.

## Lifecycle contract

- The record of a new VM, wanting it present with nothing sent, is stored
  before any EC2 call. Every goal (`start`, `stop`, `restart`, `down`) is
  stored before anything acts on it.
- Before RunInstances, the record is stored marked as create sent. If that
  store fails, nothing is sent.
- Every record a step returns, success or failure, is stored before the
  next step, including reported instance IDs and the last diagnostic. If
  that store fails, the command stops and makes no further EC2 call until
  the record is stored; a crash instead leaves the last stored record.
- An ambiguous create (lost reply, cancellation, deadline, crash) is never
  sent again and never given a new ClientToken. It is resolved only by
  searching for the stored ClientToken and owner tags. If the search
  observes nothing, the record is held; only `down` continues it, searching
  again so that it can terminate what it finds. This bounds the identity to
  one create intent and prevents an automatic second launch; it is not a
  promise of exactly-once delivery by AWS.
- A failed stop, start or terminate is not retried by the command and is
  not reported as done: the record keeps the phase last observed and a
  diagnostic saying the call may have taken effect. Running the command
  again is the operator's decision. A crash can repeat the last stop, start
  or terminate of the owned instance, which EC2 treats idempotently.
- `down` is latched: afterwards `start`, `stop` and `restart` are refused.
- `restart` is stored as "wants stopped, restart pending". When a step
  observes the VM stopped, "wants present" is stored, clearing the flag,
  before the start is sent. Running `restart` again while one is pending
  resumes it rather than stopping twice.
- A record is held, and the command exits 3, for an identity or account
  conflict, an instance that is not owned, unverified reported instances,
  an unobserved create, or a CPU credit mismatch. `status` shows why.
- `status` prints stored evidence and makes no call. `status -refresh`
  also describes the owned instance (by ID, or by ClientToken if only a
  create was sent) and prints that observation separately; it stores
  nothing. For a running VM that wants present, `start` makes no call, so
  it is not a refresh.
- A deadline, cancellation, held record or reported terminated state is
  not evidence that a mutation did or did not happen, nor that the
  instance's root volume and network interface are gone.

## Tests

```sh
(cd integrations/ec2 && GOWORK=off go test ./...)
```

Tests compose the real state files (fsync, rename, flock, permissions
under a temporary directory), `ec2vm.Step`, a real `ec2api.Client` and the
command's `run` function against the `ec2test` local TLS endpoints, which
verify every request signature and use fixture credentials only. Store
failures are injected only between real writes. The fixture models the EC2
behavior the code depends on; passing tests are not evidence of AWS
behavior.

`ec2api.NewClientWithEndpoints` exists so tests can point a real client at
the local endpoints with explicit trusted roots. The command never calls it.

## Not yet done

This module is one slice of the S12 provider work. Still required before
EC2 is a skygo execution target:

- agent bootstrap on the VM and finite job execution, artifact and log
  collection and cancellation through the SSH remote executor;
- composing a persisted skygo target and Environment with this lifecycle,
  rather than a separate state directory;
- an independent maximum-lifetime cleanup backstop, and inventory and
  cleanup of root volumes and network interfaces after termination;
- separately authorized real AWS evidence for create, a job, retry and
  restart with one owned VM, stop and start, and confirmed termination of
  every task resource.

Native use is on hold: no AWS account, credentials or budget have been
authorized for it.
