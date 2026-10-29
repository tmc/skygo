# Directory snapshots and Linux workspaces

Upload a directory, run a finite local job with that snapshot as its working
directory, and download its declared outputs. A Ref identifies immutable archive
bytes; it is not a credential or a host path. The server still checks access.

Local workspace jobs require Linux and the server's process-ownership support.
macOS local targets and the Kubernetes backend do not advertise workspace
support. Transfer support and execution support are separate.

## Install storage

Stop the server and every previous writer before installation. Create an empty
private directory, then explicitly initialize it:

```sh
umask 077
mkdir artifacts
skygo state artifacts init --data-dir state --artifact-dir artifacts --ack-writers-stopped
skygo server --data-dir state --artifact-dir artifacts
```

The server runs in the foreground. Start it with the same two directories on
later runs. Startup validates the existing installation; it does not initialize
or repair a directory. Omitting `--artifact-dir` leaves transfers disabled.
Use a dedicated directory, separate from the data directory and task inputs.

The commands below use the default loopback endpoint. For a remote server, set
`SKYGO_ENDPOINT` and choose one credential source: `--token-file` or `SKYGO_TOKEN`.
Use the same principal for uploading, submitting, and downloading. See
[scoped authentication](authentication.md). Local programs run as the server's
OS user; a private working directory is not a sandbox. Keep credentials out of
uploaded files and collected outputs.

## Upload, run, and download

This example uses one Linux host for the client and server. Build `skygo` as
shown in the [README](../README.md#run-locally) and put it on `PATH`. Start in a
new private working directory. In the first terminal, run the storage-install
commands above. Leave that server running. In a second terminal, use the same
working directory. Check the running server before transferring files:

```sh
skygo info --require artifact-snapshot-service-v1
skygo info --target local --require workdir-snapshot-v1
```

Create an input directory and a small Go program. Setup writes `prepared`;
the main program combines it with the uploaded seed and writes an output file.
The program must be available at its absolute path on the server host.

```sh
umask 077
mkdir input
printf 'seed\n' > input/seed.txt
cat > workload.go <<'EOF'
package main

import (
    "encoding/json"
    "errors"
    "fmt"
    "os"
)

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func run() error {
    if len(os.Args) == 2 && os.Args[1] == "ref" {
        var job struct {
            Result struct {
                State string `json:"state"`
                Reference json.RawMessage `json:"reference"`
            } `json:"result"`
        }
        if err := json.NewDecoder(os.Stdin).Decode(&job); err != nil {
            return err
        }
        if job.Result.State != "ARTIFACT_RESULT_STATE_AVAILABLE" || len(job.Result.Reference) == 0 {
            return errors.New("job has no available result")
        }
        return json.NewEncoder(os.Stdout).Encode(job.Result.Reference)
    }
    if len(os.Args) == 2 && os.Args[1] == "setup" {
        return os.WriteFile("prepared", []byte("prepared\n"), 0600)
    }
    seed, err := os.ReadFile("seed.txt")
    if err != nil {
        return err
    }
    prepared, err := os.ReadFile("prepared")
    if err != nil {
        return err
    }
    if err := os.Mkdir("out", 0700); err != nil {
        return err
    }
    return os.WriteFile("out/result.txt", append(seed, prepared...), 0600)
}
EOF
CGO_ENABLED=0 go build -o workload workload.go
skygo artifacts upload input > input.json
cat > task.json <<EOF
{
  "argv": ["$PWD/workload"],
  "setup": ["$PWD/workload", "setup"],
  "workdir": $(cat input.json),
  "outputs": ["out"],
  "target": "local",
  "timeout_seconds": 10
}
EOF
skygo --json run -f task.json --request-id snapshot-example --wait > job.json
./workload ref < job.json > result.json
skygo artifacts download --ref result.json result
printf 'seed\nprepared\n' > expected.txt
cmp expected.txt result/out/result.txt
```

Stop the foreground server with Ctrl-C when finished. Keep its state and artifact
directories together for the next startup.

Keep `task.json`, its request ID, and any printed request domain together for
submission recovery. Retrying that exact accepted request replays the existing
job; changing the task conflicts. Interrupting the client does not cancel the
job. Use `skygo jobs cancel JOB_ID --yes` to request cancellation.

`workdir` contains the uploaded Ref, `setup` is an argument vector, and `outputs`
contains relative paths to collect after the main program succeeds. Programs
are executed directly; shell syntax requires an explicit shell program. A task
with setup or outputs also needs workdir. Download requires a new destination;
it verifies the archive and digest before creating that directory. An extraction
failure can leave the newly created private destination for inspection.

## Synchronize a SkyPilot workdir

`run --sync-workdir` snapshots a supported SkyPilot YAML task's local workdir,
uploads it, and submits the task. The client may run on macOS or Linux; workspace
execution still requires a Linux server advertising `workdir-snapshot-v1`.
The server also needs artifact storage and retained requests enabled.

With the server stopped, enable retention in its existing data directory, then
restart it with the same storage and authentication configuration:

```sh
skygo state retention enable --data-dir state
skygo server --data-dir state --artifact-dir artifacts
```

See [retention](retention.md) before enabling it on existing state. This is an
alternative to the explicit upload example above. In a second terminal, start
in a new private working directory, check the server, and create a task:

```sh
skygo info --require artifact-begin-requests-v1
skygo info --target local --require workdir-snapshot-v1
umask 077
mkdir input
printf 'frozen\n' > input/seed.txt
cat > task.yaml <<'EOF'
workdir: input
setup: touch prepared
run: test "$EXPLICIT" = frozen && test -f prepared && cat seed.txt > result.txt
EOF
skygo --json run task.yaml --sync-workdir --workspace-state "$PWD/workspace-state" \
  --env EXPLICIT=frozen --output result.txt --wait
```

Keep the workdir quiescent while it is snapshotted. The complete snapshot is
uploaded; keep credentials and unrelated files outside it. The existing
[snapshot limits](#expiry-and-limits) apply. Paths must be trusted, and symbolic
links, hard links and special files are refused. This is not a filesystem sandbox.

`--workspace-state` names the private journal directory, outside the workdir;
its parent must already exist. If omitted, the client uses its platform user
cache directory and prints `workspace-state:` to stderr. The journal contains
the frozen YAML, explicit options, archive and recovery identities. Keep it
private and preserve it after a timeout or interruption.

Resume with the same HTTP(S) endpoint and the same effective credential:

```sh
skygo --json run --resume-workspace "$PWD/workspace-state" --wait
```

Resume uses the saved task and archive even if the original YAML or workdir is
no longer present. Once submission is accepted, it returns the same job ID;
it does not create another job. Do not supply a task path or environment,
output, placement or request overrides when resuming. Interrupting the client
does not cancel an accepted job.

The initial workflow refuses SSH transports, redirects and credential rotation.
A changed endpoint or credential, an incomplete journal, unknown journal files,
or an expired request domain can prevent recovery. Preserve the existing intent
for inspection; do not delete it or start a new request to resolve an uncertain
submission. Resume does not recreate missing server storage or extend expiry.

## Resume or remove a transfer

Upload prints an `upload-id:` recovery hint to stderr. Preserve it if the command
fails. Keep the source directory unchanged and quiescent, then use the exact ID:

```sh
skygo artifacts status UPLOAD_ID
skygo artifacts upload input --resume UPLOAD_ID > input.json
```

Resume reconstructs the snapshot and refuses a different digest or size. The
client retries some transient failures within one bounded retry budget and one
transfer deadline. An ambiguous Begin is not blindly repeated. `--timeout`
sets the whole upload or download deadline; increase it for a slower connection.
A timeout does not establish whether an upload or job was accepted.

To cancel an unfinished upload or remove a completed snapshot:

```sh
skygo artifacts cancel UPLOAD_ID --yes
skygo artifacts remove --ref input.json --yes
```

Removal refuses while an accepted job or read still needs the object, and
also waits for its deletion deadline and any release grace.
Do not remove files directly from the server's artifact directory.

## Recoverable Begin requests

RPC clients may require `artifact-begin-requests-v1` before supplying both
`request_id` and `domain` to `BeginArtifactUpload`. Persist the domain, request
ID, archive digest and size before sending the first request. Repeating that
principal/domain/ID with the same digest and size returns the original upload;
changing either value refuses the request. Omitting both fields preserves the
legacy Begin operation. An empty or partial pair refuses the request.

Replay does not extend upload expiry, recreate a part, or adopt an unfinished
creator. A returned `creating` upload can require operator recovery; it is not
permission to start another Begin. The request domain's existing acceptance,
retry and retirement deadlines still apply. Finalize and Submit continue using
their existing upload and job request identities.

This RPC feature requires database schema 14. Opening a schema 13 database
validates it before adding the empty request table and format marker. Older
servers refuse the resulting database. The serving storage family remains v1
and the artifact root format remains 8. The CLI's
[workspace synchronization](#synchronize-a-skypilot-workdir) uses this feature to
recover its persisted Begin request.

## Expiry and limits

Upload sessions expire one hour after Begin. Completed upload grants and
published results carry 24-hour expiry timestamps; input and result pins can
preserve access while the object is needed. Expiry is not a physical deletion
deadline. Save needed results with download;
a saved Ref alone does not extend access or preserve server bytes. Job result
metadata distinguishes available, absent, held, and expired results.

Snapshots use bounded uncompressed USTAR. Each archive and regular-file payload
is at most 32 MiB, with at most 2,048 regular files and 512 directories, including
implicit parents. Paths have at most 16 components and 240 bytes. Symbolic links,
hard links, devices, and other special files are refused. Outputs select at most
16 disjoint relative roots; globs are not expanded. Keep upload sources quiescent.

Jobs can create working files beyond their inputs, but storage accounting and
finite scan limits still apply. Unexpected storage identity, insufficient space,
or excess usage can hold publication instead of returning an unsafe result.
These limits do not impose an OS disk quota on a workload. The server remains a
single writer for its data and artifact installation; do not overlap instances.

## Storage and backup

The existing `state backup` command copies only the database. It excludes artifact
bytes, local logs, credentials, workload programs and external resources.
`state restore` still refuses snapshots with installed artifact metadata. A saved
Ref or a manually copied artifact directory cannot replace its missing bytes or
establish the stored filesystem identities. Do not initialize a replacement root
against an existing database.

### Stopped companion backup and restore

The separate `state artifacts backup` and `state artifacts restore` commands
handle an eligible stopped database/artifact pair. They copy **all complete
objects**, including completed uploads that no job references, together with the
database and a bounded format-1 manifest. They do not copy logs, credentials,
workload programs, container images or external infrastructure.

Stop and positively join the original server, backends and every artifact writer
before either operation. A database lock, terminal job status, expired lease or
old PID is not that handoff. `--ack-writers-stopped` records the operator's premise;
it does not establish it. Keep every old copy stopped before starting a restored
installation: logical owner and artifact identities are shared.

Source and bundle directories must be private and owned by the effective user;
object and database files must be private, regular and single-linked. The
publication parent must also belong to the effective user, have no group/other
write permissions or special mode bits, and remain the same real directory.
An owned 0755 parent is allowed; a shared writable parent is refused.

Keep the namespace trusted and stable throughout: no other process may rename,
replace or populate these paths. Private sibling staging is created relative to
the opened parent and checked against its original identity. Observed replacement
refuses publication; these separate checks do not prevent hostile same-user
mutation between syscalls. Stopping workload writers is a different premise.

Choose the bundle beside the source directories, not inside the data or artifact
root. The restored state's parent must be outside the input bundle. Overlap
through ancestor symlinks is also refused before staging. Destination names must
be new. The commands refuse replacement, and restore fixes its artifact directory
to `NEW_STATE/artifacts`; there is no artifact-directory override or follow-up
initialization step.

This first cohort refuses active or unknown execution, pending retries,
unqualified historical execution, executed jobs with unspecified output placement,
unfinished uploads, live readers, candidates, pending object/workspace cleanup,
and unresolved native/manual cleanup. Source
logs must be absent or empty. Completed retained local-output jobs require actual
normal output cleanup: no reservations, closure records, pending operations or
charged output bytes/files may remain. Historical tracking and valid expiration
records stay intact; deleting log files by hand does not qualify a job.
Some provably never-started retained queued jobs and fully resolved taskless
artifact-input pins are eligible. Queued work can start when the restored server
starts. Remote job metadata does not back up its external
workloads or logs. Do not force eligibility by editing the database or deleting
files underneath original owners.

The global object inventory is bounded to 128 complete objects and 512 MiB of
encoded archives; each principal is limited to 64 objects and 128 MiB. Each object
retains the snapshot limits above. The manifest is at most 2 MiB. Staging admission
accounts for the copied database and objects plus 2 GiB
of free space; this is a point-in-time check, not an OS quota or a reservation.

Backup leaves the original database and object bytes unchanged. Restore leaves
the bundle unchanged, verifies its exact inventory/digests/manifests, and builds
a new unpublished installation before publication. Logical artifact Refs and
owner identity survive; actual new file IDs and filesystem geometry are rebound
from the new files, not copied from the old inodes. Empty local-output geometry
is revalidated where enabled. Enabled request retention receives a fresh request
lineage, closes legacy replay and rejects the old request domain. Use the restored
server's discovered domain for new requests. No command resumes an unknown owner
or converts missing bytes into a completed artifact.

#### Transfer-only example

Use this separate example in a fresh directory. It does not make the earlier
workspace job's log history eligible. Build `skygo` from the matching checkout
and put it on `PATH`. In the first terminal:

```sh
umask 077
mkdir companion-demo
cd companion-demo
mkdir artifacts
skygo state artifacts init --data-dir state --artifact-dir artifacts --ack-writers-stopped
skygo server --data-dir state --artifact-dir artifacts --listen 127.0.0.1:8081
```

In a second terminal, enter that same `companion-demo` directory:

```sh
export SKYGO_ENDPOINT=http://127.0.0.1:8081
mkdir input
printf 'companion bytes\n' > input/seed.txt
skygo artifacts upload input > input.json
```

Stop the original foreground server with Ctrl-C in the first terminal and wait
for it to return successfully. Then, in the second terminal:

```sh
skygo state artifacts backup --data-dir state --artifact-dir artifacts --output bundle --ack-writers-stopped
skygo state artifacts restore --input bundle --data-dir restored --ack-writers-stopped
```

Leave the original server stopped. In the first terminal, start only the restored
installation:

```sh
skygo server --data-dir restored --artifact-dir restored/artifacts --listen 127.0.0.1:8081
```

In the second terminal, use the original Ref to verify the restored object:

```sh
skygo artifacts download --ref input.json recovered
cmp input/seed.txt recovered/seed.txt
```

Stop and join that foreground server when finished. With scoped authentication,
preserve and supply the latest matching auth file and client credentials
separately; a companion bundle contains neither. The example uses the default
single-operator loopback mode and proves a transfer roundtrip when executed,
not restored native workload execution or arbitrary backup eligibility.

#### Failure and handoff

Every failed operation retains any stage it created, even when its original
closes succeeded. A destination published before a Sync or Close failure is also
retained. Keep the original error and its source, bundle, stage and destination
for an explicitly authorized stopped recovery handoff. No failed stage is
automatically deleted, adopted or retried. After namespace interference, a printed
original path may no longer name the original inode; the path alone grants no
recovery authority. Do not remove it, overwrite a destination or close an owner
through a replacement handle. A failure writing the final CLI readback
can follow a completed operation; it is not rollback or permission to repeat it.
Context cancellation is checked between bounded copy chunks. It does not impose
a deadline on a blocked filesystem call or prove an original owner joined.
