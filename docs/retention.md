# Retention and storage maintenance

Retention is disabled by default. Opening or upgrading state does not activate
request expiry or pruning. Historical admission retains its job/task limits and
known-request replay behavior. Activation preserves valid existing work, including
over-limit state; subsequent growth can be refused. It does not stop workloads.

## Activate stopped state

Stop the server before these commands. Inspect reads an existing database without
migration or writes. It reports physical observations and bounded schema/binding
geometry; unknown fields stay absent, and an incomplete scan is not an invalid
installation verdict or migration safety certificate. Backup reads the stopped
source and writes a new snapshot; it excludes local logs.

```sh
skygo state backup --data-dir state --output before-retention.db
skygo --json state retention inspect --data-dir state
skygo state retention enable --data-dir state
```

Enable returns the request domain, deadlines, configured limits and a restart
requirement. Restart with the same state and existing authentication configuration:

```sh
skygo server --data-dir state
```

The default request window is 30 days. `--request-window` accepts one hour through
90 days. `--max-*` activation flags can lower the default logical limits, not
raise them. Local output retention is enabled by default; `--disable-local-output`
selects remote-only retention and requires an absent or empty owned log directory.

## Requests, results and logs

```sh
skygo jobs retention
skygo run -f task.yaml
```

Discovery reports the active request domain and deadlines. Admins also receive
usage, physical observations, conditions and one page of cleanup obligations.
Unavailable physical observations are unknown, not zero.

For a new request, `run` discovers an accepting domain and prints its domain,
request ID and task-file hint to stderr before submission. To retry an uncertain
submission, reuse that exact domain, request ID and original task with
`--request-domain` and `--request-id`. Do not replace the domain just to bypass an
expiry error. An explicitly supplied ID without a domain uses legacy lookup;
once enabled, an unknown legacy ID cannot create new work. Retained replay checks
Task identity, and result expiry can remain recorded for deduplication even when
the result is no longer available.

Request epochs bound admission and replay, rather than scheduling deletion of
results at a fixed age. Typed expiry distinguishes result expiry, request epoch
expiry, required domain, changed lineage and expired logs. Capacity errors carry
the logical quota domain, used value and limit. Restoring enabled state creates a
fresh request lineage and closes legacy replay; old request domains are not a way
to submit into the restored installation.

`jobs logs ID` selects one attempt once. Retained text streams print a resume
cursor to stderr; JSON chunks carry their cursor. Resume with `--cursor TOKEN`;
a cursor is scoped, does not grant authorization, and cannot be combined with a
nonzero offset. It cannot restore expired output. Local output retains a bounded
prefix per attempt; it is not an archive of arbitrary workload files.

## Prune deliberately

An admin can preview one eligible page, then execute the selection:

```sh
skygo jobs prune --before 2026-01-01T00:00:00Z
skygo jobs prune --before 2026-01-01T00:00:00Z --yes
```

Without `--yes`, pruning is read-only. Review selected/skipped counts and reasons.
A later execute request re-evaluates eligibility; it is not a frozen preview.
Follow returned page tokens for additional pages. Active, unknown and retry-backoff
jobs are protected from whole/metadata pruning. Whole pruning requires appropriate
stop/output proof; age alone does not authorize native execution deletion.

Admins can use `--result-only` with a nonzero, one-based `--attempt` to expire
one fixed attempt's artifact result while preserving its input, job and attempt
history, and request replay. For illustration, with `CUTOFF` set to your chosen
RFC3339 timestamp, preview and execute the same selection:

```sh
skygo --json jobs prune --result-only --attempt 2 --before "$CUTOFF"
skygo --json jobs prune --result-only --attempt 2 --before "$CUTOFF" --yes
```

This selects attempt 2 of each eligible job in the page; it is not a job-ID
filter. The cutoff compares each job's last update time, not that attempt's
finish time. Continue preview pages and execution pages separately, using their
returned page tokens and unchanged selectors. Execution re-evaluates eligibility.

After a successful mark, the selected result is reported as expired and cannot
start another download. Replaying the original request returns that history
without rerunning setup or the task. Result expiry does not promise immediate
physical deletion or released storage reservations.

`--logs-only` expires eligible closed output while retaining history and native
execution. `--metadata-only --ack-unverified-terminal` expires eligible terminal
metadata without asserting that execution stopped. It leaves a manual obligation
and does not automatically create cleanup work for that execution/output. Inspect
obligations with `jobs retention`; `--logs-only --obligation ID` selects one stored
obligation instead of an age/page selector. Cleanup still requires valid output
closure proof; the acknowledgement does not supply it. A mark is not a completed
unlink, released reservation or successful native cleanup.

## Storage maintenance and limits

Local retention reserves space per output domain using the validated filesystem
allocation unit as well as the retained prefix. Byte, domain and file limits apply
together: a default maximum of 256 domains is not a promise that 256 fit in the
pool. Output credit is released through completed cleanup, not merely metadata
expiry. Backend cleanup support is target-specific; inspect target information
rather than assuming remote/native cleanup support.

Logical accounting, database file length, allocated blocks, reusable database
space and filesystem free space are different quantities. Physical samples can
be unavailable or stale. Admission/maintenance watermarks are operational guards,
not disk reservations, a hard physical ceiling or a universal recovery promise.
Valid historical data may exceed newer logical limits.

After stopping the server and **all local output writers**, use
`state retention revalidate --data-dir state --ack-local-writers-stopped` when
local output accounting needs reconciliation. It validates known inventory and
updates accounting/device facts; it does not adopt missing output, prove stopped
execution or delete files.

`state compact --data-dir state --output compacted.db` validates the stopped
source and writes a new compacted database. It preserves the original, requires
space for the operation and does not replace live state automatically. Restore
requires a new directory; backup/compaction snapshots exclude local logs. Keep the
original installation stopped before running a restored copy. See the
[backup contract](contracts.md#state-backup-and-rollback).

Retained output is a backend log prefix, not an archive of arbitrary files.
Inspection and metadata expiry do not grant native cleanup authority. These
commands do not establish that an untested backend, native cleanup path or
physical capacity scenario is qualified.
