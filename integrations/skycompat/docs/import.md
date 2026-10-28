# Importing SkyPilot tasks

`skycompat import` lowers one SkyPilot task YAML document, at upstream commit
`922a9f046c4a2714edf2b76fec3d1862dcd56ae3`, to a skygo task file. It is
pure and offline: it reads only the task and the optional workdir reference
named on its command line, never the environment, and makes no network
connections. Planning, capability checks, staging and submission stay with
the existing `skygo` commands.

```
skygo artifacts upload ./project > workdir.json
skycompat import --shell /bin/sh --workdir-ref workdir.json \
    --output out --target local --allow-drop name task.yaml > task.json
skygo plan -f task.json
skygo run -f task.json --wait --request-id R
```

`task.json` is a JSON object whose keys are the `skygo run -f` task file
keys; skygo decodes it strictly, so an unknown key fails before any call.
Integers are exact decimal numbers. Standard error carries one line per
recognized source field, `PATH<TAB>STATUS<TAB>DETAIL`, where STATUS is
`lowered`, `default` (an upstream default or null that needs no
representation) or `dropped` (a nonexecuting field dropped by
`--allow-drop`). Notes and errors never contain source values.

A field that cannot be represented is refused. Standard output stays empty,
standard error carries one line such as

```
task yaml: unsupported resources.cloud at 3:10: select a skygo target
```

and the exit status is 1. Usage errors exit 2. Position `0:0` means the
field is absent, as for a task without `run`.

## Flags

| Flag | Meaning |
| --- | --- |
| `--shell PATH` | Required. Clean absolute path of the shell on the target. `run` and `setup` text become `[PATH, "-c", TEXT]`, byte-exact. |
| `--workdir-ref FILE` | The reference `skygo artifacts upload` writes. Required exactly when the task has `workdir`. |
| `--output ROOT` | Result root relative to the workdir. Repeatable. Requires `workdir`. |
| `--target NAME` | skygo target name. |
| `--timeout-seconds N` | Attempt timeout, 0..604800. Zero keeps the server default. |
| `--allow-drop FIELD` | Drop `name` or `_user_specified_yaml`. Repeatable. |
| `--env KEY=VALUE` | Environment override, also required for a null `envs` value. Repeatable. |

## Field mapping

| Source field | Lowering | Refused when |
| --- | --- | --- |
| `run` | `argv = [shell, "-c", text]` | absent or null; no `--shell`; NUL |
| `setup` | `setup = [shell, "-c", text]` | no `--shell`; no `workdir`; over 16 KiB encoded |
| `workdir` | `workdir` from `--workdir-ref`; the path is never read | no `--workdir-ref`; text contains `$` |
| `envs` | `env`, after the parser's Python string coercion and `--env` | invalid name or value |
| `resources.cpus: N`, `N+` | `cpu_millis = N × 1000`; `+` lowers to the same exact request | not whole millicpus |
| `resources.memory: N[unit][+]` | `memory_bytes`; no unit means GiB; KB/KI through PB/PI are powers of 1024, any case | not whole bytes or over int64 |
| `resources.accelerators` | `accelerator`, `accelerator_count` | count not a whole number in 1..2147483647; more than one choice |
| `resources.image_id: docker:REF` | `image = REF` | any other form |
| `resources.disk_size: 256` | default: the upstream unspecified value, not a disk request | any other value |
| `resources.infra` | `target` only through an exact operator-authorized `Options.InfraBinding` | missing or mismatched binding, or an explicit target conflict |
| `resources.autostop`, `job_recovery` | default when null | any value, including `false` or `{}` |
| `resources.use_spot: false` | default | `true` |
| `resources.cloud/region/zone/instance_type` | default when null | any value: select a skygo target |
| `num_nodes: 1` | default | greater than 1 |
| `file_mounts`, `volumes` | default when empty | not empty |
| `name`, `_user_specified_yaml` | dropped with `--allow-drop` | otherwise |

Every other field is accepted only as null or refused by the parser with its
source position. The importer accepts one task document, not the SDK's DAG
envelope.

The local target refuses images and resource requests at plan time, so an
imported resource request fails `skygo plan` and creates no job; it is never
dropped in translation.

## Differences from upstream execution

- The text runs as `shell -c TEXT`: no `~/.bashrc`, conda activation,
  `set -a` or `PYTHONUNBUFFERED`. The working directory is the attempt's
  private workdir root, not `~/sky_workdir`.
- `SKYPILOT_*` variables (node rank, IPs, task ID) are not set.
- Upstream Kubernetes defaults of 2 vCPUs and 1 GiB per vCPU are not
  injected. Absent resources mean skygo's default: no request.
- Upstream environment substitution in `workdir` is not implemented.
- skygo job IDs are opaque, and a safe retry reuses `--request-id`.
- On the local target the text runs as the server's OS user on the server
  host, which is not a sandbox. On Kubernetes the shell must exist in the
  image.

## Command mapping

skygo adds no SkyPilot command aliases; each would hide a difference.

| Upstream | skygo | Difference |
| --- | --- | --- |
| `sky jobs launch task.yaml` | `skycompat import …` then `skygo run -f task.json --wait` | explicit shell and workdir reference |
| `sky jobs queue` | `skygo jobs list` | one page; `next-page-token` on stderr |
| `sky jobs logs ID` | `skygo jobs logs -f ID` | upstream follows by default |
| `sky jobs cancel -y ID` | `skygo jobs cancel -y ID` | same spelling |
| exit codes 100/103 | exit 2 | any unsuccessful waited job |
| `sky launch`, `exec`, `down` | none | persistent clusters are not finite jobs |

## Qualification

The fixture scripts in `cmd/skycompat/testdata/script` run the actual
importer, the actual `skygo` client against a recording server, and an
actual `skygo` server on the test host. They cover grammar, previews,
refusals, strict decoding of every emitted file with exact integer and
reference values, a resource refusal at plan time with no job, a workspace
refusal before submission and a byte-exact local run of an imported task.
The workspace upload-run-download workflow on Linux, Kubernetes execution
and comparison with upstream `sky jobs launch` are separate acceptance runs.

`Options.InfraBinding` is for an operator-configured or authenticated transport
that already knows the infrastructure selector and its fixed target. The task
cannot supply this binding or select a server endpoint. A matching selector
sets the target only for that field; an explicit different target is refused.
An unused binding does not change a task's target. This mapping establishes no
GPU capability, storage semantics or provider access.
