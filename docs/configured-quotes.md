# Configured quote planning

A server can attach operator-configured rates and advisory capacity to its named targets. It reads `--target-facts` once at startup, before opening the state directory. The file is limited to 64 KiB and supported on Linux and macOS. It must be a stable regular file; symlinks and FIFOs are rejected. Regular filesystem reads have no universal deadline guarantee.

```yaml
version: 1
targets:
- name: local
  quote:
    amount_micros: 1000000
    currency: USD
    unit: target-hour
    source: operator:example
    observed_at: "2026-10-18T12:00:00Z"
    valid_until: "2026-10-18T13:00:00Z"
  capacity:
    state: available
    source: operator:example
    observed_at: "2026-10-18T12:00:00Z"
    valid_until: "2026-10-18T13:00:00Z"
```

Set the timestamps to your actual observation and expiry; the quoted dates above are illustrative. Validity windows must be ordered and at most 24 hours, using full UTC timestamps with a `Z` suffix and up to nine fractional digits. Sources are public identifiers, not credential strings or URLs. Every entry must match a configured target. Unknown fields, duplicates, aliases, numeric coercions and multiple documents are rejected.

`amount_micros` is mandatory in a quote. `1000000` means one currency unit per declared rate unit. Explicit `0` is a configured free rate; an absent quote remains unknown. Supported units are `target-hour`, `cpu-hour` and `accelerator-hour`. A capacity snapshot has state `available`, `unavailable` or `error`; `available_slots` is optional, so an unknown count remains distinct from zero. Capacity does not reserve a slot or affect ranking.

```sh
skygo server --data-dir ./state --target-facts ./facts.yaml
skygo plan -f task.yaml --objective lowest-rate --currency USD
skygo plan -f task.yaml --objective lowest-rate --currency USD --billable-duration 45m --json
```

The same facts flag works on `skygo-kubernetes server` alongside its normal target configuration flags. Planning applies ordinary hard constraints, compares fresh matching currency/unit rates, then breaks ties by bytewise target name. The response includes every target, rejection and freshness/status reason. No selection exits with status 1. Old servers return an unsupported-method error; the client does not fall back to ordinary planning or submission.

The recommendation is advisory. It does not set `task.target`, reserve capacity or change the behavior of a later `run`. To request a particular target, set that target explicitly in the submitted task. Ordinary `plan`, explicit `--objective first-fit`, and `run` keep their existing behavior. Quote-only flags are rejected on first-fit planning, and `run` has no objective flags.

A billable duration is an optional user-declared total of whole seconds from 1 second to 168 hours. It is never inferred from timeout, retries or resource requests. A target-hour quote can estimate compute cost by rounding rate × seconds / 3600 upward to the next micro-unit. Other units report unknown quantity. Missing duration reports unknown duration. Estimates exclude storage, transfer, tax, licenses and provider adjustments. They are neither a bill nor a prediction of execution time.

These facts are startup snapshots, not refreshed provider prices. Freshness is evaluated at each request using the captured server time. Future and expired facts remain visible with explicit status; they are not treated as zero. The complete response is limited to 64 KiB in protobuf and both compact JSON field-name profiles; overflow returns an error without dropping candidates. JSON output preserves optional zero values and uses compact protobuf JSON plus one newline.

## Manual verification

1. Configure two named Kubernetes targets `a` and `b` using the existing targets file, with valid kubeconfig/context/namespace values. Attach fresh target-hour USD rates of 2000000 and 1000000 micros respectively. Start `skygo-kubernetes server --disable-local --targets targets.yaml --target-facts facts.yaml`. Use a task containing an image and argv accepted by both targets. `plan --objective lowest-rate --currency USD` should recommend `b`, while ordinary `plan` should select `a`. Neither operation creates a Kubernetes Job.
2. Set the task's target to `a`; advisory planning must select `a` and retain `b` as ineligible. Remove the target constraint, make `b`'s rate explicit zero, restart the server and provide `--billable-duration 1h`: rate and estimated cost must both print `0.000000`, with a capacity warning when no fresh available snapshot exists.
3. Remove both quotes, restart, and request JSON quote planning. Expect `NO_COMPARABLE_QUOTE`, visible candidates, unknown estimates and exit 1. Do not expect a zero cost. Set an expiry to the present or past; expect `STALE` rather than comparison.
4. Stop the server, point `--target-facts` at invalid YAML and use an absent state directory. Startup must fail without creating that directory. Start with a valid file, remove it after readiness, and repeat planning: the configured startup snapshot remains available until its expiry.
5. Point quote planning at an older server without QuoteTask. Expect an unsupported-method error and no result. Ordinary first-fit planning must still work with that server.

Real provider refresh and native pricing/provider qualification remain separate open work.
