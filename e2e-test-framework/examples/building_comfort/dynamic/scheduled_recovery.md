# Daily SIGKILL Recovery

The [building-comfort recovery workflow](../../../../.github/workflows/e2e-building-comfort-recovery.yml)
runs daily at **22:00 UTC**, following the nightly convention in
[test-infra PR #93](https://github.com/drasi-project/test-infra/pull/93).
GitHub schedules run the workflow on the repository's default branch; publishing
only to `crash-recovery` does not activate its cron schedule. GitHub may delay
scheduled execution.

## Scheduled Configuration

| Setting | Value |
|---------|-------|
| Server source | `drasi-project/drasi-server`, branch `main` |
| Core dependency override | `drasi-project/drasi-core`, branch `main` |
| Plugin registry / tag | `ghcr.io/drasi-project` / `drasi-nightly-test` |
| Variants | HTTP standard/adaptive and gRPC standard/adaptive |
| Scenario | SIGKILL after successful dispatcher drain, restart preserving data |
| Workload | 12 rooms, 100,000 changes, both queries, bootstrap preset off |
| Persistent index / state store | Both enabled |
| Outbox capacity | 20,000 per query |
| Batching speed / query tuning | 10,000 / 10,000 |
| Test wait timeout | 45 minutes per wait, excluding builds |
| JSONL capture | Enabled |
| Golden | Always `building-comfort-small-v1` |

The shared runner injects the core-main dependency override and checks the
resolved Cargo package sources before building the server. The prepare job builds
the binaries once and shares them across all four recovery jobs. Existing build
artifacts retain server commit, lockfile, and binary hashes; server logs record
loaded plugin metadata.

## Nightly Plugin Semantics

[Core's nightly workflow](https://github.com/drasi-project/drasi-core/blob/main/.github/workflows/nightly.yml)
starts at 14:00 UTC and publishes the mutable `drasi-nightly-test` tag. This
recovery workflow consumes that tag; it does not trigger core, require a fresh
successful core nightly, or guarantee that core-main HEAD equals the plugin build
commit. A delayed or failed publication can leave yesterday's plugins available.
An ABI or behavior mismatch should remain visible as a failed test, not silently
fall back to another tag. Compare recorded provenance when investigating failures.

## Manual Runs and Verification

Manual dispatch retains its existing transport, server, and plugin settings;
scheduled defaults do not override manual `false` selections or force a core-main
override on manual builds. Both modes require persistence, both queries, bootstrap
off, and the fixed golden. Missing, malformed, invalid, or structurally incomplete
comparison reports fail, including evaluation errors or missing expected query
results. Valid passed, failed, and inconclusive comparison verdicts remain
advisory with existing completeness/identity limitations. Counts/hashes
and the strict SIGKILL recovery gates remain enforced.

No SIGTERM or receiver-rejection scenario is scheduled. Those remain on the
separate follow-up branch. This schedule does not add results publication or
cross-repository dispatch permissions.

Regression check from the repository root:

```bash
ruby e2e-test-framework/examples/building_comfort/dynamic/test_sigkill_golden_workflow.rb
```