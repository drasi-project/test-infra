# Local Building-Comfort Golden Capture

This directory contains the fresh 100,000-change standard-gRPC building-comfort
golden capture started on 2026-09-14. The uninterrupted run exited zero with
99,981 per-room and 49,860 aggregate results; both stored SHA-256 expectations
matched. No recovery comparison against this capture has yet been confirmed.

Configuration: default small building hierarchy, seed 123456789, both queries,
persistent index and state store, outbox capacity 20,000. Server SHA-256:
`ed6b4b97938231be238a479ff2d206c7376b56a8f3e440dbf85c7ded5230612e`.

Raw run artifacts are retained at `/tmp/recovery-golden-84.LGboor`.
The normalized baseline is stored compressed as `golden-actual.json.gz`;
decompress it outside this directory before manually configuring it as
the handler's `baseline_path`. The original successful API snapshots, workload
description, strict-hash verdict, and binary provenance are stored alongside it.

The capture is a **candidate golden baseline**. The existing fixed-count
observers do not prove a terminal producer/output boundary, and current gRPC
logs lack stable producer event identities. Thus `capture.complete` remains
false and identity contracts remain null. The comparator may establish equality
of the observed snapshots, but its overall verdict must remain inconclusive.
Do not relabel this capture complete merely because the old count/hash gate passes.

No delivery fields are used to manufacture identities. Snapshot comparisons
preserve full row values and multiplicity, including repeated floor IDs.

## GitHub Run

Publish the framework comparator implementation, these golden files, and the
workflow/runner changes together. In `E2E - building_comfort recovery`, select:

```text
golden_snapshot: local-20260914-LGboor
http_standard: false
http_adaptive: false
grpc_standard: true
grpc_adaptive: false
query_building_comfort: true
query_floor_agg: true
bootstrap_size: off
batching_speed: 10000
query_tuning: 10000
outbox_capacity: 20000
persist_index: true
state_store: true
drasi_server_repo: ruokun-niu/drasi-server
drasi_server_ref: 6ffbfc0
drasi_server_version: ""
plugin_registry: ghcr.io/ruokun-niu
plugin_tag: composite-key
timeout_minutes: 30
```

The workflow runs recovery only, validates source/query compatibility, decompresses
this stored baseline, and enables the framework handler before the completion
log marker. Its policy is `exactly_once` with `allow_reordering: false`.
The standard-gRPC values above are one example: any combination of HTTP/gRPC
standard/adaptive checkboxes may now be enabled. Each compares the same golden
snapshot. Only dispatcher configuration is excluded from workload matching;
the input generator and queries must still match. HTTP uses its logger's nested
payload path and unavailable identity diagnostics. Compatible HTTP plugins must
be present in the selected registry/tag before selecting HTTP modes.
The comparison remains advisory because of the candidate capture limitations;
the existing strict SHA-256 gate remains enforced. No completion evidence is
fabricated, and no new golden run is generated.

The job summary's Advisory Golden Comparison section shows per-query state and
delivery verdicts. Full differences are saved as `recovery_verdict.json` beneath
the test-run storage in the uploaded artifacts. Overall inconclusive is expected
with the current missing identity/boundary evidence, even if snapshots match.
Missing/invalid verdicts are not successful comparisons. The Linux CI server
binary differs from the macOS golden binary; retain provenance with both runs.