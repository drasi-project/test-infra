# Completed Building Comfort reference, version 2

This is a **completed uninterrupted reference**, not the earlier partial capture
with its flag changed. The old `building-comfort-small-v1` directory is unchanged.

| Evidence | Result |
|---|---|
| Successfully dispatched and independently captured input | 100,000 consecutive changes |
| Per-room query | 100,000 inputs committed; terminal output 99,981 recorded |
| Floor aggregate query | 100,000 inputs committed; terminal output 49,860 recorded |
| Final room snapshot | 12 rows, matching the final source values |
| Final floor snapshot | 3 rows, one per floor, matching independently computed aggregates |
| Capture finalization | Both receiver workers drained and all loggers finalized |
| Shared comparator against this reference | Passed, including producer-key uniqueness and ordering |

The reference was captured with the actual Server, separately built gRPC source
and reaction libraries, and test-service. It used Rust 1.95.0 debug builds on
macOS; it is not a throughput benchmark or a claim about every failure mode.
`provenance.json` records exact executable/plugin hashes and these source revisions:

- Core: `3dd8a27d4d7d38e48031be0d3774d255e35300e0`
- Server: `006b038cffffd948cd54d4b4fb71f57e5e27aa02`
- Test-infra: `2aef8c3463b7e8d1b98186b4f6c8b6f5896f4ac3`

This build includes the startup WAL-retention repair discovered during the first
real crash run. The refreshed uninterrupted output also matches the earlier
completed reference exactly; the retention repair did not change query results
or delivery order.

`completion-boundary.json` contains the fresh, current-process OTLP observations
and complete receiver batch records. Those observations are not inferred from
record counts or quiet output. `final-source-state.json` preserves the independent
model used to check the snapshots. Only that model's floating-point average
check permits absolute roundoff up to `1e-7`; the shared artifact comparator
performs exact reference/recovery state and delivery comparisons.

`golden-actual.json.gz` expands to an artifact with SHA-256:

```text
d44fe5561d1d203f66db22d259484634e7aeddf14dd522a54a6b10d7f331ff51
```

The workload fingerprint is
`cb3248faa7e1b362c52adb516c008f1fbab03681d6474618c5cd895df72b1c3d`.

**Representation-only rebase.** Query declarations now use `inputs` instead of the removed
`sources`/`sourceId` objects, which changes the canonical workload and query fingerprints.
This is not a new capture: the artifacts differ from the original capture only in those
fingerprint strings. The original capture's artifact SHA-256 was
`f53a8e949413c448dc7816b5b0e8dff751dd649fd403f7640d724683e6f7c527` and its workload
fingerprint `7c10344dbfa46fef5071661c945f970386782a07264a2d77fdc0418cfb1e0fc5`; both,
with the per-query fingerprints, are recorded in
[representation-rebase.json](representation-rebase.json). `result.json`, `comparison.json`,
`recovery-verification.json` and `completion-boundary.json` are the original run's reports and
keep the original values. No new completion or recovery evidence is asserted.
The configuration remains the finite, two-query, 12-room workload with seed
`123456789` and bootstrap off.

Use [the strict qualifier](../../qualify_building_comfort.py) to capture a fresh
reference for another build and require matching-build SIGKILL recovery.
The matching-build SIGKILL run also passed. `recovery-verification.json` records
the exact shared comparison and source replay evidence: the room query restored
input 100,000, while the aggregate restored 88,748 and replayed the remaining
11,252 events. Both finished at input 100,000. Delivery matched all 99,981 room
outputs and 49,860 aggregate outputs, with no missing, extra, duplicated,
conflicting or reordered records.

`recovered-actual.json.gz` and `recovery-boundary.json` retain that run. Its
uncompressed artifact SHA-256 was originally
`6f631d0e2921f72836d7734c20de62e86603a4e2e1e3603197a2c9f0c9ae584f` (matching
`recovery-verification.json`); after the representation-only rebase above it is
`d3d3e387c44ce99e874a3e93c77cca385e312c15265376f91ffcbcb3ea4e6dc6`.
Both runs' executable and plugin hashes were rechecked after qualification.
This proves the recorded standard-gRPC pending-input crash case, not every
possible transport/failure window.

The separate multi-transport scheduled workflow still uses its documented snapshot-only gate;
it must not be represented as completed processing/delivery qualification.
