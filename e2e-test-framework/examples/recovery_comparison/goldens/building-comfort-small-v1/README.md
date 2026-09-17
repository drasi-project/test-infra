# Building-Comfort Small Golden v1

Workflow selection: `building-comfort-small-v1`. The small workload has 12 rooms
and 100,000 source changes. Version 1 identifies this saved baseline, shared by
the recovery scenarios. Previously named `local-20260915-I1FGSs`; renaming did
not change capture data, provenance, fingerprints, or expected results.

Fresh uninterrupted standard-gRPC capture completed on 2026-09-15. The default
100,000-change building-comfort workload used seed 123456789, both queries,
persistent index/state store, and outbox capacity 20,000.

| Query | Captured results | Missing producer keys | Repeated producer keys | Snapshot rows |
|-------|-----------------:|----------------------:|-----------------------:|--------------:|
| building-comfort | 99,981 | 0 | 0 | 12 |
| building-comfort-floor-agg | 49,860 | 0 | 0 | 12 |

Both existing ordered SHA-256 expectations passed. The new capture preserves
producer query ID, sequence, row signature, and operation using identity contract
`grpc-query-sequence-row-operation-v1`. The tuple is unique within each query in
this capture. This does not yet establish stable identity mapping across separate
runs or transports.

## Files

- [Golden room snapshot](query_results__building-comfort.json)
- [Golden aggregate snapshot](query_results__building-comfort-floor-agg.json)
- [Producer-key checks](identity-validation.json)
- [Strict hash checks](determinism_verdict.json)
- [Workload](workload.json) and [provenance](provenance.json)
- [Comparison policy](policy.json) and [self-comparison](self-comparison.json)
- `golden-actual.json.gz`: normalized artifact containing producer identities,
  semantic event payloads, and both snapshots. Decompress before using as the
  comparator's baseline; the comparator does not directly read gzip.

The policy requires exactly-once, ordered output. A self-comparison passed both
delivery and state checks; it is only an artifact sanity check, not a recovery
test. The overall verdict remains inconclusive under the existing completion
policy because the terminal output boundary was not independently established.
The snapshots record state at the existing record-count stop point. No new
completion evidence was asserted.

The aggregate snapshot preserves 12 rows, including repeated floor IDs. Its
equivalence to another capture is not independent proof of aggregation correctness.

Raw logs and saved data: `/tmp/recovery-golden-producer-84.I1FGSs`.
The older `local-20260914-LGboor` golden has been retired. This capture is available
as a workflow option in the prepared files but has not yet been compared against
a fresh recovery run.
It is not certified as an interchangeable event-level baseline for HTTP or
adaptive variants. The logical final-state expectation is the same across modes;
transport normalization and producer identity compatibility must be checked.

## Select in GitHub

The SIGKILL building-comfort recovery workflow always runs golden comparison.
`building-comfort-small-v1` is used automatically; there is no golden input.
Both queries and bootstrap off are required until other workload goldens are
enabled. Missing comparison reports fail the workflow. The comparison verdict
remains advisory under the existing completeness/identity limitations; mandatory
execution does not change `enforce: false` or the enforced count/hash checks.

After committing and pushing the workflow, capture code, helper scripts, and
golden files together, open **E2E - building_comfort recovery**, choose **Run
workflow**, and select the branch containing those changes. For a standard-gRPC
run, set:

```text
http_standard: false
http_adaptive: false
grpc_standard: true
grpc_adaptive: false
query_building_comfort: true
query_floor_agg: true
bootstrap_size: off
outbox_capacity: 20000
persist_index: true
state_store: true
drasi_server_repo: ruokun-niu/drasi-server
drasi_server_ref: 6ffbfc0
plugin_registry: ghcr.io/ruokun-niu
plugin_tag: composite-key
```

Leave the release version empty, batching/query tuning at 10000, and timeout at
30 minutes. To run multiple modes, enable any combination of `http_standard`,
`http_adaptive`, `grpc_standard`, and `grpc_adaptive`; all four can be true.
Use a plugin registry/tag containing compatible plugins for all selected modes.
Both queries and bootstrap off remain required because they define this golden's
workload. Dispatcher transport/batching settings are excluded from workload equality;
the source generator, seed, event budget, timing, and query definitions still match.

The helper automatically selects the producer-key identity pointer for gRPC.
For HTTP it uses the HTTP logger's nested payload and a run-local copy of this
baseline with identity fields unavailable. The snapshots and expected event
payloads are unchanged; the original golden is never modified. HTTP delivery
diagnostics remain inconclusive pending compatible producer ID capture, while
the same golden snapshot is checked. Exactly-once and ordered delivery are required by
the comparison policy, with advisory enforcement retained and SHA-256 enforced.
Snapshot and delivery verdicts are shown separately; the overall verdict still
reflects the unchanged completion-evidence limitation. Selecting the older golden
retains its null identity contract and cannot diagnose delivery by producer ID.