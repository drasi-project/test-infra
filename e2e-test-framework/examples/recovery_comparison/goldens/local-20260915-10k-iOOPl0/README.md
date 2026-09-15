# 10k-Bootstrap Golden Capture

Uninterrupted standard-gRPC capture with `bootstrap_size: 10k`, seed 123456789,
both building-comfort queries, persistent index/state store, and outbox capacity
20,000. The input graph contains 100 buildings, 1,000 floors, and 10,000 rooms.
The generator sends 22,100 bootstrap events followed by 100,000 changes.

| Query | Captured results | Missing producer keys | Repeated producer keys | Snapshot rows |
|-------|-----------------:|----------------------:|-----------------------:|--------------:|
| building-comfort | 105,000 | 0 | 0 | 10,000 |
| building-comfort-floor-agg | 50,000 | 0 | 0 | 10,000 |

Both pre-existing pinned 10k gRPC hashes passed. These counts are the preset's
sample-stop thresholds, not independently verified terminal output totals. The
capture is a **candidate stop-point golden**, not proof that all query processing
had finished. `capture.complete` remains false; the self-comparison has passed
delivery/state subchecks and an overall inconclusive verdict. Self-comparison is
only an artifact check, not a recovery test.

## Saved Artifacts

- [Room snapshot](query_results__building-comfort.json)
- [Aggregate snapshot](query_results__building-comfort-floor-agg.json)
- [Pinned hash verdict](determinism_verdict.json)
- [Producer key validation](identity-validation.json)
- [Workload](workload.json) and [provenance](provenance.json)
- [Strict policy](policy.json) and [self-comparison](self-comparison.json)
- `golden-actual.json.gz`: normalized capture with the producer identity contract
  `grpc-query-sequence-row-operation-v1`. Decompress before passing to the comparator.

Raw logs and data are retained under `/tmp/recovery-golden-10k-84.iOOPl0`.
The aggregate snapshot contains 10,000 rows despite there being 1,000 floors;
the existing repeated-floor-ID issue remains unresolved. Rows and multiplicity
are preserved, not collapsed into one row per floor. Matching a later recovery
snapshot verifies equivalence to this capture, not independent aggregate accuracy.

The snapshot is distinct from the small 12-room golden. Do not compare a 10k
workload against that smaller baseline. This capture has not yet been wired into
the GitHub dropdown or compared against a separate recovery capture. No existing
golden or pinned hash was overwritten to make the capture pass.