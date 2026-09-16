# 100k-Bootstrap Candidate Golden

Standard-gRPC uninterrupted capture with `bootstrap_size: 100k`, both queries,
seed 123456789, persistence, and outbox capacity 20,000. The graph contains 1,000
buildings, 10,000 floors, and 100,000 rooms. The source summary records 321,000
inputs: 221,000 bootstrap events plus 100,000 changes.

| Query | Captured results | Missing producer keys | Repeated producer keys | Snapshot rows |
|-------|-----------------:|----------------------:|-----------------------:|--------------:|
| building-comfort | 195,000 | 0 | 0 | 100,000 |
| building-comfort-floor-agg | 140,000 | 0 | 0 | 100,000 |

Both pre-existing pinned 100k gRPC hashes matched. These are the preset's sample
stop counts, not independently verified terminal totals. `capture.complete`
remains false. The self-comparison passes delivery/state checks and reports
overall inconclusive for completion evidence; it is not a recovery test.

## Reporting Failure

This was not a clean runner exit. The framework completed successfully and wrote
its determinism verdict. The runner captured query snapshots and reaction states,
then encountered a shell parse error before copying the verdict to the top-level
artifact folder. The runner source had been edited during execution; Bash can
read later parts after those edits. The capture wrapper exited 2 on the missing
top-level file.

No workload was rerun. Artifact-only finalization checked the original nested
framework verdict against the saved configuration, source/receiver totals, API
snapshot validity, producer keys, and normalized self-comparison. The reporting
failure is preserved in [provenance](provenance.json) and the
[runner console](runner-console.log). Do not describe it as a clean driver success.

## Files

- [Room snapshot](query_results__building-comfort.json)
- [Aggregate snapshot](query_results__building-comfort-floor-agg.json)
- [Original framework hash verdict](determinism_verdict.json)
- [Producer-key checks](identity-validation.json)
- [Source summary](source_summary.json)
- [Workload](workload.json), [provenance](provenance.json), and [policy](policy.json)
- [Self-comparison](self-comparison.json)
- `golden-actual.json.gz`: compressed normalized events and snapshots; decompress
  before passing it to the comparator.

Raw evidence and the finalization script remain under
`/tmp/recovery-golden-100k-retry-84.3AcgJd`. Test processes were cleaned up. The
initial 600-second startup-timeout attempt remains separate and contributed no
data to this capture.

The aggregate snapshot has 100,000 rows despite 10,000 floors. The existing
repeated-floor-ID question remains unresolved; all rows are preserved. Matching
this capture establishes equivalence, not independent aggregate correctness.
This candidate is not yet a GitHub dropdown option and has not been compared
against a separate recovery run.