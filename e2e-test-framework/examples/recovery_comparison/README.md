# Recovery Artifact Comparison

An opt-in framework completion handler and standalone Rust artifact comparator
for test-infra #84. Both share the same comparison engine. Baseline storage and
producer-side completion/identity instrumentation are separate work; existing
ordered SHA-256 checks remain available and unchanged.

## Run the Example

From `e2e-test-framework`:

```bash
cargo run --locked -p test-run-host --bin recovery-compare -- \
  examples/recovery_comparison/baseline.json \
  examples/recovery_comparison/recovered.json \
  examples/recovery_comparison/policy.json
```

The synthetic recovery fixture has one duplicate and reordered delivery. Its
explicit at-least-once, reorder-permitting policy passes while still reporting
both conditions. Changing the policy to `exactly_once` or disallowing reordering
makes that fixture fail. Policy describes required behavior for this comparison;
choosing it does not establish the transport's actual delivery guarantee.

Exit codes: `0` passed, `1` failed comparison, `2` inconclusive or invalid input.
Valid comparisons write a JSON report to stdout. Invalid input writes a diagnostic
to stderr without a report; shell callers must check the exit code, not merely
whether a report file exists. Build/run errors from Cargo have Cargo's exit codes.

## Framework Configuration

Add [framework-handler.json](framework-handler.json) to the test definition's
`completion_handlers` array alongside existing `Log`/`Sha256Determinism` handlers.
Replace its fingerprint and baseline placeholders with values for your workload.
This is a handler definition, not a complete test-service configuration file.

For each configured `test_reaction_id`, enable the existing logger in the runtime
reaction configuration's `output_loggers` array:

```json
{"kind": "JsonlFile", "max_lines_per_file": 10000}
```

No separate binary invocation is needed for configured framework runs. On normal
test completion the handler:

1. Reads finalized `JsonlFile` logger results from the completion summary. Exactly
  one logger and one distinct reaction per configured query are required. Chunk
  files are numerically ordered and missing/repeated chunks are rejected.
2. Reads a snapshot from `snapshot_path`, or fetches `snapshot_url` with a ten-second
  request timeout. These options are mutually exclusive. API responses must
  contain `success: true` and an array in `data`. Missing snapshot configuration
  leaves state inconclusive; network/API errors are invalid comparisons.
3. Writes `recovery_capture.json` and `recovery_actual.json`, then compares the
  actual artifact to `baseline_path` with the configured policy.
4. Writes `recovery_verdict.json`, including the run ID, enforcement mode, and full
  comparison report (or an explicit `invalid` verdict and diagnostic).

Files are written under the test run's storage directory. A relative baseline path
is resolved against the cached test definition directory, so it can be shipped
inside a local/GitHub/Azure-backed test repository. Absolute baseline paths are
also accepted. Relative snapshot/evidence paths are resolved under the run's
storage directory; they are not relative to the test definition. HTTP snapshot
responses are retained as `recovery_snapshot_<index>.json`.

`enforce` defaults to `true`: failed, inconclusive, and invalid comparisons return
a completion-handler error, which the existing monitor propagates to
`TestRunStatus::Error`. The sample deliberately uses `enforce: false` for advisory
rollout alongside SHA-256. Advisory comparisons still record their real non-pass
verdict and warn; they do not relabel it as a pass or override other handlers'
failures. An inability to write the verdict file remains an error in either mode.

The handler runs only when the existing completion monitor invokes handlers.
Explicit stop paths that do not invoke completion handlers, a killed framework,
or an external workflow timeout do not acquire a comparison verdict automatically.
No verdict must never be interpreted as success.

### Completion Evidence

Logger finalization is necessary to read files but is not proof that all producer
output was captured. Without a `capture_evidence_path` file the handler marks
the capture incomplete. Component errors or prematurely stopped sources also
override any completion claim. A test controller that actually verifies the
terminal output boundary must write evidence before allowing test completion:

```json
{
  "test_run_id": "repo.test.run",
  "recorded_at_ns": 1789425000000000000,
  "complete": true,
  "evidence": "Describe the verified producer/output boundary and snapshot consistency"
}
```

Use the real run ID and current UTC Unix time in nanoseconds, not these sample
values. Timestamps before handler construction or in the future are rejected,
as are run-ID mismatches. This guards accidental stale reuse; it does not prove
the controller's claim. Do not generate `complete: true` from receiver count,
quiet output, source ingress, health, or log flushing alone. File snapshots must
belong to this same terminal boundary; URL snapshots require a stable/quiescent
query at that boundary. The handler does not pause Drasi to establish consistency.

The legacy gRPC example retains null identity pointers/contracts for compatibility
with the saved golden, which has no producer metadata. Fresh framework captures
now preserve the existing wire fields as described below. A new compatible
baseline is required before enabling event-level comparison; old captures cannot
be repaired by substituting receiver-local sequence numbers.

### gRPC Producer Metadata

Rebuilt test-service captures these fields under `payload.headers` in the existing
`JsonlFile` records (capture metadata, not newly added wire headers):

- `x-drasi-producer-query-id`: query ID from the wire batch.
- `x-drasi-producer-sequence`: query output sequence, as an exact decimal string.
- `x-drasi-producer-row-signature`: row signature, as an exact decimal string.
- `x-drasi-producer-item-type`: numeric wire operation enum, as a string.
- `x-drasi-producer-key`: JSON tuple of query ID, sequence string, row signature
  string, and numeric operation. Absent when sequence is zero (unavailable).

These fields survive repeated delivery and batch regrouping; receiver invocation
IDs remain separate. Unary and streaming gRPC paths share this capture code.
The semantic request body is unchanged, so existing SHA-256 baselines do not
change. No drasi-core, drasi-server, or plugin rebuild is required for the tested
protocol; only rebuild test-service.

For a workload with at most one diff per query/sequence/row/operation tuple and
stable cross-run sequences/signatures, use the following in both capture configs:

```json
{
  "identity_contract": "grpc-query-sequence-row-operation-v1",
  "identity_pointer": "/payload/headers/x-drasi-producer-key"
}
```

Use `delivery: exactly_once` and `allow_reordering: false` for these recovery
tests. First verify tuple uniqueness in a fresh uninterrupted baseline. If the
producer legitimately emits the same tuple twice, the comparator rejects that
baseline; do not add a receiver counter or batch offset to hide the collision.
That workload needs a finer producer-side diff identity. Identical payloads on
different rows/sequences remain distinct. Payload values are never part of the
key, so conflicting values for the same key remain detectable.

The saved `local-20260914-LGboor` baseline and its workflow selection deliberately
keep null identity contracts until recaptured. This metadata change does not
change completion checks or turn the existing candidate golden into a complete
capture. The payload hash gate remains independent.

## Artifact Contract

[baseline.json](baseline.json) and [recovered.json](recovered.json) illustrate
schema version 1. Unknown fields are rejected. Each artifact declares:

- `workload_fingerprint`: matching identity of the input data, bootstrap, script,
  timing assumptions, selected queries, and comparison normalization contract.
  Generate it from canonical configuration/data digests in a future capture layer;
  do not reuse an arbitrary label across different workloads.
- `capture.complete` and `capture.evidence`: whether a verified terminal delivery
  boundary and corresponding final snapshot were captured, with a description of
  that evidence. These are caller assertions, not facts inferred by this tool.
  Quiet counts, server health, source ingress completion alone, a timeout, and
  fixed-count observers that can miss later outputs do not prove complete capture.
  Set `complete: false` when evidence is insufficient. The overall verdict then
  stays inconclusive, even if individual observed-artifact checks match or differ.
- `queries`: one entry per query; duplicate or empty query IDs are invalid.
  Missing and extra query entries are reported independently.
- `config_fingerprint`: compatible query definition, grouping/join semantics, and
  logical configuration epoch. It must match across baseline and recovery. Do not
  use a fresh process ID as the logical epoch, or conflate a reconfigured query
  with a restarted instance of the same query.
- `identity_contract`: the declared cross-run producer identity scheme, or null
  when unavailable. Different contracts are rejected, not guessed compatible.
- `events`: observed order, each with `identity` and the exact semantic `payload`.
  The baseline must have one event per identity. Recovery may repeat identities.
- `snapshot`: complete final rows, or null when unavailable. An empty array is a
  known empty state, not a substitute for a failed/missing snapshot request.

The fixtures use stable synthetic `sequence:result-index` identities. Real query
output sequences are usable only when their correspondence is stable between
these runs; batching/coalescing or nondeterministic query evaluation can break
that assumption. A sequence identifies an emission, not necessarily a single
diff, so include a stable result index or logical ID if an emission has multiple
results. A row signature identifies a row, not every successive update to it.
Never use receiver-local counters or payload hashes as producer identities.

Payloads are compared exactly as structured JSON, ignoring object-key order but
preserving array order, numeric representation, and business fields. No recursive
stripping of `timestamp` or other field names occurs. Transport adapters must
explicitly select the semantic payload and apply a shared normalization contract
if envelopes differ. Legitimately identical payloads with distinct event IDs are
not duplicates.

## Report Semantics

Delivery and state have separate verdicts:

| Field | Meaning |
|-------|---------|
| `missing` | Baseline identities not observed in recovery; not proof of permanent source-data loss |
| `unexpected` | Identities observed only in recovery |
| `duplicates` | Extra observations per identity beyond its first occurrence |
| `conflicting` | Same identity with a different baseline payload or inconsistent repeated payloads |
| `reordered` | Order of first occurrences of shared identities differs; null if identity unavailable |
| State `missing` / `unexpected` | Full row values and multiplicity differences |

Identical redelivery is permitted only by `at_least_once`. Conflicts and unexpected
IDs always fail. Reordering is governed independently by `allow_reordering`.
Repeated delivery positions are not separately order-checked: first-occurrence
order is the documented criterion. Missing IDs remain failures regardless of
duplicate counts or reordering policy.

Snapshots are multisets of complete canonical JSON rows. Row order is irrelevant,
but multiplicity is not: two equal rows remain two rows. No last-row-wins map by
`FloorId`, `RoomId`, or another assumed unique key is built. Changed rows appear
as one missing value and one unexpected value; semantic row identity and deeper
field-level localization are future work. Final state is not reconstructed by
folding possibly reordered delivery events.

Unknown event identities make delivery inconclusive. Missing snapshots make state
inconclusive. With complete captures, known failures take precedence over unknown
checks; otherwise all checks must pass for an overall pass. The report diagnoses
saved observations; it does not prove general exactly-once behavior or exclude
events arriving beyond the asserted capture boundary.

## Import Existing JSONL Logs

`--import` converts a file manifest into the normalized artifact format. Paths are
relative to the manifest (absolute paths also work). Files are consumed in the
explicit listed order; glob expansion and numeric chunk sorting belong to the
caller. Duplicate file paths are rejected. Missing files, malformed JSONL,
missing payloads, wrong query IDs, and unsuccessful snapshot responses are errors.
Snapshot files must contain the API shape `{"success":true,"data":[...]}`.

Example manifest for existing per-query gRPC logger files:

```json
{
  "schema_version": 1,
  "workload_fingerprint": "replace-with-verified-workload-digest",
  "capture": {
    "complete": false,
    "evidence": "Legacy fixed-count receiver; no verified terminal output boundary"
  },
  "queries": [{
    "query_id": "building-comfort",
    "config_fingerprint": "replace-with-verified-query-config-digest",
    "identity_contract": null,
    "identity_pointer": null,
    "query_id_pointer": "/payload/request_body/query_id",
    "payload_pointer": "/payload/request_body/result",
    "event_files": ["outputs_00000.jsonl", "outputs_00001.jsonl"],
    "snapshot_file": "query_results__building-comfort.json"
  }]
}
```

```bash
target/debug/recovery-compare --import baseline-capture.json > baseline.json
target/debug/recovery-compare --import recovery-capture.json > recovery.json
target/debug/recovery-compare baseline.json recovery.json \
  examples/recovery_comparison/policy.json > report.json
```

The importer does not split raw batches or infer event IDs. One JSONL record must
represent one comparison event at the declared granularity. When stable producer
IDs are actually captured, specify their JSON pointer and a matching identity
contract. IDs must be strings or unsigned integers; the importer encodes these
with JSON type information to avoid conflating numeric `1` with string `"1"`.
Absent/null IDs remain unavailable, never default to receiver sequence numbers.

Existing CI gRPC artifacts lack producer IDs and an independently verified final
delivery boundary. Importing them can diagnose final-state differences but cannot
produce a complete recovery pass. In run 34898271191, both saved query snapshots
matched as full row multisets while delivery and overall comparison correctly
remained inconclusive. Existing strict hashes remain the established CI gate.

Artifacts are loaded in memory; the CLI and framework handler are intended for
bounded comparisons, not streaming multi-million-event captures. Reports list full
differences and contain query data; handle them like the source artifacts.

## Tests

```bash
cargo test --locked -p test-run-host --lib recovery_
cargo test --locked -p test-run-host --test recovery_compare_cli
```

Remaining work: recapture and validate producer-key uniqueness for real workloads,
establish verified terminal boundaries, configure workflows, and establish baseline
storage/versioning before replacing existing CI gates.