# Scripted Pause Comparison (Local)

## Current Scenario: 12,000 Changes

The Bash runner starts Drasi Server and test-service for two fresh runs of the
same repository-resident JSONL scripts: first with inline pauses ignored, then honored.
By default there is no crash injection. Two opt-in restart variants are described
below. This is a correctness check, not a throughput benchmark.

```text
File 1: Header -> inserts 1..4000 -> PauseCommand "after-4000"
File 2: inserts 4001..8000 -> PauseCommand "after-8000"
File 3: inserts 8001..12000 -> Finish
```

The input files live in this example and are shared by both runs:

- [changes_000.jsonl](dev_repo/pause_comparison/sources/script-db/source_change_scripts/changes_000.jsonl)
- [changes_001.jsonl](dev_repo/pause_comparison/sources/script-db/source_change_scripts/changes_001.jsonl)
- [changes_002.jsonl](dev_repo/pause_comparison/sources/script-db/source_change_scripts/changes_002.jsonl)

They are not regenerated at runtime. `generate_scripts.sh` is retained for
reproducibility checks. Only run outputs, caches, and server data go into temporary
artifact directories. The only scenario setting changed between runs is
`ignore_scripted_pause_commands`. Headers/pauses/finish are additional script
records, not part of the 12,000 source changes or expected query results.

From `e2e-test-framework`:

```bash
cargo build --locked -p test-service
bash examples/scripted_recovery/run_compare.sh \
  --drasi-server-bin /absolute/path/to/drasi-server
```

For trusted locally rebuilt plugins only, add `--allow-local-plugins`. Requires
Bash, jq, curl, lsof, shasum, and compatible gRPC source/reaction plugins beside
the server binary. `TEST_SERVICE_BIN` and `DRASI_SERVER_BIN` can supply paths.

The paused run verifies the next input is 4,001 or 8,001, waits for the receiver
and query snapshot to reach exactly 4,000 or 8,000, then holds the pause for
`PAUSE_SECS` (default 5). During that hold it checks the cursor and count stay
unchanged. `/start` resumes playback. Drasi stays running throughout each run.

Each run must produce 12,000 inputs, 12,000 receiver invocations, and exactly one
query row and delivered ADD result for each ordinal 1..12000. Final snapshots and
delivered identities are compared across runs. Counts alone cannot hide a missing
record offset by a duplicate. Delivery order is not a pass/fail criterion.
Observers have no fixed-count stop trigger; after expected rows/counts are reached,
the receiver is explicitly stopped to flush logs. This bounded test cannot rule
out arbitrarily late extra output or prove general exactly-once recovery.

Defaults, configurable through environment variables:

| Variable | Default |
|----------|---------|
| `SERVICE_PORT` | 63125 |
| `ADMIN_PORT` | 8092 |
| `SOURCE_PORT` | 50063 |
| `REACTION_PORT` | 50064 |
| `TIMEOUT_SECS` | 300 per wait |
| `PAUSE_SECS` | 5 |

Each run has fresh persisted data and saved logs, snapshots, cursors, input and
delivery ordinals, and verdicts under the printed comparison directory.
Binary SHA-256 fingerprints are saved. Shutdown uses TERM, with KILL as a cleanup
fallback for a child that does not exit. Only the opt-in restart variants inject
SIGKILL during the test.

Validation on 2026-09-09: uninterrupted and paused runs both passed with 12,000
results against the existing local Drasi build. This does not validate the pending
recovery PRs. No new GitHub workflow is added yet.

```bash
bash examples/scripted_recovery/test_compare.sh
cargo test --locked -p test-run-host script_source_change_generator --lib
```

## Restart Variants

Each invocation first runs the uninterrupted baseline on fresh data, then the
selected variant on its own fresh data. The existing `--variant pause` is the
default. The same 12,000-change files are used in all cases.

| Variant | First pause, after input 4,000 |
|---------|--------------------------------|
| `pause` | Wait for 4,000 rows and deliveries, hold for `PAUSE_SECS`, resume without restarting |
| `restart-caught-up` | Wait for 4,000 rows and deliveries, complete the hold, then SIGKILL/restart Drasi |
| `restart-immediate` | SIGKILL/restart when the script pause is observed, before waiting for rows/deliveries or holding |

The second pause at 8,000 remains a normal hold/resume in all variants; there is
only one injected restart. "Immediate" means no deliberate catch-up wait or hold:
API polling, cursor validation, and scheduling still introduce some delay.

```bash
bash examples/scripted_recovery/run_compare.sh \
  --variant restart-caught-up \
  --drasi-server-bin /absolute/path/to/drasi-server

bash examples/scripted_recovery/run_compare.sh \
  --variant restart-immediate \
  --drasi-server-bin /absolute/path/to/drasi-server
```

Add `--allow-local-plugins` only for trusted locally rebuilt plugins. Neither
variant restarts test-service or the generator. Drasi is relaunched from the same
working directory using the same binary, saved config, and data, without deleting
storage or reapplying components. After health returns, the runner verifies the
generator cursor is unchanged and waits for exactly the first 4,000 rows and
deliveries **before** resuming input 4,001. The run then completes all 12,000 inputs
and compares results against its uninterrupted baseline.

`restart-caught-up` targets preservation of completed work and continuation.
`restart-immediate` attempts to exercise pending work, but **a pause does not prove
that query work was pending when the process died**. A successful result comparison
sets `passed: true` for the result checks, while `pending_work_coverage: "unverified"`
and `pending_query_work_confirmed: null` explicitly prevent a claim of verified
pending-query recovery. The console also reports inconclusive coverage. Review
source checkpoints and replay evidence before claiming that coverage; a receiver
shortfall alone could be pending delivery rather than pending query processing.

The variants retain the existing strict counts and one-ADD-per-ordinal checks.
Duplicates therefore fail this comparison; that is a diagnostic mismatch, not by
itself proof of data loss or a violation of an at-least-once delivery contract.
The snapshot and delivery paths remain separate checks. General duplicate-aware
recovery validation is still future work.

Artifacts are stored under `uninterrupted` and `restart-caught-up` or
`restart-immediate` (the default remains `paused`). The variant directory also
contains `crash.json`, `crash-restarted.json`, `generator-after-restart.json`,
`snapshot-restored.json`, and `reaction-recovered.json` when those stages complete.
Server logs are appended across restart with a clear boundary. Default query and
reaction debug logging helps diagnose sequence/checkpoint behavior; override with
`DRASI_RUST_LOG` as needed.

Validation on 2026-09-14: **both restart variants passed against the local
codec-fixed Drasi Server build**, each with a fresh uninterrupted baseline.
All four runs produced exactly 12,000 inputs, deliveries, and query rows, with
matching final rows and delivered identities. Both restarts restored output
sequence 4,000 and 4,000 live rows before continuing at input 4,001.

Both runs replayed zero source events, including `restart-immediate`: the query
had already checkpointed all pre-pause inputs. This validates saved-state
restoration and continuation, not pending-query replay coverage. The insert-only
scenario does not directly exercise the Update codec regression. See the
[progress log](../../../docs/recovery-testing-progress.md) for binary provenance,
artifact paths, and coverage limits. Bash syntax and mocked
routing/process-lifecycle/cursor tests also passed previously.

## GitHub Recovery Workflow

The manual [PauseCommand recovery workflow](../../../.github/workflows/e2e-pause-command-recovery.yml)
supports `all`, `restart-caught-up`, and `restart-immediate`. `all` runs only the
two pause/restart variants, each on its own runner using the same binaries from
one preparation job. Building-comfort SIGKILL testing has its own
[building-comfort recovery workflow](../../../.github/workflows/e2e-building-comfort-recovery.yml),
separate from both this workflow and the building-comfort throughput workflow.

Server inputs match building-comfort:

- `drasi_server_repo`: GitHub `owner/name` to build from source.
- `drasi_server_ref`: branch, tag, or SHA. A ref alone uses
  `drasi-project/drasi-server`; a repo alone uses its default branch.
- `drasi_server_version`: release tag, used only when repo and ref are empty;
  leaving all three empty downloads the latest release.
- `plugin_registry` and `plugin_tag`: optional OCI plugin overrides. Plugins
  are automatically installed in CI with verification enabled.
- `timeout_minutes`: maximum per test wait, excluding builds.

The server checkout determines its core dependencies; this workflow does not
inject the locally tested core revision or build plugins from core source.
Select a server branch containing the required recovery dependencies and a
compatible published plugin registry/tag. A successful build alone does not
establish that the recovery fixes are present. The build artifact records the
server commit and resolved Cargo lockfile for source builds, plus binary hashes.

The building-comfort recovery workflow provides the standard building-comfort
inputs for HTTP/gRPC standard/adaptive variants, query selection, `batching_speed`,
`query_tuning`, and `bootstrap_size`. Each selected variant runs only SIGKILL
recovery after ingress, with zero extra crash delay and no component reapplication.
Final output is checked against the existing committed count/hash expectations;
the workflow does not regenerate a golden run. An opt-in
[recovery comparator and completion handler](../recovery_comparison/README.md) now
implement separate delivery/state diagnostics. The framework supports
`kind: RecoveryComparison`, but this workflow has not enabled it by default:
producer identity and verified boundary capture are still needed. Reusable
golden-run storage remains part of issue #84. Baselines must match
the workload; missing baselines for custom presets are not correctness evidence.
Select either saved `golden_snapshot` to enable candidate golden comparison in
advisory mode. All four HTTP/gRPC standard/adaptive variants can be selected
together. Both queries and bootstrap off are required for these captures.
The workflow validates the logical workload independently of dispatcher settings, uses the stored
capture without rerunning the golden, and reports snapshot/delivery verdicts in
the job summary. Its policy rejects duplicates and reordering; SHA-256 remains
enforced while missing producer/boundary evidence keeps the comparator overall
inconclusive. See the [golden run inputs](../recovery_comparison/goldens/local-20260914-LGboor/README.md).
Every mode compares against the same full snapshot rows. HTTP captures use their
own payload path and report delivery identity unavailable; the new producer-aware
golden enables gRPC delivery diagnostics. HTTP snapshot comparison does not invent
producer IDs or relax the existing strict hash gate. Cross-mode configuration and
import tests pass; live recovery success is not implied by exposing these options.
`persist_index` and `state_store` default to true
and must remain enabled. Embedded `drasi_lib` is omitted because this recovery
runner kills an external server. Empty variant/query selections fail validation.

The recovery-only `outbox_capacity` input defaults to `1000` and accepts a
positive integer. It sets `outboxCapacity` on every selected query in the
recovery run, independently of `query_tuning`. Use `20000` for the
larger-outbox comparison after a Strict reaction outbox-gap failure; a passing
comparison is retention evidence, not proof that startup ordering is fixed.
The runner logs the applied capacity. Local runs can set `OUTBOX_CAPACITY`;
when it is unset, existing query configuration and server defaults are preserved.

HTTP standard and gRPC standard are selected by default, matching the standard
workflow's external variants. Select only gRPC when using a registry/tag with
only gRPC plugins published. Each variant requires compatible source/reaction
plugins for its protocol. Exposing an option does not establish recovery coverage
for it; standard gRPC passed on GitHub with outbox capacity 20,000, but the other
variants have not been validated by that run. Large bootstrap
presets retain the existing runner's baseline policy and hosted-runner resource
limits; the 1m preset previously exceeded CI memory capacity.

These building-comfort configuration inputs do not affect PauseCommand. Its variants use
the fixed 12,000-change configuration and their own fresh uninterrupted baseline;
`pause_seconds` controls their hold duration. Both numeric time inputs accept
positive integers up to 60.

Artifacts are uploaded on success or failure and retained for 14 days, including
logs, verdicts, saved state, and snapshots. Immediate-restart pending-work coverage
remains unverified unless checkpoint/replay evidence establishes it.

Status: split workflow files prepared and locally validated; not published or run on GitHub.
Only `workflow_dispatch` is configured. GitHub normally requires this workflow
on the default branch before it can be manually dispatched; no temporary push
trigger is included.

## Earlier Four-Change Prototype

The following describes the retained Python prototype, not the recommended
12,000-change Bash scenario above. Its small checked-in script is also used by
the Rust regression tests and is intentionally unchanged.

A small scenario for pausing the test-framework generator at a known script
position, optionally restarting Drasi Server, and resuming from the next change.
It is separate from the building-comfort 100k drain test.

## Scenario

```text
Insert 1 -> Insert 2 -> Pause "before-crash"
   -> optional SIGKILL/restart of Drasi Server only
   -> resume -> Insert 3 -> Insert 4 -> Pause "after-resume" -> finish
```

The generator runs inside test-service, which stays alive while Drasi restarts.
This does not test recovery of the generator process or persistence of its cursor.
`PauseCommand` is an indefinite pause, not a timed delay; `/start` resumes it.
`/stop` closes dispatchers and requires `/reset`, which rewinds the script.

## Run Locally

From `e2e-test-framework`, with Rust and Python 3 available:

```bash
cargo build --locked -p test-service
python3 examples/scripted_recovery/run_local.py --mode framework
```

Framework mode needs no Drasi Server. It uses the real REST API and JSONL
dispatcher to verify the pause cursor and exactly one dispatch of each input.

For a clean server run, provide a Drasi binary with compatible gRPC source and
reaction plugins installed beside it:

```bash
python3 examples/scripted_recovery/run_local.py --mode clean \
  --drasi-server-bin /absolute/path/to/drasi-server
```

Only for trusted locally rebuilt plugins whose registry lockfile no longer
matches, add `--allow-local-plugins`. This disables plugin verification for this
run; verification remains enabled by default. The runner does not install plugins.

After the recovery PR stack has merged and the server/plugins have been rebuilt:

```bash
python3 examples/scripted_recovery/run_local.py --mode crash \
  --drasi-server-bin /absolute/path/to/drasi-server
```

Always run `clean` on that same build first. The crash mode is prepared but has
not yet been validated against the merged recovery fixes. No GitHub Actions
workflow is added in this first local version.

`TEST_SERVICE_BIN` and `DRASI_SERVER_BIN` may supply the binary paths instead.
The default test-service binary is `target/debug/test-service`.

| Option | Default |
|--------|---------|
| `--service-port` | 63124 |
| `--admin-port` | 8091 |
| `--source-port` | 50061 |
| `--reaction-port` | 50062 |
| `--timeout` | 60 seconds per wait |

Occupied or duplicate ports fail preflight; choose another port with the flags.
Each run creates and prints a new temporary directory. It retains configuration,
logs, state, and results, and shuts down only the child processes it started.
`DRASI_RUST_LOG` and `TEST_SERVICE_RUST_LOG` override logging filters.

## Checks and Limits

- At the first pause, inputs are exactly `[1, 2]`, and the next script input is 3.
- Server modes wait for the first two query rows and their reaction deliveries
  before proceeding. This initial scenario tests preserved processed state, not a
  guaranteed backlog of unprocessed source events.
- Crash mode SIGKILLs the server, restarts the same binary/configuration in the
  same directory without deleting data or reapplying components, and checks that
  the generator cursor and the first two query rows remain unchanged.
- After resume, input dispatch must be exactly `[1, 2, 3, 4]`.
- The final server snapshot must contain exactly one row per ordinal 1..4. Empty,
  incomplete, duplicate, or extra rows cannot pass. Row order is irrelevant.
- The receiver must observe each expected ordinal. Clean mode requires the exact
  delivery sequence `[1, 2, 3, 4]`. Crash mode reports duplicate deliveries without
  failing solely for redelivery; this is a scenario-level no-missing-results
  check, not a claim that gRPC provides durable/exactly-once delivery.
- No fixed-count observer stop trigger or quiet-count cutoff is used as proof of
  catch-up. A timeout fails the run. This small test does not replace a general
  recovery oracle or prove all recovery paths correct.

Inspect `verdict.json`, `generator-*.json`, `snapshot-*.json`, and both process
logs in the printed artifact directory. `run.json` records binary paths and SHA-256
fingerprints; also record your core/server/plugin commits when validating a new
build. Existing plugin lockfile verification warnings are retained in server logs.

## Focused Tests

```bash
cargo test --locked -p test-run-host script_source_change_generator --lib
python3 -B -m unittest discover -s examples/scripted_recovery -p 'test_*.py'
```

The Rust tests use this scenario's checked-in script and a recording dispatcher.
They cover pause/resume, ignored pauses, and stop/reset. Python tests cover result
validation and port checks. The local framework and clean gRPC modes were exercised
successfully on 2026-09-09; crash validation remains deferred until the fixes merge.