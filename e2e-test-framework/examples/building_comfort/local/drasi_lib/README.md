# Building Comfort &mdash; Internal drasi-lib instance

End-to-end test that runs **drasi-lib in-process** inside the E2E test
service. No external Drasi Server is required.

## What this test does

1. The test service generates change events for a `BuildingHierarchy` model
   (Building &rarr; Floor &rarr; Room with `temperature` / `humidity` /
   `co2` sensor properties).
2. Events are delivered via an **in-process channel** directly to a
   drasi-lib instance hosted by the test service (`source_change_dispatchers
   .kind = "DrasiLibInstanceChannel"`).
3. The drasi-lib instance evaluates the `all-rooms` projection and `floor-agg`
   traversal/aggregation queries, and pushes results back to
   the test service over the same in-process channel
   (`output_handler.kind = "DrasiLibInstanceChannel"`).
4. The generator is configured for **100,000** changes. Results are written
   through JSONL and performance loggers. The example's reaction stop thresholds
   are **95,000** room records and **45,000** floor records; these are output
   thresholds, not proof that every input has finished processing.

```
test-service ── in-process channel ──> drasi-lib ── in-process channel ──> test-service
   (Model source data generator)        (queries + reactions)              (loggers)
```

This is the lowest-friction way to exercise drasi-lib end-to-end: there is
no network hop, no separate process to start, and no plugin install step.

## Configuration highlights

- `data_store.test_repos[0].local_tests[0].drasi_lib_instances[0]`: the
  embedded drasi-lib instance, with one source (`facilities-db`,
  `kind: application`), two queries, and two application reactions.
- `data_store_path: "./test_data_cache"` and `source_path: "./dev_repo"`
  are resolved relative to this folder, so paths stay valid when the test
  is launched from inside the folder.

## Prerequisites

- The sibling `drasi-core` checkout; `test-run-host` uses its local path
  dependencies. No runtime feature opt-in is required.
- This repository buildable via `cargo build --locked --release`.
- No external services.

## Embedded runtime

ComputationGraph is the only embedded runtime. The checked-in configuration
uses it without an engine override:

```json
{
  "test_drasi_lib_instance_id": "internal-drasi-lib",
  "start_immediately": true
}
```

This object belongs in `test_run_host.test_runs[0].drasi_lib_instances`.
The existing application source/reaction implementations remain in use; no
Drasi Server or dynamic plugins are loaded. The removed
`test_run_overrides.execution_mode` setting is rejected for all values, including
`componentGraph`, `computationGraph`, and null. Omit it rather than using an old
runtime label for a new benchmark. Runtime overrides support `log_level` only.

Confirm the live instance with:

```bash
curl http://localhost:63123/api/test_runs/drasi_lib_dev_repo.building_comfort.test_run_001/drasi_lib_instances/internal-drasi-lib/runtime
```

The response includes the fixed informational `execution_mode: "computationGraph"`
identifier and the actual embedded DrasiLib instance's `running` status.
For performance comparisons, finish builds first, warm up, repeat runs, and use
identical logging and workload settings. Historical ComponentGraph measurements
must identify the assessed commit; that engine is not selectable here.
The supplied local example and the gRPC example have different
generator intervals, query aliases, and stop thresholds; align those explicitly
when comparing the same workload across hosting modes.

## Run the test

All commands assume you are in this folder.

### Option A: helper script

```bash
./run_test.sh
```

Use `./run_test_debug.sh` for verbose tracing.

#### Optional: per-reaction SHA-256 determinism check

Set `SHA_CHECK=1` to attach a `DeterminismHash` output logger to each
reaction and a `Sha256Determinism` completion handler (with an empty
baseline and `missing_baseline: "Warn"`) to the test definition. The
run then prints the per-reaction SHA-256 in the framework log without
failing the test:

```bash
SHA_CHECK=1 ./run_test.sh
```

Requires `jq` on `PATH`. SHA stability across hosts is not guaranteed
for the embedded drasi-lib variant (see the
[`ci/drasi_lib`](../../ci/drasi_lib) README for details); two
consecutive runs on the same machine should match.

### Option B: cargo run directly

The script is just a wrapper around `cargo run` against the workspace's
`test-service` crate. The equivalent invocation from this folder is:

```bash
cargo run --release \
  --manifest-path ../../../../test-service/Cargo.toml \
  -- --config config.json
```

Tune `RUST_LOG` to control log verbosity, e.g.:

```bash
RUST_LOG="off,test_run_host=info,test_run_service=info,test_data_store=info" \
  cargo run --release \
    --manifest-path ../../../../test-service/Cargo.toml \
    -- --config config.json
```

## Inspect / control while running

The test service exposes a REST API on `http://localhost:63123` by default. The
`web_api_drasi_lib_instance.http`, `web_api_source.http`,
`web_api_query.http`, and `web_api_reaction.http` files in this folder
contain ready-to-run requests for VS Code's REST Client extension (or
`curl`).

## Output

- `./test_data_cache/` &mdash; transient test data store; cleared on each
  run (`delete_on_start: true`, `delete_on_stop: true`).
- JSONL output for the `building-comfort` reaction is written under the
  test data cache by the `JsonlFile` output logger
  (`max_lines_per_file: 15000`).
- Per-query performance metrics are emitted by the `PerformanceMetrics`
  output logger.
