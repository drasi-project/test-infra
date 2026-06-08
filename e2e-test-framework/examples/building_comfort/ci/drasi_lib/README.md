# Building Comfort &mdash; Embedded drasi-lib instance (CI)

End-to-end test that runs **drasi-lib in-process** inside the E2E test
service. No external Drasi Server is required. This is the CI variant of
[`local/drasi_lib`](../../local/drasi_lib) and mirrors the
[`ci/drasi_server_http`](../drasi_server_http) and
[`ci/drasi_server_grpc`](../drasi_server_grpc) variants; the difference
is that here drasi-lib runs in-process via an `application` source/reaction
instead of going over HTTP or gRPC.

## What this test does

1. The test service generates change events for a `BuildingHierarchy`
   model (1 building × 1 floor × 1 room; rooms carry
   `temperature` / `humidity` / `co2`).
2. Events are delivered via an **in-process channel** directly to a
   drasi-lib instance hosted by the test service
   (`source_change_dispatchers[].kind = "DrasiLibInstanceChannel"`).
3. The drasi-lib instance evaluates a Cypher query
   (`all-rooms`: `MATCH (r:Room) RETURN ...`) and pushes results back to
   the test service over the same in-process channel
   (`output_handler.kind = "DrasiLibInstanceChannel"`).
4. The test service stops the reaction once **99,000** events have been
   observed (`stop_triggers.RecordCount`).

```
test-service ── in-process channel ──> drasi-lib ── in-process channel ──> test-service
   (Model source data generator)       (queries + reactions)               (loggers)
```

## CI vs local

- **Determinism check is on**: `config.json` declares a
  `Sha256Determinism` completion handler. On first run, leave
  `expected: {}` and `missing_baseline: Warn` so the framework just logs
  the actual SHA. Once you have a trusted baseline, copy the SHA from
  the run's `DeterminismHash` logger summary into the `expected` map and
  flip `missing_baseline` to `Fail`.
- **No `delete_on_stop`**: the runner script patches `delete_on_start`
  and `delete_on_stop` to `false` so artifacts survive between phases.
- **No drasi-server binary**: drasi-lib runs in-process, so there's
  nothing to download and no admin-port patching to do.

## Run it from CI

Add a job to `.github/workflows/e2e-building-comfort.yml` that points
`WORKDIR` at this folder; copy the existing `building-comfort-drasi-server-*`
jobs and drop their drasi-server-specific steps if you want.

## Run it locally

```bash
cd e2e-test-framework/examples/building_comfort/ci/drasi_lib

export ARTIFACTS_DIR="$PWD/.local_run/artifacts"
export WORK_DIR="$PWD/.local_run/work"

./run_test_ci.sh
```

## Default ports

| Component                     | Port  |
|-------------------------------|-------|
| Test service REST API         | 63123 |

The drasi-lib host runs in-process, so it has no port of its own.

## Troubleshooting

- **`address already in use: 63123`** &mdash; another test service is
  bound. Override with `TEST_SERVICE_PORT`.
- **`Unsupported drasi-lib instance source kind`** &mdash; the embedded
  drasi-lib host only supports `kind: "application"` sources/reactions.
  Don't change those in `config.json`; if you need HTTP/gRPC transport,
  use the `ci/drasi_server_http` or `ci/drasi_server_grpc` example.
- **`Determinism mismatch`** &mdash; expected on the first run with empty
  baselines (`missing_baseline: Warn` makes that a non-fatal warning).
  Copy the actual SHA value from the run's `DeterminismHash` logger
  summary (or `determinism_verdict.json`) into `expected` in
  `config.json`, then flip `missing_baseline` to `Fail`.
