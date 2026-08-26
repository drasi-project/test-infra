# Building Comfort &mdash; External Drasi Server (gRPC, CI)

End-to-end test that drives an **external Drasi Server** over gRPC instead
of HTTP webhooks. This is the CI variant of
[`local/drasi_server_grpc`](../../local/drasi_server_grpc) and mirrors the
[`ci/drasi_server_http`](../drasi_server_http) variant; the only difference
is the transport between the test framework and Drasi Server.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (1 building × 3 floors × 4 rooms; rooms carry
   `temperature` / `humidity` / `co2`).
2. Events are dispatched as gRPC `SourceService` calls to Drasi Server's
   source `facilities-db` listening on **grpc://localhost:50051**.
3. Drasi Server runs two Cypher queries against the same source:
   - `building-comfort` &mdash; per-room raw values
     (`MATCH (r:Room) RETURN ...`) &mdash; results streamed to
     **grpc://localhost:50052**.
   - `building-comfort-floor-agg` &mdash; one-hop traversal plus
     `avg` / `min` / `max` / `count` aggregations per floor
     (`MATCH (f:Floor)-[:FLOOR_ROOM]->(r:Room) RETURN ...`) &mdash;
     results streamed to **grpc://localhost:50053**.
4. The test service stops each reaction after its `RecordCount`
   stop-trigger fires (see `config.json`).

```
test-service --gRPC source--> Drasi Server --gRPC reaction--> test-service
   :63123        :50051             :8080      :50052 (per-room)
                                               :50053 (floor-agg)
```

## CI vs local

- **Determinism check is on**: `config.json` declares a
  `Sha256Determinism` completion handler. On first run, leave
  `expected: {}` and `missing_baseline: Warn` so the framework just logs
  the actual SHAs. Once you have a trusted baseline, copy the SHAs from
  the run's `DeterminismHash` logger summary into the `expected` map and
  flip `missing_baseline` to `Fail`.
- **No `delete_on_stop`**: the runner script patches `delete_on_start` and
  `delete_on_stop` to `false` so artifacts survive between phases.
- **Admin port is moved**: Drasi Server's admin API moves off `8080` to
  `8090` so the test service (default `63123`) and Drasi Server can
  coexist on one host.

## Run it from CI

The workflow at `.github/workflows/e2e-building-comfort.yml` runs this
example automatically when a job points at this folder. To add a job:
copy the existing `building-comfort-drasi-server-http` job, change
`WORKDIR` to
`e2e-test-framework/examples/building_comfort/ci/drasi_server_grpc`, and
give it a distinct job id and artifact name.

## Run it locally (Apple Silicon)

```bash
cd e2e-test-framework/examples/building_comfort/ci/drasi_server_grpc

# Pre-download a binary once so the script doesn't hit GitHub each time.
mkdir -p .local_run/bin
curl -fsSL -o .local_run/bin/drasi-server \
  https://github.com/drasi-project/drasi-server/releases/download/0.1.6/drasi-server-aarch64-apple-darwin
chmod +x .local_run/bin/drasi-server
xattr -d com.apple.quarantine .local_run/bin/drasi-server 2>/dev/null || true

export DRASI_SERVER_BIN="$PWD/.local_run/bin/drasi-server"
export ARTIFACTS_DIR="$PWD/.local_run/artifacts"
export WORK_DIR="$PWD/.local_run/work"

./run_test_ci.sh
```

## Default ports

| Component                                          | Port            |
|----------------------------------------------------|-----------------|
| Test service REST API                              | 63123           |
| Drasi Server admin API (patched by runner)         | 8090            |
| Drasi Server gRPC source (`facilities-db`)         | 50051           |
| Test service gRPC reaction handler (per-room)      | 50052           |
| Test service gRPC reaction handler (floor-agg)     | 50053           |

## Running in CI

This variant runs as the `building_comfort / drasi_server_grpc` job of the
`E2E - building_comfort` workflow, which downloads the latest drasi-server
release by default or builds from a branch/fork on demand. See
[`../README.md`](../README.md) for how to trigger it and pass a
`drasi_server_ref` / `drasi_server_repo`.

## Troubleshooting

- **`address already in use`** &mdash; one of the four ports above is held
  by another process. Either stop the conflicting process or override the
  port via `DRASI_ADMIN_PORT` / `DRASI_SOURCE_PORT` (the script only knows
  about the source port; the admin port lives in `drasi_server_config.yaml`
  and gets patched to `8090` automatically).
- **gRPC plugin version drift** &mdash; if drasi-server bumps the gRPC
  source/reaction plugin schema, you'll see Drasi Server fail to load the
  config with a deserialization error. Compare against
  `local/drasi_server_grpc/drasi_server_config.yaml` and update the fields
  in lockstep.
- **`Determinism mismatch`** &mdash; expected on the first run with empty
  baselines (`missing_baseline: Warn` makes that a non-fatal warning).
  Copy the actual SHA values from the run's `DeterminismHash` logger
  summary (or `determinism_verdict.json`) into `expected` in
  `config.json`, then flip `missing_baseline` to `Fail`.
