# Building Comfort &mdash; External Drasi Server (HTTP webhook)

End-to-end test that drives an **external Drasi Server** over HTTP webhooks
instead of gRPC.

The default Server configuration uses `drasi.network/http-source` and
`drasi.network/http-sink` in the hosting DrasiLib instance's ComputationGraph.
It keeps the same source/query identities and webhook protocols without legacy
Source/Reaction adapters. The generator supplies initial state as ordinary
inserts, so query bootstrap is explicitly disabled.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (1 building × 3 floors × 4 rooms; rooms carry
   `temperature` / `humidity` / `co2`).
2. Events are POSTed as HTTP webhooks to Drasi Server's source
   `facilities-db` listening on **http://localhost:9000**.
3. Drasi Server runs two Cypher queries against the same source:
   - `building-comfort` &mdash; per-room raw values
     (`MATCH (r:Room) RETURN ...`) &mdash; results POSTed to
     **http://localhost:9001/reaction**.
   - `building-comfort-floor-agg` &mdash; one-hop traversal plus
     `avg` / `min` / `max` / `count` aggregations per floor
     (`MATCH (f:Floor)-[:FLOOR_ROOM]->(r:Room) RETURN ...`) &mdash;
     results POSTed to **http://localhost:9002/reaction**.
4. The test service stops each reaction after **100,000** events
   (`stop_triggers.RecordCount`).

```
test-service --HTTP source--> Drasi Server --HTTP reaction--> test-service
   :8080         :9000              :8080      :9001/reaction (per-room)
                                               :9002/reaction (floor-agg)
```

## Prerequisites

- A Server built against the matching drasi-core checkout and the independently
  built `drasi-computation-source-http` and `drasi-computation-sink-http`
  libraries. From drasi-core, run `make build-native-network-plugins`.
  Native plugins are not rebuilt merely by rebuilding the Server.
- This repository buildable via `cargo build --release`.

## 1. Start the external Drasi Server

Run from this folder, pointing at the directory containing the matching local
native libraries. Automatic registry installation is disabled in this profile.

```bash
<path/to/drasi-server> --config drasi_server_config.yaml \
  --plugins-dir <path/to/native-libraries> --skip-verification
```

Drasi Server will:

- Bind its admin API on `0.0.0.0:8080`.
- Start the HTTP webhook source `facilities-db` on `0.0.0.0:9000`.
- Auto-start the `building-comfort` query.
- Auto-start the `building-comfort-out` HTTP reaction, posting results to
  `http://localhost:9001/reaction`.

> Drasi Server's admin port (`8080`) clashes with the E2E test service's
> default API port. Edit `host`/`port` in `drasi_server_config.yaml` (or
> change the test service's API port) so they don't collide.

## 2. Run the E2E test

From this folder:

```bash
./run_test.sh
```

Use `./run_test_debug.sh` for verbose tracing.

### Optional: per-reaction SHA-256 determinism check

Set `SHA_CHECK=1` to attach a `DeterminismHash` output logger to each
reaction and a `Sha256Determinism` completion handler (with an empty
baseline and `missing_baseline: "Warn"`) to the test definition. The
run then prints the per-reaction SHA-256 in the framework log without
failing the test:

```bash
SHA_CHECK=1 ./run_test.sh
```

Requires `jq` on `PATH`. Two successive runs on the same machine should
produce identical SHAs; the CI `http_standard` variant
([`dynamic/config.http.json`](../../dynamic/config.http.json)) maintains a
locked baseline. To enforce a baseline locally, paste the SHAs into a
`Sha256Determinism` block in `config.json` directly (see the CI variant's
config for the exact shape).

## Inspect / control while running

The test service exposes a REST API on `http://localhost:8080`. The
`web_api_source.http`, `web_api_query.http`, and `web_api_reaction.http`
files in this folder contain ready-to-run requests for VS Code's REST
Client extension (or `curl`).

## Default ports

| Component                                          | Port                    |
|----------------------------------------------------|-------------------------|
| Test service REST API                              | 8080                    |
| Drasi Server admin API                             | 8080 (override)         |
| Drasi Server HTTP source (`facilities-db`)         | 9000                    |
| Test service HTTP reaction handler (per-room)      | 9001 (path `/reaction`) |
| Test service HTTP reaction handler (floor-agg)     | 9002 (path `/reaction`) |

## Troubleshooting

- **Connection refused on 9000** &mdash; Drasi Server is not running or the
  HTTP source plugin failed to bind. Confirm the server logs show
  `http source listening on 0.0.0.0:9000`.
- **No reaction events received** &mdash; The reaction handler must be
  listening on port 9001 before Drasi Server starts pushing. The test
  service binds it as soon as the run starts; if the test service starts
  *after* Drasi Server, strict native delivery can fail after exhausted retries;
  start the receiver before activating the source rather than accepting loss.
- **`address already in use: 8080`** &mdash; Either the test service or
  Drasi Server is already bound. Stop the other process or change one of
  their ports.
