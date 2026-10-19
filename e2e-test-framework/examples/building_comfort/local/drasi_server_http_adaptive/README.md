# Building Comfort &mdash; External Drasi Server (HTTP, adaptive batching)

Same end-to-end shape as `drasi_server_http/`, but the test service uses the
**adaptive HTTP dispatcher** to optimise throughput, and the source/reaction
ports are different.

The Server configuration uses the native HTTP source and sink factories in its
DrasiLib instance's single graph. The adaptive dispatcher, query text, ports and
output templates keep their existing behavior; initial data arrives as ordinary
inserts rather than a bootstrap callback.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (Rooms with `temperature` / `humidity` / `co2`).
2. Events are POSTed over HTTP with **adaptive batching**
   (`batch_size: 1000`, `batch_timeout_ms: 50`, dynamic 10&ndash;1000
   messages per batch) to Drasi Server's source `events-http` on
   **http://localhost:8081**.
3. Drasi Server runs the `building-comfort` Cypher query
   (`MATCH (r:Room) RETURN ...`) and POSTs results to the test service's
   HTTP reaction handler at **http://localhost:3000/events**.
4. The test service stops after **100,000** events.

```
test-service  --adaptive HTTP--> Drasi Server  --HTTP reaction--> test-service
   :8080        batch up to 1000      :8081         :3000/events
```

> Note the source id is `events-http` (not `facilities-db`) and the
> reaction id is `http-webhook` (not `building-comfort`) to match the test
> framework's `config.json`.

## Prerequisites

- A matching local Server and the independent
  `drasi-computation-source-http`/`drasi-computation-sink-http` libraries.
  Build them from the matching drasi-core checkout with
  `make build-native-network-plugins`; Server builds alone do not update plugins.
- This repository buildable via `cargo build --release`.

## 1. Start the external Drasi Server

Run from this folder with the matching native library directory. Automatic
legacy plugin installation is disabled.

```bash
<path/to/drasi-server> --config drasi_server_config.yaml \
  --plugins-dir <path/to/native-libraries> --skip-verification
```

Drasi Server will:

- Bind its admin API on `0.0.0.0:8080` (override if it conflicts with the
  test service).
- Start the HTTP webhook source `events-http` on `0.0.0.0:8081`.
- Auto-start the `building-comfort` query.
- Auto-start the `http-webhook` HTTP reaction, POSTing results to
  `http://localhost:3000/events`.

## 2. Run the E2E test

From this folder:

```bash
./run_test.sh
```

To watch adaptive batcher decisions, set
`RUST_LOG=info,test_run_host::utils::adaptive_batcher=debug` before running.

## Default ports

| Component                                     | Port                   |
|-----------------------------------------------|------------------------|
| Test service REST API                         | 8080                   |
| Drasi Server admin API                        | 8080 (override needed) |
| Drasi Server HTTP source (`events-http`)      | 8081                   |
| Test service HTTP reaction handler            | 3000 (path `/events`)  |

## Troubleshooting

- **Throughput plateaus** &mdash; Confirm the adaptive batcher is growing
  batches via debug logs. If they stay near the lower bound (~10), the
  bottleneck is downstream (Drasi Server, query plan, or reaction
  endpoint).
- **Connection refused on 8081 / 3000** &mdash; See
  `drasi_server_http/README.md` for the same diagnostics.
- **`address already in use: 8080`** &mdash; Test service and Drasi Server
  both default to 8080. Override one of them.
