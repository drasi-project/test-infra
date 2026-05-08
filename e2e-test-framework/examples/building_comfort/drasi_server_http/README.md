# Building Comfort &mdash; External Drasi Server (HTTP webhook)

End-to-end test that drives an **external Drasi Server** over HTTP webhooks
instead of gRPC.

## What this test does

1. The E2E test service generates change events for a `BuildingHierarchy`
   model (Rooms with `temperature` / `humidity` / `co2`).
2. Events are POSTed as HTTP webhooks to Drasi Server's source
   `facilities-db` listening on **http://localhost:9000**.
3. Drasi Server runs the `building-comfort` Cypher query
   (`MATCH (r:Room) RETURN ...`) and POSTs results to the test service's
   HTTP reaction handler at **http://localhost:9001/reaction**.
4. The test service stops after **100,000** events
   (`stop_triggers.RecordCount`).

```
test-service  --HTTP source--> Drasi Server  --HTTP reaction--> test-service
   :8080        :9000                :8080      :9001/reaction      (logs/JSONL)
```

## Prerequisites

- One of:
  - a prebuilt `drasi-server` binary (see the [official download
    instructions](https://drasi.io/drasi-server/how-to-guides/installation/download-binary/)
    &mdash; downloads the binary into `./bin/drasi-server`); **or**
  - a checkout of the [drasi-server](https://github.com/drasi-project/drasi-server)
    repo with a working Rust toolchain.

  In either case the `source/http`, `reaction/http`, and `reaction/log`
  plugins are fetched automatically (`autoInstallPlugins: true`).
- This repository buildable via `cargo build --release`.

## 1. Start the external Drasi Server

Pick whichever option matches how you installed Drasi Server. In both
cases run from this folder so the relative `--config` path resolves.

### Option A: prebuilt binary

Replace `<path/to>` with the relative path from this folder to your
Drasi Server install (the binary lives at `<path/to>/bin/drasi-server`):

```bash
<path/to>/bin/drasi-server --config drasi_server_config.yaml
```

### Option B: cargo run from a drasi-server repo checkout

Replace `<path/to/drasi-server>` with the relative path from this folder
to your local drasi-server repo:

```bash
cargo run --release --manifest-path <path/to/drasi-server>/Cargo.toml -- --config drasi_server_config.yaml
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

## Inspect / control while running

The test service exposes a REST API on `http://localhost:8080`. The
`web_api_source.http`, `web_api_query.http`, and `web_api_reaction.http`
files in this folder contain ready-to-run requests for VS Code's REST
Client extension (or `curl`).

## Default ports

| Component                                     | Port               |
|-----------------------------------------------|--------------------|
| Test service REST API                         | 8080               |
| Drasi Server admin API                        | 8080 (override)    |
| Drasi Server HTTP source (`facilities-db`)    | 9000               |
| Test service HTTP reaction handler            | 9001 (path `/reaction`) |

## Troubleshooting

- **Connection refused on 9000** &mdash; Drasi Server is not running or the
  HTTP source plugin failed to bind. Confirm the server logs show
  `http source listening on 0.0.0.0:9000`.
- **No reaction events received** &mdash; The reaction handler must be
  listening on port 9001 before Drasi Server starts pushing. The test
  service binds it as soon as the run starts; if the test service starts
  *after* Drasi Server, the first batch may be lost while the reaction
  retries.
- **`address already in use: 8080`** &mdash; Either the test service or
  Drasi Server is already bound. Stop the other process or change one of
  their ports.
