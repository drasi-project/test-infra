# Stock Market &mdash; Stocks (HTTP) joined with Watchlist (gRPC)

End-to-end test that drives an **external Drasi Server** with two sources
of different shapes and transports, joined by one Cypher query that
returns only the stock prices for symbols currently in the watchlist.

## What this test does

1. The E2E test framework runs two sources concurrently:
   - `stock-trades-db` &mdash; **Model** source using the `StockTrade`
     generator. Produces a steady stream of `:Stock` node inserts /
     updates and dispatches them as **HTTP** webhooks to
     `http://localhost:9000`.
   - `watchlist-db` &mdash; **Script** source. Reads a hand-written
     JSONL change script
     (`dev_repo/stock_market/sources/watchlist-db/source_change_scripts/`)
     that seeds 3 `:WatchlistItem` nodes at `offset_ns=0` and then emits
     4 timed edits at +3s, +6s, +9s, +12s (insert, insert, delete, update).
     Events are dispatched over **gRPC** to `localhost:50051`.

2. Drasi Server runs one Cypher query subscribed to **both** sources:
   ```cypher
   MATCH (s:Stock), (w:WatchlistItem)
   WHERE s.symbol = w.symbol
   RETURN w.symbol   AS Symbol,
          s.name     AS Name,
          s.price    AS Price,
          s.volume   AS Volume,
          w.added_by AS AddedBy
   ```
   This is a real cross-source join: a stock price update only emits a
   reaction when its symbol is in the current watchlist, and editing
   the watchlist immediately adds/removes rows from the result set.

3. Query results are POSTed as an HTTP reaction to
   `http://localhost:9002/reaction`.

4. The test service stops the reaction after **400** records via
   `stop_triggers.RecordCount`.

```
test-service --HTTP source--> Drasi Server --HTTP reaction--> test-service
   :8080         :9000              :8080      :9002/reaction
                                        ^
test-service --gRPC source--+
   :8080         :50051
```

## Files in this folder

- `config.json` &mdash; E2E test framework config (two sources + one reaction).
- `drasi_server_config.yaml` &mdash; Drasi Server config (two sources, one
  Cypher query, one HTTP reaction).
- `dev_repo/stock_market/sources/watchlist-db/source_change_scripts/source_change_scripts_00000.jsonl`
  &mdash; The watchlist replay script (seed + timed edits).
- `run_test_ci.sh` &mdash; CI runner: downloads drasi-server, patches
  configs (admin port, absolute paths), launches both processes, waits
  for the test-run completion signal, and writes a markdown summary
  (reaction status + throughput) to `$GITHUB_STEP_SUMMARY`. Same shape
  as the `building_comfort/ci/drasi_server_*` runners. **No SHA-256
  determinism check is performed for this variant**: the cross-source
  join over an HTTP + gRPC pair produces a different multiset of
  emitted rows on each run (the relative ordering of stock ticks vs
  watchlist edits varies, and the `RecordCount` stop trigger truncates
  the tail at different points), so no SHA baseline is stable. The
  count-based stop trigger is the meaningful assertion; performance
  metrics are reported in the workflow summary for visibility.
- `render_server_config.rb` &mdash; renders the selected query-capacity,
  RocksDB index, redb state-store, source-WAL, and plugin settings into the
  scratch server config without changing the committed scenario definition.
- `test_render_server_config.rb` and `test_render_configs.sh` &mdash; cover the
  structured server renderer and the runner's complete scratch-config path.

## Why the watchlist seed is in `source_change_scripts`, not `bootstrap_scripts`

External Drasi Server source plugins (HTTP / gRPC) don't *pull*
`bootstrap_data_generator` content from the test framework &mdash; they
only consume the dispatched event stream. So the initial 3 watchlist
items are dispatched as `op:"i"` events at `offset_ns=0` alongside the
timed edits. Drasi Server treats them like any other insert and builds
its query state from them.

## Prerequisites

- One of:
  - a prebuilt `drasi-server` binary (see the
    [official download instructions](https://drasi.io/drasi-server/how-to-guides/installation/download-binary/));
    **or**
  - a checkout of the [drasi-server](https://github.com/drasi-project/drasi-server)
    repo with a working Rust toolchain.

  In either case the `source/http`, `source/grpc`, `reaction/http`, and
  `reaction/log` plugins are fetched automatically
  (`autoInstallPlugins: true`).
- This repository buildable via `cargo build --release`.

## Running locally

```bash
./run_test_ci.sh
```

The CI script will:

1. Download the latest `drasi-server` binary (or reuse `DRASI_SERVER_BIN`).
2. Render the selected server profile, patch the workload and CI paths, and
  disable `delete_on_start/stop` so artifacts are preserved.
3. Start `drasi-server` (waiting for both port `9000` and port `50051`)
   and `test-service`.
4. Poll the `watchlist-prices` reaction until it reaches `Stopped`.
5. Write a markdown report (reaction status + throughput) to
   `$GITHUB_STEP_SUMMARY` (or skip the report when run locally).

Artifacts (logs, captured JSONL, reaction state) land in
`./ci_artifacts/`.

## Configuration controls

The manual workflow exposes the Phase I controls that preserve this scenario's
cross-source join contract:

| Input | Values | Effect |
| --- | --- | --- |
| `workload_size` | `100000`, `250000`, `500000` | Number of stock changes; the reaction stop target remains 75% of the workload. |
| `query_tuning` | `1000`, `10000`, `100000` | Sets query priority, dispatch, and bootstrap buffer capacities. |
| `persist_index` | boolean | Enables the RocksDB index and replay-capable WAL durability on both sources. |
| `state_store` | boolean | Enables the redb plugin state store under the run's scratch directory. |
| `drasi_server_version` | release tag or empty | Selects a release; empty uses latest. |
| `drasi_server_repo` / `drasi_server_ref` | repo and optional ref | Builds an arbitrary server repo/ref from source. |
| `plugin_registry` / `plugin_tag` | OCI registry/tag or empty | Overrides the source/reaction plugin packages. |

`stock_market` intentionally has no source/reaction variant matrix or query
checkboxes: its HTTP stock source, gRPC watchlist source, and sole join query
are the behavior under test. It also has no large-bootstrap preset. External
server plugins consume dispatched events rather than pulling framework
bootstrap data, so `workload_size` is the scenario's data-volume control.

## Running in CI against a drasi-server branch or fork

This variant runs as the `stock_market / drasi_server_http_grpc_join` job of
the [`E2E - stock_market join`](../../../../../.github/workflows/e2e-stock-market-join.yml)
workflow. By default (scheduled or a plain manual run) it downloads the latest
`drasi-project/drasi-server` release. To instead **build drasi-server from
source**, trigger it via *Actions → E2E - stock_market join → Run workflow* and
set:

- `drasi_server_version` &mdash; a release tag to download. Empty downloads the
  latest release when no source repo/ref is selected.
- `drasi_server_ref` &mdash; a branch, tag, or commit SHA to build. Empty keeps
  the default release-download behavior unless `drasi_server_repo` is set.
- `drasi_server_repo` &mdash; the repo to build from (`owner/name`). Point it at
  a fork to test its default branch or the selected ref. Empty defaults to
  `drasi-project/drasi-server`.

The runner clones that repo/ref and runs `cargo build --release`, then uses the
freshly built binary. The step summary labels the run with the resolved source
(`source <repo>@<ref> (<sha>)` or `release <tag> (<repo>)`). The repo/branch must
be pushed and public (the clone is anonymous). Locally, export `DRASI_SERVER_REF`
(and optionally `DRASI_REPO`) before running the script for the same effect.

## Default ports

| Component                                          | Port                    |
|----------------------------------------------------|-------------------------|
| Test service REST API                              | 63123                   |
| Drasi Server admin API                             | 8090 (CI patches 8080)  |
| Drasi Server HTTP source (`stock-trades-db`)       | 9000                    |
| Drasi Server gRPC source (`watchlist-db`)          | 50051                   |
| Test service HTTP reaction handler                 | 9002 (path `/reaction`) |

## Editing the watchlist scenario

Open `dev_repo/stock_market/sources/watchlist-db/source_change_scripts/source_change_scripts_00000.jsonl`
and add / change / delete records. Each record is one line:

- `offset_ns` &mdash; nanoseconds from script start (script uses
  `spacing_mode: "recorded"` and `time_mode: "live"`, so offsets become
  real wall-clock delays).
- `source_change_event.op` &mdash; `"i"` (insert), `"u"` (update), `"d"`
  (delete).
- `source_change_event.payload.before` / `after` &mdash; the node state
  before and after the change (`null` for the absent side of i/d).
- The seed records use `offset_ns: 0` so they are dispatched immediately
  when the source starts.

If you adjust the script, you'll likely need to retune `record_count` in
`config.json` for the `watchlist-prices` reaction.
