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
2. Patch port `8080` &rarr; `8090` (avoid colliding with the test service),
   make `data_store_path` and `source_path` absolute, and disable
   `delete_on_start/stop` so artifacts are preserved.
3. Start `drasi-server` (waiting for both port `9000` and port `50051`)
   and `test-service`.
4. Poll the `watchlist-prices` reaction until it reaches `Stopped`.
5. Write a markdown report (reaction status + throughput) to
   `$GITHUB_STEP_SUMMARY` (or skip the report when run locally).

Artifacts (logs, captured JSONL, reaction state) land in
`./ci_artifacts/`.

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
