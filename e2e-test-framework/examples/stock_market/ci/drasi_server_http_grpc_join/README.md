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

4. Both variants stop the reaction at **75% of the stock workload** via
  `stop_triggers.RecordCount`: 75,000 records for the default 100,000 changes.

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

- Bash, Ruby, jq, curl, and a Rust toolchain (unless using prebuilt binaries).
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

# Adaptive dispatchers for BOTH sources; the HTTP reaction stays unchanged.
VARIANT=drasi_server_http_grpc_join_adaptive BATCHING_SPEED=10000 ./run_test_ci.sh
```

The CI script will:

1. Download the latest `drasi-server` binary (or reuse `DRASI_SERVER_BIN`).
2. Render the selected server profile, patch the workload and CI paths, and
  disable `delete_on_start/stop` so artifacts are preserved.
3. Start `drasi-server` (waiting for both port `9000` and port `50051`)
   and `test-service`.
4. Poll the `watchlist-prices` reaction until it reaches `Stopped`.
5. Write a markdown report (variant, reaction status, throughput) to
  `ci_artifacts/summary.md` and append it to `$GITHUB_STEP_SUMMARY` when set.

Artifacts (logs, captured JSONL, reaction state) land in
`./ci_artifacts/`.

## Configuration controls

The manual workflow exposes the Phase I controls that preserve this scenario's
cross-source join contract:

| Input | Values | Effect |
| --- | --- | --- |
| `variant` | `standard` (default), `adaptive`, `both` | Selects the original join, adaptive dispatch on both sources, or both variants. |
| `batching_speed` | `5000`, `10000` (default), `50000` | Maximum events per adaptive batch for both dispatchers; maximum wait is 50 ms. Standard dispatchers are unchanged. |
| `workload_size` | `100000`, `250000`, `500000` | Number of stock changes; the reaction stop target remains 75% of the workload. |
| `query_tuning` | `1000`, `10000`, `100000` | Sets query priority, dispatch, and bootstrap buffer capacities. |
| `persist_index` | boolean | Enables the RocksDB index and replay-capable WAL durability on both sources. |
| `state_store` | boolean | Enables the redb plugin state store under the run's scratch directory. |
| `drasi_server_version` | release tag or empty | Selects a release; empty uses latest. |
| `drasi_server_repo` / `drasi_server_ref` | repo and optional ref | Builds an arbitrary server repo/ref from source. |
| `plugin_registry` / `plugin_tag` | OCI registry/tag or empty | Overrides the source/reaction plugin packages. |

Both variants keep the HTTP stock source, gRPC watchlist source, sole join query,
and HTTP reaction. There are no query checkboxes or large-bootstrap presets. External
server plugins consume dispatched events rather than pulling framework
bootstrap data, so `workload_size` is the scenario's data-volume control.

### Adaptive join

`drasi_server_http_grpc_join` retains the committed standard dispatchers.
`drasi_server_http_grpc_join_adaptive` enables the framework's adaptive
dispatchers for **both** `stock-trades-db` (HTTP) and `watchlist-db` (gRPC).
HTTP sends to the stock source's `/events/batch` endpoint; gRPC uses its
streaming batch dispatcher. The runner sets each dispatcher's `source_id`
explicitly. The batch limit is a maximum, not a required batch size: the
slow-changing watchlist will normally flush smaller batches at the time limit.

This does not enable server-side HTTP source batching or HTTP reaction batching.
The seed, watchlist script, queries, output handler, and completion targets stay
the same. Cross-source timing can still change the emitted row stream, so neither
variant has a stable SHA baseline. Count-based completion is retained, not a
guarantee of full-stream equivalence or losslessness.

Both **E2E - stock_market join** and **Stock market Azure** expose `variant` and
`batching_speed`. Selecting `both` creates separate jobs on GitHub-hosted
runners and sequential runs on one Azure VM. Artifacts and summary records use
the full variant names so adaptive results remain separate from the existing
standard history. Scheduled GitHub-hosted runs continue to run only `standard`;
scheduled Azure runs cover both variants on all three hardware tiers.

## Running in CI against a drasi-server branch or fork

The selected variants run as `stock_market / <variant>` jobs of
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

## Running on an Azure VM

Use **Actions > Stock market Azure > Run workflow** via
[stock-market-azure.yml](../../../../../.github/workflows/stock-market-azure.yml).
This is a separate workflow from the GitHub-hosted join test. The new
workflow must be committed, pushed, and present on the repository's default
branch for the **Run workflow** button to appear; after that, select the branch
containing the version you want to test.

It reuses the provisioning and cleanup in
[building-comfort-azure.yml](../../../../../.github/workflows/building-comfort-azure.yml).
The workload runs natively on an ephemeral Ubuntu 24.04 Azure VM, not on the
GitHub controller runner. No self-hosted GitHub runner registration is needed.

Use the same server and plugin settings that passed the GitHub-hosted run.
Choose a region, VM size, OS disk type, and disk size alongside the stock-market
variant, batch size, workload, query capacity, and persistence inputs. The default hardware is
`Standard_D4s_v6` in `westus3` with a 128 GB Premium SSD.

The repository needs `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and
`AZURE_SUBSCRIPTION_ID` secrets plus the Azure OIDC trust and provisioning
permissions used by **Building comfort Azure**. An OIDC policy restricted to
the calling workflow identity must also allow this workflow.

The workflow uploads throughput summaries, logs, Azure hardware metadata, and
binary/plugin fingerprints. It deletes the per-run VM and network resources
on completion, including test failures, and verifies cleanup. Runs share the
existing Azure resource-group concurrency lock. Manual results are not published
to the scheduled performance history. Azure VM and disk charges apply while the
resources exist.

### Daily performance schedule

**Stock market Azure** runs daily at **07:30 UTC** from the default branch,
offset from building comfort's 07:00 UTC schedule. It reuses the same hardware
matrix: `Standard_D4s_v3`, `Standard_D4s_v6`, and `Standard_F4as_v7`. VM jobs run
sequentially, and each VM runs both `standard` and `adaptive`, producing six
performance records. The shared concurrency lock queues this workflow while
another Azure test is running, so the actual start may be later.

Scheduled defaults are `westus3`, a 128 GB Premium SSD, 100,000 stock changes,
query capacity 10,000, adaptive max batch size 10,000 (50 ms max wait), and both
persistence options off. Like building comfort, scheduled runs download the
latest Drasi Server release and use its default plugin registry and compatible
plugin versions; branch and plugin overrides remain manual-run options.

After all VM jobs finish, the shared publisher posts the available summaries to
`drasi-project/test-results`, including failed-test records. Results are keyed by
scenario, variant, Azure hardware profile, and run ID to prevent overwrites.
Publishing requires the same `TEST_RESULTS_APP_PRIVATE_KEY` secret and
`TEST_RESULTS_APP_ID` repository variable as building comfort, with the GitHub
App authorized to write to `drasi-project/test-results`. The stock-market caller
forwards that secret through the reusable Azure workflow. Enable the schedule
by merging these changes into the repository's default branch; scheduled
workflows must also be enabled in forks.

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
