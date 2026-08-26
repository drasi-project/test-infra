# Building Comfort &mdash; CI E2E tests

This folder holds the CI variants of the `building_comfort` scenario. They are
driven by the [`E2E - building_comfort`](../../../../.github/workflows/e2e-building-comfort.yml)
GitHub Actions workflow, which runs a single matrix job over the selected
variants:

| Variant | Runner | Transport to Drasi |
| --- | --- | --- |
| `http_standard` | [`dynamic/run_dynamic.sh`](../dynamic/run_dynamic.sh) | bare drasi-server over HTTP webhooks, components applied via REST |
| `http_adaptive` | [`dynamic/run_dynamic.sh`](../dynamic/run_dynamic.sh) | as above, with `adaptiveEnabled` on the server source |
| `grpc_standard` | [`dynamic/run_dynamic.sh`](../dynamic/run_dynamic.sh) | bare drasi-server over gRPC, components applied via REST |
| `grpc_adaptive` | [`dynamic/run_dynamic.sh`](../dynamic/run_dynamic.sh) | as above, with the adaptive dispatcher |
| `drasi_lib` | [`drasi_lib/run_test_ci.sh`](drasi_lib/run_test_ci.sh) | drasi-lib embedded in-process |

The dynamic variants boot a **bare** drasi-server and apply the scenario at
runtime from the reusable component JSON under [`../dynamic`](../dynamic); a
variant is a component/config selection rather than a whole folder. Only
`drasi_lib` keeps a dedicated folder here, since it needs no external server.

Every runner builds the test-service, drives change events, waits for the
determinism/completion signal, and uploads artifacts.

## When it runs

- **Scheduled:** daily at 07:00 UTC (standard set) and 07:30 UTC (adaptive set).
  Scheduled runs always download the **latest `drasi-project/drasi-server`
  release** for the http/grpc variants.
- **Manually:** via *Actions → E2E - building_comfort → Run workflow*.

## Triggering a run

1. Go to the repo's **Actions** tab and select **E2E - building_comfort**.
2. Click **Run workflow**.
3. Under **Use workflow from**, pick the branch whose workflow + runner scripts
   you want to use (usually `main`).
4. Fill in the inputs (all optional) and click **Run workflow**.

### Inputs

Tick a checkbox per variant to build the matrix; the remaining inputs apply to
whichever variants run.

| Input | Default | Effect |
| --- | --- | --- |
| `drasi_lib` / `http_standard` / `grpc_standard` | checked | Include that variant in the matrix. |
| `http_adaptive` / `grpc_adaptive` | unchecked | Include the adaptive variants. |
| `batching_speed` | `medium` | Adaptive batching preset (adaptive variants only). |
| `query_tuning` | `medium` | Query capacity preset (priority/dispatch buffers, bootstrap buffer). Perf only &mdash; results must not change. |
| `persist_index` | unchecked | Use the built-in RocksDB persistent index. |
| `state_store` | unchecked | Use the redb plugin state store. |
| `drasi_server_version` | empty | drasi-server release tag (e.g. `v0.1.5`). Empty = latest. |
| `drasi_server_repo` | empty | drasi-server repo (`owner/name`) to build from. Empty falls back to `drasi-project/drasi-server`. Only used when `drasi_server_ref` is set. |
| `drasi_server_ref` | empty | drasi-server branch/tag/SHA to **build from source**. Empty = download the release binary. |
| `timeout_minutes` | 30 | Max minutes to wait for each test to finish. |

### Default run (latest release)

Leave every input empty and click **Run workflow**. The http/grpc variants
download the latest `drasi-project/drasi-server` release and test against it
&mdash; same as the scheduled run.

### Test a drasi-server branch or fork

Set `drasi_server_ref` (and optionally `drasi_server_repo` for a fork). The
http/grpc runner then `git clone`s that repo/ref and `cargo build --release`
instead of downloading a release. This is a manual-dispatch escape hatch:
scheduled runs leave it empty so the published result history stays comparable.

Example &mdash; test branch `my-branch` on a fork:

- `drasi_server_repo` = `my-user/drasi-server`
- `drasi_server_ref` = `my-branch`

Or with the CLI:

```bash
gh workflow run e2e-building-comfort.yml \
  --ref main \
  -f drasi_server_repo=my-user/drasi-server \
  -f drasi_server_ref=my-branch \
  -f timeout_minutes=45
```

Requirements:

- The branch/tag/SHA must be **pushed** to the target repo.
- The repo must be **public** (the runner clones anonymously).
- The first source build is uncached, so it runs longer &mdash; bump
  `timeout_minutes` if a run times out.

The `drasi_lib` variant ignores these inputs (it has no external binary); see
below.

### Test a drasi-core branch (drasi_lib variant)

The `drasi_lib` variant compiles drasi-core / drasi-lib **into** the
test-service, so there is no binary to point at a branch. Instead, override the
crate sources via Cargo and run the workflow from your branch:

1. On your branch of this repo, add a git-based `[patch.crates-io]` block in
   [`e2e-test-framework/Cargo.toml`](../../../Cargo.toml) pointing the
   drasi-core crates at your branch/fork of `drasi-core`.
2. Push the branch, then run the workflow with **Use workflow from** set to that
   branch. Cargo pulls the patched sources when it builds the test-service.

See [`drasi_lib/README.md`](drasi_lib/README.md) for the full walkthrough.

## Reading results

- **Step summary:** each http/grpc run labels its drasi-server source as
  `source <repo>@<ref> (<sha>)` or `release <tag> (<repo>)`, followed by a table
  of reaction status, record counts, runtimes, SHA-256s, and the determinism
  verdict.
- **Artifacts:** every matrix leg uploads a `building_comfort-<variant>` artifact
  with logs, per-reaction final states, performance metrics, and the determinism
  verdict for offline inspection.
