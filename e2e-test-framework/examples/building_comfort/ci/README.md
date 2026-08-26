# Building Comfort &mdash; CI E2E tests

This folder holds the CI variants of the `building_comfort` scenario. They are
driven by the [`E2E - building_comfort`](../../../../.github/workflows/e2e-building-comfort.yml)
GitHub Actions workflow, which runs three jobs:

| Job | Folder | Transport to Drasi |
| --- | --- | --- |
| `building_comfort / drasi_server_http` | [`drasi_server_http`](drasi_server_http) | external drasi-server over HTTP webhooks |
| `building_comfort / drasi_server_grpc` | [`drasi_server_grpc`](drasi_server_grpc) | external drasi-server over gRPC |
| `building_comfort / drasi_lib` | [`drasi_lib`](drasi_lib) | drasi-lib embedded in-process |

Each job runs the `run_test_ci.sh` in its folder, which builds the test-service,
drives change events, waits for the determinism/completion signal, and uploads
artifacts.

## When it runs

- **Scheduled:** daily at 07:00 UTC. Scheduled runs always download the **latest
  `drasi-project/drasi-server` release** for the http/grpc jobs.
- **Manually:** via *Actions → E2E - building_comfort → Run workflow*.

## Triggering a run

1. Go to the repo's **Actions** tab and select **E2E - building_comfort**.
2. Click **Run workflow**.
3. Under **Use workflow from**, pick the branch whose workflow + runner scripts
   you want to use (usually `main`).
4. Fill in the inputs (all optional) and click **Run workflow**.

### Inputs

| Input | Default | Effect |
| --- | --- | --- |
| `drasi_server_repo` | empty | drasi-server repo (`owner/name`) to build from. Empty falls back to `drasi-project/drasi-server`. Only used when `drasi_server_ref` is set. |
| `drasi_server_ref` | empty | drasi-server branch/tag/SHA to **build from source** (http/grpc jobs). Empty = download the latest release binary. |
| `timeout_minutes` | 30 | Max minutes to wait for each test to finish. |

### Default run (latest release)

Leave every input empty and click **Run workflow**. The http/grpc jobs download
the latest `drasi-project/drasi-server` release and test against it &mdash; same
as the scheduled run.

### Test a drasi-server branch or fork

Set `drasi_server_ref` (and optionally `drasi_server_repo` for a fork). The
http/grpc runners then `git clone` that repo/ref and `cargo build --release`
instead of downloading a release.

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

The `drasi_lib` job ignores these inputs (it has no external binary); see below.

### Test a drasi-core branch (drasi_lib job)

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
- **Artifacts:** every job uploads a `building_comfort-<variant>` artifact with
  logs, per-reaction final states, performance metrics, and the determinism
  verdict for offline inspection.
