# Building Comfort — Dynamic component-config driver (prototype)

This is a **dynamic** alternative to the static `ci/drasi_server_*` folders.
Instead of a fully-populated `drasi_server_config.yaml` per variant, it boots a
**bare Drasi Server** ([base/drasi_server.empty.yaml](base/drasi_server.empty.yaml))
and applies the scenario at runtime through the Drasi Server admin REST API,
composing it from reusable building blocks under [components/server/](components/server).

## Why

As we broaden coverage (issue #71) we want to test many component
configurations (source transport/batching, query buffers/middleware, reaction
handlers) and combinations of them. Maintaining a whole server YAML per
combination doesn't scale. Here a variant is just a small component JSON (or a
different `*_FILE` selection), applied via REST.

## What varies where

| Surface | How it's configured | Why |
| --- | --- | --- |
| Drasi Server source / queries / reactions | **Dynamic** — `POST /api/v1/{sources,queries,reactions}` from `components/server/*.json` | Drasi Server accepts full component configs over REST |
| Test-service (framework) source dispatcher + reaction handler | Static `config.json` | The framework keeps dispatcher/handler in the *test definition*; its REST API only references components by id, so they can't be injected |

## Layout

```
dynamic/
  base/drasi_server.empty.yaml     # instance + plugins only, no components
  components/server/
    source_grpc.json               # gRPC source        (POST /api/v1/sources)
    source_http.json               # HTTP source, adaptiveEnabled: false
    source_http_adaptive.json      # HTTP source, adaptiveEnabled: true (+ tuned)
    queries.json                   # array; constant across variants
    reactions_grpc.json            # array; gRPC reactions
    reactions_http.json            # array; HTTP webhook reactions
  config.json                      # test-service config (gRPC dispatcher/handlers)
  config.http.json                 # test-service config (HTTP dispatcher/handlers)
  run_dynamic.sh                   # the driver
```

## Variants

Selectable via `run_dynamic.sh` env vars (or the CI workflow's `variant` input):

| Variant | Source component | Reactions | Ingress port | Test-service config |
| --- | --- | --- | --- | --- |
| `grpc_standard` | `source_grpc.json` | `reactions_grpc.json` | 50051 | `config.json` |
| `grpc_adaptive` | `source_grpc.json` | `reactions_grpc.json` | 50051 | `config.grpc_adaptive.json` (adaptive dispatcher) |
| `http_standard` | `source_http.json` (`adaptiveEnabled:false`) | `reactions_http.json` | 9000 | `config.http.json` |
| `http_adaptive` | `source_http_adaptive.json` (`adaptiveEnabled:true`) | `reactions_http.json` | 9000 | `config.http.json` |

Note `http_standard` and `http_adaptive` differ by **one swapped source
component only** — adaptive batching lives inside the Drasi Server HTTP source,
so queries, reactions and the test-service config are identical and the
determinism baselines match (verified in CI: both hashes equal the standard
baseline).

For **gRPC**, the source has no `adaptiveEnabled` field, so `grpc_adaptive` puts
adaptive on the framework *dispatcher* instead (`config.grpc_adaptive.json`);
gRPC streaming delivers each event as a discrete message, so that is also
loss-free. (HTTP's `/events/batch` dispatcher path is **not** loss-free — it
collapses per-room updates — which is why HTTP adaptive is done server-side.)


## Apply order

The driver applies **source → queries → reactions** (a query needs its source to
exist; a reaction needs its query). Reactions `autoStart` and retry-connect to
the test-service's handler ports, so the test-service can start afterward.

## Run it locally (Apple Silicon)

```bash
cd e2e-test-framework/examples/building_comfort/dynamic

# Pre-download a server binary once (macOS arm64):
mkdir -p .local_run/bin
curl -fsSL -o .local_run/bin/drasi-server \
  https://github.com/drasi-project/drasi-server/releases/download/0.1.6/drasi-server-aarch64-apple-darwin
chmod +x .local_run/bin/drasi-server
xattr -d com.apple.quarantine .local_run/bin/drasi-server 2>/dev/null || true

export DRASI_SERVER_BIN="$PWD/.local_run/bin/drasi-server"
export ARTIFACTS_DIR="$PWD/.local_run/artifacts"
export WORK_DIR="$PWD/.local_run/work"

./run_dynamic.sh
```

On CI/Linux the driver downloads `drasi-server-x86_64-linux-gnu` automatically
(override with `DRASI_SERVER_VERSION` / `DRASI_TARGET`).

### Selecting a variant locally

```bash
# HTTP standard
SERVER_SOURCE_FILE=source_http.json SERVER_REACTIONS_FILE=reactions_http.json \
DRASI_SOURCE_PORT=9000 TEST_CFG_SRC="$PWD/config.http.json" ./run_dynamic.sh

# HTTP adaptive — same command, only the source component changes
SERVER_SOURCE_FILE=source_http_adaptive.json SERVER_REACTIONS_FILE=reactions_http.json \
DRASI_SOURCE_PORT=9000 TEST_CFG_SRC="$PWD/config.http.json" ./run_dynamic.sh
```

## Run it in CI

The dynamic variants are part of the shared workflow
`.github/workflows/e2e-building-comfort.yml` (`workflow_dispatch`). Add any of
these to the comma-separated **variants** input:

- `dynamic_http_standard`
- `dynamic_http_adaptive`
- `dynamic_grpc_standard`

The workflow's *Resolve variant* step maps `dynamic_*` onto the
`run_dynamic.sh` env knobs; static `ci/*` variants continue to use their own
`run_test_ci.sh`.


## Adding a new variant

1. Drop a new component JSON under `components/server/` (e.g. a query set with
   different buffer/middleware config, or a reaction with different batching).
2. If the transport changes, add a matching test-service `config.*.json`.
3. Run with the `*_FILE` / `DRASI_SOURCE_PORT` / `TEST_CFG_SRC` overrides, or add
   an option to the workflow's `variant` input.

Queries stay constant (`queries.json`) across transports.


## Plugin availability (important)

The bare server auto-installs the plugins listed in
[base/drasi_server.empty.yaml](base/drasi_server.empty.yaml) from the OCI
registry (`ghcr.io/drasi-project`) at startup. Install failures are **non-fatal**
(logged as warnings), so a missing plugin otherwise surfaces late as
`Unknown source kind: 'grpc'. Available: []`. The driver runs a **preflight
check** (`GET /api/v1/plugins/kinds`) and fails fast with an actionable message.

Known gap: on **darwin-arm64** the registry may not publish plugin builds for
the server's SDK version, so the plugins won't resolve locally. This affects the
static `ci/drasi_server_*` configs identically — it is not specific to this
driver. To get a green run:

- run on a **Linux x86_64** host / CI (where the registry has the plugins), or
- point `pluginRegistry` at a local plugins directory built from `drasi-core`.

