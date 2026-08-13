# Drasi E2E Aspire proof of concept

This folder contains an isolated .NET Aspire spike for the `building_comfort`
external Drasi Server scenario. It does not replace or modify the existing
GitHub Actions workflows or shell runners.

## What it models

The AppHost in [Drasi.E2E.AppHost](./Drasi.E2E.AppHost) defines:

- `drasi-server`: a container image running a bare Drasi Server config with the
  gRPC source and reaction plugins enabled.
- `redis`: an Aspire-managed Redis container. The current `grpc_standard` spike
  does not need Redis for the data path, but it is present so the environment
  shape can grow toward the cloud-hosted E2E topology.
- `e2e-runner`: a one-shot container that builds the Rust `test-service`, applies
  the `building_comfort` gRPC source/query/reaction components to Drasi Server
  over the admin API, runs the test-service, waits for completion, and writes
  artifacts.

The runner receives the Drasi Server endpoint, gRPC source endpoint, reaction
callback host, Redis connection string, and artifact directory through
environment variables from the AppHost.

## Prerequisites

- Docker Desktop or a compatible local Docker engine.
- .NET SDK 10.x.
- Aspire CLI 13.x.

Install the Aspire CLI with one of the official methods, for example:

```bash
dotnet tool install -g Aspire.Cli
```

## Drasi Server image

By default, the AppHost builds [Drasi.Server.Container](./Drasi.Server.Container)
from the published Drasi Server release binary and runs that as the
`drasi-server` container. This avoids depending on a pre-published image while
still keeping Drasi Server containerized.

If you want to test a locally built or fork-specific image, set
`DRASI_SERVER_IMAGE`:

```bash
export DRASI_SERVER_IMAGE=drasi-server:local
```

Any override image must contain the `drasi-server` entrypoint and be able to
auto-install the `source/grpc`, `reaction/grpc`, and `reaction/log` plugins declared in
[drasi-server.empty.yaml](./Drasi.E2E.AppHost/config/drasi-server.empty.yaml).

## Run locally

From the repository root:

```bash
aspire run --apphost aspire/Drasi.E2E.AppHost/Drasi.E2E.AppHost.csproj
```

Aspire starts the dashboard plus the three modeled resources. The `e2e-runner`
container is expected to exit when the test run finishes. Artifacts are written
under [Drasi.E2E.AppHost/artifacts](./Drasi.E2E.AppHost/artifacts).

Useful knobs:

```bash
DRASI_SERVER_IMAGE=drasi-server:local \
TIMEOUT_SECS=1800 \
aspire run --apphost aspire/Drasi.E2E.AppHost/Drasi.E2E.AppHost.csproj
```

## GitHub Actions POC

[e2e-building-comfort-aspire.yml](../.github/workflows/e2e-building-comfort-aspire.yml)
adds a manual `workflow_dispatch` path that installs .NET and Aspire, runs the
AppHost detached, waits for the one-shot `e2e-runner` container to finish, and
uploads the AppHost artifact directory.

This workflow is intentionally opt-in while the Aspire topology is being
validated locally.
