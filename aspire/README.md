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

## Azure Container Apps publish spike

The AppHost includes an Azure Container Apps environment named
`drasi-e2e-aca` in publish/deploy mode. Local `aspire run` behavior is
unchanged.

The GitHub workflow defaults to the ACA `Consumption` profile so it can run in
new subscriptions without a Dedicated-cores quota request. Each resource uses
one fixed replica with Consumption-compatible limits:

| Resource | CPU | Memory |
| --- | ---: | ---: |
| Drasi Server | 4 vCPU | 8 GiB |
| E2E runner | 4 vCPU | 8 GiB |
| Redis | 0.5 vCPU | 1 GiB |

Set `AZURE_WORKLOAD_PROFILE=dedicated-d8` (or select `dedicated-d8` in the
manual workflow) to use one Dedicated D8 node with the original fixed
allocations. That option requires at least eight Managed Environment General
Purpose cores in the target region.

Consumption fixes requested CPU, memory, and replica count but runs on shared
serverless infrastructure, so it is suitable for validating the deployment
pipeline rather than establishing a hardware-controlled performance baseline.

The Container Apps environment uses Aspire compact resource naming so globally
unique resources such as the Azure Files storage account retain their generated
unique suffix across subscriptions and tenants.

In publish mode, the E2E runner remains alive after writing its result
artifacts. It is currently modeled as an internal Container App rather than an
Azure Container Apps Job because Drasi Server must connect back to the runner's
gRPC reaction endpoints while the test is running.

Generate deployment artifacts without creating Azure resources:

```bash
aspire publish \
  --apphost aspire/Drasi.E2E.AppHost/Drasi.E2E.AppHost.csproj \
  --output-path aspire/Drasi.E2E.AppHost/aspire-output \
  --environment azure-dev \
  --non-interactive
```

The Drasi Server config is baked into its image for Azure publishing. Local
`aspire run` still bind-mounts the config so it remains easy to edit during
development.

### Deploy to a disposable Azure resource group

Sign in and select the target subscription:

```bash
az login
az account set --subscription "<subscription-id-or-name>"
```

Set the deployment target:

```bash
export Azure__SubscriptionId="$(az account show --query id -o tsv)"
export Azure__Location="westus2"
export Azure__ResourceGroup="drasi-e2e-aspire-poc"
```

Deploy:

```bash
aspire deploy \
  --apphost aspire/Drasi.E2E.AppHost/Drasi.E2E.AppHost.csproj \
  --environment azure-dev
```

The E2E runner remains alive after the test so its internal gRPC reaction
endpoints and result files remain available. Check the runner logs in the Azure
portal or with the Azure CLI:

```bash
az containerapp logs show \
  --resource-group "$Azure__ResourceGroup" \
  --name e2e-runner \
  --type console \
  --follow
```

The artifacts mount becomes an Azure Files share during deployment. The
runner's `summary.md`, `run-summary.json`, determinism verdict, and raw
performance metrics are written there.

Discover and download the generated Azure Files share:

```bash
ACA_ENV="$(az containerapp env list \
  --resource-group "$Azure__ResourceGroup" \
  --query '[0].name' -o tsv)"

STORAGE_JSON="$(az containerapp env storage list \
  --resource-group "$Azure__ResourceGroup" \
  --name "$ACA_ENV" \
  --query '[0].properties.azureFile' -o json)"

STORAGE_ACCOUNT="$(jq -r '.accountName' <<< "$STORAGE_JSON")"
FILE_SHARE="$(jq -r '.shareName' <<< "$STORAGE_JSON")"
STORAGE_KEY="$(az storage account keys list \
  --resource-group "$Azure__ResourceGroup" \
  --account-name "$STORAGE_ACCOUNT" \
  --query '[0].value' -o tsv)"

mkdir -p ./azure-e2e-artifacts
az storage file download-batch \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$STORAGE_KEY" \
  --source "$FILE_SHARE" \
  --destination ./azure-e2e-artifacts
```

Then view:

```bash
cat ./azure-e2e-artifacts/summary.md
jq . ./azure-e2e-artifacts/run-summary.json
```

Azure resources incur charges while the environment exists. Delete
the disposable resource group when the experiment is complete:

```bash
az group delete \
  --name "$Azure__ResourceGroup" \
  --yes \
  --no-wait
```
