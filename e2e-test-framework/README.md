# Running the Examples
The E2E Test Framework (ETF) includes examples in the ```examples``` folder that show how to use it:

The ```population``` example uses population data from wikidata and demonstrates 2 different test configurations:

1. Running the Test Service as a local process and using a local test repo
1. Running the Test Service in Drasi and using an Azure Storage hosted test repo

Both of these are fully scripted for ease of execution.

## Running the Population example in a local process

From a terminal, change to the ```test-infra/e2e-test-framework``` folder, and run:

```
./examples/population/run_local
```

## Running the Population example in Drasi
From a terminal, change to the ```test-infra/e2e-test-framework``` folder, and run:

```
./examples/population/run_kind_drasi
```

# Building the E2E Test Framework (ETF)

Do the following steps from the ```test-infra/e2e-test-framework``` folder ...

To create the docker images for the ETF:

```
make
```

This will create the following docker images and push them to the local docker repo:
- drasi-project/e2e-test-service:latest
- drasi-project/e2e-test-proxy:latest

To see that the images are in the local repo, run:

```
docker image list
```

To push the images to a local Kind cluster:

```
make kind-load
```

Publish to GHCR:

```
docker tag drasi-project/e2e-test-service:latest ghcr.io/drasi-project/e2e-test-service:0.1.6

docker push ghcr.io/drasi-project/e2e-test-service:0.1.6

docker tag drasi-project/e2e-test-proxy:latest ghcr.io/drasi-project/e2e-test-proxy:0.1.6

docker push ghcr.io/drasi-project/e2e-test-proxy:0.1.6
```

# Running E2E Test Framework on Kind

## Install Drasi on Kind Cluster
https://drasi.io/how-to-guides/installation/install-on-kind/

Create Kind cluster:

```
kind create cluster
```

Install Drasi CLI:

```
curl -fsSL https://raw.githubusercontent.com/drasi-project/drasi-platform/main/cli/installers/install-drasi-cli.sh | /bin/bash
```

Install Drasi:

```
drasi init
```

## Register E2ETestService SourceProvider

From the ```test-infra/e2e-test-framework``` folder, run:

```
make drasi-apply
```

View the registered SourceProvider:

```
drasi list sourceprovider
```

See the following output:

```
        ID
------------------
  PostgreSQL
  SQLServer
  CosmosGremlin
  Dataverse
  EventHub
  E2ETestService
```