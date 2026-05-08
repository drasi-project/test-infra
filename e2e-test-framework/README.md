# Drasi E2E Test Framework

A comprehensive testing framework for validating Drasi, a Change Processing Platform that makes it easy to build change-driven solutions. The framework enables functional, regression, integration, and performance testing by simulating data sources, dispatching change events, and monitoring query results and reaction output.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Deployment Modes](#deployment-modes)
  - [Standalone Process (Testing drasi-lib instance)](#standalone-process-testing-drasi-lib-instance)
  - [Kubernetes Deployment (Testing Drasi Platform)](#kubernetes-deployment-testing-drasi-platform)
  - [Library Integration (Custom Testing)](#library-integration-custom-testing)
- [Configuration](#configuration)
  - [Command Line Arguments](#command-line-arguments)
  - [Environment Variables](#environment-variables)
  - [Configuration File Format](#configuration-file-format)
- [Test Repositories](#test-repositories)
  - [Local Storage](#local-storage)
  - [Azure Blob Storage](#azure-blob-storage)
  - [GitHub Repository](#github-repository)
- [Test Definitions](#test-definitions)
- [Sources](#sources)
  - [Model Sources (Synthetic Data)](#model-sources-synthetic-data)
  - [Script Sources (Recorded Data)](#script-sources-recorded-data)
  - [Source Change Dispatchers](#source-change-dispatchers)
- [Queries](#queries)
  - [Result Stream Handlers](#result-stream-handlers)
  - [Result Stream Loggers](#result-stream-loggers)
  - [Stop Triggers](#stop-triggers)
- [Reactions](#reactions)
  - [Reaction Output Handlers](#reaction-output-handlers)
  - [Output Loggers](#output-loggers)
- [drasi-lib instances](#drasi-lib-instances)
- [REST API](#rest-api)
- [Examples](#examples)
- [Development](#development)

---

## Overview

The Drasi E2E Test Framework provides:

- **Functional Testing**: Validate query logic and reaction behavior
- **Regression Testing**: Replay recorded scenarios to detect regressions
- **Integration Testing**: Test end-to-end data flow through Drasi components
- **Performance Testing**: Measure throughput, latency, and resource utilization

The framework supports multiple deployment scenarios:
1. **Standalone Process**: Test drasi-lib instance directly via HTTP/gRPC APIs
2. **Kubernetes Deployment**: Test the full Drasi Platform in a cluster
3. **Library Integration**: Embed the test engine in custom test applications

---

## Organization

The framework is organized as a Rust workspace with these components:

```
e2e-test-framework/
├── test-service/       # REST API server for test management
├── test-run-host/      # Core test execution engine (library)
├── test-data-store/    # Storage abstraction (Local, Azure, GitHub)
├── data-collector/     # Records live data for test scenarios
├── proxy/              # Source Proxy implementation for Drasi Platform integration
├── reactivator/        # Source Reactivator implementation for Drasi Platform integration
└── infrastructure/     # Communication layer (Dapr integration)
```
---

## Prerequisites

- Rust toolchain (1.75+)
- Docker (for container builds)
- Redis (for query result streaming)
- Kind/K3D (for Kubernetes testing)

---

## Deployment Modes

### Standalone Process (Testing drasi-lib instance)

Use the Test Service as a standalone process to test drasi-lib instance directly:

```bash
# Run with a configuration file
cargo run -p test-service -- \
  --config config.yaml \
  --port 8080 \
  --data /tmp/test-data

# Run in release mode for performance testing
cargo run --release -p test-service -- --config config.yaml
```

The Test Service will:
1. Load test definitions from configured repositories
2. Initialize sources that dispatch changes via HTTP/gRPC to drasi-lib instance
3. Monitor query results via Redis streams or reaction endpoints
4. Collect profiling metrics and logs

**Example configuration for HTTP dispatcher:**

```yaml
sources:
  - test_source_id: my-source
    kind: Model
    source_change_dispatchers:
      - kind: Http
        url: http://localhost
        port: 9000
        source_id: drasi-source-id
        timeout_seconds: 60
```

### Kubernetes Deployment (Testing Drasi Platform)

Deploy the Test Service to a Kubernetes cluster to test the full Drasi Platform:

#### 1. Build Docker Images

```bash
cd e2e-test-framework

# Build all images
make docker-build

# Build with specific platforms
make docker-build DOCKERX_OPTS="--platform linux/amd64,linux/arm64"

# Build debug images
make docker-build BUILD_CONFIG=debug
```

#### 2. Load Images to Cluster

```bash
# For Kind clusters
make kind-load

# For K3D clusters
make k3d-load

# Specify cluster name
make kind-load CLUSTER_NAME=my-cluster
```

#### 3. Deploy as Drasi SourceProvider

```bash
# Register the E2E Test Source Provider with Drasi
make drasi-apply

# This applies: devops/drasi/e2e_test_source_provider.yaml
```

#### 4. Create Test Sources

```yaml
apiVersion: v1
kind: Source
name: e2e-test-source
spec:
  kind: E2ETestSource
  properties:
    SOURCE_ID: my-source
    TEST_RUN_ID: repo.test.run001
    TEST_SOURCE_ID: facilities-db
    TEST_SERVICE_HOST: drasi-test-service
    TEST_SERVICE_PORT: 63123
```

### Library Integration (Custom Testing)

Use `test-run-host` as a library for custom test applications:

```rust
use std::sync::Arc;
use test_data_store::{TestDataStore, TestDataStoreConfig};
use test_run_host::{TestRunHost, TestRunHostConfig, TestRunConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create the data store
    let data_store_config = TestDataStoreConfig {
        data_store_path: Some("/tmp/test-data".to_string()),
        delete_on_start: Some(true),
        delete_on_stop: Some(true),
        ..Default::default()
    };
    let data_store = Arc::new(TestDataStore::new(data_store_config).await?);

    // Create the test run host
    let host_config = TestRunHostConfig::default();
    let test_run_host = Arc::new(TestRunHost::new(host_config, data_store.clone()).await?);

    // Add a test run programmatically
    let test_run_config = TestRunConfig {
        test_id: "my-test".to_string(),
        test_repo_id: "local-repo".to_string(),
        test_run_id: "run-001".to_string(),
        sources: vec![/* source configs */],
        queries: vec![/* query configs */],
        reactions: vec![/* reaction configs */],
        drasi_lib_instances: vec![/* server configs */],
    };

    let test_run_id = test_run_host.add_test_run(test_run_config).await?;

    // Initialize and start sources
    test_run_host.initialize_sources(test_run_host.clone()).await?;

    // Control test execution
    test_run_host.start_test_run(&test_run_id).await?;

    // ... monitor progress ...

    test_run_host.stop_test_run(&test_run_id).await?;

    Ok(())
}
```

**Key Library Types:**

```rust
// Main orchestrator
pub struct TestRunHost { /* ... */ }

// Configuration types
pub struct TestRunHostConfig { pub test_runs: Vec<TestRunConfig> }
pub struct TestRunConfig { /* test_id, sources, queries, reactions, drasi_lib_instances */ }
pub struct TestRunSourceConfig { /* source configuration */ }
pub struct TestRunQueryConfig { /* query configuration */ }
pub struct TestRunReactionConfig { /* reaction configuration */ }

// Status types
pub enum TestRunHostStatus { Initialized, Running, Error(String) }
pub enum TestRunStatus { Initialized, Running, Stopped, Error(String) }
```

**Available Methods:**

```rust
impl TestRunHost {
    // Lifecycle
    pub async fn new(config: TestRunHostConfig, data_store: Arc<TestDataStore>) -> Result<Self>;
    pub async fn add_test_run(&self, config: TestRunConfig) -> Result<TestRunId>;
    pub async fn initialize_sources(&self, self_ref: Arc<Self>) -> Result<()>;
    pub async fn start_test_run(&self, id: &TestRunId) -> Result<()>;
    pub async fn stop_test_run(&self, id: &TestRunId) -> Result<()>;
    pub async fn delete_test_run(&self, id: &TestRunId) -> Result<()>;

    // Status
    pub async fn get_status(&self) -> Result<TestRunHostStatus>;
    pub async fn get_test_run_status(&self, id: &TestRunId) -> Result<TestRunStatus>;

    // Source control
    pub async fn test_source_start(&self, id: &str) -> Result<Response>;
    pub async fn test_source_stop(&self, id: &str) -> Result<Response>;
    pub async fn test_source_pause(&self, id: &str) -> Result<Response>;
    pub async fn test_source_reset(&self, id: &str) -> Result<Response>;
    pub async fn test_source_step(&self, id: &str, steps: u64, spacing: Option<SpacingMode>) -> Result<Response>;
    pub async fn test_source_skip(&self, id: &str, skips: u64, spacing: Option<SpacingMode>) -> Result<Response>;

    // Query control
    pub async fn test_query_start(&self, id: &str) -> Result<Response>;
    pub async fn test_query_stop(&self, id: &str) -> Result<Response>;
    pub async fn test_query_pause(&self, id: &str) -> Result<Response>;
    pub async fn test_query_reset(&self, id: &str) -> Result<Response>;

    // Reaction control
    pub async fn test_reaction_start(&self, id: &str) -> Result<Response>;
    pub async fn test_reaction_stop(&self, id: &str) -> Result<Response>;
    pub async fn test_reaction_pause(&self, id: &str) -> Result<Response>;
    pub async fn test_reaction_reset(&self, id: &str) -> Result<Response>;
}
```

---

## Configuration

### Command Line Arguments

| Argument | Short | Environment Variable | Default | Description |
|----------|-------|---------------------|---------|-------------|
| `--config` | `-c` | `DRASI_CONFIG_FILE` | None | Path to configuration file (JSON or YAML) |
| `--data` | `-d` | `DRASI_DATA_STORE_PATH` | `drasi_data_store` | Override data store path |
| `--prune` | `-x` | `DRASI_PRUNE_DATA_STORE` | `false` | Delete data store on startup |
| `--port` | `-p` | `DRASI_PORT` | `63123` | REST API port |

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DRASI_CONFIG_FILE` | Configuration file path | `/app/config.yaml` |
| `DRASI_DATA_STORE_PATH` | Data store directory | `/tmp/test-data` |
| `DRASI_PRUNE_DATA_STORE` | Delete data on start | `true` |
| `DRASI_PORT` | API port number | `8080` |
| `RUST_LOG` | Log level configuration | `info,drasi_core=error` |
| `RUST_BACKTRACE` | Enable stack traces | `1` |

**Logging Configuration:**

```bash
# Basic logging
RUST_LOG=info cargo run -p test-service

# Debug logging with drasi_core suppressed (recommended)
RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' cargo run -p test-service

# Full debug logging
RUST_LOG=debug cargo run -p test-service
```

### Configuration File Format

The framework supports both **YAML** and **JSON** formats. Format detection is automatic based on content.

**Root Structure:**

```yaml
# Data store configuration
data_store:
  data_store_path: ./test_data_cache
  delete_on_start: true
  delete_on_stop: true
  data_collection_folder: data_collections  # default
  test_repo_folder: test_repos              # default
  test_run_folder: test_runs                # default
  test_repos: []                            # Repository definitions

# Test run host configuration
test_run_host:
  test_runs: []                             # Test run definitions

# Data collector configuration (optional)
data_collector:
  data_collections: []                      # Live data collection configs
```

---

## Test Repositories

Test repositories store test definitions, bootstrap data, and change scripts. Three storage backends are supported.

### Local Storage

```yaml
test_repos:
  - kind: LocalStorage
    id: local_repo
    source_path: ./examples/my_test/dev_repo

    # Optional: Inline test definitions
    local_tests:
      - test_id: my-test
        version: 1
        description: My test description
        test_folder: my_test_data
        sources: []
        queries: []
        reactions: []
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `LocalStorage` |
| `id` | string | Yes | Unique repository identifier |
| `source_path` | string | Yes | Local filesystem path |
| `local_tests` | array | No | Inline test definitions |

### Azure Blob Storage

```yaml
test_repos:
  - kind: AzureStorageBlob
    id: azure_repo
    account_name: myaccount
    access_key: ${AZURE_ACCESS_KEY}
    container: test-data
    root_path: tests/integration
    force_cache_refresh: false
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `AzureStorageBlob` |
| `id` | string | Yes | Unique repository identifier |
| `account_name` | string | Yes | Azure Storage account name |
| `access_key` | string | Yes | Account access key |
| `container` | string | Yes | Blob container name |
| `root_path` | string | No | Path prefix within container |
| `force_cache_refresh` | boolean | No | Skip local cache (default: false) |

### GitHub Repository

```yaml
test_repos:
  - kind: GitHub
    id: github_repo
    owner: my-org
    repo: test-data
    branch: main
    root_path: tests/e2e
    token: ${GITHUB_TOKEN}  # Optional, for private repos
    force_cache_refresh: false
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `kind` | string | Yes | Must be `GitHub` |
| `id` | string | Yes | Unique repository identifier |
| `owner` | string | Yes | GitHub user or organization |
| `repo` | string | Yes | Repository name |
| `branch` | string | Yes | Branch name |
| `root_path` | string | No | Path prefix within repository |
| `token` | string | No | GitHub PAT for private repos |
| `force_cache_refresh` | boolean | No | Skip local cache (default: false) |

---

## Test Definitions vs Test Runs

Understanding the distinction between Test Definitions and Test Runs is fundamental to using the framework effectively.

### Test Definition

A **Test Definition** is the static test specification stored in a test repository. It defines **what** the test does:

- **Sources**: Data sources that generate or replay changes
- **Queries**: Cypher queries to monitor with their stop triggers
- **Reactions**: Reaction outputs to capture with their stop triggers
- **Completion handlers**: Actions to execute when the test finishes

Test definitions are:
- Stored in test repositories (Local, Azure Blob, GitHub)
- Reusable across multiple test runs
- Focused on the test's intrinsic criteria (e.g., stop triggers define when the test is complete)

### Test Run

A **Test Run** is a specific execution of a test definition. It defines **how** the test executes:

- **Start modes**: Whether to start immediately or wait for API trigger
- **Loggers**: Console, file, profiler, or OpenTelemetry logging
- **Runtime overrides**: Modify source, query, or reaction behavior for this run
- **Execution context**: Unique run ID, timestamps, metrics collection

Test runs are:
- Created at runtime via config file or REST API
- Ephemeral instances of a test definition
- Focused on runtime concerns like observability and execution control

### Key Distinction

| Aspect | Test Definition | Test Run |
|--------|-----------------|----------|
| Purpose | What to test | How to execute |
| Storage | Repository (persistent) | Runtime (ephemeral) |
| Scope | Reusable template | Single execution |
| Contains | Sources, queries, reactions, stop triggers | Loggers, overrides, start flags |

### What Goes Where

**Test Definition** (stored in repository):
- Source configurations (data generators, change scripts)
- Query definitions with stop triggers
- Reaction definitions with stop triggers
- Completion handlers

**Test Run** (specified at runtime):
- `start_immediately` flag
- Query loggers (Console, JsonlFile, Profiler, OtelMetric, OtelTrace)
- Reaction output loggers (Console, JsonlFile, PerformanceMetrics)
- Override configurations for sources, queries, reactions

### Example: Same Definition, Different Runs

A single test definition can be executed multiple ways:

```yaml
# Run 1: Quick validation with console logging
run:
  start_immediately: true
  queries:
    - test_query_id: room-comfort
      loggers:
        - kind: Console

# Run 2: Performance testing with metrics collection
run:
  start_immediately: true
  queries:
    - test_query_id: room-comfort
      loggers:
        - kind: Profiler
          filename: perf_results.json
        - kind: OtelMetric
          service_name: perf-test
```

This separation allows you to define a test once and run it with different logging, timing, or override configurations as needed.

---

## Sources

Sources generate or replay data changes that are dispatched to Drasi.

### Model Sources (Synthetic Data)

Model sources generate synthetic data based on configurable parameters.

#### BuildingHierarchy Model

Generates a building hierarchy with sensors:

```yaml
sources:
  - test_source_id: facilities-db
    kind: Model

    model_data_generator:
      kind: BuildingHierarchy
      seed: 123456789                    # Random seed for reproducibility
      change_count: 100000               # Total changes to generate
      change_interval: [2000000000, 500000000, 500000000, 4000000000]
                                         # [mean_ns, std_dev_ns, min_ns, max_ns]
      spacing_mode: none                 # none | recorded | <rate>
      time_mode: live                    # live | recorded | ISO8601 timestamp
      send_initial_inserts: true         # Send bootstrap INSERTs

      # Building structure
      building_count: [5, 2]             # [mean, std_dev]
      floor_count: [3, 1]                # [mean, std_dev]
      room_count: [10, 3]                # [mean, std_dev]

      # Sensor definitions
      room_sensors:
        - kind: NormalFloat
          id: temperature
          value_init: [72, 5]            # [mean, std_dev]
          value_range: [60, 85]          # [min, max]
          value_change: [0.5, 0.2]       # [mean, std_dev]
          momentum_init: [0, 1, 0.3]     # [mean, std_dev, reversal_probability]

        - kind: NormalInt
          id: occupancy
          value_init: [5, 2]
          value_range: [0, 20]
          value_change: [1, 0.5]
          momentum_init: [0, 1, 0.5]

    source_change_dispatchers: []
    subscribers: []
```

#### StockTrade Model

Generates stock trading data:

```yaml
sources:
  - test_source_id: market-data
    kind: Model

    model_data_generator:
      kind: StockTrade
      seed: 987654321
      change_count: 50000
      spacing_mode: "1000"               # 1000 events/second
      time_mode: live

      stock_definitions:
        - symbol: AAPL
          name: Apple Inc.
        - symbol: GOOGL
          name: Alphabet Inc.
        - symbol: MSFT
          name: Microsoft Corporation

      price_init: [150, 50]              # [mean, std_dev]
      price_range: [10, 1000]            # [min, max]
      price_change: [0.5, 0.2]           # [mean, std_dev]
      price_momentum: [0, 1, 0.3]        # [mean, std_dev, reversal_prob]

      volume_init: [10000, 5000]
      volume_range: [100, 100000]
      volume_change: [500, 200]
      volume_momentum: [0, 1, 0.5]

    source_change_dispatchers: []
    subscribers: []
```

**Model Generator Common Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `kind` | string | `BuildingHierarchy` or `StockTrade` |
| `seed` | integer | Random seed for reproducibility |
| `change_count` | integer | Number of changes to generate |
| `change_interval` | array | `[mean_ns, std_dev_ns, min_ns, max_ns]` |
| `spacing_mode` | string | `none`, `recorded`, or rate (e.g., `"1000"`) |
| `time_mode` | string | `live`, `recorded`, or ISO8601 timestamp |
| `send_initial_inserts` | boolean | Send bootstrap data as INSERTs |

### Script Sources (Recorded Data)

Script sources replay recorded change scripts:

```yaml
sources:
  - test_source_id: geo-db
    kind: Script

    # Bootstrap data generator
    bootstrap_data_generator:
      kind: Script
      script_file_folder: bootstrap_scripts
      time_mode: recorded

    # Source change generator
    source_change_generator:
      kind: Script
      script_file_folder: source_change_scripts
      spacing_mode: recorded             # Preserve original timing
      time_mode: recorded                # Use original timestamps
      ignore_scripted_pause_commands: false

    source_change_dispatchers: []
    subscribers:
      - node_id: default
        query_id: city-population
```

**Script Generator Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `kind` | string | Must be `Script` |
| `script_file_folder` | string | Folder containing script files |
| `spacing_mode` | string | `none`, `recorded`, or rate |
| `time_mode` | string | `live`, `recorded`, or ISO8601 |
| `ignore_scripted_pause_commands` | boolean | Skip PAUSE commands in scripts |

### Timing Modes

**SpacingMode:**
- `none` - No delay between events (maximum throughput)
- `recorded` - Preserve original timing from recordings
- `<integer>` - Events per second (e.g., `"1000"` = 1000 events/sec)

**TimeMode:**
- `live` - Use current wall clock time
- `recorded` - Use original recorded timestamps
- ISO8601 string - Rebase to specific time (e.g., `"2025-01-15T10:00:00Z"`)

### Source Change Dispatchers

Dispatchers send generated/replayed changes to Drasi components.

#### Console Dispatcher

Logs changes to stdout (debugging):

```yaml
source_change_dispatchers:
  - kind: Console
    date_time_format: "%Y-%m-%d %H:%M:%S%.3f"  # Optional
```

#### JsonlFile Dispatcher

Writes changes to JSONL files:

```yaml
source_change_dispatchers:
  - kind: JsonlFile
    max_events_per_file: 10000  # Optional, default: unlimited
```

#### HTTP Dispatcher

Sends changes via HTTP POST:

```yaml
source_change_dispatchers:
  - kind: Http
    url: http://localhost
    port: 9000
    endpoint: /api/changes           # Optional
    source_id: drasi-source-id       # Required for Drasi
    timeout_seconds: 60              # Optional
    batch_events: false              # Optional
    adaptive_enabled: false          # Optional (adaptive batching)
    batch_size: 100                  # Optional
    batch_timeout_ms: 1000           # Optional
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | string | Required | Base URL |
| `port` | integer | Required | Port number |
| `endpoint` | string | `/` | API endpoint path |
| `source_id` | string | Required | Drasi source identifier |
| `timeout_seconds` | integer | 30 | Request timeout |
| `batch_events` | boolean | false | Send events in batches |
| `adaptive_enabled` | boolean | false | Enable adaptive batching |
| `batch_size` | integer | 100 | Max events per batch |
| `batch_timeout_ms` | integer | 1000 | Batch timeout |

#### gRPC Dispatcher

Sends changes via gRPC (Drasi v1 protocol):

```yaml
source_change_dispatchers:
  - kind: Grpc
    host: localhost
    port: 50051
    source_id: drasi-source-id       # Required
    tls: false                       # Optional
    timeout_seconds: 30              # Optional
    batch_events: false              # Optional
    adaptive_enabled: false          # Optional
    batch_size: 50                   # Optional
    batch_timeout_ms: 500            # Optional
```

#### Redis Stream Dispatcher

Publishes changes to Redis streams:

```yaml
source_change_dispatchers:
  - kind: RedisStream
    host: localhost                  # Optional
    port: 6379                       # Optional
    stream_name: source-changes      # Optional
```

#### Dapr PubSub Dispatcher

Publishes changes via Dapr:

```yaml
source_change_dispatchers:
  - kind: Dapr
    host: localhost                  # Optional
    port: 3500                       # Optional
    pubsub_name: pubsub              # Required
    pubsub_topic: source-changes     # Required
```

#### DrasiLibInstanceApi Dispatcher

Sends changes to an embedded Drasi server via API:

```yaml
source_change_dispatchers:
  - kind: DrasiLibInstanceApi
    drasi_lib_instance_id: embedded-server
    source_id: facilities-source
    timeout_seconds: 30              # Optional
    batch_events: false              # Optional
```

#### DrasiLibInstanceChannel Dispatcher

Sends changes to an embedded Drasi server via internal channel:

```yaml
source_change_dispatchers:
  - kind: DrasiLibInstanceChannel
    drasi_lib_instance_id: embedded-server
    source_id: facilities-source
    buffer_size: 2048                # Optional
```

### Source Runtime Configuration

Configure source behavior at runtime:

```yaml
test_run_host:
  test_runs:
    - test_id: my-test
      test_repo_id: local_repo
      test_run_id: run-001

      sources:
        - test_source_id: facilities-db
          start_mode: auto           # auto | bootstrap | manual

          # Override test definition settings
          test_run_overrides:
            source_change_generator:
              spacing_mode: "5000"   # 5000 events/sec
              time_mode: live
```

**Start Modes:**
- `auto` - Load bootstrap data and start changes automatically
- `bootstrap` - Only load bootstrap data, wait for manual start
- `manual` - Wait for manual start signal

---

## Queries

Query observers monitor query results from Drasi.

### Result Stream Handlers

Define where to receive query results.

#### Redis Stream Handler

```yaml
queries:
  - test_query_id: room-comfort
    result_stream_handler:
      kind: RedisStream
      host: localhost                # Optional
      port: 6379                     # Optional
      stream_name: room-comfort-results  # Optional
      process_old_entries: false     # Optional
```

#### Dapr PubSub Handler

```yaml
queries:
  - test_query_id: room-comfort
    result_stream_handler:
      kind: DaprPubSub
      host: localhost                # Optional
      port: 3500                     # Optional
      pubsub_name: pubsub            # Optional
      pubsub_topic: query-results    # Optional
```

### Result Stream Loggers

Loggers process and record query results. Configure in runtime config:

```yaml
test_run_host:
  test_runs:
    - test_id: my-test
      test_repo_id: local_repo
      test_run_id: run-001

      queries:
        - test_query_id: room-comfort
          start_immediately: true

          loggers:
            - kind: Console
              date_time_format: "%Y-%m-%d %H:%M:%S%.3f"

            - kind: JsonlFile
              max_lines_per_file: 50000

            - kind: Profiler
              write_bootstrap_log: true
              write_change_log: true
              write_change_image: true
              image_width: 1920
              max_lines_per_file: 100000

            - kind: OtelMetric
              otel_endpoint: http://localhost:4318

            - kind: OtelTrace
              otel_endpoint: http://localhost:4318
```

**Logger Types:**

| Type | Description |
|------|-------------|
| `Console` | Logs results to stdout |
| `JsonlFile` | Writes results to JSONL files |
| `Profiler` | Generates profiling data and visualizations |
| `OtelMetric` | Exports metrics via OpenTelemetry |
| `OtelTrace` | Exports traces via OpenTelemetry |

### Stop Triggers

Define when to stop monitoring:

```yaml
queries:
  - test_query_id: room-comfort

    # Stop after specific sequence number
    stop_trigger:
      kind: RecordSequenceNumber
      record_sequence_number: 10000

    # Or stop after record count
    stop_trigger:
      kind: RecordCount
      record_count: 5000
```

---

## Reactions

Reaction observers receive and process reaction outputs from Drasi.

### Reaction Output Handlers

Define how to receive reaction outputs.

#### HTTP Handler

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    output_handler:
      kind: Http
      host: localhost                # Optional
      port: 9001                     # Optional
      path: /reaction                # Optional
      correlation_header: X-Query-Sequence  # Optional
```

#### gRPC Handler (Drasi v1)

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    output_handler:
      kind: Grpc
      host: localhost                # Optional
      port: 50052                    # Optional
      query_ids:                     # Required
        - query-1
        - query-2
      include_initial_state: true    # Optional
      correlation_metadata_key: correlation-id  # Optional
```

#### EventGrid Handler

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    output_handler:
      kind: EventGrid
      endpoint: https://example.com/eventgrid
      access_key: ${EVENT_GRID_KEY}
```

#### DrasiLibInstanceCallback Handler

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    output_handler:
      kind: DrasiLibInstanceCallback
      drasi_lib_instance_id: embedded-server
      reaction_id: drasi-reaction-id
      callback_type: default         # Optional
```

#### DrasiLibInstanceChannel Handler

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    output_handler:
      kind: DrasiLibInstanceChannel
      drasi_lib_instance_id: embedded-server
      reaction_id: drasi-reaction-id
      buffer_size: 1024              # Optional
```

### Output Loggers

Configure in runtime config:

```yaml
test_run_host:
  test_runs:
    - test_id: my-test
      test_repo_id: local_repo
      test_run_id: run-001

      reactions:
        - test_reaction_id: comfort-alerts
          start_immediately: true

          output_loggers:
            - kind: Console
              date_time_format: "%Y-%m-%d %H:%M:%S%.3f"

            - kind: JsonlFile
              max_lines_per_file: 25000

            - kind: PerformanceMetrics
              filename: reaction_metrics.json  # Optional
```

**PerformanceMetrics Logger Output:**

```json
{
  "start_time_ns": 1627849200000000000,
  "end_time_ns": 1627849260000000000,
  "duration_ns": 60000000000,
  "record_count": 150000,
  "records_per_second": 2500.0,
  "test_run_reaction_id": "repo.test.run001.reaction",
  "timestamp": "2025-07-31T19:45:00Z"
}
```

### Reaction Stop Triggers

```yaml
reactions:
  - test_reaction_id: comfort-alerts
    stop_triggers:
      - kind: RecordCount
        record_count: 10000
```

---

## drasi-lib instances

Embed Drasi server functionality for in-process testing:

```yaml
local_tests:
  - test_id: embedded-test
    drasi_lib_instances:
      - id: embedded-server
        name: Test Server
        description: In-process Drasi server for testing

        config:
          # Runtime configuration
          runtime:
            worker_threads: 4
            max_blocking_threads: 512
            thread_name_prefix: drasi-worker
            enable_metrics: true

          # Storage backend
          storage:
            type: memory             # memory | file | redis
            max_size: 1000000        # For memory type
            # path: /tmp/storage     # For file type
            # persist: true          # For file type
            # url: redis://localhost # For redis type
            # key_prefix: drasi:     # For redis type

          # Authentication
          auth:
            type: none               # none | basic | token | oauth2
            # username: admin        # For basic
            # password: secret       # For basic
            # token: api-token       # For token

          # Drasi components
          sources:
            - id: facilities-source
              source_type: application
              auto_start: true
              properties: {}

          queries:
            - id: room-comfort
              query: |
                MATCH (r:Room)-[:HAS_SENSOR]->(s:Sensor)
                WHERE s.temperature > 75
                RETURN r.name, s.temperature
              sources:
                - facilities-source
              auto_start: true

          reactions:
            - id: comfort-alerts
              reaction_type: application
              queries:
                - room-comfort
              auto_start: true

          log_level: info
```

**Runtime configuration in test runs:**

```yaml
test_run_host:
  test_runs:
    - test_id: embedded-test
      test_repo_id: local_repo
      test_run_id: run-001

      drasi_lib_instances:
        - test_drasi_lib_instance_id: embedded-server
          start_immediately: true

          test_run_overrides:
            log_level: debug
            storage:
              type: memory
              max_size: 2000000
```

---

## REST API

The Test Service provides a comprehensive REST API. Interactive documentation is available at `/docs`.

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| **Service** |||
| GET | `/api/state` | Get service state |
| **Test Runs** |||
| GET | `/api/test_runs` | List all test runs |
| POST | `/api/test_runs` | Create test run |
| GET | `/api/test_runs/{id}` | Get test run details |
| DELETE | `/api/test_runs/{id}` | Delete test run |
| POST | `/api/test_runs/{id}/start` | Start test run |
| POST | `/api/test_runs/{id}/stop` | Stop test run |
| **Sources** |||
| GET | `/api/test_runs/{id}/sources` | List sources |
| POST | `/api/test_runs/{id}/sources` | Create source |
| GET | `/api/test_runs/{id}/sources/{sid}` | Get source |
| DELETE | `/api/test_runs/{id}/sources/{sid}` | Delete source |
| POST | `/api/test_runs/{id}/sources/{sid}/start` | Start source |
| POST | `/api/test_runs/{id}/sources/{sid}/stop` | Stop source |
| POST | `/api/test_runs/{id}/sources/{sid}/pause` | Pause source |
| POST | `/api/test_runs/{id}/sources/{sid}/reset` | Reset source |
| POST | `/api/test_runs/{id}/sources/{sid}/step` | Step through events |
| POST | `/api/test_runs/{id}/sources/{sid}/skip` | Skip events |
| POST | `/api/test_runs/{id}/sources/{sid}/bootstrap` | Get bootstrap data |
| **Queries** |||
| GET | `/api/test_runs/{id}/queries` | List queries |
| POST | `/api/test_runs/{id}/queries` | Create query |
| GET | `/api/test_runs/{id}/queries/{qid}` | Get query |
| DELETE | `/api/test_runs/{id}/queries/{qid}` | Delete query |
| POST | `/api/test_runs/{id}/queries/{qid}/start` | Start query observer |
| POST | `/api/test_runs/{id}/queries/{qid}/stop` | Stop query observer |
| POST | `/api/test_runs/{id}/queries/{qid}/pause` | Pause query observer |
| POST | `/api/test_runs/{id}/queries/{qid}/reset` | Reset query observer |
| **Reactions** |||
| GET | `/api/test_runs/{id}/reactions` | List reactions |
| POST | `/api/test_runs/{id}/reactions` | Create reaction |
| GET | `/api/test_runs/{id}/reactions/{rid}` | Get reaction |
| DELETE | `/api/test_runs/{id}/reactions/{rid}` | Delete reaction |
| POST | `/api/test_runs/{id}/reactions/{rid}/start` | Start reaction observer |
| POST | `/api/test_runs/{id}/reactions/{rid}/stop` | Stop reaction observer |
| **Repositories** |||
| GET | `/api/repos` | List repositories |
| POST | `/api/repos` | Create repository |
| GET | `/api/repos/{id}` | Get repository details |

---

## Development

### Git Hooks Setup

```bash
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
```

### Building

```bash
# Build all crates
cargo build

# Build in release mode
cargo build --release

# Build Docker images
make docker-build
```

### Testing

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p test-service

# Run specific test
cargo test test_name -- --exact
```

### Linting

```bash
# Check formatting and clippy
make lint-check

# Auto-fix issues
cargo fmt
cargo clippy --fix
```

### Docker Images

| Image | Description |
|-------|-------------|
| `drasi-project/e2e-test-service` | Main test service |
| `drasi-project/e2e-proxy` | Source proxy for Drasi Platform |
| `drasi-project/e2e-reactivator` | Test reactivator |

**Build Variants:**

```bash
# Default build
make docker-build BUILD_CONFIG=default

# Debug build
make docker-build BUILD_CONFIG=debug

# Azure Linux build
make docker-build BUILD_CONFIG=azure-linux

# With custom tag
make docker-build DOCKER_TAG_VERSION=v1.0.0
```

---

## License

Copyright 2025 The Drasi Authors. Licensed under the Apache License, Version 2.0.
