# Drasi E2E Test Service

The Test Service is a REST API component of the Drasi E2E Test Framework that manages test repositories, sources, queries, and test run execution. It provides comprehensive capabilities for simulating data sources, dispatching change events, and monitoring query results during end-to-end testing of Drasi's reactive graph intelligence platform.

## Table of Contents

- [Installation](#installation)
- [Running the Test Service](#running-the-test-service)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Examples](#examples)
- [Advanced Features](#advanced-features)

## Installation

The Test Service is built using Rust and requires a proper Rust toolchain to be installed.

```bash
# Clone the repository
git clone <repository-url>
cd drasi-test-infra/e2e-test-framework

# Build the project
cargo build -p test-service

# Or build all Docker images
make
```

## Running the Test Service

### Basic Usage

The simplest way to run the Test Service:

```bash
cargo run -p test-service
```

This starts the service with default configuration on port 63123.

### With Configuration File

```bash
cargo run -p test-service -- --config path/to/config.json
```

### With Command Line Arguments

```bash
cargo run -p test-service -- \
  --config examples/population/local/config.json \
  --data ./test_data_cache \
  --port 8080 \
  --prune
```

### With Environment Variables

```bash
export DRASI_CONFIG_FILE=examples/population/local/config.json
export DRASI_DATA_STORE_PATH=./test_data_cache
export DRASI_PORT=8080
export DRASI_PRUNE_DATA_STORE=true

cargo run -p test-service
```

### Command Line Options

| Option | Short | Environment Variable | Default | Description |
|--------|-------|---------------------|---------|-------------|
| `--config` | `-c` | `DRASI_CONFIG_FILE` | None | Path to the configuration file. If not provided, service starts uninitialized |
| `--data` | `-d` | `DRASI_DATA_STORE_PATH` | See config | Path where test data is stored. Overrides config file value |
| `--port` | `-p` | `DRASI_PORT` | 63123 | Port for the Web API |
| `--prune` | `-x` | `DRASI_PRUNE_DATA_STORE` | false | Delete data store on startup |

## Configuration

The Test Service uses a JSON configuration file with three main sections:

### Complete Configuration Example

```json
{
  "data_store": {
    "data_store_path": "./test_data_cache",
    "delete_on_start": true,
    "delete_on_stop": false,
    "data_collection_folder": "data_collections",
    "test_repo_folder": "test_repos",
    "test_run_folder": "test_runs",
    "test_repos": [
      {
        "id": "local_dev_repo",
        "kind": "LocalStorage",
        "source_path": "./dev_repo",
        "local_tests": [
          {
            "test_id": "my_test",
            "version": 1,
            "description": "Test description",
            "test_folder": "test_data_folder",
            "queries": [...],
            "sources": [...]
          }
        ]
      }
    ]
  },
  "test_run_host": {
    "queries": [...],
    "sources": [...]
  },
  "data_collector": {
    "data_collections": [...]
  }
}
```

### Data Store Configuration

The `data_store` section manages test data storage and repositories:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `data_store_path` | string | `"drasi_data_store"` | Root directory for all test data |
| `delete_on_start` | boolean | false | Delete data store when service starts |
| `delete_on_stop` | boolean | false | Delete data store when service stops |
| `data_collection_folder` | string | `"data_collections"` | Subfolder for data collections |
| `test_repo_folder` | string | `"test_repos"` | Subfolder for test repositories |
| `test_run_folder` | string | `"test_runs"` | Subfolder for test run data |
| `test_repos` | array | [] | List of test repository configurations |

#### Test Repository Types

**LocalStorage Repository:**
```json
{
  "id": "local_repo",
  "kind": "LocalStorage",
  "source_path": "./path/to/repo",
  "local_tests": [...]
}
```

**AzureBlob Repository:**
```json
{
  "id": "azure_repo",
  "kind": "AzureBlob",
  "account_name": "myaccount",
  "container_name": "test-data",
  "access_key": "...",
  "tests": [...]
}
```

**GitHub Repository:**
```json
{
  "id": "github_repo",
  "kind": "GitHub",
  "owner": "organization",
  "repo": "repository",
  "path": "test-data",
  "branch": "main",
  "token": "...",
  "tests": [...]
}
```

### Test Run Host Configuration

The `test_run_host` section defines queries and sources for test execution:

#### Query Configuration

```json
{
  "queries": [
    {
      "test_id": "my_test",
      "test_repo_id": "local_repo",
      "test_run_id": "run_001",
      "test_query_id": "query1",
      "start_immediately": true,
      "loggers": [
        {
          "kind": "Profiler",
          "write_bootstrap_log": false,
          "write_change_image": true,
          "write_change_log": true,
          "write_change_rates": true,
          "write_distributions": true,
          "max_lines_per_file": 10000
        },
        {
          "kind": "JsonlFile",
          "max_lines_per_file": 5000
        }
      ],
      "test_run_overrides": {
        "stop_trigger": {
          "kind": "RecordSequenceNumber",
          "record_sequence_number": 1000
        }
      }
    }
  ]
}
```

**Query Properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `test_id` | string | required | Test identifier |
| `test_repo_id` | string | required | Repository containing the test |
| `test_run_id` | string | auto-generated | Unique run identifier (defaults to timestamp) |
| `test_query_id` | string | required | Query identifier within the test |
| `start_immediately` | boolean | false | Start monitoring immediately |
| `loggers` | array | [] | Result logging configurations |
| `test_run_overrides` | object | null | Override test definition settings |

**Logger Types:**

- **Profiler Logger:** Captures performance metrics and statistics
  - `write_bootstrap_log`: Log bootstrap phase metrics
  - `write_change_image`: Log query result snapshots
  - `write_change_log`: Log all changes
  - `write_change_rates`: Calculate and log change rates
  - `write_distributions`: Log statistical distributions

- **JsonlFile Logger:** Writes results to JSONL files
  - `max_lines_per_file`: Maximum lines before rotating files

**Stop Trigger Types:**

- `RecordSequenceNumber`: Stop at specific record number
- `Duration`: Stop after specified time
- `ChangeCount`: Stop after number of changes

#### Source Configuration

```json
{
  "sources": [
    {
      "test_id": "my_test",
      "test_repo_id": "local_repo",
      "test_run_id": "run_001",
      "test_source_id": "source1",
      "start_mode": "auto",
      "test_run_overrides": {
        "bootstrap_data_generator": {
          "time_mode": "live"
        },
        "source_change_generator": {
          "spacing_mode": "recorded",
          "time_mode": "live"
        },
        "source_change_dispatchers": [
          { "kind": "Console" },
          { "kind": "JsonlFile" }
        ]
      }
    }
  ]
}
```

**Source Properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `test_id` | string | required | Test identifier |
| `test_repo_id` | string | required | Repository containing the test |
| `test_run_id` | string | auto-generated | Unique run identifier |
| `test_source_id` | string | required | Source identifier within the test |
| `start_mode` | string | `"bootstrap"` | Startup behavior: `auto`, `bootstrap`, or `manual` |
| `test_run_overrides` | object | null | Override test definition settings |

**Start Modes:**
- `auto`: Start generating changes immediately after bootstrap
- `bootstrap`: Load bootstrap data and wait for manual start
- `manual`: Wait for manual bootstrap and start commands

**Time Modes:**
- `recorded`: Use original timestamps from recorded data
- `rebased`: Rebase timestamps to specific start time (ISO 8601 format)
- `live`: Use current time when events are dispatched

**Spacing Modes:**
- `recorded`: Maintain original time intervals between events
- `none`: Dispatch events as fast as possible
- `fixed`: Fixed interval in milliseconds (e.g., `"fixed:100"`)
- `scaled`: Scale recorded intervals (e.g., `"scaled:0.5"` for 2x speed)

**Dispatcher Types:**
- `Console`: Log events to stdout
- `JsonlFile`: Write events to JSONL files
- `Dapr`: Publish via Dapr pubsub
- `Redis`: Publish to Redis streams

### Data Collector Configuration

The `data_collector` section configures data collection from external systems:

```json
{
  "data_collector": {
    "data_collections": [
      {
        "id": "collection1",
        "queries": [
          {
            "query_id": "query1",
            "result_event_recorders": [...],
            "result_set_loggers": [...]
          }
        ],
        "sources": [...]
      }
    ]
  }
}
```

## API Documentation

The Test Service provides a REST API on the configured port (default: 63123). Interactive API documentation is available at `/docs` when the service is running.

### Key Endpoints

#### Service Management
- `GET /` - Service information and status

#### Repository Management
- `GET /test_repos` - List repositories
- `POST /test_repos` - Add repository
- `DELETE /test_repos/{id}` - Remove repository

#### Source Management
- `GET /test_run_host/sources` - List all sources
- `GET /test_run_host/sources/{id}` - Get source state
- `POST /test_run_host/sources` - Add source
- `POST /test_run_host/sources/{id}/start` - Start source
- `POST /test_run_host/sources/{id}/pause` - Pause source
- `POST /test_run_host/sources/{id}/stop` - Stop source
- `POST /test_run_host/sources/{id}/step` - Step through changes
- `POST /test_run_host/sources/{id}/skip` - Skip changes
- `POST /test_run_host/sources/{id}/reset` - Reset to beginning
- `POST /test_run_host/sources/{id}/bootstrap` - Get bootstrap data

#### Query Management
- `GET /test_run_host/queries` - List all queries
- `GET /test_run_host/queries/{id}` - Get query state
- `POST /test_run_host/queries` - Add query
- `POST /test_run_host/queries/{id}/start` - Start monitoring
- `POST /test_run_host/queries/{id}/stop` - Stop monitoring
- `POST /test_run_host/queries/{id}/pause` - Pause monitoring
- `POST /test_run_host/queries/{id}/reset` - Reset query
- `GET /test_run_host/queries/{id}/profile` - Get query profiling data

## Examples

### Local File-Based Test

```bash
# Run population example locally
RUST_LOG=info cargo run -p test-service -- \
  --config examples/population/local/config.json
```

### Building Comfort Model Test

```bash
# Run building comfort simulation
cargo run -p test-service -- \
  --config examples/building_comfort/local/config.json \
  --data ./building_test_cache
```

### API Usage Examples

Note: Source and Query IDs follow the format: `{test_repo_id}.{test_id}.{test_run_id}.{source_id|query_id}`

```bash
# Get service state
curl http://localhost:63123/

# Start a source
curl -X POST http://localhost:63123/test_run_host/sources/local_dev_repo.population.test_run_001.geo-db/start

# Step through 100 changes
curl -X POST http://localhost:63123/test_run_host/sources/local_dev_repo.population.test_run_001.geo-db/step \
  -H "Content-Type: application/json" \
  -d '{"num_steps": 100, "spacing_mode": "none"}'

# Get query state
curl http://localhost:63123/test_run_host/queries/local_dev_repo.population.test_run_001.city-population
```

## Advanced Features

### OpenTelemetry Support

The service includes OpenTelemetry instrumentation for distributed tracing. Configure tracing by setting standard OpenTelemetry environment variables.

### Redis Integration

Query results can be streamed to Redis for real-time monitoring:

```json
{
  "result_stream_handler": {
    "kind": "RedisStream",
    "host": "localhost",
    "port": 6379,
    "stream_name": "query-results",
    "process_old_entries": true
  }
}
```

### Model-Based Data Generation

For synthetic testing, use model-based generators:

```json
{
  "model_data_generator": {
    "kind": "BuildingHierarchy",
    "seed": 123456789,
    "building_count": [10, 2],
    "floor_count": [5, 1],
    "room_count": [20, 5],
    "change_interval": [1000, 100, 50, 5000],
    "change_count": 10000
  }
}
```

### Kubernetes Deployment

Deploy as a Drasi SourceProvider:

```bash
# Build and push images
make DOCKERX_OPTS="--platform linux/amd64,linux/arm64"

# Load to Kind cluster
make kind-load

# Deploy to Drasi
make drasi-apply
```

## Troubleshooting

### Common Issues

1. **Port Already in Use**
   ```bash
   # Use a different port
   cargo run -p test-service -- --port 8090
   ```

2. **Permission Denied on Data Store**
   ```bash
   # Ensure write permissions or use --prune
   cargo run -p test-service -- --prune
   ```

3. **Redis Connection Failed**
   ```bash
   # Ensure Redis is running
   docker run -d -p 6379:6379 redis:latest
   ```

### Debug Logging

Enable detailed logging:

```bash
RUST_LOG=debug cargo run -p test-service -- --config config.json
```

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run test-service tests only
cargo test -p test-service
```

### Linting

```bash
# Check formatting and lints
make lint-check

# Auto-fix issues
cargo fmt
cargo clippy --fix
```