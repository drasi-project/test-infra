# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Drasi E2E Test Framework - a Rust-based testing infrastructure for validating Drasi's reactive graph intelligence platform. The framework simulates data sources, dispatches change events, and monitors query results.

## Key Commands

### Building
```bash
# Build all Docker images
make

# Build with specific options
make DOCKERX_OPTS="--platform linux/amd64,linux/arm64"

# Push to local Kind cluster
make kind-load

# Deploy to Drasi as SourceProvider
make drasi-apply
```

### Development
```bash
# Run the test service locally
cargo run -p test-service

# Run all tests
cargo test

# Run tests for a specific package
cargo test -p test-service

# Run a specific test
cargo test test_name

# Lint check (format + clippy)
make lint-check

# Auto-fix lint issues
cargo fmt
cargo clippy --fix
```

### Running Examples
```bash
# Local population example
./examples/population/run_local

# Drasi population example (requires Kind cluster)
./examples/population/run_kind_drasi
```

## Architecture

The project is a Rust workspace with these main components:

- **test-service**: REST API for managing test runs (port 8080, `/docs` for OpenAPI)
- **test-run-host**: Core test execution engine that orchestrates sources and queries
- **data-collector**: Records test data from external systems
- **proxy**: Routes test traffic between components
- **reactivator**: Reactivates test scenarios for replay
- **test-data-store**: Storage layer supporting Local, Azure Blob, and GitHub backends
- **infrastructure**: Dapr-based messaging abstractions

## Key Concepts

### Test Repository
- Contains bootstrap data and change scripts
- Supports multiple storage backends (Local filesystem, Azure Blob, GitHub)
- JSON-based configuration files

### Test Sources
- Simulate data sources by replaying recorded changes
- Support multiple timing modes: recorded, rebased, live
- Can generate change events with configurable spacing

### Test Queries
- Monitor query results through Redis streams
- Built-in profiling and performance metrics
- Support for various output formats

### Change Dispatchers
- Console: Logs to stdout
- Dapr: Publishes via Dapr pubsub
- Redis: Publishes to Redis streams
- File: Writes to local files

## API Endpoints

Main REST API endpoints (test-service):
- `/api/repos` - Manage test repositories
- `/api/sources` - Configure test sources
- `/api/queries` - Define test queries
- `/api/runs` - Execute and monitor test runs
- `/docs` - Interactive API documentation

## TestDataStore Cleanup Behavior

The test service properly handles cleanup of test data when configured with `delete_on_stop: true`:
- **Signal Handling**: Explicit cleanup is performed when receiving SIGINT (Ctrl+C) or SIGTERM signals
- **Async-Safe**: Uses async I/O operations to avoid blocking during shutdown
- **No Double Cleanup**: Tracks cleanup state to prevent duplicate cleanup between signal handler and Drop trait
- **Graceful Shutdown**: Ensures test data is cleaned up before the service terminates

This ensures that temporary test data directories are properly removed even when the service is interrupted with Ctrl+C.

## Configuration

Test configurations use JSON format with these key sections:
- `repo`: Repository location and credentials
- `sources`: Data source definitions
- `queries`: Query definitions with profiling options
- `run`: Execution parameters (timing, dispatch, etc.)

## Integration with Drasi

The framework deploys as a Drasi SourceProvider:
1. Build Docker images with `make`
2. Load to Kind cluster with `make kind-load`
3. Deploy provider with `make drasi-apply`
4. Create sources using the E2ETestService provider type

## Important Notes

- Always run `make lint-check` before committing
- The service includes OpenTelemetry for distributed tracing
- Test data can include bootstrap files and change scripts
- Redis is required for query result streaming
- Dapr sidecar is optional but recommended for distributed scenarios

## gRPC Integration with Drasi (2025-08-08)

**Breaking Change**: The gRPC implementation has been completely replaced to use Drasi's official v1 protocol:
- The custom proto files (`source_dispatcher.proto`, `reaction_handler.proto`) have been removed
- Now uses Drasi's official proto definitions from `drasi/v1/*.proto`
- `GrpcSourceChangeDispatcher` now implements `drasi.v1.SourceService` client
- `GrpcReactionHandler` now implements `drasi.v1.ReactionService` server
- Configuration changes:
  - Source dispatcher requires `source_id` field
  - Reaction handler requires `query_ids` array and optional `include_initial_state`
- Data model conversions handle Drasi's Node/Relation/Element structure
- This is a breaking change - existing gRPC configurations must be updated

## Configuration Changes (2025-07-25)

**Breaking Change**: Logger configurations have been moved from test definitions to runtime configurations:
- Query loggers should now be specified in `TestRunQueryConfig` instead of `TestQueryDefinition`
- Reaction output loggers should now be specified in `TestRunReactionConfig` instead of `TestReactionDefinition`
- This allows different logging strategies when running the same test multiple times
- Test definitions in repositories should only contain the core test structure, not runtime concerns like logging

**Important**: Stop triggers remain in test definitions:
- Query stop triggers are specified in `TestQueryDefinition.stop_trigger`
- Reaction stop triggers are specified in `TestReactionDefinition.stop_triggers`
- Stop triggers define test completion criteria and are intrinsic to the test itself
- Runtime overrides for stop triggers are available via `TestRunQueryOverrides` and `TestRunReactionOverrides`

## drasi-lib instance Full Configuration (2025-07-28)

**New Feature**: drasi-lib instances can now be fully configured with Sources, Queries, and Reactions:
- Add `sources`, `queries`, and `reactions` arrays to `DrasiLibInstanceConfig`
- TestSources can send data to configured sources via `DrasiLibInstanceChannel` dispatcher
- TestReactions can receive data from configured reactions via `DrasiLibInstanceChannel` handler
- The framework validates that TestSource/TestReaction IDs match configured component names
- See `examples/building_comfort/drasi_lib_instance_internal` for a complete example

## Embedded drasi-lib Instance Hosting (2026-05-06)

**Breaking change**: embedded hosting is now called a drasi-lib instance, not a Drasi Server. External Drasi Server gRPC/HTTP scenarios remain separate and keep their Drasi Server terminology.

- Test repo schema: `drasi_servers` → `drasi_lib_instances`; `test_drasi_server_id` → `test_drasi_lib_instance_id`.
- Runtime config: `TestRunConfig.drasi_servers` → `drasi_lib_instances`.
- Storage folder: `drasi_servers` → `drasi_lib_instances`.
- REST routes: `/api/test_runs/.../drasi_servers[/{id}]` → `/api/test_runs/.../drasi_lib_instances[/{id}]`.
- Dispatcher/handler kinds: `DrasiServerChannel` → `DrasiLibInstanceChannel`, `DrasiServerCallback` → `DrasiLibInstanceCallback`.
- Removed `DrasiServerApi` source dispatcher; embedded drasi-lib instances do not expose an HTTP endpoint.

Instance config uses published crates: `drasi-lib`, `drasi-core`, `drasi-bootstrap-noop`, `drasi-bootstrap-application`, `drasi-source-application`, and `drasi-reaction-application`. The workspace `Cargo.toml` includes a commented `[patch.crates-io]` block for local Drasi core development, following the drasi-server pattern.

Schema shape:

```json
{
  "drasi_lib_instances": [{
    "test_drasi_lib_instance_id": "internal-drasi-lib",
    "name": "Internal drasi-lib instance",
    "description": "Embedded drasi-lib instance for testing",
    "config": {
      "log_level": "info",
      "sources": [{ "id": "source1", "kind": "application", "auto_start": true, "config": {} }],
      "queries": [{ "id": "query1", "query": "MATCH (n) RETURN n", "sources": ["source1"], "auto_start": true, "config": {} }],
      "reactions": [{ "id": "reaction1", "kind": "application", "queries": ["query1"], "auto_start": true, "config": {} }]
    }
  }]
}
```

Lifecycle: `DrasiLib::builder().with_id(...).with_source(...).with_query(...).with_reaction(...).build().await?`, then `core.start().await?`; shutdown uses `core.shutdown().await?`. Only `kind == "application"` sources and reactions are supported initially.

## External Drasi Server Testing

External Drasi Server scenarios use gRPC/HTTP dispatchers and handlers (`GrpcSourceChangeDispatcher`, `GrpcReactionHandler`, HTTP dispatchers/handlers) and the `drasi.v1.SourceService` / `drasi.v1.ReactionService` protocol. Do not rename these external Drasi Server concepts when changing embedded drasi-lib instance hosting.

## Logging Configuration (2025-07-31)

**Suppressing drasi_core Logs**: The drasi_core library uses the `tracing` crate with `#[tracing::instrument]` attributes that generate INFO level logs. To suppress these logs while keeping other logs visible:

```bash
# Set drasi_core modules to error level
RUST_LOG='info,drasi_core::query::continuous_query=error,drasi_core::path_solver=error' cargo run ...
```

**Important Notes**:
- Use `error` level instead of `off` for drasi_core modules (due to tracing/log interop)
- The test-service uses `env_logger` which bridges tracing events to log events
- Apply this pattern to both regular and debug test scripts

## Reaction Output Loggers (2025-07-31)

**Performance Metrics Logger**: A new output logger that tracks timing and throughput metrics for reactions:

```json
{
  "output_loggers": [
    {
      "kind": "PerformanceMetrics",
      "filename": "custom_metrics.json"  // optional, defaults to performance_metrics_TIMESTAMP.json
    }
  ]
}
```

The logger captures:
- First record timestamp (nanoseconds)
- Last record timestamp when stop trigger fires
- Total record count
- Records per second throughput

Output format:
```json
{
  "start_time_ns": 1627849200000000000,
  "end_time_ns": 1627849260000000000,
  "duration_ns": 60000000000,
  "record_count": 150000,
  "records_per_second": 2500.0,
  "test_run_reaction_id": "test_repo.test_id.run_001.reaction_001",
  "timestamp": "2025-07-31T19:45:00Z"
}
```