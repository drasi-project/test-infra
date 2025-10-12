# Drasi Server Test Scripts

This directory contains test scripts for running E2E tests with a standalone Drasi Server. These tests demonstrate how the E2E Test Framework (E2ETF) and Drasi Server communicate to process streaming data.

## Test Folders

Each subfolder contains a different test configuration:
- `drasi_server_grpc/` - gRPC-based communication test
- `drasi_server_grpc_adaptive/` - gRPC with adaptive batching
- `drasi_server_http/` - HTTP-based communication test
- `drasi_server_http_adaptive/` - HTTP with adaptive batching

## Scripts in Each Folder

### 1. `run_debug.sh`
Runs tests with **debug** build profile and debug-level logging.
- Builds Drasi Server in debug mode
- Builds E2E Test Framework in debug mode
- Uses `RUST_LOG=debug` for maximum verbosity
- Uses the permanent `server-config.yaml` file
- Logs output to `drasi-server-debug.log`

**Usage from e2e-test-framework directory:**
```bash
./examples/building_comfort/drasi_server/drasi_server_grpc/run_debug.sh
```

### 2. `run_release.sh`
Runs tests with **release** build profile and info-level logging.
- Builds both components with `--release` flag
- Uses optimized builds for performance testing
- Suppresses noisy drasi_core logs
- Uses the permanent `server-config.yaml` file
- Logs output to `drasi-server-release.log`

**Usage from e2e-test-framework directory:**
```bash
./examples/building_comfort/drasi_server/drasi_server_grpc/run_release.sh
```

### 3. `stop.sh`
Terminates both Drasi Server and E2E Test Framework processes.
- Attempts graceful shutdown first (SIGTERM)
- Force kills if processes don't stop within 5 seconds
- Cleans up processes on specific test ports
- Preserves log files for debugging

**Usage from e2e-test-framework directory:**
```bash
./examples/building_comfort/drasi_server/drasi_server_grpc/stop.sh
```

### 4. `server-config.yaml`
Drasi Server configuration file that defines the server-side components.
- **Sources**: Configures gRPC or HTTP endpoints to receive data from E2ETF
- **Queries**: Defines the Cypher queries to process incoming data
- **Reactions**: Configures how query results are sent back to E2ETF
- Contains adaptive batching settings (for adaptive tests)
- Can be modified without changing scripts

### 5. `config.json`
E2E Test Framework configuration that defines test execution.
- **Test Sources**: Generates synthetic Building Hierarchy data using model generators
- **Source Dispatchers**: Sends data to Drasi Server via gRPC or HTTP
- **Reaction Handlers**: Receives query results from Drasi Server reactions
- **Stop Triggers**: Defines when tests complete (e.g., after 100,000 records)
- **Output Loggers**: Records test data and performance metrics

## Port Configuration

| Test Type | Source Port | Reaction Port | Server API |
|-----------|-------------|---------------|------------|
| gRPC tests | 50051 | 50052 | 8080 |
| gRPC adaptive | 50051 | 50052 | 8080 |
| HTTP tests | 9000 | 9001 | 8080 |
| HTTP adaptive | 9000 | 9001 | 8080 |

## Architecture

These tests use a **standalone Drasi Server** located at `../../drasi-server` (relative to e2e-test-framework).

### Data Flow
1. **E2E Test Framework** generates synthetic Building Hierarchy data
2. **Source Dispatchers** (in E2ETF) send data to Drasi Server sources via gRPC or HTTP
3. **Drasi Server** processes the data through configured queries (Cypher)
4. **Drasi Reactions** send query results back to E2ETF reaction handlers
5. **E2ETF Reaction Handlers** receive results and log performance metrics

The workflow is:
1. Build and start Drasi Server with appropriate configuration
2. Wait for server to be ready (health check on port 8080)
3. Run E2E Test Framework which:
   - Generates and sends test data to Drasi Server sources
   - Receives query results from Drasi Server reactions
   - Records performance metrics and test results

## Configuration Files

### Server Configuration (`server-config.yaml`)
Defines Drasi Server components:
- **Sources**: Endpoints to receive data (gRPC on port 50051 or HTTP on port 9000)
- **Queries**: Cypher queries to process data (e.g., `MATCH (r:Room) RETURN r`)
- **Reactions**: How to send results back (gRPC to port 50052 or HTTP to port 9001)
- **Adaptive Batching**: Optional settings for optimizing throughput

### E2ETF Configuration (`config.json`)
Defines test execution parameters:
- **Model Data Generator**: Creates synthetic Building Hierarchy data with rooms, sensors (temperature, CO2, humidity)
- **Source Dispatchers**: Sends generated data to Drasi Server sources
  - gRPC: Connects to `localhost:50051`
  - HTTP: Connects to `localhost:9000`
- **Reaction Handlers**: Listens for query results from Drasi Server
  - gRPC: Listens on `0.0.0.0:50052`
  - HTTP: Listens on port `9001` at path `/reaction`
- **Stop Triggers**: Test completes after processing 100,000 records
- **Output Loggers**: JsonlFile and PerformanceMetrics loggers

## Communication Protocol

### gRPC Tests
- E2ETF → Drasi: Uses Drasi v1 protocol `drasi.v1.SourceService` (port 50051)
- Drasi → E2ETF: Uses Drasi v1 protocol `drasi.v1.ReactionService` (port 50052)
- Correlation: Uses `x-query-sequence` metadata key for tracking

### HTTP Tests
- E2ETF → Drasi: POST requests with JSON payload (port 9000)
- Drasi → E2ETF: POST to `/reaction` endpoint (port 9001)
- Correlation: Uses `X-Query-Sequence` header for tracking

### Adaptive Batching
When enabled, optimizes throughput by:
- Dynamically adjusting batch sizes based on load
- Balancing between latency and throughput
- Configurable min/max batch sizes and wait times

## Log Files

- `drasi-server-debug.log` - Server output from debug runs
- `drasi-server-release.log` - Server output from release runs
- Test data and metrics are saved in `test_data_cache/`

## Troubleshooting

If tests fail to start:
1. Run the stop script to clean up any lingering processes
2. Check that ports are not in use: `lsof -i:8080`
3. Ensure Drasi Server directory exists at `../../drasi-server`
4. Check log files for error messages

## Recent Updates (October 2025)

### Test Configuration
- Tests now use separate configuration files:
  - `server-config.yaml`: Drasi Server component configuration
  - `config.json`: E2E Test Framework test execution settings
- All examples have been updated to use the official Drasi v1 protocol for gRPC communication
- HTTP examples use standard REST endpoints for data exchange

### Data Transfer Mechanisms
The E2ETF and Drasi Server communicate through two channels:

1. **Source Data Transfer** (E2ETF → Drasi):
   - gRPC: Uses `drasi.v1.SourceService` protocol
   - HTTP: Uses POST requests with JSON payloads

2. **Query Results Transfer** (Drasi → E2ETF):
   - gRPC: Implements `drasi.v1.ReactionService` server
   - HTTP: Receives POST requests at `/reaction` endpoint

### Performance Testing
- All tests generate 100,000 synthetic Building Hierarchy records
- Performance metrics are automatically captured and saved
- Results include throughput (records/second) and timing data

## Requirements

- Rust toolchain installed
- Drasi Server repository at `../../drasi-server`
- Git submodules initialized in drasi-server:
  ```bash
  cd ../../drasi-server
  git submodule update --init --recursive
  ```