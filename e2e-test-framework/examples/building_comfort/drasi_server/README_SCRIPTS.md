# Drasi Server Test Scripts

This directory contains test scripts for running E2E tests with a standalone Drasi Server.

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
Permanent Drasi Server configuration file for each test.
- Defines sources, queries, and reactions
- Specifies ports and connection settings
- Contains adaptive batching settings (for adaptive tests)
- Can be modified without changing scripts

## Port Configuration

| Test Type | Source Port | Reaction Port | Server API |
|-----------|-------------|---------------|------------|
| gRPC tests | 50051 | 50052 | 8080 |
| HTTP (non-adaptive) | 9000 | 9001 | 8080 |
| HTTP (adaptive) | 8081 | 3000 | 8080 |

## Architecture

These tests use a **standalone Drasi Server** located at `../../drasi-server` (relative to e2e-test-framework).

The workflow is:
1. Build and start Drasi Server with appropriate configuration
2. Wait for server to be ready (health check on port 8080)
3. Run E2E Test Framework which:
   - Sends data to Drasi Server sources
   - Receives query results from Drasi Server reactions
   - Logs performance metrics

## Server Configuration

Each test folder contains a permanent `server-config.yaml` file with:
- Source configuration (gRPC or HTTP)
- Query definitions
- Reaction configuration
- Adaptive batching settings (for adaptive tests)

The configuration file is used by both debug and release scripts. Logging levels are controlled via the `RUST_LOG` environment variable in the scripts, not the config file.

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

## Requirements

- Rust toolchain installed
- Drasi Server repository at `../../drasi-server`
- Git submodules initialized in drasi-server:
  ```bash
  cd ../../drasi-server
  git submodule update --init --recursive
  ```