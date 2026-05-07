# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Drasi Test Infrastructure - A comprehensive testing framework for Drasi's reactive graph intelligence platform. The repository contains:
- **E2E Test Framework**: Rust-based testing infrastructure that simulates data sources, dispatches change events, and monitors query results
- **Data Tools**: CLI utilities for working with GDELT and Wikidata external data sources

## Recent Changes (2025-08-08)

### gRPC Integration with Drasi v1 Protocol
**Breaking Change**: The gRPC implementation has been completely replaced to use Drasi's official v1 protocol:
- Custom proto files removed in favor of Drasi's official `drasi/v1/*.proto` definitions
- `GrpcSourceChangeDispatcher` now acts as a client for `drasi.v1.SourceService`
- `GrpcReactionHandler` now implements `drasi.v1.ReactionService` server
- New required configuration fields:
  - Source dispatcher: `source_id` 
  - Reaction handler: `query_ids` array, `include_initial_state` flag
- Full data model conversion between test framework and Drasi Node/Relation/Element structures

## Key Commands

### Git Hooks Setup (First Time)
```bash
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
```

### E2E Test Framework Commands

#### Building and Deployment
```bash
cd e2e-test-framework

# Build all Docker images
make

# Deploy to local Kind cluster
make kind-load

# Deploy as Drasi SourceProvider
make drasi-apply

# Build with specific platforms
make DOCKERX_OPTS="--platform linux/amd64,linux/arm64"
```

#### Development
```bash
cd e2e-test-framework

# Run test service locally (API on http://localhost:8080)
cargo run -p test-service

# Run all tests
cargo test

# Run tests for specific package
cargo test -p test-service

# Run a single test by name
cargo test test_name -- --exact

# Lint and format checks
make lint-check

# Auto-fix formatting
cargo fmt
cargo clippy --fix
```

#### Running Examples
```bash
cd e2e-test-framework

# Local population example
./examples/population/run_local

# Drasi population example (requires Kind cluster)
./examples/population/run_kind_drasi
```

### Data Tools Commands

```bash
# GDELT CLI
cd data-tools/gdelt/gdelt-cli
cargo run -- --help

# Wikidata CLI  
cd data-tools/wikidata/wikidata-cli
cargo run -- --help
```

## Architecture

### E2E Test Framework Structure
The framework is organized as a Rust workspace with interconnected services:

- **test-service**: REST API (port 8080) for test management with OpenAPI docs at `/docs`
- **test-run-host**: Core engine that orchestrates test execution
- **data-collector**: Records real-world data for test scenarios
- **test-data-store**: Storage abstraction supporting Local, Azure Blob, and GitHub
- **infrastructure**: Communication layer using Dapr for distributed messaging

### Key Architectural Patterns

1. **Storage Backends**: Abstracted storage interface allows switching between local files, Azure Blob Storage, and GitHub repositories
2. **Change Event Streaming**: Uses Redis streams for real-time query result monitoring
3. **Timing Modes**: Supports recorded (original timestamps), rebased (shifted to current time), and live (real-time) playback
4. **Distributed Communication**: Optional Dapr integration for cloud-native deployments

### Test Execution Flow
1. Load bootstrap data from test repository
2. Configure sources with change scripts
3. Start queries to monitor results via Redis
4. Dispatch changes according to timing configuration
5. Collect profiling metrics and results

### Configuration Philosophy
- **Test Definitions** (stored in repositories): Core test structure including stop triggers that define completion criteria
- **Runtime Configurations**: Execution-specific settings like loggers that control observability
- Stop triggers are intrinsic to tests (when to stop), while loggers are runtime concerns (how to observe)

## Configuration

Test configurations use JSON format. Key sections:

```json
{
  "repo": {
    "url": "path/to/test/data",
    "auth": { /* optional credentials */ }
  },
  "sources": [{
    "id": "source1",
    "name": "Test Source",
    "config": { /* source-specific config */ }
  }],
  "queries": [{
    "id": "query1",
    "name": "Test Query",
    "profiling": true
  }],
  "run": {
    "timing_mode": "recorded|rebased|live",
    "dispatcher": "console|dapr|redis|file"
  }
}
```

## API Integration

The test-service provides comprehensive REST APIs:
- `/api/repos` - Test repository management
- `/api/sources` - Data source configuration
- `/api/queries` - Query definition and monitoring
- `/api/runs` - Test execution control
- `/docs` - Interactive Swagger documentation

## Development Notes

- Pre-commit hooks automatically run `cargo fmt` - ensure they're enabled
- All Rust code should pass `cargo clippy` without warnings
- Docker images support multi-platform builds (amd64/arm64)
- Redis is required for query result streaming functionality
- Dapr sidecar is optional but enables distributed scenarios
- Test data can be stored locally or in cloud storage (Azure Blob, GitHub)

## Testing Strategy

- Unit tests: Standard Rust tests in each crate
- Integration tests: Use interactive HTTP files in `tests/interactive/`
- E2E tests: The entire framework IS the E2E testing solution for Drasi
- Performance profiling: Built into query execution with optional metrics collection

## Common Workflows

### Adding a New Test Source
1. Create bootstrap data files in your test repository
2. Record or create change scripts
3. Configure the source in your test JSON
4. Run with appropriate timing mode

### Debugging Test Failures
1. Check test-service logs at http://localhost:8080/docs
2. Monitor Redis streams for query results
3. Enable profiling on queries for performance metrics
4. Use console dispatcher for immediate change visibility