# Drasi Test Infrastructure

Testing infrastructure and tools for Drasi, a Change Processing Platform that makes it easy to build change-driven solutions.

## Repository Structure

```
test-infra/
├── e2e-test-framework/    # End-to-end testing framework
└── data-tools/            # Data utilities and CLI tools
    ├── gdelt/             # GDELT news data tools
    ├── wikidata/          # Wikidata query tools
    └── redis/             # Redis stream utilities
```

## E2E Test Framework

The `e2e-test-framework/` directory contains a comprehensive Rust-based testing framework for functional, regression, integration, and performance testing of Drasi.

**Key capabilities:**
- Simulates data sources with synthetic or recorded data
- Dispatches change events via HTTP, gRPC, Redis, or Dapr
- Monitors query results and reaction outputs
- Collects profiling metrics and performance data

**Deployment options:**
- **Standalone process** - Test Drasi Server directly
- **Kubernetes** - Test Drasi Platform in a cluster
- **Library** - Embed in custom test applications

See [e2e-test-framework/README.md](e2e-test-framework/README.md) for detailed documentation.

## Data Tools

The `data-tools/` directory contains CLI utilities for working with external data sources:

| Tool | Description |
|------|-------------|
| `gdelt/gdelt-cli` | Fetches and processes GDELT news event data |
| `wikidata/wikidata-cli` | Queries Wikidata for entity information |
| `redis/redis-stream-cli` | Utilities for Redis stream operations |

## Development Setup

### Git Hooks

Enable git hooks for code quality checks:

```bash
chmod +x .githooks/pre-commit
git config core.hooksPath .githooks
```

### Building

```bash
# E2E Test Framework
cd e2e-test-framework
cargo build

# Data Tools
cd data-tools/gdelt/gdelt-cli
cargo build
```

## License

Copyright 2025 The Drasi Authors. Licensed under the Apache License, Version 2.0.
