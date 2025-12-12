# Drasi Server Redis Example

This example demonstrates using Redis as the communication mechanism between the E2E Test Framework (E2ETF) and Drasi Server for the Building Comfort test scenario.

## Overview

This test configuration uses Redis streams for bidirectional communication:
- **Source Data Flow**: E2ETF → Redis Stream (`drasi:source:facilities-db`) → Drasi Server
- **Reaction Results Flow**: Drasi Server → Redis Stream (`drasi:reaction:building-comfort:results`) → E2ETF

## Architecture

```
┌─────────────────┐          Redis Streams           ┌──────────────┐
│                 │  ──────────────────────────────>  │              │
│   E2E Test      │   drasi:source:facilities-db     │    Drasi     │
│   Framework     │                                   │    Server    │
│                 │  <──────────────────────────────  │              │
└─────────────────┘  drasi:reaction:building-comfort └──────────────┘
                            :results
```

## Prerequisites

1. **Docker**: Required for running Redis container
2. **Rust**: Required for building and running the test framework
3. **Drasi Server**: The drasi-server directory should be at `../../../../drasi-server` relative to this example

## Files

- `config.json` - E2ETF configuration with Redis dispatchers and handlers
- `server-config.yaml` - Drasi Server configuration with Redis source and reaction
- `run_test.sh` - Main test execution script
- `run_redis.sh` - Redis container management utility
- `README.md` - This file

## Configuration Details

### E2ETF Configuration (`config.json`)

The test framework is configured with:
- **Source Dispatcher**: Sends changes to Redis stream `drasi:source:facilities-db`
  - Batch size: 100 events
  - Batch timeout: 100ms
  - Max stream length: 10,000 entries
- **Reaction Handler**: Reads from Redis stream `drasi:reaction:building-comfort:results`
  - Max stream length: 10,000 entries
- **Model Generator**: Generates 100,000 building hierarchy changes
- **Stop Trigger**: Completes after processing 100,000 records

### Drasi Server Configuration (`server-config.yaml`)

The Drasi Server is configured with:
- **Redis Source**: Consumes from `drasi:source:facilities-db` stream
  - Consumer group: `drasi-server`
  - Batch size: 100 events
  - Block timeout: 1 second
- **Query**: Simple room matching query
- **Redis Reaction**: Publishes to `drasi:reaction:building-comfort:results` stream
  - Batch size: 1,000 events
  - Batch timeout: 100ms

## Running the Test

### Quick Start

```bash
# Run the complete test (starts Redis, Drasi Server, and E2ETF)
./run_test.sh
```

The script will:
1. Start a Redis container
2. Build and start Drasi Server
3. Run the E2E Test Framework
4. Display results and performance metrics
5. Clean up all processes

### Manual Steps

If you want to run components separately:

```bash
# 1. Start Redis
./run_redis.sh start

# 2. Check Redis status
./run_redis.sh status

# 3. Run the test
./run_test.sh

# 4. View Redis streams
./run_redis.sh cli
# Then use commands like:
#   XLEN drasi:source:facilities-db
#   XRANGE drasi:source:facilities-db - + COUNT 10

# 5. Clean up
./run_redis.sh stop
```

## Redis Management

The `run_redis.sh` script provides comprehensive Redis management:

```bash
# Start Redis container
./run_redis.sh start

# Stop Redis (preserves data)
./run_redis.sh stop

# Restart Redis
./run_redis.sh restart

# Check status and stream info
./run_redis.sh status

# View Redis logs
./run_redis.sh logs

# Connect to Redis CLI with Drasi hints
./run_redis.sh cli

# Clear all Drasi streams
./run_redis.sh clear

# Remove container and all data
./run_redis.sh remove
```

## Monitoring

### During Test Execution

Monitor Redis streams in real-time:

```bash
# In a separate terminal, connect to Redis
./run_redis.sh cli

# Monitor source stream
XLEN drasi:source:facilities-db

# Monitor reaction stream
XLEN drasi:reaction:building-comfort:results

# View latest entries
XRANGE drasi:source:facilities-db - + COUNT 5
```

### Test Output

The test produces:
1. **Console Output**: Real-time progress and status
2. **Drasi Server Log**: `drasi-server.log`
3. **Performance Metrics**: `test_data_cache/performance_metrics_*.json`
4. **JSONL Files**: Event logs in `test_data_cache/`

## Troubleshooting

### Redis Connection Issues

```bash
# Check if Redis is running
./run_redis.sh status

# Check Docker containers
docker ps | grep redis

# Test Redis connectivity
docker exec drasi-test-redis redis-cli ping
```

### Stream Data Issues

```bash
# Check stream contents
./run_redis.sh cli
KEYS drasi:*
XINFO STREAM drasi:source:facilities-db

# Clear streams and retry
./run_redis.sh clear
```

### Performance Tuning

Adjust these parameters in the configuration files:
- **Batch Size**: Increase for higher throughput
- **Batch Timeout**: Decrease for lower latency
- **Max Stream Length**: Increase for larger tests
- **Block Timeout**: Adjust based on event rate

## Comparison with Other Examples

| Example | Communication | Use Case |
|---------|--------------|----------|
| drasi_server_grpc | gRPC protocol | Direct RPC communication |
| drasi_server_http | HTTP REST | Web-based integration |
| **drasi_server_redis** | **Redis Streams** | **Pub/Sub with persistence** |
| drasi_server_internal | In-memory channels | Embedded testing |

### Advantages of Redis Communication

- **Persistence**: Events are stored and can be replayed
- **Decoupling**: Components can run independently
- **Monitoring**: Easy to inspect stream contents
- **Scalability**: Supports multiple consumers/producers
- **Recovery**: Can resume from last processed message

## Clean Up

To completely clean up after testing:

```bash
# Stop and remove Redis container
./run_redis.sh remove

# Remove test data cache
rm -rf test_data_cache/

# Remove log files
rm -f drasi-server.log
```