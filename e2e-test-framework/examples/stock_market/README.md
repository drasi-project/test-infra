# Stock Market Example - Configuration Options

The stock market example provides multiple configuration options to demonstrate different integration patterns with Drasi. Each configuration showcases the same stock trading simulation but with different architectural approaches.

## Available Configurations

### 1. Basic Local Configuration (`/local/`)
- **Purpose**: Simple standalone testing without Drasi integration
- **Use Case**: Testing the StockTradeDataGenerator in isolation
- **Output**: Console and file logging only

### 2. Internal Drasi Server (`/drasi_server_internal/`)
- **Architecture**: Embedded Drasi Server within test service
- **Communication**: In-memory channels (DrasiServerChannel)
- **Advantages**: 
  - No external dependencies
  - Fast, direct communication
  - Single process for easy debugging
- **Best For**: Local development, CI/CD, quick prototyping

### 3. External Drasi Server via gRPC (`/drasi_server_grpc/`)
- **Architecture**: Separate Drasi Server instance
- **Communication**: gRPC protocol (drasi.v1)
- **Advantages**:
  - Production-like architecture
  - Network resilience testing
  - Independent scaling
- **Best For**: Integration testing, performance benchmarking, production validation

### 4. External Drasi Server via HTTP (`/drasi_server_http/`)
- **Architecture**: Separate Drasi Server with HTTP source
- **Communication**: REST API (HTTP POST)
- **Advantages**:
  - Simple integration
  - No gRPC dependencies
  - Easy debugging with standard HTTP tools
- **Best For**: REST-based systems, webhook integrations, simple deployments

### 5. External Drasi Server via HTTP with Adaptive Batching (`/drasi_server_http_adaptive/`)
- **Architecture**: Separate Drasi Server with HTTP source
- **Communication**: REST API with intelligent batching
- **Advantages**:
  - High throughput (10x-100x improvement)
  - Automatic batch optimization
  - Reduced network overhead
- **Best For**: High-volume data ingestion, performance-critical scenarios

## Choosing a Configuration

| Scenario | Recommended Config | Reason |
|----------|-------------------|---------|
| Local Development | `drasi_server_internal` | Simple setup, fast iteration |
| CI/CD Pipeline | `drasi_server_internal` | Self-contained, no external deps |
| Integration Testing | `drasi_server_grpc` | Tests real network communication |
| REST Integration | `drasi_server_http` | Standard HTTP, no gRPC required |
| High Volume Data | `drasi_server_http_adaptive` | Optimized batching for throughput |
| Performance Testing | All configs | Compare different architectures |
| Production Validation | `drasi_server_grpc` or `http` | Matches production setup |
| Query Development | `drasi_server_internal` | Immediate feedback, easy debugging |

## Stock Market Simulation Features

All configurations use the same `StockTradeDataGenerator` that simulates:

### Stocks Included
- **Tech Giants**: MSFT, AAPL, GOOGL, AMZN, NVDA
- **Other Major**: TSLA, META, BRK.B, V, JPM

### Simulation Characteristics
- **Price Movement**: Momentum-based with configurable volatility
- **Volume Patterns**: Realistic trading volume fluctuations
- **Market Hours**: Continuous or time-based generation
- **Deterministic**: Seed-based for reproducible testing

### Generated Events
```json
{
  "op": "u",  // Update operation
  "payload": {
    "after": {
      "id": "MSFT",
      "labels": ["Stock"],
      "properties": {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 425.50,
        "volume": 52000000,
        "daily_high": 428.00,
        "daily_low": 423.25,
        "daily_open": 424.00
      }
    }
  }
}
```

## Drasi Queries

Both Drasi configurations include example Cypher queries:

### All Stocks Monitor
```cypher
MATCH (s:Stock) 
RETURN elementId(s) AS StockId, 
       s.symbol, s.name, s.price, 
       s.volume, s.daily_high, 
       s.daily_low, s.daily_open
```

### High Volume Detection
```cypher
MATCH (s:Stock) 
WHERE s.volume > 50000000 
RETURN elementId(s) AS StockId, 
       s.symbol, s.volume, s.price
```

### Significant Price Movements
```cypher
MATCH (s:Stock) 
WHERE abs(s.price - s.daily_open) / s.daily_open > 0.05 
RETURN elementId(s) AS StockId, 
       s.symbol, s.price, s.daily_open, 
       (s.price - s.daily_open) / s.daily_open AS price_change_pct
```

## Quick Start

### Running Internal Configuration
```bash
cd drasi_server_internal
./run_test.sh
```

### Running gRPC Configuration
```bash
# First, ensure external Drasi Server is running
# Then:
cd drasi_server_grpc
./run_test.sh
```

### Running HTTP Configuration
```bash
# First, ensure Drasi Server with HTTP source is running on port 9000
# Then:
cd drasi_server_http
./run_test.sh
```

### Running HTTP with Adaptive Batching
```bash
# First, ensure Drasi Server with HTTP source is running on port 9000
# Then:
cd drasi_server_http_adaptive
./run_test.sh
```

### Running with Debug Output
```bash
# For either configuration:
./run_test_debug.sh
```

## Common Operations

### Start Generation
```bash
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/start
```

### Check Status
```bash
curl http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/state
```

### Pause Generation
```bash
curl -X POST http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/pause
```

### Step Through Events
```bash
curl -X POST "http://localhost:8080/api/runs/stock_market_run_001/sources/stock-exchange/step?steps=10"
```

## Configuration Parameters

Key parameters that can be adjusted in any configuration:

```json
{
  "price_init": [150.0, 50.0],        // Initial price: mean, std dev
  "price_change": [0.5, 2.0],         // Price change per update
  "price_momentum": [5, 2.0, 0.3],    // Steps, std dev, reversal prob
  "volume_init": [10000000, 5000000], // Initial volume
  "volume_change": [100000, 50000],   // Volume change per update
  "change_count": 100000,             // Total events to generate
  "seed": 42                          // Random seed for reproducibility
}
```

## Performance Considerations

- **Internal Config**: Lowest latency, highest throughput
- **gRPC Config**: Network overhead, more realistic for production
- **Batching**: Enable for better gRPC performance with high event rates
- **Buffer Sizes**: Adjust based on event generation rate

## Further Reading

- [Internal Configuration Details](./drasi_server_internal/README.md)
- [gRPC Configuration Details](./drasi_server_grpc/README.md)
- [HTTP Configuration Details](./drasi_server_http/README.md)
- [HTTP Adaptive Batching Details](./drasi_server_http_adaptive/README.md)
- [Basic Configuration](./README.md)