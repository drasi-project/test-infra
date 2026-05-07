# Performance Metrics Logger Example

This example demonstrates the use of the PerformanceMetrics output logger for reactions.

## Configuration

The PerformanceMetrics logger is configured in `config.json`:

```json
{
  "kind": "PerformanceMetrics",
  "filename": "reaction_performance.json"  // optional
}
```

## Output

When the test run completes, the logger writes a JSON file containing:

- `start_time_ns`: Timestamp when first record was received (nanoseconds since UNIX epoch)
- `end_time_ns`: Timestamp when stop trigger fired (nanoseconds since UNIX epoch)
- `duration_ns`: Total duration in nanoseconds
- `record_count`: Total number of records processed
- `records_per_second`: Calculated throughput
- `test_run_reaction_id`: Full reaction identifier
- `timestamp`: When the metrics were written (ISO 8601 format)

Example output file:

```json
{
  "start_time_ns": 1627849200000000000,
  "end_time_ns": 1627849260000000000,
  "duration_ns": 60000000000,
  "record_count": 150000,
  "records_per_second": 2500.0,
  "test_run_reaction_id": "local.reaction-logging-example.run_001.http-reaction",
  "timestamp": "2025-07-31T19:45:00.123456Z"
}
```

## Use Cases

- Measure reaction throughput performance
- Compare performance across different configurations
- Identify performance bottlenecks
- Generate performance reports for different test scenarios