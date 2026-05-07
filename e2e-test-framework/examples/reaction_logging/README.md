# Reaction Logging Example

This example demonstrates how reaction invocations are logged to the test data cache, similar to how query results and source outputs are logged.

## Overview

When the test framework runs, reactions generate output that is stored in the test data cache under:
```
test_data_cache/test_runs/<test_run_id>/reactions/<reaction_id>/output_log/
```

## Features Demonstrated

1. **Reaction Output Storage**: Reactions write their invocation logs to a dedicated folder structure
2. **Multiple Logger Support**: Configure multiple loggers (JSONL file, console) for reaction output
3. **Integration with TestDataStore**: The TestDataStore API provides methods to access reaction output data

## Running the Example

1. First, ensure you have a test repository with reaction definitions set up
2. Run the test service:
   ```bash
   cargo run -p test-service
   ```
3. Create a test run using this configuration:
   ```bash
   curl -X POST http://localhost:8080/api/runs \
     -H "Content-Type: application/json" \
     -d @config.json
   ```

## Output Structure

After running, you'll find reaction logs at:
```
test_data_cache/
└── test_runs/
    └── <test_run_id>/
        ├── sources/
        │   └── population/
        ├── queries/
        │   └── population-query/
        └── reactions/
            └── http-reaction/
                └── output_log/
                    └── reaction_invocations/
                        └── reaction_invocations.jsonl
```

## Log Format

Reaction invocations are logged as JSONL (JSON Lines) with the following structure:
```json
{
  "id": "reaction-1",
  "sequence": 1,
  "created_time_ns": 1234567890000000000,
  "processed_time_ns": 1234567891000000000,
  "traceparent": "00-...",
  "payload": {
    "type": "ReactionInvocation",
    "reaction_type": "Http",
    "query_id": "population-query",
    "request_method": "POST",
    "request_path": "/callback",
    "request_body": {...},
    "headers": {...}
  }
}
```

## Configuration Options

### Logger Types

1. **JSONL File Logger**:
   ```json
   {
     "type": "jsonl_file",
     "output_folder": "custom_folder_name"
   }
   ```

2. **Console Logger**:
   ```json
   {
     "type": "console"
   }
   ```

3. **Profiler Logger** (for performance metrics):
   ```json
   {
     "type": "profiler",
     "flush_frequency_secs": 10
   }
   ```

## API Access

You can access reaction output data via the TestDataStore API:

```rust
// Get reaction storage
let reaction_id = TestRunReactionId::new(&test_run_id, "http-reaction");
let storage = data_store.get_test_run_reaction_storage(&reaction_id).await?;

// Access log files
let log_path = storage.reaction_output_path.join("reaction_invocations");
```

## Notes

- Reaction logging follows the same pattern as query result logging
- All reaction types (HTTP, EventGrid) are supported
- Logs include full request/response data and timing information
- The logging infrastructure is designed to be extensible for future reaction types