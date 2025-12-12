# Reaction Output Loggers

Output loggers capture and store reaction invocation data from the E2E test framework. They follow the same pattern as query result stream loggers but are specifically designed for reaction outputs.

## Available Output Loggers

### Console Logger
Logs reaction outputs to stdout with timestamps.

**Configuration:**
```json
{
  "kind": "Console",
  "date_time_format": "%Y-%m-%d %H:%M:%S%.f"  // Optional, defaults to "%Y-%m-%d %H:%M:%S%.f"
}
```

### JSONL File Logger
Writes reaction outputs to JSONL (JSON Lines) files with automatic file rotation.

**Configuration:**
```json
{
  "kind": "JsonlFile",
  "max_lines_per_file": 10000  // Optional, defaults to 10000
}
```

## Usage Example

In your test configuration, add output loggers to reactions:

```json
{
  "reactions": [
    {
      "test_reaction_id": "my-reaction",
      "output_handler": {
        "kind": "Http",
        "port": 9001
      },
      "output_loggers": [
        {
          "kind": "Console"
        },
        {
          "kind": "JsonlFile",
          "max_lines_per_file": 5000
        }
      ]
    }
  ]
}
```

## Output Format

All loggers process `HandlerRecord` structures with the `ReactionOutput` payload type:

```rust
HandlerRecord {
    id: String,
    sequence: u64,
    created_time_ns: u64,
    processed_time_ns: u64,
    traceparent: Option<String>,
    tracestate: Option<String>,
    payload: HandlerPayload::ReactionOutput {
        reaction_output: serde_json::Value,
    },
}
```

## File Output Location

JSONL files are written to:
`<test_run_storage_path>/outputs/jsonl_file/outputs_XXXXX.jsonl`

Files are automatically rotated when they reach the configured `max_lines_per_file`.