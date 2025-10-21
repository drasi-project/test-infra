# Redis Stream CLI

A command-line utility for working with Redis streams - read stream records and list available streams.

## Overview

`redis-stream-cli` is a Rust-based CLI tool that provides an easy way to work with Redis streams. It supports:

- **Reading stream records** from any position with multiple output formats
- **Listing all streams** on a Redis server
- **Automatic JSON parsing** in the `data` field for proper JSON objects (not escaped strings)
- **Flexible output** to console or file with auto-generated filenames

**Key Feature:** Automatically parses JSON in the `data` field, outputting proper JSON objects instead of escaped JSON strings. This makes the output directly usable by JSON processors and easier to read.

## Installation

### Build from Source

```bash
cd data-tools/redis/redis-stream-cli
cargo build --release
```

The binary will be available at `target/release/redis-stream-cli`.

### Running with Cargo

```bash
cargo run -p redis-stream-cli -- [OPTIONS]
```

## Usage

### Subcommands

`redis-stream-cli` supports two subcommands:

- **`read`** - Read records from a specific Redis stream
- **`list`** - List all stream names available on a Redis server

Use `redis-stream-cli --help` to see all available subcommands, or `redis-stream-cli <subcommand> --help` for detailed help on a specific subcommand.

### `read` Subcommand

Read records from a Redis stream with various filtering and output options.

#### Arguments

- `-u, --url <REDIS_URL>`: Redis server URL (default: `redis://localhost:6379`)
  - Can also be set via `REDIS_URL` environment variable
- `-s, --stream <STREAM_NAME>`: Stream name (required)
- `-t, --timestamp <START_TIMESTAMP>`: Timestamp to read from (default: `0` for beginning)
  - Use `0` or `0-0` to start from the beginning
  - Use `$` to start from the latest entry
  - Use a specific stream ID like `1234567890-0`
- `-c, --count <COUNT>`: Number of records to read (default: unlimited)
- `-f, --file [OUTPUT_FILE]`: Write output to file (optional filename)
  - Without filename: Auto-generates filename from stream name and format (e.g., `my-stream.json`)
  - With filename: Uses the specified filename (e.g., `-f output.json`)
  - Omit flag entirely: Outputs to console
- `-o, --format <FORMAT>`: Output format: `json` or `text` (default: `json`)

#### Examples

##### Read all records from a stream and display as JSON

```bash
redis-stream-cli read -s my-stream
```

##### Read from a specific Redis server

```bash
redis-stream-cli read -u redis://myserver:6379 -s my-stream
```

##### Read only 10 records

```bash
redis-stream-cli read -s my-stream -c 10
```

##### Read from a specific timestamp

```bash
redis-stream-cli read -s my-stream -t 1609459200000-0
```

##### Read from the latest entries

```bash
redis-stream-cli read -s my-stream -t $
```

##### Auto-generate filename from stream name (creates my-stream.json)

```bash
redis-stream-cli read -s my-stream -f
```

##### Auto-generate filename with text format (creates my-stream.txt)

```bash
redis-stream-cli read -s my-stream -f -o text
```

##### Output to a specific file in JSON format

```bash
redis-stream-cli read -s my-stream -f output.json
```

##### Output to a specific file in text format

```bash
redis-stream-cli read -s my-stream -f output.txt -o text
```

##### Using environment variable for Redis URL

```bash
export REDIS_URL=redis://myserver:6379
redis-stream-cli read -s my-stream
```

##### Enable debug logging

```bash
RUST_LOG=debug redis-stream-cli read -s my-stream
```

### `list` Subcommand

List all stream names available on a Redis server. Requires Redis 6.0+ for efficient stream type filtering.

#### Arguments

- `-u, --url <REDIS_URL>`: Redis server URL (default: `redis://localhost:6379`)
  - Can also be set via `REDIS_URL` environment variable
- `-f, --file [OUTPUT_FILE]`: Write output to file (optional filename)
  - Without filename: Auto-generates filename `streams.json`
  - With filename: Uses the specified filename (e.g., `-f my-streams.json`)
  - Omit flag entirely: Outputs to console

#### Examples

##### List all streams and display on console

```bash
redis-stream-cli list
```

##### List streams from a specific Redis server

```bash
redis-stream-cli list -u redis://myserver:6379
```

##### List streams and save to auto-generated file (streams.json)

```bash
redis-stream-cli list -f
```

##### List streams and save to a specific file

```bash
redis-stream-cli list -f my-streams.json
```

##### Enable debug logging

```bash
RUST_LOG=debug redis-stream-cli list
```

#### Output Format

The `list` command outputs a JSON array of stream names, sorted alphabetically:

```json
[
  "query:room-comfort:results",
  "query:test:results",
  "stream1",
  "stream2"
]
```

## Integration with Drasi Test Infrastructure

This tool can be used to inspect query results from the Drasi test framework, which uses Redis streams to publish query results.

```bash
# List all available streams (including query result streams)
redis-stream-cli list

# Read results from a query stream
redis-stream-cli read -s query:my-query-id:results -f query-results.json

# Monitor recent results
redis-stream-cli read -s query:my-query-id:results -t $ -c 100
```

### JSON Parsing

The tool automatically parses the `data` field as JSON when it contains valid JSON. This is particularly useful for Drasi query results where the `data` field contains structured query output.

**Example:**
If a Redis stream contains:
```
data: '{"kind":"control","queryId":"test","sequence":1}'
```

The output will be:
```json
{
  "fields": {
    "data": {
      "kind": "control",
      "queryId": "test",
      "sequence": 1
    }
  }
}
```

If the `data` field is not valid JSON, it will be stored as a string.

## Output Formats

### JSON Format

Records are output as a **JSON array** with proper JSON objects (not JSON strings). The `data` field is automatically parsed if it contains valid JSON:

```json
[
  {
    "id": "1234567890-0",
    "timestamp_ms": 1234567890,
    "fields": {
      "field1": "value1",
      "field2": "value2"
    }
  }
]
```

**With parsed JSON data field:**

```json
[
  {
    "id": "1760998189031-0",
    "timestamp_ms": 1760998189031,
    "fields": {
      "data": {
        "kind": "control",
        "queryId": "room-comfort-level",
        "sequence": 1,
        "sourceTimeMs": 1760998188944,
        "controlSignal": {
          "kind": "running"
        }
      }
    }
  }
]
```

**Note:** The output is valid JSON that can be directly parsed by any JSON processor. The `data` field is a JSON object, not a JSON string.

### Text Format

Records are output as human-readable text. JSON objects in the `data` field are pretty-printed:

```
ID: 1234567890-0
Timestamp: 1234567890
Fields:
  field1: value1
  field2: value2

---
ID: 1760998189031-0
Timestamp: 1760998189031
Fields:
  data: {
  "kind": "control",
  "queryId": "room-comfort-level",
  "sequence": 1,
  "sourceTimeMs": 1760998188944,
  "controlSignal": {
    "kind": "running"
  }
}
```

## Error Handling

The tool provides clear error messages for common issues:

- Connection failures to Redis
- Non-existent streams
- Invalid stream IDs
- File write errors

Exit codes:
- `0`: Success
- `1`: Error occurred

## Development

### Running Tests

```bash
cargo test -p redis-stream-cli
```

### Code Formatting

```bash
cargo fmt
```

### Linting

```bash
cargo clippy
```

## License

Copyright 2025 The Drasi Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
