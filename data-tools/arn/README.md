# ARN Test Data Generator

A Rust CLI tool for generating fictional Azure Resource Notification (ARN) test data for Drasi testing. This tool creates realistic ARN Schema V3 compliant data including Management Groups, Service Groups, and their relationships.

## Overview

The ARN data generator creates two types of output files:
- **Bootstrap scripts**: Initial data snapshot with all resources
- **Source change scripts**: Debezium-style change events (inserts and updates)

## Installation

Build the tool from the `data-tools/arn` directory:

```bash
cd data-tools/arn
cargo build --release
```

## Usage

### Basic Command

```bash
cargo run -- generate -i <test-id> [OPTIONS]
```

### Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--test-id` | `-i` | Test identifier (required) | - |
| `--source-id` | `-d` | Source identifier | `arn-events` |
| `--management-group-count` | `-m` | Number of management groups | `10` |
| `--service-group-count` | `-g` | Number of service groups | `5` |
| `--relationship-count` | `-r` | Number of relationships | `20` |
| `--hierarchy-depth` | `-p` | Management group hierarchy depth | `4` |
| `--start-time` | `-s` | Start timestamp (YYYY-MM-DD HH:MM:SS) | Current time |
| `--change-count` | `-c` | Number of change events | `50` |
| `--change-duration-secs` | `-u` | Duration for changes (seconds) | `3600` |
| `--tenant-id` | `-t` | Azure tenant ID (optional) | Generated UUID |
| `--notification-format` | `-f` | Notification format (see below) | `full-payload` |
| `--batch-size` | `-b` | Resources per batch (for batched formats) | `3` |
| `--subscription-id` | | Azure subscription ID (for batched) | Generated UUID |

#### Notification Formats

The tool supports four different Event Grid notification formats:

1. **`full-payload`** - Single resource per notification with complete ARM payload
   - One Event Grid event per resource
   - Includes full `armResource` object with all properties
   - Best for: Testing individual resource notifications

2. **`payload-less`** - Single resource per notification with resourceId only
   - One Event Grid event per resource
   - Only includes `resourceId` and `correlationId`
   - ARG must make GET calls to fetch resource details
   - Best for: Testing resource lookup scenarios

3. **`batched-ids`** - Multiple resources per notification with resourceId only
   - Multiple resources grouped in single Event Grid event
   - Only includes `resourceId` and `correlationId` for each
   - Subject is subscription-level (not resource-level)
   - Best for: Testing high-volume batch scenarios with lookups

4. **`batched-payloads`** - Multiple resources per notification with full payloads
   - Multiple resources grouped in single Event Grid event
   - Includes full `armResource` object for each resource
   - Subject is subscription-level (not resource-level)
   - Best for: Testing high-volume batch scenarios without lookups

### Examples

#### Generate Small Test Dataset (Default Full Payload Format)

```bash
cargo run -- generate -i my-test -m 3 -g 2 -r 5 -p 2
```

This creates:
- 3 management groups in 2-level hierarchy
- 2 service groups
- 5 relationships between them
- 50 change events (default)
- Event Grid events with full ARM payloads

#### Generate Payload-Less Notifications

```bash
cargo run -- generate -i payloadless-test -m 10 -f payload-less
```

This creates notifications with only `resourceId` and `correlationId`, requiring ARG to fetch resource details.

#### Generate Batched Notifications (IDs only)

```bash
cargo run -- generate -i batch-ids-test -m 20 -f batched-ids -b 5
```

This creates:
- 20 management groups batched into groups of 5
- Each Event Grid event contains 5 resources (resourceId only)
- Subject is at subscription level

#### Generate Batched Notifications (Full Payloads)

```bash
cargo run -- generate -i batch-full-test -m 20 -f batched-payloads -b 5
```

This creates:
- 20 management groups batched into groups of 5
- Each Event Grid event contains 5 resources with full ARM payloads
- Subject is at subscription level

#### Generate Large Dataset with Custom Parameters

```bash
cargo run -- generate \
  -i large-test \
  -m 20 \
  -g 10 \
  -r 50 \
  -p 5 \
  -c 100 \
  -u 7200 \
  -t "12345678-1234-1234-1234-123456789012"
```

This creates:
- 20 management groups in 5-level hierarchy
- 10 service groups
- 50 relationships
- 100 change events over 2 hours
- Uses specified tenant ID

#### Specify Custom Start Time

```bash
cargo run -- generate \
  -i time-test \
  -s "2024-01-01 00:00:00" \
  -m 5 \
  -g 3 \
  -r 10
```

## Event Grid Event Structure

All generated data is wrapped in Azure Event Grid event envelopes with the following structure:

```json
{
  "topic": "custom-domain-topic/eg-topic",
  "subject": "<resource-id or subscription-id>",
  "eventType": "Microsoft.Management/managementGroups/write",
  "eventTime": "2025-12-19T22:29:50.335102+00:00",
  "id": "<unique-event-id>",
  "data": {
    "resourceLocation": "global",
    "publisherInfo": "Microsoft.Resources",
    "apiVersion": "2020-03-01-preview",
    "resources": [
      {
        "resourceId": "/providers/Microsoft.Management/managementGroups/<id>",
        "correlationId": "<unique-correlation-id>",
        "apiVersion": "2021-04-01",
        "armResource": { /* Full ARM resource object */ },
        "resourceSystemProperties": { /* System metadata */ }
      }
    ]
  },
  "dataVersion": "3.0",
  "metadataVersion": "1"
}
```

**Key differences by format:**
- **full-payload**: `subject` is resource-level, `armResource` is populated
- **payload-less**: `subject` is resource-level, only `resourceId` and `correlationId` present
- **batched-ids**: `subject` is subscription-level, multiple resources with `resourceId` only
- **batched-payloads**: `subject` is subscription-level, multiple resources with full `armResource`

## Output Structure

Generated files are placed in `./arn_data/scripts/<test-id>/sources/<source-id>/`:

```
arn_data/
└── scripts/
    └── <test-id>/
        └── sources/
            └── <source-id>/
                ├── bootstrap_scripts/
                │   └── arn_resources/
                │       └── arn_resources_00000.jsonl
                └── source_change_scripts/
                    └── source_change_scripts_00000.jsonl
```

### Bootstrap Script Format

The bootstrap script contains a header followed by Node records for all resources:

```json
{"kind":"Header","start_time":"2025-11-17T21:23:49.765379Z","description":"Drasi Bootstrap Data Script for TestID test-bootstrap, SourceID: arn-events"}
{"kind":"Node","id":"/providers/Microsoft.Management/managementGroups/mg-root-...","labels":["ManagementGroup"],"properties":{...}}
{"kind":"Node","id":"/providers/Microsoft.Management/serviceGroups/sg-...","labels":["ServiceGroup"],"properties":{...}}
{"kind":"Node","id":"/providers/microsoft.relationships/servicegroupmembers/rel-...","labels":["Relationship","ServiceGroupMember"],"properties":{...}}
```

### Source Change Script Format

The source change script contains Debezium-style change events:

```json
{"kind":"Header","start_time":"2025-11-17T21:23:49.765379Z","description":"Drasi Source Change Data Script for TestID test-bootstrap, SourceID: arn-events"}
{"kind":"SourceChange","offset_ns":1000000000,"source_change_event":{"op":"i","reactivator_start_ns":0,"reactivator_end_ns":0,"payload":{"source":{"db":"arn-events","table":"node","ts_ns":1731878629765379000,"lsn":1},"before":null,"after":{...}}}}
{"kind":"SourceChange","offset_ns":2000000000,"source_change_event":{"op":"u","reactivator_start_ns":0,"reactivator_end_ns":0,"payload":{"source":{...},"before":{...},"after":{...}}}}
```

## Generated Resource Types

### Management Groups
- Hierarchical structure with configurable depth
- Azure resource ID format: `/providers/Microsoft.Management/managementGroups/<id>`
- Properties include display name, tenant ID, parent relationships

### Service Groups
- Service-specific groups (Compute, Storage, Networking, Database)
- Azure resource ID format: `/providers/Microsoft.Management/serviceGroups/<id>`
- Properties include display name, description, tenant ID

### Relationships
- ServiceGroupMember relationships linking management groups to service groups
- Azure resource ID format: `/providers/microsoft.relationships/servicegroupmembers/<id>`
- Properties include SourceId and TargetId references

## ARN Schema V3 Compliance

All generated resources follow ARN Schema V3 structure:
- `homeTenantId` and `resourceHomeTenantId`
- `resourcesContainer: "Inline"`
- `resourceLocation: "global"`
- `publisherInfo: "Microsoft.Management"`
- `apiVersion: "2021-04-01"`
- `armResource` with full resource details
- `resourceSystemProperties` with changeAction, createdBy, createdTime
- Correlation IDs for event tracking

## Use Cases

### Drasi Testing
Use the generated data to test Drasi continuous queries:

1. Load bootstrap data to initialize the system
2. Apply source change scripts to simulate data changes
3. Verify query results and reactions

### Integration Testing
- Test multi-source data integration scenarios
- Validate graph query patterns across resources
- Test change detection and reaction pipelines

### Performance Testing
Generate large datasets to test system performance:

```bash
cargo run -- generate -i perf-test -m 100 -g 50 -r 500 -c 1000
```

## Notes

- All timestamps are in UTC
- Resource IDs are generated with unique suffixes
- Tenant IDs are consistent across all resources in a single generation
- Change events include both inserts (new resources) and updates (modified resources)
- The tool uses randomization for realistic variety in display names, creators, and timing

## Troubleshooting

### Build Warnings
The tool may show unused import warnings during build. These are non-critical and don't affect functionality.

### File Permissions
Ensure the `arn_data` directory has write permissions. The tool will create the directory structure if it doesn't exist.

### Invalid Timestamps
If providing a custom start time with `-s`, use the format: `YYYY-MM-DD HH:MM:SS`

## Related Documentation

- [ARN Summary](summary.md) - Detailed ARN schema and architecture
- [CLAUDE.md](CLAUDE.md) - ARN data generator specification
- [Drasi Documentation](https://drasi.io) - Drasi platform documentation
