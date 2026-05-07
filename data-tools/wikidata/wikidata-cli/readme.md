# Wikidata CLI

A command-line tool for downloading Wikidata item revisions and generating test scripts for the Drasi E2E Test Framework. This tool can download items by type, specific items by ID, and convert the downloaded data into bootstrap and change scripts.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Download Items by Type](#download-items-by-type)
  - [Download Specific Items](#download-specific-items)
  - [Generate Test Scripts](#generate-test-scripts)
- [Commands Reference](#commands-reference)
- [Configuration](#configuration)
- [Item Types](#item-types)
- [Script Output Format](#script-output-format)
- [Examples](#examples)
- [Wikidata Schema](#wikidata-schema)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## Features

- **Download Wikidata items by type** - Automatically downloads all items of a specific type (continent, country, city)
- **Download specific items by ID** - Download revisions for specific Wikidata item IDs
- **Revision history support** - Download complete revision history with date range filtering
- **Generate E2E test scripts** - Creates bootstrap and change scripts compatible with Drasi's test framework
- **Concurrent downloads** - Parallel processing with semaphore-controlled concurrency
- **Incremental downloads** - Skip already downloaded revisions for efficient re-runs
- **Configurable caching** - Local file cache for downloaded item revisions

## Installation

### Prerequisites

- Rust toolchain (1.70 or later)
- Internet connection (for downloading Wikidata)

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd wikidata-cli

# Build the project
cargo build --release

# Run directly with cargo
cargo run -- --help

# Or install locally
cargo install --path .
```

## Usage

### Download Items by Type

Download all items of a specific type (continent, country, or city) with their revision history.

```bash
# Download all countries with their latest revision
wikidata get-types -t country

# Download all cities with revisions from 2024
wikidata get-types -t city -s 2024-01-01 -e 2024-12-31

# Download with specific batch size and revision count
wikidata get-types -t continent -b 10 -c 50

# Download and automatically generate scripts
wikidata get-types -t country -g

# Download multiple types
wikidata get-types -t country,city -s 2023-01-01
```

### Download Specific Items

Download revisions for specific Wikidata items by their IDs.

```bash
# Download revisions for London (Q84)
wikidata get-items -i Q84

# Download multiple cities
wikidata get-items -t city -i Q84,Q90,Q1490

# Download with date range
wikidata get-items -i Q84 -s 2023-01-01 -e 2023-12-31

# Download with custom batch size
wikidata get-items -i Q84 -b 5 -c 100
```

### Generate Test Scripts

Convert downloaded item revisions into Drasi E2E test scripts.

```bash
# Generate scripts for a test ID
wikidata make-script -i test001

# Generate scripts for specific item types
wikidata make-script -i test001 -t country,city

# Generate with date range
wikidata make-script -i test001 -s 2023-01-01 -e 2023-12-31

# Generate with custom source ID
wikidata make-script -i test001 -d my-wikidata-source

# Overwrite existing scripts
wikidata make-script -i test001 -o
```

## Commands Reference

### Global Options

- `-c, --cache <PATH>` - Wikidata cache directory (default: `./wiki_data_cache`)
  - Can also be set via `WIKIDATA_CACHE_PATH` environment variable

### Commands

#### `get-types` - Download Items by Type

Downloads all Wikidata items of specified types.

**Options:**
- `-b, --batch-size <SIZE>` - Size of revision download batches (default: 10)
- `-t, --item-types <TYPE>` - Item types to download: city, continent, country (comma-separated)
- `-c, --rev-count <COUNT>` - Maximum number of revisions per item
- `-s, --rev-start <DATETIME>` - Start datetime for revisions
- `-e, --rev-end <DATETIME>` - End datetime for revisions
- `-o, --overwrite` - Overwrite existing data
- `-g, --script` - Automatically generate test scripts after download

#### `get-items` - Download Specific Items

Downloads revisions for specific Wikidata item IDs.

**Options:**
- `-b, --batch-size <SIZE>` - Size of revision download batches (default: 10)
- `-i, --item-ids <IDS>` - Item IDs to download (comma-separated, default: Q84)
- `-t, --item-type <TYPE>` - Item type (default: city)
- `-c, --rev-count <COUNT>` - Maximum number of revisions
- `-s, --rev-start <DATETIME>` - Start datetime for revisions
- `-e, --rev-end <DATETIME>` - End datetime for revisions
- `-o, --overwrite` - Overwrite existing data
- `-g, --script` - Automatically generate test scripts after download

#### `make-script` - Generate Test Scripts

Generates bootstrap and change scripts from downloaded data.

**Options:**
- `-t, --item-types <TYPE>` - Item types to include in scripts
- `-s, --rev-start <DATETIME>` - Start datetime for script generation
- `-e, --rev-end <DATETIME>` - End datetime for script generation
- `-d, --source-id <ID>` - Source ID for scripts (default: "wikidata")
- `-i, --test-id <ID>` - Test ID for script generation (required)
- `-o, --overwrite` - Overwrite existing scripts

## Configuration

### Date/Time Format

All datetime parameters support various formats including:
- ISO 8601: `2024-01-15T14:30:00`
- Date only: `2024-01-15` (defaults to 00:00:00)
- With timezone: `2024-01-15T14:30:00Z`

See [chrono documentation](https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str) for all supported formats.

### Cache Directory Structure

```
wiki_data_cache/
├── items/
│   ├── city/
│   │   ├── Q84/                    # London
│   │   │   ├── 2024-01-01_12-00-00Z.json
│   │   │   ├── 2024-01-02_15-30-00Z.json
│   │   │   └── ...
│   │   └── Q90/                    # Paris
│   │       └── ...
│   ├── country/
│   │   └── Q30/                    # United States
│   │       └── ...
│   └── continent/
│       └── Q15/                    # Africa
│           └── ...
└── scripts/
    └── test001/
        └── sources/
            └── wikidata/
                ├── bootstrap_scripts/
                │   ├── City_00000.jsonl
                │   ├── Country_00000.jsonl
                │   └── ...
                └── source_change_scripts_00000.jsonl
```

## Item Types

The Wikidata CLI supports three item types:

### Continent (`Q5107`)
- Large geographical regions
- Properties: area, coordinate location, name, population

### Country (`Q6256`)
- Sovereign states and territories
- Properties: area, continent_id, coordinate location, name, population
- Links to: continent

### City (`Q515`)
- Urban settlements
- Properties: area, coordinate location, country_id, name, population
- Links to: country

## Script Output Format

The generated scripts follow the Drasi E2E Test Framework format using JSONL (JSON Lines).

### Bootstrap Scripts

Located in `bootstrap_scripts/` subdirectory, organized by item type:

Example bootstrap record:
```json
{"kind":"Node","id":"Q84","labels":["City"],"properties":{"name":"London","population":8982000,"area":1572.0,"coordinate_location":"51.507222222222,-0.1275","country_id":"Q145"}}
```

### Change Scripts

Located in the root scripts directory as `source_change_scripts_*.jsonl`:

Example change record:
```json
{
  "kind": "SourceChange",
  "offset_ns": 86400000000000,
  "source_change_event": {
    "op": "u",
    "reactivatorStart_ns": 1704067200000000000,
    "reactivatorEnd_ns": 1704067201000000,
    "payload": {
      "source": {
        "db": "wikidata",
        "table": "node",
        "ts_ns": 1704067200000000000,
        "lsn": 42
      },
      "before": {"id": "Q84", "labels": ["City"], "properties": {...}},
      "after": {"id": "Q84", "labels": ["City"], "properties": {...}}
    }
  }
}
```

### Script Structure

1. **Header Record** - Contains test metadata and start time
2. **Data Records** - Bootstrap nodes or source changes
3. **Finish Record** - Marks script completion

## Examples

### Complete Workflow Example

```bash
# 1. Download all countries with 2024 revisions
wikidata get-types -t country -s 2024-01-01 -e 2024-12-31 -c 100

# 2. Download specific cities
wikidata get-items -t city -i Q84,Q90,Q1490 -s 2024-01-01

# 3. Generate test scripts
wikidata make-script -i perf-test-2024 -t country,city -s 2024-01-01 -e 2024-12-31
```

### Incremental Download Example

```bash
# Initial download (might fail on some items)
wikidata get-types -t country -b 30 -s 2023-01-01

# Retry failed items with smaller batch size
wikidata get-types -t country -b 5 -s 2023-01-01
```

### Custom Cache Location

```bash
# Using environment variable
export WIKIDATA_CACHE_PATH=/data/wikidata_cache
wikidata get-types -t city

# Or command line
wikidata -c /data/wikidata_cache get-types -t city
```

## Wikidata Schema

### Common Properties

All item types extract these Wikidata properties:

- **P2046** - Area (in square kilometers)
- **P625** - Coordinate location (latitude, longitude)
- **P1082** - Population
- **Labels** - Item name in English

### Type-Specific Properties

#### Continent
- No additional properties

#### Country
- **P30** - Continent (relationship to continent item)

#### City
- **P17** - Country (relationship to country item)

### Future Properties (TODO)

Additional properties that could be extracted:
- **P2250** - Life expectancy
- **P36** - Capital
- **P35** - Head of state
- **P2131** - Nominal GDP
- **P10622** - Per capita income
- **P571** - Founded/inception date
- **P421** - Time zone
- **P6** - Head of government

## Architecture

### Module Structure

- `main.rs` - CLI interface and command handling
- `script.rs` - Test script generation logic
- `wikidata/mod.rs` - Wikidata API integration and data structures
- `wikidata/extractors.rs` - Property extraction from Wikidata JSON

### Key Components

1. **ItemType** - Enum representing supported Wikidata item types
2. **ItemRevisionFileContent** - Structured revision data for file storage
3. **ScriptGenerator** - Converts Wikidata items to Drasi test format
4. **Concurrent Downloads** - Semaphore-controlled parallel processing
5. **Incremental Processing** - Skip existing files for efficient re-runs

### Data Flow

1. Query Wikidata SPARQL endpoint for item lists
2. Download revision metadata for each item
3. Batch download full revision content
4. Extract relevant properties from JSON
5. Generate bootstrap records for initial state
6. Generate change records for revision history

## Troubleshooting

### Response Truncation Errors

The most common issue is Wikidata truncating large responses. Symptoms include:

```
[ERROR wikidata::wikidata] Chunk download failed - Item ID: "Q408". 
Revision IDs: "1060059333|1056723095|...". 
Error: error decoding response body
```

**Solution**: Reduce batch size and retry:

```bash
# Original command that failed
wikidata get-types -t country -b 30 -s 2019-01-01

# Retry with smaller batch size
wikidata get-items -t country -i Q408 -b 5 -s 2019-01-01
```

### Missing Claims Error

Some revisions might not have claims data:

```
[ERROR] No claims found in Item Q123 Revision 456789
```

**Solution**: This is normal for deleted or vandalized revisions. The tool automatically skips these.

### Connection Timeouts

For large downloads, you might experience timeouts.

**Solution**: 
- Use smaller date ranges
- Reduce concurrent downloads (hardcoded semaphore limit)
- Check internet connection stability

## Development

### Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Check code
cargo check
cargo clippy
```

### Adding New Features

1. **New Item Types**
   - Add variant to `ItemType` enum in `wikidata/mod.rs`
   - Add Wikidata class ID mapping
   - Add property extractors in `extractors.rs`

2. **New Properties**
   - Add extractor function in `extractors.rs`
   - Update item parsing logic
   - Add to bootstrap/change record generation

### Performance Considerations

- Downloads use semaphore-controlled concurrency (limit: 2 for lists, 5 for revisions)
- Incremental downloads skip existing files
- Batch processing reduces API calls
- SPARQL queries retrieve all items in single request

### Known Limitations

- Only supports three item types (continent, country, city)
- English labels only
- No support for qualifiers or references
- Limited property extraction
- No data validation beyond basic JSON parsing

### Future Enhancements

- Support for more item types (person, organization, event)
- Multi-language label support
- Property qualifier extraction
- Data validation and quality checks
- Configurable concurrency limits
- Resume capability for interrupted downloads
- Compressed cache storage
- Incremental script generation

## License

Copyright 2025 The Drasi Authors. Licensed under the Apache License, Version 2.0.

## Wikidata Reference URLs

- [Wikidata Homepage](https://www.wikidata.org/)
- [SPARQL Query Service](https://query.wikidata.org/)
- [MediaWiki API](https://www.wikidata.org/w/api.php)
- [Tools for Programmers](https://www.wikidata.org/wiki/Wikidata:Tools/For_programmers)
- [Wikibase CLI](https://github.com/maxlath/wikibase-cli)
- [Database Downloads](https://www.wikidata.org/wiki/Wikidata:Database_download)
- [WikiProject Cities](https://www.wikidata.org/wiki/Wikidata:WikiProject_Cities)