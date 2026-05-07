# GDELT CLI

A command-line tool for working with GDELT (Global Database of Events, Language, and Tone) data. This tool can download, unzip, load GDELT data into a PostgreSQL database, and generate test scripts for the Drasi E2E Test Framework.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Download GDELT Data](#download-gdelt-data)
  - [Unzip Files](#unzip-files)
  - [Load into Database](#load-into-database)
  - [Generate E2E Test Scripts](#generate-e2e-test-scripts)
- [Commands Reference](#commands-reference)
- [Configuration](#configuration)
- [Data Types](#data-types)
- [Script Output Format](#script-output-format)
- [Examples](#examples)
- [GDELT Data Information](#gdelt-data-information)
- [Architecture](#architecture)
- [Development](#development)

## Features

- **Download GDELT data files** - Automatically downloads events, graph (GKG), and mentions data from GDELT v2
- **Unzip downloaded files** - Extracts compressed CSV files for processing
- **Load data into PostgreSQL** - Batch inserts GDELT events into a PostgreSQL database
- **Generate E2E test scripts** - Creates bootstrap and change scripts compatible with Drasi's test framework
- **Date range support** - Process specific time periods with 15-minute granularity
- **Multiple data types** - Support for events, graph (GKG), and mentions data
- **Configurable caching** - Local file cache for downloaded data

## Installation

### Prerequisites

- Rust toolchain (1.70 or later)
- PostgreSQL (for database loading functionality)
- Internet connection (for downloading GDELT data)

### Building from Source

```bash
# Clone the repository
git clone <repository-url>
cd gdelt-cli

# Build the project
cargo build --release

# Run directly with cargo
cargo run -- --help

# Or install locally
cargo install --path .
```

## Usage

### Download GDELT Data

GDELT publishes new data files every 15 minutes. You can download specific date ranges or the current data.

```bash
# Download today's data
gdelt-cli get

# Download specific date range
gdelt-cli get -s 20240101 -e 20240131

# Download and automatically unzip
gdelt-cli get -u

# Download specific data types only
gdelt-cli get -t event,graph

# Overwrite existing files
gdelt-cli get -o
```

### Unzip Files

Extract previously downloaded zip files to CSV format.

```bash
# Unzip all downloaded files
gdelt-cli unzip

# Unzip specific date range
gdelt-cli unzip -s 20240101 -e 20240131

# Overwrite existing unzipped files
gdelt-cli unzip -o
```

### Load into Database

Load GDELT event data into a PostgreSQL database. Currently supports event data only.

```bash
# Initialize database (create database and tables)
gdelt-cli init-db -d localhost -u postgres -p password

# Initialize with overwrite (drops existing database)
gdelt-cli init-db -d localhost -u postgres -p password -o

# Load data for specific date range
gdelt-cli load -s 20240101 -e 20240131 -d localhost -u postgres -p password

# Load specific data types
gdelt-cli load -t event -s 20240101
```

### Generate E2E Test Scripts

Generate bootstrap and change scripts formatted for the Drasi E2E Test Framework.

```bash
# Generate both bootstrap and change scripts
gdelt-cli generate-scripts -s 20240101 -e 20240131 -o ./output

# Generate only bootstrap scripts
gdelt-cli generate-scripts -s 20240101 -b

# Generate only change scripts
gdelt-cli generate-scripts -s 20240101 -c

# Generate for specific data types
gdelt-cli generate-scripts -t event,mention -o ./scripts

# Specify custom cache directory
gdelt-cli -c /path/to/cache generate-scripts -s 20240101
```

## Commands Reference

### Global Options

- `-c, --cache <PATH>` - GDELT data cache directory (default: `./gdelt_data_cache`)
  - Can also be set via `GDELT_CACHE_PATH` environment variable

### Commands

#### `get` - Download GDELT Data

Downloads GDELT data files from the official repository.

**Options:**
- `-t, --data-type <TYPE>` - Data types to download: event, graph, mention (default: all)
- `-s, --file-start <YYYYMMDDHHMMSS>` - Start datetime (defaults to current time)
- `-e, --file-end <YYYYMMDDHHMMSS>` - End datetime (defaults to start time)
- `-o, --overwrite` - Overwrite existing files
- `-u, --unzip` - Automatically unzip after download

#### `unzip` - Extract Data Files

Extracts downloaded zip files to CSV format.

**Options:**
- `-t, --data-type <TYPE>` - Data types to process (default: all)
- `-s, --file-start <YYYYMMDDHHMMSS>` - Start datetime
- `-e, --file-end <YYYYMMDDHHMMSS>` - End datetime
- `-o, --overwrite` - Overwrite existing files

#### `load` - Load into Database

Loads GDELT data into PostgreSQL database.

**Options:**
- `-t, --data-type <TYPE>` - Data types to load (currently only 'event' is implemented)
- `-s, --file-start <YYYYMMDDHHMMSS>` - Start datetime
- `-e, --file-end <YYYYMMDDHHMMSS>` - End datetime
- `-d, --db_url <URL>` - Database host (default: localhost)
- `-u, --db_user <USER>` - Database user (default: postgres)
- `-p, --db_password <PASSWORD>` - Database password (default: password)

Environment variables: `GDELT_DB_URL`, `GDELT_DB_USER`, `GDELT_DB_PASSWORD`

#### `init-db` - Initialize Database

Creates database and tables for GDELT data.

**Options:**
- `-d, --db_url <URL>` - Database host
- `-u, --db_user <USER>` - Database user
- `-p, --db_password <PASSWORD>` - Database password
- `-o, --overwrite` - Drop and recreate database if it exists

#### `generate-scripts` - Generate Test Scripts

Generates scripts for the Drasi E2E Test Framework.

**Options:**
- `-t, --data-type <TYPE>` - Data types to process (default: all)
- `-s, --file-start <YYYYMMDDHHMMSS>` - Start datetime
- `-e, --file-end <YYYYMMDDHHMMSS>` - End datetime
- `-o, --output <PATH>` - Output directory (default: `./scripts_output`)
- `-b, --bootstrap` - Generate bootstrap scripts (default: true)
- `-c, --changes` - Generate change scripts (default: true)

## Configuration

### Date/Time Format

All datetime parameters use the format `YYYYMMDDHHMMSS`. You can provide partial dates:
- `2024` - January 1, 2024 at 00:00:00
- `202401` - January 1, 2024 at 00:00:00
- `20240115` - January 15, 2024 at 00:00:00
- `202401151230` - January 15, 2024 at 12:30:00

Times are automatically aligned to 15-minute boundaries (GDELT's update frequency).

### Cache Directory Structure

```
gdelt_data_cache/
├── zip/                    # Downloaded zip files
│   ├── 20240101000000.export.CSV.zip
│   ├── 20240101000000.gkg.csv.zip
│   └── 20240101000000.mentions.CSV.zip
├── event/                  # Extracted event files
│   └── 20240101000000.export.CSV
├── graph/                  # Extracted GKG files
│   └── 20240101000000.gkg.CSV
└── mention/                # Extracted mention files
    └── 20240101000000.mentions.CSV
```

## Data Types

### Events (`event`)
Global events with actors, actions, and geographic information. Contains 61 fields including:
- Event identifiers and timestamps
- Actor information (codes, names, types)
- Geographic data (locations, coordinates)
- Event metrics (tone, Goldstein scale, mentions)

### Graph/GKG (`graph`)
Global Knowledge Graph data containing:
- Themes and topics
- Persons and organizations mentioned
- Locations and dates
- Document metadata
- Social media embeds

### Mentions (`mention`)
References to events in media, including:
- Source information
- Confidence scores
- Document tone
- Character offsets
- Raw text excerpts

## Script Output Format

The generated scripts follow the Drasi E2E Test Framework format using JSONL (JSON Lines).

### Bootstrap Scripts

Located in `bootstrap_scripts/` subdirectory:
- `{type}_header.jsonl` - Contains header record with metadata
- `{type}_00000.jsonl` - Node records for initial data
- `{type}_finish.jsonl` - Finish record marking completion

Example bootstrap record:
```json
{"kind":"Node","id":"event_123","labels":["Event"],"properties":{...}}
```

### Change Scripts

Located in `source_change_scripts/` subdirectory:
- `source_change_scripts_00000.jsonl` - Header record
- `source_change_scripts_00001.jsonl` - Change events
- Sequential numbered files for additional changes

Example change record:
```json
{
  "kind": "SourceChange",
  "offset_ns": 1704067200000000000,
  "source_change_event": {
    "op": "c",
    "reactivatorStart_ns": 1704067200000000000,
    "reactivatorEnd_ns": 1704067201000000,
    "payload": {
      "source": {
        "db": "gdelt",
        "table": "event",
        "ts_ns": 1704067200000000000,
        "lsn": 1704067200
      },
      "before": null,
      "after": {...}
    }
  }
}
```

## Examples

### Complete Workflow Example

```bash
# 1. Download and unzip a day's worth of event data
gdelt-cli get -t event -s 20240101 -e 20240101235959 -u

# 2. Initialize database
gdelt-cli init-db

# 3. Load events into database
gdelt-cli load -t event -s 20240101 -e 20240101235959

# 4. Generate test scripts
gdelt-cli generate-scripts -t event -s 20240101 -e 20240101235959 -o ./test_scripts
```

### Processing Recent Data

```bash
# Download last 6 hours of data
gdelt-cli get -s $(date -u -d '6 hours ago' +%Y%m%d%H%M%S)

# Generate scripts for testing
gdelt-cli generate-scripts -s $(date -u -d '6 hours ago' +%Y%m%d%H%M%S)
```

### Custom Cache Location

```bash
# Using environment variable
export GDELT_CACHE_PATH=/data/gdelt_cache
gdelt-cli get -s 20240101

# Or command line
gdelt-cli -c /data/gdelt_cache get -s 20240101
```

## GDELT Data Information

### Update Frequency
- New files published every 15 minutes
- File naming: `YYYYMMDDHHMMSS.{type}.CSV.zip`

### Data Sources

#### Main Website
- https://www.gdeltproject.org/

#### Documentation
- https://www.gdeltproject.org/data.html#rawdatafiles

#### Data URLs

##### Master File Lists
- http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
- http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt

##### Latest Updates
- http://data.gdeltproject.org/gdeltv2/lastupdate.txt
- http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt

##### Direct File Access (15-minute intervals)
- Events: `http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.export.CSV.zip`
- Mentions: `http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.mentions.CSV.zip`
- GKG: `http://data.gdeltproject.org/gdeltv2/YYYYMMDDHHMMSS.gkg.csv.zip`

## Architecture

### Module Structure

- `main.rs` - CLI interface and command handling
- `gdelt.rs` - GDELT data structures (Event, Graph, Mention)
- `postgres.rs` - Database operations and batch loading
- `script_generator.rs` - E2E test script generation

### Key Components

1. **FileInfo** - Tracks file processing state through download, unzip, and load stages
2. **ScriptGenerator** - Converts GDELT data to Drasi test format
3. **Batch Processing** - Efficient database loading with configurable batch sizes
4. **Async Operations** - Concurrent file downloads and processing

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

1. **New Data Type Support**
   - Add struct in `gdelt.rs`
   - Implement parsing logic in `script_generator.rs`
   - Add database table and loading logic in `postgres.rs`

2. **New Output Formats**
   - Extend `ScriptGenerator` with new format methods
   - Add command line options in `main.rs`

### Known Limitations

- Database loading currently only supports event data
- Script generation uses placeholder parsing (needs full GDELT CSV parsing implementation)
- No support for GDELT translation files
- No incremental/resume support for interrupted downloads

### Future Enhancements

- Implement full CSV parsing for all GDELT data types
- Add support for graph and mention database loading
- Implement incremental download/processing
- Add data validation and error recovery
- Support for GDELT historical archives
- Parallel processing for large date ranges
- Data filtering and transformation options

## License

Copyright 2025 The Drasi Authors. Licensed under the Apache License, Version 2.0.