// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// CLI parameters for the redis-stream-cli utility
#[derive(Parser, Debug)]
#[command(name = "redis-stream-cli")]
#[command(about = "CLI for reading records from Redis streams", long_about = None)]
pub struct Params {
    /// Redis server URL
    #[arg(
        short = 'u',
        long = "url",
        env = "REDIS_URL",
        default_value = "redis://localhost:6379"
    )]
    pub redis_url: String,

    /// Stream name (mandatory)
    #[arg(short = 's', long = "stream")]
    pub stream_name: String,

    /// Timestamp to read from (default: 0 for beginning, use $ for latest)
    #[arg(short = 't', long = "timestamp", default_value = "0")]
    pub start_timestamp: String,

    /// Number of records to read (default: unlimited)
    #[arg(short = 'c', long = "count")]
    pub count: Option<usize>,

    /// Write output to file. If no filename provided, uses stream name with appropriate extension.
    /// Example: -f (auto-generates filename), -f myfile.json (uses specified name)
    #[arg(short = 'f', long = "file", num_args = 0..=1, default_missing_value = "")]
    pub output_file: Option<String>,

    /// Output format (json or text)
    #[arg(short = 'o', long = "format", default_value = "json")]
    pub output_format: OutputFormat,
}

impl Params {
    /// Get the output file path, generating one if -f was used without a value
    pub fn get_output_path(&self) -> Option<PathBuf> {
        self.output_file.as_ref().map(|f| {
            if f.is_empty() {
                // Generate filename from stream name and format
                let extension = match self.output_format {
                    OutputFormat::Json => "json",
                    OutputFormat::Text => "txt",
                };
                PathBuf::from(format!("{}.{}", self.stream_name, extension))
            } else {
                PathBuf::from(f)
            }
        })
    }
}

/// Output format options
#[derive(Clone, Debug, ValueEnum)]
pub enum OutputFormat {
    /// JSON format with pretty printing
    Json,
    /// Plain text format
    Text,
}

/// Represents a single record from a Redis stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    /// The Redis stream ID (e.g., "1234567890-0")
    pub id: String,
    /// Timestamp in milliseconds extracted from the stream ID
    pub timestamp_ms: u64,
    /// The field-value pairs from the stream entry
    /// Values are either strings or parsed JSON objects
    pub fields: HashMap<String, serde_json::Value>,
}

impl StreamRecord {
    /// Create a new StreamRecord from a Redis stream ID and field map
    /// Attempts to parse the "data" field as JSON if present
    pub fn new(id: String, fields: HashMap<String, String>) -> Self {
        // Extract timestamp from the stream ID (format: "timestamp-sequence")
        let timestamp_ms = id
            .split('-')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        // Convert fields to serde_json::Value, parsing "data" field as JSON if possible
        let mut parsed_fields = HashMap::new();
        for (key, value) in fields {
            if key == "data" {
                // Try to parse as JSON
                match serde_json::from_str::<serde_json::Value>(&value) {
                    Ok(json_value) => {
                        parsed_fields.insert(key, json_value);
                    }
                    Err(_) => {
                        // If parsing fails, store as string
                        parsed_fields.insert(key, serde_json::Value::String(value));
                    }
                }
            } else {
                // For non-data fields, store as string
                parsed_fields.insert(key, serde_json::Value::String(value));
            }
        }

        Self {
            id,
            timestamp_ms,
            fields: parsed_fields,
        }
    }

    /// Format the record as JSON
    pub fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    /// Format the record as plain text
    pub fn to_text(&self) -> String {
        let mut lines = vec![
            format!("ID: {}", self.id),
            format!("Timestamp: {}", self.timestamp_ms),
            "Fields:".to_string(),
        ];

        for (key, value) in &self.fields {
            // Format value based on its type
            let value_str = match value {
                serde_json::Value::String(s) => s.clone(),
                _ => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
            };
            lines.push(format!("  {}: {}", key, value_str));
        }

        lines.join("\n")
    }
}
