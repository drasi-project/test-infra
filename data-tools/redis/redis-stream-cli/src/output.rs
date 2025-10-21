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

use crate::types::{OutputFormat, StreamRecord};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Output writer enum that handles both console and file output
pub enum Writer {
    Console,
    File(PathBuf),
}

impl Writer {
    /// Write records to the output destination
    pub async fn write_records(
        &self,
        records: &[StreamRecord],
        format: &OutputFormat,
    ) -> anyhow::Result<()> {
        match self {
            Writer::Console => write_to_console(records, format).await,
            Writer::File(path) => write_to_file(records, format, path).await,
        }
    }
}

/// Write records to console
async fn write_to_console(records: &[StreamRecord], format: &OutputFormat) -> anyhow::Result<()> {
    match format {
        OutputFormat::Json => {
            // Output as a JSON array
            let json = serde_json::to_string_pretty(&records)?;
            println!("{}", json);
        }
        OutputFormat::Text => {
            // Output each record as text
            for (i, record) in records.iter().enumerate() {
                if i > 0 {
                    println!("\n---");
                }
                println!("{}", record.to_text());
            }
        }
    }
    Ok(())
}

/// Write records to a file
async fn write_to_file(
    records: &[StreamRecord],
    format: &OutputFormat,
    path: &PathBuf,
) -> anyhow::Result<()> {
    let content = match format {
        OutputFormat::Json => serde_json::to_string_pretty(&records)?,
        OutputFormat::Text => {
            let mut lines = Vec::new();
            for (i, record) in records.iter().enumerate() {
                if i > 0 {
                    lines.push("\n---".to_string());
                }
                lines.push(record.to_text());
            }
            lines.join("\n")
        }
    };

    log::info!("Writing output to file: {:?}", path);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }

    let mut file = File::create(path).await?;
    file.write_all(content.as_bytes()).await?;
    file.flush().await?;

    log::info!("Successfully wrote {} records to file", records.len());

    Ok(())
}

/// Create an output writer based on the output file option
pub fn create_writer(output_file: Option<PathBuf>) -> Writer {
    match output_file {
        Some(path) => Writer::File(path),
        None => Writer::Console,
    }
}
