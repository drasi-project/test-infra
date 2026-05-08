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

use clap::Parser;
use redis_stream_cli::output::create_writer;
use redis_stream_cli::reader::{list_streams, RedisStreamReader};
use redis_stream_cli::types::{Cli, Commands, ListArgs, ReadArgs};

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();

    let res = match cli.command {
        Commands::Read(args) => handle_read_command(args).await,
        Commands::List(args) => handle_list_command(args).await,
    };

    match res {
        Ok(_) => {
            log::info!("Command completed successfully");
        }
        Err(e) => {
            eprintln!("redis-stream-cli command failed: {:?}", e);
            std::process::exit(1);
        }
    }
}

async fn handle_read_command(args: ReadArgs) -> anyhow::Result<()> {
    log::debug!("Read command:");
    log::debug!("  Redis URL: {}", args.redis_url);
    log::debug!("  Stream name: {}", args.stream_name);
    log::debug!("  Start timestamp: {}", args.start_timestamp);
    log::debug!("  Count: {:?}", args.count);
    log::debug!("  Output file: {:?}", args.output_file);
    log::debug!("  Output format: {:?}", args.output_format);

    // Create the Redis stream reader
    let mut reader = RedisStreamReader::new(&args.redis_url, args.stream_name.clone()).await?;

    // Read records from the stream
    let records = reader
        .read_records(&args.start_timestamp, args.count)
        .await?;

    log::info!("Successfully read {} records", records.len());

    // Get the output file path (auto-generated or specified)
    let output_path = args.get_output_path();

    // Log the output destination
    if let Some(ref path) = output_path {
        log::info!("Writing output to file: {:?}", path);
    }

    // Create the appropriate output writer
    let writer = create_writer(output_path);

    // Write the records
    writer.write_records(&records, &args.output_format).await?;

    Ok(())
}

async fn handle_list_command(args: ListArgs) -> anyhow::Result<()> {
    log::debug!("List command:");
    log::debug!("  Redis URL: {}", args.redis_url);
    log::debug!("  Output file: {:?}", args.output_file);

    // List all streams
    let streams = list_streams(&args.redis_url).await?;

    log::info!("Found {} streams", streams.len());

    // Get the output file path (auto-generated or specified)
    let output_path = args.get_output_path();

    // Log the output destination
    if let Some(ref path) = output_path {
        log::info!("Writing output to file: {:?}", path);
    }

    // Format as JSON
    let json_output = serde_json::to_string_pretty(&streams)?;

    // Output to console or file
    match output_path {
        Some(path) => {
            // Write to file
            use tokio::fs::File;
            use tokio::io::AsyncWriteExt;

            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }

            let mut file = File::create(&path).await?;
            file.write_all(json_output.as_bytes()).await?;
            file.flush().await?;

            log::info!("Successfully wrote {} streams to file", streams.len());
        }
        None => {
            // Write to console
            println!("{}", json_output);
        }
    }

    Ok(())
}
