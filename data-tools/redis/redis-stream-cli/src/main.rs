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
use redis_stream_cli::reader::RedisStreamReader;
use redis_stream_cli::types::Params;

#[tokio::main]
async fn main() {
    env_logger::init();

    let params = Params::parse();

    let res = handle_read_command(params).await;

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

async fn handle_read_command(params: Params) -> anyhow::Result<()> {
    log::debug!("Read command:");
    log::debug!("  Redis URL: {}", params.redis_url);
    log::debug!("  Stream name: {}", params.stream_name);
    log::debug!("  Start timestamp: {}", params.start_timestamp);
    log::debug!("  Count: {:?}", params.count);
    log::debug!("  Output file: {:?}", params.output_file);
    log::debug!("  Output format: {:?}", params.output_format);

    // Create the Redis stream reader
    let mut reader = RedisStreamReader::new(&params.redis_url, params.stream_name.clone()).await?;

    // Read records from the stream
    let records = reader
        .read_records(&params.start_timestamp, params.count)
        .await?;

    log::info!("Successfully read {} records", records.len());

    // Create the appropriate output writer
    let writer = create_writer(params.output_file);

    // Write the records
    writer
        .write_records(&records, &params.output_format)
        .await?;

    Ok(())
}
