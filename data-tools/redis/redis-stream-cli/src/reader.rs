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

use crate::types::StreamRecord;
use redis::{streams::StreamRangeReply, AsyncCommands, Client};
use std::collections::HashMap;

/// Redis stream reader for reading records from a stream
pub struct RedisStreamReader {
    connection: redis::aio::MultiplexedConnection,
    stream_name: String,
}

impl RedisStreamReader {
    /// Create a new RedisStreamReader
    ///
    /// # Arguments
    /// * `redis_url` - The Redis server URL (e.g., "redis://localhost:6379")
    /// * `stream_name` - The name of the stream to read from
    pub async fn new(redis_url: &str, stream_name: String) -> anyhow::Result<Self> {
        log::info!("Connecting to Redis at: {}", redis_url);
        let client = Client::open(redis_url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        log::info!("Successfully connected to Redis");

        Ok(Self {
            connection,
            stream_name,
        })
    }

    /// Read records from the stream
    ///
    /// # Arguments
    /// * `start_id` - The stream ID to start reading from (e.g., "0", "0-0", "$", or a specific ID)
    /// * `count` - Optional maximum number of records to read
    ///
    /// # Returns
    /// A vector of StreamRecord objects
    pub async fn read_records(
        &mut self,
        start_id: &str,
        count: Option<usize>,
    ) -> anyhow::Result<Vec<StreamRecord>> {
        log::info!(
            "Reading from stream '{}' starting at ID '{}'",
            self.stream_name,
            start_id
        );

        let mut all_records = Vec::new();
        // For XRANGE, use "-" to start from the beginning instead of "0"
        let mut current_id = if start_id == "0" || start_id == "0-0" {
            "-".to_string()
        } else {
            start_id.to_string()
        };
        let remaining_count = count;

        // If we have a count limit, we'll read in batches
        let batch_size = count.unwrap_or(100);

        loop {
            log::debug!("Executing XRANGE from ID: {}", current_id);

            // Use XRANGE for reading historical data from the beginning
            // xrange returns StreamRangeReply which contains the IDs
            let result: StreamRangeReply = self
                .connection
                .xrange_count(&self.stream_name, &current_id, "+", batch_size)
                .await?;

            let ids = result.ids;

            if ids.is_empty() {
                log::debug!("No more records found in stream");
                break;
            }

            let mut batch_count = 0;

            for stream_id in ids {
                let id = stream_id.id;
                let mut fields = HashMap::new();

                // Convert the stream entry fields to a HashMap
                for (key, value) in stream_id.map {
                    if let redis::Value::BulkString(bytes) = value {
                        let value_str = String::from_utf8_lossy(&bytes).to_string();
                        fields.insert(key, value_str);
                    } else {
                        // Handle other value types by converting to string
                        fields.insert(key, format!("{:?}", value));
                    }
                }

                all_records.push(StreamRecord::new(id.clone(), fields));

                // Update current_id for next iteration - need to go past this ID
                // For XRANGE, we use "(" prefix to mean exclusive
                current_id = format!("({}", id);
                batch_count += 1;

                // Check if we've reached the count limit
                if let Some(limit) = remaining_count {
                    if all_records.len() >= limit {
                        log::info!("Reached count limit of {} records", limit);
                        return Ok(all_records);
                    }
                }
            }

            // If we got fewer records than the batch size, we've reached the end
            if batch_count < batch_size {
                log::debug!(
                    "Reached end of stream (got {} records in batch)",
                    batch_count
                );
                break;
            }

            // If no count limit, read another batch
            if let Some(limit) = count {
                // With a count limit, check if we should continue
                if all_records.len() >= limit {
                    break;
                }
            }
        }

        log::info!("Read {} total records from stream", all_records.len());
        Ok(all_records)
    }
}
