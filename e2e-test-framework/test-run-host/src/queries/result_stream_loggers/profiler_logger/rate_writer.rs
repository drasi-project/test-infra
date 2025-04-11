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

use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::queries::result_stream_handlers::ResultStreamRecord;
use crate::queries::result_stream_record::ChangeEvent;

// Component indices for the 2D array
const REACTIVATOR_IDX: usize = 0;
const CHANGE_ROUTER_IDX: usize = 1;
const CHANGE_DISPATCHER_IDX: usize = 2;
const QUERY_PUB_API_IDX: usize = 3;
const QUERY_HOST_IDX: usize = 4;
const QUERY_SOLVER_IDX: usize = 5;
const COMPONENT_COUNT: usize = 6;

// Component names for CSV output
const COMPONENT_NAMES: [&str; COMPONENT_COUNT] = [
    "reactivator",
    "change_router",
    "change_dispatcher",
    "query_pub_api",
    "query_host",
    "query_solver",
];

// Main rate tracker struct
pub struct RateTracker {
    file_path: PathBuf,
    // Has any component received an event yet?
    has_started: bool,
    // Time when the first event was observed (from reactivator_start)
    start_time_ns: u64,
    // 2D array of counts: [time_window][component_idx]
    counts: Vec<[u32; COMPONENT_COUNT]>,
}

impl RateTracker {
    pub fn new(folder_path: PathBuf, file_name: String, ) -> Self {
        Self {
            file_path: folder_path.join(format!("{}_rates.csv", file_name)),
            has_started: false,
            start_time_ns: 0,
            counts: Vec::with_capacity(100), // Initial capacity for 100 time windows
        }
    }
    
    // Process a record and update all relevant components
    pub fn process_record(&mut self, _record: &ResultStreamRecord, change: &ChangeEvent) {
        // Fixed window size of 1 second in nanoseconds
        const ONE_SECOND_NS: u64 = 1_000_000_000;

        let metadata = &change.base.metadata.as_ref().unwrap().tracking;
        
        // Check if this is the first event with a reactivator_start component
        if !self.has_started {
            if metadata.source.reactivator_start_ns > 0 {
                // Initialize with this start time
                self.start_time_ns = metadata.source.reactivator_start_ns;
                // Add the first time window
                self.counts.push([0; COMPONENT_COUNT]);
                self.has_started = true;
            }
        }
        
        // Only process if we've been initialized
        if !self.has_started {
            return;
        }

        // Process query tracking
        // First, find the maximum window index needed from the latest timestamp
        // query_start_ns should be the latest timestamp in the sequence
        if metadata.query.query_start_ns > 0 {
            let elapsed_ns = metadata.query.query_start_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            
            // Resize counts vector if needed (only once per record)
            self.ensure_window_capacity(window_index);
            
            // Now process query_solver
            self.counts[window_index][QUERY_SOLVER_IDX] += 1;
        }
        
        // Process query_pub_api
        if metadata.query.enqueue_ns > 0 {
            let elapsed_ns = metadata.query.enqueue_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            // No need to check capacity again
            self.counts[window_index][QUERY_PUB_API_IDX] += 1;
        }
        
        // Process query_host
        if metadata.query.dequeue_ns > 0 {
            let elapsed_ns = metadata.query.dequeue_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            // No need to check capacity again
            self.counts[window_index][QUERY_HOST_IDX] += 1;
        }

        // Process source tracking if available
        // No need to check capacity again - we've already resized for the latest timestamp
        
        // Process reactivator
        if metadata.source.reactivator_start_ns > 0 {
            let elapsed_ns = metadata.source.reactivator_start_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            self.counts[window_index][REACTIVATOR_IDX] += 1;
        }
        
        // Process change_router
        if metadata.source.change_router_start_ns > 0 {
            let elapsed_ns = metadata.source.change_router_start_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            self.counts[window_index][CHANGE_ROUTER_IDX] += 1;
        }
        
        // Process change_dispatcher
        if metadata.source.change_dispatcher_start_ns > 0 {
            let elapsed_ns = metadata.source.change_dispatcher_start_ns.saturating_sub(self.start_time_ns);
            let window_index = (elapsed_ns / ONE_SECOND_NS) as usize;
            self.counts[window_index][CHANGE_DISPATCHER_IDX] += 1;
        }
    }
    
    // Ensure we have enough windows to store data at the given index
    fn ensure_window_capacity(&mut self, window_index: usize) {
        if window_index >= self.counts.len() {
            // Grow by at least 100 windows at a time for efficiency
            let new_size = (window_index + 100).max(self.counts.len() + 100);
            self.counts.resize(new_size, [0; COMPONENT_COUNT]);
        }
    }
    
    // Write to CSV with each component as a column
    pub async fn write_to_csv(&self) -> Result<()> {
        // Don't write anything if we haven't started yet
        if !self.has_started {
            return Ok(());
        }
        
        let file_exists = Path::new(&self.file_path).exists();
        
        let mut file = if file_exists {
            OpenOptions::new()
                .write(true)
                .truncate(true)  // Overwrite the file
                .open(&self.file_path)
                .await?
        } else {
            File::create(&self.file_path).await?
        };
        
        // Fixed window size of 1 second in nanoseconds
        const ONE_SECOND_NS: u64 = 1_000_000_000;
        
        // Write header
        let header = format!(
            "index,window_start_ns,window_end_ns,{},{},{},{},{},{}\n",
            COMPONENT_NAMES[REACTIVATOR_IDX],
            COMPONENT_NAMES[CHANGE_ROUTER_IDX],
            COMPONENT_NAMES[CHANGE_DISPATCHER_IDX],
            COMPONENT_NAMES[QUERY_PUB_API_IDX],
            COMPONENT_NAMES[QUERY_HOST_IDX],
            COMPONENT_NAMES[QUERY_SOLVER_IDX]
        );
        file.write_all(header.as_bytes()).await?;
        
        // Write each window's data as a row
        for (window_index, window) in self.counts.iter().enumerate() {
            let window_start_ns = self.start_time_ns + (window_index as u64 * ONE_SECOND_NS);
            let window_end_ns = window_start_ns + ONE_SECOND_NS;
            
            let row = format!(
                "{},{},{},{},{},{},{},{},{}\n",
                window_index,
                window_start_ns,
                window_end_ns,
                window[REACTIVATOR_IDX],
                window[CHANGE_ROUTER_IDX],
                window[CHANGE_DISPATCHER_IDX],
                window[QUERY_PUB_API_IDX],
                window[QUERY_HOST_IDX],
                window[QUERY_SOLVER_IDX]
            );
            file.write_all(row.as_bytes()).await?;
        }
        
        Ok(())
    }
}