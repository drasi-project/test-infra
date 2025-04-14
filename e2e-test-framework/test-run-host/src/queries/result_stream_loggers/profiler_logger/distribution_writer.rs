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

use super::ChangeRecordProfile;

// Component indices for time tracking
const REACTIVATOR_IDX: usize = 0;
const SRC_CHANGE_Q_IDX: usize = 1;
const SRC_CHANGE_RTR_IDX: usize = 2;
const SRC_DISP_Q_IDX: usize = 3;
const SRC_CHANGE_DISP_IDX: usize = 4;
const QUERY_PUB_API_IDX: usize = 5;
const QUERY_CHANGE_Q_IDX: usize = 6;
const QUERY_HOST_IDX: usize = 7;
const QUERY_SOLVER_IDX: usize = 8;
const RESULT_Q_IDX: usize = 9;
const COMPONENT_COUNT: usize = 10;

// Component names for CSV output (without "time_in_" prefix)
const COMPONENT_NAMES: [&str; COMPONENT_COUNT] = [
    "reactivator",
    "src_change_q",
    "src_change_rtr",
    "src_disp_q",
    "src_change_disp",
    "query_pub_api",
    "query_change_q",
    "query_host",
    "query_solver",
    "result_q",
];

// Define power-of-10 buckets for nanosecond time distributions
// From 1ns to 100s (10^0 to 10^11 ns)
const BUCKET_COUNT: usize = 12; // 10^0 through 10^11

// Time distribution tracker struct
pub struct TimeDistributionTracker {
    file_path: PathBuf,
    // 2D array of counts: [component_idx][bucket_idx]
    distributions: [[u32; BUCKET_COUNT]; COMPONENT_COUNT],
}

impl TimeDistributionTracker {
    pub fn new(folder_path: PathBuf, file_name: String) -> Self {
        Self {
            file_path: folder_path.join(format!("{}_distributions.csv", file_name)),
            distributions: [[0; BUCKET_COUNT]; COMPONENT_COUNT],
        }
    }
    
    // Process a change record profile and update the distributions
    pub fn process_record(&mut self, profile: &ChangeRecordProfile) {
        // Increment the counts for each component's bucket directly
        self.distributions[REACTIVATOR_IDX][self.find_bucket_for_time(profile.time_in_reactivator)] += 1;
        self.distributions[SRC_CHANGE_Q_IDX][self.find_bucket_for_time(profile.time_in_src_change_q)] += 1;
        self.distributions[SRC_CHANGE_RTR_IDX][self.find_bucket_for_time(profile.time_in_src_change_rtr)] += 1;
        self.distributions[SRC_DISP_Q_IDX][self.find_bucket_for_time(profile.time_in_src_disp_q)] += 1;
        self.distributions[SRC_CHANGE_DISP_IDX][self.find_bucket_for_time(profile.time_in_src_change_disp)] += 1;
        self.distributions[QUERY_PUB_API_IDX][self.find_bucket_for_time(profile.time_in_query_pub_api)] += 1;
        self.distributions[QUERY_CHANGE_Q_IDX][self.find_bucket_for_time(profile.time_in_query_change_q)] += 1;
        self.distributions[QUERY_HOST_IDX][self.find_bucket_for_time(profile.time_in_query_host)] += 1;
        self.distributions[QUERY_SOLVER_IDX][self.find_bucket_for_time(profile.time_in_query_solver)] += 1;
        self.distributions[RESULT_Q_IDX][self.find_bucket_for_time(profile.time_in_result_q)] += 1;
    }
    

    // Find the correct bucket index for a given time using logarithm
    fn find_bucket_for_time(&self, time_ns: u64) -> usize {
        // Handle the special case of time_ns = 0
        // log10(0) is mathematically undefined (negative infinity)
        if time_ns == 0 {
            return 0;
        }
        
        // Calculate the bucket index using log10
        // Since our buckets are powers of 10, we can use log10 directly
        let log10_time = (time_ns as f64).log10().floor() as usize;
        
        // Ensure we don't exceed maximum bucket (10^11)
        // Anything above bucket 11 goes into bucket 11
        std::cmp::min(log10_time, BUCKET_COUNT - 1)
    }
    
    // Write to CSV with buckets as rows and components as columns
    pub async fn generate_csv(&self) -> Result<()> {
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
        
        // Write header with bucket as first column followed by component names
        let mut header = String::from("bucket");
        for component_name in COMPONENT_NAMES.iter() {
            header.push_str(&format!(",{}", component_name));
        }
        header.push_str("\n");
        file.write_all(header.as_bytes()).await?;
        
        // Write data for each bucket as a row
        for bucket_idx in 0..BUCKET_COUNT {
            // Get human-readable bucket name
            let bucket_name = match bucket_idx {
                0 => "0-1ns",
                1 => "1-10ns",
                2 => "10-100ns",
                3 => "100ns-1us",
                4 => "1-10us",
                5 => "10-100us",
                6 => "100us-1ms",
                7 => "1-10ms",
                8 => "10-100ms",
                9 => "100ms-1s",
                10 => "1-10s",
                11 => "10s+",
                _ => "unknown",
            };
            
            let mut row = String::from(bucket_name);
            
            // Add counts for each component
            for component_idx in 0..COMPONENT_COUNT {
                row.push_str(&format!(",{}", self.distributions[component_idx][bucket_idx]));
            }
            
            row.push_str("\n");
            file.write_all(row.as_bytes()).await?;
        }
        
        Ok(())
    }
}