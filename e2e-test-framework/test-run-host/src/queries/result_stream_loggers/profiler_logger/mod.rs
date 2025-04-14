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

use std::path::PathBuf;

use async_trait::async_trait;
use distribution_writer::TimeDistributionTracker;
use image_writer::ProfileImageWriter;
use log_writer::ProfileLogWriter;
use rate_writer::RateTracker;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, write};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError, ResultStreamLoggerResult};

mod distribution_writer;
mod image_writer;
mod log_writer;
mod rate_writer;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
    pub bootstrap_log_name: Option<String>,
    pub change_image_name: Option<String>,
    pub change_log_name: Option<String>, 
    pub distribution_log_name: Option<String>,
    pub rates_log_name: Option<String>,   
    pub image_width: Option<u32>,
    pub max_lines_per_file: Option<u64>,
    pub write_bootstrap_log: Option<bool>,
    pub write_change_image: Option<bool>,
    pub write_change_log: Option<bool>,
    pub write_change_rates: Option<bool>,
    pub write_distributions: Option<bool>,
}

#[derive(Debug)]
pub struct ProfilerResultStreamLoggerSettings {
    pub bootstrap_log_name: String,
    pub change_image_name: String,
    pub change_log_name: String,    
    pub distribution_log_name: String,
    pub rates_log_name: String,
    pub folder_path: PathBuf,
    pub image_width: u32,
    pub test_run_query_id: TestRunQueryId,
    pub max_lines_per_file: u64,
    pub write_bootstrap_log: bool,
    pub write_change_image: bool,
    pub write_change_log: bool,
    pub write_change_rates: bool,
    pub write_distributions: bool,
}

impl ProfilerResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &ProfilerResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            bootstrap_log_name: config.bootstrap_log_name.clone().unwrap_or("bootstrap".to_string()),
            change_image_name: config.change_image_name.clone().unwrap_or("change".to_string()),
            change_log_name: config.change_log_name.clone().unwrap_or("change".to_string()),
            distribution_log_name: config.distribution_log_name.clone().unwrap_or("change".to_string()),
            rates_log_name: config.rates_log_name.clone().unwrap_or("change".to_string()),
            image_width: config.image_width.unwrap_or(1200),
            folder_path,
            test_run_query_id,
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            write_bootstrap_log: config.write_bootstrap_log.unwrap_or(false),
            write_change_image: config.write_change_image.unwrap_or(false),
            write_change_log: config.write_change_log.unwrap_or(false),
            write_change_rates: config.write_change_rates.unwrap_or(false),
            write_distributions: config.write_distributions.unwrap_or(false),
        });
    }
}

/// Stats structure for recording timing metrics, including mean and standard deviation
#[derive(Debug, Serialize, Default, Clone, Copy)]
struct TimingStats {
    pub mean: f64,      // Mean value (replaces avg)
    pub max: u64,
    pub min: u64,
    pub std_dev: f64,   // Standard deviation
    
    // Private fields for online calculation
    #[serde(skip)]
    count: usize,       // Count of values seen
    #[serde(skip)]
    m2: f64,            // Sum of squared differences from the mean
}

impl TimingStats {
    /// Create a new TimingStats with default values
    pub fn new() -> Self {
        Self {
            mean: 0.0,
            max: 0,
            min: std::u64::MAX,
            std_dev: 0.0,
            count: 0,
            m2: 0.0,
        }
    }

    /// Update stats with a new value using Welford's online algorithm
    pub fn update(&mut self, value: u64) {
        let value_f64 = value as f64;
        self.count += 1;
        
        // Update min and max
        self.max = std::cmp::max(self.max, value);
        self.min = std::cmp::min(self.min, value);
        
        // Update mean and variance using Welford's algorithm
        let delta = value_f64 - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value_f64 - self.mean;
        self.m2 += delta * delta2;
    }

    /// Finalize statistics calculation
    pub fn finalize(&mut self) {
        // Count parameter is ignored as we track count internally now
        if self.count > 1 {
            // Calculate standard deviation from M2
            self.std_dev = (self.m2 / (self.count - 1) as f64).sqrt();
        } else if self.count == 1 {
            // Only one sample, no deviation
            self.std_dev = 0.0;
        } else {
            // No samples, reset everything
            self.mean = 0.0;
            self.max = 0;
            self.min = 0;
            self.std_dev = 0.0;
        }
    }
}

#[derive(Debug, Serialize)]
struct BootstrapRecordProfile {
    pub seq: i64,
    pub time_total: u64,
}

impl BootstrapRecordProfile {
    pub fn new(record: &ResultStreamRecord, change: &ChangeEvent) -> Self {
        Self {
            seq: change.base.sequence,
            time_total: record.dequeue_time_ns.saturating_sub((change.base.source_time_ms as u64) * 1_000_000)
        }
    }
}

#[derive(Debug, Serialize)]
struct ChangeRecordProfile {
    pub seq: i64,

    // Source processing times
    pub time_in_reactivator: u64,
    pub time_in_src_change_q: u64,
    pub time_in_src_change_rtr: u64,
    pub time_in_src_disp_q: u64,
    pub time_in_src_change_disp: u64,

    // Query processing times
    pub time_in_query_pub_api: u64,
    pub time_in_query_change_q: u64,
    pub time_in_query_host: u64,
    pub time_in_query_solver: u64,
    pub time_in_result_q: u64,

    // Total processing time
    pub time_total: u64,
}

impl ChangeRecordProfile {
    pub fn new(record: &ResultStreamRecord, change: &ChangeEvent) -> Self {

        let metadata = &change.base.metadata.as_ref().unwrap().tracking;

        let record_dequeue_time_ns = record.dequeue_time_ns;
        let time_in_query_solver = metadata.query.query_end_ns.saturating_sub(metadata.query.query_start_ns); 
        let time_in_query_host = (metadata.query.query_end_ns.saturating_sub(metadata.query.dequeue_ns)).saturating_sub(time_in_query_solver);

        Self {
            seq: change.base.sequence,
            time_in_reactivator: metadata.source.reactivator_end_ns.saturating_sub(metadata.source.reactivator_start_ns),
            time_in_src_change_q: metadata.source.change_router_start_ns.saturating_sub(metadata.source.reactivator_end_ns),
            time_in_src_change_rtr: metadata.source.change_router_end_ns.saturating_sub(metadata.source.change_router_start_ns),
            time_in_src_disp_q: metadata.source.change_dispatcher_start_ns.saturating_sub(metadata.source.change_router_end_ns),
            time_in_src_change_disp: metadata.source.change_dispatcher_end_ns.saturating_sub(metadata.source.change_dispatcher_start_ns),
            
            time_in_query_pub_api:metadata.query.enqueue_ns.saturating_sub(metadata.source.change_dispatcher_end_ns),    
            time_in_query_change_q: metadata.query.dequeue_ns.saturating_sub(metadata.query.enqueue_ns),

            time_in_query_host,
            time_in_query_solver,
            time_in_result_q: record_dequeue_time_ns.saturating_sub(metadata.query.query_end_ns),
            time_total: record_dequeue_time_ns.saturating_sub(metadata.source.reactivator_start_ns),
        }
    }
}

#[derive(Debug)]
struct ProfilerSummary{
    // Bootstrap record metrics
    pub bootstrap_rec_count: usize,
    pub bootstrap_rec_time_total: TimingStats,
    
    // Change record counts
    pub change_rec_count: usize,
    
    // Source processing timing metrics
    pub change_rec_time_in_reactivator: TimingStats,
    pub change_rec_time_in_src_change_q: TimingStats,
    pub change_rec_time_in_src_change_rtr: TimingStats,
    pub change_rec_time_in_src_disp_q: TimingStats,
    pub change_rec_time_in_src_change_disp: TimingStats,
    
    // Query processing timing metrics
    pub change_rec_time_in_query_pub_api: TimingStats,
    pub change_rec_time_in_query_change_q: TimingStats,
    pub change_rec_time_in_query_host: TimingStats,
    pub change_rec_time_in_query_solver: TimingStats,
    pub change_rec_time_in_result_q: TimingStats,
    
    // Total processing timing metrics
    pub change_rec_time_total: TimingStats,
    
    // Control record count
    pub control_rec_count: usize
}

impl Default for ProfilerSummary {
    fn default() -> Self {
        Self {
            bootstrap_rec_count: 0,
            bootstrap_rec_time_total: TimingStats::new(),
            change_rec_count: 0,
            change_rec_time_in_reactivator: TimingStats::new(),
            change_rec_time_in_src_change_q: TimingStats::new(),
            change_rec_time_in_src_change_rtr: TimingStats::new(),
            change_rec_time_in_src_disp_q: TimingStats::new(),
            change_rec_time_in_src_change_disp: TimingStats::new(),
            change_rec_time_in_query_pub_api: TimingStats::new(),
            change_rec_time_in_query_change_q: TimingStats::new(),
            change_rec_time_in_query_host: TimingStats::new(),
            change_rec_time_in_query_solver: TimingStats::new(),
            change_rec_time_in_result_q: TimingStats::new(),
            change_rec_time_total: TimingStats::new(),
            control_rec_count: 0
        }
    }
}

// Implementing Serialize for ProfilerSummary to control the order of serialization
impl Serialize for ProfilerSummary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        
        let mut state = serializer.serialize_struct("ProfilerSummary", 15)?;
        
        // Control record count
        state.serialize_field("control_rec_count", &self.control_rec_count)?;
        
        // Serialize fields in the desired order
        state.serialize_field("bootstrap_rec_count", &self.bootstrap_rec_count)?;
        state.serialize_field("bootstrap_rec_time_total", &self.bootstrap_rec_time_total)?;

        state.serialize_field("change_rec_count", &self.change_rec_count)?;
        
        // Source processing timing metrics
        state.serialize_field("change_rec_time_in_reactivator", &self.change_rec_time_in_reactivator)?;
        state.serialize_field("change_rec_time_in_src_change_q", &self.change_rec_time_in_src_change_q)?;
        state.serialize_field("change_rec_time_in_src_change_rtr", &self.change_rec_time_in_src_change_rtr)?;
        state.serialize_field("change_rec_time_in_src_disp_q", &self.change_rec_time_in_src_disp_q)?;
        state.serialize_field("change_rec_time_in_src_change_disp", &self.change_rec_time_in_src_change_disp)?;
        
        // Query processing timing metrics
        state.serialize_field("change_rec_time_in_query_pub_api", &self.change_rec_time_in_query_pub_api)?;
        state.serialize_field("change_rec_time_in_query_change_q", &self.change_rec_time_in_query_change_q)?;
        state.serialize_field("change_rec_time_in_query_host", &self.change_rec_time_in_query_host)?;
        state.serialize_field("change_rec_time_in_query_solver", &self.change_rec_time_in_query_solver)?;
        state.serialize_field("change_rec_time_in_result_q", &self.change_rec_time_in_result_q)?;
        
        // Total processing timing metrics
        state.serialize_field("change_rec_time_total", &self.change_rec_time_total)?;
        
        state.end()
    }
}

pub struct ProfilerResultStreamLogger {    
    bootstrap_log_writer: Option<ProfileLogWriter>,
    distribution_writer: Option<TimeDistributionTracker>,
    change_image_writer: Option<ProfileImageWriter>,
    change_log_writer: Option<ProfileLogWriter>,
    rates_log_writer: Option<RateTracker>,
    settings: ProfilerResultStreamLoggerSettings,
    summary: ProfilerSummary,    
}

impl ProfilerResultStreamLogger {
    pub async fn new(test_run_query_id: TestRunQueryId, def: &ProfilerResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating ProfilerResultStreamLogger for {}, from {:?}, ", test_run_query_id, def);

        let folder_path = output_storage.result_change_path.join("profiler");
        let settings = ProfilerResultStreamLoggerSettings::new(test_run_query_id, &def, folder_path)?;
        log::trace!("Creating ProfilerResultStreamLogger with settings {:?}, ", settings);

        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {},
                Err(e) => return Err(ResultStreamLoggerError::Io(e).into()),
            };
        }        

        let bootstrap_log_writer = if settings.write_bootstrap_log {
            Some(ProfileLogWriter::new(settings.folder_path.clone(), settings.bootstrap_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            None
        };

        let distribution_writer = if settings.write_distributions {
            Some(TimeDistributionTracker::new(settings.folder_path.clone(), settings.distribution_log_name.clone()))
        } else {
            None
        };

        let change_log_writer = if settings.write_change_log {
            Some(ProfileLogWriter::new(settings.folder_path.clone(), settings.change_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            None
        };

        let change_image_writer = if settings.write_change_image {
            Some(ProfileImageWriter::new(settings.folder_path.clone(), settings.change_image_name.clone(), settings.image_width).await?)
        } else {
            None
        };        

        let rates_log_writer = if settings.write_change_rates {
            Some(RateTracker::new(settings.folder_path.clone(), settings.rates_log_name.clone()))
        } else {
            None
        };

        Ok(Box::new( Self { 
            bootstrap_log_writer,
            distribution_writer,
            change_image_writer,
            change_log_writer,
            rates_log_writer,
            settings,
            summary: ProfilerSummary::default(),
        }))
    }

    // Process a bootstrap record
    async fn process_bootstrap_record(
        &mut self, 
        record: &ResultStreamRecord, 
        change: &ChangeEvent
    ) -> anyhow::Result<()> {
        let profile = BootstrapRecordProfile::new(record, change);

        // Log bootstrap profile if writer is enabled
        if let Some(writer) = &mut self.bootstrap_log_writer {
            writer.write_bootstrap_profile(&profile).await?;
        }

        // Update summary statistics
        self.summary.bootstrap_rec_count += 1;
        self.summary.bootstrap_rec_time_total.update(profile.time_total);
        
        Ok(())
    }
    
    // Process a change record with metadata
    async fn process_change_record(
        &mut self, 
        record: &ResultStreamRecord, 
        change: &ChangeEvent
    ) -> anyhow::Result<()> {
        let profile = ChangeRecordProfile::new(record, change);

        // Process through various writers if enabled
        if let Some(writer) = &mut self.distribution_writer {
            writer.process_record(&profile);
        }

        if let Some(writer) = &mut self.change_log_writer {
            writer.write_change_profile(&profile).await?;
        }

        if let Some(writer) = &mut self.change_image_writer {
            writer.add_change_profile(&profile).await?;    
        }

        if let Some(writer) = &mut self.rates_log_writer {
            writer.process_record(record, change);
        }

        // Update summary statistics
        self.summary.change_rec_count += 1;
        self.summary.change_rec_time_in_reactivator.update(profile.time_in_reactivator);
        self.summary.change_rec_time_in_src_change_q.update(profile.time_in_src_change_q);
        self.summary.change_rec_time_in_src_change_rtr.update(profile.time_in_src_change_rtr);
        self.summary.change_rec_time_in_src_disp_q.update(profile.time_in_src_disp_q);
        self.summary.change_rec_time_in_src_change_disp.update(profile.time_in_src_change_disp);
        self.summary.change_rec_time_in_query_pub_api.update(profile.time_in_query_pub_api);
        self.summary.change_rec_time_in_query_change_q.update(profile.time_in_query_change_q);
        self.summary.change_rec_time_in_query_host.update(profile.time_in_query_host);
        self.summary.change_rec_time_in_query_solver.update(profile.time_in_query_solver);
        self.summary.change_rec_time_in_result_q.update(profile.time_in_result_q);
        self.summary.change_rec_time_total.update(profile.time_total);
        
        Ok(())
    }
    
    // Finalize all statistics for the summary
    fn finalize_summary_stats(&mut self) {

        // Finalize bootstrap statistics - count is now tracked inside TimingStats
        self.summary.bootstrap_rec_time_total.finalize();
        
        // Finalize change record statistics - count is now tracked inside each TimingStats
        self.summary.change_rec_time_in_reactivator.finalize();
        self.summary.change_rec_time_in_src_change_q.finalize();
        self.summary.change_rec_time_in_src_change_rtr.finalize();
        self.summary.change_rec_time_in_src_disp_q.finalize();
        self.summary.change_rec_time_in_src_change_disp.finalize();
        self.summary.change_rec_time_in_query_pub_api.finalize();
        self.summary.change_rec_time_in_query_change_q.finalize();
        self.summary.change_rec_time_in_query_host.finalize();
        self.summary.change_rec_time_in_query_solver.finalize();
        self.summary.change_rec_time_in_result_q.finalize();
        self.summary.change_rec_time_total.finalize();
    }

    // Write summary to JSON file
    async fn write_summary(&self) -> anyhow::Result<()> {
        let summary_path = self.settings.folder_path.join("summary.json");
        write(
            summary_path, 
            serde_json::to_string_pretty(&self.summary)?
        ).await?;
        Ok(())
    }
}

#[async_trait]
impl ResultStreamLogger for ProfilerResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {

        // Close open writers.
        if let Some(writer) = &mut self.bootstrap_log_writer {
            writer.close().await?;
        }

        if let Some(writer) = &mut self.change_log_writer {
            writer.close().await?;
        }        

        // Finalize statistics and write summary
        self.finalize_summary_stats();
        self.write_summary().await?;

        // Generate final outputs from writers
        if let Some(writer) = &mut self.distribution_writer {
            writer.generate_csv().await?;
        }

        if let Some(writer) = &mut self.change_image_writer {
            writer.generate_image().await?;
        }

        if let Some(writer) = &mut self.rates_log_writer {
            writer.generate_csv().await?;
        }
        
        Ok(ResultStreamLoggerResult {
            has_output: true,
            logger_name: "Profiler".to_string(),
            output_folder_path: Some(self.settings.folder_path.clone()),
        })
    }
    
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        match &record.record_data {
            QueryResultRecord::Change(change) => {
                if change.base.metadata.is_some() {
                    // Process change record with metadata
                    self.process_change_record(record, change).await?;
                } else {
                    // Process bootstrap record
                    self.process_bootstrap_record(record, change).await?;
                }
            },
            QueryResultRecord::Control(_) => {
                self.summary.control_rec_count += 1;
            }
        }

        Ok(())
    }
}
