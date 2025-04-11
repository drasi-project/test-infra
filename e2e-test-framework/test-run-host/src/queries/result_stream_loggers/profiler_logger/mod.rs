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
use image_writer::ProfileImageWriter;
use log_writer::ProfileLogWriter;
use rate_writer::RateTracker;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, write};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError, ResultStreamLoggerResult};

mod image_writer;
mod log_writer;
mod rate_writer;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
    pub bootstrap_log_name: Option<String>,
    pub change_image_name: Option<String>,
    pub change_log_name: Option<String>, 
    pub rates_log_name: Option<String>,   
    pub image_width: Option<u32>,
    pub max_lines_per_file: Option<u64>,
    pub write_bootstrap_log: Option<bool>,
    pub write_change_image: Option<bool>,
    pub write_change_log: Option<bool>,
    pub write_change_rates: Option<bool>,
}

#[derive(Debug)]
pub struct ProfilerResultStreamLoggerSettings {
    pub bootstrap_log_name: String,
    pub change_image_name: String,
    pub change_log_name: String,    
    pub rates_log_name: String,
    pub folder_path: PathBuf,
    pub image_width: u32,
    pub test_run_query_id: TestRunQueryId,
    pub max_lines_per_file: u64,
    pub write_bootstrap_log: bool,
    pub write_change_image: bool,
    pub write_change_log: bool,
    pub write_change_rates: bool,
}

impl ProfilerResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &ProfilerResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            bootstrap_log_name: config.bootstrap_log_name.clone().unwrap_or("bootstrap".to_string()),
            change_image_name: config.change_image_name.clone().unwrap_or("change".to_string()),
            change_log_name: config.change_log_name.clone().unwrap_or("change".to_string()),
            rates_log_name: config.rates_log_name.clone().unwrap_or("change".to_string()),
            image_width: config.image_width.unwrap_or(1200),
            folder_path,
            test_run_query_id,
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            write_bootstrap_log: config.write_bootstrap_log.unwrap_or(false),
            write_change_image: config.write_change_image.unwrap_or(false),
            write_change_log: config.write_change_log.unwrap_or(false),
            write_change_rates: config.write_change_rates.unwrap_or(false),
        });
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
    pub time_in_reactivator: u64,
    pub time_in_src_change_q: u64,
    pub time_in_src_change_rtr: u64,
    pub time_in_src_disp_q: u64,
    pub time_in_src_change_disp: u64,
    pub time_in_query_pub_api: u64,
    pub time_in_query_change_q: u64,
    pub time_in_query_host: u64,
    pub time_in_query_solver: u64,
    pub time_in_result_q: u64,
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

#[derive(Debug, Serialize)]
struct ProfilerSummary{
    pub bootstrap_rec_count: usize,
    pub bootstrap_rec_time_total_avg: f64,
    pub bootstrap_rec_time_total_max: u64,
    pub bootstrap_rec_time_total_min: u64,
    pub change_rec_count: usize,
    pub change_rec_time_in_reactivator_avg: f64,
    pub change_rec_time_in_reactivator_max: u64,
    pub change_rec_time_in_reactivator_min: u64,
    pub change_rec_time_in_src_change_q_avg: f64,
    pub change_rec_time_in_src_change_q_max: u64,
    pub change_rec_time_in_src_change_q_min: u64,
    pub change_rec_time_in_src_change_rtr_avg: f64,
    pub change_rec_time_in_src_change_rtr_max: u64,
    pub change_rec_time_in_src_change_rtr_min: u64,
    pub change_rec_time_in_src_disp_q_avg: f64,
    pub change_rec_time_in_src_disp_q_max: u64,
    pub change_rec_time_in_src_disp_q_min: u64,
    pub change_rec_time_in_src_change_disp_avg: f64,
    pub change_rec_time_in_src_change_disp_max: u64,
    pub change_rec_time_in_src_change_disp_min: u64,
    pub change_rec_time_in_query_pub_api_avg: f64,
    pub change_rec_time_in_query_pub_api_max: u64,
    pub change_rec_time_in_query_pub_api_min: u64,
    pub change_rec_time_in_query_change_q_avg: f64,
    pub change_rec_time_in_query_change_q_max: u64,
    pub change_rec_time_in_query_change_q_min: u64,
    pub change_rec_time_in_query_host_avg: f64,
    pub change_rec_time_in_query_host_max: u64,
    pub change_rec_time_in_query_host_min: u64,
    pub change_rec_time_in_query_solver_avg: f64,
    pub change_rec_time_in_query_solver_max: u64,
    pub change_rec_time_in_query_solver_min: u64,
    pub change_rec_time_in_result_q_avg: f64,
    pub change_rec_time_in_result_q_max: u64,
    pub change_rec_time_in_result_q_min: u64,
    pub change_rec_time_total_avg: f64,
    pub change_rec_time_total_max: u64,
    pub change_rec_time_total_min: u64,
    pub control_rec_count: usize,
}

impl Default for ProfilerSummary {
    fn default() -> Self {
        Self {
            bootstrap_rec_count: 0,
            bootstrap_rec_time_total_avg: 0.0,
            bootstrap_rec_time_total_max: 0,
            bootstrap_rec_time_total_min: std::u64::MAX,
            change_rec_count: 0,
            change_rec_time_in_reactivator_avg: 0.0,
            change_rec_time_in_reactivator_max: 0,
            change_rec_time_in_reactivator_min: 0,        
            change_rec_time_in_src_change_q_avg: 0.0,
            change_rec_time_in_src_change_q_max: 0,
            change_rec_time_in_src_change_q_min: std::u64::MAX,
            change_rec_time_in_src_change_rtr_avg: 0.0,
            change_rec_time_in_src_change_rtr_max: 0,
            change_rec_time_in_src_change_rtr_min: std::u64::MAX,
            change_rec_time_in_src_disp_q_avg: 0.0,
            change_rec_time_in_src_disp_q_max: 0,
            change_rec_time_in_src_disp_q_min: std::u64::MAX,
            change_rec_time_in_src_change_disp_avg: 0.0,
            change_rec_time_in_src_change_disp_max: 0,
            change_rec_time_in_src_change_disp_min: std::u64::MAX,
            change_rec_time_in_query_pub_api_avg: 0.0,
            change_rec_time_in_query_pub_api_max: 0,
            change_rec_time_in_query_pub_api_min: std::u64::MAX,
            change_rec_time_in_query_change_q_avg: 0.0,
            change_rec_time_in_query_change_q_max: 0,
            change_rec_time_in_query_change_q_min: std::u64::MAX,
            change_rec_time_in_query_host_avg: 0.0,
            change_rec_time_in_query_host_max: 0,
            change_rec_time_in_query_host_min: std::u64::MAX,
            change_rec_time_in_query_solver_avg: 0.0,
            change_rec_time_in_query_solver_max: 0,
            change_rec_time_in_query_solver_min: std::u64::MAX,
            change_rec_time_in_result_q_avg: 0.0,
            change_rec_time_in_result_q_max: 0,
            change_rec_time_in_result_q_min: std::u64::MAX,
            change_rec_time_total_avg: 0.0,
            change_rec_time_total_max: 0,
            change_rec_time_total_min: std::u64::MAX,
            control_rec_count: 0,
        }
    }
    
}

pub struct ProfilerResultStreamLogger {    
    bootstrap_log_writer: Option<ProfileLogWriter>,
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
            log::error!("Creating bootstrap log writer");
            Some(ProfileLogWriter::new(settings.folder_path.clone(), settings.bootstrap_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            log::error!("Not creating bootstrap log writer");
            None
        };

        let change_log_writer = if settings.write_change_log {
            log::error!("Creating change log writer");
            Some(ProfileLogWriter::new(settings.folder_path.clone(), settings.change_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            log::error!("Not creating change log writer");
            None
        };

        let change_image_writer = if settings.write_change_image {
            log::error!("Creating change image writer");
            Some(ProfileImageWriter::new(settings.folder_path.clone(), settings.change_image_name.clone(), settings.image_width).await?)
        } else {
            log::error!("Not creating change image writer");
            None
        };        

        let rates_log_writer = if settings.write_change_rates {
            log::error!("Creating rates log writer");
            Some(RateTracker::new(settings.folder_path.clone(), settings.rates_log_name.clone()))
        } else {
            log::error!("Not creating rates log writer");
            None
        };

        Ok(Box::new( Self { 
            bootstrap_log_writer,
            change_image_writer,
            change_log_writer,
            rates_log_writer,
            settings,
            summary: ProfilerSummary::default(),
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for ProfilerResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {

        if let Some(writer) = &mut self.bootstrap_log_writer {
            log::error!("Closing bootstrap log writer");
            writer.close().await?;
        }

        if let Some(writer) = &mut self.change_log_writer {
            log::error!("Closing change log writer");
            writer.close().await?;
        }        

        let summary_path = self.settings.folder_path.join("test_run_summary.json");

        if self.summary.bootstrap_rec_count > 0 {
            self.summary.bootstrap_rec_time_total_avg /= self.summary.bootstrap_rec_count as f64;
        } else {
            self.summary.bootstrap_rec_time_total_avg = 0.0;
            self.summary.bootstrap_rec_time_total_max = 0;
            self.summary.bootstrap_rec_time_total_min = 0;
        }

        if self.summary.change_rec_count > 0 {
            self.summary.change_rec_time_in_reactivator_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_change_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_change_rtr_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_disp_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_change_disp_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_pub_api_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_change_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_host_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_solver_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_result_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_total_avg /= self.summary.change_rec_count as f64;
        } else {
            self.summary.change_rec_time_in_reactivator_avg = 0.0;
            self.summary.change_rec_time_in_reactivator_max = 0;
            self.summary.change_rec_time_in_reactivator_min = 0;
            self.summary.change_rec_time_in_src_change_q_avg = 0.0;
            self.summary.change_rec_time_in_src_change_q_max = 0;
            self.summary.change_rec_time_in_src_change_q_min = 0;
            self.summary.change_rec_time_in_src_change_rtr_avg = 0.0;
            self.summary.change_rec_time_in_src_change_rtr_max = 0;
            self.summary.change_rec_time_in_src_change_rtr_min = 0;
            self.summary.change_rec_time_in_src_disp_q_avg = 0.0;
            self.summary.change_rec_time_in_src_disp_q_max = 0;
            self.summary.change_rec_time_in_src_disp_q_min = 0;
            self.summary.change_rec_time_in_src_change_disp_avg = 0.0;
            self.summary.change_rec_time_in_src_change_disp_max = 0;
            self.summary.change_rec_time_in_src_change_disp_min = 0;
            self.summary.change_rec_time_in_query_pub_api_avg = 0.0;
            self.summary.change_rec_time_in_query_pub_api_max = 0;
            self.summary.change_rec_time_in_query_pub_api_min = 0;
            self.summary.change_rec_time_in_query_change_q_avg = 0.0;
            self.summary.change_rec_time_in_query_change_q_max = 0;
            self.summary.change_rec_time_in_query_change_q_min = 0;
            self.summary.change_rec_time_in_query_host_avg = 0.0;
            self.summary.change_rec_time_in_query_host_max = 0;
            self.summary.change_rec_time_in_query_host_min = 0;
            self.summary.change_rec_time_in_query_solver_avg = 0.0;
            self.summary.change_rec_time_in_query_solver_max = 0;
            self.summary.change_rec_time_in_query_solver_min = 0;
            self.summary.change_rec_time_in_result_q_avg = 0.0;
            self.summary.change_rec_time_in_result_q_max = 0;
            self.summary.change_rec_time_in_result_q_min = 0;
            self.summary.change_rec_time_total_avg = 0.0;
            self.summary.change_rec_time_total_max = 0;
            self.summary.change_rec_time_total_min = 0;
        }

        write(summary_path, serde_json::to_string_pretty(&self.summary)?).await?;

        if let Some(writer) = &mut self.change_image_writer {
            log::error!("Closing change image writer");
            writer.generate_image().await?;
        }

        if let Some(writer) = &mut self.rates_log_writer {
            log::error!("Closing rates log writer");
            writer.write_to_csv().await?;
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
                    let profile = ChangeRecordProfile::new(&record, &change);

                    if let Some(writer) = &mut self.change_log_writer {
                        log::error!("Writing change log profile");
                        writer.write_change_profile(&profile).await?;
                    }

                    if let Some(writer) = &mut self.change_image_writer {
                        log::error!("Writing change image profile");
                        writer.add_change_profile(&profile).await?;    
                    }

                    if let Some(writer) = &mut self.rates_log_writer {
                        log::error!("Writing change rates profile");
                        writer.process_record(&record, &change);
                    }

                    self.summary.change_rec_count += 1;
                    self.summary.change_rec_time_in_reactivator_avg += profile.time_in_reactivator as f64;
                    self.summary.change_rec_time_in_reactivator_max = std::cmp::max(self.summary.change_rec_time_in_reactivator_max, profile.time_in_reactivator);
                    self.summary.change_rec_time_in_reactivator_min = std::cmp::min(self.summary.change_rec_time_in_reactivator_min, profile.time_in_reactivator);
                    self.summary.change_rec_time_in_src_change_q_avg += profile.time_in_src_change_q as f64;
                    self.summary.change_rec_time_in_src_change_q_max = std::cmp::max(self.summary.change_rec_time_in_src_change_q_max, profile.time_in_src_change_q);
                    self.summary.change_rec_time_in_src_change_q_min = std::cmp::min(self.summary.change_rec_time_in_src_change_q_min, profile.time_in_src_change_q);
                    self.summary.change_rec_time_in_src_change_rtr_avg += profile.time_in_src_change_rtr as f64;
                    self.summary.change_rec_time_in_src_change_rtr_max = std::cmp::max(self.summary.change_rec_time_in_src_change_rtr_max, profile.time_in_src_change_rtr);
                    self.summary.change_rec_time_in_src_change_rtr_min = std::cmp::min(self.summary.change_rec_time_in_src_change_rtr_min, profile.time_in_src_change_rtr);
                    self.summary.change_rec_time_in_src_disp_q_avg += profile.time_in_src_disp_q as f64;
                    self.summary.change_rec_time_in_src_disp_q_max = std::cmp::max(self.summary.change_rec_time_in_src_disp_q_max, profile.time_in_src_disp_q);
                    self.summary.change_rec_time_in_src_disp_q_min = std::cmp::min(self.summary.change_rec_time_in_src_disp_q_min, profile.time_in_src_disp_q);
                    self.summary.change_rec_time_in_src_change_disp_avg += profile.time_in_src_change_disp as f64;
                    self.summary.change_rec_time_in_src_change_disp_max = std::cmp::max(self.summary.change_rec_time_in_src_change_disp_max, profile.time_in_src_change_disp);
                    self.summary.change_rec_time_in_src_change_disp_min = std::cmp::min(self.summary.change_rec_time_in_src_change_disp_min, profile.time_in_src_change_disp);
                    self.summary.change_rec_time_in_query_pub_api_avg += profile.time_in_query_pub_api as f64;
                    self.summary.change_rec_time_in_query_pub_api_max = std::cmp::max(self.summary.change_rec_time_in_query_pub_api_max, profile.time_in_query_pub_api);
                    self.summary.change_rec_time_in_query_pub_api_min = std::cmp::min(self.summary.change_rec_time_in_query_pub_api_min, profile.time_in_query_pub_api);                
                    self.summary.change_rec_time_in_query_change_q_avg += profile.time_in_query_change_q as f64;
                    self.summary.change_rec_time_in_query_change_q_max = std::cmp::max(self.summary.change_rec_time_in_query_change_q_max, profile.time_in_query_change_q);
                    self.summary.change_rec_time_in_query_change_q_min = std::cmp::min(self.summary.change_rec_time_in_query_change_q_min, profile.time_in_query_change_q);
                    self.summary.change_rec_time_in_query_host_avg += profile.time_in_query_host as f64;
                    self.summary.change_rec_time_in_query_host_max = std::cmp::max(self.summary.change_rec_time_in_query_host_max, profile.time_in_query_host);
                    self.summary.change_rec_time_in_query_host_min = std::cmp::min(self.summary.change_rec_time_in_query_host_min, profile.time_in_query_host);
                    self.summary.change_rec_time_in_query_solver_avg += profile.time_in_query_solver as f64;
                    self.summary.change_rec_time_in_query_solver_max = std::cmp::max(self.summary.change_rec_time_in_query_solver_max, profile.time_in_query_solver);
                    self.summary.change_rec_time_in_query_solver_min = std::cmp::min(self.summary.change_rec_time_in_query_solver_min, profile.time_in_query_solver);
                    self.summary.change_rec_time_in_result_q_avg += profile.time_in_result_q as f64;
                    self.summary.change_rec_time_in_result_q_max = std::cmp::max(self.summary.change_rec_time_in_result_q_max, profile.time_in_result_q);
                    self.summary.change_rec_time_in_result_q_min = std::cmp::min(self.summary.change_rec_time_in_result_q_min, profile.time_in_result_q);
                    self.summary.change_rec_time_total_avg += profile.time_total as f64;
                    self.summary.change_rec_time_total_max = std::cmp::max(self.summary.change_rec_time_total_max, profile.time_total);
                    self.summary.change_rec_time_total_min = std::cmp::min(self.summary.change_rec_time_total_min, profile.time_total);   
                } else {
                    let profile = BootstrapRecordProfile::new(&record, &change);

                    if let Some(writer) = &mut self.bootstrap_log_writer {
                        writer.write_bootstrap_profile(&profile).await?;
                    }

                    self.summary.bootstrap_rec_count += 1;
                    self.summary.bootstrap_rec_time_total_avg += profile.time_total as f64;
                    self.summary.bootstrap_rec_time_total_max = std::cmp::max(self.summary.bootstrap_rec_time_total_max, profile.time_total);
                    self.summary.bootstrap_rec_time_total_min = std::cmp::min(self.summary.bootstrap_rec_time_total_min, profile.time_total);   
                }
            },
            QueryResultRecord::Control(_) => {
                self.summary.control_rec_count += 1;
            }
        }

        Ok(())
    }
}
