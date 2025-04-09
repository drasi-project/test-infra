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

use std::{cmp::max, path::PathBuf};

use async_trait::async_trait;
use image::{Rgb, RgbImage};
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File, write}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError, ResultStreamLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
    pub bootstrap_log_name: Option<String>,
    pub change_image_name: Option<String>,
    pub change_log_name: Option<String>,    
    pub image_width: Option<u32>,
    pub max_lines_per_file: Option<u64>,
    pub write_bootstrap_log: Option<bool>,
    pub write_change_image: Option<bool>,
    pub write_change_log: Option<bool>,
}

#[derive(Debug)]
pub struct ProfilerResultStreamLoggerSettings {
    pub bootstrap_log_name: String,
    pub change_image_name: String,
    pub change_log_name: String,    
    pub folder_path: PathBuf,
    pub image_width: u32,
    pub test_run_query_id: TestRunQueryId,
    pub max_lines_per_file: u64,
    pub write_bootstrap_log: bool,
    pub write_change_image: bool,
    pub write_change_log: bool,
}

impl ProfilerResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &ProfilerResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            bootstrap_log_name: config.bootstrap_log_name.clone().unwrap_or("bootstrap".to_string()),
            change_image_name: config.change_image_name.clone().unwrap_or("change".to_string()),
            change_log_name: config.change_log_name.clone().unwrap_or("change".to_string()),
            image_width: config.image_width.unwrap_or(1200),
            folder_path,
            test_run_query_id,
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            write_bootstrap_log: config.write_bootstrap_log.unwrap_or(false),
            write_change_image: config.write_change_image.unwrap_or(false),
            write_change_log: config.write_change_log.unwrap_or(false),
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
            time_in_query_change_q: metadata.query.dequeue_ns.saturating_sub(metadata.source.change_dispatcher_end_ns),
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

        Ok(Box::new( Self { 
            bootstrap_log_writer,
            change_image_writer,
            change_log_writer,
            settings,
            summary: ProfilerSummary::default(),
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for ProfilerResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {

        if let Some(writer) = &mut self.bootstrap_log_writer {
            writer.close().await?;
        }
        if let Some(writer) = &mut self.change_log_writer {
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
            writer.generate_image().await?;
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
                        writer.write_change_profile(&profile).await?;
                    }

                    if let Some(writer) = &mut self.change_image_writer {
                        writer.write_change_profile(&profile).await?;    
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

const PROFILE_COLORS: [Rgb<u8>; 10] = [
    Rgb([230, 25, 75]),    // Red - reactivator
    Rgb([60, 180, 75]),    // Green - source change queue
    Rgb([255, 225, 25]),   // Yellow - source change router
    Rgb([0, 130, 200]),    // Blue - source dispatch queue
    Rgb([245, 130, 48]),   // Orange - source change dispatcher
    Rgb([145, 30, 180]),   // Purple - query change queue
    Rgb([70, 240, 240]),   // Cyan - query host
    Rgb([240, 50, 230]),   // Magenta - query solver
    Rgb([210, 245, 60]),   // Lime - result queue
    Rgb([128, 128, 128]),  // Gray - shortfall
];

#[allow(dead_code)]
struct ProfileImageWriter {
    all_file_abs_path: PathBuf,
    all_file_rel_path: PathBuf,
    drasi_only_file_abs_path: PathBuf,
    drasi_only_file_rel_path: PathBuf,
    image_times: Vec<u64>,
    max_total_time: u64,
    max_drasi_only_time: u64,
    record_count: usize,
    width: u32,
}

impl ProfileImageWriter {
    pub async fn new(folder_path: PathBuf, file_name: String, width: u32) -> anyhow::Result<Self> {

        Ok(Self {
            all_file_abs_path: folder_path.join(format!("{}_all_abs.png", file_name)),
            all_file_rel_path: folder_path.join(format!("{}_all_rel.png", file_name)),
            drasi_only_file_abs_path: folder_path.join(format!("{}_drasi_only_abs.png", file_name)),
            drasi_only_file_rel_path: folder_path.join(format!("{}_drasi_only_rel.png", file_name)),
            image_times: Vec::new(),
            max_total_time: 0,
            max_drasi_only_time: 0,
            record_count: 0,
            width
        })
    }

    async fn write_change_profile(&mut self, profile: &ChangeRecordProfile) -> anyhow::Result<()> {
        
        let mut times = [
            profile.time_in_reactivator,
            profile.time_in_src_change_q,
            profile.time_in_src_change_rtr,
            profile.time_in_src_disp_q,
            profile.time_in_src_change_disp,
            profile.time_in_query_change_q,
            profile.time_in_query_host,
            profile.time_in_query_solver,
            profile.time_in_result_q,
            0,  // shortfall
            profile.time_total,
            0   // total for drasi only components
        ];

        let drasi_sum = times[0] + times[2] + times[4] + times[6] + times[7];
        let all_sum = drasi_sum + times[1] + times[3] + times[5] + times[8];

        times[9] = times[10] - all_sum;
        times[11] = drasi_sum;

        self.max_total_time = max(self.max_total_time, profile.time_total);
        self.max_drasi_only_time = max(self.max_drasi_only_time, drasi_sum);

        self.image_times.extend(&times);

        self.record_count += 1;

        Ok(())
    }

    pub async fn generate_image(&self) -> anyhow::Result<()> {
        self.generate_all_image().await?;
        self.generate_drasi_only_image().await?;
        Ok(())
    }

    async fn generate_all_image(&self) -> anyhow::Result<()> {

        let header_height: u32 = 20;
        let header_span_width = self.width / PROFILE_COLORS.len() as u32; 
        let height = self.record_count as u32 + header_height;
        let times_per_profile: usize = 12;
        let mut img_abs = RgbImage::new(self.width, height);
        let mut img_rel = RgbImage::new(self.width, height);

        // Draw the header (equal-length spans for each color)
        for y in 0..header_height {
            let mut x = 0;
            for &color in &PROFILE_COLORS {
                for px in x..x + header_span_width {
                    if px < self.width {
                        img_abs.put_pixel(px, y, color);
                        img_rel.put_pixel(px, y, color);
                    }
                }
                x += header_span_width;
            }
        }

        // Draw the image from the spans
        self.image_times
            .chunks(times_per_profile)
            .enumerate()
            .for_each(|(y, raw_times)| {

                // Absolute
                let mut x = 0;
                let mut pixels_per_unit = self.width as f64 / self.max_total_time as f64;
                let mut span_width: u32;
                for i in 0..10 {
                    if raw_times[i] > 0 {
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_abs.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }

                // Relative
                x = 0;
                pixels_per_unit = self.width as f64 / raw_times[10] as f64;
                for i in 0..10 {
                    if raw_times[i] > 0 {                        
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_rel.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }
            });

        // Save the image
        img_abs.save(&self.all_file_abs_path)?;
        img_rel.save(&self.all_file_rel_path)?;

        Ok(())
    }

    async fn generate_drasi_only_image(&self) -> anyhow::Result<()> {

        let header_height: u32 = 20;
        let header_span_width = self.width / PROFILE_COLORS.len() as u32; 
        let height = self.record_count as u32 + header_height;
        let times_per_profile: usize = 12;
        let mut img_abs = RgbImage::new(self.width, height);
        let mut img_rel = RgbImage::new(self.width, height);

        // Draw the header (equal-length spans for each color)
        for y in 0..header_height {
            let mut x = 0;
            for &color in &PROFILE_COLORS {
                for px in x..x + header_span_width {
                    if px < self.width {
                        img_abs.put_pixel(px, y, color);
                        img_rel.put_pixel(px, y, color);
                    }
                }
                x += header_span_width;
            }
        }

        // Draw the image from the spans
        self.image_times
            .chunks(times_per_profile)
            .enumerate()
            .for_each(|(y, raw_times)| {

                // Absolute
                let mut x = 0;
                let mut pixels_per_unit = self.width as f64 / self.max_drasi_only_time as f64;
                let mut span_width: u32;
                for i in [0, 2, 4, 6, 7] {
                    if raw_times[i] > 0 {
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_abs.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }

                // Relative
                x = 0;
                pixels_per_unit = self.width as f64 / raw_times[11] as f64;
                for i in [0, 2, 4, 6, 7] {
                    if raw_times[i] > 0 {                        
                        span_width = (raw_times[i] as f64 * pixels_per_unit).round() as u32;

                        if span_width > 0 {
                            for px in x..x + span_width {
                                if px < self.width {
                                    img_rel.put_pixel(px, y as u32 + header_height, PROFILE_COLORS[i]);
                                }
                            }
                            x += span_width;
                        }
                    };
                }
            });

        // Save the image
        img_abs.save(&self.drasi_only_file_abs_path)?;
        img_rel.save(&self.drasi_only_file_rel_path)?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProfileLogWriterError {
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

struct ProfileLogWriter {
    folder_path: PathBuf,
    log_file_name: String,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_event_count: u64,
}

impl ProfileLogWriter { 
    pub async fn new(folder_path: PathBuf, log_file_name: String, max_size: u64) -> anyhow::Result<Self> {
        let mut writer = ProfileLogWriter {
            folder_path,
            log_file_name,
            next_file_index: 0,
            current_writer: None,
            max_size,
            current_file_event_count: 0,
        };

        writer.open_next_file().await?;
        Ok(writer)
    }

    pub async fn write_bootstrap_profile(&mut self, profile: &BootstrapRecordProfile) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!("{}\n", to_string(profile).map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?;

            self.current_file_event_count += 1;

            if self.current_file_event_count >= self.max_size {
                self.open_next_file().await?;
            }
        }

        Ok(())
    }

    pub async fn write_change_profile(&mut self, profile: &ChangeRecordProfile) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!("{}\n", to_string(profile).map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?;

            self.current_file_event_count += 1;

            if self.current_file_event_count >= self.max_size {
                self.open_next_file().await?;
            }
        }

        Ok(())
    }

    async fn open_next_file(&mut self) -> anyhow::Result<()> {
        // If there is a current writer, flush it and close it.
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the script file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!("{}/{}_{:05}.jsonl", self.folder_path.to_string_lossy(), self.log_file_name, self.next_file_index);

        // Create the file and open it for writing
        let file = File::create(&file_path).await.map_err(|_| ProfileLogWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and event count
        self.next_file_index += 1;
        self.current_file_event_count = 0;

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| ProfileLogWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}
