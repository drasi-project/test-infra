use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File, write}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
    pub max_lines_per_file: Option<u64>,
    pub write_bootstrap_log: Option<bool>,
    pub write_change_log: Option<bool>,
}

#[derive(Debug)]
pub struct ProfilerResultStreamLoggerSettings {
    pub bootstrap_log_name: String,
    pub change_log_name: String,    
    pub folder_path: PathBuf,
    pub test_run_query_id: TestRunQueryId,
    pub max_lines_per_file: u64,
    pub write_bootstrap_log: bool,
    pub write_change_log: bool,
}

impl ProfilerResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &ProfilerResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        let time = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();

        return Ok(Self {
            bootstrap_log_name: format!("{}_bootstrap.jsonl", time),
            change_log_name: format!("{}_change.jsonl", time),
            folder_path,
            test_run_query_id,
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            write_bootstrap_log: config.write_bootstrap_log.unwrap_or(false),
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
            time_total: (record.dequeue_time_ns / 1_000_000) - (change.base.source_time_ms as u64),
        }
    }
}

#[derive(Debug, Serialize)]
struct ChangeRecordProfile {
    pub seq: i64,
    pub time_in_src_change_q: u64,
    pub time_in_src_change_rtr: u64,
    pub time_in_src_disp_q: u64,
    pub time_in_src_change_disp: u64,
    pub time_in_src_change_pub: u64,
    pub time_in_query_host: u64,
    pub time_in_query_solver: u64,
    pub time_in_result_disp_q: u64,
    pub time_total: u64,
}

impl ChangeRecordProfile {
    pub fn new(record: &ResultStreamRecord, change: &ChangeEvent) -> Self {

        let record_dequeue_time_ms = record.dequeue_time_ns / 1_000_000;

        let metadata = &change.base.metadata.as_ref().unwrap().tracking;

        Self {
            seq: change.base.sequence,
            time_in_src_change_q: metadata.source.change_svc_start_ms - metadata.source.reactivator_ms,
            time_in_src_change_rtr: metadata.source.change_svc_end_ms - metadata.source.change_svc_start_ms,
            time_in_src_disp_q: metadata.source.change_dispatcher_start_ms - metadata.source.change_svc_end_ms,
            time_in_src_change_disp: metadata.source.change_dispatcher_end_ms - metadata.source.change_dispatcher_start_ms,
            time_in_src_change_pub: metadata.query.dequeue_ms - metadata.source.change_dispatcher_end_ms,
            time_in_query_host: metadata.query.query_end_ms - metadata.query.dequeue_ms,
            time_in_query_solver: metadata.query.query_end_ms - metadata.query.query_start_ms,
            time_in_result_disp_q: record_dequeue_time_ms - metadata.query.query_end_ms,
            time_total: record_dequeue_time_ms - metadata.source.reactivator_ms,
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
    pub change_rec_time_in_change_pub_avg: f64,
    pub change_rec_time_in_change_pub_max: u64,
    pub change_rec_time_in_change_pub_min: u64,
    pub change_rec_time_in_query_host_avg: f64,
    pub change_rec_time_in_query_host_max: u64,
    pub change_rec_time_in_query_host_min: u64,
    pub change_rec_time_in_query_solver_avg: f64,
    pub change_rec_time_in_query_solver_max: u64,
    pub change_rec_time_in_query_solver_min: u64,
    pub change_rec_time_in_result_disp_q_avg: f64,
    pub change_rec_time_in_result_disp_q_max: u64,
    pub change_rec_time_in_result_disp_q_min: u64,
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
            change_rec_time_in_change_pub_avg: 0.0,
            change_rec_time_in_change_pub_max: 0,
            change_rec_time_in_change_pub_min: std::u64::MAX,
            change_rec_time_in_query_host_avg: 0.0,
            change_rec_time_in_query_host_max: 0,
            change_rec_time_in_query_host_min: std::u64::MAX,
            change_rec_time_in_query_solver_avg: 0.0,
            change_rec_time_in_query_solver_max: 0,
            change_rec_time_in_query_solver_min: std::u64::MAX,
            change_rec_time_in_result_disp_q_avg: 0.0,
            change_rec_time_in_result_disp_q_max: 0,
            change_rec_time_in_result_disp_q_min: std::u64::MAX,
            change_rec_time_total_avg: 0.0,
            change_rec_time_total_max: 0,
            change_rec_time_total_min: std::u64::MAX,
            control_rec_count: 0,
        }
    }
    
}

pub struct ProfilerResultStreamLogger {
    bootstrap_log_writer: Option<RecordProfileLogWriter>,
    change_log_writer: Option<RecordProfileLogWriter>,
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
            Some(RecordProfileLogWriter::new(settings.folder_path.clone(), settings.bootstrap_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            None
        };

        let change_log_writer = if settings.write_change_log {
            Some(RecordProfileLogWriter::new(settings.folder_path.clone(), settings.change_log_name.clone(), settings.max_lines_per_file).await?)
        } else {
            None
        };

        Ok(Box::new( Self { 
            bootstrap_log_writer,
            change_log_writer,
            settings,
            summary: ProfilerSummary::default(),
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for ProfilerResultStreamLogger {
    async fn close(&mut self) -> anyhow::Result<()> {

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
            self.summary.change_rec_time_in_src_change_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_change_rtr_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_disp_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_src_change_disp_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_change_pub_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_host_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_query_solver_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_in_result_disp_q_avg /= self.summary.change_rec_count as f64;
            self.summary.change_rec_time_total_avg /= self.summary.change_rec_count as f64;
        } else {
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
            self.summary.change_rec_time_in_change_pub_avg = 0.0;
            self.summary.change_rec_time_in_change_pub_max = 0;
            self.summary.change_rec_time_in_change_pub_min = 0;
            self.summary.change_rec_time_in_query_host_avg = 0.0;
            self.summary.change_rec_time_in_query_host_max = 0;
            self.summary.change_rec_time_in_query_host_min = 0;
            self.summary.change_rec_time_in_query_solver_avg = 0.0;
            self.summary.change_rec_time_in_query_solver_max = 0;
            self.summary.change_rec_time_in_query_solver_min = 0;
            self.summary.change_rec_time_in_result_disp_q_avg = 0.0;
            self.summary.change_rec_time_in_result_disp_q_max = 0;
            self.summary.change_rec_time_in_result_disp_q_min = 0;
            self.summary.change_rec_time_total_avg = 0.0;
            self.summary.change_rec_time_total_max = 0;
            self.summary.change_rec_time_total_min = 0;
        }

        write(summary_path, serde_json::to_string_pretty(&self.summary)?).await?;

        Ok(())
    }
    
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        match &record.record_data {
            QueryResultRecord::Change(change) => {
                if change.base.metadata.is_some() {
                    let profile = ChangeRecordProfile::new(&record, &change);

                    if let Some(writer) = &mut self.change_log_writer {
                        writer.write_change_profile(&profile).await?;
                    }

                    self.summary.change_rec_count += 1;
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
                    self.summary.change_rec_time_in_change_pub_avg += profile.time_in_src_change_pub as f64;
                    self.summary.change_rec_time_in_change_pub_max = std::cmp::max(self.summary.change_rec_time_in_change_pub_max, profile.time_in_src_change_pub);
                    self.summary.change_rec_time_in_change_pub_min = std::cmp::min(self.summary.change_rec_time_in_change_pub_min, profile.time_in_src_change_pub);
                    self.summary.change_rec_time_in_query_host_avg += profile.time_in_query_host as f64;
                    self.summary.change_rec_time_in_query_host_max = std::cmp::max(self.summary.change_rec_time_in_query_host_max, profile.time_in_query_host);
                    self.summary.change_rec_time_in_query_host_min = std::cmp::min(self.summary.change_rec_time_in_query_host_min, profile.time_in_query_host);
                    self.summary.change_rec_time_in_query_solver_avg += profile.time_in_query_solver as f64;
                    self.summary.change_rec_time_in_query_solver_max = std::cmp::max(self.summary.change_rec_time_in_query_solver_max, profile.time_in_query_solver);
                    self.summary.change_rec_time_in_query_solver_min = std::cmp::min(self.summary.change_rec_time_in_query_solver_min, profile.time_in_query_solver);
                    self.summary.change_rec_time_in_result_disp_q_avg += profile.time_in_result_disp_q as f64;
                    self.summary.change_rec_time_in_result_disp_q_max = std::cmp::max(self.summary.change_rec_time_in_result_disp_q_max, profile.time_in_result_disp_q);
                    self.summary.change_rec_time_in_result_disp_q_min = std::cmp::min(self.summary.change_rec_time_in_result_disp_q_min, profile.time_in_result_disp_q);
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

#[derive(Debug, thiserror::Error)]
pub enum RecordProfileLogWriterError {
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

struct RecordProfileLogWriter {
    folder_path: PathBuf,
    log_file_name: String,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_event_count: u64,
}

impl RecordProfileLogWriter { 
    pub async fn new(folder_path: PathBuf, log_file_name: String, max_size: u64) -> anyhow::Result<Self> {
        let mut writer = RecordProfileLogWriter {
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
            let json = format!("{}\n", to_string(profile).map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?;

            self.current_file_event_count += 1;

            if self.current_file_event_count >= self.max_size {
                self.open_next_file().await?;
            }
        }

        Ok(())
    }

    pub async fn write_change_profile(&mut self, profile: &ChangeRecordProfile) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!("{}\n", to_string(profile).map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?;

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
            writer.flush().await.map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the script file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!("{}/{}_{:05}.jsonl", self.folder_path.to_string_lossy(), self.log_file_name, self.next_file_index);

        // Create the file and open it for writing
        let file = File::create(&file_path).await.map_err(|_| RecordProfileLogWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and event count
        self.next_file_index += 1;
        self.current_file_event_count = 0;

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| RecordProfileLogWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}
