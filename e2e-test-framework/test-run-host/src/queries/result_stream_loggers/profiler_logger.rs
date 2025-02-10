use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, write};

use test_data_store::test_run_storage::TestRunQueryStorage;

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
}

#[derive(Debug)]
pub struct ProfilerResultStreamLoggerSettings {
    pub folder_path: PathBuf,
}

impl ProfilerResultStreamLoggerSettings {
    pub fn new(_config: &ProfilerResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
        });
    }
}

struct BootstrapRecordProfile {
    pub total_dur_ms: u64,
}

impl BootstrapRecordProfile {
    pub fn new(record: &ResultStreamRecord, change: &ChangeEvent) -> Self {
        Self {
            total_dur_ms: (record.dequeue_time_ns / 1_000_000) - (change.base.source_time_ms as u64),
        }
    }
}

struct ChangeRecordProfile {
    pub source_change_queue_dur_ms: u64,
    pub source_change_router_dur_ms: u64,
    pub source_dispatch_queue_dur_ms: u64,
    pub source_change_dispatcher_dur_ms: u64,
    pub change_dispatch_dur_ms: u64,
    pub query_host_dur_ms: u64,
    pub query_solver_dur_ms: u64,
    pub result_dispatch_dur_ms: u64,
    pub total_dur_ms: u64,
}

impl ChangeRecordProfile {
    pub fn new(record: &ResultStreamRecord, change: &ChangeEvent) -> Self {

        let record_dequeue_time_ms = record.dequeue_time_ns / 1_000_000;

        let metadata = change.base.metadata.as_ref().unwrap();

        let source_change_queue_dur_ms = metadata.tracking.source.change_svc_start_ms - metadata.tracking.source.reactivator_ms;
        let source_change_router_dur_ms = metadata.tracking.source.change_svc_end_ms - metadata.tracking.source.change_svc_start_ms;
        let source_dispatch_queue_dur_ms = metadata.tracking.source.change_dispatcher_start_ms - metadata.tracking.source.change_svc_end_ms;
        let source_change_dispatcher_dur_ms = metadata.tracking.source.change_dispatcher_end_ms - metadata.tracking.source.change_dispatcher_start_ms;
        let change_dispatch_dur_ms = metadata.tracking.query.dequeue_ms - metadata.tracking.source.change_dispatcher_end_ms;
        let query_host_dur_ms = metadata.tracking.query.query_end_ms - metadata.tracking.query.dequeue_ms;
        let query_solver_dur_ms = metadata.tracking.query.query_end_ms - metadata.tracking.query.query_start_ms;
        let result_dispatch_dur_ms = record_dequeue_time_ms - metadata.tracking.query.query_end_ms;
        let total_dur_ms = record_dequeue_time_ms - metadata.tracking.source.reactivator_ms;

        Self {
            source_change_queue_dur_ms,
            source_change_router_dur_ms,
            source_dispatch_queue_dur_ms,
            source_change_dispatcher_dur_ms,
            change_dispatch_dur_ms,
            query_host_dur_ms,
            query_solver_dur_ms,
            result_dispatch_dur_ms,
            total_dur_ms,
        }
    }
}

#[derive(Debug, Serialize)]
struct ProfilerSummary{
    pub control_record_count: usize,
    pub bootstrap_record_count: usize,
    pub bootstrap_total_dur_ms_avg: f64,
    pub bootstrap_total_dur_ms_max: u64,
    pub bootstrap_total_dur_ms_min: u64,
    pub change_record_count: usize,
    pub source_change_queue_dur_ms_avg: f64,
    pub source_change_queue_dur_ms_max: u64,
    pub source_change_queue_dur_ms_min: u64,
    pub source_change_router_dur_ms_avg: f64,
    pub source_change_router_dur_ms_max: u64,
    pub source_change_router_dur_ms_min: u64,
    pub source_dispatch_queue_dur_ms_avg: f64,
    pub source_dispatch_queue_dur_ms_max: u64,
    pub source_dispatch_queue_dur_ms_min: u64,
    pub source_change_dispatcher_dur_ms_avg: f64,
    pub source_change_dispatcher_dur_ms_max: u64,
    pub source_change_dispatcher_dur_ms_min: u64,
    pub change_dispatch_dur_ms_avg: f64,
    pub change_dispatch_dur_ms_max: u64,
    pub change_dispatch_dur_ms_min: u64,
    pub query_host_dur_ms_avg: f64,
    pub query_host_dur_ms_max: u64,
    pub query_host_dur_ms_min: u64,
    pub query_solver_dur_ms_avg: f64,
    pub query_solver_dur_ms_max: u64,
    pub query_solver_dur_ms_min: u64,
    pub result_dispatch_dur_ms_avg: f64,
    pub result_dispatch_dur_ms_max: u64,
    pub result_dispatch_dur_ms_min: u64,
    pub total_dur_ms_avg: f64,
    pub total_dur_ms_max: u64,
    pub total_dur_ms_min: u64,
}

impl ProfilerSummary {
    pub fn new() -> Self {
        Self {
            control_record_count: 0,
            bootstrap_record_count: 0,
            bootstrap_total_dur_ms_avg: 0.0,
            bootstrap_total_dur_ms_max: 0,
            bootstrap_total_dur_ms_min: std::u64::MAX,
            change_record_count: 0,
            source_change_queue_dur_ms_avg: 0.0,
            source_change_queue_dur_ms_max: 0,
            source_change_queue_dur_ms_min: std::u64::MAX,
            source_change_router_dur_ms_avg: 0.0,
            source_change_router_dur_ms_max: 0,
            source_change_router_dur_ms_min: std::u64::MAX,
            source_dispatch_queue_dur_ms_avg: 0.0,
            source_dispatch_queue_dur_ms_max: 0,
            source_dispatch_queue_dur_ms_min: std::u64::MAX,
            source_change_dispatcher_dur_ms_avg: 0.0,
            source_change_dispatcher_dur_ms_max: 0,
            source_change_dispatcher_dur_ms_min: std::u64::MAX,
            change_dispatch_dur_ms_avg: 0.0,
            change_dispatch_dur_ms_max: 0,
            change_dispatch_dur_ms_min: std::u64::MAX,
            query_host_dur_ms_avg: 0.0,
            query_host_dur_ms_max: 0,
            query_host_dur_ms_min: std::u64::MAX,
            query_solver_dur_ms_avg: 0.0,
            query_solver_dur_ms_max: 0,
            query_solver_dur_ms_min: std::u64::MAX,
            result_dispatch_dur_ms_avg: 0.0,
            result_dispatch_dur_ms_max: 0,
            result_dispatch_dur_ms_min: std::u64::MAX,
            total_dur_ms_avg: 0.0,
            total_dur_ms_max: 0,
            total_dur_ms_min: std::u64::MAX,
        }
    }
    
}

pub struct ProfilerResultStreamLogger {
    bootstrap_records: Vec<BootstrapRecordProfile>,
    change_records: Vec<ChangeRecordProfile>,
    control_record_count: usize,
    settings: ProfilerResultStreamLoggerSettings,
}

impl ProfilerResultStreamLogger {
    pub async fn new(def:&ProfilerResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating ProfilerResultStreamLogger from {:?}, ", def);

        let folder_path = output_storage.result_change_path.join("profiler");
        let settings = ProfilerResultStreamLoggerSettings::new(&def, folder_path)?;
        log::trace!("Creating ProfilerResultStreamLogger with settings {:?}, ", settings);

        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {},
                Err(e) => return Err(ResultStreamLoggerError::Io(e).into()),
            };
        }        

        Ok(Box::new( Self { 
            bootstrap_records: Vec::new(),
            change_records: Vec::new(),
            control_record_count: 0,
            settings,
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for ProfilerResultStreamLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        let summary_path = self.settings.folder_path.join("test_run_summary.json");
        let mut summary = ProfilerSummary::new();

        summary.control_record_count = self.control_record_count;

        if !self.bootstrap_records.is_empty() {
            for record in &self.bootstrap_records {
                summary.bootstrap_record_count += 1;
                summary.bootstrap_total_dur_ms_avg += record.total_dur_ms as f64;

                summary.bootstrap_total_dur_ms_max = std::cmp::max(summary.bootstrap_total_dur_ms_max, record.total_dur_ms);
                summary.bootstrap_total_dur_ms_min = std::cmp::min(summary.bootstrap_total_dur_ms_min, record.total_dur_ms);
            };

            summary.bootstrap_total_dur_ms_avg /= summary.bootstrap_record_count as f64;
        }

        if !self.change_records.is_empty() {
            for record in &self.change_records {
                summary.change_record_count += 1;
                summary.source_change_queue_dur_ms_avg += record.source_change_queue_dur_ms as f64;
                summary.source_change_router_dur_ms_avg += record.source_change_router_dur_ms as f64;
                summary.source_dispatch_queue_dur_ms_avg += record.source_dispatch_queue_dur_ms as f64;
                summary.source_change_dispatcher_dur_ms_avg += record.source_change_dispatcher_dur_ms as f64;
                summary.change_dispatch_dur_ms_avg += record.change_dispatch_dur_ms as f64;
                summary.query_host_dur_ms_avg += record.query_host_dur_ms as f64;
                summary.query_solver_dur_ms_avg += record.query_solver_dur_ms as f64;
                summary.result_dispatch_dur_ms_avg += record.result_dispatch_dur_ms as f64;
                summary.total_dur_ms_avg += record.total_dur_ms as f64;

                summary.source_change_queue_dur_ms_max = std::cmp::max(summary.source_change_queue_dur_ms_max, record.source_change_queue_dur_ms);
                summary.source_change_router_dur_ms_max = std::cmp::max(summary.source_change_router_dur_ms_max, record.source_change_router_dur_ms);
                summary.source_dispatch_queue_dur_ms_max = std::cmp::max(summary.source_dispatch_queue_dur_ms_max, record.source_dispatch_queue_dur_ms);
                summary.source_change_dispatcher_dur_ms_max = std::cmp::max(summary.source_change_dispatcher_dur_ms_max, record.source_change_dispatcher_dur_ms);
                summary.change_dispatch_dur_ms_max = std::cmp::max(summary.change_dispatch_dur_ms_max, record.change_dispatch_dur_ms);
                summary.query_host_dur_ms_max = std::cmp::max(summary.query_host_dur_ms_max, record.query_host_dur_ms);
                summary.query_solver_dur_ms_max = std::cmp::max(summary.query_solver_dur_ms_max, record.query_solver_dur_ms);
                summary.result_dispatch_dur_ms_max = std::cmp::max(summary.result_dispatch_dur_ms_max, record.result_dispatch_dur_ms);
                summary.total_dur_ms_max = std::cmp::max(summary.total_dur_ms_max, record.total_dur_ms);

                summary.source_change_queue_dur_ms_min = std::cmp::min(summary.source_change_queue_dur_ms_min, record.source_change_queue_dur_ms);
                summary.source_change_router_dur_ms_min = std::cmp::min(summary.source_change_router_dur_ms_min, record.source_change_router_dur_ms);
                summary.source_dispatch_queue_dur_ms_min = std::cmp::min(summary.source_dispatch_queue_dur_ms_min, record.source_dispatch_queue_dur_ms);
                summary.source_change_dispatcher_dur_ms_min = std::cmp::min(summary.source_change_dispatcher_dur_ms_min, record.source_change_dispatcher_dur_ms);
                summary.change_dispatch_dur_ms_min = std::cmp::min(summary.change_dispatch_dur_ms_min, record.change_dispatch_dur_ms);
                summary.query_host_dur_ms_min = std::cmp::min(summary.query_host_dur_ms_min, record.query_host_dur_ms);
                summary.query_solver_dur_ms_min = std::cmp::min(summary.query_solver_dur_ms_min, record.query_solver_dur_ms);
                summary.result_dispatch_dur_ms_min = std::cmp::min(summary.result_dispatch_dur_ms_min, record.result_dispatch_dur_ms);
                summary.total_dur_ms_min = std::cmp::min(summary.total_dur_ms_min, record.total_dur_ms);
            };

            summary.source_change_queue_dur_ms_avg /= summary.change_record_count as f64;
            summary.source_change_router_dur_ms_avg /= summary.change_record_count as f64;
            summary.source_dispatch_queue_dur_ms_avg /= summary.change_record_count as f64;
            summary.source_change_dispatcher_dur_ms_avg /= summary.change_record_count as f64;
            summary.change_dispatch_dur_ms_avg /= summary.change_record_count as f64;
            summary.query_host_dur_ms_avg /= summary.change_record_count as f64;
            summary.query_solver_dur_ms_avg /= summary.change_record_count as f64;
            summary.result_dispatch_dur_ms_avg /= summary.change_record_count as f64;
            summary.total_dur_ms_avg /= summary.change_record_count as f64;
            
        }

        write(summary_path, serde_json::to_string_pretty(&summary)?).await?;
        Ok(())
    }
    
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        match &record.record_data {
            QueryResultRecord::Change(change) => {
                if change.base.metadata.is_some() {
                    self.change_records.push(ChangeRecordProfile::new(&record, &change));
                } else {
                    self.bootstrap_records.push(BootstrapRecordProfile::new(&record, &change));
                }
            },
            QueryResultRecord::Control(_) => {
                self.control_record_count += 1;
            }
        }

        Ok(())
    }
}