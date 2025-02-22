use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, runtime, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector;
use opentelemetry_sdk::metrics::{Aggregation, InstrumentKind, MeterProvider as SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File, write}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfilerResultStreamLoggerConfig {
    pub max_lines_per_file: Option<u64>,
    pub otel_endpoint: Option<String>,
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
    pub otel_endpoint: String,
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
            otel_endpoint: config.otel_endpoint.clone().unwrap_or("http://otel-collector:4317".to_string()),
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

struct ProfilerMetrics {
    pub bootstrap_rec_count: Counter<u64>,
    pub bootstrap_rec_time_total: Histogram<f64>,
    pub change_rec_count: Counter<u64>,
    pub change_rec_time_in_src_change_q: Histogram<f64>,
    pub change_rec_time_in_src_change_rtr: Histogram<f64>,
    pub change_rec_time_in_src_disp_q: Histogram<f64>,
    pub change_rec_time_in_src_change_disp: Histogram<f64>,
    pub change_rec_time_in_change_pub: Histogram<f64>,
    pub change_rec_time_in_query_host: Histogram<f64>,
    pub change_rec_time_in_query_solver: Histogram<f64>,
    pub change_rec_time_in_result_disp_q: Histogram<f64>,
    pub change_rec_time_total: Histogram<f64>,
    pub control_rec_count: Counter<u64>,
}

impl ProfilerMetrics {
    pub fn new() -> Self {

        // Get a Bootstrap Record meter
        let bootstrap_rec_meter = global::meter("query-result-profiler-bootstrap-meter");

        let bootstrap_rec_count = bootstrap_rec_meter
            .u64_counter("drasi.query-result-profiler.bootstrap_rec_count")
            .with_description("Count of Query Result Bootstrap Records")
            .init();

        let bootstrap_rec_time_total = bootstrap_rec_meter
            .f64_histogram("drasi.query-result-profiler.bootstrap_rec_time_total")
            .with_description("Total time taken to process a Bootstrap Record")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();

        // Get a Change Record meter
        let change_rec_meter = global::meter("query-result-profiler-change-meter");

        let change_rec_count = change_rec_meter
            .u64_counter("drasi.query-result-profiler.change_rec_count")
            .with_description("Count of Query Result Change Records")
            .init();

        let change_rec_time_in_src_change_q = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_src_change_q")
            .with_description("Total time Query Result Change spent in Source Change Queue")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();

        let change_rec_time_in_src_change_rtr = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_src_change_rtr")
            .with_description("Total time Query Result Change spent in Source Change Router")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();

        let change_rec_time_in_src_disp_q = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_src_disp_q")
            .with_description("Total time Query Result Change spent in Source Dispatch Queue")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();      

        let change_rec_time_in_src_change_disp = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_src_change_disp")
            .with_description("Total time Query Result Change spent in Source Change Dispatcher")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();    

        let change_rec_time_in_change_pub = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_change_pub")
            .with_description("Total time Query Result Change spent being dispatched")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();   

        let change_rec_time_in_query_host = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_query_host")
            .with_description("Total time Query Result Change spent in Query Host")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();   

        let change_rec_time_in_query_solver = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_query_solver")
            .with_description("Total time Query Result Change spent in Query Solver")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();   

        let change_rec_time_in_result_disp_q = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_in_result_disp_q")
            .with_description("Total time Query Result Change spent in Query Result Dispatch Queue")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();              

        let change_rec_time_total = change_rec_meter
            .f64_histogram("drasi.query-result-profiler.change_rec_time_total")
            .with_description("Total time Query Result Change took to process")
            .with_unit(opentelemetry::metrics::Unit::new("ms"))
            .init();  

        // Get a Control Record meter
        let control_rec_meter = global::meter("query-result-profiler-control-meter");

        let control_rec_count = control_rec_meter
            .u64_counter("drasi.query-result-profiler.control_rec_count")
            .with_description("Count of Query Result Control Records")
            .init();

        Self {
            bootstrap_rec_count,
            bootstrap_rec_time_total,
            change_rec_count,
            change_rec_time_in_src_change_q,
            change_rec_time_in_src_change_rtr,
            change_rec_time_in_src_disp_q,
            change_rec_time_in_src_change_disp,
            change_rec_time_in_change_pub,
            change_rec_time_in_query_host,
            change_rec_time_in_query_solver,
            change_rec_time_in_result_disp_q,
            change_rec_time_total,
            control_rec_count,
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
    meter_provider: SdkMeterProvider,
    metrics: ProfilerMetrics,
    metrics_attributes: Vec<KeyValue>,
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

        // Initialize meter provider using pipeline
        let meter_provider = opentelemetry_otlp::new_pipeline()
            .metrics(runtime::Tokio)
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(&settings.otel_endpoint) )
            .with_resource(Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                "query-result-profiler",
            )]))
            .with_period(Duration::from_secs(5))
            .with_temporality_selector(DefaultTemporalitySelector::new())
            .with_aggregation_selector(|kind: InstrumentKind| {
                match kind {
                    InstrumentKind::Counter
                    | InstrumentKind::UpDownCounter
                    | InstrumentKind::ObservableCounter
                    | InstrumentKind::ObservableUpDownCounter => Aggregation::Sum,
                    InstrumentKind::ObservableGauge => Aggregation::LastValue,
                    InstrumentKind::Histogram => Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![
                            0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0,
                            2500.0, 5000.0, 7500.0, 10000.0, 15000.0, 20000.0, 25000.0, 30000.0, 
                        ],
                        record_min_max: true,
                    },
                }

            })
            .build()?;
        
        // Set the global meter provider
        global::set_meter_provider(meter_provider.clone());

        // Create the metrics:
        let metrics = ProfilerMetrics::new();

        let metrics_attributes = vec![
            KeyValue::new("test_id", settings.test_run_query_id.test_run_id.test_id.to_string()),
            KeyValue::new("test_run_id", settings.test_run_query_id.test_run_id.to_string()),
            KeyValue::new("test_run_query_id", settings.test_run_query_id.to_string()),
        ];

        Ok(Box::new( Self { 
            bootstrap_log_writer,
            change_log_writer,
            meter_provider,
            metrics,
            metrics_attributes,
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

        self.meter_provider.shutdown()?;
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


                    self.metrics.change_rec_count.add(1, &self.metrics_attributes);             
                    self.metrics.change_rec_time_in_src_change_q.record(profile.time_in_src_change_q as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_src_change_rtr.record(profile.time_in_src_change_rtr as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_src_disp_q.record(profile.time_in_src_disp_q as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_src_change_disp.record(profile.time_in_src_change_disp as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_change_pub.record(profile.time_in_src_change_pub as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_query_host.record(profile.time_in_query_host as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_query_solver.record(profile.time_in_query_solver as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_in_result_disp_q.record(profile.time_in_result_disp_q as f64, &self.metrics_attributes);
                    self.metrics.change_rec_time_total.record(profile.time_total as f64, &self.metrics_attributes);    

                } else {
                    let profile = BootstrapRecordProfile::new(&record, &change);

                    if let Some(writer) = &mut self.bootstrap_log_writer {
                        writer.write_bootstrap_profile(&profile).await?;
                    }

                    self.summary.bootstrap_rec_count += 1;
                    self.summary.bootstrap_rec_time_total_avg += profile.time_total as f64;
                    self.summary.bootstrap_rec_time_total_max = std::cmp::max(self.summary.bootstrap_rec_time_total_max, profile.time_total);
                    self.summary.bootstrap_rec_time_total_min = std::cmp::min(self.summary.bootstrap_rec_time_total_min, profile.time_total);   

                    self.metrics.bootstrap_rec_count.add(1, &self.metrics_attributes);  
                    self.metrics.bootstrap_rec_time_total.record(profile.time_total as f64, &self.metrics_attributes);               
 
                }
            },
            QueryResultRecord::Control(_) => {
                self.summary.control_rec_count += 1;

                self.metrics.control_rec_count.add(1, &self.metrics_attributes);                 
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
