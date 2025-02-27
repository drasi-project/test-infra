use std::time::Duration;

use async_trait::async_trait;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, runtime, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::reader::DefaultTemporalitySelector;
use opentelemetry_sdk::metrics::{Aggregation, InstrumentKind, MeterProvider as SdkMeterProvider};
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};

use test_data_store::test_run_storage::TestRunQueryId;

use crate::queries::{result_stream_handlers::ResultStreamRecord, result_stream_record::{ChangeEvent, QueryResultRecord}};

use super::{ResultStreamLogger, ResultStreamLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelMetricResultStreamLoggerConfig {
    pub otel_endpoint: Option<String>,
}

#[derive(Debug)]
pub struct OtelMetricResultStreamLoggerSettings {
    pub test_run_query_id: TestRunQueryId,
    pub otel_endpoint: String,
}

impl OtelMetricResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &OtelMetricResultStreamLoggerConfig) -> anyhow::Result<Self> {
        return Ok(Self {
            test_run_query_id,
            otel_endpoint: config.otel_endpoint.clone().unwrap_or("http://otel-collector:4317".to_string()),
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

#[allow(dead_code)]
pub struct OtelMetricResultStreamLogger {
    meter_provider: SdkMeterProvider,
    metrics: ProfilerMetrics,
    metrics_attributes: Vec<KeyValue>,
    settings: OtelMetricResultStreamLoggerSettings,
}

impl OtelMetricResultStreamLogger {
    pub async fn new(test_run_query_id: TestRunQueryId, cfg: &OtelMetricResultStreamLoggerConfig) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating OtelMetricResultStreamLogger for {}, from {:?}, ", test_run_query_id, cfg);

        let settings = OtelMetricResultStreamLoggerSettings::new(test_run_query_id, &cfg)?;
        log::trace!("Creating OtelMetricResultStreamLogger with settings {:?}, ", settings);

        // Initialize meter provider using pipeline
        let meter_provider = opentelemetry_otlp::new_pipeline()
            .metrics(runtime::Tokio)
            .with_exporter(opentelemetry_otlp::new_exporter().tonic().with_endpoint(&settings.otel_endpoint) )
            .with_resource(Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                "query-result-profiler",
            )]))
            .with_period(Duration::from_millis(100))
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
            meter_provider,
            metrics,
            metrics_attributes,
            settings,
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for OtelMetricResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {
        self.meter_provider.shutdown()?;

        Ok(ResultStreamLoggerResult {
            has_output: false,
            logger_name: "OtelMetric".to_string(),
            output_folder_path: None,
        })
    }
    
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {

        match &record.record_data {
            QueryResultRecord::Change(change) => {
                if change.base.metadata.is_some() {
                    let profile = ChangeRecordProfile::new(&record, &change);

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

                    self.metrics.bootstrap_rec_count.add(1, &self.metrics_attributes);  
                    self.metrics.bootstrap_rec_time_total.record(profile.time_total as f64, &self.metrics_attributes);               
                }
            },
            QueryResultRecord::Control(_) => {
                self.metrics.control_rec_count.add(1, &self.metrics_attributes);                 
            }
        }

        Ok(())
    }
}