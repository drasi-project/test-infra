use async_trait::async_trait;
use opentelemetry::{propagation::TextMapPropagator, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{propagation::TraceContextPropagator, Resource};
use serde::{Deserialize, Serialize};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, Registry};

use crate::queries::result_stream_handlers::ResultStreamRecord;

use super::ResultStreamLogger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelTraceResultStreamLoggerConfig {
    pub otel_endpoint: Option<String>,
}

#[derive(Debug)]
pub struct OtelTraceResultStreamLoggerSettings {
    pub otel_endpoint: String,
    pub test_run_query_id: TestRunQueryId,
}

impl OtelTraceResultStreamLoggerSettings {
    pub fn new(def: &OtelTraceResultStreamLoggerConfig, test_run_query_id: TestRunQueryId) -> anyhow::Result<Self> {
        return Ok(Self {
            otel_endpoint: def.otel_endpoint.clone().unwrap_or("http://otel-collector:4317".to_string()),
            test_run_query_id,
        });
    }
}

#[allow(unused)]
pub struct OtelTraceResultStreamLogger {
    settings: OtelTraceResultStreamLoggerSettings,
    trace_propogator: TraceContextPropagator,
}

impl OtelTraceResultStreamLogger {
    pub fn new(def: &OtelTraceResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating OtelTraceResultStreamLogger from {:?}, ", def);

        let settings = OtelTraceResultStreamLoggerSettings::new(&def, output_storage.id.clone())?;
        log::trace!("Creating OtelTraceResultStreamLogger with settings {:?}, ", settings);

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(settings.otel_endpoint.clone()),
            )
            .with_trace_config(
                opentelemetry::sdk::trace::config().with_resource(Resource::new(vec![KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    format!("drasi-query-result-observer-{}", settings.test_run_query_id),
                )])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio);

        match tracer {
            Ok(tracer) => {
                let telemetry = tracing_opentelemetry::layer()
                    .with_tracer(tracer)
                    .with_exception_fields(true)
                    .with_location(true);
                let subscriber = Registry::default().with(telemetry);
                tracing::subscriber::set_global_default(subscriber)
                    .expect("setting tracing default failed");

                Ok(Box::new(Self { 
                    settings,
                    trace_propogator: TraceContextPropagator::new(),
                }))
            }
            Err(err) => Err(err.into()),
        }
    }
}  

#[async_trait]
impl ResultStreamLogger for OtelTraceResultStreamLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        // opentelemetry::global::shutdown_tracer_provider();
        Ok(())
    }

    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {
        create_span(&self.settings, &self.trace_propogator, record);
        Ok(())
    }
}

fn create_span(setings: &OtelTraceResultStreamLoggerSettings, trace_propogator: &TraceContextPropagator, record: &ResultStreamRecord) {
    let parent_context = trace_propogator.extract(record);
    let span = tracing::span!(tracing::Level::INFO, "query_result");
    span.set_parent(parent_context);
    span.set_attribute("test_id", setings.test_run_query_id.test_run_id.test_id.to_string());
    span.set_attribute("test_run_id", setings.test_run_query_id.test_run_id.to_string());
    span.set_attribute("test_run_query_id", setings.test_run_query_id.to_string());
    let _ = span.enter();
}