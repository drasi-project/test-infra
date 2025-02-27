use async_trait::async_trait;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::BatchConfig, Resource};
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, Registry};

use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::reactions::reaction_handlers::ReactionOutputRecord;

use super::ReactionLogger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelTraceTestRunReactionLoggerConfig {
    pub otel_endpoint: Option<String>,
}

#[derive(Debug)]
pub struct OtelTraceReactionLoggerSettings {
    pub otel_endpoint: String,
    pub test_run_reaction_id: TestRunReactionId,
}

impl OtelTraceReactionLoggerSettings {
    pub fn new(def: &OtelTraceTestRunReactionLoggerConfig, test_run_reaction_id: TestRunReactionId) -> anyhow::Result<Self> {
        return Ok(Self {
            otel_endpoint: def.otel_endpoint.clone().unwrap_or("http://otel-collector:4317".to_string()),
            test_run_reaction_id,
        });
    }
}

#[allow(unused)]
pub struct OtelTraceReactionLogger {
    settings: OtelTraceReactionLoggerSettings,
    // trace_propogator: TraceContextPropagator,
}

impl OtelTraceReactionLogger {
    pub fn new(def: &OtelTraceTestRunReactionLoggerConfig, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
        log::debug!("Creating OtelTraceReactionLogger from {:?}, ", def);

        let settings = OtelTraceReactionLoggerSettings::new(&def, output_storage.id.clone())?;
        log::trace!("Creating OtelTraceReactionLogger with settings {:?}, ", settings);


        let batch_config = BatchConfig::default()
            .with_max_queue_size(8192) // Increase queue size
            .with_max_export_batch_size(100) // Match with collector
            .with_scheduled_delay(std::time::Duration::from_secs(2));

        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_batch_config(batch_config)
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(settings.otel_endpoint.clone()),
            )
            .with_trace_config(
                opentelemetry_sdk::trace::config().with_resource(Resource::new(vec![KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    format!("drasi-reaction-observer-{}", settings.test_run_reaction_id),
                )])),
            )
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;

        let telemetry = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_exception_fields(true)
            .with_location(true);
        let subscriber = Registry::default().with(telemetry);
        tracing::subscriber::set_global_default(subscriber)
            .expect("setting tracing default failed");

        Ok(Box::new(Self {
            settings,
            // trace_propagator: TraceContextPropagator::new(), 
        }))        
    
    }
}  

#[async_trait]
impl ReactionLogger for OtelTraceReactionLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        // opentelemetry::global::shutdown_tracer_provider();
        Ok(())
    }

    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) -> anyhow::Result<()> {
        create_span(&self.settings, record);
        Ok(())
    }
}

fn create_span(setings: &OtelTraceReactionLoggerSettings, record: &ReactionOutputRecord) {

    // Extract the context using the API's global propagator
    let parent_context = opentelemetry_api::global::get_text_map_propagator(|propagator| propagator.extract(record));

    let span = tracing::span!(tracing::Level::INFO, "query_result");
    span.set_parent(parent_context);
    span.set_attribute("test_id", setings.test_run_reaction_id.test_run_id.test_id.to_string());
    span.set_attribute("test_run_id", setings.test_run_reaction_id.test_run_id.to_string());
    span.set_attribute("test_run_reaction_id", setings.test_run_reaction_id.to_string());
    let _ = span.enter();
}