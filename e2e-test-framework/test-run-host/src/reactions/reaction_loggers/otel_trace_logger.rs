use async_trait::async_trait;
use opentelemetry::{global::{self, BoxedTracer}, trace::{Span, SpanId, TraceFlags, TraceId, Tracer}, Context, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use serde::{Deserialize, Serialize};
use tracing_subscriber::{layer::SubscriberExt, Registry};

use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::reactions::reaction_collector::ReactionOutputRecord;

use super::ReactionLogger;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtelTraceTestRunReactionLoggerConfig {
    pub otel_endpoint: Option<String>,
    pub service_name: Option<String>,
}

#[derive(Debug)]
pub struct OtelTraceReactionLoggerSettings {
    pub otel_endpoint: String,
    pub service_name: String,
}

impl OtelTraceReactionLoggerSettings {
    pub fn new(def: &OtelTraceTestRunReactionLoggerConfig, id: TestRunReactionId) -> anyhow::Result<Self> {
        return Ok(Self {
            otel_endpoint: def.otel_endpoint.clone().unwrap_or("http://otel-collector:4317".to_string()),
            service_name: def.service_name.clone().unwrap_or(id.to_string()),
        });
    }
}

#[allow(unused)]
pub struct OtelTraceReactionLogger {
    settings: OtelTraceReactionLoggerSettings,
    tracer: BoxedTracer
}

impl OtelTraceReactionLogger {
    pub fn new(def: &OtelTraceTestRunReactionLoggerConfig, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
        log::debug!("Creating OtelTraceReactionLogger from {:?}, ", def);

        let settings = OtelTraceReactionLoggerSettings::new(&def, output_storage.id.clone())?;
        log::trace!("Creating OtelTraceReactionLogger with settings {:?}, ", settings);

        let service_name = settings.service_name.clone();

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
                    service_name.clone(),
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
                    tracer: global::tracer(service_name.clone())
                }))
            }
            Err(err) => Err(err.into()),
        }
    }
}  

#[async_trait]
impl ReactionLogger for OtelTraceReactionLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        opentelemetry::global::shutdown_tracer_provider();
        Ok(())
    }

    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) -> anyhow::Result<()> {

        create_and_end_span(&self.tracer, &record.traceparent);

        Ok(())
    }
}

fn context_from_traceparent(traceparent: &str) -> Option<Context> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    if parts.len() == 4 {
        let trace_id = parts[1];
        let span_id = parts[2];
        let trace_flags = parts[3];

        // Validate and build the trace context
        if let (Ok(trace_id), Ok(span_id)) = (
            TraceId::from_hex(trace_id),
            SpanId::from_hex(span_id),
        ) {
            let span_context = opentelemetry::trace::SpanContext::new(
                trace_id,
                span_id,
                TraceFlags::new(trace_flags.parse::<u8>().unwrap_or_default()),
                true, // Remote context
                Default::default(),
            );
            return Some(Context::current_with_value(span_context));
        }
    }
    None
}

fn create_and_end_span(tracer: &impl Tracer, traceparent: &str) {
    if let Some(parent_context) = context_from_traceparent(traceparent) {
        // Start a new span with the parent context
        let mut span = tracer.start_with_context("terminate_trace", &parent_context);

        // Add any attributes or logs to the span
        // span.set_attribute("termination".into(), "true".into());
        // span.add_event("Trace termination event".to_string(), vec![]);

        // End the span explicitly
        span.end();
    } else {
        eprintln!("Invalid traceparent: {}", traceparent);
    }
}