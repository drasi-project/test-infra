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

use async_trait::async_trait;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::BatchConfigBuilder, Resource};
use serde::{Deserialize, Serialize};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{layer::SubscriberExt, Registry};

use test_data_store::test_run_storage::TestRunQueryId;

use crate::common::{HandlerPayload, HandlerRecord};

use super::{ResultStreamLogger, ResultStreamLoggerResult};

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
    pub fn new(
        test_run_query_id: TestRunQueryId,
        def: &OtelTraceResultStreamLoggerConfig,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            otel_endpoint: def
                .otel_endpoint
                .clone()
                .unwrap_or("http://otel-collector:4317".to_string()),
            test_run_query_id,
        })
    }
}

#[allow(unused)]
pub struct OtelTraceResultStreamLogger {
    settings: OtelTraceResultStreamLoggerSettings,
}

impl OtelTraceResultStreamLogger {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        test_run_query_id: TestRunQueryId,
        def: &OtelTraceResultStreamLoggerConfig,
    ) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating OtelTraceResultStreamLogger for {test_run_query_id} from {def:?}, ");

        let settings = OtelTraceResultStreamLoggerSettings::new(test_run_query_id, def)?;
        log::trace!("Creating OtelTraceResultStreamLogger with settings {settings:?}, ");

        let batch_config = BatchConfigBuilder::default()
            .with_max_queue_size(16384) // Increase queue size
            .with_max_export_batch_size(512) // Match with collector
            .with_scheduled_delay(std::time::Duration::from_secs(1))
            .build();

        let provider = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_batch_config(batch_config)
            .with_exporter(
                opentelemetry_otlp::new_exporter()
                    .tonic()
                    .with_endpoint(settings.otel_endpoint.clone()),
            )
            .with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(
                Resource::new(vec![KeyValue::new(
                    opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                    format!("drasi-query-result-observer-{}", settings.test_run_query_id),
                )]),
            ))
            .install_batch(opentelemetry_sdk::runtime::Tokio)?;
        let tracer = provider.tracer("opentelemetry-otlp");
        // OTLP 0.17 returns the provider instead of installing it globally.
        opentelemetry::global::set_tracer_provider(provider);

        let telemetry = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_error_fields_to_exceptions(true)
            .with_error_records_to_exceptions(false)
            .with_error_events_to_exceptions(false)
            .with_error_events_to_status(false)
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
impl ResultStreamLogger for OtelTraceResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {
        // opentelemetry::global::shutdown_tracer_provider();
        Ok(ResultStreamLoggerResult {
            has_output: false,
            logger_name: "OtelTrace".to_string(),
            output_folder_path: None,
        })
    }

    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        // Only process ResultStream payloads
        if let HandlerPayload::ResultStream { .. } = &record.payload {
            create_span(&self.settings, record);
        }
        Ok(())
    }
}

fn create_span(settings: &OtelTraceResultStreamLoggerSettings, record: &HandlerRecord) {
    let parent_context = parent_context(record);

    let span = tracing::span!(tracing::Level::INFO, "query_result");
    span.set_parent(parent_context);
    span.set_attribute(
        "test_id",
        settings.test_run_query_id.test_run_id.test_id.to_string(),
    );
    span.set_attribute(
        "test_run_id",
        settings.test_run_query_id.test_run_id.to_string(),
    );
    span.set_attribute("test_run_query_id", settings.test_run_query_id.to_string());
    let _ = span.enter();
}

fn parent_context(record: &HandlerRecord) -> opentelemetry::Context {
    let context =
        opentelemetry::global::get_text_map_propagator(|propagator| propagator.extract(record));
    if context.span().span_context().is_valid() {
        context
    } else {
        // Preserve the record's W3C context when the new API's global
        // propagator has not been configured by the embedding application.
        opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_traceparent_and_tracestate_survive_the_api_upgrade() {
        let record = HandlerRecord {
            id: "record".into(),
            sequence: 1,
            created_time_ns: 0,
            processed_time_ns: 0,
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
            profiling: None,
            payload: HandlerPayload::ReactionOutput {
                reaction_output: serde_json::json!({}),
            },
        };
        let context = parent_context(&record);
        let span = context.span();
        assert_eq!(
            span.span_context().trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(
            span.span_context().span_id().to_string(),
            "00f067aa0ba902b7"
        );
        assert_eq!(
            span.span_context().trace_state().get("vendor"),
            Some("value")
        );
        assert!(span.span_context().is_remote());
    }
}
