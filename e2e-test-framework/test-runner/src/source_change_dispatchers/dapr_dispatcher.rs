use async_trait::async_trait;

use drasi_comms_abstractions::comms::{Headers, Publisher};
use drasi_comms_dapr::comms::DaprHttpPublisher;

use test_data_store::test_repo_storage::{models::DaprSourceChangeDispatcherDefinition, scripts::SourceChangeEvent};

use super::SourceChangeDispatcher;

#[derive(Debug)]
pub struct DaprSourceChangeDispatcherSettings {
    pub host: String,
    pub port: u16,
    pub pubsub_name: String,
    pub pubsub_topic: String,
}

impl DaprSourceChangeDispatcherSettings {
    pub fn new(config: &DaprSourceChangeDispatcherDefinition, source_id: String) -> anyhow::Result<Self> {
        Ok(Self {
            host: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            port: config.port.unwrap_or(3500),
            pubsub_name: config.pubsub_name.clone().unwrap_or("drasi-pubsub".to_string()),
            pubsub_topic: config.pubsub_topic.clone().unwrap_or(format!("{}-change", source_id)),
        })
    }
}

pub struct DaprSourceChangeDispatcher {
    _settings: DaprSourceChangeDispatcherSettings,
    publisher: DaprHttpPublisher,
}

impl DaprSourceChangeDispatcher {
    pub fn new(settings: DaprSourceChangeDispatcherSettings) -> anyhow::Result<Box<dyn SourceChangeDispatcher + Send + Sync>> {

        log::debug!("Initializing from {:?}", settings);

        let publisher = DaprHttpPublisher::new(
            settings.host.clone(),
            settings.port,
            settings.pubsub_name.clone(),
            settings.pubsub_topic.clone(),
        );

        Ok(Box::new(DaprSourceChangeDispatcher {
            _settings: settings,
            publisher,
        }))
    }
}  

#[async_trait]
impl SourceChangeDispatcher for DaprSourceChangeDispatcher {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch source change events");

        let data = serde_json::to_value(events)?;

        let headers: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        // let traceparent = "000".to_string();
        // headers.insert("traceparent".to_string(), traceparent.clone());
        let _headers = Headers::new(headers);

        match self.publisher.publish(data, _headers).await {
            Ok(_) => Ok(()),
            Err(e) => {
                let msg = format!("Error dispatching source change event: {:?}", e);
                log::error!("{}", msg);
                anyhow::bail!(msg);
            }
        }
    }
}