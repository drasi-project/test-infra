use async_trait::async_trait;

use drasi_comms_abstractions::comms::{Headers, Publisher};
use drasi_comms_dapr::comms::DaprHttpPublisher;

use crate::test_script::{SourceChangeEvent, change_script_player::ChangeScriptPlayerConfig};
use super::SourceChangeEventDispatcher;

pub struct DaprSourceChangeEventDispatcher {
    publisher: DaprHttpPublisher,
}

impl DaprSourceChangeEventDispatcher {
    pub fn new(app_config: &ChangeScriptPlayerConfig) -> anyhow::Result<Box<dyn SourceChangeEventDispatcher>> {

        let dapr_host = "127.0.0.1".to_string();
        let dapr_port = 3500;
        let pubsub = "pubsub".to_string();
        let topic = format!("{}-change", app_config.player_settings.source_id.clone());

        let publisher = DaprHttpPublisher::new(
            dapr_host,
            dapr_port,
            pubsub,
            topic,
        );

        Ok(Box::new(DaprSourceChangeEventDispatcher {
            publisher,
        }))
    }
}  

#[async_trait]
impl SourceChangeEventDispatcher for DaprSourceChangeEventDispatcher {
    async fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()> {

        let data = serde_json::to_value(event)?;

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