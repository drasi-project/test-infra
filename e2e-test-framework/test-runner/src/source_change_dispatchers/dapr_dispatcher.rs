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

        let publisher = DaprHttpPublisher::new(
            app_config.player_settings.dapr_pubsub_host.clone(),
            app_config.player_settings.dapr_pubsub_port,
            app_config.player_settings.dapr_pubsub_name.clone(),
            format!("{}-change", app_config.player_settings.source_id.clone())
        );

        Ok(Box::new(DaprSourceChangeEventDispatcher {
            publisher,
        }))
    }
}  

#[async_trait]
impl SourceChangeEventDispatcher for DaprSourceChangeEventDispatcher {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

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