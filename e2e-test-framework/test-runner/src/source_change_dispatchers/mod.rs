use async_trait::async_trait;

use crate::script_source::SourceChangeEvent;

pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod jsonl_file_dispatcher;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeDispatcherError {
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for SourceChangeDispatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

#[async_trait]
pub trait SourceChangeDispatcher : Send + Sync {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()>;
}

#[async_trait]
impl SourceChangeDispatcher for Box<dyn SourceChangeDispatcher> {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {
        (**self).dispatch_source_change_events(events).await
    }
}