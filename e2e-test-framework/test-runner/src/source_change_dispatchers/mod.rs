use crate::test_script::SourceChangeEvent;

pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod file_dispatcher;
pub mod null_dispatcher;

#[derive(Debug, thiserror::Error)]
pub enum SourceChangeDispatcherError {
    NotImplemented(String),
    Io(#[from] std::io::Error),
    Serde(#[from]serde_json::Error),
}

impl std::fmt::Display for SourceChangeDispatcherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotImplemented(e) => write!(f, "Not implemented: {}:", e),
            Self::Io(e) => write!(f, "IO error: {}:", e),
            Self::Serde(e) => write!(f, "Serde error: {}:", e),
        }
    }
}

pub trait SourceChangeEventDispatcher : Send {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()>;
}

impl SourceChangeEventDispatcher for Box<dyn SourceChangeEventDispatcher> {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()> {
        (**self).dispatch_source_change_event(event)
    }
}