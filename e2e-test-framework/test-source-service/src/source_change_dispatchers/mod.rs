use crate::test_script::SourceChangeEvent;

pub mod console_dispatcher;
pub mod dapr_dispatcher;
pub mod file_dispatcher;
pub mod null_dispatcher;

#[derive(Debug)]
pub enum SourceChangeDispatcherResult {
    Error(String),
    IoError(std::io::Error),
    SerdeError(serde_json::Error),
}

impl From<String> for SourceChangeDispatcherResult {
    fn from(e: String) -> Self {
        SourceChangeDispatcherResult::Error(e)
    }
}

pub trait SourceChangeEventDispatcher : Send {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> Result<(), SourceChangeDispatcherResult>;
}

impl SourceChangeEventDispatcher for Box<dyn SourceChangeEventDispatcher> {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> Result<(), SourceChangeDispatcherResult> {
        (**self).dispatch_source_change_event(event)
    }
}