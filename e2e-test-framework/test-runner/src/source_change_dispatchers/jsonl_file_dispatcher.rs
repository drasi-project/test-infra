use async_trait::async_trait;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

use crate::config::JsonlFileSourceChangeDispatcherConfig;
use crate::script_source::SourceChangeEvent;
use super::{SourceChangeDispatcher, SourceChangeDispatcherError};

#[derive(Debug)]
pub struct JsonlFileSourceChangeDispatcherSettings {
    pub folder_path: String,
}

impl JsonlFileSourceChangeDispatcherSettings {
    pub fn try_from_config(_config: &JsonlFileSourceChangeDispatcherConfig, folder_path: String) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
        });
    }
}

pub struct JsonlFileSourceChangeDispatcher {
    _settings: JsonlFileSourceChangeDispatcherSettings,
    writer: BufWriter<File>,
}

impl JsonlFileSourceChangeDispatcher {
    pub fn new(settings: JsonlFileSourceChangeDispatcherSettings) -> anyhow::Result<Box<dyn SourceChangeDispatcher>> {

        log::info!("Initializing JsonlFileSourceChangeDispatcher from {:?}", settings);

        // Make sure the local change_data_folder exists, if not, create it.
        // If the folder cannot be created, return an error.
        if !std::path::Path::new(&settings.folder_path).exists() {
            match std::fs::create_dir_all(&settings.folder_path) {
                Ok(_) => {},
                Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
            };
        }        

        let file_path = format!("{}/source_change_dispatcher.jsonl", &settings.folder_path);

        let writer = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path) {
                Ok(f) => BufWriter::new(f),
                Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
            };

        Ok(Box::new( Self { 
            _settings: settings,
            writer,
        }))
    }
}

#[async_trait]
impl SourceChangeDispatcher for JsonlFileSourceChangeDispatcher {
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("JsonlFileSourceChangeDispatcher - dispatch_source_change_events");

        let json_event = match serde_json::to_string(&events) {
            Ok(e) => e,
            Err(e) => return Err(SourceChangeDispatcherError::Serde(e).into()),
        };

        match writeln!(self.writer, "{}", json_event) {
            Ok(_) => {
                match self.writer.flush() {
                    Ok(_) => {},
                    Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
                }
            },
            Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
        }
        Ok(())
    }
}