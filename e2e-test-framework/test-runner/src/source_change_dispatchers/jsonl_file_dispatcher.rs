use async_trait::async_trait;
use test_data_store::test_repo_storage::models::JsonlFileSourceChangeDispatcherDefinition;
use test_data_store::test_run_storage::TestRunSourceStorage;

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use test_data_store::scripts::SourceChangeEvent;

use super::{SourceChangeDispatcher, SourceChangeDispatcherError};

#[derive(Debug)]
pub struct JsonlFileSourceChangeDispatcherSettings {
    pub folder_path: PathBuf,
}

impl JsonlFileSourceChangeDispatcherSettings {
    pub fn new(_config: &JsonlFileSourceChangeDispatcherDefinition, folder_path: PathBuf) -> anyhow::Result<Self> {
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
    pub fn new(def:&JsonlFileSourceChangeDispatcherDefinition, output_storage: &TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeDispatcher + Send + Sync>> {
        log::debug!("Creating JsonlFileSourceChangeDispatcher from {:?}, ", def);

        let folder_path = output_storage.path.join("jsonl_file_dispatcher");
        let settings = JsonlFileSourceChangeDispatcherSettings::new(&def, folder_path)?;
        log::trace!("Creating JsonlFileSourceChangeDispatcher with settings {:?}, ", settings);

        // Make sure the local change_data_folder exists, if not, create it.
        // If the folder cannot be created, return an error.
        if !std::path::Path::new(&settings.folder_path).exists() {
            match std::fs::create_dir_all(&settings.folder_path) {
                Ok(_) => {},
                Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
            };
        }        

        let file_path =  &settings.folder_path.clone().join("source_change_events.jsonl");

        let writer = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path) {
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

        log::trace!("Dispatch source change events");

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