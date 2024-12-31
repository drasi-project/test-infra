use async_trait::async_trait;
use chrono::Utc;
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
    pub single_file: bool,
}

impl JsonlFileSourceChangeDispatcherSettings {
    pub fn new(config: &JsonlFileSourceChangeDispatcherDefinition, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
            single_file: config.single_file.unwrap_or(false),
        });
    }
}

pub struct JsonlFileSourceChangeDispatcher {
    settings: JsonlFileSourceChangeDispatcherSettings,
    writer: Option<BufWriter<File>>,
}

impl JsonlFileSourceChangeDispatcher {
    pub fn new(def:&JsonlFileSourceChangeDispatcherDefinition, output_storage: &TestRunSourceStorage) -> anyhow::Result<Box<dyn SourceChangeDispatcher + Send + Sync>> {
        log::debug!("Creating JsonlFileSourceChangeDispatcher from {:?}, ", def);

        let folder_path = output_storage.source_change_path.clone();
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

        let writer = match settings.single_file {
            true => {
                let file_path =  &settings.folder_path.clone().join("source_change_events.jsonl");
                match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(file_path) {
                        Ok(f) => Some(BufWriter::new(f)),
                        Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
                    }
            },
            _ => None,
        };

        Ok(Box::new( Self { 
            settings,
            writer,
        }))
    }
}

#[async_trait]
impl SourceChangeDispatcher for JsonlFileSourceChangeDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        match &mut self.writer {
            Some(w) => {
                w.flush()?;
            },
            None => {},
        }
        Ok(())
    }
    
    async fn dispatch_source_change_events(&mut self, events: Vec<&SourceChangeEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch source change events");

        for event in events {
            let json = serde_json::to_string(&event)?;

            match &mut self.writer {
                Some(w) => {
                    writeln!(w, "{}", json)?; // Write the JSON string with a newline
                },
                None => {
                    // Create a file name based on the current Utc date and time.
                    let file_name = Utc::now().format("source_change_event_%Y-%m-%d_%H-%M-%S%.f.json");
                    let file_path = &self.settings.folder_path.clone().join(file_name.to_string());
                    
                    let mut w = match OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(file_path) {
                            Ok(f) => BufWriter::new(f),
                            Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
                        };
                    match w.write_all(json.as_bytes()) {
                        Ok(_) => { w.flush()?; },
                        Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
                    }
                }
            }
        }
        Ok(())
    }
}