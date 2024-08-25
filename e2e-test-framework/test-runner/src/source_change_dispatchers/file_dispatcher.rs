use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

use crate::test_script::{SourceChangeEvent, change_script_player::ChangeScriptPlayerConfig};
use super::{SourceChangeEventDispatcher, SourceChangeDispatcherError};

pub struct JsonlFileSourceChangeDispatcher {
    // file_path: String,
    writer: BufWriter<File>,
}

impl JsonlFileSourceChangeDispatcher {
    pub fn new(app_config: &ChangeScriptPlayerConfig) -> anyhow::Result<Box<dyn SourceChangeEventDispatcher>> {

        log::info!("Initializing JsonlFileSourceChangeDispatcher...");

        // let test_run_settings = app_config.test_run_settings.as_ref().unwrap();

        // Construct the path to the local file used to store the generated SourceChangeEvents.
        let local_dispatcher_folder = format!("{}/test_runs/{}/{}/logs/sources/{}", 
            app_config.player_settings.data_cache_path, 
            app_config.player_settings.test_id, 
            app_config.player_settings.test_run_id, 
            app_config.player_settings.source_id);
    
        // Make sure the local change_data_folder exists, if not, create it.
        // If the folder cannot be created, return an error.
        if !std::path::Path::new(&local_dispatcher_folder).exists() {
            match std::fs::create_dir_all(&local_dispatcher_folder) {
                Ok(_) => {},
                Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
            };
        }        

        let file_path = format!("{}/source_change_event_dispatcher.jsonl", local_dispatcher_folder);

        let writer = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path) {
                Ok(f) => BufWriter::new(f),
                Err(e) => return Err(SourceChangeDispatcherError::Io(e).into()),
            };

        Ok(Box::new( Self { 
            // file_path, 
            writer,
        }))
    }
}

impl SourceChangeEventDispatcher for JsonlFileSourceChangeDispatcher {
    fn dispatch_source_change_event(&mut self, event: &SourceChangeEvent) -> anyhow::Result<()> {
        let json_event = match serde_json::to_string(event) {
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