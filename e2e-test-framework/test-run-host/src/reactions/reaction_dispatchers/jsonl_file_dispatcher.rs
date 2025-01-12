use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::{test_repo_storage::models::JsonlFileTestReactionDispatcherDefinition, test_run_storage::{ReactionDataEvent, TestRunReactionStorage}};

use super::{ReactionDataDispatcher, ReactionDataDispatcherError};

#[derive(Debug)]
pub struct JsonlFileReactionDataDispatcherSettings {
    pub folder_path: PathBuf,
    pub max_events_per_file: u64,
}

impl JsonlFileReactionDataDispatcherSettings {
    pub fn new(config: &JsonlFileTestReactionDispatcherDefinition, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
            max_events_per_file: config.max_lines_per_file.unwrap_or(10000),
        });
    }
}

pub struct JsonlFileReactionDataDispatcher {
    #[allow(dead_code)]
    settings: JsonlFileReactionDataDispatcherSettings,
    writer: ReactionDataEventLogWriter,
}

impl JsonlFileReactionDataDispatcher {
    pub async fn new(def:&JsonlFileTestReactionDispatcherDefinition, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionDataDispatcher + Send + Sync>> {
        log::debug!("Creating JsonlFileReactionDataDispatcher from {:?}, ", def);

        let folder_path = output_storage.result_change_path.clone();
        let settings = JsonlFileReactionDataDispatcherSettings::new(&def, folder_path)?;
        log::trace!("Creating JsonlFileReactionDataDispatcher with settings {:?}, ", settings);

        // Make sure the local change_data_folder exists, if not, create it.
        // If the folder cannot be created, return an error.
        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {},
                Err(e) => return Err(ReactionDataDispatcherError::Io(e).into()),
            };
        }        

        let script_name = Utc::now()
            .format("%Y-%m-%d_%H-%M-%S")
            .to_string();

        let writer = ReactionDataEventLogWriter::new(
            settings.folder_path.clone(),
            script_name,
            settings.max_events_per_file
        ).await?;

        Ok(Box::new( Self { 
            settings,
            writer,
        }))
    }
}

#[async_trait]
impl ReactionDataDispatcher for JsonlFileReactionDataDispatcher {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.writer.close().await
    }
    
    async fn dispatch_reaction_data(&mut self, events: Vec<&ReactionDataEvent>) -> anyhow::Result<()> {

        log::trace!("Dispatch reaction data");

        for event in events {
            self.writer.write_reaction_data(event).await?;
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionDataEventLogWriterError {
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

pub struct ReactionDataEventLogWriter {
    folder_path: PathBuf,
    log_file_name: String,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_event_count: u64,
}

impl ReactionDataEventLogWriter {
    pub async fn new(folder_path: PathBuf, log_file_name: String, max_size: u64) -> anyhow::Result<Self> {

        let mut writer = ReactionDataEventLogWriter {
            folder_path,
            log_file_name,
            next_file_index: 0,
            current_writer: None,
            max_size,
            current_file_event_count: 0,
        };

        writer.open_next_file().await?;
        Ok(writer)
    }

    pub async fn write_reaction_data(&mut self, event: &ReactionDataEvent) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!("{}\n", to_string(event).map_err(|e| ReactionDataEventLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| ReactionDataEventLogWriterError::FileWriteError(e.to_string()))?;

            self.current_file_event_count += 1;

            if self.current_file_event_count >= self.max_size {
                self.open_next_file().await?;
            }
        }

        Ok(())
    }

    async fn open_next_file(&mut self) -> anyhow::Result<()> {
        // If there is a current writer, flush it and close it.
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| ReactionDataEventLogWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the script file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!("{}/{}_{:05}.jsonl", self.folder_path.to_string_lossy(), self.log_file_name, self.next_file_index);

        // Create the file and open it for writing
        let file = File::create(&file_path).await.map_err(|_| ReactionDataEventLogWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and event count
        self.next_file_index += 1;
        self.current_file_event_count = 0;

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| ReactionDataEventLogWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}