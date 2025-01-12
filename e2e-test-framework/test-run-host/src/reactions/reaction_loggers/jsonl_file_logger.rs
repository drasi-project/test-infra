use std::path::PathBuf;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::test_run_storage::TestRunReactionStorage;

use crate::reactions::reaction_collector::ReactionOutputRecord;

use super::{ReactionLogger, ReactionLoggerError};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileTestRunReactionLoggerConfig {
    pub max_lines_per_file: Option<u64>,
}

#[derive(Debug)]
pub struct JsonlFileReactionLoggerSettings {
    pub folder_path: PathBuf,
    pub max_events_per_file: u64,
}

impl JsonlFileReactionLoggerSettings {
    pub fn new(config: &JsonlFileTestRunReactionLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
            max_events_per_file: config.max_lines_per_file.unwrap_or(10000),
        });
    }
}

pub struct JsonlFileReactionLogger {
    #[allow(dead_code)]
    settings: JsonlFileReactionLoggerSettings,
    writer: ReactionDataEventLogWriter,
}

impl JsonlFileReactionLogger {
    pub async fn new(def:&JsonlFileTestRunReactionLoggerConfig, output_storage: &TestRunReactionStorage) -> anyhow::Result<Box<dyn ReactionLogger + Send + Sync>> {
        log::debug!("Creating JsonlFileReactionLogger from {:?}, ", def);

        let folder_path = output_storage.result_change_path.clone();
        let settings = JsonlFileReactionLoggerSettings::new(&def, folder_path)?;
        log::trace!("Creating JsonlFileReactionLogger with settings {:?}, ", settings);

        // Make sure the local change_data_folder exists, if not, create it.
        // If the folder cannot be created, return an error.
        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {},
                Err(e) => return Err(ReactionLoggerError::Io(e).into()),
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
impl ReactionLogger for JsonlFileReactionLogger {
    async fn close(&mut self) -> anyhow::Result<()> {
        self.writer.close().await
    }
    
    async fn log_reaction_record(&mut self, record: &ReactionOutputRecord) -> anyhow::Result<()> {
        self.writer.write_reaction_data(record).await?;
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

    pub async fn write_reaction_data(&mut self, event: &ReactionOutputRecord) -> anyhow::Result<()> {
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