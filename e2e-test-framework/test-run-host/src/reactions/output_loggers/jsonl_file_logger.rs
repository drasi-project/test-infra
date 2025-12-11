// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{
    fs::{create_dir_all, File},
    io::{AsyncWriteExt, BufWriter},
};

use test_data_store::test_run_storage::{TestRunReactionId, TestRunReactionStorage};

use crate::common::HandlerRecord;

use super::{OutputLogger, OutputLoggerError, OutputLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileOutputLoggerConfig {
    pub max_lines_per_file: Option<u64>,
}

#[derive(Debug)]
pub struct JsonlFileOutputLoggerSettings {
    pub folder_path: PathBuf,
    pub log_name: String,
    pub max_lines_per_file: u64,
    pub test_run_reaction_id: TestRunReactionId,
}

impl JsonlFileOutputLoggerSettings {
    pub fn new(
        test_run_reaction_id: TestRunReactionId,
        config: &JsonlFileOutputLoggerConfig,
        folder_path: PathBuf,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            folder_path,
            log_name: "outputs".to_string(),
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            test_run_reaction_id,
        })
    }
}

pub struct JsonlFileOutputLogger {
    #[allow(dead_code)]
    settings: JsonlFileOutputLoggerSettings,
    writer: ReactionOutputRecordLogWriter,
}

impl JsonlFileOutputLogger {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        test_run_reaction_id: TestRunReactionId,
        def: &JsonlFileOutputLoggerConfig,
        output_storage: &TestRunReactionStorage,
    ) -> anyhow::Result<Box<dyn OutputLogger + Send + Sync>> {
        log::debug!("Creating JsonlFileOutputLogger for {test_run_reaction_id} from {def:?}, ");

        let folder_path = output_storage.reaction_output_path.join("jsonl_file");
        let settings = JsonlFileOutputLoggerSettings::new(test_run_reaction_id, def, folder_path)?;
        log::trace!("Creating JsonlFileOutputLogger with settings {settings:?}, ");

        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {}
                Err(e) => return Err(OutputLoggerError::Io(e).into()),
            };
        }

        let writer = ReactionOutputRecordLogWriter::new(&settings).await?;

        Ok(Box::new(Self { settings, writer }))
    }
}

#[async_trait]
impl OutputLogger for JsonlFileOutputLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<OutputLoggerResult> {
        self.writer.close().await?;

        Ok(OutputLoggerResult {
            has_output: true,
            logger_name: "JsonlFile".to_string(),
            output_folder_path: Some(self.settings.folder_path.clone()),
        })
    }

    async fn log_handler_record(&mut self, record: &HandlerRecord) -> anyhow::Result<()> {
        self.writer.write_record(record).await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReactionOutputRecordLogWriterError {
    #[error("Can't open log file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

struct ReactionOutputRecordLogWriter {
    folder_path: PathBuf,
    log_file_name: String,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_event_count: u64,
}

impl ReactionOutputRecordLogWriter {
    pub async fn new(settings: &JsonlFileOutputLoggerSettings) -> anyhow::Result<Self> {
        let mut writer = ReactionOutputRecordLogWriter {
            folder_path: settings.folder_path.clone(),
            log_file_name: settings.log_name.clone(),
            next_file_index: 0,
            current_writer: None,
            max_size: settings.max_lines_per_file,
            current_file_event_count: 0,
        };

        writer.open_next_file().await?;
        Ok(writer)
    }

    pub async fn write_record(&mut self, event: &HandlerRecord) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!(
                "{}\n",
                to_string(event).map_err(
                    |e| ReactionOutputRecordLogWriterError::FileWriteError(e.to_string())
                )?
            );
            writer
                .write_all(json.as_bytes())
                .await
                .map_err(|e| ReactionOutputRecordLogWriterError::FileWriteError(e.to_string()))?;

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
            writer
                .flush()
                .await
                .map_err(|e| ReactionOutputRecordLogWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the log file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!(
            "{}/{}_{:05}.jsonl",
            self.folder_path.to_string_lossy(),
            self.log_file_name,
            self.next_file_index
        );

        // Create the file and open it for writing
        let file = File::create(&file_path)
            .await
            .map_err(|_| ReactionOutputRecordLogWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and event count
        self.next_file_index += 1;
        self.current_file_event_count = 0;

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer
                .flush()
                .await
                .map_err(|e| ReactionOutputRecordLogWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}
