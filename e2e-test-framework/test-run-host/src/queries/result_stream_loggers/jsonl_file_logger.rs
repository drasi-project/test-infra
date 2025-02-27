use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::{fs::{create_dir_all, File}, io::{AsyncWriteExt, BufWriter}};

use test_data_store::test_run_storage::{TestRunQueryId, TestRunQueryStorage};

use crate::queries::result_stream_handlers::ResultStreamRecord;

use super::{ResultStreamLogger, ResultStreamLoggerError, ResultStreamLoggerResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JsonlFileResultStreamLoggerConfig {
    pub max_lines_per_file: Option<u64>,
}

#[derive(Debug)]
pub struct JsonlFileResultStreamLoggerSettings {
    pub folder_path: PathBuf,
    pub log_name: String,
    pub max_lines_per_file: u64,
    pub test_run_query_id: TestRunQueryId,
}

impl JsonlFileResultStreamLoggerSettings {
    pub fn new(test_run_query_id: TestRunQueryId, config: &JsonlFileResultStreamLoggerConfig, folder_path: PathBuf) -> anyhow::Result<Self> {
        return Ok(Self {
            folder_path,
            log_name: "results".to_string(),
            max_lines_per_file: config.max_lines_per_file.unwrap_or(10000),
            test_run_query_id,
        });
    }
}

pub struct JsonlFileResultStreamLogger {
    #[allow(dead_code)]
    settings: JsonlFileResultStreamLoggerSettings,
    writer: ResultStreamRecordLogWriter,
}

impl JsonlFileResultStreamLogger {
    pub async fn new(test_run_query_id: TestRunQueryId, def:&JsonlFileResultStreamLoggerConfig, output_storage: &TestRunQueryStorage) -> anyhow::Result<Box<dyn ResultStreamLogger + Send + Sync>> {
        log::debug!("Creating JsonlFileResultStreamLogger for {} from {:?}, ", test_run_query_id, def);

        let folder_path = output_storage.result_change_path.join("jsonl_file");
        let settings = JsonlFileResultStreamLoggerSettings::new(test_run_query_id, &def, folder_path)?;
        log::trace!("Creating JsonlFileResultStreamLogger with settings {:?}, ", settings);

        if !std::path::Path::new(&settings.folder_path).exists() {
            match create_dir_all(&settings.folder_path).await {
                Ok(_) => {},
                Err(e) => return Err(ResultStreamLoggerError::Io(e).into()),
            };
        }        

        let writer = ResultStreamRecordLogWriter::new(&settings).await?;

        Ok(Box::new( Self { 
            settings,
            writer,
        }))
    }
}

#[async_trait]
impl ResultStreamLogger for JsonlFileResultStreamLogger {
    async fn end_test_run(&mut self) -> anyhow::Result<ResultStreamLoggerResult> {
        self.writer.close().await?;

        Ok(ResultStreamLoggerResult {
            has_output: true,
            logger_name: "JsonlFile".to_string(),
            output_folder_path: Some(self.settings.folder_path.clone()),
        })
    }
    
    async fn log_result_stream_record(&mut self, record: &ResultStreamRecord) -> anyhow::Result<()> {
        self.writer.write_record(record).await?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResultStreamRecordLogWriterError {
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

struct ResultStreamRecordLogWriter {
    folder_path: PathBuf,
    log_file_name: String,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_event_count: u64,
}

impl ResultStreamRecordLogWriter { 
    pub async fn new(settings: &JsonlFileResultStreamLoggerSettings) -> anyhow::Result<Self> {
        let mut writer = ResultStreamRecordLogWriter {
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

    pub async fn write_record(&mut self, event: &ResultStreamRecord) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let json = format!("{}\n", to_string(event).map_err(|e| ResultStreamRecordLogWriterError::FileWriteError(e.to_string()))?);
            writer.write_all(json.as_bytes()).await.map_err(|e| ResultStreamRecordLogWriterError::FileWriteError(e.to_string()))?;

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
            writer.flush().await.map_err(|e| ResultStreamRecordLogWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the script file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!("{}/{}_{:05}.jsonl", self.folder_path.to_string_lossy(), self.log_file_name, self.next_file_index);

        // Create the file and open it for writing
        let file = File::create(&file_path).await.map_err(|_| ResultStreamRecordLogWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and event count
        self.next_file_index += 1;
        self.current_file_event_count = 0;

        Ok(())
    }

    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer.flush().await.map_err(|e| ResultStreamRecordLogWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}