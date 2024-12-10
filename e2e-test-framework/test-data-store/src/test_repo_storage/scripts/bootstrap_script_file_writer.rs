use std::{fs::File, io::{BufWriter, Write}, path::PathBuf};
use serde_json::to_string;

use super::BootstrapScriptRecord;

#[derive(Debug, thiserror::Error)]
pub enum BootstrapScriptWriterError {
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error writing to file: {0}")]
    FileWriteError(String),
}

#[derive(Debug)]
pub struct BootstrapScriptWriterSettings {
    pub folder_path: PathBuf,
    pub script_name: String,
    pub max_size: Option<u64>,
}

pub struct BootstrapScriptWriter {
    folder_path: PathBuf,
    script_file_name: String,
    files: Vec<PathBuf>,
    next_file_index: usize,
    current_writer: Option<BufWriter<File>>,
    max_size: u64,
    current_file_record_count: u64,
}

impl BootstrapScriptWriter {
    pub fn new(settings: BootstrapScriptWriterSettings) -> anyhow::Result<Self> {
        log::debug!("Creating new BootstrapScriptWriter with settings: {:?}", settings);

        let BootstrapScriptWriterSettings { folder_path, script_name, max_size } = settings;

        let mut writer = BootstrapScriptWriter {
            folder_path: folder_path.join(&script_name),
            script_file_name: script_name,
            files: Vec::new(),
            next_file_index: 0,
            current_writer: None,
            max_size: max_size.unwrap_or(10000),
            current_file_record_count: 0,
        };

        // Make sure the folder exists for the script files
        std::fs::create_dir_all(&writer.folder_path)?;

        writer.open_next_file()?;
        Ok(writer)
    }

    pub fn write_record(&mut self, record: &BootstrapScriptRecord) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            let record_str = to_string(record).map_err(|e| BootstrapScriptWriterError::FileWriteError(e.to_string()))?;
            writeln!(writer, "{}", record_str).map_err(|e| BootstrapScriptWriterError::FileWriteError(e.to_string()))?;

            self.current_file_record_count += 1;

            if self.current_file_record_count >= self.max_size {
                self.open_next_file()?;
            }
        }

        Ok(())
    }

    fn open_next_file(&mut self) -> anyhow::Result<()> {
        // If there is a current writer, flush it and close it.
        if let Some(writer) = &mut self.current_writer {
            writer.flush().map_err(|e| BootstrapScriptWriterError::FileWriteError(e.to_string()))?;
        }

        // Construct the next file name using the folder path as a base, the script file name, and the next file index.
        // The file index is used to create a 5 digit zero-padded number to ensure the files are sorted correctly.
        let file_path = format!("{}/{}_{:05}.jsonl", self.folder_path.to_string_lossy(), self.script_file_name, self.next_file_index);

        // Create the file and open it for writing
        let file = File::create(&file_path).map_err(|_| BootstrapScriptWriterError::CantOpenFile(file_path.clone()))?;
        self.current_writer = Some(BufWriter::new(file));

        // Increment the file index and record count
        self.next_file_index += 1;
        self.current_file_record_count = 0;
        self.files.push(PathBuf::from(file_path));

        Ok(())
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        if let Some(writer) = &mut self.current_writer {
            writer.flush().map_err(|e| BootstrapScriptWriterError::FileWriteError(e.to_string()))?;
        }
        self.current_writer = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;    
    use tempfile::tempdir;

    use crate::test_repo_storage::scripts::CommentRecord;

    use super::*;

    #[test]
    fn test_create_new_files_when_exceeding_max_size() {
        let temp_dir = tempdir().unwrap();
        let folder_path = temp_dir.path().to_path_buf();
        let script_name = "test_script".to_string();
        let max_size = 5;

        let writer_settings = BootstrapScriptWriterSettings {
            folder_path: folder_path.clone(),
            script_name: script_name.clone(),
            max_size: Some(max_size),
        };

        let mut writer = BootstrapScriptWriter::new(writer_settings).unwrap();

        for i in 0..12 {
            let record = BootstrapScriptRecord::Comment(CommentRecord { comment: format!("record_{}", i) });
            writer.write_record(&record).unwrap();
        }

        writer.close().unwrap();

        // Check that three files were created
        let script_path = folder_path.join(&script_name);
        let mut files: Vec<_> = fs::read_dir(&script_path)
            .unwrap()
            .map(|res| res.unwrap().path())
            .collect();

        files.sort();

        assert_eq!(files.len(), 3);

        // Check the contents of the files
        for (i, file) in files.iter().enumerate() {
            let file_content = fs::read_to_string(file).unwrap();
            let lines: Vec<_> = file_content.lines().collect();
            assert!(lines.len() <= max_size as usize);

            for (j, line) in lines.iter().enumerate() {
                let record: BootstrapScriptRecord = serde_json::from_str(line).unwrap();
                match record {
                    BootstrapScriptRecord::Comment(comment) => {
                        assert_eq!(comment.comment, format!("record_{}", i * max_size as usize + j));
                    },
                    _ => panic!("Unexpected record type"),
                }
            }
        }
    }
}