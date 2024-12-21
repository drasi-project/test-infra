use std::{fs::File, io::{BufRead, BufReader}, path::PathBuf, pin::Pin, task::{Context, Poll}};

use futures::Stream;
use serde::Serialize;

use super::{BootstrapScriptRecord, BootstrapFinishRecord, BootstrapHeaderRecord};

#[derive(Debug, thiserror::Error)]
pub enum BootstrapScriptReaderError {
    #[error("Script is missing Header record: {0}")]
    MissingHeader(String),
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error reading file: {0}")]
    FileReadError(String),
    #[error("Bad record format in file {0}: Error - {1}; Record - {2}")]
    BadRecordFormat(String, String, String),
}

// The SequencedBootstrapScriptRecord struct wraps a BootstrapScriptRecord and ensures that each record has a 
// sequence number and an offset_ns field. The sequence number is the order in which the record was read
// from the script files. The offset_ns field the nanos since the start of the script starting time, 
// which is the start_time field in the Header record.
#[derive(Clone, Debug, Serialize)]
pub struct SequencedBootstrapScriptRecord {
    pub seq: u64,
    pub record: BootstrapScriptRecord,
}

pub struct BootstrapScriptReader {
    files: Vec<PathBuf>,
    next_file_index: usize,
    current_reader: Option<BufReader<File>>,
    header: BootstrapHeaderRecord,
    footer: Option<SequencedBootstrapScriptRecord>,
    seq: u64,
}

impl BootstrapScriptReader {
    pub fn new(files: Vec<PathBuf>) -> anyhow::Result<Self> {
        let mut reader = BootstrapScriptReader {
            files,
            next_file_index: 0,
            current_reader: None,
            header: BootstrapHeaderRecord::default(),
            footer: None,
            seq: 0,
        };

        let read_result = reader.get_next_record();

        if let Ok(seq_rec) = read_result {
            if let BootstrapScriptRecord::Header(header) = seq_rec.record {
                reader.header = header;
                return Ok(reader);
            } else {
                return Err(BootstrapScriptReaderError::MissingHeader(reader.get_current_file_name()).into());
            }
        } else {
            return Err(BootstrapScriptReaderError::MissingHeader(reader.get_current_file_name()).into());
        }
    }

    // Function to get the header record from the script.
    pub fn get_header(&self) -> BootstrapHeaderRecord {
        self.header.clone()
    }

    // Function to get the next record from the BootstrapScriptReader.
    // The BootstrapScriptReader reads lines from the sequence of script files in the order they were provided.
    // If there are no more records to read, None is returned.
    fn get_next_record(&mut self) -> anyhow::Result<SequencedBootstrapScriptRecord> {
        // Once we have reached the end of the script, always return the Finish record.
        if self.footer.is_some() {
            return Ok(self.footer.as_ref().unwrap().clone());
        }

        if self.current_reader.is_none() {
            self.open_next_file()?;
        }

        if let Some(reader) = &mut self.current_reader {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    self.current_reader = None;
                    self.get_next_record()
                },
                Ok(_) => {
                    let record: BootstrapScriptRecord = match serde_json::from_str(&line) {
                        Ok(r) => r,
                        Err(e) => {
                            return Err(BootstrapScriptReaderError::BadRecordFormat(
                                self.get_current_file_name(), e.to_string(), line).into());
                        },
                    };

                    let seq_rec = match &record {
                        BootstrapScriptRecord::Comment(_) => {
                            // The BootstrapScriptReader should never return a Comment record.
                            // Return the next record, but need to increment sequence counter
                            self.seq += 1;
                            return self.get_next_record();
                        },
                        BootstrapScriptRecord::Header(_) => {
                            let seq_rec = SequencedBootstrapScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                            };

                            // Warn if there is a Header record in the middle of the script.
                            if seq_rec.seq > 0 {
                                log::warn!("Header record found not at start of the script: {:?}", seq_rec);
                            }

                            seq_rec
                        },
                        _ => {
                            SequencedBootstrapScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                            }
                        },                   
                    };
                    self.seq += 1;

                    // If the record is a Finish record, set the footer and return it so it is always returned in the future.
                    if let BootstrapScriptRecord::Finish(_) = seq_rec.record {
                        self.footer = Some(seq_rec.clone());
                    }
                    Ok(seq_rec)
                },
                Err(e) => Err(BootstrapScriptReaderError::FileReadError(e.to_string()).into()),
            }
        } else {
            // Generate a synthetic Finish record to mark the end of the script.
            self.footer = Some(SequencedBootstrapScriptRecord {
                record: BootstrapScriptRecord::Finish(BootstrapFinishRecord { description: "Auto generated at end of script.".to_string() }),
                seq: self.seq,
            });
            Ok(self.footer.as_ref().unwrap().clone())
        }
    }

    fn get_current_file_name(&self) -> String {
        if self.current_reader.is_some() {
            let path = self.files[self.next_file_index-1].clone();
            path.to_string_lossy().into_owned()
        } else {
            "None".to_string()
        }
    }

    // Function to open the next file in the sequence of script files.
    fn open_next_file(&mut self) -> anyhow::Result<()> {
        if self.next_file_index < self.files.len() {
            let file_path = &self.files[self.next_file_index];
            let file = match File::open(file_path) {
                Ok(f) => f,
                Err(_) => return Err(BootstrapScriptReaderError::CantOpenFile(file_path.to_string_lossy().into_owned()).into()),
            };
            self.current_reader = Some(BufReader::new(file));
            self.next_file_index += 1;
        } else {
            self.current_reader = None;
        }
        Ok(())
    }
}

impl Iterator for BootstrapScriptReader {
    type Item = anyhow::Result<SequencedBootstrapScriptRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // If the BootstrapScriptReader has finished reading all files, return None.
        // This is determined by the value of 'footer', which will be set to a Finish record when the last record is read.
        if self.footer.is_some() {
            None
        } else {
            Some(self.get_next_record())
        }
    }
}

impl Stream for BootstrapScriptReader {
    type Item = anyhow::Result<SequencedBootstrapScriptRecord>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.next() {
            Poll::Ready(Some(item))
        } else {
            Poll::Ready(None)
        }
    }
}