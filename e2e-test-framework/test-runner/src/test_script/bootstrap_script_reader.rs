use std::{fs::File, io::{BufRead, BufReader}, path::PathBuf};

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // This will use the "type" field to determine the enum variant
pub enum BootstrapScriptRecord {
    Comment(CommentRecord),
    Header(HeaderRecord),
    Label(LabelRecord),
    Node(NodeRecord),
    Relation(RelationRecord),
    Finish(FinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommentRecord {
    pub comment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
}

impl Default for HeaderRecord {
    fn default() -> Self {
        HeaderRecord {
            start_time: DateTime::parse_from_rfc3339("1970-01-01T00:00:00.000-00:00").unwrap(),
            description: "Error: Header record not found.".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelRecord {
    pub label: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub labels: Vec<String>,
    #[serde(default)]
    pub properties: serde_json::Value
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelationRecord {
    pub id: String,
    pub labels: Vec<String>,
    pub start_id: String,
    pub start_label: Option<String>,
    pub end_id: String,
    pub end_label: Option<String>,    
    #[serde(default)]
    pub properties: serde_json::Value
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinishRecord {
    #[serde(default)]
    pub description: String,
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
    header: HeaderRecord,
    footer: Option<SequencedBootstrapScriptRecord>,
    seq: u64,
}

impl BootstrapScriptReader {
    pub fn new(files: Vec<PathBuf>) -> anyhow::Result<Self> {
        let mut reader = BootstrapScriptReader {
            files,
            next_file_index: 0,
            current_reader: None,
            header: HeaderRecord::default(),
            footer: None,
            seq: 0,
        };

        // TODO: The first record from a new BootstrapScriptReader should always be a Header record.
        // Read the first record from the BootstrapScriptReader and check that it is a Header record; if not return an error.
        // I made this decision to simplify the determination of script starting time. This may need to be revisited, as we could derive
        // the starting time from the first SourceChangeEvent record. However, we would also need to deal with Label and Pause records that
        // currently only have offsets, not absolute times. A Header record will do for now and may eventually be useful for other purposes.
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
    pub fn _get_header(&self) -> HeaderRecord {
        self.header.clone()
    }

    fn get_current_file_name(&self) -> String {
        if self.current_reader.is_some() {
            let path = self.files[self.next_file_index-1].clone();
            path.to_string_lossy().into_owned()
        } else {
            "None".to_string()
        }
    }

    // Function to get the next record from the BootstrapScriptReader.
    // The BootstrapScriptReader reads lines from the sequence of script files in the order they were provided.
    // If there are no more records to read, None is returned.
    pub fn get_next_record(&mut self) -> anyhow::Result<SequencedBootstrapScriptRecord> {
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
                record: BootstrapScriptRecord::Finish(FinishRecord { description: "Auto generated at end of script.".to_string() }),
                seq: self.seq,
            });
            Ok(self.footer.as_ref().unwrap().clone())
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