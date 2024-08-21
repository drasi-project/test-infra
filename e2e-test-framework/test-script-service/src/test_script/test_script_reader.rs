use std::{fs::File, io::{self, BufRead, BufReader}, path::PathBuf};

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use super::SourceChangeEvent;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // This will use the "type" field to determine the enum variant
pub enum TestScriptRecord {
    Comment(CommentRecord),
    Header(HeaderRecord),
    Label(LabelRecord),
    PauseCommand(PauseCommandRecord),
    SourceChange(SourceChangeRecord),
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
    #[serde(default)]
    pub offset_ns: u64,
    pub label: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PauseCommandRecord {
    #[serde(default)]
    pub offset_ns: u64,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FinishRecord {
    #[serde(default)]
    pub offset_ns: u64,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeRecord {
    #[serde(default)]
    pub offset_ns: u64,
    pub source_change_event: SourceChangeEvent,
}

// The SequencedTestScriptRecord struct wraps a TestScriptRecord and ensures that each record has a 
// sequence number and an offset_ns field. The sequence number is the order in which the record was read
// from the test script files. The offset_ns field the nanos since the start of the test script starting time, 
// which is the start_time field in the Header record.
#[derive(Clone, Debug, Serialize)]
pub struct SequencedTestScriptRecord {
    pub seq: u64,
    pub offset_ns: u64,
    pub record: TestScriptRecord,
}

pub struct TestScriptReader {
    files: Vec<PathBuf>,
    current_file_index: usize,
    current_reader: Option<BufReader<File>>,
    header: HeaderRecord,
    footer: Option<SequencedTestScriptRecord>,
    seq: u64,
    offset_ns: u64,
}

impl TestScriptReader {
    pub fn new(files: Vec<PathBuf>) -> io::Result<Self> {
        let mut reader = TestScriptReader {
            files,
            current_file_index: 0,
            current_reader: None,
            header: HeaderRecord::default(),
            footer: None,
            seq: 0,
            offset_ns: 0,
        };

        // TODO: The first record from a new TestScriptReader should always be a Header record.
        // Read the first record from the TestScriptReader and check that it is a Header record; if not return an error.
        // I made this decision to simplify the determination of script starting time. This may need to be revisited, as we could derive
        // the starting time from the first SourceChangeEvent record. However, we would also need to deal with Label and Pause records that
        // currently only have offsets, not absolute times. A Header record will do for now and may eventually be useful for other purposes.
        let read_result = reader.get_next_record();
        if let Ok(seq_rec) = read_result {
            if let TestScriptRecord::Header(header) = seq_rec.record {
                reader.header = header;
                return Ok(reader);
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid test script format. Header record not found."));
            }
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid test script format. Header record not found."));
        }
    }

    // Function that returns true if the TestScriptReader has finished reading all files.
    // pub fn is_finished(&self) -> bool {
    //     self.current_file_index >= self.files.len() && self.current_reader.is_none()
    // }

    // Function to get the header record from the test script.
    pub fn get_header(&self) -> HeaderRecord {
        self.header.clone()
    }

    // Function to get the next record from the TestScriptReader.
    // The TestScriptReader reads lines from the sequence of test script files in the order they were provided.
    // If there are no more records to read, None is returned.
    pub fn get_next_record(&mut self) -> io::Result<SequencedTestScriptRecord> {
        // Once we have reached the end of the test script, always return the Finish record.
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
                    let record: TestScriptRecord = serde_json::from_str(&line)?;

                    let seq_rec = match &record {
                        TestScriptRecord::Comment(_) => {
                            // The TestScriptReader should never return a Comment record.
                            // Return the next record, but need to increment sequence counter
                            self.seq += 1;
                            return self.get_next_record();
                        },
                        TestScriptRecord::Header(_) => {
                            let seq_rec = SequencedTestScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: 0
                            };

                            // Warn if there is a Header record in the middle of the test script.
                            if seq_rec.seq > 0 {
                                log::warn!("Header record found not at start of the test script: {:?}", seq_rec);
                            }

                            seq_rec
                        },
                        TestScriptRecord::Finish(r) => {
                            SequencedTestScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        TestScriptRecord::Label(r) => {
                            SequencedTestScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        TestScriptRecord::PauseCommand(r) => {
                            SequencedTestScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        TestScriptRecord::SourceChange(r) => {
                            SequencedTestScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },                        
                    };
                    self.seq += 1;

                    // Validate and adjust offset_ns for the record and the reader.
                    if seq_rec.offset_ns == 0 {
                        // Missing offsets are defaulted to 0 during deserialization.
                        // They are adjusted to be the same as the previous offset.
                        self.offset_ns = self.offset_ns;
                    } else {
                        if seq_rec.offset_ns >= self.offset_ns {
                            self.offset_ns = seq_rec.offset_ns;
                        } else {
                            // Throw an error if the offset_ns is less than the previous record's offset_ns.
                            let error_message = format!("Offset_ns for record {:?} is less than the previous record's offset_ns {}.", seq_rec, self.offset_ns);
                            log::error!("{}", error_message);
                            return Err(io::Error::new(io::ErrorKind::InvalidData, error_message));
                        }
                    }
                    self.offset_ns = seq_rec.offset_ns;

                    // If the record is a Finish record, set the footer and return it so it is always returned in the future.
                    if let TestScriptRecord::Finish(_) = seq_rec.record {
                        self.footer = Some(seq_rec.clone());
                    }
                    Ok(seq_rec)
                },
                Err(e) => {
                    Err(e)
                }
            }
        } else {
            // Generate a synthetic Finish record to mark the end of the test script.
            self.footer = Some(SequencedTestScriptRecord {
                record: TestScriptRecord::Finish(FinishRecord { offset_ns: self.offset_ns, description: "Auto generated at end of test script.".to_string() }),
                seq: self.seq,
                offset_ns: self.offset_ns
            });
            Ok(self.footer.as_ref().unwrap().clone())
        }
    }

    // Function to open the next file in the sequence of test script files.
    fn open_next_file(&mut self) -> io::Result<()> {
        if self.current_file_index < self.files.len() {
            let file_path = &self.files[self.current_file_index];
            let file = File::open(file_path)?;
            self.current_reader = Some(BufReader::new(file));
            self.current_file_index += 1;
        } else {
            self.current_reader = None;
        }
        Ok(())
    }
}