use std::{fs::File, io::{BufRead, BufReader}, path::PathBuf};

use super::{ChangeScriptRecord, FinishRecord, HeaderRecord, SequencedChangeScriptRecord};

#[derive(Debug, thiserror::Error)]
pub enum ChangeScriptReaderError {
    #[error("Script is missing Header record: {0}")]
    MissingHeader(String),
    #[error("Can't open script file: {0}")]
    CantOpenFile(String),
    #[error("Error reading file: {0}")]
    FileReadError(String),
    #[error("Offset_ns for record {0} is less than the previous record's offset_ns.")]
    RecordOutOfSequence(u64),
    #[error("Bad record format in file {0}: Error - {1}; Record - {2}")]
    BadRecordFormat(String, String, String),
}

pub struct ChangeScriptReader {
    files: Vec<PathBuf>,
    next_file_index: usize,
    current_reader: Option<BufReader<File>>,
    header: HeaderRecord,
    footer: Option<SequencedChangeScriptRecord>,
    seq: u64,
    offset_ns: u64,
}

impl ChangeScriptReader {
    pub fn new(files: Vec<PathBuf>) -> anyhow::Result<Self> {
        let mut reader = ChangeScriptReader {
            files,
            next_file_index: 0,
            current_reader: None,
            header: HeaderRecord::default(),
            footer: None,
            seq: 0,
            offset_ns: 0,
        };

        // TODO: The first record from a new ChangeScriptReader should always be a Header record.
        // Read the first record from the ChangeScriptReader and check that it is a Header record; if not return an error.
        // I made this decision to simplify the determination of script starting time. This may need to be revisited, as we could derive
        // the starting time from the first SourceChangeEvent record. However, we would also need to deal with Label and Pause records that
        // currently only have offsets, not absolute times. A Header record will do for now and may eventually be useful for other purposes.
        let read_result = reader.get_next_record();
        if let Ok(seq_rec) = read_result {
            if let ChangeScriptRecord::Header(header) = seq_rec.record {
                reader.header = header;
                return Ok(reader);
            } else {
                return Err(ChangeScriptReaderError::MissingHeader(reader.get_current_file_name()).into());
            }
        } else {
            return Err(ChangeScriptReaderError::MissingHeader(reader.get_current_file_name()).into());
        }
    }

    // Function that returns true if the ChangeScriptReader has finished reading all files.
    // pub fn is_finished(&self) -> bool {
    //     self.current_file_index >= self.files.len() && self.current_reader.is_none()
    // }

    // Function to get the header record from the script.
    pub fn get_header(&self) -> HeaderRecord {
        self.header.clone()
    }

    // Function to get the next record from the ChangeScriptReader.
    // The ChangeScriptReader reads lines from the sequence of script files in the order they were provided.
    // If there are no more records to read, None is returned.
    fn get_next_record(&mut self) -> anyhow::Result<SequencedChangeScriptRecord> {
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
                    let record: ChangeScriptRecord = match serde_json::from_str(&line) {
                        Ok(r) => r,
                        Err(e) => {
                            return Err(ChangeScriptReaderError::BadRecordFormat(
                                self.get_current_file_name(), e.to_string(), line).into());
                        },
                    };

                    let seq_rec = match &record {
                        ChangeScriptRecord::Comment(_) => {
                            // The ChangeScriptReader should never return a Comment record.
                            // Return the next record, but need to increment sequence counter
                            self.seq += 1;
                            return self.get_next_record();
                        },
                        ChangeScriptRecord::Header(_) => {
                            let seq_rec = SequencedChangeScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: 0
                            };

                            // Warn if there is a Header record in the middle of the script.
                            if seq_rec.seq > 0 {
                                log::warn!("Header record found not at start of the script: {:?}", seq_rec);
                            }

                            seq_rec
                        },
                        ChangeScriptRecord::Finish(r) => {
                            SequencedChangeScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        ChangeScriptRecord::Label(r) => {
                            SequencedChangeScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        ChangeScriptRecord::PauseCommand(r) => {
                            SequencedChangeScriptRecord {
                                record: record.clone(),
                                seq: self.seq,
                                offset_ns: r.offset_ns
                            }
                        },
                        ChangeScriptRecord::SourceChange(r) => {
                            SequencedChangeScriptRecord {
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
                            return Err(ChangeScriptReaderError::RecordOutOfSequence(self.seq).into());
                        }
                    }
                    self.offset_ns = seq_rec.offset_ns;

                    // If the record is a Finish record, set the footer and return it so it is always returned in the future.
                    if let ChangeScriptRecord::Finish(_) = seq_rec.record {
                        self.footer = Some(seq_rec.clone());
                    }
                    Ok(seq_rec)
                },
                Err(e) => Err(ChangeScriptReaderError::FileReadError(e.to_string()).into()),
            }
        } else {
            // Generate a synthetic Finish record to mark the end of the script.
            self.footer = Some(SequencedChangeScriptRecord {
                record: ChangeScriptRecord::Finish(FinishRecord { offset_ns: self.offset_ns, description: "Auto generated at end of script.".to_string() }),
                seq: self.seq,
                offset_ns: self.offset_ns
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
                Err(_) => return Err(ChangeScriptReaderError::CantOpenFile(file_path.to_string_lossy().into_owned()).into()),
            };
            self.current_reader = Some(BufReader::new(file));
            self.next_file_index += 1;
        } else {
            self.current_reader = None;
        }
        Ok(())
    }
}

impl Iterator for ChangeScriptReader {
    type Item = anyhow::Result<SequencedChangeScriptRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // If the ChangeScriptReader has finished reading all files, return None.
        // This is determined by the value of 'footer', which will be set to a Finish record when the last record is read.
        if self.footer.is_some() {
            None
        } else {
            Some(self.get_next_record())
        }
    }
}