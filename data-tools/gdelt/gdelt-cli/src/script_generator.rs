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

use std::path::{Path, PathBuf};
use chrono::{DateTime, FixedOffset, Utc, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use csv::ReaderBuilder;

// Re-create the structs from test-data-store/src/scripts/mod.rs to match the expected format

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEvent {
    pub op: String,
    #[serde(rename = "reactivatorStart_ns")]
    pub reactivator_start_ns: u64,
    #[serde(rename = "reactivatorEnd_ns")]
    pub reactivator_end_ns: u64,
    pub payload: SourceChangeEventPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventPayload {
    pub source: SourceChangeEventSourceInfo,
    pub before: Value,
    pub after: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceChangeEventSourceInfo {
    pub db: String,
    pub table: String,
    pub ts_ns: u64,
    pub lsn: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum BootstrapScriptRecord {
    Comment(CommentRecord),
    Header(BootstrapHeaderRecord),
    Label(LabelRecord),
    Node(NodeRecord),
    Relation(RelationRecord),
    Finish(BootstrapFinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ChangeScriptRecord {
    Comment(CommentRecord),
    Header(ChangeHeaderRecord),
    Label(LabelRecord),
    PauseCommand(PauseCommandRecord),
    SourceChange(SourceChangeRecord),
    Finish(ChangeFinishRecord),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommentRecord {
    pub comment: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapHeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeHeaderRecord {
    pub start_time: DateTime<FixedOffset>,
    #[serde(default)]
    pub description: String,
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
pub struct BootstrapFinishRecord {
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeFinishRecord {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub labels: Vec<String>,
    #[serde(default)]
    pub properties: Value,
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
    pub properties: Value,
}

pub struct ScriptGenerator {
    output_path: PathBuf,
}

impl ScriptGenerator {
    pub fn new(output_path: PathBuf) -> Self {
        Self { output_path }
    }

    pub async fn generate_bootstrap_scripts(
        &self,
        source_files: Vec<PathBuf>,
        source_type: &str,
    ) -> anyhow::Result<()> {
        let bootstrap_path = self.output_path.join("bootstrap_scripts");
        create_dir_all(&bootstrap_path).await?;

        // Create header record
        let header = BootstrapScriptRecord::Header(BootstrapHeaderRecord {
            start_time: Utc::now().with_timezone(&FixedOffset::east_opt(0).unwrap()),
            description: format!("GDELT {} bootstrap data", source_type),
        });

        // Write header to the first file
        let header_file = bootstrap_path.join(format!("{}_header.jsonl", source_type));
        let mut file = File::create(&header_file).await?;
        file.write_all(format!("{}\n", serde_json::to_string(&header)?).as_bytes()).await?;

        // Process each source file and convert to node records
        for (idx, source_file) in source_files.iter().enumerate() {
            let output_file = bootstrap_path.join(format!("{}_{:05}.jsonl", source_type, idx));
            self.convert_to_bootstrap_nodes(&source_file, &output_file, source_type).await?;
        }

        // Create finish record
        let finish = BootstrapScriptRecord::Finish(BootstrapFinishRecord {
            description: format!("Completed {} bootstrap data", source_type),
        });

        let finish_file = bootstrap_path.join(format!("{}_finish.jsonl", source_type));
        let mut file = File::create(&finish_file).await?;
        file.write_all(format!("{}\n", serde_json::to_string(&finish)?).as_bytes()).await?;

        Ok(())
    }

    pub async fn generate_change_scripts(
        &self,
        source_files: Vec<PathBuf>,
        source_type: &str,
        start_time: DateTime<FixedOffset>,
    ) -> anyhow::Result<()> {
        // Ensure output directory exists
        create_dir_all(&self.output_path).await?;
        
        let change_file = self.output_path.join("source_change_scripts_00000.jsonl");
        let mut output = File::create(&change_file).await?;

        // Create header record
        let header = ChangeScriptRecord::Header(ChangeHeaderRecord {
            start_time,
            description: format!("GDELT {} changes", source_type),
        });
        
        output.write_all(format!("{}\n", serde_json::to_string(&header)?).as_bytes()).await?;

        // Process each source file
        for source_file in source_files {
            self.append_change_events(&source_file, &mut output, source_type, start_time).await?;
        }

        // Create finish record
        let finish = ChangeScriptRecord::Finish(ChangeFinishRecord {
            offset_ns: 0,
            description: format!("Completed {} changes", source_type),
        });
        
        output.write_all(format!("{}\n", serde_json::to_string(&finish)?).as_bytes()).await?;

        Ok(())
    }

    async fn convert_to_bootstrap_nodes(
        &self,
        source_file: &Path,
        output_file: &Path,
        source_type: &str,
    ) -> anyhow::Result<()> {
        let file_content = tokio::fs::read_to_string(source_file).await?;
        let mut output = File::create(output_file).await?;

        // Parse CSV using csv crate
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'\t')
            .from_reader(file_content.as_bytes());

        for result in rdr.records() {
            let record = result?;
            
            let node = match source_type {
                "event" => self.parse_event_to_node(&record)?,
                "graph" => self.parse_graph_to_node(&record)?,
                "mention" => self.parse_mention_to_node(&record)?,
                _ => continue,
            };

            let bootstrap_record = BootstrapScriptRecord::Node(node);
            output.write_all(format!("{}\n", serde_json::to_string(&bootstrap_record)?).as_bytes()).await?;
        }

        Ok(())
    }

    async fn append_change_events(
        &self,
        source_file: &Path,
        output: &mut File,
        source_type: &str,
        start_time: DateTime<FixedOffset>,
    ) -> anyhow::Result<()> {
        let file_content = tokio::fs::read_to_string(source_file).await?;

        // Parse CSV using csv crate
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'\t')
            .from_reader(file_content.as_bytes());

        // Get timestamp from filename
        let timestamp = self.extract_timestamp_from_filename(source_file)?;
        let offset_ns = self.calculate_offset_ns(timestamp, start_time);

        for result in rdr.records() {
            let record = result?;
            
            let (before, after) = match source_type {
                "event" => self.parse_event_change(&record)?,
                "graph" => self.parse_graph_change(&record)?,
                "mention" => self.parse_mention_change(&record)?,
                _ => continue,
            };

            let source_change_event = SourceChangeEvent {
                op: if before.is_null() { "c" } else { "u" }.to_string(),
                reactivator_start_ns: offset_ns,
                reactivator_end_ns: offset_ns + 1000000, // 1ms later
                payload: SourceChangeEventPayload {
                    source: SourceChangeEventSourceInfo {
                        db: "gdelt".to_string(),
                        table: source_type.to_string(),
                        ts_ns: offset_ns,
                        lsn: offset_ns / 1000000, // Convert to ms for LSN
                    },
                    before,
                    after,
                },
            };

            let change_record = ChangeScriptRecord::SourceChange(SourceChangeRecord {
                offset_ns,
                source_change_event,
            });

            output.write_all(format!("{}\n", serde_json::to_string(&change_record)?).as_bytes()).await?;
        }

        Ok(())
    }

    fn parse_event_to_node(&self, record: &csv::StringRecord) -> anyhow::Result<NodeRecord> {
        // GDELT event has 61 fields
        if record.len() < 61 {
            anyhow::bail!("Invalid event record: expected 61 fields, got {}", record.len());
        }

        let id = record.get(0).unwrap_or("").to_string(); // GlobalEventID
        
        let properties = json!({
            "GlobalEventID": record.get(0).unwrap_or(""),
            "Day": record.get(1).unwrap_or(""),
            "MonthYear": record.get(2).unwrap_or(""),
            "Year": record.get(3).unwrap_or(""),
            "FractionDate": record.get(4).unwrap_or(""),
            "Actor1Code": record.get(5).unwrap_or(""),
            "Actor1Name": record.get(6).unwrap_or(""),
            "Actor1CountryCode": record.get(7).unwrap_or(""),
            "Actor2Code": record.get(15).unwrap_or(""),
            "Actor2Name": record.get(16).unwrap_or(""),
            "Actor2CountryCode": record.get(17).unwrap_or(""),
            "EventCode": record.get(26).unwrap_or(""),
            "EventBaseCode": record.get(27).unwrap_or(""),
            "EventRootCode": record.get(28).unwrap_or(""),
            "QuadClass": record.get(29).unwrap_or(""),
            "GoldsteinScale": record.get(30).unwrap_or(""),
            "NumMentions": record.get(31).unwrap_or(""),
            "NumSources": record.get(32).unwrap_or(""),
            "NumArticles": record.get(33).unwrap_or(""),
            "AvgTone": record.get(34).unwrap_or(""),
            "DateAdded": record.get(59).unwrap_or(""),
            "SourceURL": record.get(60).unwrap_or("")
        });

        Ok(NodeRecord {
            id,
            labels: vec!["Event".to_string()],
            properties,
        })
    }

    fn parse_graph_to_node(&self, record: &csv::StringRecord) -> anyhow::Result<NodeRecord> {
        // GKG records have variable fields but we'll use what's available
        let id = record.get(0).unwrap_or("").to_string(); // Record ID or Date
        
        let properties = json!({
            "RecordID": record.get(0).unwrap_or(""),
            "Date": record.get(1).unwrap_or(""),
            "SourceCollectionIdentifier": record.get(2).unwrap_or(""),
            "SourceCommonName": record.get(3).unwrap_or(""),
            "DocumentIdentifier": record.get(4).unwrap_or(""),
            "Counts": record.get(5).unwrap_or(""),
            "Themes": record.get(7).unwrap_or(""),
            "Locations": record.get(9).unwrap_or(""),
            "Persons": record.get(11).unwrap_or(""),
            "Organizations": record.get(13).unwrap_or(""),
            "Tone": record.get(15).unwrap_or("")
        });

        Ok(NodeRecord {
            id,
            labels: vec!["Graph".to_string()],
            properties,
        })
    }

    fn parse_mention_to_node(&self, record: &csv::StringRecord) -> anyhow::Result<NodeRecord> {
        // Mention records have about 15 fields
        let global_event_id = record.get(0).unwrap_or("").to_string();
        let mention_time = record.get(2).unwrap_or("").to_string();
        let id = format!("{}-{}", global_event_id, mention_time);
        
        let properties = json!({
            "GlobalEventID": record.get(0).unwrap_or(""),
            "EventTimeDate": record.get(1).unwrap_or(""),
            "MentionTimeDate": record.get(2).unwrap_or(""),
            "MentionType": record.get(3).unwrap_or(""),
            "MentionSourceName": record.get(4).unwrap_or(""),
            "MentionIdentifier": record.get(5).unwrap_or(""),
            "SentenceID": record.get(6).unwrap_or(""),
            "Actor1CharOffset": record.get(7).unwrap_or(""),
            "Actor2CharOffset": record.get(8).unwrap_or(""),
            "ActionCharOffset": record.get(9).unwrap_or(""),
            "InRawText": record.get(10).unwrap_or(""),
            "Confidence": record.get(11).unwrap_or(""),
            "MentionDocLen": record.get(12).unwrap_or(""),
            "MentionDocTone": record.get(13).unwrap_or("")
        });

        Ok(NodeRecord {
            id,
            labels: vec!["Mention".to_string()],
            properties,
        })
    }

    fn parse_event_change(&self, record: &csv::StringRecord) -> anyhow::Result<(Value, Value)> {
        let node = self.parse_event_to_node(record)?;
        let after = json!({
            "id": node.id,
            "labels": node.labels,
            "properties": node.properties
        });
        Ok((Value::Null, after))
    }

    fn parse_graph_change(&self, record: &csv::StringRecord) -> anyhow::Result<(Value, Value)> {
        let node = self.parse_graph_to_node(record)?;
        let after = json!({
            "id": node.id,
            "labels": node.labels,
            "properties": node.properties
        });
        Ok((Value::Null, after))
    }

    fn parse_mention_change(&self, record: &csv::StringRecord) -> anyhow::Result<(Value, Value)> {
        let node = self.parse_mention_to_node(record)?;
        let after = json!({
            "id": node.id,
            "labels": node.labels,
            "properties": node.properties
        });
        Ok((Value::Null, after))
    }

    fn extract_timestamp_from_filename(&self, path: &Path) -> anyhow::Result<NaiveDateTime> {
        let filename = path.file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;
        
        // Extract YYYYMMDDHHMMSS from filename
        // Filename can be like "20240101000000.export" or just "20240101000000"
        let parts: Vec<&str> = filename.split('.').collect();
        let timestamp_str = parts.get(0).unwrap_or(&"");
        
        if timestamp_str.len() >= 14 {
            let year = timestamp_str[0..4].parse::<i32>()?;
            let month = timestamp_str[4..6].parse::<u32>()?;
            let day = timestamp_str[6..8].parse::<u32>()?;
            let hour = timestamp_str[8..10].parse::<u32>()?;
            let minute = timestamp_str[10..12].parse::<u32>()?;
            let second = timestamp_str[12..14].parse::<u32>()?;
            
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| anyhow::anyhow!("Invalid date: {}-{}-{}", year, month, day))?;
            let time = chrono::NaiveTime::from_hms_opt(hour, minute, second)
                .ok_or_else(|| anyhow::anyhow!("Invalid time: {}:{}:{}", hour, minute, second))?;
            
            Ok(NaiveDateTime::new(date, time))
        } else {
            anyhow::bail!("Invalid timestamp in filename: {}", filename)
        }
    }

    fn calculate_offset_ns(&self, event_time: NaiveDateTime, start_time: DateTime<FixedOffset>) -> u64 {
        let event_datetime = event_time.and_utc();
        let start_datetime = start_time.with_timezone(&Utc);
        let duration = event_datetime.signed_duration_since(start_datetime);
        duration.num_nanoseconds().unwrap_or(0) as u64
    }
}