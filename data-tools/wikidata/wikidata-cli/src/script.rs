use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{FixedOffset, NaiveDateTime, ParseError, TimeZone};
use serde::Serialize;
use strum::IntoEnumIterator;
use test_data_store::test_repo_storage::scripts::bootstrap_script_file_writer::{BootstrapScriptWriter, BootstrapScriptWriterSettings};
use test_data_store::test_repo_storage::scripts::change_script_file_writer::{ChangeScriptWriter, ChangeScriptWriterSettings};
use test_data_store::test_repo_storage::scripts::{BootstrapHeaderRecord, BootstrapScriptRecord, ChangeHeaderRecord, ChangeScriptRecord, SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo, SourceChangeRecord};
use tokio::fs;

use crate::wikidata::extractors::item_revision_to_bootstrap_data_record;
use crate::MakeScriptCommandArgs;
use crate::wikidata::{ItemRevisionFileContent, ItemType, extractors::parse_item_revision};

#[derive(Debug, Serialize)]
pub struct ScriptSourceFiles {
    start_datetime: NaiveDateTime,
    start_datetime_ns: u64,
    end_datetime: NaiveDateTime,
    bootstrap_script_files: HashMap<ItemType, Vec<(NaiveDateTime, PathBuf)>>,
    change_script_files: Vec<(NaiveDateTime, ItemType, PathBuf, Option<(NaiveDateTime, PathBuf)>)>,
}

async fn create_bootstrap_data_record(item_type: ItemType, path: PathBuf) -> anyhow::Result<BootstrapScriptRecord> {
    let revision_file_str = fs::read_to_string(path).await?;
    let item_revision: ItemRevisionFileContent = serde_json::from_str(&revision_file_str)?;

    item_revision_to_bootstrap_data_record(item_type, &item_revision)
}

async fn create_source_change_record(item_type: ItemType, op: String, offset_ns: u64, ts_ms: u64, source: SourceChangeEventSourceInfo, before: Option<PathBuf>, after: Option<PathBuf>) -> anyhow::Result<ChangeScriptRecord> {
    let before = match before {
        Some(path) => {
            let revision_file_str = fs::read_to_string(path).await?;
            let item_revision: ItemRevisionFileContent = serde_json::from_str(&revision_file_str)?;
            parse_item_revision(item_type, &item_revision)?
        },
        None => serde_json::json!({}),
    };

    let after = match after {
        Some(path) => {
            let revision_file_str = fs::read_to_string(path).await?;
            let item_revision: ItemRevisionFileContent = serde_json::from_str(&revision_file_str)?;
            parse_item_revision(item_type, &item_revision)?
        },
        None => serde_json::json!({}),
    };

    Ok(ChangeScriptRecord::SourceChange(SourceChangeRecord {
        offset_ns,
        source_change_event: SourceChangeEvent {
            op,
            ts_ms,
            schema: "".to_string(),
            payload: SourceChangeEventPayload {
                source,
                before,
                after,
            }
        }
    }))
}

pub async fn create_test_scripts(args: &MakeScriptCommandArgs, item_root_path: PathBuf, script_root_path: PathBuf, overwrite: bool) -> anyhow::Result<()> {
    log::info!("Generating test scripts for test ID: {}", args.test_id);

    // Create the list of item type folder paths to generate scripts for
    let type_paths: Vec<(ItemType, PathBuf)> = match &args.item_types.len() {
        0 => ItemType::iter().map(|t| (t, item_root_path.join(t.to_string()))).collect(),
        _ => args.item_types.iter().map(|t| (*t, item_root_path.join(t.to_string()))).collect(),
    };

    // Configure start and end times, providing defaults if not specified.
    let (start_datetime, end_datetime) = match (args.rev_start, args.rev_end) {
        (Some(start), Some(end)) => {
            (start, end)
        },
        (Some(start), None) => {
            (start, NaiveDateTime::MAX)
        },
        (None, Some(end)) => {
            (NaiveDateTime::MIN, end)
        },
        (None, None) => {
            (NaiveDateTime::MIN, NaiveDateTime::MAX)
        },
    };
    let start_datetime_ns = start_datetime.and_utc().timestamp_nanos_opt().unwrap() as u64;

    let source_id = match &args.source_id {
        Some(id) => id.clone(),
        None => "wikidata".to_string(),
    };

    let script_path = script_root_path.join( format!("{}/sources/{}", &args.test_id, &source_id));

    if overwrite && script_path.exists() {
        fs::remove_dir_all(&script_path).await?;
    }

    if !script_path.exists() {
        fs::create_dir_all(&script_path).await?;
    }

    let script_source_files = select_script_source_files(
        type_paths.clone(),
        start_datetime,
        end_datetime,
    ).await?;

    // Write the list of source files to the script folder for reference.
    let script_source_files_path = script_path.join("script_source_files.json");
    fs::write(&script_source_files_path, serde_json::to_string_pretty(&script_source_files)?).await?;

    // GENERATE BOOTSTRAP SCRIPTS
    // Create the bootstrap script files from script_source_files.bootstrap_script_files
    for (item_type, item_files) in script_source_files.bootstrap_script_files {
        log::trace!("Processing bootstrap script files for type: {:?}", item_type);

        // Create the Bootstrap Script Writer and write the header record.
        let bootstrap_script_path = script_path.join("bootstrap_scripts");
        let mut bootstrap_script_writer = BootstrapScriptWriter::new(
            BootstrapScriptWriterSettings {
                folder_path: bootstrap_script_path,
                script_name: item_type.as_label().to_string(),
                max_size: Some(1000),
        })?;
        let header = BootstrapScriptRecord::Header(BootstrapHeaderRecord {
            start_time: FixedOffset::east_opt(0).unwrap().from_utc_datetime(&start_datetime),
            description: format!("Drasi Bootstrap Data Script for TestID {}, SourceID: {}",args.test_id, source_id),
        });
        bootstrap_script_writer.write_record(&header)?;
    
        for (_, path) in item_files {
            log::trace!("Processing bootstrap script file: {:?}", path);

            let rec_create = create_bootstrap_data_record(item_type, path.clone()).await;
            match rec_create {
                Ok(record) => {
                    bootstrap_script_writer.write_record(&record)?;
                },
                Err(e) => {
                    log::error!("Error creating bootstrap data record from revision: {:?}, error: {:?}", path, e);
                }
            }
        }
    }

    // GENERATE CHANGE SCRIPTS
    // Create the Change Script Writer and write the header record.
    let mut change_script_writer = ChangeScriptWriter::new(
        ChangeScriptWriterSettings {
            folder_path: script_path.clone(),
            script_name: "source_change_scripts".to_string(),
            max_size: Some(1000)    
    })?;
    let header = ChangeScriptRecord::Header(ChangeHeaderRecord {
        start_time: FixedOffset::east_opt(0).unwrap().from_utc_datetime(&start_datetime),
        description: format!("Drasi Source Change Script for TestID {}, SourceID: {}",args.test_id, source_id),
    });
    change_script_writer.write_record(&header)?;

    let mut lsn: u64 = 0;
    let source_info = SourceChangeEventSourceInfo {
        db: source_id.clone(),
        table: "node".to_string(),
        ts_ms: 0,
        ts_sec: 0,
        lsn,
    };

    for (timestamp, item_type, path, previous) in script_source_files.change_script_files {
        log::trace!("Processing change script file: {:?}", path);

        if timestamp >= start_datetime && timestamp <= end_datetime {
            let ts_ns = timestamp.and_utc().timestamp_nanos_opt().unwrap_or_default() as u64;
            let source_info = SourceChangeEventSourceInfo {
                ts_ms: ts_ns / 1_000_000,
                ts_sec: ts_ns / 1_000_000_000,
                lsn,
                ..source_info.clone()
            };

            let (op, before_path, after_path) = match previous {
                Some((_, prev_path)) => {
                    ("u".to_string(), Some(prev_path.clone()), Some(path.clone()))
                },
                None => {
                    ("i".to_string(), None, Some(path.clone()))
                }
            }; 

            let rec_create = create_source_change_record(
                item_type, op, ts_ns - start_datetime_ns, source_info.ts_ms, source_info, before_path, after_path);

            match rec_create.await {
                Ok(record) => {
                    change_script_writer.write_record(&record)?;
                    lsn += 1;
                },
                Err(e) => {
                    log::error!("Error creating source change record from revision: {:?}, error: {:?}", path.clone(), e);
                }
            }
        };
    }

    Ok(())
}

fn parse_timestamp_from_filename(file_name: &str) -> anyhow::Result<NaiveDateTime, ParseError> {
    NaiveDateTime::parse_from_str(file_name.trim_end_matches(".json"), "%Y-%m-%d_%H-%M-%SZ")
}

pub async fn select_script_source_files( 
    item_type_paths: Vec<(ItemType, PathBuf)>, start_datetime: NaiveDateTime, end_datetime: NaiveDateTime,
) -> anyhow::Result<ScriptSourceFiles> {

    let mut bootstrap_script_files = HashMap::new();
    let mut change_script_files = Vec::new();

    // Loop through the item type folders.
    for (item_type, item_type_path) in item_type_paths {
        if !item_type_path.exists() || !item_type_path.is_dir() {
            continue;
        }

        let mut item_type_bootstrap_files: Vec<(NaiveDateTime, PathBuf)> = Vec::new();

        if let Ok(mut items) = fs::read_dir(item_type_path).await {
            while let Some(item) = items.next_entry().await? {
                let item_path = item.path();
                if !item_path.is_dir() {
                    continue;
                }

                let mut item_revision_files: Vec<(NaiveDateTime, PathBuf)> = Vec::new();

                let mut files = fs::read_dir(item_path).await?;
                while let Some(file) = files.next_entry().await? {
                    let file_path = file.path();
                    if let Some(file_name) = file_path.file_name().and_then(|f| f.to_str()) {
                        if file_name.ends_with(".json") {
                            item_revision_files.push((parse_timestamp_from_filename(file_name)?, file_path));
                        }
                    }
                }

                item_revision_files.sort_by_key(|(timestamp, _)| *timestamp);

                let mut selected_item_rev_files: Vec<(NaiveDateTime, ItemType, PathBuf, Option<(NaiveDateTime, PathBuf)>)> = Vec::new();
                let mut bootstrap_rev: Option<(NaiveDateTime, PathBuf)> = None;
                let mut prev_rev: Option<(NaiveDateTime, PathBuf)> = None;

                // Iterate through the possible item_revision_files and select only those that fall in the time range for inclusion
                // in the change_script. Handle identification of bootstrap revisions.
                for (timestamp, path) in item_revision_files {
                    if timestamp < start_datetime {
                        bootstrap_rev = Some((timestamp, path.clone()));
                        prev_rev = Some((timestamp, path));
                    } else if timestamp >= start_datetime && timestamp <= end_datetime {
                        selected_item_rev_files.push((timestamp, item_type, path.clone(), prev_rev));
                        prev_rev = Some((timestamp, path));
                    } else if timestamp > end_datetime {
                        break;
                    }
                }

                if let Some((timestamp, path)) = bootstrap_rev {
                    item_type_bootstrap_files.push((timestamp, path));
                }

                if selected_item_rev_files.len() > 1 {
                    change_script_files.extend(selected_item_rev_files.into_iter());
                }
            }
        }

        if item_type_bootstrap_files.len() > 0 {
            bootstrap_script_files.insert(item_type, item_type_bootstrap_files);
        };
    }

    change_script_files.sort_by_key(|(timestamp, _, _, _)| *timestamp);

    let result = ScriptSourceFiles {
        bootstrap_script_files,
        change_script_files,
        start_datetime,
        start_datetime_ns: start_datetime.and_utc().timestamp_nanos_opt().unwrap() as u64,
        end_datetime,
    };

    Ok(result)
}