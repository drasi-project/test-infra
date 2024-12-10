use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{NaiveDateTime, ParseError};
use strum::IntoEnumIterator;
use test_data_store::test_repo_storage::scripts::change_script_file_writer::{ChangeScriptWriter, ChangeScriptWriterSettings};
use test_data_store::test_repo_storage::scripts::{ChangeScriptRecord, SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo, SourceChangeRecord};
use tokio::fs;

use crate::MakeScriptCommandArgs;
use crate::wikidata::{ItemRevisionFileContent, ItemType, extractors::parse_item_revision};

pub async fn generate_test_scripts(args: &MakeScriptCommandArgs, item_root_path: PathBuf, script_root_path: PathBuf, overwrite: bool) -> anyhow::Result<()> {
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

    let script_begin_datetime = args.begin_script.unwrap_or(start_datetime);
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

    let bootstrap_script_source_files = get_bootstrap_script_source_files(
        type_paths.clone(),
        start_datetime,
        end_datetime,
    ).await?;
    log::error!("Bootstrap script source files: {:#?}", bootstrap_script_source_files);

    let change_script_source_files = get_change_script_source_files(
        type_paths.clone(),
        start_datetime,
        end_datetime,
    ).await?;
    log::error!("Change script source files: {:#?}", change_script_source_files); 

    let change_script_path = script_path.join("source_change_scripts");
    fs::create_dir_all(&change_script_path).await?;

    let script_begin_ns = script_begin_datetime.and_utc().timestamp_nanos_opt().unwrap_or_default() as u64;
    let mut lsn = 0;

    let change_script_settings = ChangeScriptWriterSettings {
        folder_path: change_script_path.clone(),
        script_name: "change_log".to_string(),
        max_size: Some(100)    
    };

    let mut script_writer = ChangeScriptWriter::new(change_script_settings)?;

    for (timestamp, item_type, path) in change_script_source_files {
        // Read the revision from the file.
        let revision_file_str = fs::read_to_string(path).await?;
        let item_revision: ItemRevisionFileContent = serde_json::from_str(&revision_file_str)?;        

        let ts_ns = timestamp.and_utc().timestamp_nanos_opt().unwrap_or_default() as u64;
        let ts_ms = ts_ns / 1_000_000;
        let ts_sec = ts_ms / 1_000;

        let script_record = ChangeScriptRecord::SourceChange(SourceChangeRecord {
            offset_ns: ts_ns - script_begin_ns,
            source_change_event: SourceChangeEvent {
                op: "u".to_string(),
                ts_ms,
                schema: "".to_string(),
                payload: SourceChangeEventPayload {
                    source: SourceChangeEventSourceInfo {
                        db: source_id.clone(),
                        table: "node".to_string(),
                        ts_ms,
                        ts_sec,
                        lsn,
                    },
                    before: parse_item_revision(item_type, &item_revision)?,
                    after: serde_json::json!({}),
                },
            },
        });

        script_writer.write_record(&script_record)?;

        lsn += 1;
    }

    Ok(())
}

/// Parses the timestamp from a filename and returns a NaiveDateTime.
fn parse_timestamp_from_filename(file_name: &str) -> Result<NaiveDateTime, ParseError> {
    NaiveDateTime::parse_from_str(file_name.trim_end_matches(".json"), "%Y-%m-%d_%H-%M-%SZ")
}

/// Gets the bootstrap script files based on the requirements.
pub async fn get_bootstrap_script_source_files(
    type_paths: Vec<(ItemType, PathBuf)>,
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
) -> anyhow::Result<HashMap<ItemType, Vec<(NaiveDateTime, PathBuf)>>> {
    let mut result = HashMap::new();

    for (item_type, type_path) in type_paths {
        if !type_path.exists() || !type_path.is_dir() {
            continue;
        }

        let mut type_files = Vec::new();

        if let Ok(mut items) = fs::read_dir(type_path).await {
            while let Some(item) = items.next_entry().await? {
                let item_path = item.path();
                if !item_path.is_dir() {
                    continue;
                }

                // Find the oldest revision file within the date range.
                let mut oldest_file: Option<(NaiveDateTime, PathBuf)> = None;
                if let Ok(mut files) = fs::read_dir(item_path).await {
                    while let Some(file) = files.next_entry().await? {
                        if let Some(file_name) = file.file_name().to_str() {
                            if let Ok(timestamp) = parse_timestamp_from_filename(file_name) {
                                if timestamp >= start_datetime && timestamp <= end_datetime {
                                    match &oldest_file {
                                        Some((oldest_time, _)) if &timestamp < oldest_time => {
                                            oldest_file = Some((timestamp, file.path()))
                                        }
                                        None => oldest_file = Some((timestamp, file.path())),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }

                if let Some((timestamp, path)) = oldest_file {
                    type_files.push((timestamp, path));
                }
            }
        }

        if !type_files.is_empty() {
            result.insert(item_type, type_files);
        }
    }

    Ok(result)
}

/// Gets the change script files based on the requirements.
pub async fn get_change_script_source_files(
    type_paths: Vec<(ItemType, PathBuf)>,
    start_datetime: NaiveDateTime,
    end_datetime: NaiveDateTime,
) -> anyhow::Result<Vec<(NaiveDateTime, ItemType, PathBuf)>> {
    let mut result = Vec::new();

    for (item_type, type_path) in type_paths {
        if !type_path.exists() || !type_path.is_dir() {
            continue;
        }

        if let Ok(mut items) = fs::read_dir(type_path).await {
            while let Some(item) = items.next_entry().await? {
                let item_path = item.path();
                if !item_path.is_dir() {
                    continue;
                }

                let mut valid_files = Vec::new();

                if let Ok(mut files) = fs::read_dir(item_path).await {
                    while let Some(file) = files.next_entry().await? {
                        if let Some(file_name) = file.file_name().to_str() {
                            if let Ok(timestamp) = parse_timestamp_from_filename(file_name) {
                                if timestamp >= start_datetime && timestamp <= end_datetime {
                                    valid_files.push((timestamp, item_type, file.path()));
                                }
                            }
                        }
                    }
                }

                // Sort files by timestamp and discard the oldest.
                valid_files.sort_by_key(|(timestamp, _, _)| *timestamp);
                if valid_files.len() > 1 {
                    result.extend(valid_files.into_iter().skip(1));
                }
            }
        }
    }

    // Sort the result by timestamp.
    result.sort_by_key(|(timestamp, _, _)| *timestamp);
    Ok(result)
}
