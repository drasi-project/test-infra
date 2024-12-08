use std::collections::HashMap;
use std::path::PathBuf;
use chrono::{NaiveDateTime, ParseError};
use strum::IntoEnumIterator;
use tokio::fs;

use crate::MakeScriptCommandArgs;
use crate::wikidata::ItemType;

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

    let script_path = script_root_path.join(&args.test_id);

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
) -> anyhow::Result<Vec<(NaiveDateTime, PathBuf)>> {
    let mut result = Vec::new();

    for (_, type_path) in type_paths {
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
                                    valid_files.push((timestamp, file.path()));
                                }
                            }
                        }
                    }
                }

                // Sort files by timestamp and discard the oldest.
                valid_files.sort_by_key(|(timestamp, _)| *timestamp);
                if valid_files.len() > 1 {
                    result.extend(valid_files.into_iter().skip(1));
                }
            }
        }
    }

    // Sort the result by timestamp.
    result.sort_by_key(|(timestamp, _)| *timestamp);
    Ok(result)
}
