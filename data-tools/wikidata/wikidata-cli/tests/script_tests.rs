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

use std::path::PathBuf;
use chrono::{NaiveDateTime, TimeZone, FixedOffset};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::fs;

use wikidata::{create_test_scripts, ItemType, ItemRevisionFileContent, MakeScriptCommandArgs};

// Helper function to create test revision data
fn create_test_revision(item_id: &str, item_type: ItemType, name: &str, population: i64, timestamp: &str) -> ItemRevisionFileContent {
    let content = json!({
        "type": "item",
        "id": item_id,
        "labels": {
            "en": {
                "language": "en",
                "value": name
            }
        },
        "claims": {
            "P31": [{
                "mainsnak": {
                    "snaktype": "value",
                    "property": "P31",
                    "datavalue": {
                        "value": {
                            "entity-type": "item",
                            "numeric-id": match item_type {
                                ItemType::City => 515,
                                ItemType::Country => 6256,
                                ItemType::Continent => 5107,
                            },
                            "id": match item_type {
                                ItemType::City => "Q515",
                                ItemType::Country => "Q6256",
                                ItemType::Continent => "Q5107",
                            }
                        },
                        "type": "wikibase-entityid"
                    }
                },
                "type": "statement",
                "rank": "normal"
            }],
            "P1082": [{
                "mainsnak": {
                    "snaktype": "value",
                    "property": "P1082",
                    "datavalue": {
                        "value": {
                            "amount": format!("+{}", population),
                            "unit": "1"
                        },
                        "type": "quantity"
                    }
                },
                "type": "statement",
                "rank": "normal"
            }],
            "P17": if item_type == ItemType::City {
                json!([{
                    "mainsnak": {
                        "snaktype": "value",
                        "property": "P17",
                        "datavalue": {
                            "value": {
                                "entity-type": "item",
                                "numeric-id": 145,
                                "id": "Q145"
                            },
                            "type": "wikibase-entityid"
                        }
                    },
                    "type": "statement",
                    "rank": "normal"
                }])
            } else {
                json!([])
            },
            "P625": [{
                "mainsnak": {
                    "snaktype": "value",
                    "property": "P625",
                    "datavalue": {
                        "value": {
                            "latitude": 51.507222222222,
                            "longitude": -0.1275,
                            "altitude": null,
                            "precision": 0.000277778,
                            "globe": "http://www.wikidata.org/entity/Q2"
                        },
                        "type": "globecoordinate"
                    }
                },
                "type": "statement",
                "rank": "normal"
            }],
            "P2046": [{
                "mainsnak": {
                    "snaktype": "value",
                    "property": "P2046",
                    "datavalue": {
                        "value": {
                            "amount": "+1572",
                            "unit": "http://www.wikidata.org/entity/Q712226"
                        },
                        "type": "quantity"
                    }
                },
                "type": "statement",
                "rank": "normal"
            }]
        }
    });
    
    ItemRevisionFileContent {
        item_id: item_id.to_string(),
        item_type,
        rev_id: 123456,
        parent_id: 123455,
        timestamp: timestamp.to_string(),
        user: Some("TestUser".to_string()),
        user_id: Some(123),
        comment: Some("Test revision".to_string()),
        content: Some(content),
    }
}

// Helper function to parse JSONL file
async fn parse_jsonl_file(path: PathBuf) -> anyhow::Result<Vec<Value>> {
    let content = fs::read_to_string(path).await?;
    let mut records = Vec::new();
    
    for line in content.lines() {
        if !line.trim().is_empty() {
            records.push(serde_json::from_str(line)?);
        }
    }
    
    Ok(records)
}

#[tokio::test]
async fn test_bootstrap_script_format() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create test data structure
    let city_dir = item_root.join("city").join("Q84");
    fs::create_dir_all(&city_dir).await?;
    
    // Write test revision BEFORE the start date (for bootstrap)
    let revision = create_test_revision("Q84", ItemType::City, "London", 8982000, "2022-12-01T12:00:00Z");
    let revision_path = city_dir.join("2022-12-01_12-00-00Z.json");
    fs::write(&revision_path, serde_json::to_string(&revision)?).await?;
    
    // Create test scripts
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City],
        rev_start: Some(NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata".to_string()),
        test_id: "test001".to_string(),
    };
    
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Verify bootstrap script exists
    let bootstrap_path = script_root
        .join("test001")
        .join("sources")
        .join("wikidata")
        .join("bootstrap_scripts");
    
    assert!(bootstrap_path.exists(), "Bootstrap scripts directory should exist");
    
    // Find and parse bootstrap files
    let city_bootstrap_dir = bootstrap_path.join("City");
    assert!(city_bootstrap_dir.exists(), "City bootstrap directory should exist");
    
    let mut bootstrap_files = fs::read_dir(&city_bootstrap_dir).await?;
    let mut found_city_bootstrap = false;
    
    while let Some(entry) = bootstrap_files.next_entry().await? {
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        
        if file_name_str.starts_with("City_") && file_name_str.ends_with(".jsonl") {
            found_city_bootstrap = true;
            let records = parse_jsonl_file(entry.path()).await?;
            
            // Verify header record
            assert!(!records.is_empty(), "Bootstrap file should not be empty");
            let header = &records[0];
            assert_eq!(header["kind"], "Header");
            assert!(header["start_time"].is_string());
            assert!(header["description"].as_str().unwrap().contains("test001"));
            
            // Verify node records
            let mut found_london = false;
            for record in &records[1..] {
                if record["kind"] == "Node" && record["id"] == "Q84" {
                    found_london = true;
                    
                    // Verify node structure
                    assert_eq!(record["labels"], json!(["City"]));
                    
                    let props = &record["properties"];
                    assert_eq!(props["name"], "London");
                    assert_eq!(props["population"], 8982000);
                    assert_eq!(props["country_id"], "Q145");
                    assert!(props["coordinate_location"].is_string());
                    assert_eq!(props["area"], 1572.0);
                }
            }
            
            assert!(found_london, "Should find London node in bootstrap script");
        }
    }
    
    assert!(found_city_bootstrap, "Should find City bootstrap file");
    
    Ok(())
}

#[tokio::test]
async fn test_change_script_format() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create test data structure for city with two revisions
    let city_dir = item_root.join("city").join("Q84");
    fs::create_dir_all(&city_dir).await?;
    
    // Write bootstrap revision (before start date)
    let revision0 = create_test_revision("Q84", ItemType::City, "London", 7500000, "2023-12-01T12:00:00Z");
    let revision0_path = city_dir.join("2023-12-01_12-00-00Z.json");
    fs::write(&revision0_path, serde_json::to_string(&revision0)?).await?;
    
    // Write initial revision
    let revision1 = create_test_revision("Q84", ItemType::City, "London", 8000000, "2024-01-01T12:00:00Z");
    let revision1_path = city_dir.join("2024-01-01_12-00-00Z.json");
    fs::write(&revision1_path, serde_json::to_string(&revision1)?).await?;
    
    // Write updated revision
    let revision2 = create_test_revision("Q84", ItemType::City, "London", 8982000, "2024-01-02T12:00:00Z");
    let revision2_path = city_dir.join("2024-01-02_12-00-00Z.json");
    fs::write(&revision2_path, serde_json::to_string(&revision2)?).await?;
    
    // Create test scripts
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City],
        rev_start: Some(NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata".to_string()),
        test_id: "test002".to_string(),
    };
    
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Verify change script exists
    let script_path = script_root
        .join("test002")
        .join("sources")
        .join("wikidata");
    
    let change_script_dir = script_path.join("source_change_scripts");
    assert!(change_script_dir.exists(), "Change scripts directory should exist");
    
    let mut script_files = fs::read_dir(&change_script_dir).await?;
    let mut found_change_script = false;
    
    while let Some(entry) = script_files.next_entry().await? {
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();
        
        if file_name_str.starts_with("source_change_scripts_") && file_name_str.ends_with(".jsonl") {
            found_change_script = true;
            let records = parse_jsonl_file(entry.path()).await?;
            
            // Verify header record
            assert!(!records.is_empty(), "Change script file should not be empty");
            let header = &records[0];
            assert_eq!(header["kind"], "Header");
            assert!(header["description"].as_str().unwrap().contains("test002"));
            
            // Verify source change records
            let mut found_update = false;
            for record in &records[1..] {
                if record["kind"] == "SourceChange" {
                    let source_change = &record["source_change_event"];
                    
                    // Verify required fields
                    assert!(source_change["op"].is_string());
                    assert!(source_change["reactivatorStart_ns"].is_u64());
                    assert!(source_change["reactivatorEnd_ns"].is_u64());
                    
                    let payload = &source_change["payload"];
                    let source = &payload["source"];
                    
                    // Verify source info
                    assert_eq!(source["db"], "wikidata");
                    assert_eq!(source["table"], "node");
                    assert!(source["ts_ns"].is_u64());
                    assert!(source["lsn"].is_u64());
                    
                    // Check for update operation
                    if source_change["op"] == "u" {
                        found_update = true;
                        
                        // Verify before and after states
                        let before = &payload["before"];
                        let after = &payload["after"];
                        
                        assert_eq!(before["id"], "Q84");
                        assert_eq!(after["id"], "Q84");
                        
                        // Population should have changed from bootstrap or previous revision
                        let before_pop = before["properties"]["population"].as_i64().unwrap();
                        let after_pop = after["properties"]["population"].as_i64().unwrap();
                        
                        // Either 7500000->8000000 or 8000000->8982000
                        assert!(
                            (before_pop == 7500000 && after_pop == 8000000) ||
                            (before_pop == 8000000 && after_pop == 8982000),
                            "Population change should be 7500000->8000000 or 8000000->8982000, got {}->{}",
                            before_pop, after_pop
                        );
                    }
                    
                    // Verify offset_ns is present
                    assert!(record["offset_ns"].is_u64());
                }
            }
            
            assert!(found_update, "Should find update operation in change script");
        }
    }
    
    assert!(found_change_script, "Should find change script file");
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_item_types() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create test data for city - bootstrap revision before start date
    let city_dir = item_root.join("city").join("Q84");
    fs::create_dir_all(&city_dir).await?;
    let city_revision = create_test_revision("Q84", ItemType::City, "London", 8982000, "2022-12-01T12:00:00Z");
    fs::write(city_dir.join("2022-12-01_12-00-00Z.json"), serde_json::to_string(&city_revision)?).await?;
    
    // Create test data for country - bootstrap revision before start date
    let country_dir = item_root.join("country").join("Q145");
    fs::create_dir_all(&country_dir).await?;
    let country_revision = create_test_revision("Q145", ItemType::Country, "United Kingdom", 67000000, "2022-12-01T12:00:00Z");
    fs::write(country_dir.join("2022-12-01_12-00-00Z.json"), serde_json::to_string(&country_revision)?).await?;
    
    // Create test scripts for both types
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City, ItemType::Country],
        rev_start: Some(NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata".to_string()),
        test_id: "test003".to_string(),
    };
    
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Verify bootstrap scripts for both types exist
    let bootstrap_path = script_root
        .join("test003")
        .join("sources")
        .join("wikidata")
        .join("bootstrap_scripts");
    
    let mut found_city = false;
    let mut found_country = false;
    
    // Check City bootstrap directory
    let city_bootstrap_dir = bootstrap_path.join("City");
    if city_bootstrap_dir.exists() {
        let mut city_files = fs::read_dir(&city_bootstrap_dir).await?;
        while let Some(entry) = city_files.next_entry().await? {
            let file_name_str = entry.file_name().to_string_lossy().to_string();
            if file_name_str.ends_with(".jsonl") {
                found_city = true;
                let records = parse_jsonl_file(entry.path()).await?;
                // Verify we have city records
                assert!(records.iter().any(|r| r["id"] == "Q84" && r["labels"] == json!(["City"])));
            }
        }
    }
    
    // Check Country bootstrap directory
    let country_bootstrap_dir = bootstrap_path.join("Country");
    if country_bootstrap_dir.exists() {
        let mut country_files = fs::read_dir(&country_bootstrap_dir).await?;
        while let Some(entry) = country_files.next_entry().await? {
            let file_name_str = entry.file_name().to_string_lossy().to_string();
            if file_name_str.ends_with(".jsonl") {
                found_country = true;
                let records = parse_jsonl_file(entry.path()).await?;
                // Verify we have country records
                assert!(records.iter().any(|r| r["id"] == "Q145" && r["labels"] == json!(["Country"])));
            }
        }
    }
    
    assert!(found_city, "Should find City bootstrap file");
    assert!(found_country, "Should find Country bootstrap file");
    
    Ok(())
}

#[tokio::test]
async fn test_script_file_organization() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create minimal test data - revision before start date for bootstrap
    let city_dir = item_root.join("city").join("Q84");
    fs::create_dir_all(&city_dir).await?;
    let revision = create_test_revision("Q84", ItemType::City, "London", 8982000, "2022-12-01T12:00:00Z");
    fs::write(city_dir.join("2022-12-01_12-00-00Z.json"), serde_json::to_string(&revision)?).await?;
    
    // Create test scripts
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City],
        rev_start: Some(NaiveDateTime::parse_from_str("2023-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata-test".to_string()),
        test_id: "test004".to_string(),
    };
    
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Verify directory structure
    let expected_base = script_root.join("test004").join("sources").join("wikidata-test");
    assert!(expected_base.exists(), "Base script directory should exist");
    
    let bootstrap_dir = expected_base.join("bootstrap_scripts");
    assert!(bootstrap_dir.exists(), "Bootstrap scripts directory should exist");
    
    // Verify script_source_files.json exists
    let source_files_path = expected_base.join("script_source_files.json");
    assert!(source_files_path.exists(), "script_source_files.json should exist");
    
    // Parse and verify script_source_files.json
    let source_files_content = fs::read_to_string(source_files_path).await?;
    let source_files: Value = serde_json::from_str(&source_files_content)?;
    
    assert!(source_files["start_datetime"].is_string());
    assert!(source_files["end_datetime"].is_string());
    assert!(source_files["start_datetime_ns"].is_u64());
    assert!(source_files["bootstrap_script_files"].is_object());
    assert!(source_files["change_script_files"].is_array());
    
    Ok(())
}

#[tokio::test]
async fn test_timestamp_handling() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create test data with specific timestamps
    let city_dir = item_root.join("city").join("Q84");
    fs::create_dir_all(&city_dir).await?;
    
    // Create bootstrap revision before start date
    let bootstrap_revision = create_test_revision("Q84", ItemType::City, "London", 8900000, "2023-12-15T14:30:45Z");
    fs::write(city_dir.join("2023-12-15_14-30-45Z.json"), serde_json::to_string(&bootstrap_revision)?).await?;
    
    // Create revision with specific timestamp within range
    let revision = create_test_revision("Q84", ItemType::City, "London", 8982000, "2024-06-15T14:30:45Z");
    fs::write(city_dir.join("2024-06-15_14-30-45Z.json"), serde_json::to_string(&revision)?).await?;
    
    // Create test scripts
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City],
        rev_start: Some(NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata".to_string()),
        test_id: "test005".to_string(),
    };
    
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Read and verify bootstrap script
    let bootstrap_path = script_root
        .join("test005")
        .join("sources")
        .join("wikidata")
        .join("bootstrap_scripts");
    
    let city_bootstrap_dir = bootstrap_path.join("City");
    if city_bootstrap_dir.exists() {
        let mut city_files = fs::read_dir(&city_bootstrap_dir).await?;
        while let Some(entry) = city_files.next_entry().await? {
            if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                let records = parse_jsonl_file(entry.path()).await?;
            
            // Verify header has proper timestamp format
            let header = &records[0];
            let start_time = header["start_time"].as_str().unwrap();
            
            // Should be a valid RFC3339 timestamp
            let parsed_time = chrono::DateTime::parse_from_rfc3339(start_time);
            assert!(parsed_time.is_ok(), "Header start_time should be valid RFC3339");
            
            // Verify it matches our configured start time
            let expected_start = FixedOffset::east_opt(0).unwrap()
                .from_utc_datetime(&NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S").unwrap());
            assert_eq!(parsed_time.unwrap(), expected_start);
            }
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_empty_data_handling() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let item_root = temp_dir.path().join("items");
    let script_root = temp_dir.path().join("scripts");
    
    // Create empty directory structure (no actual data files)
    fs::create_dir_all(item_root.join("city")).await?;
    
    // Create test scripts with no data
    let args = MakeScriptCommandArgs {
        item_types: vec![ItemType::City],
        rev_start: Some(NaiveDateTime::parse_from_str("2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")?),
        rev_end: Some(NaiveDateTime::parse_from_str("2024-12-31 23:59:59", "%Y-%m-%d %H:%M:%S")?),
        source_id: Some("wikidata".to_string()),
        test_id: "test006".to_string(),
    };
    
    // This should complete without errors even with no data
    create_test_scripts(&args, item_root, script_root.clone(), true).await?;
    
    // Verify the directory structure was created
    let expected_base = script_root.join("test006").join("sources").join("wikidata");
    assert!(expected_base.exists(), "Base script directory should exist even with no data");
    
    // Bootstrap directory might not exist if no data was found
    let _bootstrap_dir = expected_base.join("bootstrap_scripts");
    
    // script_source_files.json should exist
    let source_files_path = expected_base.join("script_source_files.json");
    assert!(source_files_path.exists(), "script_source_files.json should exist");
    
    let source_files_content = fs::read_to_string(source_files_path).await?;
    let source_files: Value = serde_json::from_str(&source_files_content)?;
    
    // Should have empty collections
    assert_eq!(source_files["bootstrap_script_files"].as_object().unwrap().len(), 0);
    assert_eq!(source_files["change_script_files"].as_array().unwrap().len(), 0);
    
    Ok(())
}