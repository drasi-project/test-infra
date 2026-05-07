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
use chrono::DateTime;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use gdelt::ScriptGenerator;

// Helper function to create test CSV data for events
fn create_test_event_csv() -> String {
    // GDELT event CSV has 61 fields
    vec![
        "20240101120000",  // 0. GlobalEventID
        "20240101",        // 1. Day
        "202401",          // 2. MonthYear
        "2024",            // 3. Year
        "2024.0027",       // 4. FractionDate
        "USA",             // 5. Actor1Code
        "United States",   // 6. Actor1Name
        "USA",             // 7. Actor1CountryCode
        "",                // 8. Actor1KnownGroupCode
        "",                // 9. Actor1EthnicCode
        "",                // 10. Actor1Religion1Code
        "",                // 11. Actor1Religion2Code
        "",                // 12. Actor1Type1Code
        "",                // 13. Actor1Type2Code
        "",                // 14. Actor1Type3Code
        "CHN",             // 15. Actor2Code
        "China",           // 16. Actor2Name
        "CHN",             // 17. Actor2CountryCode
        "",                // 18. Actor2KnownGroupCode
        "",                // 19. Actor2EthnicCode
        "",                // 20. Actor2Religion1Code
        "",                // 21. Actor2Religion2Code
        "",                // 22. Actor2Type1Code
        "",                // 23. Actor2Type2Code
        "",                // 24. Actor2Type3Code
        "1",               // 25. IsRootEvent
        "036",             // 26. EventCode
        "03",              // 27. EventBaseCode
        "036",             // 28. EventRootCode
        "3",               // 29. QuadClass
        "1.4",             // 30. GoldsteinScale
        "10",              // 31. NumMentions
        "5",               // 32. NumSources
        "15",              // 33. NumArticles
        "5.5",             // 34. AvgTone
        "1",               // 35. Actor1Geo_Type
        "US",              // 36. Actor1Geo_FullName
        "US",              // 37. Actor1Geo_CountryCode
        "",                // 38. Actor1Geo_ADM1Code
        "",                // 39. Actor1Geo_ADM2Code (missing)
        "38.9072",         // 40. Actor1Geo_Lat
        "-77.0369",        // 41. Actor1Geo_Long
        "531871",          // 42. Actor1Geo_FeatureID
        "1",               // 43. Actor2Geo_Type
        "CN",              // 44. Actor2Geo_FullName
        "CN",              // 45. Actor2Geo_CountryCode
        "",                // 46. Actor2Geo_ADM1Code
        "",                // 47. Actor2Geo_ADM2Code (missing)
        "39.9042",         // 48. Actor2Geo_Lat
        "116.4074",        // 49. Actor2Geo_Long
        "1816670",         // 50. Actor2Geo_FeatureID
        "1",               // 51. ActionGeo_Type
        "WA",              // 52. ActionGeo_FullName
        "US",              // 53. ActionGeo_CountryCode
        "US-WA",           // 54. ActionGeo_ADM1Code
        "",                // 55. ActionGeo_ADM2Code (missing)
        "38.9072",         // 56. ActionGeo_Lat
        "-77.0369",        // 57. ActionGeo_Long
        "531871",          // 58. ActionGeo_FeatureID
        "20240101120000",  // 59. DATEADDED
        "http://example.com/news/article1" // 60. SOURCEURL
    ].join("\t")
}

// Helper function to create test CSV data for graph/GKG
fn create_test_graph_csv() -> String {
    // Simplified GKG record
    vec![
        "20240101120000",  // DATE
        "1",               // NUMARTS
        "2",               // COUNTS
        "AFFECT,10;LEADER,5", // THEMES
        "Washington,United States,US,US-DC,38.9072,-77.0369,531871", // LOCATIONS
        "Barack Obama;Joe Biden", // PERSONS
        "United Nations;World Bank", // ORGANIZATIONS
        "5.5",             // TONE
        "",                // CAMEOEVENTIDS
        "http://example.com/news/article1", // SOURCES
        ""                 // SOURCEURLS
    ].join("\t")
}

// Helper function to create test CSV data for mentions
fn create_test_mention_csv() -> String {
    vec![
        "20240101120000",  // GlobalEventID
        "20240101120000",  // EventTimeDate
        "20240101120000-1", // MentionTimeDate
        "1",               // MentionType
        "http://example.com/news/article1", // MentionSourceName
        "Example News",    // MentionIdentifier
        "150",             // SentenceID
        "1",               // Actor1CharOffset
        "15",              // Actor2CharOffset
        "30",              // ActionCharOffset
        "1",               // InRawText
        "80",              // Confidence
        "5",               // MentionDocLen
        "3.5",             // MentionDocTone
        ""                 // MentionDocTranslationInfo
    ].join("\t")
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
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create event directory and file
    let event_dir = cache_dir.join("event");
    fs::create_dir_all(&event_dir).await?;
    
    let event_file = event_dir.join("20240101000000.export.CSV");
    let mut file = fs::File::create(&event_file).await?;
    file.write_all(create_test_event_csv().as_bytes()).await?;
    file.sync_all().await?;
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate bootstrap scripts
    generator.generate_bootstrap_scripts(vec![event_file], "event").await?;
    
    // Verify bootstrap script exists
    let bootstrap_path = output_dir.join("bootstrap_scripts");
    assert!(bootstrap_path.exists(), "Bootstrap scripts directory should exist");
    
    // Check for header file
    let header_file = bootstrap_path.join("event_header.jsonl");
    assert!(header_file.exists(), "Event header file should exist");
    
    let header_records = parse_jsonl_file(header_file).await?;
    assert_eq!(header_records.len(), 1);
    let header = &header_records[0];
    assert_eq!(header["kind"], "Header");
    assert!(header["start_time"].is_string());
    assert!(header["description"].as_str().unwrap().contains("GDELT event bootstrap"));
    
    // Check for data file
    let data_file = bootstrap_path.join("event_00000.jsonl");
    assert!(data_file.exists(), "Event data file should exist");
    
    let data_records = parse_jsonl_file(data_file).await?;
    assert!(!data_records.is_empty(), "Data file should contain records");
    
    // Verify node record structure
    let node = &data_records[0];
    assert_eq!(node["kind"], "Node");
    assert_eq!(node["id"], "20240101120000");
    assert_eq!(node["labels"], json!(["Event"]));
    
    let props = &node["properties"];
    assert!(props["GlobalEventID"].is_string());
    assert!(props["EventCode"].is_string());
    assert!(props["GoldsteinScale"].is_string());
    assert!(props["AvgTone"].is_string());
    
    // Check for finish file
    let finish_file = bootstrap_path.join("event_finish.jsonl");
    assert!(finish_file.exists(), "Event finish file should exist");
    
    let finish_records = parse_jsonl_file(finish_file).await?;
    assert_eq!(finish_records.len(), 1);
    assert_eq!(finish_records[0]["kind"], "Finish");
    
    Ok(())
}

#[tokio::test]
async fn test_change_script_format() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create event directory and files
    let event_dir = cache_dir.join("event");
    fs::create_dir_all(&event_dir).await?;
    
    // Create two event files to simulate changes
    let event_file1 = event_dir.join("20240101000000.export.CSV");
    let mut file1 = fs::File::create(&event_file1).await?;
    file1.write_all(create_test_event_csv().as_bytes()).await?;
    file1.sync_all().await?;
    
    let event_file2 = event_dir.join("20240101001500.export.CSV");
    let mut file2 = fs::File::create(&event_file2).await?;
    // Modify the CSV slightly to simulate an update
    let mut csv2 = create_test_event_csv();
    csv2 = csv2.replace("20240101120000", "20240101121500");
    file2.write_all(csv2.as_bytes()).await?;
    file2.sync_all().await?;
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate change scripts
    let start_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")?;
    generator.generate_change_scripts(vec![event_file1, event_file2], "event", start_time).await?;
    
    // Verify change script exists
    let change_file = output_dir.join("source_change_scripts_00000.jsonl");
    assert!(change_file.exists(), "Change script file should exist");
    
    let records = parse_jsonl_file(change_file).await?;
    assert!(!records.is_empty(), "Change script should contain records");
    
    // Verify header record
    let header = &records[0];
    assert_eq!(header["kind"], "Header");
    assert!(header["start_time"].is_string());
    assert!(header["description"].as_str().unwrap().contains("GDELT event changes"));
    
    // Verify source change records
    let mut found_change = false;
    for record in &records[1..] {
        if record["kind"] == "SourceChange" {
            found_change = true;
            
            // Verify structure
            assert!(record["offset_ns"].is_u64());
            
            let source_change = &record["source_change_event"];
            assert!(source_change["op"].is_string());
            assert!(source_change["reactivatorStart_ns"].is_u64());
            assert!(source_change["reactivatorEnd_ns"].is_u64());
            
            let payload = &source_change["payload"];
            let source = &payload["source"];
            
            assert_eq!(source["db"], "gdelt");
            assert_eq!(source["table"], "event");
            assert!(source["ts_ns"].is_u64());
            assert!(source["lsn"].is_u64());
            
            // Verify before/after structure
            assert!(payload["before"].is_object() || payload["before"].is_null());
            assert!(payload["after"].is_object());
        }
    }
    
    assert!(found_change, "Should find at least one change record");
    
    Ok(())
}

#[tokio::test]
async fn test_multiple_data_types() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create directories for each data type
    let event_dir = cache_dir.join("event");
    let graph_dir = cache_dir.join("graph");
    let mention_dir = cache_dir.join("mention");
    
    fs::create_dir_all(&event_dir).await?;
    fs::create_dir_all(&graph_dir).await?;
    fs::create_dir_all(&mention_dir).await?;
    
    // Create test files
    let event_file = event_dir.join("20240101000000.export.CSV");
    let mut file = fs::File::create(&event_file).await?;
    file.write_all(create_test_event_csv().as_bytes()).await?;
    
    let graph_file = graph_dir.join("20240101000000.gkg.CSV");
    let mut file = fs::File::create(&graph_file).await?;
    file.write_all(create_test_graph_csv().as_bytes()).await?;
    
    let mention_file = mention_dir.join("20240101000000.mentions.CSV");
    let mut file = fs::File::create(&mention_file).await?;
    file.write_all(create_test_mention_csv().as_bytes()).await?;
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate bootstrap scripts for each type
    generator.generate_bootstrap_scripts(vec![event_file], "event").await?;
    generator.generate_bootstrap_scripts(vec![graph_file], "graph").await?;
    generator.generate_bootstrap_scripts(vec![mention_file], "mention").await?;
    
    // Verify all bootstrap scripts exist
    let bootstrap_path = output_dir.join("bootstrap_scripts");
    
    for data_type in ["event", "graph", "mention"] {
        let header_file = bootstrap_path.join(format!("{}_header.jsonl", data_type));
        assert!(header_file.exists(), "{} header file should exist", data_type);
        
        let data_file = bootstrap_path.join(format!("{}_00000.jsonl", data_type));
        assert!(data_file.exists(), "{} data file should exist", data_type);
        
        let finish_file = bootstrap_path.join(format!("{}_finish.jsonl", data_type));
        assert!(finish_file.exists(), "{} finish file should exist", data_type);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_timestamp_handling() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create event directory and file
    let event_dir = cache_dir.join("event");
    fs::create_dir_all(&event_dir).await?;
    
    // Create file with specific timestamp
    let event_file = event_dir.join("20240615143045.export.CSV");
    let mut csv = create_test_event_csv();
    csv = csv.replace("20240101120000", "20240615143045");
    
    let mut file = fs::File::create(&event_file).await?;
    file.write_all(csv.as_bytes()).await?;
    file.sync_all().await?;
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate change scripts with specific start time
    let start_time = DateTime::parse_from_rfc3339("2024-06-15T00:00:00Z")?;
    generator.generate_change_scripts(vec![event_file], "event", start_time).await?;
    
    // Verify change script
    let change_file = output_dir.join("source_change_scripts_00000.jsonl");
    
    // Check if file exists
    if !change_file.exists() {
        anyhow::bail!("Change script file does not exist: {:?}", change_file);
    }
    
    let records = parse_jsonl_file(change_file).await?;
    
    if records.is_empty() {
        anyhow::bail!("No records found in change script file");
    }
    
    // Verify header timestamp
    let header = &records[0];
    let header_time = header["start_time"].as_str().unwrap();
    let parsed_time = DateTime::parse_from_rfc3339(header_time)?;
    assert_eq!(parsed_time, start_time);
    
    // Verify offset calculation
    for record in &records[1..] {
        if record["kind"] == "SourceChange" {
            let offset_ns = record["offset_ns"].as_u64().unwrap();
            
            // Offset should be the difference between event time and start time
            // 14:30:45 - 00:00:00 = 14h 30m 45s = 52245 seconds
            let expected_offset_ns = 52245 * 1_000_000_000u64;
            assert_eq!(offset_ns, expected_offset_ns);
            
            break;
        }
    }
    
    Ok(())
}

#[tokio::test]
async fn test_script_file_organization() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create event directory and multiple files
    let event_dir = cache_dir.join("event");
    fs::create_dir_all(&event_dir).await?;
    
    // Create multiple event files to test file splitting
    let mut files = Vec::new();
    for i in 0..4 {  // Changed from 5 to 4 to avoid 60 minutes
        let filename = format!("2024010100{:02}00.export.CSV", i * 15);
        let event_file = event_dir.join(&filename);
        let mut file = fs::File::create(&event_file).await?;
        file.write_all(create_test_event_csv().as_bytes()).await?;
        files.push(event_file);
    }
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate bootstrap scripts
    generator.generate_bootstrap_scripts(files.clone(), "event").await?;
    
    // Verify file structure
    let bootstrap_path = output_dir.join("bootstrap_scripts");
    assert!(bootstrap_path.exists());
    assert!(bootstrap_path.is_dir());
    
    // Generate change scripts
    let start_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")?;
    generator.generate_change_scripts(files, "event", start_time).await?;
    
    // Verify change script files
    let change_file = output_dir.join("source_change_scripts_00000.jsonl");
    assert!(change_file.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_empty_data_handling() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let output_dir = temp_dir.path().join("output");
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Try to generate scripts with empty file list
    let result = generator.generate_bootstrap_scripts(vec![], "event").await;
    
    // Should complete without error
    assert!(result.is_ok());
    
    // Bootstrap directory should be created even with no data
    let bootstrap_path = output_dir.join("bootstrap_scripts");
    assert!(bootstrap_path.exists());
    
    Ok(())
}

#[tokio::test]
async fn test_gdelt_csv_parsing() -> anyhow::Result<()> {
    // Create temporary directories
    let temp_dir = TempDir::new()?;
    let cache_dir = temp_dir.path().join("cache");
    let output_dir = temp_dir.path().join("output");
    
    // Create event directory and file with edge cases
    let event_dir = cache_dir.join("event");
    fs::create_dir_all(&event_dir).await?;
    
    // Create CSV with special characters and empty fields
    let special_csv = vec![
        "20240101120000",  // GlobalEventID
        "20240101",        // Day
        "202401",          // MonthYear
        "2024",            // Year
        "2024.0027",       // FractionDate
        "USA",             // Actor1Code
        "United \"States\"", // Actor1Name with quotes
        "USA",             // Actor1CountryCode
        "",                // Empty fields
        "",                
        "",                
        "",                
        "",                
        "",                
        "",                
        "CHN",             
        "People's Republic of China", // Name with apostrophe
        "CHN",             
        "",                
        "",                
        "",                
        "",                
        "",                
        "",                
        "",                
        "1",               
        "036",             
        "03",              
        "036",             
        "3",               
        "-2.5",            // Negative GoldsteinScale
        "10",              
        "5",               
        "15",              
        "-10.5",           // Negative AvgTone
        "1",               
        "US",              
        "US",              
        "",                
        "",                // ADM2Code
        "38.9072",         
        "-77.0369",        
        "531871",          
        "1",               
        "CN",              
        "CN",              
        "",                
        "",                // ADM2Code
        "39.9042",         
        "116.4074",        
        "1816670",         
        "1",               
        "WA",              
        "US",              
        "US-WA",           
        "",                // ADM2Code
        "38.9072",         
        "-77.0369",        
        "531871",          
        "20240101120000",  
        "http://example.com/news/article-with-special?param=value&other=123"
    ].join("\t");
    
    let event_file = event_dir.join("20240101000000.export.CSV");
    let mut file = fs::File::create(&event_file).await?;
    file.write_all(special_csv.as_bytes()).await?;
    
    // Create script generator
    let generator = ScriptGenerator::new(output_dir.clone());
    
    // Generate bootstrap scripts
    generator.generate_bootstrap_scripts(vec![event_file], "event").await?;
    
    // Parse and verify the generated script handles special characters correctly
    let data_file = output_dir.join("bootstrap_scripts").join("event_00000.jsonl");
    let records = parse_jsonl_file(data_file).await?;
    
    assert!(!records.is_empty());
    let node = &records[0];
    
    // Verify special characters are preserved
    let props = &node["properties"];
    assert!(props["Actor1Name"].as_str().unwrap().contains("\"States\""));
    assert!(props["Actor2Name"].as_str().unwrap().contains("'s Republic"));
    assert_eq!(props["GoldsteinScale"], "-2.5");
    assert_eq!(props["AvgTone"], "-10.5");
    
    Ok(())
}