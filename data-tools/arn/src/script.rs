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

use chrono::{FixedOffset, TimeZone, Utc};
use rand::Rng;
use test_data_store::scripts::bootstrap_script_file_writer::{
    BootstrapScriptWriter, BootstrapScriptWriterSettings,
};
use test_data_store::scripts::change_script_file_writer::{
    ChangeScriptWriter, ChangeScriptWriterSettings,
};
use test_data_store::scripts::{
    BootstrapHeaderRecord, BootstrapScriptRecord, ChangeHeaderRecord,
    ChangeScriptRecord, NodeRecord, SourceChangeEvent, SourceChangeEventPayload, SourceChangeEventSourceInfo,
    SourceChangeRecord,
};
use tokio::fs;
use uuid::Uuid;

use crate::arn::generator::{ArnDataGenerator, ChangeEvent, GeneratorConfig};
use crate::arn::models::{create_batched_event_grid_event, NotificationFormat};
use crate::GenerateCommandArgs;

pub async fn generate_test_scripts(
    args: &GenerateCommandArgs,
    script_root_path: PathBuf,
    overwrite: bool,
) -> anyhow::Result<()> {
    log::info!("Generating test scripts for test ID: {}", args.test_id);

    let script_path = script_root_path.join(format!("{}/sources/{}", args.test_id, args.source_id));

    if overwrite && script_path.exists() {
        fs::remove_dir_all(&script_path).await?;
    }

    if !script_path.exists() {
        fs::create_dir_all(&script_path).await?;
    }

    // Determine tenant ID
    let tenant_id = args
        .tenant_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Determine subscription ID for batched notifications
    let subscription_id = args
        .subscription_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    // Parse notification format
    let notification_format = match args.notification_format.as_str() {
        "full-payload" => NotificationFormat::FullPayload,
        "payload-less" => NotificationFormat::PayloadLess,
        "batched-ids" => NotificationFormat::BatchedIds,
        "batched-payloads" => NotificationFormat::BatchedPayloads,
        _ => {
            eprintln!("Invalid notification format: {}. Using full-payload.", args.notification_format);
            NotificationFormat::FullPayload
        }
    };

    // Determine start time
    let start_time_naive = args.start_time.unwrap_or_else(|| Utc::now().naive_utc());
    let start_time = start_time_naive.and_utc();

    // Create generator config
    let config = GeneratorConfig {
        tenant_id: tenant_id.clone(),
        management_group_count: args.management_group_count,
        service_group_count: args.service_group_count,
        relationship_count: args.relationship_count,
        hierarchy_depth: args.hierarchy_depth,
        start_time,
    };

    let mut generator = ArnDataGenerator::new(config);

    // Generate resources
    println!("Generating resources...");
    let management_groups = generator.generate_management_groups();
    let service_groups = generator.generate_service_groups();
    let relationships =
        generator.generate_relationships(&management_groups, &service_groups);

    println!("  - Generated {} management groups", management_groups.len());
    println!("  - Generated {} service groups", service_groups.len());
    println!("  - Generated {} relationships", relationships.len());

    // GENERATE BOOTSTRAP SCRIPT
    println!("Generating bootstrap script...");
    let bootstrap_script_path = script_path.join("bootstrap_scripts");

    let mut bootstrap_script_writer = BootstrapScriptWriter::new(BootstrapScriptWriterSettings {
        folder_path: bootstrap_script_path,
        script_name: "arn_resources".to_string(),
        max_size: Some(1000),
    })?;

    let header = BootstrapScriptRecord::Header(BootstrapHeaderRecord {
        start_time: FixedOffset::east_opt(0)
            .unwrap()
            .from_utc_datetime(&start_time_naive),
        description: format!(
            "Drasi Bootstrap Data Script for TestID {}, SourceID: {}",
            args.test_id, args.source_id
        ),
    });
    bootstrap_script_writer.write_record(&header)?;

    // Write management groups based on notification format
    match notification_format {
        NotificationFormat::BatchedIds | NotificationFormat::BatchedPayloads => {
            // For batched formats, group resources into batches
            for batch in management_groups.chunks(args.batch_size) {
                let event_grid_event = create_batched_event_grid_event(
                    batch.iter().collect(),
                    notification_format,
                    &subscription_id,
                );

                // Convert Event Grid event to Node record with special handling
                let record = BootstrapScriptRecord::Node(NodeRecord {
                    id: format!("event-grid/{}", event_grid_event.id),
                    labels: vec!["EventGridEvent".to_string()],
                    properties: serde_json::to_value(&event_grid_event)?,
                });
                bootstrap_script_writer.write_record(&record)?;
            }
        }
        NotificationFormat::FullPayload => {
            // Full payload - one Event Grid event per resource
            for mg in &management_groups {
                let event_grid_event = mg.to_event_grid_event_full("Create");
                let record = BootstrapScriptRecord::Node(NodeRecord {
                    id: format!("event-grid/{}", event_grid_event.id),
                    labels: vec!["EventGridEvent".to_string()],
                    properties: serde_json::to_value(&event_grid_event)?,
                });
                bootstrap_script_writer.write_record(&record)?;
            }
        }
        NotificationFormat::PayloadLess => {
            // Payload-less - one Event Grid event per resource with minimal data
            for mg in &management_groups {
                let event_grid_event = mg.to_event_grid_event_payloadless();
                let record = BootstrapScriptRecord::Node(NodeRecord {
                    id: format!("event-grid/{}", event_grid_event.id),
                    labels: vec!["EventGridEvent".to_string()],
                    properties: serde_json::to_value(&event_grid_event)?,
                });
                bootstrap_script_writer.write_record(&record)?;
            }
        }
    }

    // Write service groups
    for sg in &service_groups {
        let arn_resource = sg.to_arn_resource("Create");
        let record = BootstrapScriptRecord::Node(NodeRecord {
            id: arn_resource.id,
            labels: arn_resource.labels,
            properties: serde_json::to_value(&arn_resource.properties)?,
        });
        bootstrap_script_writer.write_record(&record)?;
    }

    // Write relationships
    for rel in &relationships {
        let arn_resource = rel.to_arn_resource("Create");
        let record = BootstrapScriptRecord::Node(NodeRecord {
            id: arn_resource.id,
            labels: arn_resource.labels,
            properties: serde_json::to_value(&arn_resource.properties)?,
        });
        bootstrap_script_writer.write_record(&record)?;
    }

    println!(
        "  - Bootstrap script written with {} resources",
        management_groups.len() + service_groups.len() + relationships.len()
    );

    // GENERATE CHANGE SCRIPT
    println!("Generating change script...");
    let mut change_script_writer = ChangeScriptWriter::new(ChangeScriptWriterSettings {
        folder_path: script_path.clone(),
        script_name: "source_change_scripts".to_string(),
        max_size: Some(1000),
    })?;

    let header = ChangeScriptRecord::Header(ChangeHeaderRecord {
        start_time: FixedOffset::east_opt(0)
            .unwrap()
            .from_utc_datetime(&start_time_naive),
        description: format!(
            "Drasi Source Change Script for TestID {}, SourceID: {}",
            args.test_id, args.source_id
        ),
    });
    change_script_writer.write_record(&header)?;

    // Generate change events
    let change_events = generator.generate_change_events(
        &management_groups,
        &service_groups,
        args.change_count,
        args.change_duration_secs,
    );

    println!("  - Generated {} change events", change_events.len());

    let start_datetime_ns = start_time.timestamp_nanos_opt().unwrap_or_default() as u64;
    let mut lsn: u64 = 0;
    let mut rng = rand::thread_rng();
    let mut current_offset_ns: u64 = 0;

    for event in change_events {
        // Add a random interval to the offset (between 100ms and 5s in nanoseconds)
        let interval_ns = rng.gen_range(100_000_000..5_000_000_000);
        current_offset_ns += interval_ns;

        match event {
            ChangeEvent::Insert { management_group } => {
                let arn_resource = management_group.to_arn_resource("Create");
                let ts_ns = start_datetime_ns + current_offset_ns;

                // Generate realistic reactivator timing (processing takes 1-10ms)
                let reactivator_start_ns = ts_ns + rng.gen_range(1_000_000..5_000_000);
                let reactivator_end_ns = reactivator_start_ns + rng.gen_range(1_000_000..10_000_000);

                let source_info = SourceChangeEventSourceInfo {
                    db: args.source_id.clone(),
                    table: "node".to_string(),
                    ts_ns,
                    lsn,
                };

                let record = ChangeScriptRecord::SourceChange(SourceChangeRecord {
                    offset_ns: current_offset_ns,
                    source_change_event: SourceChangeEvent {
                        op: "i".to_string(),
                        reactivator_start_ns,
                        reactivator_end_ns,
                        payload: SourceChangeEventPayload {
                            source: source_info,
                            before: serde_json::Value::Null,
                            after: serde_json::json!({
                                "id": arn_resource.id,
                                "labels": arn_resource.labels,
                                "properties": arn_resource.properties,
                            }),
                        },
                    },
                });

                change_script_writer.write_record(&record)?;
                lsn += 1;
            }
            ChangeEvent::Update {
                management_group,
                new_display_name,
                updated_at,
                updated_by,
            } => {
                let before_arn = management_group.to_arn_resource("Create");
                let after_arn =
                    management_group.to_updated_arn_resource(new_display_name, updated_at, updated_by);

                let ts_ns = start_datetime_ns + current_offset_ns;

                // Generate realistic reactivator timing (processing takes 1-10ms)
                let reactivator_start_ns = ts_ns + rng.gen_range(1_000_000..5_000_000);
                let reactivator_end_ns = reactivator_start_ns + rng.gen_range(1_000_000..10_000_000);

                let source_info = SourceChangeEventSourceInfo {
                    db: args.source_id.clone(),
                    table: "node".to_string(),
                    ts_ns,
                    lsn,
                };

                let record = ChangeScriptRecord::SourceChange(SourceChangeRecord {
                    offset_ns: current_offset_ns,
                    source_change_event: SourceChangeEvent {
                        op: "u".to_string(),
                        reactivator_start_ns,
                        reactivator_end_ns,
                        payload: SourceChangeEventPayload {
                            source: source_info,
                            before: serde_json::json!({
                                "id": before_arn.id,
                                "labels": before_arn.labels,
                                "properties": before_arn.properties,
                            }),
                            after: serde_json::json!({
                                "id": after_arn.id,
                                "labels": after_arn.labels,
                                "properties": after_arn.properties,
                            }),
                        },
                    },
                });

                change_script_writer.write_record(&record)?;
                lsn += 1;
            }
        }
    }

    println!("  - Change script written with {} events", lsn);

    println!("\nTest scripts generated successfully!");
    println!("  Location: {:?}", script_path);
    println!("  Tenant ID: {}", tenant_id);
    println!("  Start Time: {}", start_time_naive);

    Ok(())
}
