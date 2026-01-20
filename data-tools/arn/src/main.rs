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

use chrono::NaiveDateTime;
use clap::{Args, Parser, Subcommand};
use script::generate_test_scripts;

mod arn;
mod script;

/// String constant representing the default ARN data output folder path
const DEFAULT_OUTPUT_FOLDER_PATH: &str = "./arn_data";

#[derive(Parser)]
#[command(name = "ARN")]
#[command(about = "CLI for generating fictional Azure Resource Notification (ARN) test data", long_about = None)]
struct Params {
    /// The path for generated ARN test data
    #[arg(short = 'o', long = "output", env = "ARN_OUTPUT_PATH")]
    pub output_folder_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate ARN test scripts (bootstrap and source change scripts)
    Generate {
        #[command(flatten)]
        data_selection: GenerateCommandArgs,

        /// A flag to indicate whether existing files should be overwritten
        #[arg(short = 'w', long, default_value_t = false)]
        overwrite: bool,
    },
}

#[derive(Args, Debug)]
struct GenerateCommandArgs {
    /// The ID of the test scenario
    #[arg(short = 'i', long)]
    test_id: String,

    /// The source ID for the ARN events
    #[arg(short = 'd', long, default_value = "arn-events")]
    source_id: String,

    /// Number of management groups to generate
    #[arg(short = 'm', long, default_value_t = 10)]
    management_group_count: usize,

    /// Number of service groups to generate
    #[arg(short = 'g', long, default_value_t = 5)]
    service_group_count: usize,

    /// Number of relationship resources to generate
    #[arg(short = 'r', long, default_value_t = 20)]
    relationship_count: usize,

    /// Maximum depth of management group hierarchy
    #[arg(short = 'p', long, default_value_t = 4)]
    hierarchy_depth: usize,

    /// Start datetime for the test scenario
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 's', long)]
    start_time: Option<NaiveDateTime>,

    /// Number of change events to generate
    #[arg(short = 'c', long, default_value_t = 50)]
    change_count: usize,

    /// Duration in seconds over which to spread the change events
    #[arg(short = 'u', long, default_value_t = 3600)]
    change_duration_secs: u64,

    /// Tenant ID to use for resources (will be generated if not provided)
    #[arg(short = 't', long)]
    tenant_id: Option<String>,

    /// Notification format: full-payload, payload-less, batched-ids, batched-payloads
    #[arg(short = 'f', long, default_value = "full-payload")]
    notification_format: String,

    /// Batch size for batched notification formats
    #[arg(short = 'b', long, default_value_t = 3)]
    batch_size: usize,

    /// Subscription ID for batched notifications
    #[arg(long)]
    subscription_id: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let params = Params::parse();

    let output_folder_path = params
        .output_folder_path
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_FOLDER_PATH));

    let res = match params.command {
        Commands::Generate {
            data_selection,
            overwrite,
        } => handle_generate_command(data_selection, output_folder_path, overwrite).await,
    };

    match res {
        Ok(_) => {
            println!("Command completed successfully");
        }
        Err(e) => {
            eprintln!("arn command failed: {:?}", e);
        }
    }
}

async fn handle_generate_command(
    args: GenerateCommandArgs,
    output_folder_path: PathBuf,
    overwrite: bool,
) -> anyhow::Result<()> {
    log::info!("Generate command using {:?}", args);

    // Display a summary of what the command is going to do.
    println!("Generating ARN Test Scripts:");
    println!("  - test ID: {}", args.test_id);
    println!("  - source ID: {}", args.source_id);
    println!("  - management groups: {}", args.management_group_count);
    println!("  - service groups: {}", args.service_group_count);
    println!("  - relationships: {}", args.relationship_count);
    println!("  - hierarchy depth: {}", args.hierarchy_depth);
    println!("  - notification format: {}", args.notification_format);
    println!("  - batch size: {}", args.batch_size);
    println!("  - change events: {}", args.change_count);
    println!("  - change duration: {} seconds", args.change_duration_secs);
    println!("  - output folder: {:?}", output_folder_path);
    println!("  - overwrite: {}", overwrite);

    let script_root_path = output_folder_path.join("scripts");

    generate_test_scripts(&args, script_root_path, overwrite).await?;

    Ok(())
}
