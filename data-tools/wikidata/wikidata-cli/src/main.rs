use std::path::PathBuf;

use chrono::NaiveDateTime;
use clap::{Args, Parser, Subcommand};
use script::generate_test_scripts;
use wikidata::{download_item_list, download_item_type, ItemListQueryArgs, ItemType, ItemTypeQueryArgs };

mod script;
mod wikidata;

/// String constant representing the default WikiData data cache folder path
/// This is the folder where WikiData data is downloaded and stored if not provided by the user.
const DEFAULT_CACHE_FOLDER_PATH: &str = "./wiki_data_cache";

#[derive(Parser)]
#[command(name = "WIKIDATA")]
#[command(about = "CLI for working with WikiData data", long_about = None)]
struct Params {
    /// The path of the WikiData data cache
    #[arg(short = 'c', long = "cache", env = "WIKIDATA_CACHE_PATH")]
    pub cache_folder_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Downloads WikiData Items based on Item Type and store them in the local cache.
    GetTypes {
        #[command(flatten)]
        data_selection: GetTypeCommandArgs,

        /// A flag to indicate whether existing data should be overwritten
        #[arg(short = 'o', long, default_value_t = false)]
        overwrite: bool,

        /// A flag to indicate whether scripts should be automatically generated from the downloaded data
        #[arg(short = 'g', long, default_value_t = false)]
        script: bool,
    },
    /// Downloads WikiData Item and stores them in the local cache.
    GetItems {
        #[command(flatten)]
        data_selection: GetItemCommandArgs,

        /// A flag to indicate whether existing data should be overwritten
        #[arg(short = 'o', long, default_value_t = false)]
        overwrite: bool,

        /// A flag to indicate whether scripts should be automatically generated from the downloaded data
        #[arg(short = 'g', long, default_value_t = false)]
        script: bool,
    },    
    /// Generate test scripts from the downloaded item data.
    MakeScript {
        #[command(flatten)]
        data_selection: MakeScriptCommandArgs,

        /// A flag to indicate whether existing files should be overwritten
        #[arg(short = 'o', long, default_value_t = false)]
        overwrite: bool,
    },
}

#[derive(Args, Debug)]
struct GetTypeCommandArgs {
    /// The types of WikiData Items to get
    #[arg(short = 't', long, value_enum, default_value = "city", value_delimiter=',')]
    item_types: Vec<ItemType>,

    /// The maximum number of Item Revisions to get
    /// If not provided, all files will be processed
    #[arg(short = 'c', long)]
    rev_count: Option<usize>,

    /// The datetime to stop getting Item revisions.
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 'e', long)]
    rev_end: Option<NaiveDateTime>,

    /// The datetime from which to start getting Item revisions.
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 's', long)]
    rev_start: Option<NaiveDateTime>,
}

impl GetTypeCommandArgs {
    fn get_type_query_args(&self, parent_path: PathBuf, overwrite: bool) -> anyhow::Result<Vec<ItemTypeQueryArgs>> {

        let rev_count = match (self.rev_start, self.rev_end) {
            // For a date range, the default is to retrieve all revisions; we use a large number,
            // otherwise the default is to retrieve only 1 revision.
            (Some(_), Some(_)) => self.rev_count.unwrap_or(9999999),
            _ => self.rev_count.unwrap_or(1),
        };

        let mut queries = Vec::new();

        for item_type in &self.item_types {
            queries.push(ItemTypeQueryArgs {
                folder_path: parent_path.join(item_type.to_string()),
                item_type: item_type.clone(),
                overwrite,
                rev_count,
                rev_end: self.rev_end.clone(),
                rev_start: self.rev_start.clone()
            });
        }
        Ok(queries)
    }
}

#[derive(Args, Debug)]
struct GetItemCommandArgs {
    /// The Item Ids to get
    #[arg(short = 'i', long, value_enum, default_value = "Q84", value_delimiter=',')]
    item_ids: Vec<String>,

    /// The type of WikiData Item to get
    #[arg(short = 't', long, value_enum, default_value = "city")]
    item_type: ItemType,

    /// The maximum number of Item Revisions to get
    /// If not provided, all files will be processed
    #[arg(short = 'c', long)]
    rev_count: Option<usize>,

    /// The datetime to stop getting Item revisions.
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 'e', long)]
    rev_end: Option<NaiveDateTime>,

    /// The datetime from which to start getting Item revisions.
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 's', long)]
    rev_start: Option<NaiveDateTime>,
}

impl GetItemCommandArgs {
    fn get_item_list_query_args(&self, parent_path: PathBuf, overwrite: bool) -> anyhow::Result<ItemListQueryArgs> {

        let rev_count = match (self.rev_start, self.rev_end) {
            // For a date range, the default is to retrieve all revisions; w use a large number,
            // otherwise the default is to retrieve only 1 revision.
            (Some(_), Some(_)) => self.rev_count.unwrap_or(9999999),
            _ => self.rev_count.unwrap_or(1),
        };

        Ok(ItemListQueryArgs {
            folder_path: parent_path.join(self.item_type.to_string()),
            item_type: self.item_type,
            item_ids: self.item_ids.clone(),
            overwrite,
            rev_count,
            rev_end: self.rev_end.clone(),
            rev_start: self.rev_start.clone()
        })
    }
}

#[derive(Args, Debug)]
struct MakeScriptCommandArgs {
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 'b', long)]
    begin_script: Option<NaiveDateTime>,

    /// The types of WikiData Items to script
    #[arg(short = 't', long, value_enum, value_delimiter=',')]
    item_types: Vec<ItemType>,
    
    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 'e', long)]
    rev_end: Option<NaiveDateTime>,

    /// Supported formats are here https://docs.rs/chrono/latest/chrono/naive/struct.NaiveDateTime.html#method.parse_from_str
    #[arg(short = 's', long)]
    rev_start: Option<NaiveDateTime>,

    #[arg(short = 'd', long)]
    source_id: Option<String>,
    
    /// The ID of the test to generate scipts for.
    #[arg(short = 'i', long, )]
    test_id: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let params = Params::parse();

    let cache_folder_path = params.cache_folder_path.unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE_FOLDER_PATH));

    let res = match params.command {
        Commands::GetTypes { data_selection, overwrite, script } => {
            handle_get_types_command(data_selection, cache_folder_path, overwrite, script).await
        },
        Commands::GetItems { data_selection, overwrite, script } => {
            handle_get_items_command(data_selection, cache_folder_path, overwrite, script).await
        }
        Commands::MakeScript { data_selection, overwrite } => {
            handle_make_script_command(data_selection, cache_folder_path, overwrite).await
        }
    };

    match res {
        Ok(_) => {
            println!("Command completed successfully");
        }
        Err(e) => {
            eprintln!("wikidata command failed: {:?}", e);
        }
    }
}

async fn handle_get_types_command(args: GetTypeCommandArgs, cache_folder_path: PathBuf, overwrite: bool, script: bool ) -> anyhow::Result<()> {
    log::info!("Get Types command using {:?}", args);

    // Display a summary of what the command is going to do.
    println!("Getting WikiData Types:");
    println!("  - date range: {:?} to {:?}", &args.rev_start, &args.rev_end);
    println!("  - item types: {:?}", args.item_types);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);
    println!("  - script: {}", script);

    let item_folder = cache_folder_path.join("items");

    // For each ItemType, download the Item Revisions based on the criteria provided.
    let query_args = args.get_type_query_args(item_folder, overwrite)?;
    for query_arg in query_args {
        download_item_type(&query_arg).await?;
    };

    // let downloads = create_gdelt_file_list(
    //     start_datetime, 
    //     end_datetime,
    //     data_selection.data_type.iter().cloned().collect(),
    //     cache_folder_path,
    //     overwrite,
    // ).unwrap();

    // Display the list of files to be downloaded, without taking ownership of the list or content
    // println!("Download Tasks:");
    // for file_info in &downloads {
    //     println!("  - {:?}", file_info);
    // }

    // Download the files
    // let download_results = download_item_revisions(downloads).await.unwrap();

    // println!("File download results:");
    // for file_info in &download_results {
    //     println!("  - {:?}", file_info);
    // }

    // Unzip the files if the unzip flag is set
    // if unzip {
    //     let unzip_results = unzip_gdelt_files(download_results).await.unwrap();

        // println!("File unzip results:");
        // for file_info in &unzip_results {
        //     println!("  - {:?}", file_info);
        // }

        // summarize_fileinfo_results(unzip_results);

    // } else {
        // summarize_fileinfo_results(download_results);
    // }

    Ok(())
}

async fn handle_get_items_command(args: GetItemCommandArgs, cache_folder_path: PathBuf, overwrite: bool, script: bool ) -> anyhow::Result<()> {
    log::info!("Get Items command using {:?}", args);

    // Display a summary of what the command is going to do.
    println!("Getting WikiData Items:");
    println!("  - date range: {:?} to {:?}", &args.rev_start, &args.rev_end);
    println!("  - item type: {:?}", args.item_type);
    println!("  - item ids: {:?}", args.item_ids);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);
    println!("  - script: {}", script);

    let item_folder = cache_folder_path.join("items");

    // For each Item, download the Item Revisions based on the criteria provided.
    let query_args = args.get_item_list_query_args(item_folder, overwrite)?;
    download_item_list(&query_args).await?;

    Ok(())
}

async fn handle_make_script_command(args: MakeScriptCommandArgs, cache_folder_path: PathBuf, overwrite: bool) -> anyhow::Result<()> {
    log::info!("Make Script command using {:?}", args);

    // Display a summary of what the command is going to do.
    println!("Scripting WikiData Types:");
    println!("  - test ID: {:?}", &args.test_id);
    println!("  - source ID: {:?}", &args.source_id);
    println!("  - begin script: {:?}", &args.begin_script);
    println!("  - date range: {:?} to {:?}", &args.rev_start, &args.rev_end);
    println!("  - item types: {:?}", &args.item_types);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);

    let item_root_path = cache_folder_path.join("items");
    let script_root_path = cache_folder_path.join("scripts");

    generate_test_scripts(&args, item_root_path, script_root_path, overwrite).await?;

    Ok(())
}