use std::{cmp, path::PathBuf};

use chrono::NaiveDateTime;
use clap::{Args, Parser, Subcommand};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use tokio::{fs, io::AsyncWriteExt};
use wikidata::{ApiResponse, Continuation, ItemRevisionFileContent, ItemType, };

mod wikidata;
// mod postgres;

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
    // /// Extracts the text data files from the zip file in the local file cache
    // Unzip {
    //     #[command(flatten)]
    //     data_selection: DataSelectionArgs,

    //     /// A flag to indicate whether existing files should be overwritten
    //     #[arg(short = 'o', long, default_value_t = false)]
    //     overwrite: bool,
    // },
    // /// Loads GDELT data from the local file cache into a database.
    // Load {
    //     #[command(flatten)]
    //     data_selection: DataSelectionArgs,

    //     #[arg(short = 'd', long = "db_url", env = "GDELT_DB_URL")]
    //     database_url: Option<String>,

    //     #[arg(short = 'u', long = "db_user", env = "GDELT_DB_USER")]
    //     database_user: Option<String>,

    //     #[arg(short = 'p', long = "db_password", env = "GDELT_DB_PASSWORD")]
    //     database_password: Option<String>,
    // },
    // InitDB {
    //     #[arg(short = 'd', long = "db_url", env = "GDELT_DB_URL")]
    //     database_url: Option<String>,

    //     #[arg(short = 'u', long = "db_user", env = "GDELT_DB_USER")]
    //     database_user: Option<String>,

    //     #[arg(short = 'p', long = "db_password", env = "GDELT_DB_PASSWORD")]
    //     database_password: Option<String>,

    //     /// A flag to indicate whether existing tables should be overwritten
    //     #[arg(short = 'o', long, default_value_t = false)]
    //     overwrite: bool,        
    // },
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

    // fn get_item_revs_query_args(&self, parent_path: PathBuf, overwrite: bool) -> Vec<ItemRevsQueryArgs> {

    //     let mut queries = Vec::new();

    //     for item_id in &self.item_ids {
    //         queries.push(ItemRevsQueryArgs {
    //             folder_path: parent_path.join(item_id.to_string()),
    //             item_type: self.item_type,
    //             item_id: item_id.clone(),
    //             overwrite,
    //             rev_count: self.rev_count,
    //             rev_end: self.rev_end.clone(),
    //             rev_start: self.rev_start.clone()
    //         });
    //     }
    //     queries
    // }
}

#[derive(Debug)]
struct ItemTypeQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub overwrite: bool,
    pub rev_count: usize,
    pub rev_end: Option<NaiveDateTime>,
    pub rev_start: Option<NaiveDateTime>,
}

#[derive(Debug)]
struct ItemListQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub item_ids: Vec<String>,
    pub overwrite: bool,
    pub rev_count: usize,
    pub rev_end: Option<NaiveDateTime>,
    pub rev_start: Option<NaiveDateTime>,
}

#[derive(Debug)]
struct ItemRevsQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub item_id: String,
    pub overwrite: bool,
    pub rev_count: usize,
    pub rev_end: Option<NaiveDateTime>,
    pub rev_start: Option<NaiveDateTime>,
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
        // Commands::Unzip { data_selection, overwrite } => {
        //     handle_unzip_command(data_selection, cache_folder_path, overwrite).await
        // }
        // Commands::Load { data_selection, database_url, database_user, database_password } => {
        //     handle_load_command(data_selection, cache_folder_path, database_url, database_user, database_password).await
        // },
        // Commands::InitDB { database_url, database_user, database_password , overwrite} => {
        //     handle_initdb_command(database_url, database_user, database_password, overwrite).await
        // }
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
    log::info!("GetTypes command using {:?}", args);

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
    log::info!("GetItems command using {:?}", args);

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

async fn download_item_type(query_args: &ItemTypeQueryArgs) -> anyhow::Result<()> {        
    log::info!("Download Item Types using {:?}", query_args);

    if query_args.overwrite && query_args.folder_path.exists() {
        fs::remove_dir_all(&query_args.folder_path).await?;
    }

    if !query_args.folder_path.exists() {
        fs::create_dir_all(&query_args.folder_path).await?;
    }

    // SPARQL query for items of the specified type
    let sparql_query = format!( 
        r#"SELECT ?item WHERE {{ ?item wdt:P31 wd:{}.}}"#, 
        query_args.item_type.as_id() 
    );

    let client = Client::new();
    let request  = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("query", sparql_query), ("format", "json".to_string())])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        .build()?;

    log::error!("Download Item Type URL: {}", request.url().as_str());

    let response: ItemListResponse = client.execute(request).await?.json().await?;

    let mut item_rev_queries: Vec<ItemRevsQueryArgs> = Vec::new();

    for binding in response.results.bindings {
        let uri = binding.item.value;
        let item_id = uri.rsplit('/').next().unwrap();

        item_rev_queries.push(ItemRevsQueryArgs {
            item_type: query_args.item_type.clone(),
            item_id: item_id.to_string(),
            folder_path: query_args.folder_path.join(item_id),
            overwrite: query_args.overwrite,
            rev_count: query_args.rev_count.clone(),
            rev_end: query_args.rev_end.clone(),
            rev_start: query_args.rev_start.clone(),
        });
    };

    for ref item_rev_query in item_rev_queries {
        download_item_revisions(item_rev_query).await?;
    }

    Ok(())
}

async fn download_item_list(query_args: &ItemListQueryArgs) -> anyhow::Result<()> {        
    log::info!("Download Item List using {:?}", query_args);

    // SPARQL query for items of the specified type that match the provided item ids
    let item_list_string = query_args.item_ids.iter()
        .map(|id| format!("wd:{}", id))
        .collect::<Vec<String>>()
        .join(" ");

    let sparql_query = format!( 
        r#"SELECT ?item WHERE {{ ?item wdt:P31 wd:{}. VALUES ?item {{ {} }} }}"#, 
        query_args.item_type.as_id(), 
        item_list_string
    );

    let client = Client::new();
    let request  = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("query", sparql_query), ("format", "json".to_string())])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        .build()?;

    log::error!("Download Item List URL: {}", request.url().as_str());

    let response: ItemListResponse = client.execute(request).await?.json().await?;

    let mut item_rev_queries: Vec<ItemRevsQueryArgs> = Vec::new();

    for binding in response.results.bindings {
        let uri = binding.item.value;
        let item_id = uri.rsplit('/').next().unwrap();

        item_rev_queries.push(ItemRevsQueryArgs {
            item_type: query_args.item_type.clone(),
            item_id: item_id.to_string(),
            folder_path: query_args.folder_path.join(item_id),
            overwrite: query_args.overwrite,
            rev_count: query_args.rev_count.clone(),
            rev_end: query_args.rev_end.clone(),
            rev_start: query_args.rev_start.clone(),
        });
    };

    for ref item_rev_query in item_rev_queries {
        download_item_revisions(item_rev_query).await?;
    }

    Ok(())
}


async fn download_item_revisions(query_args: &ItemRevsQueryArgs) -> anyhow::Result<()> {        
    log::info!("Download Item Revisions using {:?}", query_args);

    if query_args.overwrite && query_args.folder_path.exists() {
        fs::remove_dir_all(&query_args.folder_path).await?;
    }

    if !query_args.folder_path.exists() {
        fs::create_dir_all(&query_args.folder_path).await?;
    }

    let target_revision_count = query_args.rev_count;
    let mut fetched_revision_count = 0;

    let client = Client::new();
    let mut continuation_token: Option<Continuation> = None;

    loop {
        let request = create_item_revision_request(
            &client, &query_args, continuation_token)?.build()?;

        log::error!("Downloading Item Revisions URL: {}", request.url().as_str());

        let response: ApiResponse = client.execute(request).await?.json().await?;

        if let Some(page) = response.query.pages.values().next() {
            if let Some(revisions) = &page.revisions {
                for revision in revisions {
                    log::trace!(
                        "Item ID {:?}, Revision ID: {}, Timestamp: {}, User: {:?}, Comment: {:?}",
                        &query_args.item_id, revision.revid, revision.timestamp, revision.user, revision.comment
                    );

                    // Construct the revision
                    let item_rev_content = ItemRevisionFileContent::new(revision)?;

                    if item_rev_content.content.is_some() {
                        if !query_args.folder_path.exists() {
                            fs::create_dir_all(&query_args.folder_path).await?;
                        }

                        // Create a file name from the Revision Timestamp
                        let filename = item_rev_content.timestamp.replace(":", "-").replace("T", "_");

                        // Save the revision to a file in the item folder
                        let revision_file = query_args.folder_path.join(format!("{}.json", filename));
                        let mut file = fs::File::create(revision_file).await?;
                        file.write_all(serde_json::to_string(&item_rev_content)?.as_bytes()).await?;

                        fetched_revision_count += 1;

                        if fetched_revision_count >= target_revision_count {
                            break;
                        };
                    } else  {
                        log::warn!("No slots found in Item {:?} Revision {:?}", &query_args.item_id, revision.revid);
                    }
                }
            }
        }

        // If we have reached the desired rev_count or there is no continuation token for pagination, break the loop;
        if fetched_revision_count >= target_revision_count || response.continuation.is_none() {
            break;
        } else {
            continuation_token = response.continuation;
        }
    }

    Ok(())
}

fn create_item_revision_request(client: &Client, query_args: &ItemRevsQueryArgs, continuation: Option<Continuation>) -> anyhow::Result<RequestBuilder> {

    let rvlimit: usize = 50;

    let mut request = client
        .get("https://www.wikidata.org/w/api.php")
        .query(&[("action", "query")])
        .query(&[("format", "json")])
        .query(&[("prop", "revisions")])
        .query(&[("titles", query_args.item_id.clone())])
        .query(&[("rvprop", "ids|timestamp|user|comment|content")])
        .query(&[("rvslots", "main")])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)");

    request = match (query_args.rev_start, query_args.rev_end) {
        (Some(start_datetime), Some(end_datetime)) => {
            // Get the revisions between the start and end date
            // If a rev_count is provided, get that number of revisions, otherwise get 1.
            // Set rvlimit to be smart about how many to fetch with each request.
            if end_datetime < start_datetime {
                anyhow::bail!("End date must be equal to or after the start date");
            }
            request
                .query(&[("rvstart", end_datetime.to_string())])
                .query(&[("rvend", start_datetime.to_string())])
                .query(&[("rvdir", "older")])
                .query(&[("rvlimit", 50)])
        }
        (Some(start_datetime), None) => {
            // Get revisions after to the start date
            // If a rev_count is provided, get that number of revisions, otherwise get 1.
            // Set rvlimit to be smart about how many to fetch with each request.
            request
                .query(&[("rvstart", start_datetime.to_string())])
                .query(&[("rvdir", "newer")])
                .query(&[("rvlimit", cmp::min(rvlimit, query_args.rev_count).to_string())])
        }
        (None, Some(end_datetime)) => {
            // Get revisions prior to the end date
            // If a rev_count is provided, get that number of revisions, otherwise get 1.
            // Set rvlimit to be smart about how many to fetch with each request.
            request
                .query(&[("rvstart", end_datetime.to_string())])
                .query(&[("rvdir", "older")])
                .query(&[("rvlimit", cmp::min(rvlimit, query_args.rev_count).to_string())])
            },
        (None, None) => {
            // Get revisions prior to NOW.
            // If a rev_count is provided, get that number of revisions, otherwise get 1.
            // Set rvlimit to be smart about how many to fetch with each request.
            request
            .query(&[("rvstart", "now")])
            .query(&[("rvdir", "older")])
            .query(&[("rvlimit", cmp::min(rvlimit, query_args.rev_count).to_string())])
        },
    };

    Ok(if continuation.is_some() {
        request.query(&[("rvcontinue", continuation.unwrap().rvcontinue)])
    } else {
        request
    })
}

#[derive(Deserialize, Debug)]
struct ItemListResponse {
    results: ItemListResults,
}

#[derive(Deserialize, Debug)]
struct ItemListResults {
    bindings: Vec<ResultBinding>,
}

#[derive(Deserialize, Debug)]
struct ResultBinding {
    item: BindingItem,
}

#[derive(Deserialize, Debug)]
struct BindingItem {
    value: String,
}