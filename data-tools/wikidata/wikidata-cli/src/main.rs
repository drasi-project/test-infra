use std::path::PathBuf;

use chrono::NaiveDateTime;
use clap::{Args, Parser, Subcommand};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{fs, io::AsyncWriteExt};
use wikidata::ItemType;

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
        data_selection: TypeDataSelectionArgs,

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
        data_selection: ItemDataSelectionArgs,

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
struct TypeDataSelectionArgs {
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

impl TypeDataSelectionArgs {
    fn get_type_query_args(&self, parent_path: PathBuf, overwrite: bool) -> Vec<ItemTypeQueryArgs> {

        let mut queries = Vec::new();

        for item_type in &self.item_types {
            queries.push(ItemTypeQueryArgs {
                folder_path: parent_path.join(item_type.to_string()),
                item_type: item_type.clone(),
                overwrite,
                rev_count: self.rev_count,
                rev_end: self.rev_end.clone(),
                rev_start: self.rev_start.clone()
            });
        }
        queries
    }
}

#[derive(Args, Debug)]
struct ItemDataSelectionArgs {
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

impl ItemDataSelectionArgs {
    fn get_item_revs_query_args(&self, parent_path: PathBuf, overwrite: bool) -> Vec<ItemRevsQueryArgs> {

        let mut queries = Vec::new();

        for item_id in &self.item_ids {
            queries.push(ItemRevsQueryArgs {
                folder_path: parent_path.join(item_id.to_string()),
                item_type: self.item_type,
                item_id: item_id.clone(),
                overwrite,
                rev_count: self.rev_count,
                rev_end: self.rev_end.clone(),
                rev_start: self.rev_start.clone()
            });
        }
        queries
    }
}

#[derive(Debug)]
struct ItemTypeQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub overwrite: bool,
    pub rev_count: Option<usize>,
    pub rev_end: Option<NaiveDateTime>,
    pub rev_start: Option<NaiveDateTime>,
}

// #[derive(Debug)]
// struct ItemListQueryArgs {
//     pub folder_path: PathBuf,
//     pub item_type: ItemType,
//     pub item_ids: Vec<String>,
//     pub overwrite: bool,
//     pub rev_count: Option<usize>,
//     pub rev_end: Option<NaiveDateTime>,
//     pub rev_start: Option<NaiveDateTime>,
// }

#[derive(Debug)]
struct ItemRevsQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub item_id: String,
    pub overwrite: bool,
    pub rev_count: Option<usize>,
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

async fn handle_get_types_command(args: TypeDataSelectionArgs, cache_folder_path: PathBuf, overwrite: bool, script: bool ) -> anyhow::Result<()> {
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
    let query_args = args.get_type_query_args(item_folder, overwrite);
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

async fn handle_get_items_command(args: ItemDataSelectionArgs, cache_folder_path: PathBuf, overwrite: bool, script: bool ) -> anyhow::Result<()> {
    log::info!("GetItems command using {:?}", args);

    // Display a summary of what the command is going to do.
    println!("Getting WikiData Items:");
    println!("  - date range: {:?} to {:?}", &args.rev_start, &args.rev_end);
    println!("  - item type: {:?}", args.item_type);
    println!("  - item ids: {:?}", args.item_ids);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);
    println!("  - script: {}", script);

    let item_folder = cache_folder_path.join("items").join(args.item_type.to_string());

    // For each Item, download the Item Revisions based on the criteria provided.
    let query_args = args.get_item_revs_query_args(item_folder, overwrite);
    for query_arg in query_args {
        download_item_revisions(&query_arg).await?;
    };

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

    // download_item_revisions("Q29520", item_type_folder.join("Q29520"), data_selection).await?;

    // SPARQL query for items of the specified type
    let sparql_query = format!( 
        r#"SELECT ?item WHERE {{ ?item wdt:P31 wd:{}.}}"#, 
        query_args.item_type.as_id() 
    );

    let client = Client::new();
    let response  = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("query", sparql_query), ("format", "json".to_string())])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        .send()
        .await?;

    let response: ItemListResponse = response
        .json()
        .await?;

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
    log::info!("Downloading Item Revisions using {:?}", query_args);

    if query_args.overwrite && query_args.folder_path.exists() {
        fs::remove_dir_all(&query_args.folder_path).await?;
    }

    if !query_args.folder_path.exists() {
        fs::create_dir_all(&query_args.folder_path).await?;
    }

    // Fetch revisions for this item
    // let url = format!(
    //     "https://www.wikidata.org/w/api.php?action=query&format=json&prop=revisions&titles={}&rvprop=ids|timestamp|user|comment|content&rvslots=main&rvlimit=max", 
    //     item_id);

    // let response  = client
    //     .get("https://www.wikidata.org/w/api.php")
    //     .query(&[("action", "query"), ("format", "json"), ("prop", "revisions"), ("titles", item_id), ("rvprop", "ids|timestamp|user|comment|content"), ("rvslots", "main"), ("rvlimit", "max"), ("rvstart", &start_datetime.to_string()), ("rvend", &end_datetime.to_string())])
    //     .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
    //     .send()
    //     .await?;

    // let client = Client::new();
    // let mut url = format!(
    //     "https://www.wikidata.org/w/api.php?action=query&format=json&prop=revisions&titles={}&rvprop=ids|timestamp|user|comment|content&rvslots=main&rvlimit=max",
    //     item_id
    // );

    let client = Client::new();
    let mut continuation_token: Option<Continuation> = None;

    loop {
        // let request  = match continuation_token {
        //     Some(token) => {
        //         client
        //             .get("https://www.wikidata.org/w/api.php")
        //             .query(&[("action", "query")])
        //             .query(&[("format", "json")])
        //             .query(&[("prop", "revisions")])
        //             .query(&[("titles", item_id)])
        //             .query(&[("rvprop", "ids|timestamp|user|comment|content")])
        //             .query(&[("rvslots", "main")])
        //             // .query(&[("rvlimit", "max")])
        //             // .query(&[("rvend", start_datetime.to_string()), ])
        //             // .query(&[("rvstart", end_datetime.to_string())])
        //             .query(&[("rvcontinue", &token.rvcontinue)])
        //             .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        //             .build()?
        //     },
        //     None => {
        //         client
        //             .get("https://www.wikidata.org/w/api.php")
        //             .query(&[("action", "query")])
        //             .query(&[("format", "json")])
        //             .query(&[("prop", "revisions")])
        //             .query(&[("titles", item_id)])
        //             .query(&[("rvprop", "ids|timestamp|user|comment|content")])
        //             .query(&[("rvslots", "main")])
        //             // .query(&[("rvlimit", "max")])
        //             // .query(&[("rvend", start_datetime.to_string()), ])
        //             // .query(&[("rvstart", end_datetime.to_string())])
        //             .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        //             .build()?
        //     }
        // };

        let request = create_item_revision_request(
            &client, &query_args, continuation_token)?.build()?;

        println!("Built URL: {}", request.url().as_str());

        // let response: ApiResponse = client.get(&url).send().await?.json().await?;
        // let response: ApiResponse = request
        //     .send()
        //     .await?
        //     .json()
        //     .await?;

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

                    } else  {
                        log::warn!("No slots found in Item {:?} Revision {:?}", &query_args.item_id, revision.revid);
                    }

                    // match &revision.slots {
                    //     Some(slots) => {
                    //         match slots.get("main") {
                    //             Some(slot) => {
                    //                 if !query_args.folder_path.exists() {
                    //                     fs::create_dir_all(&query_args.folder_path).await?;
                    //                 }
    
                    //                 // Create a file name from the Revision Timestamp
                    //                 let filename = revision.timestamp.replace(":", "-").replace("T", "_");
    
                    //                 // Construct the revision
                    //                 let item_revision: Value = serde_json::from_str(&slot.content)?;
                    //                 let content = serde_json::to_string(&item_revision)?;

                    //                 // Save the revision to a file in the item folder
                    //                 let revision_file = query_args.folder_path.join(format!("{}.json", filename));
                    //                 let mut file = fs::File::create(revision_file).await?;
                    //                 file.write_all(content.as_bytes()).await?;
                    //             },
                    //             None => {
                    //                 log::warn!("Main slot not found in Item {:?} Revision {:?}", &query_args.item_id, revision.revid);
                    //             }
                    //         }
                    //     }
                    //     None => {
                    //         log::warn!("No slots found in Item {:?} Revision {:?}", &query_args.item_id, revision.revid);
                    //     }
                    // }
                }
            }
        }

        // Check if there is a continuation token for pagination, if not break the loop;
        continuation_token = match response.continuation {
            Some(token) => Some(token),
            None => break,
        }; 
    }

    Ok(())
}

fn create_item_revision_request(client: &Client, query_args: &ItemRevsQueryArgs, continuation: Option<Continuation>) -> anyhow::Result<RequestBuilder> {

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
            if end_datetime < start_datetime {
                anyhow::bail!("End date must be equal to or after the start date");
            }
            request
                .query(&[("rvend", start_datetime.to_string())])
                .query(&[("rvlimit", query_args.rev_count.unwrap_or(1).to_string())])
                .query(&[("rvdir", "older".to_string())])
        }
        (Some(start_datetime), None) => {
            // Get the count of revisions after to the start date
            request
                .query(&[("rvend", start_datetime.to_string())])
                .query(&[("rvlimit", query_args.rev_count.unwrap_or(1).to_string())])
                .query(&[("rvdir", "older".to_string())])
        }
        (None, Some(end_datetime)) => {
            // Get the count of revisions prior to the end date
            request
                .query(&[("rvstart", end_datetime.to_string())])
                .query(&[("rvlimit", query_args.rev_count.unwrap_or(1).to_string())])
                .query(&[("rvdir", "older".to_string())])
        },
        (None, None) => {
            request
                .query(&[("rvlimit", query_args.rev_count.unwrap_or(1).to_string())])
                .query(&[("rvdir", "newer".to_string())])
        },
    };

    Ok(if continuation.is_some() {
        request.query(&[("rvcontinue", continuation.unwrap().rvcontinue)])
    } else {
        request
    })
}


#[derive(Deserialize, Debug, Serialize)]
struct ItemRevisionFileContent {
    pub revid: u64,
    pub parentid: u64,
    pub timestamp: String,
    pub user: Option<String>,
    pub comment: Option<String>,
    pub content: Option<Value>,
}

impl ItemRevisionFileContent {
    pub fn new(revision: &Revision) -> anyhow::Result<Self> {
        let mut irfc = Self {
            revid: revision.revid,
            parentid: revision.parentid,
            timestamp: revision.timestamp.clone(),
            user: revision.user.clone(),
            comment: revision.comment.clone(),
            content: None,
        };

        if let Some(slots) = &revision.slots {
            if let Some(slot) = slots.get("main") {
                irfc.content = Some(serde_json::from_str(&slot.content)?);
            }
        }

        Ok(irfc)
    }
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




#[derive(Deserialize, Debug)]
struct ApiResponse {
    #[serde(rename = "continue")]
    continuation: Option<Continuation>,
    query: Query,
}

#[derive(Deserialize, Debug)]
struct Continuation {
    #[serde(rename = "continue")]
    continuation: String,
    rvcontinue: String,
}

#[derive(Deserialize, Debug)]
struct Query {
    pages: std::collections::HashMap<String, Page>, // Pages keyed by page ID
}

#[derive(Deserialize, Debug)]
struct Page {
    pageid: u32,
    ns: u32,
    title: String,    
    revisions: Option<Vec<Revision>>,
}

#[derive(Deserialize, Debug, Serialize)]
struct Revision {
    revid: u64,
    parentid: u64,
    timestamp: String,
    user: Option<String>,
    comment: Option<String>,
    slots: Option<std::collections::HashMap<String, Slot>>,
}

#[derive(Deserialize, Debug, Serialize)]
struct Slot {
    contentmodel: String,
    contentformat: String,
    #[serde(rename = "*")]
    content: String,
}

// async fn download_item_revisions(item_id: String) -> anyhow::Result<()> {

//     // Fetch revisions for this item
//     let mut url = format!(WIKIDATA_URL, item_id, "");

//     println!("Fetching revisions for {}", item_id);

//     loop {
//         let response = client.get(&url).send().await?.text().await?;
//         let response: serde_json::Value = serde_json::from_str(&response)?;

//         // Process revisions
//         if let Some(pages) = response["query"]["pages"].as_object() {
//             for (_id, page) in pages {
//                 if let Some(revisions) = page["revisions"].as_array() {
//                     for revision in revisions {
//                         writeln!(file, "{}", serde_json::to_string(revision)?)?;
//                     }
//                 }
//             }
//         }

//         // Handle pagination
//         if let Some(rvcontinue) = response["continue"]["rvcontinue"].as_str() {
//             url = format!(
//                 "https://www.wikidata.org/w/api.php?action=query&format=json&prop=revisions&titles={}&rvprop=ids|timestamp|user|comment|content&rvslots=main&rvlimit=max&rvstart={}&rvcontinue={}",
//                 item_id, start_datetime, rvcontinue
//             );
//         } else {
//             break;
//         }
//     }
//     OK()
// }

// async fn handle_unzip_command(data_selection: DataSelectionArgs, cache_folder_path: PathBuf, overwrite: bool) -> anyhow::Result<()> {
//     log::debug!("Unzip command:");

//     let (start_datetime, end_datetime) = get_date_range(&data_selection)?;

//     // Display a summary of what the command is going to do based on the input parameters and the calculated date range
//     println!("Unzipping GDELT Data:");
//     println!("  - date range: {} to {}", &start_datetime, &end_datetime);
//     println!("  - data types: {:?}", data_selection.data_type);
//     println!("  - cache folder: {:?}", cache_folder_path);
//     println!("  - overwrite: {}", overwrite);

//     let files_to_unzip = create_gdelt_file_list(
//         start_datetime, 
//         end_datetime,
//         data_selection.data_type.iter().cloned().collect(),
//         cache_folder_path,
//         overwrite,
//     ).unwrap();

//     let unzip_results = unzip_gdelt_files(files_to_unzip).await.unwrap();

//     // println!("File unzip results:");
//     // for file_info in &unzip_results {
//     //     println!("  - {:?}", file_info);
//     // }

//     summarize_fileinfo_results(unzip_results);

//     Ok(())
// }

// async fn handle_load_command(data_selection: DataSelectionArgs, cache_folder_path: PathBuf, database_url: Option<String>, database_user: Option<String>, database_password: Option<String>) -> anyhow::Result<()>  {
//     log::debug!("Load command:");

//     let (start_datetime, end_datetime) = get_date_range(&data_selection)?;

//     println!(
//         "Loading GDELT Data:\n\
//           - date range: {} to {}\n\
//           - data types: {:?}\n\
//           - cache folder: {:?}\n\
//           - Database URL: {:?}\n\
//           - Database User: {:?}\n\
//           - Database Password: *****",
//         &start_datetime, &end_datetime, data_selection.data_type, cache_folder_path, database_url, database_user
//     );

//     let files_to_load = create_gdelt_file_list(
//         start_datetime, 
//         end_datetime,
//         data_selection.data_type.iter().cloned().collect(),
//         cache_folder_path,
//         false,
//     ).unwrap();

//     // println!("Files to load:");
//     // for file_info in &files_to_load {
//     //     println!("  - {:?}", file_info);
//     // }

//     let db_info = DbInfo {
//         host: database_url.unwrap_or_else(|| "localhost".to_string()),
//         port: 5432,
//         user: database_user.unwrap_or_else(|| "postgres".to_string()),
//         password: database_password.unwrap_or_else(|| "password".to_string()),
//         dbname: "gdelt".to_string(),
//         use_tls: false,
//     };

//     let load_results = load_gdelt_files(&db_info, files_to_load).await?;

//     println!("File load results:");
//     for file_info in &load_results {
//         println!("  - {:?}", file_info);
//     }

//     summarize_fileinfo_results(load_results);

//     Ok(())
// }

// async fn handle_initdb_command(database_url: Option<String>, database_user: Option<String>, database_password: Option<String>, overwrite: bool) -> anyhow::Result<()>  {
//     log::debug!("InitDB command:");

//     println!(
//         "Initializing GDELT Database:\n\
//           - Database URL: {:?}\n\
//           - Database User: {:?}\n\
//           - Database Password: *****",
//         database_url, database_user, 
//     );

//     let db_info = DbInfo {
//         host: database_url.unwrap_or_else(|| "localhost".to_string()),
//         port: 5432,
//         user: database_user.unwrap_or_else(|| "postgres".to_string()),
//         password: database_password.unwrap_or_else(|| "password".to_string()),
//         dbname: "gdelt".to_string(),
//         use_tls: false,
//     };

//     initialize(&db_info, overwrite).await?;

//     Ok(())
// }

/*
#[derive(Debug)]
struct FileInfo {
    file_type: ItemType,
    url: String,
    zip_path: PathBuf,
    unzip_path: PathBuf,
    overwrite: bool,
    download_result: Option<anyhow::Result<()>>,
    extract_result: Option<anyhow::Result<()>>,
    load_result: Option<anyhow::Result<()>>,
}

impl FileInfo {
    fn new_item_file(cache_folder_path: PathBuf, file_name: &str, overwrite: bool) -> Self {
        Self {
            file_type: ItemType::Event,
            url: format!("{}/{}.export.CSV.zip", SPARQL_URL, file_name),
            zip_path: cache_folder_path.join(format!("zip/{}.export.CSV.zip", file_name)),
            unzip_path: cache_folder_path.join(format!("event/{}.export.CSV", file_name)),
            overwrite,
            download_result: None,
            extract_result: None,
            load_result: None,
        }
    }

    fn set_download_result(&mut self, result: anyhow::Result<()>) {
        self.download_result = Some(result);
    }

    fn set_extract_result(&mut self, result: anyhow::Result<()>) {
        self.extract_result = Some(result);
    }

    fn set_load_result(&mut self, result: anyhow::Result<()>) {
        self.load_result = Some(result);
    }
}

fn create_gdelt_file_list(start_datetime: NaiveDateTime, end_datetime: NaiveDateTime, item_types: HashSet<ItemType>, cache_folder_path: PathBuf, overwrite: bool) -> anyhow::Result<Vec<FileInfo>> {

    let mut download_tasks: Vec<FileInfo> = Vec::new();

    // Create the cache_folder_path if it does not exist
    if !cache_folder_path.exists() {
        std::fs::create_dir_all(&cache_folder_path)?;
    }

    // Construct the list of urls and local paths for the files to be downloaded
    let mut current_datetime = start_datetime;
    while current_datetime <= end_datetime {
        let timestamp = current_datetime.format("%Y%m%d%H%M%S").to_string();
        log::trace!("Processing timestamp: {}", timestamp);

        // Events
        if item_types.contains(&ItemType::Event) {
            download_tasks.push(FileInfo::new_event_file(cache_folder_path.clone(), &timestamp, overwrite));
        }

        // Graph
        if item_types.contains(&ItemType::Graph) {
            download_tasks.push(FileInfo::new_graph_file(cache_folder_path.clone(), &timestamp, overwrite));
        }

        // Mentions
        if item_types.contains(&ItemType::Mention) {
            download_tasks.push(FileInfo::new_mention_file(cache_folder_path.clone(), &timestamp, overwrite));
        }

        // Increment the current_datetime by 15 minutes
        current_datetime = current_datetime.checked_add_signed(TimeDelta::minutes(15)).unwrap();
    }

    // Return a Vec of the urls and local paths for the files to be downloaded
    Ok(download_tasks)
}

async fn download_item_revisions(mut download_tasks: Vec<FileInfo>) -> anyhow::Result<Vec<FileInfo>> {
    let client = Client::new();

    // Create a collection of futures for downloading files
    let mut tasks = Vec::new();

    for task in &mut download_tasks {
        let url = task.url.clone();
        let path = task.zip_path.clone();
        let client = client.clone();

        // Spawn the download task and update the result in the FileInfo
        let fut = async move {
            download_file(client, url, path, task.overwrite).await
        };

        tasks.push(fut);
    }

    // Await all the tasks and update the results
    for (i, task_result) in futures::future::join_all(tasks).await.into_iter().enumerate() {
        download_tasks[i].set_download_result(task_result);
    }

    Ok(download_tasks)
}

// Helper function to download a file from the URL and save it to the given path
async fn download_file(client: Client, url: String, path: PathBuf, overwrite: bool) -> anyhow::Result<()> {

    if !overwrite && path.exists() {
        log::info!("File already exists: {:?}", path);
        return Ok(());
    }

    // Make sure the parent directory exists
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let response = client.get(&url).send().await?.error_for_status()?;
    let mut file = File::create(&path).await?;
    let content = response.bytes().await?;

    file.write_all(&content).await?;
    file.flush().await?;

    Ok(())
}

fn summarize_fileinfo_results(file_infos: Vec<FileInfo>) {
    // Initialize counters
    let mut summary: HashMap<ItemType, (usize, usize, usize, usize)> = HashMap::new();

    for file_info in file_infos {
        let (total, successful, failed, skipped) = summary.entry(file_info.file_type).or_insert((0, 0, 0, 0));

        *total += 1; // Increment total

        // Check download result
        match &file_info.download_result {
            Some(Ok(_)) => *successful += 1,
            Some(Err(_)) => *failed += 1,
            None => *skipped += 1,
        }

        // Check extraction result
        match &file_info.extract_result {
            Some(Ok(_)) => *successful += 1,
            Some(Err(_)) => *failed += 1,
            None => *skipped += 1,
        }

        // Check load result
        match &file_info.load_result {
            Some(Ok(_)) => *successful += 1,
            Some(Err(_)) => *failed += 1,
            None => *skipped += 1,
        }
    }

    // Print the summary
    for (data_type, (total, successful, failed, skipped)) in summary {
        println!("{:?} Files:", data_type);
        println!("Downloads - Total: {}, Successful: {}, Failed: {}, Skipped: {}", total, successful, failed, skipped);
        println!("Extractions - Total: {}, Successful: {}, Failed: {}, Skipped: {}", total, successful, failed, skipped);
        println!("Loads - Total: {}, Successful: {}, Failed: {}, Skipped: {}", total, successful, failed, skipped);
        println!(); // Print a newline for better readability
    }
}
    */