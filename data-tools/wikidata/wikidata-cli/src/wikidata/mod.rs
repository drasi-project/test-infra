use std::{cmp, fmt::Display, hash::{Hash, Hasher}, path::PathBuf, str::FromStr};

use chrono::NaiveDateTime;
use clap::ValueEnum;
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use strum_macros::EnumIter;
use tokio::{fs, io::AsyncWriteExt};

pub mod extractors;

/// Enum representing the different types of WikiData Item that can be downloaded
#[derive(Copy, Clone, Debug, PartialEq, EnumIter, Eq, PartialOrd, Ord, ValueEnum, Serialize, Deserialize)]
pub enum ItemType {
    City,
    Continent,
    Country
}

impl ItemType {
    pub fn as_wikidata_id(&self) -> &str {
        match self {
            ItemType::City => "Q515",
            ItemType::Continent => "Q5107",
            ItemType::Country => "Q6256",
        }
    }

    pub fn as_label(&self) -> &str {
        match self {
            ItemType::City => "City",
            ItemType::Continent => "Continent",
            ItemType::Country => "Country",
        }
    }

    pub fn is_node(&self) -> bool {
        match self {
            ItemType::City => true,
            ItemType::Continent => true,
            ItemType::Country => true,
        }
    }
}

impl FromStr for ItemType {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim().to_lowercase();

        match input.as_str() {
            "city" => Ok(ItemType::City),
            "continent" => Ok(ItemType::Continent),
            "country" => Ok(ItemType::Country),
            _ => Err(format!("Unknown variant: {}", input)),
        }
    }
}

impl Display for ItemType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ItemType::City => write!(f, "city"),
            ItemType::Continent => write!(f, "continent"),
            ItemType::Country => write!(f, "country"),
        }
    }
}

impl Hash for ItemType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ItemType::City => "city".hash(state),
            ItemType::Continent => "continent".hash(state),
            ItemType::Country => "country".hash(state),
        }
    }
}


#[derive(Deserialize, Debug)]
pub struct ApiResponse {
    #[serde(rename = "continue")]
    pub continuation: Option<Continuation>,
    pub query: Query,
}

#[derive(Deserialize, Debug)]
pub struct Continuation {
    #[serde(rename = "continue")]
    pub continuation: String,
    pub rvcontinue: String,
}

#[derive(Deserialize, Debug)]
pub struct Query {
    pub pages: std::collections::HashMap<String, Page>, // Pages keyed by page ID
}

#[derive(Deserialize, Debug)]
pub struct Page {
    pub pageid: u32,
    pub ns: u32,
    pub title: String,    
    pub revisions: Option<Vec<Revision>>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Revision {
    pub revid: u64,
    pub parentid: u64,
    pub timestamp: String,
    pub user: Option<String>,
    pub userid: Option<u64>,
    pub comment: Option<String>,
    pub slots: Option<std::collections::HashMap<String, Slot>>,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct Slot {
    pub contentmodel: String,
    pub contentformat: String,
    #[serde(rename = "*")]
    pub content: String,
}

#[derive(Deserialize, Debug, Serialize)]
pub struct ItemRevisionFileContent {
    pub item_id: String,
    pub item_type: ItemType,
    pub rev_id: u64,
    pub parent_id: u64,
    pub timestamp: String,
    pub user: Option<String>,
    pub user_id: Option<u64>,
    pub comment: Option<String>,
    pub content: Option<Value>,
}

impl ItemRevisionFileContent {
    pub fn new(item_id: String, item_type: ItemType, revision: &Revision) -> anyhow::Result<Self> {
        let mut irfc = Self {
            item_id,
            item_type,
            rev_id: revision.revid,
            parent_id: revision.parentid,
            timestamp: revision.timestamp.clone(),
            user: revision.user.clone(),
            user_id: revision.userid.clone(),
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

#[derive(Debug)]
pub struct ItemTypeQueryArgs {
    pub folder_path: PathBuf,
    pub item_type: ItemType,
    pub overwrite: bool,
    pub rev_count: usize,
    pub rev_end: Option<NaiveDateTime>,
    pub rev_start: Option<NaiveDateTime>,
}

#[derive(Debug)]
pub struct ItemListQueryArgs {
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

pub async fn download_item_type(query_args: &ItemTypeQueryArgs) -> anyhow::Result<()> {        
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
        query_args.item_type.as_wikidata_id() 
    );

    let client = Client::new();
    let request  = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("query", sparql_query), ("format", "json".to_string())])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        .build()?;

    log::debug!("Download Item Type URL: {}", request.url().as_str());

    let response: ItemListResponse = client.execute(request).await?.json().await?;

    let mut item_rev_queries: Vec<ItemRevsQueryArgs> = Vec::new();

    println!("Downloading {} Items of type {}:", response.results.bindings.len(), query_args.item_type.as_label());

    for binding in response.results.bindings {
        let uri = binding.item.value;
        let item_id = uri.rsplit('/').next().unwrap();

        item_rev_queries.push(ItemRevsQueryArgs {
            item_type: query_args.item_type,
            item_id: item_id.to_string(),
            folder_path: query_args.folder_path.join(item_id),
            overwrite: query_args.overwrite,
            rev_count: query_args.rev_count.clone(),
            rev_end: query_args.rev_end.clone(),
            rev_start: query_args.rev_start.clone(),
        });
    };

    for (i, item_rev_query) in item_rev_queries.iter().enumerate() {
        download_item_revisions(i, item_rev_query).await?;
    }

    Ok(())
}

pub async fn download_item_list(query_args: &ItemListQueryArgs) -> anyhow::Result<()> {        
    log::info!("Download Item List using {:?}", query_args);

    // SPARQL query for items of the specified type that match the provided item ids
    let item_list_string = query_args.item_ids.iter()
        .map(|id| format!("wd:{}", id))
        .collect::<Vec<String>>()
        .join(" ");

    let sparql_query = format!( 
        r#"SELECT ?item WHERE {{ ?item wdt:P31 wd:{}. VALUES ?item {{ {} }} }}"#, 
        query_args.item_type.as_wikidata_id(), 
        item_list_string
    );

    let client = Client::new();
    let request  = client
        .get("https://query.wikidata.org/sparql")
        .query(&[("query", sparql_query), ("format", "json".to_string())])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)")
        .build()?;

    log::debug!("Download Item List URL: {}", request.url().as_str());

    let response: ItemListResponse = client.execute(request).await?.json().await?;

    println!("Downloading {} Items of type {}:", response.results.bindings.len(), query_args.item_type.as_label());

    let mut item_rev_queries: Vec<ItemRevsQueryArgs> = Vec::new();

    for binding in response.results.bindings {
        let uri = binding.item.value;
        let item_id = uri.rsplit('/').next().unwrap();

        item_rev_queries.push(ItemRevsQueryArgs {
            item_type: query_args.item_type,
            item_id: item_id.to_string(),
            folder_path: query_args.folder_path.join(item_id),
            overwrite: query_args.overwrite,
            rev_count: query_args.rev_count.clone(),
            rev_end: query_args.rev_end.clone(),
            rev_start: query_args.rev_start.clone(),
        });
    };

    for (i, item_rev_query) in item_rev_queries.iter().enumerate() {
        download_item_revisions(i, item_rev_query).await?;
    }

    Ok(())
}

async fn download_item_revisions(item_index: usize, query_args: &ItemRevsQueryArgs) -> anyhow::Result<()> {        
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

    // Build the list of revision IDs to fetch.
    let mut total_revision_count = 0;
    let mut revision_ids: Vec<String> = Vec::new();
    loop {
        let request = create_item_revision_list_request(
            &client, &query_args, continuation_token)?.build()?;

        log::debug!("Downloading Item Revision List URL: {}", request.url().as_str());

        let response: ApiResponse = client.execute(request).await?.json().await?;

        if let Some(page) = response.query.pages.values().next() {
            if let Some(revisions) = &page.revisions {
                for revision in revisions {
                    log::trace!(
                        "Item ID {:?}, Revision ID: {}, Parent ID: {}, Timestamp: {}",
                        &query_args.item_id, revision.revid, revision.parentid, revision.timestamp
                    );

                    total_revision_count += 1;

                    // Create a file name and path from the Revision Timestamp
                    let filename = revision.timestamp.replace(":", "-").replace("T", "_");
                    let revision_file = query_args.folder_path.join(format!("{}.json", filename));
                    
                    // If the revision file already exists, dont rewrite it.
                    // This will allow for incremental fetching of content over multiiple runs in case of failure
                    if revision_file.exists() {
                        log::trace!("Revision {:?} already exists in Item {:?}", revision.revid, &query_args.item_id);
                    } else {
                        revision_ids.push(revision.revid.to_string());
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

    println!("  {}. Item {:?} - downloading {} of {} available revisions.", 
        item_index, &query_args.item_id, revision_ids.len(), total_revision_count);

    // Fetch the revisions in batches of 20
    let revision_id_chunks: Vec<Vec<String>> = revision_ids
        .chunks(20) 
        .map(|chunk| chunk.to_vec()) 
        .collect();

    for revision_id_chunk in revision_id_chunks {
        let request = create_item_revisions_request(
            &client, &revision_id_chunk)?.build()?;

        log::debug!("Downloading Item Revisions URL: {}", request.url().as_str());

        let response: ApiResponse = client.execute(request).await?.json().await?;

        if let Some(page) = response.query.pages.values().next() {
            if let Some(revisions) = &page.revisions {
                for revision in revisions {
                    log::trace!(
                        "Item ID {:?}, Revision ID: {}, Timestamp: {}, User: {:?}, UserId: {:?}, Comment: {:?}",
                        &query_args.item_id, revision.revid, revision.timestamp, revision.user, revision.userid, revision.comment
                    );

                    // Construct the revision
                    let item_rev_content = ItemRevisionFileContent::new(query_args.item_id.clone(), query_args.item_type, revision)?;

                    // Create a file name and path from the Revision Timestamp
                    let filename = item_rev_content.timestamp.replace(":", "-").replace("T", "_");
                    let revision_file = query_args.folder_path.join(format!("{}.json", filename));
                    
                    // If the revision file already exists, dont rewrite it.
                    // This will allow for incremental fetching of content over multiiple runs in case of failure
                    if revision_file.exists() {
                        log::warn!("Revision {:?} already exists in Item {:?}", revision.revid, &query_args.item_id);
                        continue;
                    }

                    if item_rev_content.content.is_some() {
                        // Skip the revision if its claims don't exist or are empty.
                        let has_claims = item_rev_content.content
                            .as_ref().unwrap()
                            .get("claims")
                            .map_or(true, |claims| !claims.is_array() || claims.as_array().unwrap().is_empty());

                        if !has_claims {
                            log::error!("No claims found in Item {:?} Revision {:?}", &query_args.item_id, revision.revid);
                            continue;
                        }

                        if !query_args.folder_path.exists() {
                            fs::create_dir_all(&query_args.folder_path).await?;
                        }

                        // // Save the revision to a file in the item folder
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
    }

    Ok(())
}

fn create_item_revision_list_request(client: &Client, query_args: &ItemRevsQueryArgs, continuation: Option<Continuation>) -> anyhow::Result<RequestBuilder> {

    let rvlimit: usize = 50;

    let mut request = client
        .get("https://www.wikidata.org/w/api.php")
        .query(&[("action", "query")])
        .query(&[("format", "json")])
        .query(&[("prop", "revisions")])
        .query(&[("titles", query_args.item_id.clone())])
        .query(&[("rvprop", "ids|timestamp")])
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

fn create_item_revisions_request(client: &Client, revision_ids: &Vec<String>) -> anyhow::Result<RequestBuilder> {

    Ok(client
        .get("https://www.wikidata.org/w/api.php")
        .query(&[("action", "query")])
        .query(&[("format", "json")])
        .query(&[("revids", revision_ids.join("|"))])
        .query(&[("prop", "revisions")])
        .query(&[("rvprop", "ids|timestamp|user|userid|comment|content")])
        .query(&[("rvslots", "main")])
        .header("User-Agent", "DrasiWikiDataCli/0.1 (info@drasi.io)"))
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
