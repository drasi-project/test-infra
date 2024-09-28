use std::{collections::HashSet, hash::{Hash, Hasher}, path::PathBuf};

use async_zip::tokio::read::seek::ZipFileReader;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use reqwest::Client;
use tokio::{fs::{File, OpenOptions}, io::{AsyncWriteExt, BufReader}};
use tokio_util::compat::TokioAsyncWriteCompatExt;

/// String constant containing the address of GDELT data on the Web
const GDELT_DATA_URL: &str = "http://data.gdeltproject.org/gdeltv2";

/// String constant representing the default GDELT data cache folder path
/// This is the folder where GDELT data files are downloaded and stored if not provided by the user.
const DEFAULT_CACHE_FOLDER_PATH: &str = "./gdelt_data_cache";

/// Enum representing the different types of GDELT data that can be downloaded
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum DataType {
    Event,
    Graph,
    Mention,
}

impl Hash for DataType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            DataType::Event => "event".hash(state),
            DataType::Graph => "graph".hash(state),
            DataType::Mention => "mention".hash(state),
        }
    }
}

#[derive(Parser)]
#[command(name = "GDELT")]
#[command(about = "CLI for working with GDELT data", long_about = None)]
struct Params {
    /// The path of the GDELT data cache
    #[arg(short = 'c', long = "cache", env = "GDELT_CACHE_PATH")]
    pub cache_folder_path: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Downloads data from GDELT and stores it in the local file cache
    Get {
        #[command(flatten)]
        data_selection: DataSelectionArgs,

        /// A flag to indicate whether existing files should be overwritten
        #[arg(short = 'o', long, default_value_t = false)]
        overwrite: bool,

        /// A flag to indicate whether the downloaded files should be unzipped automatically
        #[arg(short = 'u', long, default_value_t = true)]
        unzip: bool,
    },
    /// Extracts the text data files from the zip file in the local file cache
    Unzip {
        #[command(flatten)]
        data_selection: DataSelectionArgs,

        /// A flag to indicate whether existing files should be overwritten
        #[arg(short = 'o', long, default_value_t = true)]
        overwrite: bool,
    },
    /// Loads GDELT data from the local file cache into a database.
    Load {
        #[command(flatten)]
        data_selection: DataSelectionArgs,

        #[arg(short = 'd', long = "db_url", env = "GDELT_DB_URL")]
        database_url: Option<String>,

        #[arg(short = 'u', long = "db_user", env = "GDELT_DB_USER")]
        database_user: Option<String>,

        #[arg(short = 'p', long = "db_password", env = "GDELT_DB_PASSWORD")]
        database_password: Option<String>,
    },
}

#[derive(Args, Debug)]
struct DataSelectionArgs {
    /// The types of GDELT data to process
    #[arg(short = 't', long, value_enum, default_value = "event,graph,mention", value_delimiter=',')]
    data_type: Vec<DataType>,

    /// The datetime from which to start processing files.
    /// In the format YYYYMMDDHHMMSS. Defaults for missing fields
    #[arg(short = 's', long)]
    file_start: Option<String>,

    /// The datetime at which to stop processing files
    /// In the format YYYYMMDDHHMMSS. Defaults for missing fields
    #[arg(short = 'e', long)]
    file_end: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let params = Params::parse();

    let cache_folder_path = params.cache_folder_path.unwrap_or_else(|| PathBuf::from(DEFAULT_CACHE_FOLDER_PATH));

    let res = match params.command {
        Commands::Get { data_selection, overwrite, unzip } => {
            handle_get_command(data_selection, cache_folder_path, overwrite, unzip).await
        }
        Commands::Unzip { data_selection, overwrite } => {
            handle_unzip_command(data_selection, cache_folder_path, overwrite).await
        }
        Commands::Load { data_selection, database_url, database_user, database_password } => {
            handle_load_command(data_selection, cache_folder_path, database_url, database_user, database_password).await
        }
    };

    match res {
        Ok(_) => {
            println!("Command completed successfully");
        }
        Err(e) => {
            eprintln!("gdelt command failed: {:?}", e);
        }
    }
}

fn parse_start_datetime(datetime_str: &str) -> anyhow::Result<NaiveDateTime> {
    let dt = parse_datetime(datetime_str)?;

    let seconds_per_15_min = 900;

    // We need to adjust both minutes and seconds so that the time is aligned at the NEXT 15 minute boundary,
    // which is the frequency at which GDELT data is published.
    // Find the number of seconds that need to be added to align on a 15 min boundary
    let seconds_into_interval = dt.num_seconds_from_midnight() as i64 % seconds_per_15_min;
    match seconds_into_interval {
        0 => Ok(dt),
        _ => Ok(dt.checked_add_signed(TimeDelta::seconds(seconds_per_15_min - seconds_into_interval)).unwrap())
    }
}

fn parse_end_datetime(datetime_str: &str) -> anyhow::Result<NaiveDateTime> {
    let dt = parse_datetime(datetime_str)?;

    let seconds_per_15_min = 900;

    // We need to adjust both minutes and seconds so that the time is aligned at the PREVIOUS 15 minute boundary,
    // which is the frequency at which GDELT data is published.
    // Find the number of seconds that need to be removed to align on a 15 min boundary
    let seconds_into_interval = dt.num_seconds_from_midnight() as i64 % seconds_per_15_min;
    match seconds_into_interval {
        0 => Ok(dt),
        _ => Ok(dt.checked_add_signed(TimeDelta::seconds(-seconds_into_interval)).unwrap())
    }
}

/// Function to parse the datetime string and return a NaiveDateTime with defaults for missing parts
fn parse_datetime(datetime_str: &str) -> anyhow::Result<NaiveDateTime> {
    let year = if datetime_str.len() < 4 {
        anyhow::bail!("Invalid datetime string: {}", datetime_str);
    } else {
        let year = datetime_str[0..4].parse::<i32>()?;
        if year < 2015 {
            anyhow::bail!("Invalid year: {}", year);
        };
        year
    };

    let month = if datetime_str.len() >= 6 {
        let month = datetime_str[4..6].parse::<u32>()?;
        if month > 12 || month == 0 {
            anyhow::bail!("Invalid month: {}", month);
        };
        month
    } else {
        1
    };

    let day = if datetime_str.len() >= 8 {
        let day = datetime_str[6..8].parse::<u32>()?;
        if day > 31 || day == 0 {
            anyhow::bail!("Invalid day: {}", day);
        };
        day
    } else {
        1
    };

    let hour = if datetime_str.len() >= 10 {
        let hour = datetime_str[8..10].parse::<u32>()?;
        if hour > 23 {
            anyhow::bail!("Invalid hour: {}", hour);
        };
        hour
    } else {
        0
    };

    let minute = if datetime_str.len() >= 12 {
        let minute = datetime_str[10..12].parse::<u32>()?;
        if minute > 59 {
            anyhow::bail!("Invalid minute: {}", minute);
        };
        minute
    } else {
        0
    };

    let second = if datetime_str.len() >= 14 {
        let second = datetime_str[12..14].parse::<u32>()?;
        if second > 59 {
            anyhow::bail!("Invalid second: {}", second);
        };
        second
    } else {
        0
    };

    let date: NaiveDate = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let time: NaiveTime = NaiveTime::from_hms_opt(hour, minute, second).unwrap();

    Ok(NaiveDateTime::new(date, time))
}

fn get_date_range(data_selection: &DataSelectionArgs) -> anyhow::Result<(NaiveDateTime, NaiveDateTime)> {

    // If there is a start date and end date, parse them and validate that the end date is equal to or after the start date
    // If there is only a start date, use the start date as both the start date and the end date
    // If there is only an end date return an error
    // If there is no date use the current date as both the start date and the end date

    match (data_selection.file_start.as_ref(), data_selection.file_end.as_ref()) {
        (Some(start_str), Some(end_str)) => {
            let start_datetime = parse_start_datetime(start_str)?;
            let end_datetime = parse_end_datetime(end_str)?;
            if end_datetime < start_datetime {
                anyhow::bail!("End date must be equal to or after the start date");
            }
            Ok((start_datetime, end_datetime))
        }
        (Some(start_str), None) => {
            let start_datetime = parse_start_datetime(start_str)?;
            Ok((start_datetime, start_datetime))
        }
        (None, Some(_)) => anyhow::bail!("End date provided without a start date"),
        (None, None) => {
            let current_datetime_str = Utc::now().naive_utc().format("%Y%m%d%H%M%S").to_string();
            let current_datetime = parse_start_datetime(&current_datetime_str)?;

            Ok((current_datetime, current_datetime))
        },
    }

}

async fn handle_get_command(data_selection: DataSelectionArgs, cache_folder_path: PathBuf, overwrite: bool, unzip: bool) -> anyhow::Result<()> {
    log::debug!("Get command:");

    let (start_datetime, end_datetime) = get_date_range(&data_selection)?;

    // Display a summary of what the command is going to do based on the input parameters and the calculated date range
    println!("Getting GDELT Data:");
    println!("  - date range: {} to {}", &start_datetime, &end_datetime);
    println!("  - data types: {:?}", data_selection.data_type);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);

    let downloads = create_gdelt_file_list(
        start_datetime, 
        end_datetime,
        data_selection.data_type.iter().cloned().collect(),
        cache_folder_path,
        overwrite,
    ).unwrap();

    // Display the list of files to be downloaded, without taking ownership of the list or content
    println!("Download Tasks:");
    for download in &downloads {
        println!("  - {:?}", download);
    }

    // Download the files
    let download_results = download_gdelt_zip_files(downloads).await.unwrap();

    println!("File download results:");
    for download in &download_results {
        println!("  - {:?}", download);
    }

    // Unzip the files if the unzip flag is set
    if unzip {
        let unzip_results = unzip_gdelt_files(download_results).await.unwrap();

        println!("File unzip results:");
        for unzip in &unzip_results {
            println!("  - {:?}", unzip);
        }
    }

    Ok(())
}

async fn handle_unzip_command(data_selection: DataSelectionArgs, cache_folder_path: PathBuf, overwrite: bool) -> anyhow::Result<()> {
    log::debug!("Unzip command:");

    let (start_datetime, end_datetime) = get_date_range(&data_selection)?;

    // Display a summary of what the command is going to do based on the input parameters and the calculated date range
    println!("Unzipping GDELT Data:");
    println!("  - date range: {} to {}", &start_datetime, &end_datetime);
    println!("  - data types: {:?}", data_selection.data_type);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - overwrite: {}", overwrite);

    let unzips = create_gdelt_file_list(
        start_datetime, 
        end_datetime,
        data_selection.data_type.iter().cloned().collect(),
        cache_folder_path,
        overwrite,
    ).unwrap();

    let unzip_results = unzip_gdelt_files(unzips).await.unwrap();

    println!("File unzip results:");
    for unzip in &unzip_results {
        println!("  - {:?}", unzip);
    }

    Ok(())
}

async fn handle_load_command(data_selection: DataSelectionArgs, cache_folder_path: PathBuf, database_url: Option<String>, database_user: Option<String>, database_password: Option<String>) -> anyhow::Result<()>  {
    log::debug!("Load command:");

    let (start_datetime, end_datetime) = get_date_range(&data_selection)?;

    // Display a summary of what the command is going to do based on the input parameters and the calculated date range
    println!("Unzipping GDELT Data:");
    println!("  - date range: {} to {}", &start_datetime, &end_datetime);
    println!("  - data types: {:?}", data_selection.data_type);
    println!("  - cache folder: {:?}", cache_folder_path);
    println!("  - Database URL: {:?}", database_url);
    println!("  - Database User: {:?}", database_user);
    println!("  - Database Password: {:?}", database_password);

    Ok(())
}

#[derive(Debug)]
struct FileInfo {
    url: String,
    zip_path: PathBuf,
    unzip_path: PathBuf,
    overwrite: bool,
    download_result: Option<anyhow::Result<()>>,
    extract_result: Option<anyhow::Result<()>>,
}

impl FileInfo {
    fn new(url: String, zip_path: PathBuf, unzip_path: PathBuf, overwrite: bool) -> Self {
        Self {
            url,
            zip_path,
            unzip_path,
            overwrite,
            download_result: None,
            extract_result: None,
        }
    }

    fn set_download_result(&mut self, result: anyhow::Result<()>) {
        self.download_result = Some(result);
    }

    fn set_extract_result(&mut self, result: anyhow::Result<()>) {
        self.extract_result = Some(result);
    }
}

fn create_gdelt_file_list(start_datetime: NaiveDateTime, end_datetime: NaiveDateTime, file_types: HashSet<DataType>, cache_folder_path: PathBuf, overwrite: bool) -> anyhow::Result<Vec<FileInfo>> {

    let mut download_tasks: Vec<FileInfo> = Vec::new();

    // Create the cache_folder_path if it does not exist
    if !cache_folder_path.exists() {
        std::fs::create_dir_all(&cache_folder_path)?;
    }

    // Construct the list of urls and local paths for the files to be downloaded
    let mut current_datetime = start_datetime;
    while current_datetime <= end_datetime {
        let timestamp = current_datetime.format("%Y%m%d%H%M%S").to_string();
        // println!("Timestamp: {}", timestamp);

        // Events
        if file_types.contains(&DataType::Event) {
            let zip_path = cache_folder_path.join(format!("zip/{}.export.CSV.zip", timestamp));
            let unzip_path = cache_folder_path.join(format!("event/{}.export.CSV", timestamp));
            let url = format!("{}/{}.export.CSV.zip", GDELT_DATA_URL, timestamp);
            download_tasks.push(FileInfo::new(url, zip_path, unzip_path, overwrite));
        }

        // Graph
        if file_types.contains(&DataType::Graph) {
            let zip_path = cache_folder_path.join(format!("zip/{}.gkg.csv.zip", timestamp));
            let unzip_path = cache_folder_path.join(format!("graph/{}.gkg.CSV", timestamp));
            let url = format!("{}/{}.gkg.csv.zip", GDELT_DATA_URL, timestamp);
            download_tasks.push(FileInfo::new(url, zip_path, unzip_path, overwrite));
        }

        // Mentions
        if file_types.contains(&DataType::Mention) {
            let zip_path = cache_folder_path.join(format!("zip/{}.mentions.CSV.zip", timestamp));
            let unzip_path = cache_folder_path.join(format!("mention/{}.mentions.CSV", timestamp));
            let url = format!("{}/{}.mentions.CSV.zip", GDELT_DATA_URL, timestamp);
            download_tasks.push(FileInfo::new(url, zip_path, unzip_path, overwrite));
        }

        // Increment the current_datetime by 15 minutes
        current_datetime = current_datetime.checked_add_signed(TimeDelta::minutes(15)).unwrap();
    }

    // Return a Vec of the urls and local paths for the files to be downloaded
    Ok(download_tasks)
}

async fn download_gdelt_zip_files(mut download_tasks: Vec<FileInfo>) -> anyhow::Result<Vec<FileInfo>> {
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

async fn unzip_gdelt_files(mut unzip_tasks: Vec<FileInfo>) -> anyhow::Result<Vec<FileInfo>> {
    // Create a collection of futures for downloading files
    let mut tasks = Vec::new();

    for task in &mut unzip_tasks {
        let src_path = task.zip_path.clone();
        let dest_path = task.unzip_path.clone();

        // Spawn the unzip task and update the result in the FileInfo
        let fut = async move {
            unzip_file(src_path, dest_path, task.overwrite).await
        };

        tasks.push(fut);
    }

    // Await all the tasks and update the results
    for (i, task_result) in futures::future::join_all(tasks).await.into_iter().enumerate() {
        unzip_tasks[i].set_extract_result(task_result);
    }

    Ok(unzip_tasks)
}


// Helper function to unzip a file from the source PathBuf and save it in the dest PathBuf
async fn unzip_file(src: PathBuf, dest: PathBuf, overwrite: bool) -> anyhow::Result<()> {

    // Make sure the source file exists
    if !src.exists() {
        anyhow::bail!("File does not exist: {:?}", src);
    }

    // Only proceed if the destination file does not exist or if it exists and the overwrite flag is set
    if !overwrite && dest.exists() {
        log::info!("File already exists: {:?}", dest);
        return Ok(());
    }

    // Make sure the parent directory of the destination exists
    if let Some(parent) = dest.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // Unzip the file
    // Open the source zip file asynchronously
    let mut file = BufReader::new(File::open(&src).await?);
    let mut zip_reader = ZipFileReader::with_tokio(&mut file).await?;

    // There SHOULD be only 1 entry in each zip file. If there are more, we will only process the first one.
    if zip_reader.file().entries().len() > 1 {
        log::warn!("Zip file contains more than one entry. Only the first entry will be processed - {:?}", src);
    }

    let mut entry_reader = zip_reader.reader_with_entry(0).await?;

    let writer = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&dest)
        .await?;

    futures_lite::io::copy(&mut entry_reader, &mut writer.compat_write())
        .await?;

    Ok(())    
}
