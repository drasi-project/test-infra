use std::path::PathBuf;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use clap::{Args, Parser, Subcommand, ValueEnum};

/// String constant containing the address of GDELT data on the Web
const _GDELT_DATA_URL: &str = "http://data.gdeltproject.org/gdeltv2";

/// String constant representing the default GDELT data cache folder path
/// This is the folder where GDELT data files are downloaded and stored if not provided by the user.
const _DEFAULT_CACHE_FOLDER_PATH: &str = "./gdelt_data_cache";

/// Enum representing the different types of GDELT data that can be downloaded
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum DataType {
    Event,
    Graph,
    Mention,
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
        #[arg(short = 'o', long, default_value_t = true)]
        overwrite: bool,

        /// A flag to indicate whether the downloaded files should be unzipped
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

fn main() {
    let params = Params::parse();

    match params.command {
        Commands::Get { data_selection, overwrite, unzip } => {
            handle_get_command(data_selection, overwrite, unzip);
        }
        Commands::Unzip { data_selection, overwrite } => {
            handle_unzip_command(data_selection, overwrite);
        }
        Commands::Load { data_selection, database_url, database_user, database_password } => {
            handle_load_command(data_selection, database_url, database_user, database_password);
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

fn handle_get_command(data_selection: DataSelectionArgs, overwrite: bool, unzip: bool) {
    println!("Get command:");
    match data_selection.file_start {
        Some(start) => {
            println!("  - Start: {:?}, Adjusted Start: {:?}", &start, parse_start_datetime(&start).unwrap());
        }
        None => println!("  - Start: None"),
        
    };
    match data_selection.file_end {
        Some(end) => {
            println!("  - End: {:?}, Adjusted End: {:?}", &end, parse_end_datetime(&end).unwrap());
        }
        None => println!("  - End: None"),
        
    };
    println!("  - Data Types: {:?}", data_selection.data_type);
    println!("  - Overwrite: {}", overwrite);
    println!("  - Unzip: {}", unzip);
}

fn handle_unzip_command(data_selection: DataSelectionArgs, overwrite: bool) {
    println!("Unzip command:");
    match data_selection.file_start {
        Some(start) => {
            println!("  - Start: {:?}, Adjusted Start: {:?}", &start, parse_start_datetime(&start).unwrap());
        }
        None => println!("  - Start: None"),
        
    };
    match data_selection.file_end {
        Some(end) => {
            println!("  - End: {:?}, Adjusted End: {:?}", &end, parse_end_datetime(&end).unwrap());
        }
        None => println!("  - End: None"),
        
    };
    println!("  - Data Types: {:?}", data_selection.data_type);
    println!("  - Overwrite: {}", overwrite);
}

fn handle_load_command(data_selection: DataSelectionArgs, database_url: Option<String>, database_user: Option<String>, database_password: Option<String>) {
    println!("Load command:");
    match data_selection.file_start {
        Some(start) => {
            println!("  - Start: {:?}, Adjusted Start: {:?}", &start, parse_start_datetime(&start).unwrap());
        }
        None => println!("  - Start: None"),
        
    };
    match data_selection.file_end {
        Some(end) => {
            println!("  - End: {:?}, Adjusted End: {:?}", &end, parse_end_datetime(&end).unwrap());
        }
        None => println!("  - End: None"),
        
    };
    println!("  - Data Types: {:?}", data_selection.data_type);
    println!("  - Database URL: {:?}", database_url);
    println!("  - Database User: {:?}", database_user);
    println!("  - Database Password: {:?}", database_password);
}