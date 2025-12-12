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

use std::fmt::{self, Display};
use std::str::FromStr;
use std::path::PathBuf;
use clap::{Args, ValueEnum};

#[derive(Args, Debug)]
pub struct DataSelectionArgs {
    /// The types of GDELT data to process
    #[arg(short = 't', long, value_enum, default_value = "event,graph,mention", value_delimiter=',')]
    pub data_type: Vec<DataType>,
    
    /// The datetime from which to start processing files.
    /// In the format YYYYMMDDHHMMSS. Defaults for missing fields
    #[arg(short = 's', long)]
    pub file_start: Option<String>,
    
    /// The datetime at which to stop processing files
    /// In the format YYYYMMDDHHMMSS. Defaults for missing fields
    #[arg(short = 'e', long)]
    pub file_end: Option<String>,
}

#[derive(Args, Debug)]
pub struct GenerateScriptsArgs {
    #[command(flatten)]
    pub data_selection: DataSelectionArgs,
    
    /// Output directory for generated scripts
    #[arg(short = 'o', long = "output", default_value = "./scripts_output")]
    pub output_path: PathBuf,
    
    /// Generate bootstrap scripts
    #[arg(short = 'b', long, default_value_t = true)]
    pub bootstrap: bool,
    
    /// Generate change scripts
    #[arg(short = 'c', long, default_value_t = true)]
    pub changes: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DataType {
    Event,
    Graph,
    Mention,
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.to_lowercase();
        match input.as_str() {
            "event" => Ok(DataType::Event),
            "graph" => Ok(DataType::Graph),
            "mention" => Ok(DataType::Mention),
            _ => Err(format!("Invalid data type: {}", input)),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataType::Event => write!(f, "event"),
            DataType::Graph => write!(f, "graph"),
            DataType::Mention => write!(f, "mention"),
        }
    }
}

#[derive(Debug)]
pub struct FileInfo {
    pub file_type: DataType,
    pub url: String,
    pub zip_path: PathBuf,
    pub unzip_path: PathBuf,
    pub overwrite: bool,
    pub download_result: Option<anyhow::Result<()>>,
    pub extract_result: Option<anyhow::Result<()>>,
    pub load_result: Option<anyhow::Result<()>>,
}

impl FileInfo {
    pub fn new_event_file(cache_folder_path: PathBuf, file_name: &str, overwrite: bool) -> Self {
        const GDELT_DATA_URL: &str = "http://data.gdeltproject.org/gdeltv2";
        Self {
            file_type: DataType::Event,
            url: format!("{}/{}.export.CSV.zip", GDELT_DATA_URL, file_name),
            zip_path: cache_folder_path.join(format!("zip/{}.export.CSV.zip", file_name)),
            unzip_path: cache_folder_path.join(format!("event/{}.export.CSV", file_name)),
            overwrite,
            download_result: None,
            extract_result: None,
            load_result: None,
        }
    }

    pub fn new_graph_file(cache_folder_path: PathBuf, file_name: &str, overwrite: bool) -> Self {
        const GDELT_DATA_URL: &str = "http://data.gdeltproject.org/gdeltv2";
        Self {
            file_type: DataType::Graph,
            url: format!("{}/{}.gkg.csv.zip", GDELT_DATA_URL, file_name),
            zip_path: cache_folder_path.join(format!("zip/{}.gkg.csv.zip", file_name)),
            unzip_path: cache_folder_path.join(format!("graph/{}.gkg.CSV", file_name)),
            overwrite,
            download_result: None,
            extract_result: None,
            load_result: None,
        }
    }

    pub fn new_mention_file(cache_folder_path: PathBuf, file_name: &str, overwrite: bool) -> Self {
        const GDELT_DATA_URL: &str = "http://data.gdeltproject.org/gdeltv2";
        Self {
            file_type: DataType::Mention,
            url: format!("{}/{}.mentions.CSV.zip", GDELT_DATA_URL, file_name),
            zip_path: cache_folder_path.join(format!("zip/{}.mentions.CSV.zip", file_name)),
            unzip_path: cache_folder_path.join(format!("mention/{}.mentions.CSV", file_name)),
            overwrite,
            download_result: None,
            extract_result: None,
            load_result: None,
        }
    }

    pub fn set_download_result(&mut self, result: anyhow::Result<()>) {
        self.download_result = Some(result);
    }

    pub fn set_extract_result(&mut self, result: anyhow::Result<()>) {
        self.extract_result = Some(result);
    }

    pub fn set_load_result(&mut self, result: anyhow::Result<()>) {
        self.load_result = Some(result);
    }
}