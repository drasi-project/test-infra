use std::{collections::{HashMap, HashSet}, path::PathBuf};

use repo_clients::{create_test_repo_client, RemoteTestRepoClient, RemoteTestRepoConfig};
use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

pub mod repo_clients;

#[derive(Clone, Debug)]
pub struct TestRepoStorage {
    pub config: RemoteTestRepoConfig,
    pub id: String,
    pub path: PathBuf,
}

impl TestRepoStorage {
    pub async fn new(config: RemoteTestRepoConfig, path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating TestRepoStorage in {:?} from config: {:?}", &path, &config);

        // Create the data store folder if it doesn't exist.
        if !path.exists() {
            log::info!("Creating TestRepoStorage folder: {:?}", &path);
            tokio::fs::create_dir_all(&path).await?
        }
        
        let id = config.get_id();

        Ok(TestRepoStorage {
            config,
            id,
            path,
        })
    }

    pub async fn get_test_source_storage(&self, test_id: &str, source_id: &str) -> anyhow::Result<TestSourceStorage> {
        log::debug!("Getting TestSourceStorage for ID: {:?}/{:?}", &test_id, &source_id);

        let mut path = self.path.clone();
        path.push(format!("sources/{}/{}/", &test_id, &source_id));        
        
        let source_storage = TestSourceStorage::new(test_id, source_id, path).await?;

        // Download the test source dataset from the remote test repo.
        let test_repo_client = create_test_repo_client(self.config.clone(), source_storage.path.clone()).await?;

        test_repo_client.download_test_source_dataset(test_id.to_string(), source_id.to_string(), source_storage.path.clone()).await?;

        Ok(source_storage)
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_sources = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_sources.push(folder_name.to_string());
                }
            }
        }

        Ok(test_sources)        
    }
}

#[derive(Clone, Debug)]
pub struct TestSourceStorage {
    pub bootstrap_scripts_path: PathBuf,
    pub change_scripts_path: PathBuf,
    pub id: String,
    pub source_id: String,
    pub test_id: String,
    pub path: PathBuf,
}

impl TestSourceStorage {
    pub async fn new(test_id: &str, source_id: &str, path: PathBuf) -> anyhow::Result<Self> {
        log::debug!("Creating TestSourceStorage for ID {:?}/{:?} in folder: {:?}", &test_id, &source_id, &path);

        // Create the data storage folders if they don't exist.
        // Need a folder for the bootstrap scripts.
        let bootstrap_scripts_path = path.join("bootstrap_scripts");            
        if !bootstrap_scripts_path.exists() {
            fs::create_dir_all(&bootstrap_scripts_path).await?;
        }

        // Need a folder for the change log scripts.
        let change_scripts_path = path.join("change_scripts");
        if !change_scripts_path.exists() {
            fs::create_dir_all(&change_scripts_path).await?;
        }

        Ok(Self {
            bootstrap_scripts_path,
            change_scripts_path,
            id: format!("{}/{}", test_id, source_id),
            source_id: source_id.to_string(),
            test_id: test_id.to_string(),
            path,
        })
    }

    pub async fn get_dataset(&self) -> anyhow::Result<TestSourceDataset> {

        let mut bootstrap_script_files = HashMap::new();
        let mut change_log_script_files = Vec::new();

        // Read the bootstrap script files.
        let file_path_list: Vec<PathBuf> = WalkDir::new(&self.bootstrap_scripts_path)
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?; // Skip over any errors
            let path = entry.path().to_path_buf();
            if path.is_file() {
                Some(path)
            } else {
                None
            }
        })
        .collect();

        for file_path in file_path_list {
            let data_type_name = file_path.parent().unwrap().file_name().unwrap().to_str().unwrap().to_string();
            if !bootstrap_script_files.contains_key(&data_type_name) {
                bootstrap_script_files.insert(data_type_name.clone(), vec![]);
            }
            bootstrap_script_files.get_mut(&data_type_name).unwrap().push(file_path);
        }

        // Read the change log script files.
        let mut entries = fs::read_dir(&self.change_scripts_path).await?;
    
        while let Some(entry) = entries.next_entry().await? {
            let file_path = entry.path();
    
            // Check if it's a file
            if file_path.is_file() {
                change_log_script_files.push(file_path);
            }
        }

        // Sort the list of files by the file name to get them in the correct order for processing.
        change_log_script_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(TestSourceDataset {
            bootstrap_script_files,
            change_log_script_files,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestSourceDataset {
    pub bootstrap_script_files: HashMap<String, Vec<PathBuf>>,
    pub change_log_script_files: Vec<PathBuf>,
}

impl TestSourceDataset {
    pub fn count_bootstrap_type_intersection(&self, requested_labels: &HashSet<String>) -> usize {
        let mut match_count = 0;

        // Iterate through the requested labels (data types) and count the number of matches.
        self.bootstrap_script_files.keys().filter(|key| requested_labels.contains(*key)).for_each(|_| {
            match_count += 1;
        });

        match_count
    }
}