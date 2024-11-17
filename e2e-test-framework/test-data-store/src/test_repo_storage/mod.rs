use std::{collections::{HashMap, HashSet}, path::PathBuf};

use repo_clients::{create_test_repo_client, RemoteTestRepoClient, RemoteTestRepoConfig};
use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

pub mod repo_clients;

fn get_test_source_id(test_id: &str, source_id: &str) -> String {
    format!("{}__{}", test_id, source_id)
}

#[derive(Clone, Debug)]
pub struct TestRepoStorage {
    pub config: RemoteTestRepoConfig,
    pub id: String,
    pub path: PathBuf,
    pub sources_path: PathBuf,
}

impl TestRepoStorage {
    pub async fn new(config: RemoteTestRepoConfig, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        log::debug!("Creating TestRepoStorage in {:?} from config: {:?}", &parent_path, &config);

        let id = config.get_id();

        let path = parent_path.join(&id);
        let sources_path = path.join("sources/");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&sources_path).await?;
        }

        Ok(TestRepoStorage {
            config,
            id,
            path,
            sources_path,
        })
    }

    pub async fn add_test_source(&self, test_id: &str, source_id: &str, replace: bool) -> anyhow::Result<TestSourceStorage> {
        log::debug!("Adding TestSourceStorage for ID: {:?}__{:?}", &test_id, &source_id);

        if !replace {
            self.get_test_source_storage(test_id, source_id).await
        } else {
            let source_storage = TestSourceStorage::new(test_id, source_id, self.sources_path.clone(), replace).await?;

            // Download the test source dataset from the remote test repo.
            let test_repo_client = create_test_repo_client(self.config.clone(), source_storage.path.clone()).await?;

            test_repo_client.download_test_source_dataset(test_id.to_string(), source_id.to_string(), source_storage.path.clone()).await?;

            Ok(source_storage)
        }
    }

    pub async fn get_test_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_sources = Vec::new();

        let mut entries = fs::read_dir(&self.sources_path).await?;     
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

    pub async fn get_test_source_storage(&self, test_id: &str, source_id: &str) -> anyhow::Result<TestSourceStorage> {
        let id = get_test_source_id(test_id, source_id);
        log::debug!("Getting TestSourceStorage for ID: {:?}", &id);

        match TestSourceStorage::try_get(test_id, source_id, self.sources_path.clone())? {
            Some(storage) => Ok(storage),
            None => Err(anyhow::anyhow!("TestSourceStorage not found for ID: {:?}", &id))
        }
    }
}

#[derive(Clone, Debug)]
pub struct TestSourceStorage {
    pub bootstrap_scripts_path: PathBuf,
    pub change_scripts_path: PathBuf,
    pub id: String,
    pub path: PathBuf,
    pub source_id: String,
    pub test_id: String,
}

impl TestSourceStorage {
    pub async fn new(test_id: &str, source_id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        let id = get_test_source_id(test_id, source_id);
        log::debug!("Creating TestSourceStorage for ID {:?} in folder: {:?}", &id, &parent_path);

        let path = parent_path.join(&id);
        let bootstrap_scripts_path = path.join("bootstrap_scripts");            
        let change_scripts_path = path.join("change_scripts");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&bootstrap_scripts_path).await?;
            fs::create_dir_all(&change_scripts_path).await?;
        }

        Ok(Self {
            bootstrap_scripts_path,
            change_scripts_path,
            id,
            path,
            source_id: source_id.to_string(),
            test_id: test_id.to_string(),
        })
    }

    pub fn try_get(test_id: &str, source_id: &str, parent_path: PathBuf) -> anyhow::Result<Option<Self>> {
        let id = get_test_source_id(test_id, source_id);
        let path = parent_path.join(&id);
        
        if path.exists() {
            Ok(Some(Self {
                bootstrap_scripts_path: path.join("bootstrap_scripts"),
                change_scripts_path: path.join("change_scripts"),
                id,
                path,
                source_id: source_id.to_string(),
                test_id: test_id.to_string(),
            }))
        } else {
            Ok(None)
        }
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