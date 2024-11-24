use std::{collections::{HashMap, HashSet}, path::PathBuf};

use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

use repo_clients::{create_test_repo_client, RemoteTestRepoClient, RemoteTestRepoConfig};

pub mod repo_clients;
pub mod scripts;
pub mod test_metadata;

const TESTS_FOLDER_NAME: &str = "tests";
const TEST_SOURCES_FOLDER_NAME: &str = "sources";
const BOOTSTRAP_DATA_SCRIPTS_FOLDER_NAME: &str = "bootstrap_data_scripts";
const SOURCE_CHANGE_SCRIPTS_FOLDER_NAME: &str = "source_change_scripts";

#[derive(Clone, Debug)]
pub struct TestRepoStore {
    pub path: PathBuf,
    pub remote_test_repos: HashMap<String, RemoteTestRepoConfig>,
}

impl TestRepoStore {
    pub async fn new(folder_name: String, parent_path: PathBuf, replace: bool, initial_repos: Option<Vec<RemoteTestRepoConfig>>) -> anyhow::Result<Self> {

        let path = parent_path.join(&folder_name);
        log::info!("Creating (replace = {}) TestRepoStore in folder: {:?}", replace, &path);

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
        }

        let mut store = Self {
            path,
            remote_test_repos: HashMap::new(),
        };

        if !initial_repos.is_none() {
            for repo in initial_repos.unwrap() {
                store.add_test_repo(repo, false).await?;
            }    
        }

        Ok(store)
    }

    pub async fn add_test_repo(&mut self, repo_config: RemoteTestRepoConfig, replace: bool) -> anyhow::Result<TestRepoStorage> {

        let id = repo_config.get_id();
        log::info!("Adding (replace = {}) TestRepoStorage for Test Repo: {:?}", replace, &id);

        let repo_path = self.path.join(&id);
        let tests_path = repo_path.join(TESTS_FOLDER_NAME);

        if replace && repo_path.exists() {
            fs::remove_dir_all(&repo_path).await?;
        }

        if !repo_path.exists() {
            // fs::create_dir_all(&path).await?;
            fs::create_dir_all(&tests_path).await?;

            self.remote_test_repos.insert(id.clone(), repo_config.clone());
        }

        Ok(TestRepoStorage {
            id: id.to_string(),
            path: repo_path,
            repo_config: repo_config,
            tests_path,
        })
    }

    pub async fn contains_test_repo(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.path.join(&id).exists())
    }

    pub async fn get_test_repo(&self, id: &str) -> anyhow::Result<TestRepoStorage> {
        if self.path.join(&id).exists() {
            Ok(TestRepoStorage {
                id: id.to_string(),
                path: self.path.join(&id),
                repo_config: self.remote_test_repos.get(id).unwrap().clone(),
                tests_path: self.path.join(&id).join(TESTS_FOLDER_NAME),
            })
        } else {
            anyhow::bail!("Test Repo with ID {:?} not found", &id);
        }
    }

    pub async fn get_test_repo_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_repo_ids = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_repo_ids.push(folder_name.to_string());
                }
            }
        }

        Ok(test_repo_ids)
    }    
}

#[derive(Clone, Debug)]
pub struct TestRepoStorage {
    pub id: String,
    pub path: PathBuf,
    pub repo_config: RemoteTestRepoConfig,
    pub tests_path: PathBuf,
}

impl TestRepoStorage {
    pub async fn contains_test(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.tests_path.join(&id).exists())
    }

    pub async fn get_test(&self, id: &str, replace: bool) -> anyhow::Result<TestStorage> {
        log::debug!("Getting ((replace = {}) ) TestStorage for ID {:?}", replace, &id);

        let test_path = self.tests_path.join(&id);
        let sources_path = test_path.join(TEST_SOURCES_FOLDER_NAME);

        if replace && test_path.exists() {
            fs::remove_dir_all(&test_path).await?;
        }

        if !test_path.exists() {
            // fs::create_dir_all(&path).await?;
            fs::create_dir_all(&sources_path).await?;

            // Download the test definition from the remote test repo.
            let test_repo_client = create_test_repo_client(self.repo_config.clone()).await?;
            let test_definition_path = test_repo_client.get_test_definition(id.to_string(), test_path.clone()).await?;

            // Read the test definition file into a string.
            let json_content = fs::read_to_string(test_definition_path).await?;
            let test_definition: test_metadata::TestDefinition = serde_json::from_str(&json_content)?;

            Ok(TestStorage {
                client_config: self.repo_config.clone(),
                id: id.to_string(),
                path: test_path,
                repo_id: self.id.clone(),
                sources_path,
                test_definition,
                
            })    
        } else {

            // Read the test definition file into a string.
            let test_definition_path = test_path.join(format!("{}.test", id));
            let json_content = fs::read_to_string(test_definition_path).await?;
            let test_definition: test_metadata::TestDefinition = serde_json::from_str(&json_content)?;

            Ok(TestStorage {                            
                client_config: self.repo_config.clone(),    
                id: id.to_string(),
                path: test_path,
                repo_id: self.id.clone(),
                sources_path,
                test_definition,
            })    
        }
    }

    pub async fn get_test_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut tests = Vec::new();

        let mut entries = fs::read_dir(&self.tests_path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    tests.push(folder_name.to_string());
                }
            }
        }

        Ok(tests)        
    }
}

#[derive(Clone, Debug)]
pub struct TestStorage {
    pub client_config: RemoteTestRepoConfig,
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub sources_path: PathBuf,
    pub test_definition: test_metadata::TestDefinition,
}

impl TestStorage {
    pub async fn contains_test_source(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.sources_path.join(&id).exists())
    }

    pub async fn get_test_source(&self, id: &str, replace: bool) -> anyhow::Result<TestSourceStorage> {
        log::debug!("Getting (replace = {}) TestSourceStorage for ID {:?}", replace, &id);

        let source_path = self.sources_path.join(&id);
        let bootstrap_data_scripts_path = source_path.join(BOOTSTRAP_DATA_SCRIPTS_FOLDER_NAME);            
        let source_change_scripts_path = source_path.join(SOURCE_CHANGE_SCRIPTS_FOLDER_NAME);

        if replace && source_path.exists() {
            fs::remove_dir_all(&source_path).await?;
        }

        if !source_path.exists() {
            // fs::create_dir_all(&path).await?;
            fs::create_dir_all(&bootstrap_data_scripts_path).await?;
            fs::create_dir_all(&source_change_scripts_path).await?;

            // Download the Test Source Content from the repo.
            create_test_repo_client(self.client_config.clone()).await?
            .get_test_source_content_from_def(
                &self.test_definition, 
                id.to_string(), 
                bootstrap_data_scripts_path.clone(),
                source_change_scripts_path.clone()
            ).await?;
        }

        Ok(TestSourceStorage {
            bootstrap_data_scripts_path,
            source_change_scripts_path,
            id: id.to_string(),
            path: source_path,
            repo_id: self.repo_id.to_string(),
            test_id: self.id.to_string(),
        })
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
}

#[allow(unused)]
pub struct TestSourceStorage {
    pub bootstrap_data_scripts_path: PathBuf,
    pub source_change_scripts_path: PathBuf,
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub test_id: String,
}

impl TestSourceStorage {
    pub async fn get_dataset(&self) -> anyhow::Result<TestSourceDataset> {

        let mut bootstrap_data_script_files = HashMap::new();
        let mut source_change_script_files = Vec::new();

        // Read the bootstrap script files.
        let file_path_list: Vec<PathBuf> = WalkDir::new(&self.bootstrap_data_scripts_path)
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
            if !bootstrap_data_script_files.contains_key(&data_type_name) {
                bootstrap_data_script_files.insert(data_type_name.clone(), vec![]);
            }
            bootstrap_data_script_files.get_mut(&data_type_name).unwrap().push(file_path);
        }

        // Read the change log script files.
        let mut entries = fs::read_dir(&self.source_change_scripts_path).await?;
    
        while let Some(entry) = entries.next_entry().await? {
            let file_path = entry.path();
    
            // Check if it's a file
            if file_path.is_file() {
                source_change_script_files.push(file_path);
            }
        }

        // Sort the list of files by the file name to get them in the correct order for processing.
        source_change_script_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(TestSourceDataset {
            bootstrap_data_script_files,
            source_change_script_files,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestSourceDataset {
    pub bootstrap_data_script_files: HashMap<String, Vec<PathBuf>>,
    pub source_change_script_files: Vec<PathBuf>,
}

impl TestSourceDataset {
    pub fn count_bootstrap_type_intersection(&self, requested_labels: &HashSet<String>) -> usize {
        let mut match_count = 0;

        // Iterate through the requested labels (data types) and count the number of matches.
        self.bootstrap_data_script_files.keys().filter(|key| requested_labels.contains(*key)).for_each(|_| {
            match_count += 1;
        });

        match_count
    }
}

