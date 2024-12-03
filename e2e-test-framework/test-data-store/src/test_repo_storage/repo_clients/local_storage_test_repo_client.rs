
use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use tokio::fs;
use walkdir::WalkDir;

use crate::test_repo_storage::{models::TestDefinition, TestSourceDataset};

use super::{CommonTestRepoConfig, LocalStorageTestRepoConfig, RemoteTestRepoClient};

#[derive(Debug)]
pub struct LocalStorageTestRepoClientSettings {
    pub force_cache_refresh: bool,
    pub root_path: PathBuf,
    pub test_repo_id: String,
}

impl LocalStorageTestRepoClientSettings {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: LocalStorageTestRepoConfig) -> anyhow::Result<Self> {

        Ok(Self {
            force_cache_refresh: common_config.force_cache_refresh,
            root_path: unique_config.root_path.into(),
            test_repo_id: common_config.id.clone(),
        })
    }
}

#[derive(Debug)]
pub struct LocalStorageTestRepoClient {
    pub settings: LocalStorageTestRepoClientSettings,
}

impl LocalStorageTestRepoClient {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: LocalStorageTestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
        log::debug!("Creating LocalStorageTestRepoClient from common_config:{:?} and unique_config:{:?}, ", common_config, unique_config);

        let settings = LocalStorageTestRepoClientSettings::new(common_config, unique_config).await?;
        log::trace!("Creating LocalStorageTestRepoClient with settings: {:?}, ", settings);
        
        Ok(Box::new( Self { settings }))
    }
}

#[async_trait]
impl RemoteTestRepoClient for LocalStorageTestRepoClient {
    async fn get_test_definition(&self, test_id: String, test_store_path: PathBuf) -> anyhow::Result<PathBuf> {
        log::trace!("Getting TestDefinition - {:?} to folder {:?}", test_id, test_store_path);

        // Formulate the repo path for the test definition file
        let repo_path = self.settings.root_path.join(format!("{}.test", test_id));
    
        // Formulate destination path for test definition file
        let dest_path = test_store_path.join(format!("{}.test", test_id));
    
        // Copy the test definition file
        fs::copy(repo_path, dest_path.clone()).await?;

        Ok(dest_path)
    }

    async fn get_test_source_content_from_def(&self, test_def: &TestDefinition, source_id: String, bootstrap_data_store_path: PathBuf, source_change_store_path: PathBuf) -> anyhow::Result<TestSourceDataset> {
        log::trace!("Downloading Test Source Content for {:?}", source_id);

        let mut bootstrap_data_script_files: HashMap<String, Vec<PathBuf>> = HashMap::new();
        let mut source_change_script_files = Vec::new();

        // Bootstrap Data Script Files
        // Formulate the repo path for the bootstrap script files
        let bootstrap_data_scripts_folder = test_def.sources.iter().find(|s| s.id == source_id).unwrap().bootstrap_data_generator.script_file_folder.clone();
        let bootstrap_data_scripts_repo_path = self.settings.root_path.join(format!("{}/sources/{}/{}/", test_def.id, source_id, bootstrap_data_scripts_folder));
        
        // Read the set bootstrap script files from the repo.
        let file_path_list: Vec<PathBuf> = WalkDir::new(bootstrap_data_scripts_repo_path)
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

            // Formulate destination path for bootstrap script file
            let data_type_name = file_path.parent().unwrap().file_name().unwrap().to_str().unwrap().to_string();
            let file_name = file_path.file_name().unwrap().to_str().unwrap().to_string();
            let dest_path = bootstrap_data_store_path.join(format!("{}/{}", data_type_name, file_name));
            
            // Make sure the destination folder exists and copy the file
            fs::create_dir_all(dest_path.parent().unwrap()).await?;
            fs::copy(file_path, dest_path.clone()).await?;

            // Add the file path to the dataset
            if !bootstrap_data_script_files.contains_key(&data_type_name) {
                bootstrap_data_script_files.insert(data_type_name.clone(), vec![]);
            }
            bootstrap_data_script_files.get_mut(&data_type_name).unwrap().push(dest_path);
        }

        // Change Script Files
        // Formulate the repo path for the change script files
        let source_change_scripts_folder = test_def.sources.iter().find(|s| s.id == source_id).unwrap().source_change_generator.script_file_folder.clone();
        let source_change_scripts_repo_path = self.settings.root_path.join(format!("{}/sources/{}/{}/", test_def.id, source_id, source_change_scripts_folder));

        // TODO: Currently we only have a single folder name. In the future we might have a list of files.
        // Get the list of files in the change script folder
        for entry in WalkDir::new(source_change_scripts_repo_path) {
            match entry {
                Ok(e) => {
                    if e.path().is_file() {
                        // Formulate destination path for change script file
                        let dest_path = source_change_store_path.join(e.path().file_name().unwrap());
                    
                        // Copy the change script file
                        fs::copy(e.path(), dest_path.clone()).await?;
        
                        source_change_script_files.push(dest_path);
                    }
                },
                Err(_) => continue, // Skip entries that cause errors
            };
        }

        Ok(TestSourceDataset {
            source_change_script_files,
            bootstrap_data_script_files,
        })
    }
}