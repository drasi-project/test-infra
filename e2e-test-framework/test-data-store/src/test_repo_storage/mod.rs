use std::{collections::HashMap, path::PathBuf};

use models::TestDefinition;
use serde::Serialize;
use tokio::fs;
use walkdir::WalkDir;

use repo_clients::{create_test_repo_client, RemoteTestRepoClient, TestRepoConfig};

pub mod repo_clients;
pub mod models;

const TEST_SOURCES_FOLDER_NAME: &str = "sources";

#[derive(Clone, Debug)]
pub struct TestRepoStore {
    pub path: PathBuf,
    pub test_repos: HashMap<String, TestRepoConfig>,
}

impl TestRepoStore {
    pub async fn new(folder_name: String, parent_path: PathBuf, replace: bool, initial_repos: Option<Vec<TestRepoConfig>>) -> anyhow::Result<Self> {

        let path = parent_path.join(&folder_name);
        log::debug!("Creating (replace = {}) TestRepoStore in folder: {:?}", replace, &path);

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
        }

        let mut store = Self {
            path,
            test_repos: HashMap::new(),
        };

        if !initial_repos.is_none() {
            for repo in initial_repos.unwrap() {
                store.add_test_repo(repo, false).await?;
            }    
        }

        Ok(store)
    }

    pub async fn add_test_repo(&mut self, repo_config: TestRepoConfig, replace: bool) -> anyhow::Result<TestRepoStorage> {
        log::debug!("Adding (replace = {}) Test Repo: {:?}", replace, &repo_config);

        let id = repo_config.get_id();
        let repo_path = self.path.join(&id);

        if replace && repo_path.exists() {
            fs::remove_dir_all(&repo_path).await?;
        }

        if !repo_path.exists() {
            fs::create_dir_all(&repo_path).await?;
            self.test_repos.insert(id.clone(), repo_config.clone());
        }

        Ok(TestRepoStorage {
            id: id.to_string(),
            path: repo_path,
            repo_config: repo_config,
        })
    }

    pub async fn contains_test_repo(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.path.join(&id).exists())
    }

    pub async fn get_test_repo_storage(&self, id: &str) -> anyhow::Result<TestRepoStorage> {
        if self.path.join(&id).exists() {
            Ok(TestRepoStorage {
                id: id.to_string(),
                path: self.path.join(&id),
                repo_config: self.test_repos.get(id).unwrap().clone()
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
    pub repo_config: TestRepoConfig,
}

impl TestRepoStorage {
    pub async fn add_local_test(&self, test_def: TestDefinition, replace: bool) -> anyhow::Result<TestStorage> {
        log::debug!("Adding Local ((replace = {}) ) Test {:?}", replace, &test_def);

        let test_def_path = self.path.join(format!("{}.test", &test_def.test_id));
        let test_path = self.path.join(&test_def.test_id);

        if replace {
            if test_def_path.exists() {
                fs::remove_file(&test_def_path).await?;
            }

            if test_path.exists() {
                fs::remove_dir_all(&test_path).await?;
            }
        } else {
            if test_def_path.exists() {
                anyhow::bail!("Test with ID {:?} already exists", &test_def.test_id);
            }
        }

        if !test_def_path.exists() {
            // Write the test definition to a file.
            let json_content = serde_json::to_string_pretty(&test_def)?;
            fs::write(test_def_path.clone(), json_content).await?;

            self.get_test_storage(&test_def.test_id).await
        } else {
            self.get_test_storage(&test_def.test_id).await
        }
    }

    pub async fn add_remote_test(&self, id: &str, replace: bool) -> anyhow::Result<TestStorage> {
        log::debug!("Adding Remote ((replace = {}) ) Test ID {:?}", replace, &id);

        let test_def_path = self.path.join(format!("{}.test", id));
        let test_path = self.path.join(id);

        if replace {
            if test_def_path.exists() {
                fs::remove_file(&test_def_path).await?;
            }

            if test_path.exists() {
                fs::remove_dir_all(&test_path).await?;
            }
        }

        if !test_def_path.exists() {
            // Download the test definition from the remote test repo.
            let test_repo_client = create_test_repo_client(self.repo_config.clone()).await?;
            test_repo_client.copy_test_definition(id.to_string(), test_def_path).await?;

            self.get_test_storage(id).await
        } else {
            self.get_test_storage(id).await
        }
    }

    pub async fn contains_test(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.path.join(&id).exists())
    }

    pub async fn get_test_definition(&self, id: &str) -> anyhow::Result<TestDefinition> {
        log::debug!("Getting Test Definition for ID {:?}", id);

        let test_definition_path = self.path.join(format!("{}.test", id));
        log::trace!("Looking in {:?}", test_definition_path);


        if !test_definition_path.exists() {
            anyhow::bail!("Test with ID {:?} not found", &id);
        } else {
            // Read the test definition file into a string.
            let json_content = fs::read_to_string(test_definition_path).await?;
            Ok(serde_json::from_str(&json_content)?)
        }
    }

    pub async fn get_test_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut tests = Vec::new();

        let mut entries = fs::read_dir(&self.path).await?;     
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

    pub async fn get_test_storage(&self, id: &str) -> anyhow::Result<TestStorage> {
        log::debug!("Getting Test Storage for ID {:?}", id);

        let test_path = self.path.join(&id);
        let test_definition_path = self.path.join(format!("{}.test", id));

        if !test_definition_path.exists() {
            anyhow::bail!("Test with ID {:?} not found", &id);
        } else {
            // Read the test definition file into a string.
            let json_content = fs::read_to_string(test_definition_path).await?;
            let test_definition: models::TestDefinition = serde_json::from_str(&json_content)?;

            Ok(TestStorage {                            
                client_config: self.repo_config.clone(),    
                id: id.to_string(),
                path: test_path.clone(),
                repo_id: self.id.clone(),
                sources_path: test_path.join(TEST_SOURCES_FOLDER_NAME),
                test_definition,
            })    
        }
    }
}

#[derive(Clone, Debug)]
pub struct TestStorage {
    pub client_config: TestRepoConfig,
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub sources_path: PathBuf,
    pub test_definition: models::TestDefinition,
}

impl TestStorage {
    pub async fn contains_test_source(&self, id: &str) -> anyhow::Result<bool> {
        Ok(self.sources_path.join(&id).exists())
    }

    pub async fn get_test_source(&self, id: &str, replace: bool) -> anyhow::Result<TestSourceStorage> {
        log::debug!("Getting (replace = {}) TestSourceStorage for ID {:?}", replace, &id);

        let test_source_definition = match self.test_definition.sources.iter().find(|source| source.test_source_id == id) {
            Some(source) => source,
            None => anyhow::bail!("Test Source with ID {:?} not found in Test ID {:?}", &id, &self.id),
        };

        let test_source_data_path = self.sources_path.join(&id);

        if replace && test_source_data_path.exists() {
            fs::remove_dir_all(&test_source_data_path).await?;
        }

        if !test_source_data_path.exists() {
            // fs::create_dir_all(&test_source_data_path).await?;
            
            // Download the Test Source Content from the repo.
            create_test_repo_client(self.client_config.clone()).await?
                .copy_test_source_content(
                    self.id.clone(),
                    test_source_definition, 
                    test_source_data_path.clone(),
                ).await?;
        }

        Ok(TestSourceStorage {
            id: id.to_string(),
            path: test_source_data_path,
            repo_id: self.repo_id.to_string(),
            test_id: self.id.to_string(),
            test_source_definition: test_source_definition.to_owned(),
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
#[derive(Clone, Debug, Serialize)]
pub struct TestSourceStorage {
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub test_id: String,
    pub test_source_definition: models::TestSourceDefinition,
}

impl TestSourceStorage {
    pub async fn get_script_files(&self) -> anyhow::Result<TestSourceScriptSet> {

        let mut bootstrap_data_script_files = HashMap::new();
        let mut source_change_script_files = Vec::new();

        // Read the bootstrap script files.
        match &self.test_source_definition.bootstrap_data_generator_def {
            Some(models::BootstrapDataGeneratorDefinition::Script{common_config: _, unique_config}) => {
                let bootstrap_data_scripts_repo_path = self.path.join(&unique_config.script_file_folder);

                let file_path_list: Vec<PathBuf> = WalkDir::new(&bootstrap_data_scripts_repo_path)
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
            },
            _ => {}
        }

        // Read the change log script files.
        match &self.test_source_definition.source_change_generator_def {
            Some(models::SourceChangeGeneratorDefinition::Script{common_config: _, unique_config}) => {
                let source_change_scripts_repo_path = self.path.join(&unique_config.script_file_folder);

                let mut entries = fs::read_dir(&source_change_scripts_repo_path).await?;
        
                while let Some(entry) = entries.next_entry().await? {
                    let file_path = entry.path();
            
                    // Check if it's a file
                    if file_path.is_file() {
                        source_change_script_files.push(file_path);
                    }
                }
            },
            _ => {}
        }

        // Sort the list of files by the file name to get them in the correct order for processing.
        source_change_script_files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(TestSourceScriptSet {
            bootstrap_data_script_files,
            source_change_script_files,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestSourceScriptSet {
    pub bootstrap_data_script_files: HashMap<String, Vec<PathBuf>>,
    pub source_change_script_files: Vec<PathBuf>,
}