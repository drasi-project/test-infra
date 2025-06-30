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

use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::test_repo_storage::models::{BootstrapDataGeneratorDefinition, SourceChangeGeneratorDefinition, TestSourceDefinition};

use super::{GithubTestRepoConfig, CommonTestRepoConfig, RemoteTestRepoClient};

#[derive(Debug)]
pub struct GithubTestRepoClientSettings {
    pub force_cache_refresh: bool,
    pub owner: String,
    pub repo: String,
    pub branch: String,
    pub root_path: String,
    pub test_repo_id: String,
    pub token: Option<String>,
}

impl GithubTestRepoClientSettings {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: GithubTestRepoConfig) -> anyhow::Result<Self> {
        Ok(Self {
            force_cache_refresh: unique_config.force_cache_refresh,
            owner: unique_config.owner,
            repo: unique_config.repo,
            branch: unique_config.branch,
            root_path: unique_config.root_path,
            test_repo_id: common_config.id.clone(),
            token: unique_config.token,
        })
    }
}

#[derive(Debug)]
pub struct GithubTestRepoClient {
    pub settings: GithubTestRepoClientSettings,
    pub client: Client,
}

impl GithubTestRepoClient {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: GithubTestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
        log::debug!("Creating GithubTestRepoClient from common_config:{:?} and unique_config:{:?}, ", common_config, unique_config);

        let settings = GithubTestRepoClientSettings::new(common_config, unique_config).await?;
        log::trace!("Creating GithubTestRepoClient with settings: {:?}, ", settings);
        
        let mut client_builder = Client::builder()
            .user_agent("drasi-test-framework/1.0");
        
        // Add authorization header if token is provided
        if let Some(token) = &settings.token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("token {}", token))?
            );
            client_builder = client_builder.default_headers(headers);
        }
        
        let client = client_builder.build()?;
        
        Ok(Box::new(Self { settings, client }))
    }

    async fn get_file_content(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
            self.settings.owner,
            self.settings.repo,
            path,
            self.settings.branch
        );

        log::debug!("Fetching file from GitHub: {}", url);

        let response = self.client.get(&url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to fetch file from GitHub: {} - {}", response.status(), response.text().await.unwrap_or_default()));
        }

        let json: Value = response.json().await?;
        
        if let Some(content) = json.get("content").and_then(|c| c.as_str()) {
            // GitHub API returns base64 encoded content
            let decoded = base64::general_purpose::STANDARD.decode(content.replace('\n', ""))?;
            Ok(decoded)
        } else {
            Err(anyhow::anyhow!("No content found in GitHub API response"))
        }
    }

    async fn list_directory_contents(&self, path: &str) -> anyhow::Result<Vec<Value>> {
        let url = format!(
            "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
            self.settings.owner,
            self.settings.repo,
            path,
            self.settings.branch
        );

        log::debug!("Listing directory from GitHub: {}", url);

        let response = self.client.get(&url).send().await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to list directory from GitHub: {} - {}", response.status(), response.text().await.unwrap_or_default()));
        }

        let json: Value = response.json().await?;
        
        if let Some(items) = json.as_array() {
            Ok(items.clone())
        } else {
            Err(anyhow::anyhow!("Expected array response from GitHub API"))
        }
    }

    async fn download_bootstrap_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        log::debug!("Downloading Bootstrap Script Files from {:?} to {:?}", repo_folder, local_folder);

        let file_path_list = self.download_directory_files(&repo_folder, &local_folder).await?;
        log::trace!("Bootstrap Script Files: {:?}", file_path_list);

        // Group the files by the data type name, which is the parent folder name of the file
        let mut file_path_map = HashMap::new();
        for file_path in file_path_list {
            if let Some(parent) = file_path.parent() {
                if let Some(data_type_name) = parent.file_name().and_then(|name| name.to_str()) {
                    file_path_map.entry(data_type_name.to_string()).or_insert_with(Vec::new).push(file_path);
                }
            }
        }
        
        // Sort files within each data type
        for files in file_path_map.values_mut() {
            files.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
        }

        log::trace!("Bootstrap Script Map: {:?}", file_path_map);
        Ok(file_path_map)
    }

    async fn download_change_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        log::debug!("Downloading Source Change Script Files from {:?} to {:?}", repo_folder, local_folder);

        let mut file_path_list = self.download_directory_files(&repo_folder, &local_folder).await?;
        log::trace!("Change Scripts Files: {:?}", file_path_list);

        // Sort the list of files by the file name to get them in the correct order for processing
        file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(file_path_list)
    }

    async fn download_directory_files(&self, remote_path: &str, local_path: &PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        let mut downloaded_files = Vec::new();
        
        // Create the local directory if it doesn't exist
        if !local_path.exists() {
            tokio::fs::create_dir_all(local_path).await?;
        }

        let items = self.list_directory_contents(remote_path).await?;
        
        for item in items {
            if let (Some(name), Some(item_type), Some(path)) = (
                item.get("name").and_then(|n| n.as_str()),
                item.get("type").and_then(|t| t.as_str()),
                item.get("path").and_then(|p| p.as_str())
            ) {
                let local_item_path = local_path.join(name);
                
                match item_type {
                    "file" => {
                        if name.ends_with(".jsonl") {
                            log::debug!("Downloading file: {} -> {:?}", path, local_item_path);
                            let content = self.get_file_content(path).await?;
                            let mut file = File::create(&local_item_path).await?;
                            file.write_all(&content).await?;
                            downloaded_files.push(local_item_path);
                        }
                    },
                    "dir" => {
                        // Recursively download directory contents
                        let sub_files = self.download_directory_files(path, &local_item_path).await?;
                        downloaded_files.extend(sub_files);
                    },
                    _ => {}
                }
            }
        }
        
        Ok(downloaded_files)
    }
}

#[async_trait]
impl RemoteTestRepoClient for GithubTestRepoClient {
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()> {
        log::debug!("Copying TestDefinition - {:?} to folder {:?}", test_id, test_def_path);

        // If the TestDefinition already exists, return an error
        if test_def_path.exists() {
            return Err(anyhow::anyhow!("Test Definition ID: {} already exists in location {:?}", test_id, test_def_path));
        }   
        
        // Formulate the remote repo path for the test definition file
        let remote_path = format!("{}/{}.test.json", self.settings.root_path, test_id);
    
        // Download the test definition file
        let content = self.get_file_content(&remote_path).await?;
        let mut file = File::create(test_def_path).await?;
        file.write_all(&content).await?;

        Ok(())
    }

    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()> {
        match test_source_def {
            TestSourceDefinition::Script(def) => {
                log::debug!("Copying Test Source Content for {:?} to {:?}", def.common.test_source_id, test_source_data_path);

                // Bootstrap Data Script Files
                if let Some(BootstrapDataGeneratorDefinition::Script(bs_def)) = &def.bootstrap_data_generator {
                    let repo_path = format!(
                        "{}/{}/sources/{}/{}", 
                        self.settings.root_path, 
                        test_data_folder, 
                        def.common.test_source_id, 
                        &bs_def.script_file_folder
                    );
                    let local_path = test_source_data_path.join(&bs_def.script_file_folder);
                    self.download_bootstrap_script_files(repo_path, local_path).await?;
                }

                // Source Change Script Files
                if let Some(SourceChangeGeneratorDefinition::Script(sc_def)) = &def.source_change_generator {
                    let repo_path = format!(
                        "{}/{}/sources/{}/{}", 
                        self.settings.root_path, 
                        test_data_folder, 
                        def.common.test_source_id, 
                        &sc_def.script_file_folder
                    );
                    let local_path = test_source_data_path.join(&sc_def.script_file_folder);
                    self.download_change_script_files(repo_path, local_path).await?;
                }
            },
            _ => {}
        }

        Ok(())
    }
}