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

use std::{collections::HashMap, path::PathBuf, pin::Pin};

use async_trait::async_trait;
use base64::Engine;
use reqwest::Client;
use serde_json::Value;
use tokio::{fs::File, io::AsyncWriteExt};
use base64::engine::general_purpose;

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
        
        // Add headers
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-GitHub-Api-Version",
            reqwest::header::HeaderValue::from_static("2022-11-28")
        );
        
        // Add authorization header if token is provided
        if let Some(token) = &settings.token {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("token {}", token))?
            );
        }
        
        client_builder = client_builder.default_headers(headers);
        
        let client = client_builder.build()?;
        
        Ok(Box::new(Self { settings, client }))
    }

    async fn download_bootstrap_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        todo!();
        // This is still WIP; we will work on this when we have fixed the population test.
        // log::debug!("Downloading Bootstrap Script Files from {:?} to {:?}", repo_folder, local_folder);

        // let mut file_path_list = download_github_repo_folder(
        //     self.client.clone(),
        //     self.settings.owner.clone(),
        //     self.settings.repo.clone(),
        //     self.settings.branch.clone(),
        //     local_folder,
        //     repo_folder,
        // ).await?;
        // log::trace!("Bootstrap Script Files: {:?}", file_path_list);

        // // Sort the list of files by the file name to get them in the correct order for processing.
        // file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        // // Group the files by the data type name, which is the parent folder name of the file and turn it into a HashMap
        // // using the data type name as the key and a vector of file paths as the value.
        // let mut file_path_map = HashMap::new();
        // for file_path in file_path_list {
        //     let data_type_name = file_path.parent().unwrap().file_name().unwrap().to_str().unwrap().to_string();
        //     if !file_path_map.contains_key(&data_type_name) {
        //         file_path_map.insert(data_type_name.clone(), vec![]);
        //     }
        //     file_path_map.get_mut(&data_type_name).unwrap().push(file_path);
        // }
        // log::trace!("Bootstrap Script Map: {:?}", file_path_map);

        // Ok(file_path_map)
    }

    async fn download_change_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        todo!();
        // This is still WIP; we will work on this when we have fixed the population test.
        // log::debug!("Downloading Source Change Script Files from {:?} to {:?}", repo_folder, local_folder);

        // let mut file_path_list = download_github_repo_folder(
        //     self.client.clone(),
        //     self.settings.owner.clone(),
        //     self.settings.repo.clone(),
        //     self.settings.branch.clone(),
        //     local_folder,
        //     repo_folder,
        // ).await?;
        // log::trace!("Change Scripts Files: {:?}", file_path_list);

        // // Sort the list of files by the file name to get them in the correct order for processing.
        // file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        // Ok(file_path_list)
    }
}

#[async_trait]
impl RemoteTestRepoClient for GithubTestRepoClient {
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()> {
        log::debug!("Copying TestDefinition - {:?} to folder {:?}", test_id, test_def_path);

        // If the TestDefinition already exists, return an error.
        if test_def_path.exists() {
            return Err(anyhow::anyhow!("Test Definition ID: {} already exists in location {:?}", test_id, test_def_path));
        }   
        
        // Formulate the remote repo path for the test definition file
        let remote_path = format!("{}/{}.test.json", self.settings.root_path, test_id);
    
        // Download the test definition file
        download_github_repo_file(
            self.client.clone(),
            self.settings.owner.clone(),
            self.settings.repo.clone(),
            self.settings.branch.clone(),
            remote_path,
            test_def_path
        ).await?;

        Ok(())
    }

    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()> {
        match test_source_def {
            TestSourceDefinition::Script(def) => {
                log::debug!("Copying Test Source Content for {:?} to {:?}", def.common.test_source_id, test_source_data_path);

                // Bootstrap Data Script Files
                match &def.bootstrap_data_generator {
                    Some(BootstrapDataGeneratorDefinition::Script(bs_def)) => {
                        // TODO: Currently we only have a single folder to download. In the future we might have a list of files.
                        let repo_path = format!(
                            "{}/{}/sources/{}/{}/", 
                            self.settings.root_path, 
                            test_data_folder, 
                            def.common.test_source_id, 
                            &bs_def.script_file_folder
                        );
                        let local_path = test_source_data_path.join(&bs_def.script_file_folder);
                        self.download_bootstrap_script_files(repo_path, local_path).await?
                    },
                    _ => HashMap::new()
                };

                // Source Change Script Files
                match &def.source_change_generator {
                    Some(SourceChangeGeneratorDefinition::Script(sc_def)) => {
                        // TODO: Currently we only have a single folder to download. In the future we might have a list of files.
                        let repo_path = format!(
                            "{}/{}/sources/{}/{}/", 
                            self.settings.root_path, 
                            test_data_folder, 
                            def.common.test_source_id, 
                            &sc_def.script_file_folder
                        );
                        let local_path = test_source_data_path.join(&sc_def.script_file_folder);
                        self.download_change_script_files(repo_path, local_path).await?
                    },
                    _ => Vec::new()
                };
            },
            _ => {}
        }

        Ok(())
    }
}

async fn download_github_repo_file(
    client: Client,
    owner: String,
    repo: String,
    branch: String,
    remote_path: String,
    local_file_path: PathBuf
) -> anyhow::Result<()> {
    log::debug!("Downloading file {} to {}", remote_path, local_file_path.to_str().unwrap());

    let url = format!(
        "https://api.github.com/repos/{}/{}/contents/{}?ref={}",
        owner,
        repo,
        remote_path,
        branch
    );

    let response = client.get(&url)
                    .header("Accept", "application/vnd.github.v3+json")
                    .header("X-GitHub-Api-Version", "2022-11-28")
                    .send().await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to fetch file from GitHub: {} - {}", response.status(), response.text().await.unwrap_or_default()));
    }

    let json: Value = response.json().await?;

    let download_url = json.get("download_url")
        .and_then(|url| url.as_str())
        .ok_or_else(|| anyhow::anyhow!("No download URL found in GitHub API response"))?;

    let download_response = client.get(download_url)
        .send().await?;

    if !download_response.status().is_success() {
        return Err(anyhow::anyhow!("Failed to download file from GitHub: {} -  {}", download_response.status(), download_response.text().await.unwrap_or_default()));
    }

    let content = download_response.bytes().await?;

    // Create parent directories if they don't exist
    if let Some(parent) = local_file_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(&local_file_path).await?;
    file.write_all(&content).await?;
    Ok(())

}

// This is still WIP; we will work on this when we have fixed the population test.
fn download_github_repo_folder(
    client: Client,
    owner: String,
    repo: String,
    branch: String,
    local_repo_folder: PathBuf,
    remote_repo_folder: String, 
) -> Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<PathBuf>>> + Send>> {
    todo!();
}

async fn list_github_directory_contents(
    client: &Client,
    owner: &str,
    repo: &str,
    branch: &str,
    path: &str
) -> anyhow::Result<Vec<Value>> {
    todo!();
}

