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
use futures::future::BoxFuture;
use futures::FutureExt;
use reqwest::Client;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::test_repo_storage::models::{BootstrapDataGeneratorDefinition, SourceChangeGeneratorDefinition, TestSourceDefinition};

use super::{HuggingFaceTestRepoConfig, CommonTestRepoConfig, RemoteTestRepoClient};

#[derive(Debug)]
pub struct HuggingFaceTestRepoClientSettings {
    pub force_cache_refresh: bool,
    pub organization: String,
    pub dataset: String,
    pub revision: String,
    pub root_path: String,
    pub test_repo_id: String,
    pub token: Option<String>,
}

impl HuggingFaceTestRepoClientSettings {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: HuggingFaceTestRepoConfig) -> anyhow::Result<Self> {
        Ok(Self {
            force_cache_refresh: unique_config.force_cache_refresh,
            organization: unique_config.organization,
            dataset: unique_config.dataset,
            revision: unique_config.revision,
            root_path: unique_config.root_path,
            test_repo_id: common_config.id.clone(),
            token: unique_config.token,
        })
    }
}

#[derive(Debug)]
pub struct HuggingFaceTestRepoClient {
    pub settings: HuggingFaceTestRepoClientSettings,
    pub client: Client,
}

impl HuggingFaceTestRepoClient {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: HuggingFaceTestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
        log::debug!("Creating HuggingFaceTestRepoClient from common_config:{:?} and unique_config:{:?}, ", common_config, unique_config);

        let settings = HuggingFaceTestRepoClientSettings::new(common_config, unique_config).await?;
        log::trace!("Creating HuggingFaceTestRepoClient with settings: {:?}, ", settings);

        let mut client_builder = Client::builder()
            .user_agent("drasi-test-framework/1.0");

        // Add authorization header if token is provided
        if let Some(token) = &settings.token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))?
            );
            client_builder = client_builder.default_headers(headers);
        }

        let client = client_builder.build()?;

        Ok(Box::new(Self { settings, client }))
    }

    async fn download_bootstrap_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        log::debug!("Downloading Bootstrap Script Files from {:?} to {:?}", repo_folder, local_folder);

        let mut file_path_list = download_huggingface_repo_folder(
            self.client.clone(),
            self.settings.organization.clone(),
            self.settings.dataset.clone(),
            self.settings.revision.clone(),
            local_folder,
            repo_folder,
        ).await?;
        log::trace!("Bootstrap Script Files: {:?}", file_path_list);

        // Sort the list of files by the file name to get them in the correct order for processing.
        file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        // Group the files by the data type name, which is the parent folder name of the file and turn it into a HashMap
        // using the data type name as the key and a vector of file paths as the value.
        let mut file_path_map = HashMap::new();
        for file_path in file_path_list {
            let data_type_name = file_path.parent().unwrap().file_name().unwrap().to_str().unwrap().to_string();
            if !file_path_map.contains_key(&data_type_name) {
                file_path_map.insert(data_type_name.clone(), vec![]);
            }
            file_path_map.get_mut(&data_type_name).unwrap().push(file_path);
        }
        log::trace!("Bootstrap Script Map: {:?}", file_path_map);

        Ok(file_path_map)
    }

    async fn download_change_script_files(&self, repo_folder: String, local_folder: PathBuf) -> anyhow::Result<Vec<PathBuf>> {
        log::debug!("Downloading Source Change Script Files from {:?} to {:?}", repo_folder, local_folder);

        let mut file_path_list = download_huggingface_repo_folder(
            self.client.clone(),
            self.settings.organization.clone(),
            self.settings.dataset.clone(),
            self.settings.revision.clone(),
            local_folder,
            repo_folder,
        ).await?;
        log::trace!("Change Scripts Files: {:?}", file_path_list);

        // Sort the list of files by the file name to get them in the correct order for processing.
        file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(file_path_list)
    }
}

#[async_trait]
impl RemoteTestRepoClient for HuggingFaceTestRepoClient {
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()> {
        log::debug!("Copying TestDefinition - {:?} to folder {:?}", test_id, test_def_path);

        // If the TestDefinition already exists, return an error.
        if test_def_path.exists() {
            return Err(anyhow::anyhow!("Test Definition ID: {} already exists in location {:?}", test_id, test_def_path));
        }

        // Formulate the remote repo path for the test definition file
        let remote_path = format!("{}/{}.test", self.settings.root_path, test_id);

        // Download the test definition file
        download_huggingface_repo_file(
            self.client.clone(),
            self.settings.organization.clone(),
            self.settings.dataset.clone(),
            self.settings.revision.clone(),
            remote_path,
            test_def_path
        ).await?;

        Ok(())
    }

    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()> {
        if let TestSourceDefinition::Script(def) = test_source_def {
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
        }

        Ok(())
    }
}

async fn download_huggingface_repo_file(
    client: Client,
    organization: String,
    dataset: String,
    revision: String,
    remote_path: String,
    local_file_path: PathBuf
) -> anyhow::Result<()> {
    log::debug!("Downloading file {} to {}", remote_path, local_file_path.to_str().unwrap());

    // Hugging Face direct file URL pattern
    // https://huggingface.co/datasets/{org}/{dataset}/resolve/{revision}/{path}
    let url = format!(
        "https://huggingface.co/datasets/{}/{}/resolve/{}/{}",
        organization,
        dataset,
        revision,
        remote_path
    );

    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to download file from Hugging Face: {} - {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }

    let content = response.bytes().await?;

    // Create parent directories if they don't exist
    if let Some(parent) = local_file_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(&local_file_path).await?;
    file.write_all(&content).await?;
    Ok(())
}

fn download_huggingface_repo_folder(
    client: Client,
    organization: String,
    dataset: String,
    revision: String,
    local_repo_folder: PathBuf,
    remote_repo_folder: String,
) -> BoxFuture<'static, anyhow::Result<Vec<PathBuf>>> {
    async move {
        log::debug!("Downloading folder {} to {:?}", remote_repo_folder, local_repo_folder);

        // For Hugging Face, we need to use the tree API to list directory contents
        // https://huggingface.co/api/datasets/{org}/{dataset}/tree/{revision}/{path}
        let api_url = format!(
            "https://huggingface.co/api/datasets/{}/{}/tree/{}/{}",
            organization,
            dataset,
            revision,
            remote_repo_folder.trim_end_matches('/')
        );

        let response = client.get(&api_url).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to list directory from Hugging Face: {} - {} (URL: {})",
                response.status(),
                response.text().await.unwrap_or_default(),
                api_url
            ));
        }

        let items: Vec<HuggingFaceTreeItem> = response.json().await?;

        // Create the local folder if it doesn't exist
        if !local_repo_folder.exists() {
            tokio::fs::create_dir_all(&local_repo_folder).await?;
        }

        let mut local_file_paths = vec![];
        let mut tasks = vec![];

        for item in items {
            match item.item_type.as_str() {
                "file" => {
                    // Only process .jsonl files
                    if item.path.ends_with(".jsonl") {
                        // Extract just the filename from the full path
                        let filename = item.path.rsplit('/').next().unwrap_or(&item.path);
                        let local_file_path = local_repo_folder.join(filename);
                        local_file_paths.push(local_file_path.clone());

                        let client_clone = client.clone();
                        let org_clone = organization.clone();
                        let dataset_clone = dataset.clone();
                        let revision_clone = revision.clone();
                        let remote_path = item.path.clone();

                        let task = tokio::spawn(async move {
                            download_huggingface_repo_file(
                                client_clone,
                                org_clone,
                                dataset_clone,
                                revision_clone,
                                remote_path,
                                local_file_path
                            ).await
                        });

                        tasks.push(task);
                    }
                },
                "directory" => {
                    // Recursively download subdirectories
                    let filename = item.path.rsplit('/').next().unwrap_or(&item.path);
                    let local_subdir = local_repo_folder.join(filename);
                    let remote_subdir = item.path.clone();

                    let sub_files = download_huggingface_repo_folder(
                        client.clone(),
                        organization.clone(),
                        dataset.clone(),
                        revision.clone(),
                        local_subdir,
                        remote_subdir
                    ).await?;

                    local_file_paths.extend(sub_files);
                },
                _ => {
                    log::trace!("Ignoring unknown item type: {}", item.item_type);
                }
            }
        }

        // Wait for all file download tasks to complete
        for task in tasks {
            task.await??;
        }

        Ok(local_file_paths)
    }.boxed()
}

#[derive(Debug, serde::Deserialize)]
struct HuggingFaceTreeItem {
    #[serde(rename = "type")]
    item_type: String,
    path: String,
}
