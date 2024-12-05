
use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::container::operations::BlobItem;
use futures::stream::StreamExt;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::test_repo_storage::{models::{BootstrapDataGeneratorDefinition, SourceChangeGeneratorDefinition, TestDefinition}, TestSourceDataset};

use super::{AzureStorageBlobTestRepoConfig, CommonTestRepoConfig, RemoteTestRepoClient};

#[derive(Debug)]
pub struct AzureStorageBlobTestRepoClientSettings {
    pub force_cache_refresh: bool,
    pub storage_account_name: String,
    pub storage_container: String,
    pub storage_credentials: StorageCredentials,
    pub storage_root_path: String,
    pub test_repo_id: String,
}

impl AzureStorageBlobTestRepoClientSettings {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: AzureStorageBlobTestRepoConfig) -> anyhow::Result<Self> {

        // Create storage credentials from the account name and access key.
        let storage_credentials = StorageCredentials::access_key(unique_config.account_name.clone(), unique_config.access_key.clone());

        Ok(Self {
            force_cache_refresh: common_config.force_cache_refresh,
            storage_account_name: unique_config.account_name.clone(),
            storage_container: unique_config.container.clone(),
            storage_credentials,
            storage_root_path: unique_config.root_path,
            test_repo_id: common_config.id.clone(),
        })
    }
}

#[derive(Debug)]
pub struct AzureStorageBlobTestRepoClient {
    pub settings: AzureStorageBlobTestRepoClientSettings,
}

impl AzureStorageBlobTestRepoClient {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: AzureStorageBlobTestRepoConfig) -> anyhow::Result<Box<dyn RemoteTestRepoClient + Send + Sync>> {
        log::debug!("Creating AzureStorageBlobTestRepoClient from common_config:{:?} and unique_config:{:?}, ", common_config, unique_config);

        let settings = AzureStorageBlobTestRepoClientSettings::new(common_config, unique_config).await?;
        log::trace!("Creating AzureStorageBlobTestRepoClients with settings: {:?}, ", settings);
        
        Ok(Box::new( Self { settings }))
    }

    fn create_container_client(&self) -> anyhow::Result<ContainerClient> {
        let container_client = 
            ClientBuilder::new(
                self.settings.storage_account_name.clone(), 
                self.settings.storage_credentials.clone()
            ).container_client(self.settings.storage_container.clone());

        Ok(container_client)
    }

    async fn download_change_script_files(&self, local_folder: PathBuf, repo_folder: String) -> anyhow::Result<Vec<PathBuf>> {

        let mut file_path_list = download_test_repo_folder(
            self.create_container_client()?,
            local_folder,
            repo_folder,
        ).await?;
        log::trace!("Change Scripts Files: {:?}", file_path_list);

        // Sort the list of files by the file name to get them in the correct order for processing.
        file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(file_path_list)
    }

    async fn download_bootstrap_script_files( &self, local_folder: PathBuf, repo_folder: String ) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        let mut file_path_list = download_test_repo_folder(
            self.create_container_client()?,
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
}

#[async_trait]
impl RemoteTestRepoClient for AzureStorageBlobTestRepoClient {
    async fn get_test_definition(&self, test_id: String, test_store_path: PathBuf) -> anyhow::Result<PathBuf> {
        log::trace!("Getting TestDefinition - {:?} to folder {:?}", test_id, test_store_path);

        // Formulate local path for test definition file
        let local_file_path = test_store_path.join(format!("{}.test", test_id));
        
        // Formulate the remote repo path for the test definition file
        let remote_path = format!("{}/{}.test", self.settings.storage_root_path, test_id);
    
        // Download the test definition file
        download_test_repo_file(
            self.create_container_client()?.blob_client(&remote_path),
            local_file_path.clone()
        ).await?;

        Ok(local_file_path)
    }

    async fn get_test_source_content_from_def(&self, test_def: &TestDefinition, source_id: String, bootstrap_data_store_path: PathBuf, source_change_store_path: PathBuf) -> anyhow::Result<TestSourceDataset> {
        log::trace!("Downloading Test Source Content for {:?}", source_id);

        // Bootstrap Data Script Files
        let bootstrap_script_files = match test_def.sources.iter().find(|s| s.id == source_id) {
            Some(source) => {
                match &source.bootstrap_data_generator {
                    Some(BootstrapDataGeneratorDefinition::Script{common_config: _, unique_config}) => {
                        // TODO: Currently we only have a single folder to download. In the future we might have a list of files.
                        let bootstrap_data_scripts_repo_path = format!(
                            "{}/{}/sources/{}/{}/", 
                            self.settings.storage_root_path, 
                            test_def.id, 
                            source_id, 
                            &unique_config.script_file_folder
                        );
                        self.download_bootstrap_script_files(
                            bootstrap_data_store_path, bootstrap_data_scripts_repo_path).await?
                    },
                    _ => HashMap::new()
                }
            },
            None => HashMap::new()
        };

        // Source Change Script Files
        let source_change_script_files = match test_def.sources.iter().find(|s| s.id == source_id) {
            Some(source) => {
                match &source.source_change_generator {
                    Some(SourceChangeGeneratorDefinition::Script{common_config: _, unique_config}) => {
                        // TODO: Currently we only have a single folder to download. In the future we might have a list of files.
                        let source_change_scripts_repo_path = format!(
                            "{}/{}/sources/{}/{}/", 
                            self.settings.storage_root_path, 
                            test_def.id, 
                            source_id, 
                            &unique_config.script_file_folder
                        );
                        self.download_change_script_files(
                            source_change_store_path, source_change_scripts_repo_path).await?
                    },
                    _ => Vec::new()
                }
            },
            None => Vec::new()
        };

        Ok(TestSourceDataset {
            source_change_script_files: source_change_script_files,
            bootstrap_data_script_files: bootstrap_script_files,
        })
    }
}

async fn download_test_repo_folder(
    container_client: ContainerClient,
    local_repo_folder: PathBuf,
    remote_repo_folder: String, 
) -> anyhow::Result<Vec<PathBuf>> {

    let mut stream = container_client
            .list_blobs()
            .prefix(remote_repo_folder.clone())
            .into_stream();
    
    // Vector of tasks to download the files.
    // Each task will download a single file.
    // All downloads must be complete before returning.
    let mut tasks = vec![];

    // Vector of local file paths being downloaded.
    let mut local_file_paths = vec![];

    while let Some(result) = stream.next().await {
        let blob_list = result?;
        for blob_item in blob_list.blobs.items {
            match blob_item {
                BlobItem::Blob(blob) => {
                    let blob_name = blob.name;

                    // Create the local file path for the blob.
                    let stripped_blob_file_name = blob_name.strip_prefix(&remote_repo_folder).unwrap();
                    let local_file_path = local_repo_folder.clone().join(&stripped_blob_file_name);

                    // Process the blob as a directory if it doesn't have an extension.
                    if local_file_path.extension().is_none() {
                        log::trace!("Creating directory: {:?}", local_file_path);
                        tokio::fs::create_dir_all(local_file_path).await?;
                    } else {
                        // Add the local file path to the list of files being downloaded.
                        local_file_paths.push(local_file_path.clone());

                        let task = tokio::spawn(download_test_repo_file(
                            container_client.blob_client(&blob_name), 
                            local_file_path
                        ));
    
                        tasks.push(task);
                    }
                },
                BlobItem::BlobPrefix(prefix) => {
                    log::trace!("Ignoring Blob Prefix: {:?}", prefix.name);
                }
            }
        }
    }

    match futures::future::try_join_all(tasks).await {
        Ok(_) => return Ok(local_file_paths),
        Err(e) => {
            return Err(e.into());
        }
    }
}

async fn download_test_repo_file (
    blob_client: BlobClient, 
    local_file_path: PathBuf
) -> anyhow::Result<()> {
    log::debug!("Downloading  file {} to {}", blob_client.blob_name(), local_file_path.to_str().unwrap());

    // Create the local file to hold the blob data.
    let mut local_file = File::create(local_file_path).await?;

    // Download the blob data.
    let mut stream = blob_client.get().into_stream();

    while let Some(value) = stream.next().await {

        let mut body = value?.data;

        while let Some(value) = body.next().await {

            match value {
                Ok(bytes) => {
                    let _ = local_file.write_all(&bytes).await;
                },
                Err(e) => {
                    log::error!("Error getting blob data: {}", e);
                    return Err(e.into());
                }
            };
        }
    }

    Ok(())
}
