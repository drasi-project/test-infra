
use std::{collections::HashMap, path::PathBuf};

use async_trait::async_trait;
use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::container::operations::BlobItem;
use futures::stream::StreamExt;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::config::{AzureStorageBlobTestRepoConfig, CommonTestRepoConfig};

use super::{TestRepo, TestSourceContent};

#[derive(Debug)]
pub struct AzureStorageBlobTestRepoSettings {
    pub data_cache_repo_path: PathBuf,
    pub force_cache_refresh: bool,
    pub storage_account_name: String,
    pub storage_container: String,
    pub storage_credentials: StorageCredentials,
    pub storage_root_path: String,
    pub test_repo_id: String,
}

impl AzureStorageBlobTestRepoSettings {
    pub async fn try_from_config(data_cache_repo_path: PathBuf, common_config: CommonTestRepoConfig, unique_config: AzureStorageBlobTestRepoConfig) -> anyhow::Result<Self> {

        // Create storage credentials from the account name and access key.
        let storage_credentials = StorageCredentials::access_key(unique_config.account_name.clone(), unique_config.access_key.clone());

        Ok(Self {
            data_cache_repo_path,
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
pub struct AzureStorageBlobTestRepo {
    pub settings: AzureStorageBlobTestRepoSettings,
}

impl AzureStorageBlobTestRepo {
    pub async fn new(settings: AzureStorageBlobTestRepoSettings) -> anyhow::Result<Self> {
        log::info!("Initializing AzureStorageBlobTestRepo from {:?}", settings);

        // Delete the data cache folder if it exists and the force_cache_refresh flag is set.
        if settings.force_cache_refresh && settings.data_cache_repo_path.exists() {
            tokio::fs::remove_dir_all(&settings.data_cache_repo_path).await?;
        }

        // Create the data cache folder if it doesn't exist.
        if !settings.data_cache_repo_path.exists() {
            tokio::fs::create_dir_all(&settings.data_cache_repo_path).await?
        }
        
        Ok(Self { settings })
    }

    async fn download_change_script_files(&self, local_folder: PathBuf, repo_folder: String) -> anyhow::Result<Vec<PathBuf>> {

        let mut file_path_list = download_test_repo_folder(
            self.settings.storage_account_name.clone(),
            self.settings.storage_credentials.clone(),
            self.settings.storage_container.clone(),
            self.settings.storage_root_path.clone(),
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
            self.settings.storage_account_name.clone(),
            self.settings.storage_credentials.clone(),
            self.settings.storage_container.clone(),
            self.settings.storage_root_path.clone(),
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
impl TestRepo for AzureStorageBlobTestRepo {
    async fn download_test_source_content(&self, test_id: String, source_id: String, dataset_cache_path: PathBuf) -> anyhow::Result<TestSourceContent> {
        log::trace!("Downloading DataSet - {:?}/{:?} to folder {:?}", test_id, source_id, dataset_cache_path);

        // Formulate and create the local path for change_script files
        let mut change_scripts_local_path = dataset_cache_path.clone();
        change_scripts_local_path.push("change_scripts");
        if !change_scripts_local_path.exists() {
            tokio::fs::create_dir_all(&change_scripts_local_path).await?;
        }

        // Formulate the remote repo path for the change script files
        let change_scripts_repo_path = format!("{}/{}/sources/{}/change_scripts/", 
            self.settings.storage_root_path, test_id, source_id);

        // Download the change_script files
        let change_script_files = self.download_change_script_files(
            change_scripts_local_path,
            change_scripts_repo_path
        ).await?;

        // Formulate and create the local path for bootstrap_script files
        let mut bootstrap_scripts_local_path = dataset_cache_path.clone();
        bootstrap_scripts_local_path.push("bootstrap_scripts");
        if !bootstrap_scripts_local_path.exists() {
            tokio::fs::create_dir_all(&bootstrap_scripts_local_path).await?
        }

        // Formulate the remote repo path for the bootstrap script files
        let bootstrap_scripts_repo_path = format!("{}/{}/sources/{}/change_scripts/", 
            self.settings.storage_root_path, test_id, source_id);
    
        // Download the bootstrap_script files
        let bootstrap_script_files = self.download_bootstrap_script_files(
            bootstrap_scripts_local_path,
            bootstrap_scripts_repo_path
        ).await?;
         
        Ok(TestSourceContent {
            change_log_script_files: Some(change_script_files),
            bootstrap_script_files: Some(bootstrap_script_files),
        })
    }
}

async fn download_test_repo_folder(
    storage_account: String,
    storage_credentials: StorageCredentials,
    storage_container: String,
    storage_path: String,
    local_repo_folder: PathBuf,
    remote_repo_folder: String, 
) -> anyhow::Result<Vec<PathBuf>> {
    log::info!("Downloading Remote Repo Folder - {:?} : {:?}/{:?}", storage_account, storage_container, storage_path);

    let container_client = ClientBuilder::new(&storage_account, storage_credentials)
        .container_client(&storage_container);

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
