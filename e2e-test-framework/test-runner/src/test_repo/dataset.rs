use std::{collections::{HashMap, HashSet}, path::PathBuf};

use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::container::operations::BlobItem;
use futures::stream::StreamExt;
use serde::Serialize;
use tokio::{fs::File, io::AsyncWriteExt};
use walkdir::WalkDir;

use crate::{config::{SourceConfig, SourceConfigDefaults}, mask_secret };

#[derive(Clone, Debug, Serialize)]
pub struct DataSetSettings {
    pub storage_account: String,
    #[serde(serialize_with = "mask_secret")]
    pub storage_access_key: String,
    pub storage_container: String,
    pub storage_path: String,
    pub test_id: String,
    pub source_id: String,
    pub force_test_repo_cache_refresh: bool,
}

impl DataSetSettings {
    pub fn try_from_source_config(source_config: &SourceConfig, source_defaults: &SourceConfigDefaults ) -> anyhow::Result<Self> {

        // If the SourceConfig doesn't contain a test_storage_account value, use the default value.
        // If there is no default value, return an error.
        let storage_account = match &source_config.test_storage_account {
            Some(test_storage_account) => test_storage_account.clone(),
            None => {
                match &source_defaults.test_storage_account {
                    Some(test_storage_account) => test_storage_account.clone(),
                    None => {
                        anyhow::bail!("No test_storage_account provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_access_key value, use the default value.
        // If there is no default value, return an error.
        let storage_access_key = match &source_config.test_storage_access_key {
            Some(test_storage_access_key) => test_storage_access_key.clone(),
            None => {
                match &source_defaults.test_storage_access_key {
                    Some(test_storage_access_key) => test_storage_access_key.clone(),
                    None => {
                        anyhow::bail!("No test_storage_access_key provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_access_key value, use the default value.
        // If there is no default value, return an error.
        let storage_container = match &source_config.test_storage_container {
            Some(test_storage_container) => test_storage_container.clone(),
            None => {
                match &source_defaults.test_storage_container {
                    Some(test_storage_container) => test_storage_container.clone(),
                    None => {
                        anyhow::bail!("No test_storage_container provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a test_storage_path value, use the default value.
        // If there is no default value, return an error.
        let storage_path = match &source_config.test_storage_path {
            Some(test_storage_path) => test_storage_path.clone(),
            None => {
                match &source_defaults.test_storage_path {
                    Some(test_storage_path) => test_storage_path.clone(),
                    None => {
                        anyhow::bail!("No test_storage_path provided and no default value found.");
                    }
                }
            }
        };

        // If neither the SourceConfig nor the SourceDefaults contain a test_id, return an error.
        let test_id = match &source_config.test_id {
            Some(test_id) => test_id.clone(),
            None => {
                match &source_defaults.test_id {
                    Some(test_id) => test_id.clone(),
                    None => {
                        anyhow::bail!("No test_id provided and no default value found.");
                    }
                }
            }
        };

        // If the SourceConfig doesn't contain a source_id value, use the default value.
        // If there is no default value, return an error.
        let source_id = match &source_config.source_id {
            Some(source_id) => source_id.clone(),
            None => {
                match &source_defaults.source_id {
                    Some(source_id) => source_id.clone(),
                    None => {
                        anyhow::bail!("No source_id provided and no default value found.");
                    }
                }
            }
        };

        let force_test_repo_cache_refresh = match &source_config.force_test_repo_cache_refresh {
            Some(force) => *force,
            None => *&source_defaults.force_test_repo_cache_refresh
        };

        Ok(Self {
            storage_account,
            storage_access_key,
            storage_container,
            storage_path,
            test_id,
            source_id,
            force_test_repo_cache_refresh,
        })
    }

    // pub fn from_change_script_player_settings(player_settings: &ChangeScriptPlayerSettings) -> Self {
    //     DataSetSettings {
    //         force_test_repo_cache_refresh: player_settings.
    //         storage_account: player_settings.test_storage_account.clone(),
    //         storage_access_key: player_settings.test_storage_access_key.clone(),
    //         storage_container: player_settings.test_storage_container.clone(),
    //         storage_path: player_settings.test_storage_path.clone(),
    //         test_id: player_settings.test_id.clone(),
    //         source_id: player_settings.source_id.clone(),
    //     }
    // }

    pub fn get_id(&self) -> String {
        // Formulate a unique key for the TestSourceDataSet.
        format!("{}::{}", &self.test_id, &self.source_id)
    }
}
        
#[derive(Clone, Debug, Serialize)]
pub struct DataSetContent {
    pub change_log_script_files: Option<Vec<PathBuf>>,
    pub bootstrap_script_files: Option<HashMap<String, Vec<PathBuf>>>,
}

impl DataSetContent {
    pub fn new(change_log_script_files: Option<Vec<PathBuf>>, bootstrap_script_files: Option<HashMap<String, Vec<PathBuf>>>) -> Self {
        DataSetContent {
            change_log_script_files,
            bootstrap_script_files,
        }
    }

    pub fn has_content(&self) -> bool {
        (self.change_log_script_files.is_some() && self.change_log_script_files.as_ref().unwrap().len() > 0 ) 
            || (self.bootstrap_script_files.is_some() && self.bootstrap_script_files.as_ref().unwrap().len() > 0)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DataSet {
    pub id: String,
    settings: DataSetSettings,
    data_cache_path: PathBuf,
    content: Option<DataSetContent>,
}

impl DataSet {
    pub async fn new(
        data_cache_root: PathBuf,
        settings: DataSetSettings,
    ) -> anyhow::Result<Self> {

        // Formulate the local folder path for the TestSourceDataSet.
        let mut data_cache_path = data_cache_root.clone();
        data_cache_path.push(format!("test_repo/{}/sources/{}/", &settings.test_id, &settings.source_id));

        let mut dataset = DataSet {
            id: settings.get_id(),
            settings,
            data_cache_path,
            content: None,
        };

        // For now, we will download content immediately, we may want to change this later.

        // If the folder for the DataSet already exists, we assume we want to keep the data unless the DataSet
        // is configured to force_test_repo_cache_refresh.
        if dataset.data_cache_path.exists() {
            if dataset.settings.force_test_repo_cache_refresh {
                tokio::fs::remove_dir_all(&dataset.data_cache_path).await?;
                dataset.download_content().await?;
            } else {
                dataset.catalog_existing_content().await?;
            }
        } else {
            dataset.download_content().await?;
        };

        Ok(dataset)
    }

    pub fn get_content(&self) -> Option<DataSetContent> {
        self.content.clone()
    }

    pub fn get_settings(&self) -> DataSetSettings {
        self.settings.clone()
    }

    pub fn count_bootstrap_type_intersection(&self, requested_labels: &HashSet<String>) -> usize {
        let mut match_count = 0;

        match &self.content {
            Some(content) => {
                match &content.bootstrap_script_files {
                    Some(bootstrap_script_files) => {
                        // Iterate through the data type names and count the number of matches.
                        bootstrap_script_files.keys().filter(|key| requested_labels.contains(*key)).for_each(|_| {
                            match_count += 1;
                        });
                    }
                    None => {}
                }
            },
            None => {}
        }

        match_count
    }

    async fn catalog_existing_content(&mut self) -> anyhow::Result<()> {
        let mut change_scripts_path = self.data_cache_path.clone();
        change_scripts_path.push("change_scripts");
        let change_log_script_files = get_files_in_folder(change_scripts_path);

        let mut bootstrap_scripts_path = self.data_cache_path.clone();
        bootstrap_scripts_path.push("bootstrap_scripts");
        let bootstrap_script_files = build_folder_file_map(bootstrap_scripts_path);

        self.content = Some(DataSetContent::new(Some(change_log_script_files), Some(bootstrap_script_files)));

        Ok(())
    }

    async fn download_content(&mut self) -> anyhow::Result<DataSetContent> {
        log::info!("Getting content for DataSet {}", &self.id);

        if self.content.is_none() {
            log::trace!("Downlaoding content for DataSet {} into {:?}", &self.id, self.data_cache_path);

            let mut change_scripts_path = self.data_cache_path.clone();
            change_scripts_path.push("change_scripts");
            if !change_scripts_path.exists() {
                match tokio::fs::create_dir_all(&change_scripts_path).await {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error creating data cache folder {:?}: {}", change_scripts_path, e);
                        return Err(e.into());
                    }
                }
            }
            let change_script_files = self.download_change_script_files(change_scripts_path).await?;

            let mut bootstrap_scripts_path = self.data_cache_path.clone();
            bootstrap_scripts_path.push("bootstrap_scripts");
            if !bootstrap_scripts_path.exists() {
                match tokio::fs::create_dir_all(&bootstrap_scripts_path).await {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error creating data cache folder {:?}: {}", bootstrap_scripts_path, e);
                        return Err(e.into());
                    }
                }
            }
            let bootstrap_script_files = match self.download_bootstrap_script_files(bootstrap_scripts_path).await {
                Ok(files) => files,
                Err(e) => {
                    return Err(e);
                }
            };

            self.content = Some(DataSetContent::new(Some(change_script_files), Some(bootstrap_script_files)));
        }
        Ok(self.content.as_ref().unwrap().clone())
    }

    async fn download_change_script_files(
        &self,
        local_repo_folder: PathBuf
    ) -> anyhow::Result<Vec<PathBuf>> {
        let mut file_path_list = download_remote_repo_folder(
            self.settings.storage_account.clone(),
            self.settings.storage_access_key.clone(),
            self.settings.storage_container.clone(),
            self.settings.storage_path.clone(),
            format!("{}/{}/sources/{}/change_scripts/", self.settings.storage_path, self.settings.test_id, self.settings.source_id),
            local_repo_folder,
        ).await?;
        log::trace!("Change Scripts: {:?}", file_path_list);

        // Sort the list of files by the file name to get them in the correct order for processing.
        file_path_list.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

        Ok(file_path_list)
    }

    async fn download_bootstrap_script_files(
        &self,
        local_repo_folder: PathBuf
    ) -> anyhow::Result<HashMap<String, Vec<PathBuf>>> {
        let mut file_path_list = download_remote_repo_folder(
            self.settings.storage_account.clone(),
            self.settings.storage_access_key.clone(),
            self.settings.storage_container.clone(),
            self.settings.storage_path.clone(),
            format!("{}/{}/sources/{}/bootstrap_scripts/", self.settings.storage_path, self.settings.test_id, self.settings.source_id),
            local_repo_folder,
        ).await?;

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
        log::trace!("Bootstrap Scripts: {:?}", file_path_map);

        Ok(file_path_map)
    }
}


async fn download_remote_repo_folder(
    storage_account: String,
    storage_access_key: String,
    storage_container: String,
    storage_path: String,
    remote_repo_folder: String, 
    local_repo_folder: PathBuf,
) -> anyhow::Result<Vec<PathBuf>> {
    log::info!("Downloading Remote Repo Folder - {:?} : {:?}/{:?}", storage_account, storage_container, storage_path);

    let storage_credentials = StorageCredentials::access_key(&storage_account, storage_access_key.clone());
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

                        let task = tokio::spawn(download_file(
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

async fn download_file(
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

fn get_files_in_folder(root: PathBuf) -> Vec<PathBuf> {
    let mut paths = Vec::new();

    for entry in WalkDir::new(root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path().to_path_buf();
        if path.is_file() {
            paths.push(path);
        }
    }

    // Sort the vector of file paths
    paths.sort();
    
    paths
}

fn build_folder_file_map(root: PathBuf) -> HashMap<String, Vec<PathBuf>> {
    let mut folder_map: HashMap<String, Vec<PathBuf>> = HashMap::new();

    for entry in WalkDir::new(&root).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path().to_path_buf();

        if path.is_file() {
            if let Some(parent) = path.parent() {
                if let Some(folder_name) = parent.file_name().and_then(|name| name.to_str()) {
                    folder_map
                        .entry(folder_name.to_string())
                        .or_insert_with(Vec::new)
                        .push(path);
                }
            }
        }
    }

    // Sort the vectors in the HashMap
    for files in folder_map.values_mut() {
        files.sort();
    }

    folder_map
}