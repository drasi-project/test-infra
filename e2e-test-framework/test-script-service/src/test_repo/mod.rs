use std::{collections::HashMap, error::Error, path::PathBuf};

use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::container::operations::BlobItem;
use futures::stream::StreamExt;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::test_script::test_script_player::TestScriptPlayerSettings;

pub struct LocalTestRepo {
    pub data_cache_path: PathBuf,
    pub data_sets: HashMap<String, DataSet>,
}

impl LocalTestRepo {
    pub fn new(data_cache_path: String) -> Result<Self, Box<dyn Error>> {

        let data_cache_path_buf = PathBuf::from(&data_cache_path);

        // Test if the path exists, and if not create it.
        // If there are errors creating the path, log the error and return it.
        if !data_cache_path_buf.exists() {
            match std::fs::create_dir_all(&data_cache_path_buf) {
                Ok(_) => {},
                Err(e) => {
                    log::error!("Error creating data cache folder {}: {}", &data_cache_path, e);
                    return Err(e.into());
                }
            }
        }

        Ok(LocalTestRepo {
            data_cache_path: data_cache_path_buf,
            data_sets: HashMap::new(),
        })
    }

    pub async fn add_data_set( &mut self, settings: &DataSetSettings) -> Result<DataSetContent, Box<dyn Error>> {
        let id = settings.get_id();

        if !self.data_sets.contains_key(&id) {
            let data_set = DataSet::new(
                self.data_cache_path.clone(),
                settings.clone()
            );
            self.data_sets.insert(id.clone(), data_set);
        }

        let data_set = self.data_sets.get_mut(&id).unwrap();

        // For now, we will download the data content immediately.
        // In the future, we may want to defer this until the data is actually needed.
        match data_set.get_content().await {
            Ok(content) => Ok(content),
            Err(e) => {
                Err(e)
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct DataSetSettings {
    pub storage_account: String,
    pub storage_access_key: String,
    pub storage_container: String,
    pub storage_path: String,
    pub test_id: String,
    pub source_id: String,
}

impl DataSetSettings {
    pub fn from_test_script_player_settings(player_settings: &TestScriptPlayerSettings) -> Self {
        DataSetSettings {
            storage_account: player_settings.test_storage_account.clone(),
            storage_access_key: player_settings.test_storage_access_key.clone(),
            storage_container: player_settings.test_storage_container.clone(),
            storage_path: player_settings.test_storage_path.clone(),
            test_id: player_settings.test_id.clone(),
            source_id: player_settings.source_id.clone(),
        }
    }

    pub fn get_id(&self) -> String {
        // Formulate a unique key for the TestSourceDataSet.
        format!("{}-{}", &self.test_id, &self.source_id)
    }
}
        
#[derive(Debug, Clone)]
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
}

pub struct DataSet {
    pub id: String,
    settings: DataSetSettings,
    data_cache_path: PathBuf,
    content: Option<DataSetContent>,
}

impl DataSet {
    pub fn new(
        data_cache_root: PathBuf,
        settings: DataSetSettings,
    ) -> Self {

        // Formulate the local folder path for the TestSourceDataSet.
        let mut data_cache_path = data_cache_root.clone();
        data_cache_path.push(format!("test_repo/{}/sources/{}/", &settings.test_id, &settings.source_id));

        DataSet {
            id: settings.get_id(),
            settings,
            data_cache_path,
            content: None,
        }
    }

    pub async fn get_content(&mut self) -> Result<DataSetContent, Box<dyn Error>> {
        log::info!("Getting content for DataSet {}", &self.id);

        if self.content.is_none() {
            log::trace!("Downlaoding content for DataSet {} into {:?}", &self.id, self.data_cache_path);

            let mut change_scripts_path = self.data_cache_path.clone();
            change_scripts_path.push("change_scripts");
            if !change_scripts_path.exists() {
                match std::fs::create_dir_all(&change_scripts_path) {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error creating data cache folder {}: {}", &change_scripts_path.to_str().unwrap(), e);
                        return Err(e.into());
                    }
                }
            }
            let change_script_files = match self.download_change_script_files(change_scripts_path).await {
                Ok(files) => files,
                Err(e) => {
                    return Err(e);
                }
            };
            // let mut bootstrap_scripts_path = self.data_cache_path.clone();
            // bootstrap_scripts_path.push("bootstrap_scripts");
            // if !bootstrap_scripts_path.exists() {
            //     match std::fs::create_dir_all(&bootstrap_scripts_path) {
            //         Ok(_) => {},
            //         Err(e) => {
            //             log::error!("Error creating data cache folder {}: {}", &bootstrap_scripts_path, e);
            //             return Err(e.into());
            //         }
            //     }
            // }
            // let bootstrap_script_files = match self.download_bootstrap_script_files().await {
            //     Ok(files) => files,
            //     Err(e) => {
            //         return Err(e);
            //     }
            // };

            self.content = Some(DataSetContent::new(Some(change_script_files), None));
        }
        Ok(self.content.as_ref().unwrap().clone())
    }

    async fn download_change_script_files(
        &self,
        local_repo_folder: PathBuf
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {

        log::info!("Downloading test data from Test Repo - {} : {}/{}", self.settings.storage_account, self.settings.storage_container, self.settings.storage_path);

        let storage_credentials = StorageCredentials::access_key(&self.settings.storage_account, self.settings.storage_access_key.clone());
        let container_client = ClientBuilder::new(&self.settings.storage_account, storage_credentials)
            .container_client(&self.settings.storage_container);

        let repo_path_filter = format!("{}/{}/sources/{}/change_scripts/", self.settings.storage_path, self.settings.test_id, self.settings.source_id);
        let mut stream = container_client
            .list_blobs()
            .prefix(repo_path_filter)
            .into_stream();
        
        // Vector of tasks to download the files.
        // Each task will download a single file.
        // All downloads must be complete before returning.
        let mut tasks = vec![];

        // Vector of local file paths being downloaded.
        let mut local_file_paths = vec![];

        while let Some(result) = stream.next().await {
            match result {
                Ok(blob_list) => {
                    for blob_item in blob_list.blobs.items {
                        match blob_item {
                            BlobItem::Blob(blob) => {
                                // Create a PathBuf from the blob name to get the file name.
                                let blob_file_name = PathBuf::from(blob.name.clone());

                                // Create the local file path to hold the blob data.
                                let mut local_file_path = local_repo_folder.clone();
                                local_file_path.push(blob_file_name.file_name().unwrap().to_str().unwrap());
                                local_file_paths.push(local_file_path.clone());

                                let task = tokio::spawn(download_file(
                                        container_client.blob_client(&blob.name), 
                                        local_file_path
                                    ));

                                tasks.push(task);
                            },
                            BlobItem::BlobPrefix(prefix) => {
                                log::trace!("Ignoring Blob Prefix: {}", prefix.name);
                            }
                        }
                    }
                },
                Err(e) => {
                    return Err(e.into());
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
}

async fn download_file(
    blob_client: BlobClient, 
    local_file_path: PathBuf
) -> Result<(), Box<dyn Error + Send + Sync>> {
    log::debug!("Downloading test script file {} to {}", blob_client.blob_name(), local_file_path.to_str().unwrap());

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