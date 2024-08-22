use std::{error::Error, path::PathBuf};

use azure_storage::prelude::*;
use azure_storage_blobs::prelude::*;
use azure_storage_blobs::container::operations::BlobItem;
use futures::stream::StreamExt;
use tokio::{fs::File, io::AsyncWriteExt};

use crate::test_script::test_script_player::TestScriptPlayerSettings;

pub async fn initialize_test_data_cache(test_run_settings: &TestScriptPlayerSettings) -> Result<Vec<PathBuf>, String> {

    log::info!("Initializing test data cache...");

    // Download the test data from the Test Repo.
    let mut change_log_files_to_process = match download_test_data(
        &test_run_settings.test_storage_account, 
        &test_run_settings.test_storage_access_key,
        &test_run_settings.test_storage_container, 
        &test_run_settings.test_storage_path, 
        &test_run_settings.test_id,
        &test_run_settings.source_id,
        &test_run_settings.data_cache_path
    ).await {
        Ok(paths) => {
            paths
        },
        Err(e) => {
            return Err(format!("Error downloading test data: {}", e));
        }
    };

    // Sort the list of files by the file name to get them in the correct order for processing.
    change_log_files_to_process.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Print the list of files in the change_folder_path.
    if log::log_enabled!(log::Level::Debug) {
        for file in &change_log_files_to_process {
            log::debug!(" - {:?}", file);
        };
    };

    log::info!("Test data cache initialized.");

    Ok(change_log_files_to_process)
}

async fn download_test_data(
    storage_account_name: &str, 
    storage_access_key: &str,
    container_name: &str,
    directory_path: &str,
    test_id: &str,
    source_id: &str,
    local_change_data_folder: &str
) -> Result<Vec<PathBuf>, Box<dyn Error>> {

    let path = format!("{}/{}/sources/{}/change_scripts/", directory_path, test_id, source_id);
    log::info!("Downloading test data from Test Repo - {} : {}/{}", storage_account_name, container_name, path);

    let storage_credentials = StorageCredentials::access_key(storage_account_name.to_string(), storage_access_key.to_string());
    let container_client = ClientBuilder::new(storage_account_name, storage_credentials)
        .container_client(container_name);

    let mut stream = container_client
        .list_blobs()
        .prefix(path)
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
                            let local_file_path: PathBuf = [
                                local_change_data_folder, 
                                "test_repo",
                                test_id,
                                "sources",
                                source_id,
                                "change_scripts",
                                blob_file_name.file_name().unwrap().to_str().unwrap()
                                ].iter().collect(); 
                            local_file_paths.push(local_file_path.clone());


                            // Make sure the local folder exists, if not, create it.
                            // If the folder cannot be created, return an error.
                            let local_folder_path = local_file_path.parent().unwrap();
                            if !local_folder_path.exists() {
                                match std::fs::create_dir_all(&local_folder_path) {
                                    Ok(_) => {},
                                    Err(e) => {
                                        log::error!("Error creating change data folder {}: {}", local_folder_path.to_str().unwrap(), e);
                                        return Err(e.into());
                                    }
                                }
                            }

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
