
use std::path::PathBuf;

use async_trait::async_trait;
use futures::future::join_all;
use tokio::{fs, io};

use crate::test_repo_storage::models::TestSourceDefinition;

use super::{CommonTestRepoConfig, LocalStorageTestRepoConfig, RemoteTestRepoClient};

#[derive(Debug)]
pub struct LocalStorageTestRepoClientSettings {
    pub source_path: Option<PathBuf>,
    pub test_repo_id: String,
}

impl LocalStorageTestRepoClientSettings {
    pub async fn new(common_config: CommonTestRepoConfig, unique_config: LocalStorageTestRepoConfig) -> anyhow::Result<Self> {

        Ok(Self {
            source_path: unique_config.source_path.map(|p| PathBuf::from(p)),
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
    async fn copy_test_definition(&self, test_id: String, test_def_path: PathBuf) -> anyhow::Result<()> {
        log::debug!("Copying TestDefinition - {:?} to path {:?}", test_id, test_def_path);

        // If the TestDefinition already exists, return an error.
        if test_def_path.exists() {
            return Err(anyhow::anyhow!("Test Definition ID: {} already exists in location {:?}", test_id, test_def_path));
        }   

        // If there is no source_path configured for the repo client, try to use an existing file.
        // Otherwise, copy the file from the source path to the repo location.
        match self.settings.source_path {
            Some(ref source_path) => {
                let source_file = source_path.join(format!("{}.test.json", test_id));

                if source_file.exists() {
                    fs::copy(source_file, test_def_path).await?;
                    Ok(())
                } else {
                    return Err(anyhow::anyhow!("Test Definition ID: {} not found in source location {:?}", test_id, source_file));
                }
            },
            None => {
                if test_def_path.exists() {
                    Ok(())
                } else {
                    return Err(anyhow::anyhow!("Test Definition ID: {} not found in location {:?}", test_id, test_def_path));
                }
            }
        }
    }

    async fn copy_test_source_content(&self, test_data_folder: String, test_source_def: &TestSourceDefinition, test_source_data_path: PathBuf) -> anyhow::Result<()> {
        log::debug!("Copying Test Source Content for {:?} to {:?}", test_source_def.test_source_id, test_source_data_path);

        // If there is no source_path configured for the repo client, use a existing files.
        // Otherwise, copy the files from the source path to the repo location.
        match self.settings.source_path {
            Some(ref source_path) => {
                let source = source_path.join(format!("{}/sources/{}", test_data_folder, test_source_def.test_source_id));

                if !source.exists() {
                    return Err(anyhow::anyhow!("Content for Test Source ID: {} not found in source location {:?}", test_source_def.test_source_id, source));
                }

                copy_dir_tree(source, test_source_data_path.clone()).await?;
            },
            None => {}
        }

        Ok(())
    }
}

fn copy_dir_tree_task(source: PathBuf, destination: PathBuf) -> impl std::future::Future<Output = io::Result<()>> + Send {
    async move {
        copy_dir_tree(source, destination).await
    }
}

async fn copy_dir_tree(source: PathBuf, destination: PathBuf) -> io::Result<()> {
    // Create the destination directory
    fs::create_dir_all(&destination).await?;

    // Collect directory entries
    let mut dir_entries = fs::read_dir(&source).await?;

    let mut tasks = Vec::new();

    while let Some(entry) = dir_entries.next_entry().await? {
        let path = entry.path();
        let dest_path = destination.join(entry.file_name());

        if path.is_dir() {
            // Add a new task for directory recursion
            tasks.push(tokio::spawn(copy_dir_tree_task(path, dest_path)));
        } else {
            // If the file is a jsonl file, add the copy task to the list.
            let extension = path.extension().and_then(|ext| ext.to_str());

            if let Some("jsonl") = extension {
                tasks.push(tokio::spawn(async move {
                    fs::copy(path, dest_path).await.map(|_| ())
                }));
            }
        }
    }

    // Wait for all tasks to complete
    let results = join_all(tasks).await;

    // Check for errors in tasks
    for result in results {
        result??;
    }

    Ok(())
}