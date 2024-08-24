use std::{collections::HashMap, error::Error, path::PathBuf};

use serde::Serialize;

use crate::{mask_secret, test_script::test_script_player::TestScriptPlayerSettings};

use super::download_remote_repo_folder;

#[derive(Clone, Debug, Serialize)]
pub struct DataSetSettings {
    pub storage_account: String,
    #[serde(serialize_with = "mask_secret")]
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
}

pub struct DataSet {
    pub id: String,
    pub settings: DataSetSettings,
    pub data_cache_path: PathBuf,
    pub content: Option<DataSetContent>,
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
                match tokio::fs::create_dir_all(&change_scripts_path).await {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("Error creating data cache folder {:?}: {}", change_scripts_path, e);
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
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
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
    ) -> Result<HashMap<String, Vec<PathBuf>>, Box<dyn Error>> {
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