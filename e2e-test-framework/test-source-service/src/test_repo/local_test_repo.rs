use std::{collections::HashMap, error::Error, path::PathBuf};

use super::dataset::{DataSet, DataSetSettings, DataSetContent};

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