use std::{collections::HashMap, path::PathBuf};

use super::dataset::{DataSet, DataSetSettings};

pub struct LocalTestRepo {
    pub data_cache_path: PathBuf,
    pub data_sets: HashMap<String, DataSet>,
}

impl LocalTestRepo {
    pub fn new(data_cache_path: String) -> anyhow::Result<Self> {

        let data_cache_path_buf = PathBuf::from(&data_cache_path);

        // Test if the path exists, and if not create it.
        // If there are errors creating the path, log the error and return it.
        if !data_cache_path_buf.exists() {
            match std::fs::create_dir_all(&data_cache_path_buf) {
                Ok(_) => {},
                Err(e) => {
                    let msg = format!("Error creating data cache folder {}: {}", &data_cache_path, e);
                    log::error!("{}", msg);
                    anyhow::bail!(msg);
                }
            }
        }

        Ok(LocalTestRepo {
            data_cache_path: data_cache_path_buf,
            data_sets: HashMap::new(),
        })
    }

    pub async fn add_or_get_data_set( &mut self, settings: DataSetSettings) -> anyhow::Result<DataSet> {
        let id = settings.get_id();

        let dataset = match self.data_sets.get(&id) {
            Some(ds) => ds.clone(),
            None => {
                let ds = DataSet::new(
                    self.data_cache_path.clone(),
                    settings
                ).await?;
                self.data_sets.insert(id.clone(), ds.clone());
                ds
            }
        };

        Ok(dataset)
    }
}