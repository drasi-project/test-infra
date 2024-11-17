use std::path::PathBuf;

use tokio::fs;

pub struct TestRunStorage {
    pub id: String,
    pub path: PathBuf,
    pub source_path: PathBuf,
    pub test_id: String,
    pub test_run_id: String,
}

fn get_test_run_id(test_id: &str, test_run_id: &str) -> String {
    format!("{}__{}", test_id, test_run_id)
}

fn get_test_run_source_id(repo_id: &str, source_id: &str) -> String {
    format!("{}__{}", repo_id, source_id)
}

impl TestRunStorage {
    pub async fn new(test_id: &str, test_run_id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        
        let id = get_test_run_id(test_id, test_run_id);
        log::debug!("Creating TestRunStorage for ID {:?} in folder: {:?}", &id, &parent_path);

        let path = parent_path.join(&id);
        let source_path = path.join("sources/");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&source_path).await?;
        }

        Ok(Self {
            id,
            test_id: test_id.to_string(),
            test_run_id: test_run_id.to_string(),
            path,
            source_path,
        })
    }

    pub async fn get_source_storage(&self, repo_id: &str, source_id: &str, replace: bool) -> anyhow::Result<TestRunSourceStorage> {
        log::debug!("Getting TestRunSourceStorage for ID: {:?}", get_test_run_source_id(repo_id, source_id));

        Ok(TestRunSourceStorage::new(&self.test_id, &self.test_run_id, repo_id, source_id, self.source_path.clone(), replace).await?)
    }

    pub async fn get_source_ids(&self) -> anyhow::Result<Vec<String>> {
        let mut test_run_sources = Vec::new();

        let mut entries = fs::read_dir(&self.source_path).await?;     
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                if let Some(folder_name) = entry.file_name().to_str() {
                    test_run_sources.push(folder_name.to_string());
                }
            }
        }

        Ok(test_run_sources)        
    }
}

#[derive(Clone, Debug)]
pub struct TestRunSourceStorage {
    pub id: String,
    pub path: PathBuf,
    pub repo_id: String,
    pub source_id: String,
    pub test_id: String,
    pub test_run_id: String,
}

impl TestRunSourceStorage {
    pub async fn new(test_id: &str, test_run_id: &str, repo_id: &str, source_id: &str, parent_path: PathBuf, replace: bool) -> anyhow::Result<Self> {
        
        let id = get_test_run_source_id(repo_id, source_id);
        log::debug!("Creating TestRunSourceStorage for ID {:?} in folder: {:?}", &id, &parent_path);

        let path = parent_path.join(&id);
        let bootstrap_data_path = path.join("bootstrap_data");            
        let change_log_path = path.join("change_log");

        if replace && path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        if !path.exists() {
            fs::create_dir_all(&path).await?;
            fs::create_dir_all(&bootstrap_data_path).await?;
            fs::create_dir_all(&change_log_path).await?;
        }

        Ok(Self {
            id,
            path: path,
            repo_id: repo_id.to_string(),
            source_id: source_id.to_string(),
            test_id: test_id.to_string(),
            test_run_id: test_run_id.to_string(),
        })
    }
}