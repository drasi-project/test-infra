use std::sync::Arc;

use tempfile::TempDir;
use test_runner::{TestRunner, TestRunnerConfig, TestRunnerStatus};

use test_data_store::{TestDataStore, TestDataStoreConfig};

#[tokio::test]
async fn test_new_testrunner() -> anyhow::Result<()> {

    let data_store_config = TestDataStoreConfig {
        data_store_path: Some(TempDir::new().unwrap().path().to_str().unwrap().to_string()),
        delete_on_stop: Some(true),
        ..Default::default()
    };
    let data_store = Arc::new(TestDataStore::new(data_store_config).await?);    
    
    let test_runner_config = TestRunnerConfig::default();
    let test_runner = TestRunner::new(test_runner_config, data_store.clone()).await.unwrap();

    assert_eq!(test_runner.get_status().await?, TestRunnerStatus::Initialized);

    Ok(())
}
