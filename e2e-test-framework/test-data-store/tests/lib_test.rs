use std::sync::Arc;

use tempfile::TempDir;
use test_data_store::{TestDataStore, TestDataStoreConfig};

#[tokio::test]
async fn test_new_testdatastore() -> anyhow::Result<()>{
    let temp_dir = TempDir::new().unwrap();

    let data_store_config = TestDataStoreConfig {
        data_store_path: Some(temp_dir.path().to_str().unwrap().to_string()),
        delete_on_stop: Some(true),
        ..Default::default()
    };
    let data_store = Arc::new(TestDataStore::new(data_store_config).await?);

    assert_eq!(data_store.root_path.exists(), true);
    assert_eq!(data_store.get_data_collection_ids().await?.is_empty(), true);
    assert_eq!(data_store.get_test_repo_ids().await?.is_empty(), true);
    assert_eq!(data_store.get_test_run_ids().await?.is_empty(), true);

    drop(data_store);

    assert_eq!(temp_dir.path().exists(), false);

    Ok(())
}
