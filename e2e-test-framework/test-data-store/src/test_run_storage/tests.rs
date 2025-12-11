// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::*;
    use crate::{TestDataStore, TestDataStoreConfig};
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn setup_test_env() -> anyhow::Result<(Arc<TestDataStore>, TestRunId, TempDir)> {
        let temp_dir = TempDir::new()?;
        let config = TestDataStoreConfig {
            data_store_path: Some(temp_dir.path().to_string_lossy().to_string()),
            test_repos: Some(vec![]),
            data_collection_folder: None,
            delete_on_start: None,
            delete_on_stop: None,
            test_repo_folder: None,
            test_run_folder: None,
        };
        let data_store = Arc::new(TestDataStore::new(config).await?);
        let test_run_id = TestRunId::new("test-repo", "test-001", "run-001");
        Ok((data_store, test_run_id, temp_dir))
    }

    #[tokio::test]
    async fn test_reaction_storage_creation() -> anyhow::Result<()> {
        let (data_store, test_run_id, _temp_dir) = setup_test_env().await?;

        // Create reaction storage
        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-001");
        let reaction_storage = data_store
            .get_test_run_reaction_storage(&reaction_id)
            .await?;

        // Verify storage properties
        assert_eq!(reaction_storage.id, reaction_id);
        assert!(reaction_storage.path.exists());
        assert!(reaction_storage.reaction_output_path.exists());
        assert_eq!(
            reaction_storage.reaction_output_path.file_name().unwrap(),
            "output_log"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_storage_path_structure() -> anyhow::Result<()> {
        let (data_store, test_run_id, _temp_dir) = setup_test_env().await?;

        // Create reaction storage
        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-002");
        let reaction_storage = data_store
            .get_test_run_reaction_storage(&reaction_id)
            .await?;

        // Verify path structure follows the pattern:
        // <test_data_cache>/test_runs/<test_run_id>/reactions/<reaction_id>/
        let path_str = reaction_storage.path.to_string_lossy();
        assert!(path_str.contains("test_runs"));
        assert!(path_str.contains(&test_run_id.to_string()));
        assert!(path_str.contains("reactions"));
        assert!(path_str.contains("reaction-002"));

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_reaction_storage() -> anyhow::Result<()> {
        let (data_store, test_run_id, _temp_dir) = setup_test_env().await?;

        // Create multiple reaction storages
        let reaction_ids = vec!["reaction-001", "reaction-002", "reaction-003"];
        let mut storages = Vec::new();

        for reaction_id_str in &reaction_ids {
            let reaction_id = TestRunReactionId::new(&test_run_id, reaction_id_str);
            let storage = data_store
                .get_test_run_reaction_storage(&reaction_id)
                .await?;
            storages.push(storage);
        }

        // Verify all storages are unique and exist
        assert_eq!(storages.len(), 3);
        for (i, storage) in storages.iter().enumerate() {
            assert!(storage.path.exists());
            assert!(storage.reaction_output_path.exists());
            assert_eq!(storage.id.test_reaction_id, reaction_ids[i]);
        }

        // Verify paths are different
        assert_ne!(storages[0].path, storages[1].path);
        assert_ne!(storages[1].path, storages[2].path);

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_storage_persistence() -> anyhow::Result<()> {
        let temp_dir = TempDir::new()?;
        let test_run_id = TestRunId::new("test-repo", "test-001", "run-001");
        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-001");

        // Create storage with first data store instance
        {
            let config = TestDataStoreConfig {
                data_store_path: Some(temp_dir.path().to_string_lossy().to_string()),
                test_repos: Some(vec![]),
                data_collection_folder: None,
                delete_on_start: None,
                delete_on_stop: None,
                test_repo_folder: None,
                test_run_folder: None,
            };
            let data_store = Arc::new(TestDataStore::new(config).await?);
            let reaction_storage = data_store
                .get_test_run_reaction_storage(&reaction_id)
                .await?;

            // Write a test file to verify persistence
            let test_file = reaction_storage.reaction_output_path.join("test.txt");
            std::fs::write(&test_file, "test content")?;
            assert!(test_file.exists());
        }

        // Create new data store instance and verify storage still exists
        {
            let config = TestDataStoreConfig {
                data_store_path: Some(temp_dir.path().to_string_lossy().to_string()),
                test_repos: Some(vec![]),
                data_collection_folder: None,
                delete_on_start: None,
                delete_on_stop: None,
                test_repo_folder: None,
                test_run_folder: None,
            };
            let data_store = Arc::new(TestDataStore::new(config).await?);
            let reaction_storage = data_store
                .get_test_run_reaction_storage(&reaction_id)
                .await?;

            // Verify the test file still exists
            let test_file = reaction_storage.reaction_output_path.join("test.txt");
            assert!(test_file.exists());
            let content = std::fs::read_to_string(&test_file)?;
            assert_eq!(content, "test content");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_storage_output_log_directory() -> anyhow::Result<()> {
        let (data_store, test_run_id, _temp_dir) = setup_test_env().await?;

        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-001");
        let reaction_storage = data_store
            .get_test_run_reaction_storage(&reaction_id)
            .await?;

        // Verify output_log directory is created
        assert!(reaction_storage.reaction_output_path.is_dir());
        assert_eq!(
            reaction_storage.reaction_output_path.file_name().unwrap(),
            "output_log"
        );

        // Write a file to output_log to verify it's writable
        let log_file = reaction_storage.reaction_output_path.join("reaction.jsonl");
        std::fs::write(&log_file, "{\"test\": \"data\"}\n")?;
        assert!(log_file.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_reaction_storage_alongside_query_storage() -> anyhow::Result<()> {
        let (data_store, test_run_id, _temp_dir) = setup_test_env().await?;

        // Create both query and reaction storage for the same test run
        let query_id = TestRunQueryId::new(&test_run_id, "query-001");
        let reaction_id = TestRunReactionId::new(&test_run_id, "reaction-001");

        let query_storage = data_store.get_test_run_query_storage(&query_id).await?;
        let reaction_storage = data_store
            .get_test_run_reaction_storage(&reaction_id)
            .await?;

        // Verify they're in different directories
        assert!(query_storage.path.to_string_lossy().contains("queries"));
        assert!(reaction_storage
            .path
            .to_string_lossy()
            .contains("reactions"));

        // Verify parent directory is the same (test_run directory)
        assert_eq!(
            query_storage.path.parent().unwrap().parent().unwrap(),
            reaction_storage.path.parent().unwrap().parent().unwrap()
        );

        Ok(())
    }
}
