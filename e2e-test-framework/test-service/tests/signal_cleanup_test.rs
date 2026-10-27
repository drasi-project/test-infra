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

#![cfg(unix)]

mod common;

use common::TestService;

#[tokio::test]
async fn test_ctrl_c_cleanup_with_delete_on_stop() -> anyhow::Result<()> {
    let mut service = TestService::start(true).await?;
    let test_data_path = service.data_path();

    // Verify the directory was created
    assert!(
        test_data_path.exists(),
        "Test data directory should exist after startup"
    );

    let marker = test_data_path.join("cleanup-marker");
    std::fs::write(&marker, "owned test data")?;
    let neighbor = test_data_path.with_file_name("unrelated-marker");
    std::fs::write(&neighbor, "preserve outside data store")?;
    service.shutdown().await?;

    // Verify the directory was deleted
    assert!(
        !test_data_path.exists(),
        "Test data directory should be deleted after Ctrl+C with delete_on_stop=true"
    );
    assert_eq!(
        std::fs::read_to_string(neighbor)?,
        "preserve outside data store"
    );

    Ok(())
}

#[tokio::test]
async fn test_ctrl_c_no_cleanup_without_delete_on_stop() -> anyhow::Result<()> {
    let mut service = TestService::start(false).await?;
    let test_data_path = service.data_path();

    // Verify the directory was created
    assert!(
        test_data_path.exists(),
        "Test data directory should exist after startup"
    );

    let marker = test_data_path.join("preserved-marker");
    std::fs::write(&marker, "owned test data")?;
    service.shutdown().await?;

    // Verify the directory was NOT deleted
    assert!(
        test_data_path.exists(),
        "Test data directory should NOT be deleted after Ctrl+C with delete_on_stop=false"
    );
    assert_eq!(std::fs::read_to_string(marker)?, "owned test data");

    Ok(())
}
