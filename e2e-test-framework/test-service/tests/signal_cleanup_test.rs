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

use std::process::{Command, Stdio};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

#[tokio::test]
#[ignore = "Integration test requires built binary"]
async fn test_ctrl_c_cleanup_with_delete_on_stop() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let test_data_path = temp_dir.path().join("test_data_cache");

    // Create a test config
    let config = serde_json::json!({
        "data_store": {
            "data_store_path": test_data_path.to_str().unwrap(),
            "delete_on_start": false,
            "delete_on_stop": true,
            "test_repos": []
        },
        "test_run_host": {
            "drasi_lib_instances": [],
            "queries": [],
            "reactions": [],
            "sources": []
        }
    });

    let config_file = temp_dir.path().join("test_config.json");
    std::fs::write(&config_file, serde_json::to_string_pretty(&config)?)?;

    // Start the test service
    let mut child = Command::new("cargo")
        .args([
            "run",
            "--manifest-path",
            "./test-service/Cargo.toml",
            "--",
            "--config",
            config_file.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Wait for service to start and create the directory
    sleep(Duration::from_secs(3)).await;

    // Verify the directory was created
    assert!(
        test_data_path.exists(),
        "Test data directory should exist after startup"
    );

    // Send SIGINT (Ctrl+C) to the process
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)?;
    }

    #[cfg(not(unix))]
    {
        // On Windows, we can try to terminate gracefully
        child.kill()?;
    }

    // Wait for the process to exit
    let _exit_status = child.wait()?;

    // Give a moment for cleanup to complete
    sleep(Duration::from_millis(500)).await;

    // Verify the directory was deleted
    assert!(
        !test_data_path.exists(),
        "Test data directory should be deleted after Ctrl+C with delete_on_stop=true"
    );

    Ok(())
}

#[tokio::test]
#[ignore = "Integration test requires built binary"]
async fn test_ctrl_c_no_cleanup_without_delete_on_stop() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let test_data_path = temp_dir.path().join("test_data_cache_no_delete");

    // Create a test config with delete_on_stop = false
    let config = serde_json::json!({
        "data_store": {
            "data_store_path": test_data_path.to_str().unwrap(),
            "delete_on_start": false,
            "delete_on_stop": false,
            "test_repos": []
        },
        "test_run_host": {
            "drasi_lib_instances": [],
            "queries": [],
            "reactions": [],
            "sources": []
        }
    });

    let config_file = temp_dir.path().join("test_config_no_delete.json");
    std::fs::write(&config_file, serde_json::to_string_pretty(&config)?)?;

    // Start the test service
    let mut child = Command::new("cargo")
        .args([
            "run",
            "--manifest-path",
            "./test-service/Cargo.toml",
            "--",
            "--config",
            config_file.to_str().unwrap(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;

    // Wait for service to start and create the directory
    sleep(Duration::from_secs(3)).await;

    // Verify the directory was created
    assert!(
        test_data_path.exists(),
        "Test data directory should exist after startup"
    );

    // Send SIGINT (Ctrl+C) to the process
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)?;
    }

    #[cfg(not(unix))]
    {
        // On Windows, we can try to terminate gracefully
        child.kill()?;
    }

    // Wait for the process to exit
    let _exit_status = child.wait()?;

    // Give a moment for cleanup to complete
    sleep(Duration::from_millis(500)).await;

    // Verify the directory was NOT deleted
    assert!(
        test_data_path.exists(),
        "Test data directory should NOT be deleted after Ctrl+C with delete_on_stop=false"
    );

    Ok(())
}
