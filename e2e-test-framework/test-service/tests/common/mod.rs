// Copyright 2026 The Drasi Authors.
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

use std::{
    fs::{self, File},
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    time::Duration,
};

use anyhow::{bail, ensure, Context, Result};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::{sleep, Instant};

const DEADLINE: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(25);

pub struct TestService {
    child: Child,
    directory: TempDir,
    log_path: PathBuf,
    pub client: reqwest::Client,
    pub base_url: String,
}

impl TestService {
    pub async fn start(delete_on_stop: bool) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix(".test-service-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))?;
        let source_path = directory.path().join("repo");
        let script_path = source_path.join("api_test/sources/test_source_001/changes");
        fs::create_dir_all(&script_path)?;
        fs::write(
            script_path.join("empty.jsonl"),
            concat!(
                "{\"kind\":\"Header\",\"start_time\":\"2024-01-01T00:00:00Z\"}\n",
                "{\"kind\":\"Finish\",\"offset_ns\":0}\n"
            ),
        )?;
        let config = json!({
            "data_store": {
                "data_store_path": directory.path().join("data"),
                "delete_on_start": false,
                "delete_on_stop": delete_on_stop,
                "test_repos": [{
                    "id": "test_repo",
                    "kind": "LocalStorage",
                    "source_path": source_path,
                    "local_tests": [{
                        "test_id": "api_test",
                        "version": 1,
                        "sources": [{
                            "kind": "Script",
                            "test_source_id": "test_source_001",
                            "source_change_generator": {
                                "kind": "Script",
                                "script_file_folder": "changes",
                                "spacing_mode": "none",
                                "time_mode": "recorded"
                            },
                            "source_change_dispatchers": [{"kind": "Console"}]
                        }]
                    }]
                }]
            },
            "test_run_host": {"test_runs": []},
            "data_collector": {"data_collections": []}
        });
        let config_path = directory.path().join("config.json");
        fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
        let log_path = directory.path().join("service.log");
        let log = File::create(&log_path)?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3))
            .build()?;
        let child = Command::new(env!("CARGO_BIN_EXE_test-service"))
            .args(["--port", "0", "--config"])
            .arg(&config_path)
            .current_dir(directory.path())
            .env_remove("DRASI_CONFIG_FILE")
            .env_remove("DRASI_DATA_STORE_PATH")
            .env_remove("DRASI_PORT")
            .env_remove("DRASI_PRUNE_DATA_STORE")
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .context("starting Cargo's built test-service binary")?;
        let mut service = Self {
            child,
            directory,
            log_path,
            client,
            base_url: String::new(),
        };
        service.wait_until_ready().await?;
        Ok(service)
    }

    pub fn data_path(&self) -> PathBuf {
        self.directory.path().join("data")
    }

    fn logs(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    async fn wait_until_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                bail!(
                    "test-service exited before readiness ({status}):\n{}",
                    self.logs()
                );
            }
            // Port zero lets the owned service reserve its own port without a
            // bind/drop race or contacting another user's service.
            if self.base_url.is_empty() {
                for line in self.logs().lines() {
                    if let Some((_, address)) =
                        line.split_once("Test Service Web API listening on http://")
                    {
                        let address: SocketAddr = address.trim().parse()?;
                        ensure!(address.port() != 0, "service did not report its bound port");
                        self.base_url = format!("http://127.0.0.1:{}", address.port());
                        break;
                    }
                }
            }
            if !self.base_url.is_empty() {
                if let Ok(response) = self.client.get(format!("{}/", self.base_url)).send().await {
                    if response.status().is_success() {
                        let state: serde_json::Value = response.json().await?;
                        ensure!(
                            state["data_store"]["path"].as_str() == self.data_path().to_str(),
                            "readiness response belongs to a different data store: {state}"
                        );
                        return Ok(());
                    }
                }
            }
            sleep(POLL_INTERVAL).await;
        }
        bail!("test-service readiness deadline expired:\n{}", self.logs());
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        ensure!(
            self.child.try_wait()?.is_none(),
            "test-service exited before shutdown:\n{}",
            self.logs()
        );
        #[cfg(unix)]
        {
            use nix::{
                sys::signal::{kill, Signal},
                unistd::Pid,
            };
            kill(Pid::from_raw(self.child.id().try_into()?), Signal::SIGINT)?;
        }
        #[cfg(not(unix))]
        self.child.kill()?;

        let status = self.wait_for_exit().await?;
        #[cfg(unix)]
        ensure!(
            status.success(),
            "test-service failed ({status}):\n{}",
            self.logs()
        );
        #[cfg(not(unix))]
        let _ = status;
        Ok(())
    }

    async fn wait_for_exit(&mut self) -> Result<ExitStatus> {
        let deadline = Instant::now() + DEADLINE;
        while Instant::now() < deadline {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            sleep(POLL_INTERVAL).await;
        }
        bail!("test-service shutdown deadline expired:\n{}", self.logs());
    }
}

impl Drop for TestService {
    fn drop(&mut self) {
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            // Panic/error fallback: kill only our child and reap it before the
            // private directory is dropped. No subprocess output pipes can fill.
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if std::thread::panicking() {
            eprintln!("test-service log:\n{}", self.logs());
        }
    }
}
