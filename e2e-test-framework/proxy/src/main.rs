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

use clap::Parser;

mod web_api;

#[derive(Clone, Debug, Parser)]
#[clap(disable_help_flag = true)]
pub struct Params {
    #[arg(env = "PORT")]
    pub proxy_port: u16,

    #[arg(env = "SOURCE_ID")]
    pub proxy_source_id: String,

    #[arg(short = 's', long = "test_run_source_id", env = "TEST_RUN_SOURCE_ID")]
    pub test_run_source_id: String,

    #[arg(short = 'h', long = "test_service_host", env = "TEST_SERVICE_HOST", default_value = "test-service")]
    pub test_service_host: String,

    #[arg(short = 'p', long = "test_service_port", env = "TEST_SERVICE_PORT", default_value_t = 63123)]
    pub test_service_port: u16,
}

#[tokio::main]
async fn main() {
     
     env_logger::init();

     let cfg = Params::parse();
     log::info!("Started Proxy with - {:?}", cfg);

    // Start the Web API.
    web_api::start_web_api(cfg).await;
}
