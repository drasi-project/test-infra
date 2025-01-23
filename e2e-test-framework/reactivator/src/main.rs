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
    log::info!("Started Test Reactivator with - {:?}", cfg);

    // Start the Web API.
    web_api::start_web_api(cfg).await;
}