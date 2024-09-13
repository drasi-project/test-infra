use std::str::FromStr;

use serde::Serialize;

use crate::config::{ProxyConfig, ReactivatorConfig, ServiceParams, SourceChangeDispatcherConfig, SourceConfig};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TimeMode {
    Live,
    Recorded,
    Rebased(u64),
}

impl FromStr for TimeMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "live" => Ok(Self::Live),
            "recorded" => Ok(Self::Recorded),
            _ => {
                match chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(t) => Ok(Self::Rebased(t.timestamp_nanos_opt().unwrap() as u64)),
                    Err(e) => {
                        anyhow::bail!("Error parsing TimeMode - value:{}, error:{}", s, e);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for TimeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Live => write!(f, "live"),
            Self::Recorded => write!(f, "recorded"),
            Self::Rebased(time) => write!(f, "{}", time),
        }
    }
}

impl Default for TimeMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum SpacingMode {
    None,
    Recorded,
    Fixed(u64),
}

// Implementation of FromStr for ReplayEventSpacingMode.
// For the Fixed variant, the spacing is specified as a string duration such as '5s' or '100n'.
// Supported units are seconds ('s'), milliseconds ('m'), microseconds ('u'), and nanoseconds ('n').
// If the string can't be parsed as a TimeDelta, an error is returned.
impl FromStr for SpacingMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "none" => Ok(Self::None),
            "recorded" => Ok(Self::Recorded),
            _ => {
                // Parse the string as a number, followed by a time unit character.
                let (num_str, unit_str) = s.split_at(s.len() - 1);
                let num = match num_str.parse::<u64>() {
                    Ok(num) => num,
                    Err(e) => {
                        anyhow::bail!("Error parsing SpacingMode: {}", e);
                    }
                };
                match unit_str {
                    "s" => Ok(Self::Fixed(num * 1000000000)),
                    "m" => Ok(Self::Fixed(num * 1000000)),
                    "u" => Ok(Self::Fixed(num * 1000)),
                    "n" => Ok(Self::Fixed(num)),
                    _ => {
                        anyhow::bail!("Invalid SpacingMode: {}", s);
                    }
                }
            }
        }
    }
}

impl std::fmt::Display for SpacingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Recorded => write!(f, "recorded"),
            Self::Fixed(d) => write!(f, "{}", d),
        }
    }
}

impl Default for SpacingMode {
    fn default() -> Self {
        Self::Recorded
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunSource {
    pub id: String,
    pub service_params: ServiceParams,
    pub source_id: String,
    pub test_id: String,
    pub test_repo_id: String,
    pub test_run_id: String,
    pub proxy: Option<TestRunProxy>,
    pub reactivator: Option<TestRunReactivator>,
}

impl TestRunSource {
    pub fn try_from_config(config: &SourceConfig, defaults: &SourceConfig, service_params: ServiceParams) -> anyhow::Result<Self> {
        // If neither the SourceConfig nor the SourceConfig defaults contain a source_id, return an error.
        let source_id = config.source_id.as_ref()
            .or_else( || defaults.source_id.as_ref())
            .map(|source_id| source_id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No source_id provided and no default value found."))?;
        
        // If neither the SourceConfig nor the SourceConfig defaults contain a test_id, return an error.
        let test_id = config.test_id.as_ref()
            .or_else( || defaults.test_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_id provided and no default value found."))?;

        // If neither the SourceConfig nor the SourceConfig defaults contain a test_repo_id, return an error.
        let test_repo_id = config.test_repo_id.as_ref()
            .or_else( || defaults.test_repo_id.as_ref())
            .map(|id| id.to_string())
            .ok_or_else(|| anyhow::anyhow!("No test_repo_id provided and no default value found."))?;

        // If neither the SourceConfig nor the SourceConfig defaults contain a test_run_id, generate one.
        let test_run_id = config.test_run_id.as_ref()
            .or_else( || defaults.test_run_id.as_ref())
            .map(|id| id.to_string())
            .unwrap_or_else(|| chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());

        let id = format!("{}::{}::{}::{}", &test_repo_id, &test_id, &source_id, &test_run_id);

        // If neither the SourceConfig nor the SourceConfig defaults contain a proxy, set it to None.
        let proxy = match config.proxy {
            Some(ref config) => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), config, defaults).ok(),
                    None => TestRunProxy::try_from_config(id.clone(), config, &ProxyConfig::default()).ok(),
                }
            },
            None => {
                match defaults.proxy {
                    Some(ref defaults) => TestRunProxy::try_from_config(id.clone(), defaults, &ProxyConfig::default()).ok(),
                    None => None,
                }
            }
        };

        // If neither the SourceConfig nor the SourceConfig defaults contain a reactivator, set it to None.
        let reactivator = match config.reactivator {
            Some(ref config) => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), config, defaults).ok(),
                    None => TestRunReactivator::try_from_config(id.clone(), config, &ReactivatorConfig::default()).ok(),
                }
            },
            None => {
                match defaults.reactivator {
                    Some(ref defaults) => TestRunReactivator::try_from_config(id.clone(), defaults, &ReactivatorConfig::default()).ok(),
                    None => None,
                }
            }
        };

        Ok(Self {
            id,
            service_params,
            source_id,
            test_id,
            test_repo_id,
            test_run_id,
            proxy,
            reactivator,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunProxy {
    pub test_run_source_id: String,
    pub time_mode: TimeMode,
}

impl TestRunProxy {
    pub fn try_from_config(test_run_source_id: String, config: &ProxyConfig, defaults: &ProxyConfig) -> anyhow::Result<Self> {
        let time_mode = match &config.time_mode {
            Some(time_mode) => TimeMode::from_str(time_mode)?,
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => TimeMode::from_str(time_mode)?,
                    None => TimeMode::default()
                }
            }
        };

        Ok(Self {
            test_run_source_id,
            time_mode,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TestRunReactivator {
    pub dispatchers: Vec<SourceChangeDispatcherConfig>,
    pub ignore_scripted_pause_commands: bool,
    pub spacing_mode: SpacingMode,
    pub start_immediately: bool,
    pub test_run_source_id: String,
    pub time_mode: TimeMode,
}

impl TestRunReactivator {
    pub fn try_from_config(test_run_source_id: String, config: &ReactivatorConfig, defaults: &ReactivatorConfig) -> anyhow::Result<Self> {
        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a ignore_scripted_pause_commands, 
        // set to false.
        let ignore_scripted_pause_commands = config.ignore_scripted_pause_commands
            .or(defaults.ignore_scripted_pause_commands)
            .unwrap_or(false);

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a spacing_mode, 
        // set to SpacingMode::Recorded.
        let spacing_mode = match &config.spacing_mode {
            Some(spacing_mode) => SpacingMode::from_str(spacing_mode)?,
            None => {
                match &defaults.spacing_mode {
                    Some(spacing_mode) => SpacingMode::from_str(spacing_mode)?,
                    None => SpacingMode::Recorded
                }
            }
        };

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a start_immediately, 
        // set to true.
        let start_immediately = config.start_immediately
            .or(defaults.start_immediately)
            .unwrap_or(true);

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a time_mode, 
        // set to TimeMode::Recorded.
        let time_mode = match &config.time_mode {
            Some(time_mode) => TimeMode::from_str(time_mode)?,
            None => {
                match &defaults.time_mode {
                    Some(time_mode) => TimeMode::from_str(time_mode)?,
                    None => TimeMode::Recorded
                }
            }
        };

        // If neither the ReactivatorConfig nor the ReactivatorConfig defaults contain a list of dispatchers, 
        // set to an empty Vec.
        let dispatchers = match &config.dispatchers {
            Some(dispatchers) => dispatchers.clone(),
            None => {
                match &defaults.dispatchers {
                    Some(dispatchers) => dispatchers.clone(),
                    None => Vec::new()
                }
            }
        };

        Ok(Self {
            dispatchers,
            ignore_scripted_pause_commands,
            spacing_mode,
            start_immediately,
            test_run_source_id,
            time_mode,
        })
    }
}