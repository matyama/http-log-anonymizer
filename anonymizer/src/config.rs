use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub topic: String,
    pub brokers: String,
    pub group_id: String,
    pub retries: usize,
    pub retry_delay: u64,
}

#[derive(Debug, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub tcp_keepalive: u64,
    pub target_table: String,
    pub create_table: bool,
    #[allow(unused)]
    pub max_entries: u64,
    pub rate_limit: Option<u64>,
    pub retries: u64,
}

#[inline(always)]
fn default_num_consumers() -> usize {
    1
}

#[inline(always)]
fn default_rust_log() -> String {
    "INFO".to_string()
}

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_num_consumers")]
    pub num_consumers: usize,
    #[serde(default = "default_rust_log")]
    pub rust_log: String,
    pub kafka: KafkaConfig,
    pub ch: ClickHouseConfig,
}

impl Config {
    #[inline]
    pub fn from_env() -> Result<Self> {
        config::Config::builder()
            .add_source(config::Environment::default().separator("__"))
            .build()?
            .try_deserialize()
            .map_err(|e| anyhow!(Error::Config(e)))
    }
}
