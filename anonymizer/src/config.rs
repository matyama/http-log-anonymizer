//! This module defines basic configuration structures which can be loaded based on a set of
//! environment variables.
//!
//! # Kafka
//! The [`KafkaConfig`] structure is defined by (and loaded from) the following set of environment
//! variables:
//!  - `KAFKA__TOPIC` is the topic that the consumers will subscribe to
//!  - `KAFKA__BROKERS` is the list of Kafka bootstrap servers (separated by comma)
//!  - `KAFKA__GROUP_ID` is the identifier of the common consumer group
//!  - `KAFKA__RETRIES` specifies how many times an offset commit will be retried by a consumer
//!  - `KAFKA__RETRY_DELAY` is the delay in milliseconds between consumer offset commit retries
//!
//! ## Example setup
//! ```bash
//! export KAFKA__TOPIC=http_log
//! export KAFKA__BROKERS="localhost:9092"
//! export KAFKA__GROUP_ID=anonymizer-group
//! export KAFKA__RETRIES=3
//! export KAFKA__RETRY_DELAY=2000
//! ```
//!
//! # ClickHouse
//! The [`ClickHouseConfig`] structure is defined by (and loaded from) the following set of
//! environment variables:
//!  - `CH__URL` is the URL of the ClickHouse server to connect to
//!  - `CH__USER` and `CH__PASSWORD` are the credentials
//!  - `CH__DATABASE` and `CH__TARGET_TABLE` is the database and table that the pipeline (sink)
//!    will target its output
//!  - `CH__TIMEOUT` is the maximum time in seconds for a ClickHouse request/response cycle
//!  - `CH__CREATE_TABLE` specifies whether the sink should (re)create the `CH__TARGET_TABLE` when
//!    it starts (`true`) or should expect it's alredy present in the database (`false`)
//!  - `CH__MAX_BLOCK_SIZE` is the size of each insert block buffered by the sink before output
//!  - `CH__RATE_LIMIT` is the duration in seconds between each HTTP request (e.g. insert) done by
//!    the sink
//!  - `CH__RETRIES` is the number of times a query is retried by the sink, note that currenlty
//!    this applies only to the create table query
//!
//! ## Example setup
//! ```bash
//! export CH__URL="http://localhost:8124"
//! export CH__USER=default
//! export CH__PASSWORD=
//! export CH__DATABASE=default
//! export CH__TIMEOUT=10
//! export CH__TCP_KEEPALIVE=60
//! export CH__TARGET_TABLE=http_log
//! export CH__CREATE_TABLE=true
//! export CH__MAX_BLOCK_SIZE=4096
//! export CH__RATE_LIMIT=65
//! export CH__RETRIES=3
//! ```
//!
//! # Telemetry
//! The [`TelemetryConfig`] structure is defined by (and loaded from) the following set of
//! environment variables:
//!  - `TELEMETRY__PROMETHEUS_EXPORTER_PORT` is the port that the metrics server will listen to for
//!    Prometheus metrics scraping requests
//!  - `TELEMETRY__LOKI_URL` is the URL of the Grafana Loki instance to publish logs/traces to
//!
//! ## Example setup
//! ```bash
//! export TELEMETRY__PROMETHEUS_EXPORTER_PORT=9464
//! export TELEMETRY__LOKI_URL="http://localhost:3100"
//! ```
//!
//! ## Caveat: native binary
//! Note that when executed as a native binary, the server will run on the host network, so the
//! Prometheus integration will only work as long as the Prometheus instance also runs in, or has
//! access to, the host network.
//!
//! # Anonymizer
//! The [`Config`] structure represents the whole application configuration, includes both the
//! [`KafkaConfig`] and [`ClickHouseConfig`], and additionally is defined by (and loaded from) the
//! following set of environment variables:
//!  - `RUST_LOG` is the standard Rust log configuration string
//!  - `NUM_CONSUMERS` is the number of consumer tasks that will be spawned in the Tokio runtime
//!  - `SHUTDOWN_TIMEOUT` is the time in seconds for how long will the application wait for its
//!    subsystems to gracefully shut down (accepts fractions as [`f64`])
//!  - `MPSC_BUFFER_SIZE` is the size of an `mpsc` buffer between Kafka consumers and ClickHouse
//!    sink
//!  - `MPSC_SEND_TIMEOUT` is the timeout in seconds for sending insert reqeusts to ClickHouse sink
//!    (accepts fractions as [`f64`])
//!
//! # Example setup
//! ```bash
//! export RUST_LOG=anonymizer=INFO,librdkafka=WARN
//! export NUM_CONSUMERS=2
//! export SHUTDOWN_TIMEOUT=5
//! export MPSC_BUFFER_SIZE=1024
//! export MPSC_SEND_TIMEOUT=1
//! ```
use std::time::Duration;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_with::serde_as;
use url::Url;

use crate::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub topic: String,
    pub brokers: String,
    pub group_id: String,
    pub retries: usize,
    pub retry_delay: u64,
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct ClickHouseConfig {
    pub url: Url,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub timeout: Duration,
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    pub tcp_keepalive: Duration,
    pub target_table: String,
    pub create_table: bool,
    pub max_block_size: u16,
    #[serde_as(as = "Option<serde_with::DurationSeconds<u64>>")]
    pub rate_limit: Option<Duration>,
    pub retries: u64,
}

#[derive(Debug, Deserialize)]
pub struct TelemetryConfig {
    pub prometheus_exporter_port: u16,
    pub loki_url: Url,
}

#[inline(always)]
fn default_num_consumers() -> usize {
    1
}

#[inline(always)]
fn default_rust_log() -> String {
    "INFO".to_string()
}

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct Config {
    /// Number of Kafka consumers to start for the message ingest (default: 1)
    #[serde(default = "default_num_consumers")]
    pub num_consumers: usize,

    /// Time in seconds of the graceful shutdown period (accepts fractions as [`f64`])
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub shutdown_timeout: Duration,

    /// Component log levels as the standard `RUST_LOG` configuration string (default: `INFO`)
    #[serde(default = "default_rust_log")]
    pub rust_log: String,

    /// Size of an `mpsc` buffer between Kafka consumers and ClickHouse sink
    pub mpsc_buffer_size: usize,

    /// Timeout in seconds for sending insert requests from Kafka consumers to ClickHouse sink
    /// (accepts fractions as [`f64`])
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub mpsc_send_timeout: Duration,

    /// Kafka integration configs
    pub kafka: KafkaConfig,

    /// ClickHouse integration configs
    pub ch: ClickHouseConfig,

    /// Configirations for metrics, tracing and logging
    pub telemetry: TelemetryConfig,
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
