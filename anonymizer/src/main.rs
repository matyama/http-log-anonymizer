use std::{sync::Arc, time::Duration};

use anonymizer::anonymize_ip;
use callysto::futures::StreamExt;
use callysto::prelude::message::*;
use callysto::prelude::{
    CStream, Callysto, CallystoError, Config as CallystoConfig, Context, IsolationLevel, Result,
};
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{debug, error, info, instrument};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// TODO: possibly extract to lib module
pub mod model {
    include!(concat!(env!("OUT_DIR"), "/http_log_capnp.rs"));
}

use crate::model::http_log_record;

const DEFAULT_RUST_LOG: &str = "INFO";
#[inline(always)]
fn default_rust_log() -> String {
    DEFAULT_RUST_LOG.to_string()
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_rust_log")]
    rust_log: String,
}

impl Config {
    pub fn from_env() -> std::result::Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::Environment::default().separator("__"))
            .build()?
            .try_deserialize()
    }
}

// TODO: ClickHouse works with `&str` or
//  - https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html
//  - or possibly https://docs.rs/serde_bytes/latest/serde_bytes/
#[allow(dead_code)]
#[derive(Debug, Row, Serialize)]
struct HttpLog {
    #[serde(with = "clickhouse::serde::time::datetime")]
    timestamp: OffsetDateTime,
    resource_id: u64,
    bytes_sent: u64,
    request_time_milli: u64,
    response_status: u16,
    cache_status: String,
    method: String,
    remote_addr: String,
    url: String,
}

impl TryFrom<OwnedMessage> for HttpLog {
    // TODO: thiserror and/or anyhow
    type Error = CallystoError;

    fn try_from(value: OwnedMessage) -> Result<Self> {
        let mut buffer = value.payload().expect("failed to get message payload");

        let raw_data = read_message_from_flat_slice(&mut buffer, ReaderOptions::new())
            .expect("failed to build reader");

        let data = raw_data
            .get_root::<http_log_record::Reader<'_>>()
            .expect("failed to get reader");

        // FIXME: conversion does not seem to be quite right (e.g.`1970-01-20 08:13:39`)
        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            data.get_timestamp_epoch_milli() as i128 * 1000,
        )
        .expect("invalid timestamp");

        let cache_status: &str = data.get_cache_status().expect("failed to get cache status");

        let method: &str = data.get_method().expect("failed to get method");

        let remote_addr: &str = data
            .get_remote_addr()
            .expect("failed to get remote address");

        let url: &str = data.get_url().expect("failed to get url");

        Ok(Self {
            timestamp,
            resource_id: data.get_resource_id(),
            bytes_sent: data.get_bytes_sent(),
            request_time_milli: data.get_request_time_milli(),
            response_status: data.get_response_status(),
            cache_status: cache_status.to_owned(),
            method: method.to_owned(),
            remote_addr: remote_addr.to_owned(),
            url: url.to_owned(),
        })
    }
}

#[derive(Clone)]
struct SharedState {
    client: Arc<Client>,
}

impl SharedState {
    #[inline]
    fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

#[instrument(skip_all)]
async fn anonymizer_agent(mut stream: CStream, ctx: Context<SharedState>) -> Result<()> {
    let state = ctx.state();

    // TODO: extract configs
    // TODO: consider sharing the inserter directly, not the whole client
    // TODO: delivery semantics => offset handling & idempotent write
    let mut inserter = state
        .client
        .inserter("http_log") // NOTE: inserter name = table name
        .expect("failed to create new inserter")
        .with_max_entries(10) // TODO: just for dev, use much larger value
        //.with_max_entries(500_000)
        .with_period(Some(Duration::from_secs(1)));

    while let Some(msg) = stream.next().await {
        let mut log = msg
            .map(|m| {
                // TODO: dev => remove
                debug!("received {:?}", m);

                // TODO: filter out invalid msgs (or rather report them to stats)
                HttpLog::try_from(m).expect("valid message")
            })
            .expect("infinite Kafka stream");

        // TODO: dev => remove
        debug!("parsed {:?}", log);

        log.remote_addr = anonymize_ip(log.remote_addr);
        //debug!("anonymized", log=?log);
        debug!("anonymized {:?}", log);

        inserter.write(&log).await.expect("inserter write failed");

        // FIXME: do not commit immediately => inefficient & hits proxy rate limiting
        let Ok(_) = inserter.commit().await else {
            error!("inserter commit failed");
            continue;
        };
    }

    // graceful shutdown: terminate inserting
    inserter
        .end()
        .await
        .expect("inserter shutdown flush failed");

    Ok(())
}

fn main() {
    // NOTE: proxy does the recommended 1s rate limiting - need to handle these "errors"
    // TODO: address - latency, data loss, data duplication

    // read configuration from env
    let cfg = Config::from_env().expect("failed to read configuration from environment");

    // setup tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(cfg.rust_log))
        .with(tracing_subscriber::fmt::layer())
        .init();

    //let ch_proxy_url = "http://ch-proxy:8124";
    let ch_proxy_url = "http://localhost:8124";

    // TODO: from env
    let client = Client::default()
        .with_url(ch_proxy_url)
        .with_user("default")
        .with_password("")
        .with_database("default");

    // TODO: decide which crate to use to implement the pipeline (this is just a demo)
    let mut app = Callysto::with_state(SharedState::new(client));

    let mut config = CallystoConfig::default();
    config.kafka_config.isolation_level = IsolationLevel::ReadCommitted;
    //config.kafka_config.enable_auto_commit = false;

    // TODO: include ID arg (for replica distinction)
    app.with_name("anonymizer").with_config(config);

    // TODO: topic, group_id form env
    // TODO: each agent is spawned as a new task => include id (number from args)
    app.agent("anonymizer_agent", app.topic("http_log"), anonymizer_agent);

    // TODO: propper logging/tracing
    info!("starting anonymizer app");
    app.run();
}
