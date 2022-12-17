use std::sync::Arc;
use std::time::Duration;

use anonymizer::anonymize_ip;
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};
use clickhouse::Row;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::topic_partition_list::TopicPartitionList;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{debug, debug_span, error, info, instrument, warn, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

// TODO: possibly extract to lib module
pub mod model {
    include!(concat!(env!("OUT_DIR"), "/http_log_capnp.rs"));
}

use crate::model::http_log_record;

#[derive(Debug, Deserialize, Clone)]
struct KafkaConfig {
    topic: String,
    brokers: String,
    group_id: String,
}

#[derive(Debug, Deserialize)]
struct ClickHouseConfig {
    url: String,
    user: String,
    password: String,
    database: String,
    log_table: String,
    max_entries: u64,
    insert_period: Option<u64>,
}

impl From<&ClickHouseConfig> for clickhouse::Client {
    #[inline]
    fn from(cfg: &ClickHouseConfig) -> Self {
        clickhouse::Client::default()
            .with_url(&cfg.url)
            .with_user(&cfg.user)
            .with_password(&cfg.password)
            .with_database(&cfg.database)
    }
}

const DEFAULT_WORKERS: usize = 4;
#[inline(always)]
const fn default_workers() -> usize {
    DEFAULT_WORKERS
}

const DEFAULT_RUST_LOG: &str = "INFO";
#[inline(always)]
fn default_rust_log() -> String {
    DEFAULT_RUST_LOG.to_string()
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default = "default_rust_log")]
    rust_log: String,
    kafka: KafkaConfig,
    ch: ClickHouseConfig,
}

impl Config {
    #[inline]
    pub fn from_env() -> Result<Self, config::ConfigError> {
        config::Config::builder()
            .add_source(config::Environment::default().separator("__"))
            .build()?
            .try_deserialize()
    }
}

// TODO: ClickHouse works with `&str` or
//  - https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html
//  - or possibly https://docs.rs/serde_bytes/latest/serde_bytes/
#[allow(unused)]
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
    type Error = ();

    fn try_from(value: OwnedMessage) -> Result<Self, Self::Error> {
        let mut buffer = value.payload().expect("failed to get message payload");

        let raw_data = read_message_from_flat_slice(&mut buffer, ReaderOptions::new())
            .expect("failed to build reader");

        let data = raw_data
            .get_root::<http_log_record::Reader<'_>>()
            .expect("failed to get reader");

        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            data.get_timestamp_epoch_milli() as i128 * 1_000_000,
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

type LogInserter = clickhouse::inserter::Inserter<HttpLog>;

struct HttpLogConsumerContext;

impl rdkafka::client::ClientContext for HttpLogConsumerContext {}

impl ConsumerContext for HttpLogConsumerContext {
    #[instrument(skip(self, result))]
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!("offsets committed successfully"),
            Err(e) => error!(reason = ?e, "failed to commit offsets"),
        };
    }
}

type HttpLogConsumer = StreamConsumer<HttpLogConsumerContext>;

#[derive(Clone)]
#[allow(unused)]
struct Context {
    kafka: KafkaConfig,
    ch: Arc<clickhouse::Client>,
    inserter: Arc<Mutex<LogInserter>>,
}

impl Context {
    #[inline]
    fn new(kafka: KafkaConfig, ch: clickhouse::Client, inserter: LogInserter) -> Self {
        Self {
            kafka,
            ch: Arc::new(ch),
            inserter: Arc::new(Mutex::new(inserter)),
        }
    }
}

/// Create new Kafka consumer and subscribe it to a topic specified in config.
///
/// For configuration options see:
/// [`librdkafka` docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
#[instrument(name = "create", skip_all)]
fn create_consumer(cfg: KafkaConfig) -> rdkafka::error::KafkaResult<HttpLogConsumer> {
    let consumer: HttpLogConsumer = rdkafka::ClientConfig::new()
        .set("group.id", cfg.group_id)
        .set("bootstrap.servers", cfg.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "2000")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create_with_context(HttpLogConsumerContext)?;

    consumer.subscribe(&[&cfg.topic])?;
    info!(topic = cfg.topic, "subscribed new Kafka consumer");

    Ok(consumer)
}

// XXX: delivery semantics => offset handling & idempotent write
#[instrument(name = "consumer", skip(ctx))]
async fn run_consumer(id: usize, ctx: Context) {
    let span = &Span::current();

    let consumer = create_consumer(ctx.kafka).expect("consumer creation failed");
    let consumer = Arc::new(consumer);

    let stream_processor = consumer.stream().try_for_each(|msg| {
        let consumer = consumer.clone();
        let inserter = ctx.inserter.clone();

        async move {
            // Process each message
            let msg = msg.detach();

            let topic = msg.topic().to_owned();
            let partition = msg.partition();
            let offset = msg.offset();

            debug!(parent: span, topic, partition, offset, "processing message");

            let task_span = debug_span!("task");

            tokio::spawn(async move {
                let _enter = task_span.enter();
                // executed on the main thread pool

                // TODO: filter out invalid msgs (or rather report them to stats)
                let mut log = HttpLog::try_from(msg).expect("valid message");
                debug!(partition, offset, "message parsed");

                log.remote_addr = anonymize_ip(log.remote_addr);
                debug!(partition, offset, ?log, "anonymized");

                // XXX: consider keeping just single consumer and scale the group horizontally
                // critical write section
                let insert_span = debug_span!("insert");
                {
                    let _enter = insert_span.enter();

                    let mut out = inserter.lock().await;

                    // FIXME: dev - this simulates data duplication, remove later
                    out.write(&log).await.expect("inserter write failed");

                    out.write(&log).await.expect("inserter write failed");
                    debug!(partition, offset, "written log data");

                    // FIXME: do not commit immediately => inefficient & hits proxy rate limiting
                    match out.commit().await {
                        Ok(q) => {
                            info!(quantities = ?q, "inserts committed");
                            // FIXME: saving out-of-order => potential data loss
                            consumer
                                .store_offset(&topic, partition, offset)
                                // XXX: here we actually should terminate the process
                                .expect("offsets stored");
                        }
                        Err(_) => error!("insert commit failed"),
                    };
                }
            });

            Ok(())
        }
    });

    info!("starting event loop");
    stream_processor.await.expect("stream processing failed");
    warn!("stream processing terminated");

    // TODO: graceful shutdown: terminate inserting
    //inserter
    //    .end()
    //    .await
    //    .expect("inserter shutdown flush failed");
}

// TODO: decide which crate to use to implement the pipeline (this is just a demo)
// TODO: setup shutdown hook & graceful shutdown in general
#[tokio::main]
async fn main() {
    // NOTE: proxy does the recommended 1s rate limiting - need to handle these "errors"
    // TODO: address - latency, data loss, data duplication

    // read configuration from env
    let cfg = Config::from_env().expect("configuration from environment");

    // setup tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(cfg.rust_log))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(tasks = cfg.workers, "starting anonymizer app");

    let ch = clickhouse::Client::from(&cfg.ch);

    let inserter = ch
        .inserter(&cfg.ch.log_table) // NOTE: inserter name = table name
        .expect("failed to create new inserter")
        .with_max_entries(cfg.ch.max_entries)
        .with_period(cfg.ch.insert_period.map(Duration::from_secs));

    let ctx = Context::new(cfg.kafka, ch, inserter);

    // TODO: improve
    (0..cfg.workers)
        .map(|i| tokio::spawn(run_consumer(i, ctx.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async {})
        .await
}
