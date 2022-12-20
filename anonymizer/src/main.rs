use std::time::Duration;
use std::{marker::PhantomData, sync::Arc};

use anonymizer::{
    anonymize_ip,
    error::{async_retry, Error},
    kafka::OffsetTracker,
    limiter::RequestLimiter,
};
use anyhow::{anyhow, bail, Result};
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};
use clickhouse_http_client::clickhouse_format::input::JsonCompactEachRowInput;
use clickhouse_http_client::error::{ClientExecuteError, Error as ClickHouseError};
use clickhouse_http_client::isahc::http::StatusCode;
use clickhouse_http_client::isahc::prelude::Configurable;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::topic_partition_list::TopicPartitionList;
use retry::delay::Fixed;
use retry::retry;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    retries: usize,
    retry_delay: u64,
}

#[derive(Debug, Deserialize)]
struct ClickHouseConfig {
    url: String,
    user: String,
    password: String,
    database: String,
    tcp_keepalive: u64,
    target_table: String,
    create_table: bool,
    #[allow(unused)]
    max_entries: u64,
    rate_limit: Option<u64>,
    retries: u64,
}

impl TryFrom<&ClickHouseConfig> for clickhouse_http_client::Client {
    type Error = Error;

    fn try_from(cfg: &ClickHouseConfig) -> Result<Self, Self::Error> {
        let mut builder =
            clickhouse_http_client::ClientBuilder::new().configurable(|http_client_builder| {
                http_client_builder
                    // TODO: add to env config & possibly deprecate pool_idle_timeout
                    .timeout(Duration::from_secs(5))
                    // XXX: these can potentially be on defaults if rate limiter is disabled
                    .max_connections(1)
                    .max_connections_per_host(1)
                    .tcp_keepalive(Duration::from_secs(cfg.tcp_keepalive))
            });

        builder
            .set_url(&cfg.url)
            .map_err(|_| Error::SinkConfig("url"))?;

        let mut ch = builder.build()?;
        ch.set_username_to_header(&cfg.user)
            .map_err(|_| Error::SinkConfig("user"))?;
        ch.set_password_to_header(&cfg.password)
            .map_err(|_| Error::SinkConfig("password"))?;
        ch.set_database_to_header(&cfg.database)
            .map_err(|_| Error::SinkConfig("database"))?;

        Ok(ch)
    }
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
struct Config {
    #[serde(default = "default_num_consumers")]
    num_consumers: usize,
    #[serde(default = "default_rust_log")]
    rust_log: String,
    kafka: KafkaConfig,
    ch: ClickHouseConfig,
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

#[derive(Debug, Serialize)]
struct HttpLog {
    #[serde(with = "time::serde::timestamp")]
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
    type Error = capnp::Error;

    fn try_from(value: OwnedMessage) -> Result<Self, Self::Error> {
        let mut buffer = value.payload().unwrap_or_default();

        let raw_data = read_message_from_flat_slice(&mut buffer, ReaderOptions::new())?;

        let data = raw_data.get_root::<http_log_record::Reader<'_>>()?;

        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            data.get_timestamp_epoch_milli() as i128 * 1_000_000,
        )
        .map_err(|e| capnp::Error {
            kind: capnp::ErrorKind::Failed,
            description: format!("failed to parse 'timestamp_epoch_milli': {}", e),
        })?;

        Ok(Self {
            timestamp,
            resource_id: data.get_resource_id(),
            bytes_sent: data.get_bytes_sent(),
            request_time_milli: data.get_request_time_milli(),
            response_status: data.get_response_status(),
            cache_status: data.get_cache_status()?.to_owned(),
            method: data.get_method()?.to_owned(),
            remote_addr: data.get_remote_addr()?.to_owned(),
            url: data.get_url()?.to_owned(),
        })
    }
}

type CompactJsonRow = Vec<Value>;

impl From<HttpLog> for CompactJsonRow {
    #[inline]
    fn from(value: HttpLog) -> Self {
        vec![
            value.timestamp.unix_timestamp().into(),
            value.resource_id.into(),
            value.bytes_sent.into(),
            value.request_time_milli.into(),
            value.response_status.into(),
            value.cache_status.into(),
            value.method.into(),
            value.remote_addr.into(),
            value.url.into(),
        ]
    }
}

pub trait SinkRow: Into<CompactJsonRow> {
    fn table_ddl(table: &str) -> String;

    fn insert_ddl(table: &str) -> String;
}

// TODO: safety - table name should be properly escaped
impl SinkRow for HttpLog {
    #[inline]
    fn table_ddl(table: &str) -> String {
        format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
              timestamp DateTime NOT NULL,
              resource_id UInt64 NOT NULL,
              bytes_sent UInt64 NOT NULL,
              request_time_milli UInt64 NOT NULL,
              response_status UInt16 NOT NULL,
              cache_status LowCardinality(String) NOT NULL,
              method LowCardinality(String) NOT NULL,
              remote_addr String NOT Null,
              url String NOT NULL
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (resource_id, response_status, remote_addr, timestamp)
            ",
            table,
        )
    }

    #[inline]
    fn insert_ddl(table: &str) -> String {
        format!(
            "
            INSERT INTO {} (
              timestamp,
              resource_id,
              bytes_sent,
              request_time_milli,
              response_status,
              cache_status,
              method,
              remote_addr,
              url
            )",
            table
        )
    }
}

enum ImportResult<E> {
    /// Insert is in progress, buffering the logs
    Pending,
    /// Inserts have been written and committed, returning last offsets as [`TopicPartitionList`]
    Success(TopicPartitionList),
    /// Insert has failed with error `E`
    Failure(E),
}

struct ClickHouseSink<T> {
    /// ClickHouse [`Client`](clickhouse_http_client::Client) for [`HttpLog`]
    ch: clickhouse_http_client::Client,

    /// Buffered data for the next output (ClickHouse insert)
    ///
    /// Original [`HttpLog`] data are stored as [`serde_json::Value`](serde_json::Value)s and sent
    /// to ClickHouse as an insert with values in the
    /// [JsonCompactEachRow](https://clickhouse.com/docs/en/interfaces/formats/#jsoncompacteachrow)
    /// format.
    buffer: Vec<CompactJsonRow>,

    /// Tracker of Kafka offsets corresponding to the last written [`HttpLog`].
    offset_tracker: OffsetTracker,

    /// Limiter that keeps track of the maximum allowed request rate for ClickHouse queries.
    request_limiter: RequestLimiter,

    /// Raw insert query header for the target table.
    insert_ddl: String,

    /// The witness type representing the table schema
    _t: PhantomData<T>,
}

impl<T: SinkRow> ClickHouseSink<T> {
    #[instrument(name = "sink", skip(cfg))]
    async fn new(topic: String, cfg: ClickHouseConfig) -> Result<Self> {
        let ch = clickhouse_http_client::Client::try_from(&cfg)?;
        info!(url = cfg.url, user = cfg.user, "clickhouse client created");

        if cfg.create_table {
            // make sure that the target table exists
            let delay = Duration::from_secs(cfg.rate_limit.unwrap_or_default());
            async_retry(cfg.retries, delay, || async {
                make_table::<T>(&ch, &cfg.target_table).await
            })
            .await?;
        }

        // XXX: does insert_period have to be an option? => use None to _disable_ rate limit
        let request_limiter = RequestLimiter::new(cfg.rate_limit.unwrap_or(10));

        Ok(Self {
            ch,
            buffer: Vec::new(),
            offset_tracker: OffsetTracker::new(topic),
            request_limiter,
            insert_ddl: T::insert_ddl(&cfg.target_table),
            _t: PhantomData,
        })
    }

    #[instrument(skip(self, data))]
    async fn write(
        &mut self,
        partition: i32,
        offset: i64,
        data: T,
    ) -> ImportResult<ClickHouseError> {
        self.buffer.push(data.into());
        debug!("data buffered for insert");

        self.offset_tracker.store(partition, offset);

        let time_left = self.request_limiter.remaining_time();

        if !time_left.is_zero() {
            debug!(?time_left, "waiting to commit insert");
            return ImportResult::Pending;
        }

        let entries = self.buffer.len();
        debug_assert!(entries > 0, "data loss during write");

        // TODO: metrics (inserted, failed)

        // XXX: std::mem::replace(&mut self.buffer, Vec::with_capacity(self.buffer.len()))
        let rows = std::mem::take(&mut self.buffer);
        let input = JsonCompactEachRowInput::new(rows);

        info!(rows = entries, "inserting batch of data");

        // TODO: handle 413 Payload Too Large
        // XXX: settings?
        // XXX: retry - the difficulty is that the buffer would have to be copied each time
        let result = self
            .ch
            .insert_with_format(&self.insert_ddl, input, None)
            .await;

        self.request_limiter.record_request();

        match result {
            Ok(()) => {
                info!(rows = entries, "inserts committed");
                // NOTE: commit is done only once in a while so the clone should be fine
                ImportResult::Success(self.offset_tracker.load())
            }
            Err(e) => {
                warn!(error = ?e, "insert commit failed");
                ImportResult::Failure(e)
            }
        }
    }
}

// XXX: move to SinkRow at the cost of requiring `async_trait`
#[instrument(skip(ch))]
async fn make_table<T: SinkRow>(ch: &clickhouse_http_client::Client, table: &str) -> Result<()> {
    let query = T::table_ddl(table);

    if let Err(e) = ch.execute(query, None).await {
        if let ClickHouseError::ClientExecuteError(ClientExecuteError::StatusCodeMismatch(
            StatusCode::SERVICE_UNAVAILABLE,
        )) = e
        {
            warn!("cannot execute DDL query in clickhouse at the moment due to rate limit");
            bail!(Error::RateLimit);
        } else {
            bail!(Error::Sink(e));
        };
    };

    info!(table, "made sure target table exists in clickhouse");
    Ok(())
}

struct LoggingConsumerContext;

impl rdkafka::client::ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    #[instrument(name = "commit", skip_all)]
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!(?offsets, "offsets committed successfully"),
            Err(e) => warn!(cause = ?e, "failed to commit offsets"),
        };
    }
}

type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

/// Create new Kafka consumer and subscribe it to a topic specified in config.
///
/// For configuration options see:
/// [`librdkafka` docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
#[instrument(name = "create", skip_all)]
fn create_consumer(cfg: KafkaConfig) -> Result<LoggingConsumer> {
    let consumer: LoggingConsumer = rdkafka::ClientConfig::new()
        .set("group.id", cfg.group_id)
        .set("bootstrap.servers", cfg.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create_with_context(LoggingConsumerContext)?;

    consumer.subscribe(&[&cfg.topic])?;
    info!(topic = cfg.topic, "subscribed new Kafka consumer");

    Ok(consumer)
}

#[instrument(name = "consumer", skip(kafka, sink))]
async fn run_consumer(
    id: usize,
    kafka: KafkaConfig,
    sink: Arc<Mutex<ClickHouseSink<HttpLog>>>,
) -> Result<()> {
    let span = &Span::current();

    let retries = kafka.retries;
    let retry_delay = kafka.retry_delay;

    let consumer = create_consumer(kafka)?;
    let consumer = Arc::new(consumer);

    let stream_processor = consumer.stream().try_for_each(|msg| {
        let consumer = consumer.clone();
        let sink = sink.clone();

        async move {
            // process a message
            let msg = msg.detach();

            let topic = msg.topic().to_owned();
            let partition = msg.partition();
            let offset = msg.offset();

            debug!(parent: span, topic, partition, offset, "processing message");

            let mut log = match HttpLog::try_from(msg) {
                Ok(log) => {
                    debug!(partition, offset, "message parsed");
                    log
                }
                Err(e) => {
                    // XXX: report invalid to metrics
                    // NOTE: alternatively one could fail the whole stream processing
                    warn!(cause = ?e, "ignoring invalid message");
                    return Ok(());
                }
            };

            log.remote_addr = anonymize_ip(log.remote_addr);
            debug!(partition, offset, ?log, "anonymized");

            let sink_span = debug_span!("sink");
            let result = {
                // critical write section
                let _enter = sink_span.enter();
                let mut sink = sink.lock().await;
                sink.write(partition, offset, log).await
            };

            match result {
                ImportResult::Pending => {
                    debug!(topic, partition, offset, "done processing message");
                    Ok(())
                }

                ImportResult::Success(tpl) => {
                    // since offsets are sequential and commit retrospective, it should be
                    // sufficient to commit just once after whole batch

                    // NOTE: since rate-limiting is 1 min, this might be more efficient than
                    // frequent auto-commit of _stored_ offsets
                    retry(Fixed::from_millis(retry_delay).take(retries), || {
                        consumer.commit(&tpl, CommitMode::Async)
                    })
                    .map_err(|e| {
                        error!(
                            cause = ?e,
                            tries = e.tries,
                            elapsed = ?e.total_delay,
                            "failed to commit offsets"
                        );
                        e.error
                    })
                }

                ImportResult::Failure(e) => {
                    error!(partition, error = ?e, "insert commit failed");
                    KafkaResult::Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::Application,
                    ))
                }
            }
        }
    });

    info!("starting Kafka message stream processing");

    stream_processor
        .await
        .map_err(|e| anyhow!(Error::Source(e)))?;

    warn!("stream processing terminated");

    // TODO: graceful shutdown: terminate inserting (should we wait till the next insert?)
    Ok(())
}

// TODO: setup shutdown hook & graceful shutdown in general
#[tokio::main]
async fn main() -> Result<()> {
    // read configuration from env
    let cfg = Config::from_env()?;

    // setup tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(cfg.rust_log))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!(
        consumers = cfg.num_consumers,
        "starting anonymizer pipeline"
    );

    let topic = cfg.kafka.topic.clone();

    // crete a shared clickhouse sink
    let sink = ClickHouseSink::<HttpLog>::new(topic, cfg.ch).await?;
    let sink = Arc::new(Mutex::new(sink));

    // spawn a group of Kafka consumers
    (0..cfg.num_consumers)
        .map(|i| tokio::spawn(run_consumer(i, cfg.kafka.clone(), sink.clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async {})
        .await;

    Ok(())
}
