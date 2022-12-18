use std::sync::Arc;
use std::time::Duration;

use anonymizer::anonymize_ip;
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};
use clickhouse::inserter::Inserter;
use clickhouse::Row;
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
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
    tcp_keepalive: u64,
    pool_idle_timeout: u64,
    log_table: String,
    max_entries: u64,
    insert_period: Option<u64>,
}

impl From<&ClickHouseConfig> for clickhouse::Client {
    fn from(cfg: &ClickHouseConfig) -> Self {
        use hyper::client::connect::HttpConnector;

        let mut connector = HttpConnector::new();
        connector.set_keepalive(Some(Duration::from_secs(cfg.tcp_keepalive)));

        let client = hyper::Client::builder()
            .pool_idle_timeout(Duration::from_secs(cfg.pool_idle_timeout))
            .build(connector);

        clickhouse::Client::with_http_client(client)
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

const TABLE_DDL: &str = "
      CREATE TABLE IF NOT EXISTS ? (
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
      ORDER BY (resource_id, response_status, remote_addr, timestamp)";

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

enum ImportResult<E> {
    /// Insert is in progress, buffering the logs
    Pending,
    /// Inserts have been written and committed, returning last offsets as [`TopicPartitionList`]
    Success(TopicPartitionList),
    /// Insert has failed with error `E`
    Failure(E),
}

struct ClickHouseSink {
    /// Single Kafka topic that's being consumed
    topic: String,
    /// Offsets of the last written logs for each partition and [topic]
    tpl: TopicPartitionList,
    /// ClickHouse [`Inserter`](clickhouse::inserter::Inserter) for [`HttpLog`]
    inserter: Inserter<HttpLog>,
}

impl ClickHouseSink {
    #[inline]
    fn new(topic: String, inserter: Inserter<HttpLog>) -> Self {
        Self {
            topic,
            tpl: TopicPartitionList::new(),
            inserter,
        }
    }

    // TODO: proper error type
    // FIXME: error=error=Network(hyper::Error(IncompleteMessage))
    #[instrument(skip(self, log))]
    async fn write(&mut self, partition: i32, offset: i64, log: HttpLog) -> ImportResult<()> {
        self.inserter
            .write(&log)
            .await
            .expect("inserter write failed");

        debug!(partition, offset, "written log data");

        {
            let mut p = match self.tpl.find_partition(&self.topic, partition) {
                Some(p) => p,
                None => self.tpl.add_partition(&self.topic, partition),
            };

            // XXX: offset + 1 (?)
            p.set_offset(Offset::Offset(offset))
                .expect("Kafka offset update");
        }

        // XXX: Is this pre-commit check even necessary?
        let time_left = self.inserter.time_left();
        let should_commit = time_left.map(|t| t.is_zero()).unwrap_or(false);

        if !should_commit {
            debug!(?time_left, partition, offset, "waiting to commit insert");
            return ImportResult::Pending;
        }

        // XXX: handle retries? although, quite infeasible due to strict rate-limit
        match self.inserter.commit().await {
            // TODO: should commit only if q > 0
            Ok(q) => {
                info!(partition, quantities = ?q, "inserts committed");
                // NOTE: commit is done only once in a while so the clone should be fine
                ImportResult::Success(self.tpl.clone())
            }
            Err(e) => {
                error!(partition, error = ?e, "insert commit failed: {:?}", e);
                //error!(partition, error = ?e, "insert commit failed");
                // panic!();
                ImportResult::Failure(())
            }
        }
    }
}

#[derive(Clone)]
#[allow(unused)]
struct Context {
    kafka: KafkaConfig,
    ch: Arc<clickhouse::Client>,
    sink: Arc<Mutex<ClickHouseSink>>,
}

impl Context {
    fn new(kafka: KafkaConfig, ch: clickhouse::Client, inserter: LogInserter) -> Self {
        let topic = kafka.topic.clone();
        Self {
            kafka,
            ch: Arc::new(ch),
            sink: Arc::new(Mutex::new(ClickHouseSink::new(topic, inserter))),
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
        .set("enable.auto.commit", "false")
        //.set("enable.auto.commit", "true")
        //.set("auto.commit.interval.ms", "2000")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create_with_context(HttpLogConsumerContext)?;

    consumer.subscribe(&[&cfg.topic])?;
    info!(topic = cfg.topic, "subscribed new Kafka consumer");

    Ok(consumer)
}

// TODO: struct KafkaSource, struct HttpLogConsumer

// XXX: delivery semantics => offset handling & idempotent write
#[instrument(name = "consumer", skip(ctx))]
async fn run_consumer(id: usize, ctx: Context) {
    let span = &Span::current();

    let consumer = create_consumer(ctx.kafka).expect("consumer creation failed");
    let consumer = Arc::new(consumer);

    let stream_processor = consumer.stream().try_for_each(|msg| {
        let consumer = consumer.clone();
        let sink = ctx.sink.clone();

        async move {
            // process a message
            let msg = msg.detach();

            let topic = msg.topic().to_owned();
            let partition = msg.partition();
            let offset = msg.offset();

            debug!(parent: span, topic, partition, offset, "processing message");

            // TODO: filter out invalid msgs (or rather report them to stats)
            let mut log = HttpLog::try_from(msg).expect("valid message");
            debug!(partition, offset, "message parsed");

            log.remote_addr = anonymize_ip(log.remote_addr);
            debug!(partition, offset, ?log, "anonymized");

            // XXX: consider keeping just single consumer and scale the group horizontally
            let sink_span = debug_span!("sink");
            let result = {
                // critical write section
                let _enter = sink_span.enter();
                let mut sink = sink.lock().await;
                sink.write(partition, offset, log).await
            };

            match result {
                ImportResult::Pending => {}
                ImportResult::Success(tpl) => {
                    // since offsets are sequential and commit retrospective, it should be
                    // sufficient to commit just once after whole batch

                    // XXX: consider `CommitMode::Sync` (cons: blocks execution)
                    // NOTE: since rate-limiting is 1 min, this might be more efficient than frequent
                    // auto-commit of _stored_ offsets
                    consumer
                        .commit(&tpl, CommitMode::Async)
                        // XXX: here we actually should terminate the process
                        .expect("Kafka offsets committed");
                }
                ImportResult::Failure(()) => {
                    //error!(partition, error = ?e, "insert commit failed");
                    //panic!();
                }
            }

            debug!(topic, partition, offset, "done processing message");
            Ok(())
        }
    });

    info!("starting Kafka message stream processing");
    stream_processor.await.expect("stream processing failed");
    warn!("stream processing terminated");

    // TODO: graceful shutdown: terminate inserting
    //  - `inserter.end().await.expect("inserter shutdown flush failed");`
    //  - also commit these offsets
}

// XXX: part of sink?
#[instrument(skip(ch))]
async fn make_table(ch: &clickhouse::Client, table: &str) {
    ch.query(TABLE_DDL)
        .bind(clickhouse::sql::Identifier(table))
        .execute()
        .await
        .expect("making clickhouse table failed");

    info!(table, "made sure log table exists in clickhouse");
}

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

    info!(tasks = cfg.workers, "starting anonymizer pipeline");

    let ch = clickhouse::Client::from(&cfg.ch);
    info!(
        url = cfg.ch.url,
        user = cfg.ch.user,
        "clickhouse client created"
    );

    make_table(&ch, &cfg.ch.log_table).await;

    let inserter = ch
        .inserter(&cfg.ch.log_table) // NOTE: inserter name = table name
        .expect("failed to create new clickhouse inserter")
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
