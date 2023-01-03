//! This module implements the Kafka source which drives the anonymizer pipeline logic
//!
//! TODO
use std::fmt::Debug;
use std::time::Duration;

use anyhow::bail;
use anyhow::{anyhow, Result};
use derive_new::new;
use futures::TryStreamExt;
use prometheus_metric_storage::StorageRegistry;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::TopicPartitionList;
use retry::delay::Fixed;
use retry::retry;
use stream_cancel::Valve;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::config::KafkaConfig;
use crate::error::Error;
use crate::kafka::{LoggingConsumer, LoggingConsumerContext};
use crate::sink::{Insert, InsertResult, SinkConnector, SinkRow};
use crate::telemetry::Metrics;
use crate::Anonymize;

/// Create new Kafka consumer and subscribe it to a topic specified in config.
///
/// For configuration options see:
/// [`librdkafka` docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
#[instrument(name = "create", skip_all)]
fn create_consumer(cfg: &KafkaConfig) -> Result<LoggingConsumer> {
    let consumer: LoggingConsumer = rdkafka::ClientConfig::new()
        .set("group.id", cfg.group_id.clone())
        .set("bootstrap.servers", cfg.brokers.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .create_with_context(LoggingConsumerContext)?;

    consumer.subscribe(&[&cfg.topic])?;
    info!(topic = cfg.topic, "subscribed new Kafka consumer");

    Ok(consumer)
}

#[derive(new)]
struct Pipeline<'stream, T> {
    kafka: KafkaConfig,
    consumer: &'stream LoggingConsumer,
    sink: SinkConnector<T, Error>,
    metrics: Metrics,
}

impl<'stream, T> Pipeline<'stream, T> {
    #[instrument(name = "commit", skip(self))]
    fn commit_offsets(&self, tpl: TopicPartitionList) -> Result<()> {
        retry(
            Fixed::from_millis(self.kafka.retry_delay).take(self.kafka.retries),
            || self.consumer.commit(&tpl, CommitMode::Async),
        )
        .map_err(|e| {
            error!(
                cause = ?e,
                tries = e.tries,
                elapsed = ?e.total_delay,
                "failed to commit offsets"
            );
            anyhow!(Error::Source(e.error))
        })
    }
}

impl<'stream, T> Pipeline<'stream, T>
where
    T: for<'a> TryFrom<BorrowedMessage<'a>> + Anonymize + SinkRow + Debug,
    for<'a> <T as TryFrom<BorrowedMessage<'a>>>::Error: Debug,
{
    #[instrument(name = "process", skip_all)]
    async fn process<'msg>(&'stream self, msg: BorrowedMessage<'msg>) -> Result<()>
    where
        'stream: 'msg,
    {
        let timer = self.metrics.message_latency_seconds.start_timer();

        self.metrics
            .messages_total
            .with_label_values(&["received"])
            .inc();

        self.metrics
            .message_payload_bytes
            .set(msg.payload_len() as f64);

        // process a message

        // NOTE: The config and message topic must be the same by construction/configuration.
        let partition = msg.partition();
        let offset = msg.offset();

        debug!(partition, offset, "processing message");

        let data = match T::try_from(msg) {
            Ok(data) => {
                debug!(partition, offset, "message parsed");
                data
            }
            Err(e) => {
                warn!(cause = ?e, "ignoring invalid message");
                self.metrics
                    .messages_total
                    .with_label_values(&["rejected"])
                    .inc();
                timer.observe_duration();
                return Ok(());
            }
        };

        let data = data.anonymize();
        debug!(partition, offset, ?data, "anonymized");

        let result = match self.sink.output(partition, offset, data).await {
            Ok(result) => result,
            Err(e) => {
                timer.observe_duration();
                bail!(e);
            }
        };

        let result = match result {
            InsertResult::Pending => {
                debug!(partition, offset, "done processing message");
                Ok(())
            }

            InsertResult::Success(tpl) => {
                debug_assert!(
                    tpl.count() > 0,
                    "successful import must have non-empty offsets"
                );

                // since offsets are sequential and commit retrospective, it is sufficient to
                // commit just once after whole batch

                self.commit_offsets(tpl)
            }

            InsertResult::Failure(e) => {
                error!(partition, error = ?e, "insert request failed");
                Err(anyhow!(e))
            }
        };

        self.metrics
            .messages_total
            .with_label_values(&["processed"])
            .inc();

        timer.observe_duration();

        result
    }
}

#[derive(new)]
struct KafkaConsumer<T> {
    id: usize,
    kafka: KafkaConfig,
    sink: SinkConnector<T, Error>,
    metrics: Metrics,
    valve: Valve,
}

impl<T> KafkaConsumer<T> {
    #[inline]
    fn subsys_name(&self) -> String {
        format!("KafkaConsumer-{}", self.id)
    }
}

impl<T> KafkaConsumer<T>
where
    T: for<'a> TryFrom<BorrowedMessage<'a>> + Anonymize + SinkRow + Debug,
    for<'a> <T as TryFrom<BorrowedMessage<'a>>>::Error: Debug,
{
    /// Run the anonymization pipeline.
    ///
    /// This function:
    ///  1. Constructs new Kafka consumer and subscribes to a source topic given [`KafkaConfig`]
    ///  1. Starts a Kafka message stream processing with the pipeline logic handling each message
    ///  1. Commits offsets via the consumer for successfully processed messages
    #[instrument(name = "consumer", fields(id = self.id, topic = self.kafka.topic), skip_all)]
    async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        let consumer = create_consumer(&self.kafka)?;

        let pipeline = Pipeline::new(self.kafka, &consumer, self.sink, self.metrics);

        let msg_stream = self.valve.wrap(consumer.stream());

        info!("starting Kafka message stream processing");

        let stream_processing = msg_stream
            .map_err(|e| anyhow!(Error::Source(e)))
            .try_for_each(|msg| pipeline.process(msg));

        if let Err(e) = stream_processing.await {
            error!(cause = ?e, "stream processing failed, initiating shutdown");
            subsys.request_global_shutdown();
        } else {
            warn!("stream processing terminated");
        }

        Ok(())
    }
}

/// Top-level pipeline subsystem compositor and driver
#[derive(new)]
pub struct KafkaSource<T> {
    num_consumers: usize,
    kafka: KafkaConfig,
    sink: mpsc::Sender<Insert<T, Error>>,
    mpsc_send_timeout: Duration,
    registry: StorageRegistry,
}

impl<T> KafkaSource<T>
where
    T: for<'a> TryFrom<BorrowedMessage<'a>> + Anonymize + SinkRow + Debug + Send + 'static,
    for<'a> <T as TryFrom<BorrowedMessage<'a>>>::Error: Debug,
{
    #[instrument(name = "source", fields(topic = self.kafka.topic), skip_all)]
    pub async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        info!("starting stream processing");

        // NOTE: when `exit` is dropped, all consumer Kafka streams will be interruped
        let (exit, valve) = Valve::new();

        let connector = SinkConnector::new(self.sink, self.mpsc_send_timeout);

        // XXX: additional consumer-specific metrics (`labels("consumer")`)
        let metrics = Metrics::get_or_create(&self.registry)?;

        let consumers = (0..self.num_consumers).map(|id| {
            KafkaConsumer::new(
                id,
                self.kafka.clone(),
                connector.clone(),
                metrics.clone(),
                valve.clone(),
            )
        });

        // run consumer subsystems
        for consumer in consumers {
            subsys.start(consumer.subsys_name().as_str(), |subsys| {
                consumer.run(subsys)
            });
        }

        // wait to gracefully shut down
        subsys.on_shutdown_requested().await;

        warn!("shutting down stream processing");
        drop(exit);

        Ok(())
    }
}
