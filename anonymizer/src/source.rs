//! This module implements the Kafka source which drives the anonymizer pipeline logic
//!
//! TODO
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use derive_new::new;
use futures::TryStreamExt;
use prometheus_metric_storage::StorageRegistry;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::Message;
use retry::delay::Fixed;
use retry::retry;
use stream_cancel::Valve;
use tokio::sync::mpsc::error::SendTimeoutError;
use tokio::sync::{mpsc, oneshot};
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{debug, error, info, instrument, warn, Span};

use crate::anonymize_ip;
use crate::config::KafkaConfig;
use crate::error::Error;
use crate::http_log::HttpLog;
use crate::kafka::{LoggingConsumer, LoggingConsumerContext};
use crate::sink::{Insert, InsertResult};
use crate::telemetry::Metrics;

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
        .create_with_context(LoggingConsumerContext)?;

    consumer.subscribe(&[&cfg.topic])?;
    info!(topic = cfg.topic, "subscribed new Kafka consumer");

    Ok(consumer)
}

// XXX: generalize to `ClickHouseSink<O> where I: TryFrom<OwnedMessge> + Transform<I, O>, O: ...`
/// Run the anonymization pipeline.
///
/// This function:
///  1. Constructs new Kafka consumer and subscribes to a source topic given [`KafkaConfig`]
///  1. Starts a Kafka message stream processing with the pipeline logic handling each message
///  1. Commits offsets via the consumer for successfully processed messages
#[instrument(name = "pipeline", skip_all)]
pub async fn run_consumer(
    kafka: KafkaConfig,
    sink: mpsc::Sender<Insert<HttpLog, Error>>,
    mpsc_send_timeout: Duration,
    metrics: Metrics,
    valve: Valve,
) -> Result<()> {
    let span = &Span::current();

    let retries = kafka.retries;
    let retry_delay = kafka.retry_delay;

    let consumer = create_consumer(kafka)?;
    let consumer = Arc::new(consumer);

    let msg_stream = valve.wrap(consumer.stream());

    let stream_processor = msg_stream.try_for_each(|msg| {
        let consumer = consumer.clone();
        let sink = sink.clone();
        let metrics = metrics.clone();

        async move {
            let timer = metrics.message_latency_seconds.start_timer();

            metrics
                .messages_total
                .with_label_values(&["received"])
                .inc();

            metrics.message_payload_bytes.set(msg.payload_len() as f64);

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
                    warn!(cause = ?e, "ignoring invalid message");
                    metrics
                        .messages_total
                        .with_label_values(&["rejected"])
                        .inc();
                    timer.observe_duration();
                    return Ok(());
                }
            };

            // TODO: enum AnonymizeResult { Changed(String), Unchanged(String) }
            //  - or just std::result::Result<String, String>
            log.remote_addr = anonymize_ip(log.remote_addr);
            debug!(partition, offset, ?log, "anonymized");

            let (reply_tx, reply_rx) = oneshot::channel();

            let insert = Insert::new(partition, offset, log, reply_tx);

            // XXX: retry on timeout?
            if let Err(e) = sink.send_timeout(insert, mpsc_send_timeout).await {
                let cause = match e {
                    SendTimeoutError::Closed(_) => "sink stopped receiving",
                    SendTimeoutError::Timeout(_) => "timeout",
                };

                error!(partition, cause, "request for data insert failed");

                timer.observe_duration();
                return KafkaResult::Err(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::Application,
                ));
            };

            let result = match reply_rx.await {
                Ok(result) => result,
                Err(e) => {
                    error!(partition, error = ?e, "failed to receive insert response");
                    timer.observe_duration();
                    return KafkaResult::Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::Application,
                    ));
                }
            };

            let result = match result {
                InsertResult::Pending => {
                    debug!(topic, partition, offset, "done processing message");
                    Ok(())
                }

                InsertResult::Success(tpl) => {
                    debug_assert!(
                        tpl.count() > 0,
                        "successful import must have non-empty offsets"
                    );

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

                InsertResult::Failure(e) => {
                    error!(partition, error = ?e, "insert commit failed");
                    KafkaResult::Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::Application,
                    ))
                }
            };

            metrics
                .messages_total
                .with_label_values(&["processed"])
                .inc();
            timer.observe_duration();

            result
        }
    });

    info!("starting Kafka message stream processing");

    stream_processor
        .await
        .map_err(|e| anyhow!(Error::Source(e)))?;

    warn!("stream processing terminated");

    Ok(())
}

#[derive(new)]
struct KafkaConsumer {
    id: usize,
    kafka: KafkaConfig,
    sink: mpsc::Sender<Insert<HttpLog, Error>>,
    mpsc_send_timeout: Duration,
    metrics: Metrics,
    valve: Valve,
}

// TODO: merge with `run_consumer`
impl KafkaConsumer {
    #[inline]
    fn subsys_name(&self) -> String {
        format!("KafkaConsumer-{}", self.id)
    }

    #[instrument(name = "consumer", fields(id = self.id, topic = self.kafka.topic), skip_all)]
    async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        let process_stream = run_consumer(
            self.kafka,
            self.sink,
            self.mpsc_send_timeout,
            self.metrics,
            self.valve,
        );

        if let Err(e) = process_stream.await {
            error!(cause = ?e, "stream processing failed, initiating shutdown");
            subsys.request_global_shutdown();
        }

        Ok(())
    }
}

// TODO: generalize over `HttpLog`
/// Top-level pipeline subsystem compositor and driver
#[derive(new)]
pub struct KafkaSource {
    num_consumers: usize,
    kafka: KafkaConfig,
    sink: mpsc::Sender<Insert<HttpLog, Error>>,
    mpsc_send_timeout: Duration,
    registry: StorageRegistry,
}

impl KafkaSource {
    #[instrument(name = "source", fields(topic = self.kafka.topic), skip_all)]
    pub async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        info!("starting stream processing");

        // NOTE: when `exit` is dropped, all consumer Kafka streams will be interruped
        let (exit, valve) = Valve::new();

        // XXX: additional consumer-specific metrics (`labels("consumer")`)
        let metrics = Metrics::get_or_create(&self.registry)?;

        let consumers = (0..self.num_consumers).map(|id| {
            KafkaConsumer::new(
                id,
                self.kafka.clone(),
                self.sink.clone(),
                self.mpsc_send_timeout,
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
