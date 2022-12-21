//! This module implements the Kafka source which drives the anonymizer pipeline logic
//!
//! TODO
use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures::TryStreamExt;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::Message;
use retry::delay::Fixed;
use retry::retry;
use tokio::sync::Mutex;
use tracing::{debug, debug_span, error, info, instrument, warn, Span};

use crate::anonymize_ip;
use crate::config::KafkaConfig;
use crate::error::Error;
use crate::http_log::HttpLog;
use crate::kafka::{LoggingConsumer, LoggingConsumerContext};
use crate::sink::{ClickHouseSink, ImportResult};

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

/// Run the anonymization pipeline.
///
/// This function:
///  1. Constructs new Kafka consumer and subscribes to a source topic given [`KafkaConfig`]
///  1. Starts a Kafka message stream processing with the pipeline logic handling each message
///  1. Commits offsets via the consumer for successfully processed messages
#[instrument(name = "consumer", skip(kafka, sink))]
pub async fn run_consumer(
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

    Ok(())
}
