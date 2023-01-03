//! Module that defines a single application-level [`Error`] type and a _retry_ helper function
//! which is usable in `async` contexts.
use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use clickhouse_http_client::error::Error as ClickHouseError;
use hyper::Error as HyperError;
use prometheus::Error as PrometheusError;
use rdkafka::error::KafkaError;
use tokio::sync::oneshot::error::RecvError;
use tokio_graceful_shutdown::errors::GracefulShutdownError;

/// Generic dynamic error with type erased
pub type BoxedError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Enum encapsulating all the error variants of this crate's components
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Application configuration errors
    #[error("Invalid or missing configuration")]
    Config(#[from] config::ConfigError),

    /// Kafka integration and consumer errors
    #[error("KafkaError {0:?}")]
    Source(#[from] KafkaError),

    /// Configuration errors of [`ClickHouseSink`](crate::sink::ClickHouseSink)
    #[error("Invalid ClickHouse configuration '{0}'")]
    SinkConfig(&'static str),

    /// Pipeline output errors which originated in [`ClickHouseSink`](crate::sink::ClickHouseSink)
    #[error("ClickHouseError {0:?}")]
    Sink(#[from] ClickHouseError),

    /// Insert request errors
    ///
    /// The error can be caused either by
    ///  - sink stopped receiving new insert requests
    ///  - communication (send) timed out due to backpressure
    #[error("Request to the sink for ({partition}, {offset}) data insert failed: '{cause}'")]
    SinkRequest {
        cause: &'static str,
        partition: i32,
        offset: i64,
    },

    /// Output communication error
    ///
    /// The error indicates that the [`SinkConnector`](crate::sink::SinkConnector) failed to
    /// receive an [`InsertResult`](crate::sink::InsertResult).
    ///
    /// It is not guaranteed whether the output (insert) succeeded and sink terminated afterwards,
    /// or not. Therefore, it is not advised to assume the data were actually stored.
    #[error("Sink not responding to an insert request: {0:?}")]
    SinkResponse(#[from] RecvError),

    /// Errors related to registering, access and manipulation with Prometheus metrics
    #[error("MetricsError {0:?}")]
    Metrics(#[from] PrometheusError),

    /// Metrics server errors
    #[error("Metrics exporter error {0:?}")]
    MetricsExporter(#[from] HyperError),

    /// Application shutdown errors
    #[error("Graceful shutdown failed with {0:?}")]
    ShutdownError(#[from] GracefulShutdownError<BoxedError>),

    /// Pipeline output throttling signal
    #[error("Request rate limit exceeded")]
    RateLimit,
}

/// Asynchronously runs given future-producing closure `f` `tries`-times with a fixed `delay`.
pub async fn async_retry<T, F, Fut>(mut tries: u64, delay: Duration, f: F) -> Result<T>
where
    F: Fn() -> Fut + Send,
    Fut: Future<Output = Result<T>> + Send,
{
    loop {
        match f().await {
            e @ Err(_) if tries == 0 => return e,
            Err(_) => tokio::time::sleep(delay).await,
            r => return r,
        }
        tries -= 1;
    }
}
