use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use clickhouse_http_client::error::Error as ClickHouseError;
use rdkafka::error::KafkaError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid or missing configuration")]
    Config(#[from] config::ConfigError),

    #[error("KafkaError {0:?}")]
    Source(#[from] KafkaError),

    #[error("Invalid ClickHouse configuration '{0}'")]
    SinkConfig(&'static str),

    #[error("ClickHouseError {0:?}")]
    Sink(#[from] ClickHouseError),

    #[error("Request rate limit exceeded")]
    RateLimit,
}

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
