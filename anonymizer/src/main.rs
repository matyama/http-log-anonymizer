use std::sync::Arc;

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use anonymizer::{config::Config, http_log::HttpLog, sink::ClickHouseSink, source::run_consumer};

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
