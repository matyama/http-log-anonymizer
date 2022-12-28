use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
#[cfg(target_os = "linux")]
use prometheus::process_collector::ProcessCollector;
use prometheus::Registry;
use prometheus_metric_storage::StorageRegistry;
use tokio::sync::Mutex;
use tokio_graceful_shutdown::Toplevel;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use anonymizer::{
    config::Config, error::Error, http_log::HttpLog, sink::ClickHouseSink, source::KafkaSource,
    telemetry::MetricsExporter,
};

#[tokio::main]
async fn main() -> Result<()> {
    // read configuration from env
    let cfg = Config::from_env()?;

    // setup tracing
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(cfg.rust_log))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // setup metrics
    let registry = Registry::new_custom(Some("anonymizer".to_owned()), None)?;
    let storage_registry = StorageRegistry::new(registry.clone());
    let registry = Arc::new(registry);

    #[cfg(target_os = "linux")]
    storage_registry.register(Box::new(ProcessCollector::for_self()))?;

    info!(
        consumers = cfg.num_consumers,
        "starting anonymizer pipeline"
    );

    let topic = cfg.kafka.topic.clone();

    // crete a shared clickhouse sink
    let sink = ClickHouseSink::<HttpLog>::new(topic, cfg.ch, &storage_registry).await?;
    let sink = Arc::new(Mutex::new(sink));

    // compose subsystems
    let exporter = MetricsExporter::new(cfg.telemetry.prometheus_exporter_port, registry);
    let source = KafkaSource::new(cfg.num_consumers, cfg.kafka, sink, storage_registry);

    // XXX: ClickHouseSink subsystem with mpsc channel to `KafkaConsumer`s
    Toplevel::new()
        .start("MetricsExporter", |subsys| exporter.run(subsys))
        .start("KafkaSource", |subsys| source.run(subsys))
        .catch_signals()
        .handle_shutdown_requests(Duration::from_secs(cfg.shutdown_timeout))
        .await
        .map_err(|e| anyhow!(Error::ShutdownError(e)))
}
