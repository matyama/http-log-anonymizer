use std::sync::Arc;

use anyhow::{anyhow, Result};
use maplit::hashmap;
#[cfg(target_os = "linux")]
use prometheus::process_collector::ProcessCollector;
use prometheus::Registry;
use prometheus_metric_storage::StorageRegistry;
use tokio::sync::mpsc;
use tokio_graceful_shutdown::Toplevel;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use anonymizer::{
    config::Config,
    error::Error,
    http_log::HttpLog,
    sink::{ClickHouseSink, Insert},
    source::KafkaSource,
    telemetry::{MetricsExporter, TracingExporter},
};

const APP_NAME: &str = "anonymizer";

#[tokio::main]
async fn main() -> Result<()> {
    // read configuration from env
    let cfg = Config::from_env()?;

    // setup tracing
    let (loki_layer, loki_task) = tracing_loki::layer(
        cfg.telemetry.loki_url,
        hashmap! {
            "app".to_owned() => APP_NAME.to_owned(),
            "replica".to_owned() => "0".to_owned(),
        },
        hashmap! {},
    )?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(cfg.rust_log))
        .with(tracing_subscriber::fmt::layer())
        .with(loki_layer)
        .init();

    let tracing = TracingExporter::spawn(loki_task);

    // setup metrics
    let registry = Registry::new_custom(Some(APP_NAME.to_owned()), None)?;
    let storage_registry = StorageRegistry::new(registry.clone());
    let registry = Arc::new(registry);

    #[cfg(target_os = "linux")]
    storage_registry.register(Box::new(ProcessCollector::for_self()))?;

    let topic = cfg.kafka.topic.clone();

    // crete an insert request channel from Kafka source to clickhouse sink with
    let (tx, rx) = mpsc::channel::<Insert<HttpLog, Error>>(cfg.mpsc_buffer_size);

    info!(
        topic = topic,
        consumers = cfg.num_consumers,
        mpsc_buffer = cfg.mpsc_buffer_size,
        mpsc_timout = ?cfg.mpsc_send_timeout,
        "starting anonymizer pipeline"
    );

    // compose subsystems
    let exporter = MetricsExporter::new(cfg.telemetry.prometheus_exporter_port, registry);
    let sink = ClickHouseSink::new(topic, cfg.ch, rx, &storage_registry).await?;
    let source = KafkaSource::new(
        cfg.num_consumers,
        cfg.kafka,
        tx,
        cfg.mpsc_send_timeout,
        storage_registry,
    );

    Toplevel::new()
        .start("TracingExporter", |subsys| tracing.run(subsys))
        .start("MetricsExporter", |subsys| exporter.run(subsys))
        .start("ClickHouseSink", |subsys| sink.run(subsys))
        .start("KafkaSource", |subsys| source.run(subsys))
        .catch_signals()
        .handle_shutdown_requests(cfg.shutdown_timeout)
        .await
        .map_err(|e| anyhow!(Error::ShutdownError(e)))
}
