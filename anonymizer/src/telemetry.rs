//! Module containing components and utilities for telemetry (metrics, tracing, logging).
//!
//! TODO
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use derive_new::new;
use prometheus::{Gauge, Histogram, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry};
use prometheus_hyper::Server;
use prometheus_metric_storage::{MetricStorage, StorageRegistry};
use tokio::task::JoinHandle;
use tokio_graceful_shutdown::SubsystemHandle;
use tracing::{info, instrument, warn};
use tracing_loki::BackgroundTask;

use crate::error::Error;

// XXX: split out `OutputMetrics` with `subsystem = "output"`
/// Prometheus metrics for pipeline monitoring
#[derive(Debug, Clone, MetricStorage)]
#[metric(subsystem = "pipeline")]
pub struct Metrics {
    /// Total count of consumed Kafka messages by status
    #[metric(labels("status"))]
    pub messages_total: IntCounterVec,

    /// Message processing latency in seconds
    pub message_latency_seconds: Histogram,

    /// Message payload size in bytes
    pub message_payload_bytes: Gauge,

    /// Time in seconds of issuing single insert to ClickHouse
    pub output_duration_seconds: Histogram,

    /// Number of insert blocks in the output queue
    pub output_queued_blocks: IntGauge,

    /// The percentage of the capacity of the next block to insert into ClickHouse which has been
    /// used by insert entries
    pub output_insert_block_percent: Gauge,

    /// Number of entries to output in the next insert(s) to ClickHouse
    ///
    /// Labels:
    ///  - `type="buffered"` is the current total number of buffered entries in the output queue
    ///  - `type="insert"` is the number of entries to insert in current output iteration
    #[metric(labels("type"))]
    pub output_entries: IntGaugeVec,

    /// Total count of entries inserted to ClickHouse
    pub output_inserted_total: IntCounter,
}

impl Metrics {
    /// Publicly exposes `Metrics::instance(registry)` with custom [`Error::Metrics`]
    #[inline]
    pub fn get_or_create(registry: &StorageRegistry) -> Result<&Self> {
        Self::instance(registry).map_err(|e| anyhow!(Error::Metrics(e)))
    }
}

/// Server exporting Prometheus metrics
#[derive(new)]
pub struct MetricsExporter {
    server_port: u16,
    registry: Arc<Registry>,
}

// XXX: or `impl IntoSubsystem` but with `async_trait`
impl MetricsExporter {
    #[instrument(name = "exporter", fields(port = self.server_port), skip_all)]
    pub async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        info!("starting metrics server");
        Server::run(
            self.registry,
            SocketAddr::from(([0; 4], self.server_port)),
            async {
                subsys.on_shutdown_requested().await;
                warn!("shutting down metrics server");
            },
        )
        .await
        .map_err(|e| anyhow!(Error::MetricsExporter(e)))
    }
}

/// Publisher of tracing logs to Grafana Loki.
///
/// Wraps a `tokio` task [handle](tokio::task::JoinHandle) of a spawned
/// [`BackgroundTask`](tracing_loki::BackgroundTask).
pub struct TracingExporter {
    handle: JoinHandle<()>,
}

impl TracingExporter {
    /// Spawns given `task` and stores its handle to abort it during graceful shutdown
    pub fn spawn(task: BackgroundTask) -> Self {
        Self {
            handle: tokio::spawn(task),
        }
    }

    #[instrument(name = "loki", skip_all)]
    pub async fn run(self, subsys: SubsystemHandle) -> Result<()> {
        subsys.on_shutdown_requested().await;

        warn!("shutting down log publisher");
        self.handle.abort();

        Ok(())
    }
}
