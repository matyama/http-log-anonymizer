use std::marker::PhantomData;
use std::time::Duration;

use anyhow::{bail, Result};
use clickhouse_http_client::clickhouse_format::input::JsonCompactEachRowInput;
use clickhouse_http_client::error::{ClientExecuteError, Error as ClickHouseError};
use clickhouse_http_client::isahc::http::StatusCode;
use clickhouse_http_client::isahc::prelude::Configurable;
use clickhouse_http_client::{Client, ClientBuilder};
use serde_json::Value;
use tracing::{debug, info, instrument, warn};

use crate::config::ClickHouseConfig;
use crate::error::{async_retry, Error};
use crate::kafka::{OffsetTracker, TopicPartitionList};
use crate::limiter::RequestLimiter;

impl TryFrom<&ClickHouseConfig> for Client {
    type Error = Error;

    fn try_from(cfg: &ClickHouseConfig) -> Result<Self, Self::Error> {
        let mut builder = ClientBuilder::new().configurable(|http_client_builder| {
            http_client_builder
                // TODO: add to env config
                .timeout(Duration::from_secs(5))
                // XXX: these can potentially be on defaults if rate limiter is disabled
                .max_connections(1)
                .max_connections_per_host(1)
                .tcp_keepalive(Duration::from_secs(cfg.tcp_keepalive))
        });

        builder
            .set_url(&cfg.url)
            .map_err(|_| Error::SinkConfig("url"))?;

        let mut ch = builder.build()?;
        ch.set_username_to_header(&cfg.user)
            .map_err(|_| Error::SinkConfig("user"))?;
        ch.set_password_to_header(&cfg.password)
            .map_err(|_| Error::SinkConfig("password"))?;
        ch.set_database_to_header(&cfg.database)
            .map_err(|_| Error::SinkConfig("database"))?;

        Ok(ch)
    }
}

pub type CompactJsonRow = Vec<Value>;

pub trait SinkRow: Into<CompactJsonRow> {
    fn table_ddl(table: &str) -> String;

    fn insert_ddl(table: &str) -> String;
}

pub enum ImportResult<E> {
    /// Insert is in progress, buffering the logs
    Pending,
    /// Inserts have been written and committed, returning last offsets as [`TopicPartitionList`]
    Success(TopicPartitionList),
    /// Insert has failed with error `E`
    Failure(E),
}

pub struct ClickHouseSink<T> {
    /// ClickHouse [`Client`](clickhouse_http_client::Client) for [`HttpLog`]
    ch: clickhouse_http_client::Client,

    /// Buffered data for the next output (ClickHouse insert)
    ///
    /// Original [`HttpLog`] data are stored as [`serde_json::Value`](serde_json::Value)s and sent
    /// to ClickHouse as an insert with values in the
    /// [JsonCompactEachRow](https://clickhouse.com/docs/en/interfaces/formats/#jsoncompacteachrow)
    /// format.
    buffer: Vec<CompactJsonRow>,

    /// Tracker of Kafka offsets corresponding to the last written [`HttpLog`].
    offset_tracker: OffsetTracker,

    /// Limiter that keeps track of the maximum allowed request rate for ClickHouse queries.
    request_limiter: RequestLimiter,

    /// Raw insert query header for the target table.
    insert_ddl: String,

    /// The witness type representing the table schema
    _t: PhantomData<T>,
}

impl<T: SinkRow> ClickHouseSink<T> {
    #[instrument(name = "sink", skip(cfg))]
    pub async fn new(topic: String, cfg: ClickHouseConfig) -> Result<Self> {
        let ch = clickhouse_http_client::Client::try_from(&cfg)?;
        info!(url = cfg.url, user = cfg.user, "clickhouse client created");

        if cfg.create_table {
            // make sure that the target table exists
            let delay = Duration::from_secs(cfg.rate_limit.unwrap_or_default());
            async_retry(cfg.retries, delay, || async {
                make_table::<T>(&ch, &cfg.target_table).await
            })
            .await?;
        }

        // XXX: does insert_period have to be an option? => use None to _disable_ rate limit
        let request_limiter = RequestLimiter::new(cfg.rate_limit.unwrap_or(10));

        Ok(Self {
            ch,
            buffer: Vec::new(),
            offset_tracker: OffsetTracker::new(topic),
            request_limiter,
            insert_ddl: T::insert_ddl(&cfg.target_table),
            _t: PhantomData,
        })
    }

    #[instrument(skip(self, data))]
    pub async fn write(
        &mut self,
        partition: i32,
        offset: i64,
        data: T,
    ) -> ImportResult<ClickHouseError> {
        self.buffer.push(data.into());
        debug!("data buffered for insert");

        self.offset_tracker.store(partition, offset);

        let time_left = self.request_limiter.remaining_time();

        if !time_left.is_zero() {
            debug!(?time_left, "waiting to commit insert");
            return ImportResult::Pending;
        }

        let entries = self.buffer.len();
        debug_assert!(entries > 0, "data loss during write");

        // TODO: metrics (inserted, failed)

        // XXX: std::mem::replace(&mut self.buffer, Vec::with_capacity(self.buffer.len()))
        let rows = std::mem::take(&mut self.buffer);
        let input = JsonCompactEachRowInput::new(rows);

        info!(rows = entries, "inserting batch of data");

        // TODO: handle 413 Payload Too Large
        // XXX: retry - the difficulty is that the buffer would have to be copied each time
        let result = self
            .ch
            .insert_with_format(&self.insert_ddl, input, None)
            .await;

        self.request_limiter.record_request();

        match result {
            Ok(()) => {
                info!(rows = entries, "inserts committed");
                // NOTE: commit is done only once in a while so the clone should be fine
                ImportResult::Success(self.offset_tracker.load())
            }
            Err(e) => {
                warn!(error = ?e, "insert commit failed");
                ImportResult::Failure(e)
            }
        }
    }
}

// XXX: move to SinkRow at the cost of requiring `async_trait`
#[instrument(skip(ch))]
async fn make_table<T: SinkRow>(ch: &clickhouse_http_client::Client, table: &str) -> Result<()> {
    let query = T::table_ddl(table);

    if let Err(e) = ch.execute(query, None).await {
        if let ClickHouseError::ClientExecuteError(ClientExecuteError::StatusCodeMismatch(
            StatusCode::SERVICE_UNAVAILABLE,
        )) = e
        {
            warn!("cannot execute DDL query in clickhouse at the moment due to rate limit");
            bail!(Error::RateLimit);
        } else {
            bail!(Error::Sink(e));
        };
    };

    info!(table, "made sure target table exists in clickhouse");
    Ok(())
}
