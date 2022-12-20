use std::collections::VecDeque;
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

/// List of JSON [Value]s.
pub type CompactJsonRow = Vec<Value>;

/// Typeclass of data that can be written by the [ClickHouseSink].
///
/// A type is a [SinkRow] if it provides query template for the schema of the target table and the
/// insert query header, and if it can be serialied to [CompactJsonRow].
pub trait SinkRow: Into<CompactJsonRow> {
    fn table_ddl(table: &str) -> String;

    fn insert_ddl(table: &str) -> String;
}

/// Result of a block insert performed by the [ClickHouseSink].
pub enum ImportResult<E> {
    /// Insert is in progress, buffering the logs
    Pending,
    /// Inserts have been written and committed, returning last offsets as [`TopicPartitionList`]
    Success(TopicPartitionList),
    /// Insert has failed with error `E`
    Failure(E),
}

/// Chunk of data buffered for insert
struct InsertBlock<T> {
    /// The block of data to be inserted
    buffer: Vec<T>,
    /// Tracker of Kafka offsets corresponding to the last written data
    tracker: OffsetTracker,
}

impl<T> InsertBlock<T> {
    /// Allocate new buffer with given [capacity] and store associated [topic].
    #[inline]
    fn new(capacity: usize, topic: String) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            tracker: OffsetTracker::new(topic),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }
}

/// FIFO queue of [InsertBlock]s of given block size
struct InsertQueue<T> {
    /// Non-empy queue of data blocks to insert
    blocks: VecDeque<InsertBlock<T>>,
    /// The maximum block size, when reached a new block is allocated to the back of the queue
    max_block_size: u16,
    /// Kafka origin topic of the stored data
    topic: String,
    /// Total number of entries held by all the blocks
    entries: usize,
}

impl<T> InsertQueue<T> {
    fn new(max_block_size: u16, topic: String) -> Self {
        debug_assert!(max_block_size > 0, "block size cannot be zero");
        let mut blocks = VecDeque::new();
        blocks.push_back(InsertBlock::new(max_block_size as usize, topic.clone()));
        Self {
            blocks,
            max_block_size,
            topic,
            entries: 0,
        }
    }

    // XXX: use object-pool to reuse blocks and return (buffer, tpl)
    #[instrument(skip(self))]
    fn alloc_block(&self) -> InsertBlock<T> {
        info!(
            blocks = self.blocks.len() + 1,
            block_size = self.max_block_size,
            "allocating new insert block"
        );
        InsertBlock::new(self.max_block_size as usize, self.topic.clone())
    }

    /// Push [value] to the last block in this queue together with its `(partition, offset)` pair.
    ///
    /// Allocates new block if the last one is full.
    #[instrument(name = "insert_queue", skip_all)]
    fn push(&mut self, partition: i32, offset: i64, value: T) {
        // SAFETY: pop makes sure that the queue is never empty of blocks
        let last = self.blocks.back_mut().unwrap();
        let block = if last.len() < self.max_block_size as usize {
            last
        } else {
            self.blocks.push_back(self.alloc_block());
            self.blocks.back_mut().unwrap()
        };
        block.buffer.push(value);
        block.tracker.store(partition, offset);
        self.entries += 1;
    }

    /// Retrieve and remove the front block in this queue.
    ///
    /// Maintains the invariant that the queue must always be non-empty by allocating new block if
    /// the one returned was the only one in the queue.
    fn pop(&mut self) -> InsertBlock<T> {
        // SAFETY: unwrap is sound due to the following check & no other retrieval operation
        let block = self.blocks.pop_front().unwrap();

        if self.blocks.is_empty() {
            self.blocks.push_back(self.alloc_block());
        }

        self.entries -= block.len();
        block
    }

    /// Returns the number of entries in the front block of this queue.
    #[inline]
    fn front_entries(&self) -> usize {
        self.blocks
            .front()
            .map(InsertBlock::len)
            .unwrap_or_default()
    }
}

pub struct ClickHouseSink<T> {
    /// ClickHouse [`Client`](clickhouse_http_client::Client) for
    /// [`HttpLog`](crate::http_log::HttpLog)
    ch: clickhouse_http_client::Client,

    /// Buffered data for the next output (ClickHouse insert)
    ///
    /// Original `HttpLog` data are stored as [`serde_json::Value`](serde_json::Value)s and sent
    /// to ClickHouse as an insert with values in the
    /// [JsonCompactEachRow](https://clickhouse.com/docs/en/interfaces/formats/#jsoncompacteachrow)
    /// format.
    ///
    /// Due to import limitations, all buffered data are chunked into an equal-sized blocks and
    /// sotred in a queue together with their corresponding Kafka offsets.
    insert_queue: InsertQueue<CompactJsonRow>,

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
            insert_queue: InsertQueue::new(cfg.max_block_size, topic),
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
        self.insert_queue.push(partition, offset, data.into());
        debug!("data buffered for insert");

        let time_left = self.request_limiter.remaining_time();

        if !time_left.is_zero() {
            debug!(?time_left, "waiting to commit insert");
            return ImportResult::Pending;
        }

        let block_entries = self.insert_queue.front_entries();
        let total_entries = self.insert_queue.entries;
        debug_assert!(block_entries > 0, "data loss during write");

        // TODO: metrics (inserted, failed)

        let InsertBlock { buffer, tracker } = self.insert_queue.pop();

        let input = JsonCompactEachRowInput::new(buffer);

        info!(
            block = block_entries,
            buffered = total_entries,
            "inserting batch of data"
        );

        // TODO: handle 413 Payload Too Large
        // XXX: retry - the difficulty is that the buffer would have to be copied each time
        let result = self
            .ch
            .insert_with_format(&self.insert_ddl, input, None)
            .await;

        self.request_limiter.record_request();

        match result {
            Ok(()) => {
                info!(
                    rows = block_entries,
                    remainig = total_entries - block_entries,
                    "inserts committed"
                );
                ImportResult::Success(tracker.into())
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

#[cfg(test)]
mod tests {
    use maplit::hashmap;
    use rdkafka::Offset;

    use super::*;

    const TOPIC: &str = "topic";

    #[test]
    fn insert_queue() {
        let mut q: InsertQueue<u8> = InsertQueue::new(2, TOPIC.to_owned());

        assert_eq!(q.entries, 0);
        assert_eq!(q.blocks.len(), 1);

        let InsertBlock { buffer, tracker: _ } = q.pop();
        assert!(buffer.is_empty());
        assert_eq!(q.blocks.len(), 1);

        q.push(0, 0, 10);
        q.push(1, 0, 11);
        q.push(0, 1, 20);
        assert_eq!(q.entries, 3);
        assert_eq!(q.blocks.len(), 2);
        assert_eq!(q.front_entries(), 2);

        let InsertBlock { buffer, tracker } = q.pop();
        assert_eq!(vec![10, 11], buffer);

        let actual = tracker.load().to_topic_map();

        let expected = hashmap! {
            (TOPIC.to_owned(), 0) => Offset::Offset(1),
            (TOPIC.to_owned(), 1) => Offset::Offset(1),
        };

        assert_eq!(expected, actual);

        assert_eq!(q.entries, 1);
        assert_eq!(q.blocks.len(), 1);
        assert_eq!(q.front_entries(), 1);

        let InsertBlock { buffer, tracker: _ } = q.pop();
        assert_eq!(vec![20], buffer);

        assert_eq!(q.entries, 0);
        assert_eq!(q.blocks.len(), 1);
        assert_eq!(q.front_entries(), 0);
    }
}
