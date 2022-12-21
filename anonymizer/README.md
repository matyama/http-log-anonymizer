# `anonymizer:0.1.0`

## Environment
The application expects certain set of environment variables (described below). A working
configuration can be found in `../.envrc` can can be automatically loaded with tools such as
[`direnv`](https://direnv.net/).

## Run the pipeline with `cargo`
The basic way how to run the application is as follows:
```bash
cargo run --bin anonymizer --release
```
There are no program argumens, all is done via environment variables.

## Run the pipeline as a `docker-compose` service
The application has been dockerized (see `Dockerfile`) and there is a configuration of a
`docker-compose` service inside `../docker-compose.yml`. It is currently commented out for
development purposes but should work when enabled.

The service depends only on Kafka brokers and the ClickHouse proxy.

## Documentation
The main part of the documentation follows this section. For the full Rust docs run
```bash
cargo doc --open
```

## Notes
 - Note that for development convenience `./http_log.capnp` is a copy of `../http_log.capnp`. It
   makes it easier to make it part of the build script (`build.rs`), especially for `Dockerfile`.
 - The application does not hold any state. The output are either data being inserted into
   ClickHouse or offsets committed back to the Kafka cluster. So it only depends on the persistence
   setup of the services inside `../docker-compose.yml`, which have not been altered (i.e. the
   state will be lost upon restart).

## HTTP Log Anonymizer
This crate forms basic building blocks for an HTTP log anonymizer pipeline running on [Tokio
runtime](https://tokio.rs/), consuming and parsing [Cap'N Proto](https://capnproto.org/)
messages from [Kafka](https://kafka.apache.org/) and inserting them _anonymized_ into
[ClickHouse](https://clickhouse.com/) datastore.

The core anonymization logic is realizaed by [`anonymize_ip`] and makes the _remote address_ of
each consumed log _GDPR-complient_.

### Architecture
The anonymizer pipeline is split into two main components:
 - Kafka [`StreamConsumer`](rdkafka::consumer::stream_consumer::StreamConsumer)s that are
   spawned as Tokio tasks and drive the HTTP log processing logic
 - [`ClickHouseSink`](sink::ClickHouseSink) which is responsible for efficient data import
   (insert) into ClickHouse, respecting it's request rate limits and tracking Kafka offsets

#### Kafka source & consumers
The Kafka stream consumers drive the main application logic which can be found in the
[`source`] module.

The [`StreamConsumer`](rdkafka::consumer::stream_consumer::StreamConsumer) runs an `async`
task for each consumed message which does the following:
 1. Parses received Kafka message into [`HttpLog`](http_log::HttpLog) using the
    [`capnp`](capnp) crate.
 1. Runs [`anonymize_ip`] on the `remote_addr` field of [`HttpLog`](http_log::HttpLog)
 1. Yields the log data to the [`ClickHouseSink`](sink::ClickHouseSink) to be either buffered
    or inserted into the database
 1. Based on the [`ImportResult`](sink::ImportResult) it either reports and propagates an
    error, continues with the next message, or `commit`s Kafka offsets for the inserted batch

See more implementation details in the description of the [`source`](source) module.

#### ClickHouse sink
The [`ClickHouseSink`](sink::ClickHouseSink) component is shared between all Kafka consumers
and serves two purposes:
 1. It buffers and partitions the data into insert blocks, so that insert requests are more
    efficient
 1. And at the same time tracks the time since the last insert request so that the database
    instance is not under too much stress (and inserts get rejected)

Besides the data itself, each insert block also tracks the `(partition, offset)` pair of the
last written data. This offset pair is later returned with [`ImportResult`](sink::ImportResult)
back to a consumer for commit.

See more implementation details in the description of the [`sink`](sink) module.

### Delivery Semantics
The exact delivery semantics of the anonymizer pipeline as a whole depends on several details,
but in the case of [`HttpLog`](http_log::HttpLog)s it can achive __(eventually) exactly-once__
with `SELECT` queries using `FINAL`. In general it should achieve the _at-least-once_ semantics
for arbitrary [`SinkRow`](sink::SinkRow)s.

#### Argument for _at-least-once_ semantics
1. Each Kafka consumer starts with specific configuration, specifically (see
   [kafka docs](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)):
   ```ignore
      rdkafka::ClientConfig::new()
        .set("group.id", cfg.group_id)
        .set("bootstrap.servers", cfg.brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
   ```
   From the snippet above it's clear that
    1. All consumers start with the same `group.id` and so each will __consume__ it's own
       partition (or be idle) __in-order__ of the partition offsets
    1. The pipeline (consumers) will after restart __start consuming the `latest` committed
       offset (+1)__
    1. The __`auto.commit` is disabled__ and it will be on the consumer itself to manually
       commit its offsets
    1. It signals to the Kafka brokers that it only wants to __read messages committed by
       producers__
1. The application logic __discards invalid (unparsable) messages__ which is an idempotent
   operation (i.e. the same message gets discarded again when replayed). This is a design
   choice which is discussed a bit more below. It does not change the following argument for
   the sink because even if it is not dropped, one can imagine a _"failure sink"_ that would
   behave similarly to the ClickHouse sink (but perhaps with an Kafka topic output).
1. The sink then accepts a `(partition, offset, data)` input for an insert. The soundness
   argument relies on two facts:
    a. There is only single sink and consumers must have an exclusive access to it to write the
       data
    b. Consumers write the data __sequentially__ in the offset order of their partitions
   The sink then proceeds as follows:
    1. When a data point is written to an active block, the block also __tracks it's latest
       offset__
    1. After the sink sends an insert block for insert to the underlying ClickHouse client, it
       either responds with an error or the [`TopicPartitionList`](rdkafka::TopicPartitionList)
       for the consumer to commit (which are the last offsets in that particular batch)
1. The consumer then, and only then, tries to commit the offsets (repeated `KAFKA__RETRIES`
   times), but only if the insert was successful.

Therefore upon a restart the consumer will __start from the last committed__ offset and replay
all writes from that point forward. This means that no data are accidentally lost but we can
create data duplication due to non-idempotent outputs.

#### Argument for _(eventually) exacly-once_ semantics
The way the sink deals with data duplication is simply by relying on the
[ReplacingMergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree/)
engine to _eventually_ deduplicate the data.

This is done either eventually in the background or using the `FINAL` keyword in a `SELECT`
query to view the final (deduplicated) state. So the semantics is really not _exacly-once_ in
the strict sense and depends on the implementation details of particular
[`SinkRow`](sink::SinkRow) instance.

For example the [`HttpLog`](http_log::HttpLog) table DDL specifies:
```sql
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (resource_id, response_status, remote_addr, timestamp)
```
Where the sort key should uniquely identify the HTTP logs with sufficient confidence (otherwise
one could for instance use an id formed as a hash of the log itself).

Then queries such as
```sql
SELECT remote_addr, response_status, count(*) AS cnt
FROM http_log FINAL
GROUP_BY remote_addr, response_status
ORDER_BY remote_addr, cnt DESC
```
should observe results as if the delivery semantics was _exacly-once_.

#### Discarding invalid messages
The main argument for dropping invalid messages is that they __block progress__. An ETL
pipeline has specific semantics and if some input data don't match the type of data the
pipelien can operate on (or can automatically transform to such a format), then the situation
is as follows:
 - The pipeline cannot proceess such message (invalid data), therefore it can't commit its
   offset
 - The pipeline then can only crash and restart. But this means starting from the __last
   uncommitted__ message, which is the one that caused its crash in the first place.

Such an invalid data point must be (semi-)automatically transformed to a valid one or generally
judged if it really should be dropped. This is probably a job for a dedicated service (with
possible human intervention). In any case, such process will produce essentially new data with
an old _event time_ but with a _new offset_ (needs to be re-inserted into Kafka).

#### Caveat: commiting Kafka offsets
There are two ways how commit offsets with `librdkafka`
 1. `Sync` - the commit call blocks and waits for an ack from Kafka
 1. `Async` - commit is done asynchronously without waiting.

Current implementation uses async commits, which can have implication on the delivery
semantics. However, it's an easy change in case the infrastructure is expected to exhibit such
failurs.

#### Resources
Some resources on the overvriw of "upsert" strategies when using ClickHouse can be found here:
 - [real-time updates](https://altinity.com/blog/2020/4/14/handling-real-time-updates-in-clickhouse)
 - [overview](https://www.tinybird.co/docs/guides/deduplication-strategies.html)

Interestingly, from the former post it seems that the basic `ReplacingMergeTree` approach is
not that bad in comparison with a periodic `OPTIMIZE FINAL`.

### Scalability
The anonymizer pipeline can be scaled both vertically and horizontally:
 - __vertical__: Setting `NUM_CONSUMERS=2` will spawn two tasks inside the threaded Tokio
   runtime, which (in case there are at least two Kafka topic partitions) would increase the
   ingestion rate. This can work well if the rate at which the sink sends insert queries to
   ClickHouse is long enough that it can accumulate large batches for single request.
 - __horizontal__: The whole pipeline can be easily replicated on a different cluster node
   under the assumption that all the replicas use the same `KAFKA__GROUP_ID`, so that the
   consumers join the same group (otherwise it would break the delivery semantics). Secondly,
   this also assumes that a rate limitter (proxy) is ok with accepting requests from two sinks
   at the same time, otherwise there would be no point to have this form of scaling.

### Dependecies
Here is a list with the notable dependencies that are essential to the anonymizer pipeline:
 - [Tokio async runtime](https://tokio.rs/)
 - [Cap'n Proto for Rust](https://github.com/capnproto/capnproto-rust)
 - [`librdkafka` for Rust](https://github.com/fede1024/rust-rdkafka)
 - [ClickHouse HTTP client](https://crates.io/crates/clickhouse-http-client)

And to make the implementation more sane this crate also depends on:
 - [`config`](https://crates.io/crates/config) for env-based configuration
 - [`thiserror`](https://crates.io/crates/thiserror) and
   [`anyhow`](https://crates.io/crates/anyhow) for better error handling and propagation
 - [Tokio `tracing`](https://github.com/tokio-rs/tracing) for tracing (and logging) in async
   contexts

### Known issues & limitations
 - There could be a better separation of the Kafka message ingestion & application logic of the
   anonymizer pipeline
 - The sink is currently shared with a standard [`Arc`](std::sync::Arc) and
   [`Mutex`](tokio::sync::Mutex) combination, but it would be interesting to investigate some
   _lock-free_ approaches or other design options how to overcome the congestion point
   introduced by the critical section of data writes into the sink.
 - Investigation of more complex (and perhaps better) ways to achieve deduplication in
   ClickHouse other than `ReplacingMergeTree` in DDL with `FINAL` in queries
 - A big drawback of the current [`clickhouse_http_client::Client`], the underlying ClickHouse
   driver used by [`ClickHouseSink`](sink::ClickHouseSink), is that it does not provide an API
   for custom HTTP headers and only accepts compact JSON [`Value`](serde_json::Value)s for
   insert. Therefore it's not currently possible to send compressed data which would allow us
   to increase the insert block size (`CH__MAX_BLOCK_SIZE`) and lower the data latencies for a
   large input batch accumulated in Kafka (i.e. when last committed offset is quite old).
 - The [`source`](source) code generally does not deal with a _graceful shutdown_ and only
   tries to propagate and log meaningful messages. The reason this is not yet supported is
   because it's no straightforwart how the shutdown should behave. Should it for example wait
   till the next insert block finishes? It could be quite a long time depending on
   `CH__RATE_LIMITa`. Or should it iterrupt the insert? But in that case consumers won't commit
   offsets to Kafka, which essentially defeats the purpose of a _graceful_ shutdown.
 - Sink does not retry block inserts (as it does for the create table query). This is for two
   reasons:
    1. It could singnificantly slow down the processing (inside a critical section) depending
       on the delay value (`CH__RATE_LIMIT`)
    1. It would require a (potentially expensive) complete buffer clone because the underlying
       [`clickhouse_http_client::Client`] takes the ownership of the batch being inserted.
 - Currently there is no mechanism for _backpressure_ from the sink to the Kafka consumers
 - Collecting performace metrics is not yet implemented (although this shouldn't be hard)
 - Generally way more integration and unit tests would be necessary for this to make it into a
   production-ready state

