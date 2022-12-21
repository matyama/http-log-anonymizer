//! Module with the definition of a [`HttpLog`], its `capnp` representation, and its
//! implementation of [`SinkRow`].
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};
use rdkafka::message::{Message, OwnedMessage};
use serde::Serialize;
use time::OffsetDateTime;

use crate::sink::{CompactJsonRow, SinkRow};

pub mod model {
    include!(concat!(env!("OUT_DIR"), "/http_log_capnp.rs"));
}

use crate::http_log::model::http_log_record;

#[derive(Debug, Serialize)]
pub struct HttpLog {
    #[serde(with = "time::serde::timestamp")]
    pub timestamp: OffsetDateTime,
    pub resource_id: u64,
    pub bytes_sent: u64,
    pub request_time_milli: u64,
    pub response_status: u16,
    pub cache_status: String,
    pub method: String,
    pub remote_addr: String,
    pub url: String,
}

impl TryFrom<OwnedMessage> for HttpLog {
    type Error = capnp::Error;

    fn try_from(value: OwnedMessage) -> Result<Self, Self::Error> {
        let mut buffer = value.payload().unwrap_or_default();

        let raw_data = read_message_from_flat_slice(&mut buffer, ReaderOptions::new())?;

        let data = raw_data.get_root::<http_log_record::Reader<'_>>()?;

        let timestamp = OffsetDateTime::from_unix_timestamp_nanos(
            data.get_timestamp_epoch_milli() as i128 * 1_000_000,
        )
        .map_err(|e| capnp::Error {
            kind: capnp::ErrorKind::Failed,
            description: format!("failed to parse 'timestamp_epoch_milli': {}", e),
        })?;

        Ok(Self {
            timestamp,
            resource_id: data.get_resource_id(),
            bytes_sent: data.get_bytes_sent(),
            request_time_milli: data.get_request_time_milli(),
            response_status: data.get_response_status(),
            cache_status: data.get_cache_status()?.to_owned(),
            method: data.get_method()?.to_owned(),
            remote_addr: data.get_remote_addr()?.to_owned(),
            url: data.get_url()?.to_owned(),
        })
    }
}

impl From<HttpLog> for CompactJsonRow {
    #[inline]
    fn from(value: HttpLog) -> Self {
        vec![
            value.timestamp.unix_timestamp().into(),
            value.resource_id.into(),
            value.bytes_sent.into(),
            value.request_time_milli.into(),
            value.response_status.into(),
            value.cache_status.into(),
            value.method.into(),
            value.remote_addr.into(),
            value.url.into(),
        ]
    }
}

// TODO: safety - table name should be properly escaped
impl SinkRow for HttpLog {
    #[inline]
    fn table_ddl(table: &str) -> String {
        format!(
            "
            CREATE TABLE IF NOT EXISTS {} (
              timestamp DateTime NOT NULL,
              resource_id UInt64 NOT NULL,
              bytes_sent UInt64 NOT NULL,
              request_time_milli UInt64 NOT NULL,
              response_status UInt16 NOT NULL,
              cache_status LowCardinality(String) NOT NULL,
              method LowCardinality(String) NOT NULL,
              remote_addr String NOT Null,
              url String NOT NULL
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (resource_id, response_status, remote_addr, timestamp)
            ",
            table,
        )
    }

    #[inline]
    fn insert_ddl(table: &str) -> String {
        format!(
            "
            INSERT INTO {} (
              timestamp,
              resource_id,
              bytes_sent,
              request_time_milli,
              response_status,
              cache_status,
              method,
              remote_addr,
              url
            )",
            table
        )
    }
}
