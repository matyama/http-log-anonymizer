use anonymizer::anonymize_ip;
use callysto::futures::StreamExt;
use callysto::prelude::message::*;
use callysto::prelude::*;
use capnp::{message::ReaderOptions, serialize::read_message_from_flat_slice};

// TODO: possibly extract to lib module
pub mod model {
    include!(concat!(env!("OUT_DIR"), "/http_log_capnp.rs"));
}

use crate::model::http_log_record;

// TODO: ClickHouse works with `&str` or
//  - https://docs.rs/smartstring/latest/smartstring/struct.SmartString.html
//  - or possibly https://docs.rs/serde_bytes/latest/serde_bytes/
#[allow(dead_code)]
#[derive(Debug)]
struct HttpLog {
    // TODO: schema defines u64 but DateTime maps to/from u32
    timestamp: u64,
    resource_id: u64,
    bytes_sent: u64,
    request_time_milli: u64,
    response_status: u16,
    cache_status: String,
    method: String,
    remote_addr: String,
    url: String,
}

impl TryFrom<OwnedMessage> for HttpLog {
    // TODO: thiserror and/or anyhow
    type Error = CallystoError;

    fn try_from(value: OwnedMessage) -> Result<Self> {
        let mut buffer = value.payload().expect("failed to get message payload");

        let raw_data = read_message_from_flat_slice(&mut buffer, ReaderOptions::new())
            .expect("failed to build reader");

        let data = raw_data
            .get_root::<http_log_record::Reader<'_>>()
            .expect("failed to get reader");

        let cache_status: &str = data.get_cache_status().expect("failed to get cache status");

        let method: &str = data.get_method().expect("failed to get method");

        let remote_addr: &str = data
            .get_remote_addr()
            .expect("failed to get remote address");

        let url: &str = data.get_url().expect("failed to get url");

        Ok(Self {
            timestamp: data.get_timestamp_epoch_milli(),
            resource_id: data.get_resource_id(),
            bytes_sent: data.get_bytes_sent(),
            request_time_milli: data.get_request_time_milli(),
            response_status: data.get_response_status(),
            cache_status: cache_status.to_owned(),
            method: method.to_owned(),
            remote_addr: remote_addr.to_owned(),
            url: url.to_owned(),
        })
    }
}

async fn anonymizer_agent(mut stream: CStream, _ctx: Context<()>) -> Result<()> {
    while let Some(msg) = stream.next().await {
        let mut log = msg
            .map(|m| {
                //println!("Received message: `{:?}`", m);

                // TODO: filter out invalid msgs (or rather report them to stats)
                HttpLog::try_from(m).expect("valid message")
            })
            .expect("infinite Kafka stream");

        println!("Received log: {:?}", &log);
        log.remote_addr = anonymize_ip(log.remote_addr);
        println!("Anonymized log: {:?}", log);
    }

    Ok(())
}

fn main() {
    // TODO: decide which crate to use to implement the pipeline (this is just a demo)
    let mut app = Callysto::new();

    let mut config = Config::default();
    config.kafka_config.isolation_level = IsolationLevel::ReadCommitted;
    //config.kafka_config.enable_auto_commit = false;

    // TODO: include ID arg (for replica distinction)
    app.with_name("anonymizer").with_config(config);

    // TODO: topic, group_id form env
    // TODO: each agent is spawned as a new task => include id (number from args)
    app.agent("anonymizer_agent", app.topic("http_log"), anonymizer_agent);

    // TODO: propper logging/tracing
    println!(">>> starting anonymizer app");
    app.run();
}
