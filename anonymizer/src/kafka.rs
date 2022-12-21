//! This module contains utility components for working with Kafka offsets
//!
//! # `LoggingConsumer`
//! The [`LoggingConsumer`] is simply a [`StreamConsumer`] with custom [`ConsumerContext`] which
//! adds logging output whenever an offset commit has been attempted by the consumer.
//!
//! # `OffsetTracker`
//! The [`OffsetTracker`] is a component that wraps arond a Kafka topic and a
//! [`TopicPartitionList`] and provides nicer API to `store` and `load` the last offset.
use rdkafka::consumer::{stream_consumer::StreamConsumer, ConsumerContext};
use rdkafka::error::KafkaResult;
pub(crate) use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Offset;
use tracing::{info, instrument, warn};

pub struct LoggingConsumerContext;

impl rdkafka::client::ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    #[instrument(name = "commit", skip_all)]
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!(?offsets, "offsets committed successfully"),
            Err(e) => warn!(cause = ?e, "failed to commit offsets"),
        };
    }
}

pub type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

// XXX: generalize sink over an OffsetTracker trait (make this struct an instance)
pub struct OffsetTracker {
    /// Single Kafka topic that's being consumed
    topic: String,

    /// Offsets of the last written logs for each partition and `topic`
    tpl: TopicPartitionList,
}

impl OffsetTracker {
    /// Create new empty tracker for given Kafka `topic`.
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            tpl: TopicPartitionList::new(),
        }
    }

    /// Save given `(partition, offset)` pair for this tracker's registered topic.
    pub fn store(&mut self, partition: i32, offset: i64) {
        let mut p = match self.tpl.find_partition(&self.topic, partition) {
            Some(p) => p,
            None => self.tpl.add_partition(&self.topic, partition),
        };

        // NOTE: 1 plus the offset from the last consumed message according to `commit()` docs
        p.set_offset(Offset::Offset(offset + 1))
            .expect("Kafka offset update");
    }

    /// Load latest offsets stored for this tracker's registered topic.
    #[inline]
    pub fn load(&self) -> TopicPartitionList {
        self.tpl.clone()
    }
}

impl From<OffsetTracker> for TopicPartitionList {
    #[inline]
    fn from(value: OffsetTracker) -> Self {
        value.tpl
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplit::hashmap;

    const TOPIC: &str = "test-topic";

    #[test]
    fn offset_tracking() {
        let offsets = vec![(0, 0), (1, 0), (0, 1)];

        let mut tracker = OffsetTracker::new(TOPIC.to_owned());

        for (partition, offset) in offsets {
            tracker.store(partition, offset);
        }

        let expected = hashmap! {
            (TOPIC.to_owned(), 0) => Offset::Offset(2),
            (TOPIC.to_owned(), 1) => Offset::Offset(1),
        };

        let actual = tracker.load().to_topic_map();

        assert_eq!(expected, actual);
    }

    #[test]
    fn no_offsets() {
        let tracker = OffsetTracker::new(TOPIC.to_owned());
        let stored = tracker.load().to_topic_map();
        assert!(stored.is_empty());
    }
}
