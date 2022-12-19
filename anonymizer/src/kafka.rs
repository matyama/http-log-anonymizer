use rdkafka::{topic_partition_list::TopicPartitionList, Offset};

// XXX: generalize sink over an OffsetTracker trait (make this struct an instance)
pub struct OffsetTracker {
    /// Single Kafka topic that's being consumed
    topic: String,

    /// Offsets of the last written logs for each partition and [topic]
    tpl: TopicPartitionList,
}

impl OffsetTracker {
    /// Create new empty tracker for given Kafka [topic].
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            tpl: TopicPartitionList::new(),
        }
    }

    /// Save given `(partition, offset)` pair for this tracker's registered topic.
    pub fn store(&mut self, partition: i32, offset: i64) {
        // XXX: test/assert - write(partition, offset, _) => offset = tpl[partition] + 1

        let mut p = match self.tpl.find_partition(&self.topic, partition) {
            Some(p) => p,
            None => self.tpl.add_partition(&self.topic, partition),
        };

        // XXX: offset + 1 (?)
        p.set_offset(Offset::Offset(offset))
            .expect("Kafka offset update");
    }

    /// Load latest offsets stored for this tracker's registered topic.
    #[inline]
    pub fn load(&self) -> TopicPartitionList {
        self.tpl.clone()
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
            (TOPIC.to_owned(), 0) => Offset::Offset(1),
            (TOPIC.to_owned(), 1) => Offset::Offset(0),
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
