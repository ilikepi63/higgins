//! All liveness work on the windowed specific derived streams

use crate::broker::{Broker, BrokerIndexFile};
use crate::derive::joining::opts::eager_range_take_or_wait;
use crate::derive::windowed::definition::WindowValue;
use crate::error::HigginsError;
use crate::storage::index::file::windowed_index_file::WindowedIndexFile;
use crate::storage::index::windowed_index::{self, WindowedIndex};
use crate::storage::windowing::assign_sliding_windows_range;
use crate::task::SpawnTaskConfig;
use definition::WindowedStreamDefinition;
use higgins_shared::PartitionName;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod definition;

pub async fn create_windowed_stream_from_definition(
    definition: WindowedStreamDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) {
    tracing::trace!("Calling create_windowed_stream_from_definition.");
    tracing::trace!("{:#?}", definition.base_key);
    let stream = definition.base_key.clone();

    let (client_id, condvar, subscription) = {
        tracing::trace!("Attempting to input client_id.");

        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();

        tracing::trace!("Retrieved client_id.");
        let subscription = broker.create_subscription(stream.as_bytes());

        tracing::trace!("Successfully created the subscription.");

        let (notify, subscription) = broker
            .get_subscription_by_key(stream.as_bytes(), &subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
            .unwrap();

        tracing::trace!("Retrieved the notification for said subscription.");

        (client_id, notify, subscription)
    };

    tracing::trace!("Retrieved client_id.");

    broker
        .task_handler
        .spawn(&SpawnTaskConfig::new("windowing", false), async move {
            tracing::trace!("Spawning task.");

            loop {
                let offsets =
                    eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                        .await
                        .unwrap();

                tracing::info!("Retrieved some offsets: {:#?}", offsets);

                for (partition, offsets) in offsets.iter() {
                    let resultant_stream = String::from_utf8(stream.as_bytes().to_vec()).unwrap();

                    // TODO: maybe paralellize these?
                    let mut resultant_index_file =
                        get_index_file_handle(&resultant_stream, partition, broker_ref.clone())
                            .await;

                    let derived_index_file =
                        get_index_file_handle(&definition.base_key, partition, broker_ref.clone())
                            .await;

                    match definition.window_type {
                        WindowValue::Count(count) => {
                            tracing::error!("RECEIVED COUNT FROM UNDERLYING STREAM: {count}");

                            let mut new_ranges = assign_sliding_windows_range(
                                offsets.clone(),
                                count,
                                definition.slide.normalize(),
                                0,
                            );

                            let mut guard = resultant_index_file.lock().await;
                            let index_file = guard.as_index();

                            let mut windowed_index_file = WindowedIndexFile::of(index_file);

                            tracing::info!(
                                "Applying ranges {:#?} to windowed index file.",
                                new_ranges
                            );

                            windowed_index_file.put_ranges(&mut new_ranges).unwrap();

                            tracing::info!("Successfully applied ranges to windowed function.");
                        }
                        WindowValue::Timed((count, time_unit)) => {
                            // ON timestamp type
                            //
                            //  -> We'd need to check the resultant offset's timestamp.
                            //  -> We would check if there are any "open" windows where this timestamp >
                            //  -> We add in these values and close the open ones that can be closed

                            todo!()
                        }
                    }
                }
            }
        })
        .unwrap();
}

async fn get_index_file_handle(
    stream: &str,
    key: &PartitionName,
    broker_ref: Arc<RwLock<Broker>>,
) -> BrokerIndexFile {
    let mut broker = broker_ref.write().await;
    broker
        .get_index_file(stream.to_owned(), key, WindowedIndex::size_of())
        .unwrap()
}

use std::ops::Range;

pub struct MutableRange<T>(Range<T>);

impl<T> From<Range<T>> for MutableRange<T> {
    fn from(value: Range<T>) -> Self {
        Self(value)
    }
}

// We have a range x..y that is non-zero in length.
//
// where there is another range

impl MutableRange<u64> {
    /// Takes from the front of this range, update the amount
    /// and returning how many were taken from the front of this range.
    pub fn take_front(&mut self, v: u64) -> u64 {
        let start = self.0.start + v;

        if start > self.0.end {
            self.0.start = self.0.end;
            start - self.0.end
        } else {
            self.0.start = start;
            0
        }
    }
}

#[cfg(test)]
mod test {
    use crate::derive::windowed::MutableRange;

    #[test]
    fn test_mutable_range() {
        let mut range: MutableRange<u64> = MutableRange::from(0..7);

        let result = range.take_front(5);

        assert_eq!(range.0.start, 5);
        assert_eq!(range.0.end, 7);
        assert_eq!(result, 0);

        let result = range.take_front(5);

        assert_eq!(range.0.start, 7);
        assert_eq!(range.0.end, 7);
        assert_eq!(result, 3);
    }
}
