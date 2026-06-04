//! All liveness work on the windowed specific derived streams

use crate::broker::{Broker, BrokerIndexFile};
use crate::derive::joining::opts::eager_range_take_or_wait;
use crate::derive::operation::OperationData;
use crate::derive::windowed::definition::WindowValue;
use crate::error::HigginsError;
use crate::storage::index::file::windowed_index_file::WindowedIndexFile;
use crate::storage::windowing::assign_sliding_windows_range;
use crate::subscription::Subscription;
use crate::task::SpawnTaskConfig;
use crate::topography::Key;
use definition::WindowedStreamDefinition;
use higgins_shared::PartitionName;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod definition;

pub struct WindowOperation(pub OperationData);
// /// Broker  Reference.
// pub broker: Arc<RwLock<Broker>>,
// /// This resultant stream's stream name.
// pub stream: String,
// /// This resultant streams stream definition.
// pub definition: WindowedStreamDefinition,
// /// The partition we've received offsets on.
// pub partition: PartitionName,
// /// The offsets.
// pub offsets: Range<u64>,
// // /// The references - We want to use these to commit so we have to save them over init and commit branches.
// // references: Option<Vec<Reference>>,
// /// The subscription that controls how this stream is tracked.
// pub subscription: Arc<RwLock<Subscription>>,
// // The underlying records that this operation is based on. (Current unused )
// // Vec<(
// //   Vec<u8> - IPC record batch.
// //   u64 - The offset to which it belongs.
// // )>
// // records: Vec<(Vec<u8>, u64)>,

impl WindowOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let stream_key: Key = self.0.stream.clone().into();
        let definition =
            WindowedStreamDefinition::try_from((stream_key, self.0.definition.clone()))?;

        match &definition.window_type {
            WindowValue::Count(count) => {
                let resultant_stream =
                    String::from_utf8(self.0.stream.as_bytes().to_vec()).unwrap();

                tracing::info!("Retrieving index file for stream {resultant_stream}");

                // TODO: maybe paralellize these?
                let mut resultant_index_file = get_index_file_handle(
                    &self.0.stream.to_string(),
                    &self.0.partition,
                    self.0.broker.clone(),
                )
                .await;

                tracing::info!("Retrieved index file..");

                let offsets = self.0.offsets.get().await?;

                let mut new_ranges = assign_sliding_windows_range(
                    offsets.clone(),
                    count.clone(),
                    definition.slide.normalize(),
                    0,
                );

                let mut guard = resultant_index_file.lock().await;
                let index_file = guard.as_index();

                let mut windowed_index_file = WindowedIndexFile::of(index_file);

                tracing::info!("Applying ranges {:#?} to windowed index file.", new_ranges);

                windowed_index_file.put_ranges(&mut new_ranges).unwrap();

                // acknowledge me!
                {
                    let mut guard = self
                        .0
                        .subscription
                        .as_ref()
                        .ok_or(HigginsError::Unknown)?
                        .write()
                        .await;
                    tracing::info!("Acknowledging ranges {:#?}.", offsets);
                    guard.acknowledge(&self.0.partition, &offsets).unwrap();
                }

                // debug only, remove after
                // {
                //     tracing::info!("LOOK HERE ");
                //     use crate::storage::index::windowed_index::WindowedIndex;
                //     let mut bytes = [0u8; WindowedIndex::size_of()];
                //     index_file.read_at(0, &mut bytes).unwrap();
                //     for index in bytes
                //         .chunks(WindowedIndex::size_of())
                //         .map(WindowedIndex::of)
                //     {
                //         tracing::debug!("{:#?}", index);
                //     }
                // }

                tracing::info!("Successfully applied ranges to windowed function.");
            }
            WindowValue::Timed((_count, _time_unit)) => {
                tracing::error!("TIMED STREAM IS NOT AVAILABLE");

                // ON timestamp type
                //
                //  -> We'd need to check the resultant offset's timestamp.
                //  -> We would check if there are any "open" windows where this timestamp >
                //  -> We add in these values and close the open ones that can be closed

                todo!()
            }
        }

        Ok(())
    }
}

pub async fn create_windowed_stream_from_definition(
    definition: WindowedStreamDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) {
    tracing::trace!("Calling create_windowed_stream_from_definition.");
    tracing::trace!("{:#?}", definition.base_key);
    let base_stream = definition.base_key.clone();
    let stream = definition.resultant_key.clone();

    let (client_id, condvar, subscription) = {
        tracing::trace!("Attempting to input client_id.");

        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();

        tracing::trace!("Retrieved client_id.");
        let subscription = broker.create_subscription(base_stream.as_bytes());

        tracing::trace!("Successfully created the subscription.");

        let (notify, subscription) = broker
            .get_subscription_by_key(base_stream.as_bytes(), &subscription)
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

                todo!()
                // for (partition, offsets) in offsets.iter() {
                //     let mut operation = WindowOperation {
                //         broker: broker_ref.clone(),
                //         stream: stream.clone(),
                //         definition: definition.clone(),
                //         partition: partition.clone(),
                //         offsets: offsets.clone(),
                //         subscription: subscription.clone(),
                //     };

                //     operation.init().await.unwrap();
                //     operation.prepare().await.unwrap();
                //     operation.commit().await.unwrap();
                // }
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
    broker.get_index_file(stream.to_owned(), key).unwrap()
}
