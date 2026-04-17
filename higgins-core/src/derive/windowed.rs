//! All liveness work on the windowed specific derived streams

use super::joining::opts::eager_take_from_subscription_or_wait;
use crate::broker::{Broker, BrokerIndexFile};
use crate::derive::joining::opts::eager_range_take_or_wait;
use crate::derive::windowed::definition::WindowedStreamType;
use crate::error::HigginsError;
use crate::storage::index::windowed_index::WindowedIndex;
use crate::storage::index::{Index, IndexType, index_size_from_index_type_and_definition};
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
    let stream = definition.base_stream.base.unwrap();

    let (client_id, condvar, subscription) = {
        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();
        let subscription = broker.create_subscription(stream.as_bytes());
        let (notify, subscription) = broker
            .get_subscription_by_key(stream.as_bytes(), &subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
            .unwrap();

        tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");

        (client_id, notify, subscription)
    };

    broker
        .task_handler
        .spawn(&SpawnTaskConfig::new("windowing", false), async move {
            let offsets =
                eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                    .await
                    .unwrap();

            for (partition, offsets) in offsets.iter() {
                let resultant_stream = String::from_utf8(stream.as_bytes().to_vec()).unwrap();

                // TODO: maybe paralellize these?
                let mut resultant_index_file =
                    get_index_file_handle(&resultant_stream, partition, broker_ref.clone()).await;

                let derived_index_file =
                    get_index_file_handle(&definition.base_key, partition, broker_ref.clone())
                        .await;

                match definition.window_type {
                    WindowedStreamType::Count(count) => {
                        let last_resultant_index = {
                            let mut guard = resultant_index_file.lock().await;

                            let view = guard.as_indexes_mut();
                            let len = view.len();

                            let index_bytes = match len {
                                0 => None,
                                _ => view.get(len - 1).map(|data| data.to_vec()),
                            };

                            let mut ranges_to_save: Vec<u8> = vec![];

                            if let Some(data) = index_bytes {
                                let index = WindowedIndex::of(&data);

                                let range_size = index.range().end - index.range().end;

                                if range_size < count {}
                            }
                        };

                        //  -> fill the index, then continue
                        //  -> chunk the range into {count} sized chunks and create indexes for each, append the indexes in one swoop.
                        //
                    }
                    WindowedStreamType::Timed((count, time_unit)) => {
                        // ON timestamp type
                        //
                        //  -> We'd need to check the resultant offset's timestamp.
                        //  -> We would check if there are any "open" windows where this timestamp >
                        //  -> We add in these values and close the open ones that can be closed

                        todo!()
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
