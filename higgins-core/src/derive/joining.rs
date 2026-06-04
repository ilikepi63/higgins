//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

use std::sync::Arc;

use tokio::sync::RwLock;

mod completion;
pub mod join;
pub mod mapping;
pub mod opts;

use crate::derive::joining::completion::complete_from;
use crate::derive::operation::OperationData;
use crate::storage::dereference::Reference;
use crate::storage::index::joined_index::JoinedIndex;
use crate::task::SpawnTaskConfig;
use crate::topography::StreamName;
use crate::utils::epoch;
use crate::{broker::BrokerIndexFile, derive::joining::opts::eager_range_take_or_wait};
use opts::amalgamate_join;

use crate::{broker::Broker, error::HigginsError};

use crate::{client::ClientRef, derive::joining::join::JoinDefinition};

pub struct JoinOperation {
    pub data: OperationData,
    pub definition: JoinDefinition,
    /// Offset at which this index is trying to place.
    pub optimistic_offset: Option<usize>,
    pub optimistic_index: Option<Vec<u8>>,
}

impl JoinOperation {
    pub async fn init(&mut self) -> Result<(), HigginsError> {
        Ok(())
    }
    pub async fn prepare(&mut self) -> Result<(), HigginsError> {
        let stream: Vec<u8> = self.definition.clone().base.0.into();
        let n_offsets = self.definition.joins.len();

        // Retrieve the Index file, given the stream name and partition key.
        let mut index_file = {
            let mut broker = self.data.broker.write().await;
            let index_file: BrokerIndexFile = broker
                .get_index_file(
                    String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
                    &self.data.partition,
                )
                .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
            tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
            drop(broker);
            index_file
        };

        // we first make a voodoo index.
        let optimistic_offset = {
            let guard = index_file.lock().await;
            guard.len().unwrap()
        };

        // Create the index.
        let mut optimistic_index = vec![0_u8; JoinedIndex::size_of(n_offsets)];

        let offsets = self.data.offsets.get().await?;

        tracing::debug!("We retrieved the offsets: {:#?}", offsets);

        JoinedIndex::put(
            optimistic_offset as u64,
            Reference::Null,
            epoch(),
            &(0..n_offsets)
                .into_iter()
                .map(|i| {
                    if i == self.data.join_index.unwrap() as usize {
                        Some(offsets.start)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>(),
            &mut optimistic_index,
        )
        .inspect_err(|err| tracing::error!("Error: {:#?}", err))
        .unwrap();

        tracing::debug!("Completed index: {:#?}", offsets);

        if optimistic_offset > 0 {
            tracing::trace!("Completing the index from the previous index.");
            let last_completed_index = {
                let mut guard = index_file.lock().await;
                // TODO: Fix this, if there is no previous index, just complete the current index.
                let mut buf = vec![0_u8; JoinedIndex::size_of(n_offsets)];
                guard
                    .read_at(optimistic_offset.saturating_sub(1), &mut buf)
                    .unwrap();
                buf
            };

            complete_from(&mut optimistic_index, &last_completed_index, n_offsets).unwrap();
        } else {
            tracing::trace!("Completing the index without a previous index..");

            JoinedIndex::set_completed(&mut optimistic_index);
        }

        let data = amalgamate_join(
            &optimistic_index,
            self.definition.clone(),
            self.data.partition.clone(),
            self.data.broker.clone(),
        )
        .await
        .unwrap();

        tracing::debug!("Got the data {:#?}", offsets);

        self.data.records_setter.set(vec![data.clone()]).await;

        tracing::trace!("Completed amalmagamation: {:#?}", data);

        let stream = String::from_utf8_lossy(self.definition.base.0.as_bytes()).to_string();

        let broker_guard = self.data.broker.write().await;

        let reference = broker_guard
            .put_data_store(stream.clone(), &self.data.partition, data)
            .await
            .unwrap();

        tracing::trace!("Created the Reference: {:#?}", reference);

        JoinedIndex::put_reference_static(reference, &mut optimistic_index);

        self.optimistic_index = Some(optimistic_index);
        self.optimistic_offset = Some(optimistic_offset);

        Ok(())
    }
    pub async fn commit(&mut self) -> Result<(), HigginsError> {
        let stream = String::from_utf8_lossy(self.definition.base.0.as_bytes()).to_string();

        let mut index_file = {
            let mut broker = self.data.broker.write().await;
            let index_file: BrokerIndexFile = broker
                .get_index_file(stream.clone(), &self.data.partition)
                .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
            tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
            drop(broker);
            index_file
        };

        let mut index_file_guard = index_file.lock().await;

        tracing::info!(
            "Retrieved indexfile for stream {stream} and partition {:#?}",
            self.data.partition
        );

        match (
            self.optimistic_index.as_mut(),
            self.optimistic_offset.as_ref(),
        ) {
            (Some(mut optimistic_index), Some(optimistic_offset)) => {
                index_file_guard
                    .try_range_put_at(
                        optimistic_offset.clone()..optimistic_offset.saturating_add(1),
                        &mut optimistic_index,
                    )
                    .inspect_err(|err| {
                        tracing::error!("{:#?}", err);
                    })
                    .unwrap();
                self.data
                    .offsets_setter
                    .set(
                        optimistic_offset.clone() as u64
                            ..optimistic_offset.saturating_add(1) as u64,
                    )
                    .await;

                tracing::debug!("Completed join. Length: {:#?}", index_file_guard.len());
            }
            _ => {
                tracing::error!("Attempted to place optimistic index without having one.");
            }
        }

        Ok(())
    }
}

pub async fn create_joined_stream_from_definition(
    definition: JoinDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    tracing::trace!(
        "[JOIN] Setting up Join Operator for definition: {:#?}",
        definition.base.0
    );

    // We create the resultant stream that data is zipped into.
    {
        let join_definition_schema_key = definition.clone().base.1.schema;

        let schema = broker.get_schema(&join_definition_schema_key).unwrap();

        // Create the actual derived stream.
        broker.create_stream(definition.base.0.as_bytes(), schema.clone());

        tracing::trace!("[JOIN] Successfully created the stream definition inside of the broker.");
    };

    tracing::trace!("[JOIN] Successfully created the join stream.");

    // We collect the results of each derivative stream into a channel, with which we
    // iterate over and push onto the resultant stream.

    for (i, join_stream) in definition.joins.iter().enumerate() {
        let join_stream = join_stream.clone();
        let broker_ref = broker_ref.clone();
        let definition = definition.clone();
        let stream_name = StreamName::from(definition.base.0.clone());

        let _handle = broker.task_handler.spawn(
            &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
            async move {
                // Create a subscription on each derivative
                let (client_id, condvar, subscription) = {
                    let mut broker = broker_ref.write().await;
                    let client_id = broker.clients.insert(ClientRef::NoOp).unwrap();
                    let left_subscription =
                        broker.create_subscription(join_stream.stream.0.as_bytes());
                    let stream = join_stream.stream.clone();
                    let (left_notify, left_subscription) = broker
                        .get_subscription_by_key(stream.0.as_bytes(), &left_subscription)
                        .ok_or(HigginsError::SubscriptionRetrievalFailed)
                        .unwrap();

                    tracing::trace!("[FIRST HANDLE] We are dropping the broker. ");
                    drop(broker); // Explicitly drop the lock.

                    (client_id, left_notify, left_subscription)
                };

                loop {
                    let offsets =
                        eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                            .await
                            .unwrap();

                    todo!();

                    // for (partition, offsets) in offsets.iter() {
                    //     let mut operation = JoinOperation {
                    //         stream: stream_name.clone(),
                    //         index: i.clone() as u64,
                    //         broker: broker_ref.clone(),
                    //         definition: definition.clone(),
                    //         partition: partition.clone(),
                    //         offsets: offsets.clone(),
                    //         // subscription: subscription.clone(),
                    //         optimistic_index: None,
                    //         optimistic_offset: None,
                    //     };

                    //     operation.init().await.unwrap();
                    //     operation.prepare().await.unwrap();
                    //     operation.commit().await.unwrap();
                    // }

                    tracing::trace!("Retrieved offsets {:#?} from {client_id}.", offsets);
                }
            },
        );
    }

    // // Handle the collection of indexes into the index file.
    // let _collection_handle = broker.task_handler.spawn(
    //     &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
    //     async move {
    //         while let Some((index, partition_offset_vec)) = derivative_channel_rx.recv().await {
    //             for (partition, offset) in partition_offset_vec {
    //                 let mut operation = JoinOperation {
    //                     index: index.clone() as u64,
    //                     broker: broker_ref.clone(),
    //                     definition: definition.clone(),
    //                     partition: partition.clone(),
    //                     offsets: offset..offset,
    //                     // subscription: subscription.clone(),
    //                     optimistic_index: None,
    //                     optimistic_offset: None,
    //                 };

    //                 operation.init().await.unwrap();
    //                 operation.prepare().await.unwrap();
    //                 operation.commit().await.unwrap();
    //             }
    //         }
    //     },
    // );

    Ok(())
}
