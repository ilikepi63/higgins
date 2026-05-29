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
mod subscription;

use crate::broker::BrokerIndexFile;
use crate::derive::joining::completion::complete_from;
use crate::storage::dereference::Reference;
use crate::storage::index::joined_index::JoinedIndex;
use crate::task::SpawnTaskConfig;
use crate::utils::epoch;
use opts::amalgamate_join;
use subscription::start_join_subscription_task;

use crate::{
    broker::Broker, client::ClientRef, derive::joining::join::JoinDefinition, error::HigginsError,
};

pub async fn create_joined_stream_from_definition(
    definition: JoinDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    // Instantiate Operator on this definition.
    // let operator = create_join_operator(definition, broker, broker_ref).await;

    tracing::trace!(
        "[JOIN] Setting up Join Operator for definition: {:#?}",
        definition.base.0
    );

    // Redefined for movements.
    let amalgamate_definition = definition.clone();

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
    let mut derivative_channel_rx =
        start_join_subscription_task(broker, broker_ref.clone(), amalgamate_definition.clone());

    // This task awaits all of the given derivative partitions and accumulates them into the
    // new joined stream.
    let stream: Vec<u8> = definition.clone().base.0.into();
    let n_offsets = definition.joins.len();

    // Handle the collection of indexes into the index file.
    let _collection_handle = broker.task_handler.spawn(
        &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
        async move {
            while let Some((index, partition_offset_vec)) = derivative_channel_rx.recv().await {
                for (partition, offset) in partition_offset_vec {
                    // Retrieve the Index file, given the stream name and partition key.
                    let mut index_file = {
                        let mut broker = broker_ref.write().await;
                        let index_file: BrokerIndexFile = broker
                            .get_index_file(
                                String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
                                &partition,
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

                    JoinedIndex::put(
                        optimistic_offset as u64,
                        Reference::Null,
                        epoch(),
                        &(0..n_offsets)
                            .into_iter()
                            .map(|i| if i == index { Some(offset) } else { None })
                            .collect::<Vec<_>>(),
                        &mut optimistic_index,
                    )
                    .unwrap();

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

                        complete_from(&mut optimistic_index, &last_completed_index, n_offsets)
                            .unwrap();
                    } else {
                        tracing::trace!("Completing the index without a previous index..");

                        JoinedIndex::set_completed(&mut optimistic_index);
                    }

                    let data = amalgamate_join(
                        &optimistic_index,
                        definition.clone(),
                        partition.clone(),
                        broker_ref.clone(),
                    )
                    .await
                    .unwrap();

                    tracing::trace!("Completed amalmagamation: {:#?}", data);

                    let stream = String::from_utf8_lossy(definition.base.0.as_bytes()).to_string();

                    {
                        let broker_guard = broker_ref.write().await;

                        let reference = broker_guard
                            .put_data_store(stream.clone(), &partition, data)
                            .await
                            .unwrap();

                        tracing::trace!("Created the Reference: {:#?}", reference);

                        JoinedIndex::put_reference_static(reference, &mut optimistic_index);

                        let mut index_file_guard = index_file.lock().await;

                        tracing::info!(
                            "Retrieved indexfile for stream {stream} and partition {:#?}",
                            partition
                        );

                        index_file_guard
                            .try_range_put_at(
                                optimistic_offset..optimistic_offset.saturating_add(1),
                                &mut optimistic_index,
                            )
                            .inspect_err(|err| {
                                tracing::error!("{:#?}", err);
                            })
                            .unwrap();

                        tracing::debug!("Completed join. Length: {:#?}", index_file_guard.len());
                    }
                }
            }
        },
    );

    // Add the operator to a referencable struct.
    // broker.add_operator();

    Ok(())
}
