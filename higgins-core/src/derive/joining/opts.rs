use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::RwLock;

use super::subscription::start_join_subscription_task;
use crate::broker::BrokerIndexFile;
use crate::broker::utils::get_arrow_data_at;
use crate::derive::joining::completion::complete_from;
use crate::storage::dereference::Reference;
use crate::storage::index::joined_index::JoinedIndex;
use crate::task::SpawnTaskConfig;
use crate::utils::epoch;
use crate::{broker::Broker, derive::joining::join::JoinDefinition};
use higgins_shared::PartitionName;

/// This structure represents the core asynchronous functionality that is done when a
/// join operation is applied to an underlying stream.
#[allow(unused)]
pub struct JoinOperatorHandle {
    /// Describes whether or not this Join is still operating.
    #[allow(unused)]
    is_working: AtomicBool,
    /// The handles that are currently spawned for this join.
    handles: Vec<tokio::task::JoinHandle<()>>,
}

pub async fn create_join_operator(
    definition: JoinDefinition,
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) {
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
}

use crate::{error::HigginsError, subscription::Subscription};

static N: u64 = 10;

/// Function that takes an amount from a subscription, otherwise awaits a notifier
/// for the subscription for some of the given amount.
pub async fn eager_take_from_subscription_or_wait(
    subscription: Arc<RwLock<Subscription>>,
    notify: Arc<tokio::sync::Notify>,
    client_id: u64,
) -> Result<Vec<(PartitionName, u64)>, HigginsError> {
    let mut offsets = {
        tracing::trace!("[EAGER TAKE] Querying this again, taking {N} items.");
        let mut lock = subscription.write().await;
        lock.take(N)?
    };

    // If there are no given offsts, await the wakener then.
    match offsets.len() {
        0 => {
            tracing::trace!("[EAGER TAKE] Awaiting to be notified for produce..");
            notify.notified().await;
            tracing::trace!("[EAGER TAKE] We've been notified!");

            offsets = {
                tracing::trace!("[EAGER TAKE] Acquiring the lock.!");
                let mut lock = subscription.write().await;
                tracing::trace!(
                    "[EAGER TAKE] Acquired the lock, attempting to take {N} items from {client_id}!"
                );
                let taken = lock.take(N)?;
                tracing::trace!("[EAGER TAKE] Retrieved {:#?}", taken);

                // TODO: this likely should be removed and added once the join stream has been implemented.
                // Because we don't have shadow acknowledgements, we can't really support this right now.
                for (key, offset) in taken.iter() {
                    if let Err(err) = lock.acknowledge(
                        key,
                        &std::ops::Range {
                            start: *offset,
                            end: *offset,
                        },
                    ) {
                        tracing::error!("{:#?} when trying to acknowledge the partition.", err);
                    };
                }

                taken
            };

            Ok(offsets)
        }
        _ => Ok(offsets),
    }
}

pub async fn eager_range_take_or_wait(
    subscription: Arc<RwLock<Subscription>>,
    notify: Arc<tokio::sync::Notify>,
    client_id: u64,
) -> Result<Vec<(PartitionName, std::ops::Range<u64>)>, HigginsError> {
    let mut offsets = {
        tracing::trace!("[EAGER TAKE] Querying this again, taking {N} items.");
        let mut lock = subscription.write().await;
        lock.take_range(N)?
    };

    // If there are no given offsts, await the wakener then.
    match offsets.len() {
        0 => {
            tracing::trace!("[EAGER TAKE] Awaiting to be notified for produce..");
            notify.notified().await;
            tracing::trace!("[EAGER TAKE] We've been notified!");

            offsets = {
                tracing::trace!("[EAGER TAKE] Acquiring the lock.!");
                let mut lock = subscription.write().await;
                tracing::trace!(
                    "[EAGER TAKE] Acquired the lock, attempting to take {N} items from {client_id}!"
                );
                let taken = lock.take_range(N)?;
                tracing::trace!("[EAGER TAKE] Retrieved {:#?}", taken);

                // TODO: this likely should be removed and added once the join stream has been implemented.
                // Because we don't have shadow acknowledgements, we can't really support this right now.
                for (key, range) in taken.iter() {
                    if let Err(err) = lock.acknowledge(
                        key,
                        // The reason for this is that in acknowledgement, 0..0 represents the value 0, so the
                        // range itself is inclusive.
                        &std::ops::Range {
                            start: range.start,
                            end: range.end.saturating_sub(1),
                        },
                    ) {
                        tracing::error!("{:#?} when trying to acknowledge the partition.", err);
                    };
                }

                taken
            };

            Ok(offsets)
        }
        _ => Ok(offsets),
    }
}

pub async fn amalgamate_join(
    index: &[u8],
    definition: JoinDefinition,
    partition: PartitionName,
    broker: Arc<RwLock<Broker>>,
) -> Result<RecordBatch, HigginsError> {
    let index = JoinedIndex::of(index);
    let join_mapping = definition.clone().mapping;

    // Query the other offset data from this index_file.
    let derivative_data = futures::future::join_all((0..index.offset_len()).map(async |i| {
        let offset = index.get_offset(i);

        tracing::trace!(
            "[JOIN COMPLETION] Working on the offset for derivate data: {}",
            i,
        );

        tracing::trace!("[JOIN COMPLETION] Offset data: {:#?}", offset);

        match offset {
            Some(offset) => {
                tracing::trace!("[JOIN COMPLETION] Successfully retrieved the offset.");

                tracing::trace!(
                    "[FOURTH HANDLE] We are attempting to retrieve the lock on the broker. "
                );

                let arrow_data = get_arrow_data_at(
                    definition.joins.get(i).unwrap().stream.0.as_bytes(),
                    &partition,
                    offset,
                    broker.clone(),
                )
                .await;

                Some((i, arrow_data))
            }
            None => {
                tracing::trace!("[JOIN COMPLETION] Couldn't find data for indexed value");

                // This means that a derivative offset in the joined stream doesn't exist yet.
                None
            }
        }
    }))
    .await
    .iter()
    // Retrieve the stream names for the given indexes.
    .map(|data| {
        data.as_ref().map(|(index, data)| {
            let stream = definition.joins.get(*index).unwrap();
            (
                String::from_utf8(stream.stream.0.as_bytes().to_owned()).unwrap(),
                data.clone(),
            )
        })
    })
    .collect::<Vec<_>>();

    tracing::info!("We are amalgamating the derivative data now.");
    tracing::trace!("Derived Data: {:#?}", derivative_data);
    let resultant_record_batch = join_mapping.map_arrow(derivative_data).unwrap();

    Ok(resultant_record_batch)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_eager_range_take_sync() {
        let sub_path = "sub_take_eager_range";
        let notify = Arc::new(tokio::sync::Notify::new());
        let client_id = 1;
        let subscription = Arc::new(RwLock::new(Subscription::new(sub_path)));
        let partition = &PartitionName::try_from("1").unwrap();
        let mut guard = subscription.write().await;

        guard.add_partition(&partition, 0, 0).unwrap();

        drop(guard);

        let values = eager_range_take_or_wait(subscription.clone(), notify.clone(), client_id)
            .await
            .unwrap();

        for value in values {
            for record in value.1.start..=value.1.end {
                let mut guard = subscription.write().await;

                guard.acknowledge(&partition, &(record..record)).unwrap(); // acknowledging the entire range

                dbg!(&guard.partitions);
            }
        }
        let values = tokio::time::timeout(
            Duration::from_millis(100),
            eager_range_take_or_wait(subscription.clone(), notify.clone(), client_id),
        )
        .await;

        assert!(values.is_err()); // Timeout because there is no value.

        std::fs::remove_file(sub_path).unwrap();
    }
}
