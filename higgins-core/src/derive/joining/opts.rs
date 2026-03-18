use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::RwLock;

use super::subscription::start_join_subscription_task;
use crate::broker::BrokerIndexFile;
use crate::broker::utils::get_arrow_data_at;
use crate::derive::joining::completion::complete_joined_index_file;
use crate::storage::dereference::Reference;
use crate::storage::index::joined_index::JoinedIndex;
use crate::storage::index::{Index, IndexType};
use crate::task::SpawnTaskConfig;
use crate::utils::epoch;
use crate::{broker::Broker, derive::joining::join::JoinDefinition};
use higgins_shared::PartitionName;

macro_rules! get_sub {
    ($broker: ident, $left: ident, $sub: ident) => {
        $broker
            .get_subscription_by_key($left.0.as_bytes(), &$sub)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
    };
}

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
    let amalgamate_broker = broker_ref.clone();

    // We create the resultant stream that data is zipped into.
    {
        let join_definition_schema_key = definition.base.1.schema;

        let schema = broker.get_schema(&join_definition_schema_key).unwrap();

        // Create the actual derived stream.
        broker.create_stream(&definition.base.0.as_bytes(), schema.clone());

        tracing::trace!("[JOIN] Successfully created the stream definition inside of the broker.");
    };

    tracing::trace!("[JOIN] Successfully created the join stream.");

    // We collect the results of each derivative stream into a channel, with which we
    // iterate over and push onto the resultant stream.
    let mut derivative_channel_rx =
        start_join_subscription_task(broker, broker_ref.clone(), amalgamate_definition.clone());

    // This task awaits all of the given derivative partitions and accumulates them into the
    // new joined stream.
    let stream: Vec<u8> = definition.base.0.into();
    let n_offsets = definition.joins.len();

    // Handle the collection of indexes into the index file.
    let _collection_handle = broker.task_handler.spawn(
        &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
        async move {
            while let Some((index, partition_offset_vec)) = derivative_channel_rx.recv().await {
                for (partition, offset) in partition_offset_vec {
                    tracing::trace!(
                        "[JOIN COLLECTION] Opening index file with size: {}",
                        JoinedIndex::size_of(n_offsets)
                    );

                    // Retrieve the Index file, given the stream name and partition key.
                    let mut index_file = {
                        let mut broker = broker_ref.write().await;
                        let index_file: BrokerIndexFile = broker
                            .get_index_file(
                                String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
                                &partition,
                                JoinedIndex::size_of(n_offsets),
                            )
                            .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
                        tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
                        drop(broker);
                        index_file
                    };

                    tracing::trace!("[JOIN COLLECTION] Opened the index file for appending..",);

                    // Read before write operation to append a joined index to the index file.
                    append_index_to_stream(&mut index_file, n_offsets, index, offset).await;

                    complete_joined_index_file(&mut index_file, n_offsets);
                }
            }
        },
    );

    // let (completed_index_collector_tx, mut completed_index_collector_rx) =
    //     tokio::sync::mpsc::channel(100);
    // broker.task_handler.spawn(
    // &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
    // async move {
    //     let index_file_view = index_file.view();
    //     // Get the index preceding this one.
    //     let previous_joined_index = match index {
    //         0 => None,
    //         _ => index_file_view
    //             .get((index - 1).try_into().unwrap())
    //             .map(JoinedIndex::of),
    //     };

    //     tracing::trace!(
    //         "[JOIN COLLECTION] Retrieved a previous joined index: {:#?}",
    //         previous_joined_index,
    //     );

    //     let current_joined_index = index_file_view
    //         .get(index.try_into().unwrap())
    //         .map(JoinedIndex::of)
    //         .unwrap(); // Unwrap as this should always exist.

    //     if let Some(joined_index) = previous_joined_index {
    //         let inner_current_joined_index = current_joined_index.inner();

    //         tracing::trace!("Current Joined slice: {:#?}", inner_current_joined_index);

    //         // TODO: WE need to remember what the hell this owned_slice is here?
    //         let mut owned_slice: Vec<u8> = inner_current_joined_index.to_owned();

    //         // If the previous index has been `completed`. Then we run the copy
    //         // over operation for this index and save it at the index offset.
    //         if joined_index.completed() {
    //             tracing::trace!("Previous index completed, copying over..");

    //             JoinedIndex::copy_filled_from(&mut owned_slice, joined_index.inner());
    //             JoinedIndex::set_completed(&mut owned_slice);

    //             index_file
    //                 .put_at(index.try_into().unwrap(), &mut owned_slice)
    //                 .inspect_err(|err| {
    //                     tracing::error!("Error trying to put index at in an IndexFile for a JoinedIndex: {:#?}", err);
    //                 }).unwrap();

    //             completed_index_collector_tx
    //                 .send(index.try_into().unwrap())
    //                 .await
    //                 .unwrap();

    //             // So now that the current is completed, we can iterate over the rest
    //             // and check if they need completing.
    //             iterate_from_index_and_complete(
    //                 &mut index_file,
    //                 index.try_into().unwrap(),
    //                 completed_index_collector_tx.clone(),
    //             )
    //             .await;
    //         }
    //     } else {
    //         // If this is None, then this is the first index for this partition.
    //         let mut owned_slice = current_joined_index.inner().to_owned();

    //         tracing::trace!(
    //             "[JOIN COLLECTION] Given size for owned_slice: {}",
    //             owned_slice.len()
    //         );

    //         JoinedIndex::set_completed(&mut owned_slice);

    //         index_file
    //                 .put_at(index.try_into().unwrap(), &mut owned_slice)
    //                 .inspect_err(|err| {
    //                     tracing::error!("Error trying to put index at in an IndexFile for a JoinedIndex: {:#?}", err);
    //                 }).unwrap();

    //         tracing::trace!("[JOIN COLLECTION] Successfully saved the offset!",);

    //         completed_index_collector_tx
    //             .send(index.try_into().unwrap())
    //             .await
    //             .unwrap();

    //         // So now that the current is completed, we can iterate over the rest
    //         // and check if they need completing.
    //         iterate_from_index_and_complete(
    //             &mut index_file,
    //             index.try_into().unwrap(),
    //             completed_index_collector_tx,
    //         )
    //         .await;
    //     }
    // }).unwrap();

    // // This task awaits all of the given derivative partitions and accumulates them into the
    // // new joined stream.
    // let stream: Vec<u8> = definition.base.0.into();
    // let n_offsets = definition.joins.len();
    // let _collection_handle =             broker.task_handler.spawn(
    // &SpawnTaskConfig::new("joining", true), // TODO: we probably want this referencable from the stream.
    // async move {
    //     while let Some((index, partition_offset_vec)) = derivative_channel_rx.recv().await {
    //         tracing::trace!(
    //             "[JOIN COLLECTION] Received a notification for new offsets: {}",
    //             index
    //         );
    //         // push this onto the resultant stream.
    //         for (partition, offset) in partition_offset_vec {
    //             // Redefinition for tokio copies.
    //             let amalgamate_partition = partition.clone();
    //             let amalgamate_broker = amalgamate_broker.clone();

    //             tracing::trace!(
    //                 "[JOIN COLLECTION] Opening index file with size: {}",
    //                 JoinedIndex::size_of(n_offsets)
    //             );

    //             // Retrieve the Index file, given the stream name and partition key.
    //             let mut index_file = {
    //                 let mut broker = broker_ref.write().await;
    //                 let index_file: BrokerIndexFile = broker
    //                     .get_index_file(
    //                         String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
    //                         &partition,
    //                         JoinedIndex::size_of(n_offsets),
    //                     )
    //                     .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
    //                 tracing::trace!("[SECOND HANDLE] We are dropping the broker. ");
    //                 drop(broker);
    //                 index_file
    //             };

    //             tracing::trace!("[JOIN COLLECTION] Opened the index file for appending..",);

    //             // Read before write operation to append a joined index to the index file.
    //             append_index_to_stream(&mut index_file, n_offsets, index, offset).await;

    //             let (completed_index_collector_tx, mut completed_index_collector_rx) =
    //                 tokio::sync::mpsc::channel(100);

    //             // Task that checks if previous value is completed, if not stops.
    //             // If the previous task has been completed, query if the next index has been completed,
    //             // if not, then complete it.
    //             {
    //             let mut broker = broker_ref.write().await;

    //             // This is not the most ideal place to get another reference to this index_file.
    //             // Ideally we don't want multiple mutable references to the same broker index file,
    //             // and therefore we may need to create more restrictions on this.
    //             let mut index_file = {
    //                 let mut broker = broker_ref.write().await;
    //                 let index_file: BrokerIndexFile = broker
    //                     .get_index_file(
    //                         String::from_utf8(stream.to_owned()).unwrap(), // TODO: Enforce Strings for stream names.
    //                         &partition,
    //                         JoinedIndex::size_of(n_offsets),
    //                     )
    //                     .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
    //                 index_file
    //             };

    //             // NOTES
    //             // This is the task that awaits "completed" indexes.
    //             //
    //             // A "completed" index is on where the indexes of all the derivative indexes have been retrieved,
    //             // but the data from those derivative indexes has not been amalgamated yet into a data record.

    //             let amalgamate_definition: JoinDefinition = amalgamate_definition.clone();
    //             let amalgamate_broker = amalgamate_broker.clone();
    //             // Queries the derivative data of all relying join streams and amalgamates it into
    //             // one coherent data stream.
    //             {
    //             let mut  broker = broker_ref.write().await;
    //             broker.task_handler.spawn(
    //             &SpawnTaskConfig::new("joining", true) // TODO: we probably want this referencable from the stream.
    //             ,async move {
    //                 let stream = amalgamate_definition.clone();
    //                 let partition = amalgamate_partition;
    //                 let broker = amalgamate_broker.clone();

    //                 while let Some(completed_index) = completed_index_collector_rx.recv().await {
    //                     tracing::trace!(
    //                         "[JOIN COMPLETION] Retrieved a completed index, starting the join mapping. "
    //                     );

    //                     // Get the actual mapping.
    //                     let join_mapping = amalgamate_definition.clone().mapping;

    //                     // Retrieve a view into the joined index.
    //                     let index_view = index_file.view();
    //                     // Query the offset from this index_file,
    //                     let index = index_view
    //                         .get(completed_index.try_into().unwrap())
    //                         .map(JoinedIndex::of)
    //                         .unwrap();
    //                     tracing::trace!(
    //                         "[JOIN COMPLETION] Retrieved the index for the offset {}.",
    //                         completed_index
    //                     );

    //                     // Query the other offset data from this index_file.
    //                     let derivative_data = futures::future::join_all((0..index.offset_len()).map(async |i| {
    //                         let offset = index.get_offset(i);

    //                         tracing::trace!(
    //                             "[JOIN COMPLETION] Working on the offset for derivate data: {}", i,
    //                         );

    //                         tracing::trace!(
    //                             "[JOIN COMPLETION] Offset data: {:#?}", offset
    //                         );

    //                         match offset {
    //                             Some(offset) => {
    //                                 tracing::trace!(
    //                                     "[JOIN COMPLETION] Successfully retrieved the offset."
    //                                 );

    //                                 tracing::trace!("[FOURTH HANDLE] We are attempting to retrieve the lock on the broker. ");

    //                                 let arrow_data = get_arrow_data_at(
    //                                     stream.joins.get(i).unwrap().stream.0.as_bytes(),
    //                                     &partition,
    //                                     offset,
    //                                     broker.clone()
    //                                 ).await;

    //                                 Some((i, arrow_data))
    //                             }
    //                             None => {
    //                                 tracing::trace!(
    //                                     "[JOIN COMPLETION] Couldn't find data for indexed value"
    //                                 );

    //                                 // This means that a derivative offset in the joined stream doesn't exist yet.
    //                                 None
    //                             }
    //                         }
    //                     })).await.iter()
    //                     // Retrieve the stream names for the given indexes.
    //                     .map(|data| data.as_ref().map(|(index, data)| {
    //                         let stream = stream.joins.get(*index).unwrap();
    //                         (String::from_utf8(stream.stream.0.as_bytes().to_owned()).unwrap(), data.clone())
    //                     })).collect::<Vec<_>>();

    //                     tracing::info!("We are amalgamating the derivative data now.");
    //                     tracing::trace!("Derived Data: {:#?}", derivative_data);
    //                     let resultant_record_batch =
    //                         join_mapping.map_arrow(derivative_data).unwrap();

    //                     tracing::info!("Resultant Record batch: {:#?}", resultant_record_batch);

    //                     // How do we write this back to the index now??

    //                     {
    //                         tracing::trace!(
    //                             "Awaiting a write lock.."
    //                         );

    //                         let broker = amalgamate_broker.write().await;

    //                         let mut top_level_index = Index::of(index.inner(), IndexType::Join);

    //                         tracing::trace!(
    //                             "Putting at index: {:#?}", top_level_index
    //                         );
    //                         // Places the data at the reference.
    //                         let mut new_index = broker
    //                             .put_data(
    //                                 stream.base.0.clone().into(),
    //                                 // String::from_utf8(stream.base.0.as_bytes().to_owned()).unwrap(),
    //                                 &partition,
    //                                 &mut top_level_index,
    //                                 resultant_record_batch,
    //                             )
    //                             .await
    //                             .unwrap();

    //                         tracing::trace!(
    //                             "Completed putting at index {}: data: {:#?}", completed_index, new_index
    //                         );

    //                         index_file.put_at(completed_index, &mut new_index).unwrap();

    //                         // Now do the subscription updating..
    //                     }
    //                 }
    //             }).unwrap()};
    //         }
    //     }
    // });
}

use crate::{error::HigginsError, subscription::Subscription};

/// This function iterates over the next index, using the current index to complete it.
pub async fn iterate_from_index_and_complete(
    index_file: &mut BrokerIndexFile,
    index: u64,
    tx: tokio::sync::mpsc::Sender<u64>,
) {
    let mut index = index;

    loop {
        let index_view = index_file.view();
        let next_joined_index = index_view
            .get((index + 1).try_into().unwrap())
            .map(JoinedIndex::of); // TODO: Remove this as we want offsets to be u64.

        // Return early if the next index is either completed, or the
        // index itself has yet to be written.
        if next_joined_index
            .as_ref()
            .is_none_or(|index| index.completed())
        {
            break;
        }

        let current_joined_index = index_view
            .get(index.try_into().unwrap())
            .map(JoinedIndex::of)
            .unwrap(); // Asumption is that this is always Some(_).

        let mut copied_next: Vec<u8> = next_joined_index.unwrap().inner().to_vec();

        tracing::trace!("Copying over previously implemented indexes..");
        JoinedIndex::copy_filled_from(&mut copied_next, current_joined_index.inner());

        index_file
            .put_at((index + 1).try_into().unwrap(), &mut copied_next)
            .inspect_err(|err| {
                tracing::error!(
                    "Failed to put Index at offset: {}. Error: {:#?}",
                    index + 1,
                    err,
                )
            })
            .unwrap();

        tx.send(index + 1).await.unwrap();

        index += 1;
    }
}

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

pub async fn append_index_to_stream(
    index_file: &mut BrokerIndexFile,
    n_offsets: usize,
    index: usize,
    offset: u64,
) {
    tracing::trace!("[JOIN COLLECTION] Opened the index file for appending..",);

    let mut index_file = index_file.lock().await;

    // Read before write operation to append a joined index to the index file.
    {
        let indexes = index_file.as_view();

        let joined_offset = (indexes.count() + 1) as u64; // TODO: the fact that this is a u32 is a bit smelly.

        let timestamp = epoch();

        tracing::trace!("[JOIN COLLECTION] Timestamp for JoinedIndex: {timestamp}");

        // Initialize zero byte array.
        let mut joined_index_bytes = vec![0; JoinedIndex::size_of(n_offsets)];

        tracing::trace!("[JOIN COLLECTION] Offsets with size: {n_offsets}");

        let offsets = (0..(n_offsets))
            .map(|offset_val| {
                if offset_val == index {
                    Some(offset)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        tracing::trace!("[JOIN COLLECTION] Putting in offsets: {:#?}", offsets);

        JoinedIndex::put(
            joined_offset,
            Reference::Null,
            timestamp,
            &offsets,
            &mut joined_index_bytes,
        )
        .inspect_err(|err| {
            tracing::error!(
                "Failed to put Joined Index bytes into buffer with error: {:#?}",
                err,
            );
        })
        .unwrap();

        tracing::trace!(
            "Appending JoinedIndex: {:#?}",
            JoinedIndex::of(&joined_index_bytes)
        );

        index_file
            .append(&joined_index_bytes)
            .inspect_err(|err| {
                tracing::error!(
                    "Failed to append data to the index file with error: {:#?}",
                    err
                );
            })
            .unwrap();

        tracing::trace!("[JOIN COLLECTION] Able to append the offset!",);

        tracing::trace!("[THIRD HANDLE] We are dropping the broker. ");
    };
}
