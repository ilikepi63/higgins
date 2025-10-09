use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::RwLock;

use crate::broker::BrokerIndexFile;
use crate::storage::index::joined_index::JoinedIndex;
use crate::storage::index::{IndexFile, WrapBytes};
use crate::topography::config::schema_to_arrow_schema;
use crate::utils::epoch;
use crate::{broker::Broker, derive::joining::join::JoinDefinition};

static INITIAL_SIZE_OF_HANDLE_VEC: usize = 100;

macro_rules! get_sub {
    ($broker: ident, $left: ident, $sub: ident) => {
        $broker
            .get_subscription_by_key($left.0.inner(), &$sub)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
    };
}

/// This structure represents the core asynchronous functionality that is done when a
/// join operation is applied to an underlying stream.
pub struct JoinOperatorHandle {
    /// Describes whether or not this Join is still operating.
    is_working: AtomicBool,
    /// The handles that are currently spawned for this join.
    handles: Vec<tokio::task::JoinHandle<()>>,
}

pub async fn create_join_operator(
    definition: JoinDefinition,
    broker: Arc<RwLock<Broker>>,
) -> JoinOperatorHandle {
    // We leak this handle. This is primarily so that we can access this handle from multiple
    // tasks without having to use a form a reference checking.
    let operator: &'static mut JoinOperatorHandle = Box::leak(Box::new(JoinOperatorHandle {
        is_working: AtomicBool::new(true),
        handles: Vec::with_capacity(INITIAL_SIZE_OF_HANDLE_VEC),
    }));

    // We create the resultant stream that data is zipped into.
    {
        let mut broker = broker.write().await;

        let schema = schema_to_arrow_schema(&definition.base.1.map.unwrap());

        // Create the actual derived stream.
        broker.create_stream(&definition.base.0.0, Arc::new(schema));
    };

    // We collect the results of each derivative stream into a channel, with which we
    // iterate over and push onto the resultant stream.
    let (derivative_channel_tx, mut derivative_channel_rx) = tokio::sync::mpsc::channel(100);

    // For each stream in the definition, we create a separate task to iterate over them.
    for (i, join_stream) in definition.joins.iter().enumerate() {
        let join_stream = join_stream.clone();
        let broker = broker.clone();
        let derivative_channel_tx = derivative_channel_tx.clone();
        let handle = tokio::spawn(async move {
            // Create a subscription on each derivative
            let (client_id, condvar, subscription) = {
                let mut broker = broker.write().await;
                let client_id = broker.clients.insert(super::ClientRef::NoOp);
                let left_subscription = broker.create_subscription(join_stream.stream.0.inner());
                let stream = join_stream.stream.clone();
                let (left_notify, left_subscription) =
                    get_sub!(broker, stream, left_subscription).unwrap();

                (client_id, left_notify, left_subscription)
            };

            loop {
                let offsets = eager_take_from_subscription_or_wait(
                    subscription.clone(),
                    condvar.clone(),
                    client_id,
                )
                .await
                .unwrap();

                derivative_channel_tx
                    .send((i, offsets))
                    .await
                    .inspect_err(|err| {
                        tracing::error!(
                            "Error attempting to send to derivative channel: {:#?}",
                            err
                        );
                    })
                    .unwrap();
            }
        });

        // Add the handle to the operator here.
        operator.handles.push(handle);
    }

    // This task awaits all of the given derivative partitions and accumulates them into the
    // new joined stream.
    let stream = definition.base.0.0;
    let n_offsets = definition.joins.len();
    let collection_handle = tokio::spawn(async move {
        while let Some((index, partition_offset_vec)) = derivative_channel_rx.recv().await {
            // push this onto the resultant stream.
            for (partition, offset) in partition_offset_vec {
                // Retrieve the Index file, given the stream name and partition key.
                let mut index_file = {
                    let mut broker = broker.write().await;
                    let index_file: BrokerIndexFile<JoinedIndex> = broker
                        .get_index_file(
                            String::from_utf8(stream.clone()).unwrap(), // TODO: Enforce Strings for stream names.
                            &partition,
                        )
                        .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
                    index_file
                };

                // Read before write operation to append a joined index to the index file.
                {
                    let mut lock = index_file.lock().await;

                    let indexes = lock.as_indexes_mut();

                    let joined_offset = (indexes.count() + 1) as u64; // TODO: the fact that this is a u32 is a bit smelly.

                    let timestamp = epoch();

                    // Initialize zero byte array.
                    let mut joined_index_bytes =
                        Vec::<u8>::with_capacity(JoinedIndex::size_of(n_offsets))
                            .iter_mut()
                            .map(|_| 0)
                            .collect::<Vec<_>>();

                    let offsets = (0..(n_offsets - 1))
                        .map(|offset_val| {
                            if offset_val == index {
                                Some(offset)
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();

                    JoinedIndex::put(
                        joined_offset,
                        None,
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

                    lock.append(&joined_index_bytes)
                        .await
                        .inspect_err(|err| {
                            tracing::error!(
                                "Failed to append data to the index file with error: {:#?}",
                                err
                            );
                        })
                        .unwrap();

                    drop(lock);
                };

                // Task that checks if previous value is completed, if not stops.
                // If the previous task has been completed, query if the next index has been completed,
                // if not, then complete it.
                tokio::spawn(async move {
                    let index_file_view = index_file.view();
                    // Get the index preceding this one.
                    let previous_joined_index =
                        index_file_view.get((index - 1).try_into().unwrap());

                    let current_joined_index =
                        index_file_view.get(index.try_into().unwrap()).unwrap(); // Unwrap as this should always exist.

                    if let Some(joined_index) = previous_joined_index {
                        let mut inner_current_joined_index = current_joined_index.inner();

                        let mut owned_slice: Vec<u8> =
                            Vec::with_capacity(inner_current_joined_index.len());
                        // If the previous index has been `completed`. Then we run the copy
                        // over operation for this index and save it at the index offset.
                        if joined_index.completed() {
                            JoinedIndex::copy_filled_from(&mut owned_slice, joined_index.inner());
                            JoinedIndex::set_completed(&mut owned_slice);

                            index_file
                                .put_at(index.try_into().unwrap(), &mut owned_slice)
                                .inspect_err(|err| {
                                    tracing::error!("Error trying to put index at in an IndexFile for a JoinedIndex: {:#?}", err);
                                }).unwrap();

                            // So now that the current is completed, we can iterate over the rest
                            // and check if they need completing.
                            iterate_from_index_and_complete(
                                &mut index_file,
                                index.try_into().unwrap(),
                            )
                            .await;
                        }
                    }
                });
            }
        }
    });

    operator.handles.push(collection_handle);

    // // Create the subscriptions inside of the broker for each join.
    // match definition {
    //     JoinDefinition::Inner(inner) => {
    //         // we want one task to listen on the subscription,
    //         let left = inner.left_stream;
    //         let right = inner.right_stream;

    //         // We create the resultant stream that data is zipped into.
    //         {
    //             let mut broker = broker.write().await;

    //             let schema = schema_to_arrow_schema(&inner.stream.1.map.unwrap());

    //             // Create the actual derived stream.
    //             broker.create_stream(&inner.stream.0.0, Arc::new(schema));
    //         };

    //         // First task just adds in the indexing to the value.
    //         let left_broker = broker.clone();
    //         tokio::task::spawn(async move {
    //             loop {
    //                 match offsets {
    //                     Ok(offsets) => {
    //                         for (partition, offset) in offsets {
    //                             // Get the handle to the resultant streams indexing file.
    //                             let mut index_file = {
    //                                 let mut broker = left_broker.write().await;
    //                                 let index_file: BrokerIndexFile<ArchivedJoinedIndex> = broker
    //                                     .get_index_file(
    //                                         String::from_utf8(inner.stream.0.0.clone()).unwrap(), // TODO: Enforce Strings for stream names.
    //                                         &partition,
    //                                     )
    //                                     .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
    //                                 index_file
    //                             };

    //                             let joined_index = {
    //                                 let mut lock = index_file.lock().await;

    //                                 let indexes = lock.as_indexes_mut();

    //                                 let joined_offset = (indexes.count() + 1) as u64; // TODO: the fact that this is a u32 is a bit smelly.

    //                                 let timestamp = epoch();

    //                                 let joined_index = JoinedIndex::new_with_left_offset(
    //                                     joined_offset,
    //                                     offset,
    //                                     timestamp,
    //                                 );

    //                                 joined_index
    //                             };

    //                             match rkyv::to_bytes::<rkyv::rancor::Error>(&joined_index) {
    //                                 Ok(serialized) => {
    //                                     let mut lock = index_file.lock().await;

    //                                     match lock.append(&serialized).await {
    //                                         Ok(_) => {
    //                                             drop(lock);
    //                                             // Another task to reverse iterate through each remaining index looking for the alternate index.
    //                                             // This will then fill this index with that alternate index.
    //                                             tokio::spawn(async move {
    //                                                 let view = index_file.view();
    //                                                 for i in (view.count() - 1)..=0 {
    //                                                     let index = view.get(i);
    //                                                     match index {
    //                                                         Some(val) => {
    //                                                             if let Some(right_offset) =
    //                                                                 val.right_offset.as_ref()
    //                                                             {
    //                                                                 let join_index = JoinedIndex {
    //                                                                     offset: joined_index.offset,
    //                                                                     right_offset: Some(
    //                                                                         right_offset
    //                                                                             .to_native(),
    //                                                                     ),
    //                                                                     left_offset: joined_index
    //                                                                         .left_offset,
    //                                                                     object_key: joined_index
    //                                                                         .object_key,
    //                                                                     timestamp: joined_index
    //                                                                         .timestamp,
    //                                                                 };

    //                                                                 let joined_index_bytes =
    //                                                                     rkyv::to_bytes::<
    //                                                                         rkyv::rancor::Error,
    //                                                                     >(
    //                                                                         &joined_index
    //                                                                     )
    //                                                                     .unwrap();

    //                                                                 index_file
    //                                                                     .put_at(index, join_index);

    //                                                                 break;
    //                                                             }
    //                                                         }
    //                                                         None => {
    //                                                             tracing::trace!(
    //                                                                 "Index not found for stream, moving on."
    //                                                             );
    //                                                             break;
    //                                                         }
    //                                                     }
    //                                                 }
    //                                             });
    //                                         }
    //                                         Err(err) => {
    //                                             tracing::error!(
    //                                                 "Append returned an error: {:#?}",
    //                                                 err
    //                                             );
    //                                             panic!(
    //                                                 "This should never panic - append returned an error."
    //                                             );
    //                                         }
    //                                     }
    //                                 }
    //                                 Err(err) => {
    //                                     tracing::error!(
    //                                         "Conversion to Rkyv serialization format failed. This should never happen. Err: {:#?}",
    //                                         err
    //                                     );
    //                                     unimplemented!()
    //                                 }
    //                             };

    //                             // save each index into the new joined stream index.
    //                             // Notify the other stream that there are updates to this stream.
    //                         }
    //                     }
    //                     Err(err) => {
    //                         tracing::error!(
    //                             "An error occurred when trying to retrieve offsets for  a joined stream: {:#?}. Closing this task.",
    //                             err
    //                         );
    //                         return Err::<(), HigginsError>(HigginsError::Unknown);
    //                     }
    //                 };
    //             }
    //         });

    //         // let right_broker = broker.clone();
    //         // tokio::task::spawn(async move {
    //         //     let (client_id, notify, subscription) = {
    //         //         let mut broker = right_broker.write().await;
    //         //         let client_id = broker.clients.insert(super::ClientRef::NoOp);
    //         //         let subscription = broker.create_subscription(right.0.inner());
    //         //         let (notify, subscription) = get_sub!(broker, right, subscription);

    //         //         (client_id, notify, subscription)
    //         //     };

    //         //     loop {
    //         //         let offsets = eager_take_from_subscription_or_wait(
    //         //             subscription.clone(),
    //         //             notify.clone(),
    //         //             client_id,
    //         //         )
    //         //         .await;

    //         //         match offsets {
    //         //             Ok(offsets) => {
    //         //                 for offset in offsets {
    //         //                     // save each index into the new joined stream index.
    //         //                     // Notify the other stream that there are updates to this stream.
    //         //                 }
    //         //             }
    //         //             Err(err) => {
    //         //                 tracing::error!(
    //         //                     "An error occurred when trying to retrieve offsets for  a joined stream: {:#?}. Closing this task.",
    //         //                     err
    //         //                 );
    //         //                 return Err::<(), HigginsError>(HigginsError::Unknown);
    //         //             }
    //         //         };
    //         //     }
    //         // });

    //         // // and another to action on that subscription.
    //     }
    //     JoinDefinition::Outer(outer) => match outer.side {
    //         OuterSide::Left => {
    //             let left = outer.left_stream;

    //             // let left_subscription = broker.create_subscription(left.0.inner());

    //             todo!();
    //         }
    //         OuterSide::Right => {
    //             let right = outer.right_stream;
    //             // let subscription = broker.create_subscription(right.0.inner());

    //             todo!();
    //         }
    //     },
    //     JoinDefinition::Full(full) => {
    //         let left = full.first_stream;
    //         let right = full.second_stream;
    //         todo!()
    //         // let left_subscription = broker.create_subscription(left.0.inner());
    //         // let right_subscription = broker.create_subscription(right.0.inner());
    //     }
    // };

    // Second task updates that value with the previous other streams' value.
    // Third task generates the data underlying both of them.

    //     //Get payloads from offsets.
    //     for (partition, offset) in offsets {
    //         let broker_lock = left_broker.read().await;

    //         tracing::trace!(
    //             "Reading from stream: {:#?}",
    //             String::from_utf8(left_stream_name.clone())
    //         );

    //         let mut consumption = broker_lock
    //             .consume(&left_stream_name, &partition, offset, 50_000)
    //             .await;

    //         drop(broker_lock);

    //         while let Some(val) = consumption.recv().await {
    //             tracing::trace!("[DERIVED TAKE] Received consume Response {:#?}.", val);

    //             for consume_batch in val.batches.iter() {
    //                 let stream_reader = read_arrow(&consume_batch.data);

    //                 let batches =
    //                     stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

    //                 for record_batch in batches {
    //                     let mut broker_lock = left_broker.write().await;

    //                     let epoch_val = epoch();

    //                     for index in 0..record_batch.num_rows() {
    //                         let partition_val = get_partition_key_from_record_batch(
    //                             &record_batch,
    //                             index,
    //                             String::from_utf8_lossy(left_stream_partition_key.inner())
    //                                 .to_string()
    //                                 .as_str(),
    //                         );

    //                         let right_record = broker_lock
    //                             .get_by_timestamp(
    //                                 &right_stream_name,
    //                                 &partition_val,
    //                                 epoch_val,
    //                             )
    //                             .await
    //                             .and_then(|consume| {
    //                                 consume.batches.first().map(|batch| {
    //                                     let stream_reader = read_arrow(&batch.data);

    //                                     let batches = stream_reader
    //                                         .filter_map(|val| val.ok())
    //                                         .collect::<Vec<_>>();
    //                                     batches.into_iter().next()
    //                                 })
    //                             })
    //                             .flatten();

    //                         tracing::trace!(
    //                             "[DERIVED TAKE] Right record: {:#?}",
    //                             right_record
    //                         );

    //                         let new_record_batch = match &join_type {
    //                             Join::Full(_) => todo!(),
    //                             Join::Inner(_) => {
    //                                 match (Some(record_batch.clone()), right_record) {
    //                                     (Some(left), Some(right)) => values_to_batches(
    //                                         &join_type,
    //                                         Some(left),
    //                                         Some(right),
    //                                         String::from_utf8(left_stream_name.clone())
    //                                             .unwrap(),
    //                                         String::from_utf8(right_stream_name.clone())
    //                                             .unwrap(),
    //                                         stream_def.map.clone().unwrap(),
    //                                     ),
    //                                     _ => None,
    //                                 }
    //                             }
    //                             Join::LeftOuter(_) => todo!(),
    //                             Join::RightOuter(_) => todo!(),
    //                         };

    //                         tracing::trace!(
    //                             "Managed to write to partition: {:#?}",
    //                             stream_def.partition_key
    //                         );

    //                         if let Some(new_record_batch) = new_record_batch {
    //                             let result = broker_lock
    //                                 .produce(
    //                                     stream_name.inner(),
    //                                     &partition_val,
    //                                     new_record_batch,
    //                                 )
    //                                 .await;

    //                             tracing::trace!(
    //                                 "Result from producing with a join: {:#?}",
    //                                 result
    //                             );
    //                         }
    //                     }

    //                     drop(broker_lock);
    //                 }
    //             }
    //         }

    //         let lock = left_subscription_ref.write().await;

    //         lock.acknowledge(&partition, offset).unwrap();

    //         drop(lock);
    //     }
    // } else {
    //     tracing::info!("Nothing to take, will just continue..");
    // };

    todo!()
}

use crate::{error::HigginsError, subscription::Subscription};

/// This function iterates over the next index, using the current index to complete it.
pub async fn iterate_from_index_and_complete(
    index_file: &mut BrokerIndexFile<JoinedIndex<'_>>,
    index: u64,
) {
    let mut index = index;

    loop {
        let index_view = index_file.view();
        let next_joined_index = index_view.get((index + 1).try_into().unwrap()); // TODO: Remove this as we want offsets to be u64.

        // Return early if the next index is either completed, or the
        // index itself has yet to be written.
        if next_joined_index
            .as_ref()
            .is_none_or(|index| index.completed())
        {
            break;
        }

        let current_joined_index = index_view.get(index.try_into().unwrap()).unwrap(); // Asumption is that this is always Some(_).

        let mut copied_next: Vec<u8> = next_joined_index
            .unwrap()
            .inner()
            .iter()
            .map(|b| b.clone())
            .collect();

        JoinedIndex::copy_filled_from(&mut copied_next, &current_joined_index.inner());

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

        index += 1;
    }
}

static N: u64 = 10;

async fn eager_take_from_subscription_or_wait(
    subscription: Arc<RwLock<Subscription>>,
    notify: Arc<tokio::sync::Notify>,
    client_id: u64,
) -> Result<Vec<(Vec<u8>, u64)>, HigginsError> {
    let mut offsets = {
        let mut lock = subscription.write().await;
        lock.take(client_id, N)?
    };

    // If there are no given offsts, await the wakener then.
    match offsets.len() {
        0 => {
            tracing::trace!("[DERIVED TAKE] Awaiting to be notified for produce..");
            notify.notified().await;
            tracing::trace!("[DERIVED TAKE] We've been notified!");

            offsets = {
                let mut lock = subscription.write().await;
                lock.take(client_id, N)?
            };

            Ok(offsets)
        }
        _ => Ok(offsets),
    }
}
