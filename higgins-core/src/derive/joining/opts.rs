use super::OuterSide;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::RwLock;

use crate::broker::BrokerIndexFile;
use crate::storage::index::{IndexFile, joined_index::JoinedIndex};
use crate::topography::config::schema_to_arrow_schema;
use crate::{broker::Broker, derive::joining::join::JoinDefinition};

static INITIAL_SIZE_OF_HANDLE_VEC: usize = 100;

macro_rules! get_sub {
    ($broker: ident, $left: ident, $sub: ident) => {
        $broker
            .get_subscription_by_key($left.0.inner(), &$sub)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)?
    };
}

/// This structure represents the core asynchronous functionality that is done when a
/// join operation is applied to an underlying stream.
pub struct JoinOperatorHandle {
    /// Describes whether or not this Join is still operating.
    is_working: AtomicBool,
    /// The handles that are currently spawned for this join.
    handles: Vec<tokio::runtime::Handle>,
}

pub async fn create_join_operator(
    definition: JoinDefinition,
    broker: Arc<RwLock<Broker>>,
) -> JoinOperatorHandle {
    // We leak this handle. This is primarily so that we can access this handle from multiple
    // tasks without having to use a form a reference checking.
    let operator: &'static JoinOperatorHandle = Box::leak(Box::new(JoinOperatorHandle {
        is_working: AtomicBool::new(true),
        handles: Vec::with_capacity(INITIAL_SIZE_OF_HANDLE_VEC),
    }));

    // Create the subscriptions inside of the broker for each join.
    match definition {
        JoinDefinition::Inner(inner) => {
            // we want one task to listen on the subscription,
            let left = inner.left_stream;
            let right = inner.right_stream;

            // We create the resultant stream that data is zipped into.
            {
                let mut broker = broker.write().await;

                let schema = schema_to_arrow_schema(&inner.stream.1.map.unwrap());

                // Create the actual derived stream.
                broker.create_stream(&inner.stream.0.0, Arc::new(schema));
            };

            // First task just adds in the indexing to the value.
            let left_broker = broker.clone();
            tokio::task::spawn(async move {
                let (client_id, left_notify, left_subscription) = {
                    let mut broker = left_broker.write().await;
                    let client_id = broker.clients.insert(super::ClientRef::NoOp);
                    let left_subscription = broker.create_subscription(left.0.inner());
                    let (left_notify, left_subscription) =
                        get_sub!(broker, left, left_subscription);

                    (client_id, left_notify, left_subscription)
                };

                loop {
                    let offsets = eager_take_from_subscription_or_wait(
                        left_subscription.clone(),
                        left_notify.clone(),
                        client_id,
                    )
                    .await;

                    match offsets {
                        Ok(offsets) => {
                            for (partition, offset) in offsets {
                                // Get the handle to the resultant streams indexing file.
                                let index_file = {
                                    let mut broker = left_broker.write().await;
                                    let index_file: BrokerIndexFile<JoinedIndex> = broker
                                        .get_index_file(
                                            String::from_utf8(inner.stream.0.0.clone()).unwrap(), // TODO: Enforce Strings for stream names.
                                            &partition,
                                        )
                                        .unwrap(); // This is safe because of the above. Likely should be unchecked (we create this stream at initialisation.)
                                    index_file
                                };

                                //

                                // save each index into the new joined stream index.
                                // Notify the other stream that there are updates to this stream.
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                "An error occurred when trying to retrieve offsets for  a joined stream: {:#?}. Closing this task.",
                                err
                            );
                            return Err::<(), HigginsError>(HigginsError::Unknown);
                        }
                    };
                }
            });

            let right_broker = broker.clone();
            tokio::task::spawn(async move {
                let (client_id, notify, subscription) = {
                    let mut broker = right_broker.write().await;
                    let client_id = broker.clients.insert(super::ClientRef::NoOp);
                    let subscription = broker.create_subscription(right.0.inner());
                    let (notify, subscription) = get_sub!(broker, right, subscription);

                    (client_id, notify, subscription)
                };

                loop {
                    let offsets = eager_take_from_subscription_or_wait(
                        subscription.clone(),
                        notify.clone(),
                        client_id,
                    )
                    .await;

                    match offsets {
                        Ok(offsets) => {
                            for offset in offsets {
                                // save each index into the new joined stream index.
                                // Notify the other stream that there are updates to this stream.
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                "An error occurred when trying to retrieve offsets for  a joined stream: {:#?}. Closing this task.",
                                err
                            );
                            return Err::<(), HigginsError>(HigginsError::Unknown);
                        }
                    };
                }
            });

            // and another to action on that subscription.
        }
        JoinDefinition::Outer(outer) => match outer.side {
            OuterSide::Left => {
                let left = outer.left_stream;

                // let left_subscription = broker.create_subscription(left.0.inner());

                todo!();
            }
            OuterSide::Right => {
                let right = outer.right_stream;
                // let subscription = broker.create_subscription(right.0.inner());

                todo!();
            }
        },
        JoinDefinition::Full(full) => {
            let left = full.first_stream;
            let right = full.second_stream;
            todo!()
            // let left_subscription = broker.create_subscription(left.0.inner());
            // let right_subscription = broker.create_subscription(right.0.inner());
        }
    };

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
