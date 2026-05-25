use crate::{
    broker::Broker,
    derive::{joining::opts::eager_range_take_or_wait, utils::get_partition_key_from_record_batch},
    error::HigginsError,
    functions::map::run_map_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::{PartitionName, read_arrow};
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn create_mapped_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    base_stream: (Key, StreamDefinition),
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    // Subscribe to both streams.
    // let left_subscription = broker.create_subscription(left.0.as_bytes());

    let (client_id, condvar, subscription) = {
        tracing::trace!("Attempting to input client_id.");

        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .unwrap();

        tracing::trace!("Retrieved client_id.");
        let subscription = broker.create_subscription(base_stream.0.as_bytes());

        tracing::trace!("Successfully created the subscription.");

        let (notify, subscription) = broker
            .get_subscription_by_key(base_stream.0.as_bytes(), &subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)
            .unwrap();

        tracing::trace!("Retrieved the notification for said subscription.");

        (client_id, notify, subscription)
    };

    // Left join runner for this subscription.
    tokio::task::spawn(async move {
        tracing::trace!("[DERIVED TAKE] We are being initiated");

        loop {
            let offsets =
                eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                    .await
                    .unwrap();

            let mut lock = subscription.write().await;

            let n = 10; // Generally, there is a set amount of n that we are interested in at a point.

            let offsets_result = lock.take_range(n);

            drop(lock);

            if let Ok(mut offsets) = offsets_result {
                //Get payloads from offsets.
                for (partition, offset) in offsets {
                    let mut broker_lock = broker_ref.write().await;

                    let mut records = vec![];

                    for offset in (offset.start..=offset.end) {
                        let val = val.unwrap();
                        records.push(val);
                    }

                    drop(broker_lock);

                    for val in records {
                        tracing::trace!("[DERIVED TAKE] Received consume Response");

                        let stream_reader = read_arrow(&val);

                        let batches = stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                        tracing::trace!("[DERIVED TAKE] Iterating through batches..");

                        for record_batch in batches {
                            tracing::trace!("[DERIVED TAKE] Awaiting the broker lock..");

                            let mut broker_lock = left_broker.write().await;

                            tracing::trace!("[DERIVED TAKE] We are reading the stream values in..");

                            for index in 0..record_batch.num_rows() {
                                let partition_val = get_partition_key_from_record_batch(
                                    &record_batch,
                                    index,
                                    String::from_utf8_lossy(left_stream_partition_key.as_bytes())
                                        .to_string()
                                        .as_str(),
                                );

                                let module = broker_lock
                                    .functions
                                    .get_function(stream_def.function_name.as_ref().unwrap())
                                    .await;

                                tracing::trace!("[DERIVED TAKE] We have fetched the module.");

                                let mapped_record_batch = run_map_function(&record_batch, module);

                                tracing::trace!(
                                    "[DERIVED TAKE] Result from mapping: {:#?}",
                                    mapped_record_batch
                                );

                                tracing::trace!("[DERIVED TAKE] Producing to the stream..");

                                let result = broker_lock
                                    .produce(
                                        stream_name.as_bytes(),
                                        &PartitionName::try_from(&partition_val[..]).unwrap(),
                                        mapped_record_batch,
                                    )
                                    .await;

                                tracing::trace!("Result from producing with a join: {:#?}", result);
                            }

                            drop(broker_lock);
                        }
                    }

                    let mut lock = left_subscription_ref.write().await;

                    lock.acknowledge(
                        &partition,
                        &std::ops::Range {
                            start: offset,
                            end: offset + 1,
                        },
                    )
                    .unwrap();

                    drop(lock);
                }
            } else {
                tracing::info!("Nothing to take, will just continue..");
            };
        }
    });

    Ok(())
}
