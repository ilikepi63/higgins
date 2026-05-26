use super::{joining::opts::eager_range_take_or_wait, utils::ColumnName};
use crate::{
    broker::Broker,
    derive::utils::get_partition_key_from_record_batch,
    error::HigginsError,
    functions::reduce::run_reduce_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::{PartitionName, read_arrow};
use std::sync::Arc;
use tokio::sync::RwLock;

pub async fn create_reduced_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    base_stream: (Key, StreamDefinition),
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    tracing::debug!(
        "Base stream: {}",
        String::from_utf8_lossy(base_stream.0.as_bytes())
    );
    tracing::debug!(
        "Derivative stream: {}",
        String::from_utf8_lossy(stream_name.as_bytes())
    );

    let (client_id, condvar, subscription) = {
        tracing::trace!("Attempting to input client_id.");

        let client_id = broker
            .clients
            .insert(crate::client::ClientRef::NoOp)
            .ok_or(HigginsError::Unknown)?;

        tracing::trace!("Retrieved client_id.");
        let subscription = broker.create_subscription(base_stream.0.as_bytes());

        tracing::trace!("Successfully created the subscription.");

        let (notify, subscription) = broker
            .get_subscription_by_key(base_stream.0.as_bytes(), &subscription)
            .ok_or(HigginsError::SubscriptionRetrievalFailed)?;

        tracing::trace!("Retrieved the notification for said subscription.");

        (client_id, notify, subscription)
    };

    tokio::task::spawn(async move {
        tracing::trace!("[MAP] We are being initiated");
        let result: Result<(), HigginsError> = async {
            loop {
                let offsets =
                    eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                        .await?;

                tracing::info!("[MAP] Retrieved offsets in map {:#?}", offsets);

                for (partition, offset) in offsets {
                    let records = {
                        let mut broker_guard = broker_ref.write().await;

                        broker_guard
                            .get_range(base_stream.0.as_bytes(), &partition, offset.clone())
                            .await?
                            .into_iter()
                            .filter_map(std::convert::identity)
                            .collect::<Vec<_>>()
                    };

                    tracing::trace!("[MAP] Retrieved records: {:#?}", records);


                    // In order to begin the reduction for these records, we need to
                    // retrieve the first record's previous record.
                    let mut prev_record = match offset.start {
                        0 => None,
                        _ => {

                            let mut broker_guard = broker_ref.write().await;
                            broker_guard
                            .get_at(
                                stream_name.as_bytes(),
                                &partition,
                                offset.start - 1,
                            )
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    "Failed to retrieve offset with error: {:#?}",
                                    err
                                )
                            })
                            .ok()
                            .flatten()
                            .map(|arrow_bytes| {
                                 tracing::trace!("bytes: {:#?}", arrow_bytes);
                                let mut batches = read_arrow(&arrow_bytes);
                                tracing::trace!("batches: {:#?}", batches);
                                batches.next().inspect(|val| {
                                    tracing::trace!("Correctly retrieved a value from the batches: {:#?}", val);
                                }).and_then(|result| result.ok())
                            })}
                    }
                    .flatten();


                    for val in offset.start..=offset.end {

                        tracing::trace!("[MAP] Awaiting the broker lock..");

                        let mut broker_lock = broker_ref.write().await;

                        tracing::trace!("[MAP] We are reading the stream values in..");

                        let current_value = broker_lock.get_at(base_stream.0.as_bytes(), &partition, val).await.unwrap().map(|data| {
                            let mut stream_reader = read_arrow(&data);

                            let batch = stream_reader.next().unwrap().unwrap();
                             // TODO: We need to ensure that these batches are merged if there are more than one.
                             batch
                        }).unwrap();

                        match prev_record.as_ref() {
                            Some(prev_record) => {
                                let module = broker_lock
                                    .functions
                                    .get_function(
                                        stream_def.function_name.as_ref().unwrap(),
                                    )
                                    .await;

                                tracing::trace!("Applying the function..");

                                let reduced_record_batch = run_reduce_function(
                                    &current_value,
                                    &prev_record,
                                    module,
                                );

                                tracing::trace!(
                                    "Reduced Record batch: {:#?}",
                                    reduced_record_batch
                                );

                                let result = broker_lock
                                    .produce(
                                        stream_name.as_bytes(),
                                        &partition,
                                        reduced_record_batch,
                                    )
                                    .await;

                                tracing::trace!(
                                    "Result from producing with a reduce: {:#?}",
                                    result
                                );
                            }
                            None => {
                                tracing::trace!("No previous index found..");

                                let _ = broker_lock
                                    .produce(
                                        stream_name.as_bytes(),
                                        &partition,
                                        current_value.clone(),
                                    )
                                    .await;
                            }
                        }

                        prev_record = Some(current_value);
                    }

                    let mut lock = subscription.write().await;

                    lock.acknowledge(&partition, &offset)?;

                    drop(lock);
                }
            }
        }
        .await;

        if let Err(err) = result {
            tracing::error!("An error occurred whilst mapping the stream: {:#?}", err);
        }
    });

    Ok(())
}
