use super::joining::opts::eager_range_take_or_wait;
use super::utils::put_default_index_at;
use crate::{
    broker::Broker,
    error::HigginsError,
    functions::reduce::run_reduce_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::read_arrow;
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
        tracing::trace!("[REDUCE] We are being initiated");
        let result: Result<(), HigginsError> = async {
            loop {
                let offsets =
                    eager_range_take_or_wait(subscription.clone(), condvar.clone(), client_id)
                        .await?;

                tracing::info!("[REDUCE] Retrieved offsets in REDUCE {:#?}", offsets);

                for (partition, offset) in offsets {
                    tracing::debug!("[REDUCE] Iterating offsets");
                    let records = {
                        tracing::debug!("[REDUCE] Awaiting broker lock");

                        let mut broker_guard = broker_ref.write().await;

                        tracing::debug!("[REDUCE] Acquired broker lock");

                       let range =  broker_guard
                            .get_range(base_stream.0.as_bytes(), &partition, offset.clone())
                            .await.inspect_err(|err| tracing::error!("Retrieved error on try: {:#?}", err))?;

                       tracing::debug!("Retrieved range: {:#?}", range);

                            range.into_iter()
                            .filter_map(std::convert::identity)
                            .zip(offset.start..=offset.end)
                            .collect::<Vec<_>>()
                    };

                    tracing::debug!("Retrieved {} records for reduction.", records.len());
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
                                offset.start - 1, // TODO: This should be impossible to fail as the invariant forces > 0, perhaps there is a better technique to be used here
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
                                 // tracing::trace!("bytes: {:#?}", arrow_bytes);
                                let mut batches = read_arrow(&arrow_bytes);
                                tracing::trace!("batches: {:#?}", batches);
                                batches.next().inspect(|val| {
                                    tracing::trace!("Correctly retrieved a value from the batches: {:#?}", val);
                                }).and_then(|result| result.ok())
                            })}
                    }
                    .flatten();


                    for (data, val) in records {

                        tracing::trace!("[REDUCE] Awaiting the broker lock..");

                        let mut broker_lock = broker_ref.write().await;

                        tracing::trace!("[REDUCE] We are reading the stream values in..");

                        let batch = {
                            let mut stream_reader = read_arrow(&data);

                            let batch = if let Some(batch) = stream_reader.next() {
                                batch.inspect_err(|err| tracing::error!("{:#?}",err)).unwrap()
                            }else {
                                tracing::error!("No batch returned for current value.   ");
                                panic!();
                            };
                             // TODO: We need to ensure that these batches are merged if there are more than one.
                             batch
                        };

                        tracing::debug!("Retrieved current value: {:#?}", batch);
                        tracing::debug!("Previous value: {:#?}", prev_record    );

                        match prev_record.as_ref() {
                            Some(prev_record) => {

                                tracing::info!("Using previous record..");

                                let module = broker_lock.wasm_modules.iter().find(|(n, _)| n == stream_def.function_name.as_ref().unwrap()).map(|(_, m)| m).unwrap() ;

                                tracing::trace!("Applying the function..");

                                let reduced_record_batch = run_reduce_function(
                                    &batch,
                                    &prev_record,
                                    &broker_lock.wasm_engine, module                                );

                                tracing::trace!(
                                    "Reduced Record batch: {:#?}",
                                    reduced_record_batch
                                );

                                {
                                    let stream =
                                        String::from_utf8_lossy(stream_name.as_bytes()).to_string();

                                    // CREATE REFERENCE
                                    let reference = broker_lock
                                        .put_data_store(
                                            stream.clone(),
                                           &partition,
                                            reduced_record_batch,
                                        )
                                        .await?;

                                    // PUT INDEX FILE
                                    //
                                    // TODO: Ranging would likely be better here.
                                    put_default_index_at(
                                        stream,
                                        &partition,
                                        val,
                                        &mut broker_lock,
                                        reference,
                                    )
                                    .await?;
                                }


                                let mut lock = subscription.write().await;

                                lock.acknowledge(&partition, &(val..val))?;

                                drop(lock);
                            }
                            None => {
                                tracing::trace!("No previous index found. Producing to stream {} key {} ", String::from_utf8_lossy(stream_name.as_bytes()), String::from_utf8_lossy(&partition.0));

                                let _ = broker_lock
                                    .produce(
                                        stream_name.as_bytes(),
                                        &partition,
                                        batch.clone(),
                                    )
                                    .await;

                                let mut lock = subscription.write().await;

                                lock.acknowledge(&partition, &(val..val))?;

                                drop(lock);
                            }
                        }

                        tracing::trace!("Setting previous record to current value.");
                        prev_record = Some(batch);
                    }


                }
            }
        }
        .await;

        if let Err(err) = result {
            tracing::error!("An error occurred whilst REDUCEping the stream: {:#?}", err);
        }
    });

    Ok(())
}
