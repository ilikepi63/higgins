use super::utils::ColumnName;
use crate::{
    broker::Broker,
    derive::{
        joining::opts::eager_range_take_or_wait,
        utils::{get_partition_key_from_record_batch, put_default_index_at_range},
    },
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
                            .zip(offset.start..=offset.end)
                            .collect::<Vec<_>>()
                    };

                    tracing::trace!("[MAP] Retrieved records: {:#?}", records);

                    for (val, offset) in records {
                        tracing::trace!("[MAP] Received consume Response");

                        let stream_reader = read_arrow(&val);

                        let batches = stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                        tracing::trace!("[MAP] Iterating through batches..");

                        for record_batch in batches {
                            tracing::trace!("[MAP] Awaiting the broker lock..");

                            let mut broker_lock = broker_ref.write().await;

                            tracing::trace!("[MAP] We are reading the stream values in..");

                            for _ in 0..record_batch.num_rows() {
                                let partition_val = get_partition_key_from_record_batch(
                                    &record_batch,
                                    &ColumnName::from(&stream_def),
                                );

                                let engine = &broker_lock.wasm_engine;
                                let module = broker_lock
                                    .wasm_modules
                                    .iter()
                                    .find(|(n, _)| n == stream_def.function_name.as_ref().unwrap())
                                    .map(|(_, m)| m)
                                    .unwrap();

                                tracing::trace!("[MAP] We have fetched the module.");

                                let mapped_record_batch =
                                    run_map_function(&record_batch, engine, module);

                                tracing::trace!(
                                    "[MAP] Result from mapping: {:#?}",
                                    mapped_record_batch
                                );

                                tracing::trace!("[MAP] Producing to the stream..");

                                {
                                    let stream =
                                        String::from_utf8_lossy(stream_name.as_bytes()).to_string();
                                    let partition = &PartitionName::try_from(&partition_val[..])?;

                                    // CREATE REFERENCE
                                    let reference = broker_lock
                                        .put_data_store(
                                            stream.clone(),
                                            partition,
                                            mapped_record_batch,
                                        )
                                        .await?;

                                    // PUT INDEX FILE
                                    put_default_index_at_range(
                                        stream,
                                        partition,
                                        offset,
                                        &mut broker_lock,
                                        reference,
                                    )
                                    .await?;
                                }
                            }

                            drop(broker_lock);
                        }
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
