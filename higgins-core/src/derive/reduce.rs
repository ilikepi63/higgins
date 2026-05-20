use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    broker::Broker,
    derive::utils::get_partition_key_from_record_batch,
    error::HigginsError,
    functions::reduce::run_reduce_function,
    topography::{Key, StreamDefinition},
};
use higgins_shared::{PartitionName, read_arrow};

pub async fn create_reduced_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    left: (Key, StreamDefinition),
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    // Subscribe to both streams.
    let left_subscription = broker.create_subscription(left.0.as_bytes());

    let (left_notify, left_subscription_ref) = broker
        .get_subscription_by_key(left.0.as_bytes(), &left_subscription)
        .unwrap();

    let left_broker = broker_ref.clone();
    let left_stream_name = left.0.as_bytes().to_owned();
    let left_stream_partition_key = left.1.partition_key;

    // Left join runner for this subscription.
    tokio::task::spawn(async move {
        tracing::trace!("[DERIVED TAKE] We are being initiated");

        loop {
            let mut lock = left_subscription_ref.write().await;

            let n = 10; // Generally, there is a set amount of n that we are interested in at a point.

            let offsets_result = lock.take(n);

            drop(lock);

            if let Ok(mut offsets) = offsets_result {
                // If there are no given offsts, await the wakener then.
                if offsets.is_empty() {
                    tracing::trace!("[DERIVED TAKE] Awaiting to be notified for produce..");
                    left_notify.notified().await;
                    tracing::trace!("[DERIVED TAKE] We've been notified!");

                    offsets = {
                        let mut lock = left_subscription_ref.write().await;
                        lock.take(n).unwrap()
                    };
                }

                // tracing::trace!(
                //     "[DERIVED TAKE] Received offsets {:#?}. Initiating Reduce.",
                //     offsets
                // );

                //Get payloads from offsets.
                for (partition, offset) in offsets {
                    let mut broker_lock = left_broker.write().await;

                    let consumption = broker_lock
                        .consume(&left_stream_name, &partition, offset, 50_000)
                        .await;

                    let mut records = vec![];

                    for val in consumption {
                        let val = val.unwrap();
                        records.push(val);
                    }

                    drop(broker_lock);

                    for val in records {
                        tracing::trace!("[DERIVED TAKE] Received consume Response",);

                        let stream_reader = read_arrow(&val);

                        let batches = stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                        for record_batch in batches {
                            let mut broker_lock = left_broker.write().await;

                            for index in 0..record_batch.num_rows() {
                                tracing::trace!("[DERIVED TAKE] Getting the partition key",);
                                let partition_val = get_partition_key_from_record_batch(
                                    &record_batch,
                                    index,
                                    String::from_utf8_lossy(left_stream_partition_key.as_bytes())
                                        .to_string()
                                        .as_str(),
                                );

                                tracing::trace!("[DERIVED TAKE] Getting the previous index..",);

                                tracing::trace!("[REDUCE] Check with current offset: {offset}");

                                let prev_record = match offset {
                                    0 => None,
                                    _ => broker_lock
                                        .get_at(
                                            stream_name.as_bytes(),
                                            &PartitionName::try_from(&partition_val[..]).unwrap(),
                                            offset - 1,
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
                                        }),
                                }
                                .flatten();

                                tracing::trace!(
                                    "[DERIVED TAKE] Making the change with prev record: {:#?}",
                                    prev_record
                                );

                                match prev_record {
                                    Some(prev_record) => {
                                        let module = broker_lock
                                            .functions
                                            .get_function(
                                                stream_def.function_name.as_ref().unwrap(),
                                            )
                                            .await;

                                        tracing::trace!("Applying the function..");

                                        let reduced_record_batch = run_reduce_function(
                                            &record_batch,
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
                                                &PartitionName::try_from(&partition_val[..])
                                                    .unwrap(),
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
                                                &PartitionName::try_from(&partition_val[..])
                                                    .unwrap(),
                                                record_batch.clone(),
                                            )
                                            .await;
                                    }
                                }
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
