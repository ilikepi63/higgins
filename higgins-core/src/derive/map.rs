use std::sync::Arc;
use arrow::record_batch;
use tokio::sync::RwLock;

use crate::{
    broker::Broker, client::ClientRef, derive::utils::get_partition_key_from_record_batch, error::HigginsError, functions::run_map_function, storage::arrow_ipc::read_arrow, topography::{Join, Key, StreamDefinition}, utils::epoch
};

pub async fn create_mapped_stream_from_definition(
    stream_name: Key,
    stream_def: StreamDefinition,
    left: (Key, StreamDefinition),
    broker: &mut Broker,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    let client_id = broker.clients.insert(ClientRef::NoOp);

    // Subscribe to both streams.
    let left_subscription = broker.create_subscription(left.0.inner());

    let (left_notify, left_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();
    let (_right_notify, _right_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();

    let left_broker = broker_ref.clone();
    let left_stream_name = left.0.inner().to_owned();
    let left_stream_partition_key = left.1.partition_key;

    // Left join runner for this subscription.
    tokio::task::spawn(async move {
        tracing::trace!("[DERIVED TAKE] We are being initiated");

        loop {
            let mut lock = left_subscription_ref.write().await;

            let n = 10; // Generally, there is a set amount of n that we are interested in at a point.

            let offsets_result = lock.take(client_id, n);

            drop(lock);

            if let Ok(mut offsets) = offsets_result {
                // If there are no given offsts, await the wakener then.
                if offsets.is_empty() {
                    tracing::trace!("[DERIVED TAKE] Awaiting to be notified for produce..");
                    left_notify.notified().await;
                    tracing::trace!("[DERIVED TAKE] We've been notified!");

                    offsets = {
                        let mut lock = left_subscription_ref.write().await;
                        lock.take(client_id, n).unwrap()
                    };
                }

                tracing::trace!(
                    "[DERIVED TAKE] Received offsets {:#?}. Initiating join.",
                    offsets
                );

                //Get payloads from offsets.
                for (partition, offset) in offsets {
                    let broker_lock = left_broker.read().await;

                    let mut consumption = broker_lock
                        .consume(&left_stream_name, &partition, offset, 50_000)
                        .await;

                    drop(broker_lock);

                    while let Some(val) = consumption.recv().await {
                        tracing::trace!("[DERIVED TAKE] Received consume Response {:#?}.", val);

                        for consume_batch in val.batches.iter() {
                            let stream_reader = read_arrow(&consume_batch.data);

                            let batches =
                                stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                            for record_batch in batches {
                                let mut broker_lock = left_broker.write().await;

                                let epoch_val = epoch();

                                for index in 0..record_batch.num_rows() {
                                    let partition_val = get_partition_key_from_record_batch(
                                        &record_batch,
                                        index,
                                        String::from_utf8_lossy(left_stream_partition_key.inner())
                                            .to_string()
                                            .as_str(),
                                    );

                                    /// TODO: Module retrieval logic.  
                                    let module = Vec::new();

                                    let mapped_record_batch = run_map_function(&record_batch, module);


                                    let result = broker_lock
                                        .produce(
                                            stream_name.inner(),
                                            &partition_val,
                                            mapped_record_batch,
                                        )
                                        .await;

                                    tracing::trace!(
                                        "Result from producing with a join: {:#?}",
                                        result
                                    );
                                }

                                drop(broker_lock);
                            }
                        }
                    }

                    let lock = left_subscription_ref.write().await;

                    lock.acknowledge(&partition, offset).unwrap();

                    drop(lock);
                }
            } else {
                tracing::info!("Nothing to take, will just continue..");
            };
        }
    });

    Ok(())
}
