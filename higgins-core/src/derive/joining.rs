//! Algorithms related to joining two streams.
//!
//! There exists a  set of types of stream joins, akin to SQL joins:
//! - Inner Join        -> emits a value for each corresponding index value for both underlying streams.
//! - Left Outer Join   -> emits a value for every value on the left side of the join, regardless of whether they have a matching key on the alternate stream.
//! - Right Outer Join  -> Similar to Left Outer Join, except on the right side of the join.
//! - Full Join         -> Similar to Right or Left Outer, except will emit for all values.

// TODO: How do we chain multiple streams together?.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use arrow::{
    array::{AsArray, Datum, RecordBatch},
    record_batch,
};
use tokio::sync::RwLock;

use crate::{
    broker::Broker,
    client::{self, ClientRef},
    error::HigginsError,
    storage::arrow_ipc::read_arrow,
    topography::{Join, Key, StreamDefinition},
    utils::epoch,
};

pub async fn create_derived_stream_from_definition(
    stream_def: StreamDefinition,
    left: (Key, StreamDefinition),
    right: (Key, StreamDefinition),
    join_type: Join,
    broker_ref: Arc<RwLock<Broker>>,
) -> Result<(), HigginsError> {
    let mut broker = broker_ref.write().await;
    let client_id = broker.clients.insert(ClientRef::NoOp);

    // Subscribe to both streams.
    let left_subscription = broker.create_subscription(left.0.inner());
    let right_subscription = broker.create_subscription(right.0.inner());

    let (left_notify, left_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();
    let (right_notify, right_subscription_ref) = broker
        .get_subscription_by_key(left.0.inner(), &left_subscription)
        .unwrap();

    let left_broker = broker_ref.clone();
    let left_stream_name = left.0.inner().to_owned();
    let left_stream_partition_key = left.1.partition_key;
    let right_stream_name = right.0.inner().to_owned();

    // Left join runner for this subscription.
    tokio::task::spawn(async move {
        loop {
            let mut lock = left_subscription_ref.write().await;

            let n = 10; // Generally, there is a set amount of n that we are interested in at a point.

            if let Ok(offsets) = lock.take(client_id, n) {
                // If there are no given offsts, await the wakener then.
                if offsets.len() < 1 {
                    left_notify.notified().await;
                }

                //Get payloads from offsets.
                for (partition, offset) in offsets {
                    let broker_lock = left_broker.read().await;

                    let mut consumption = broker_lock
                        .consume(&left_stream_name, &partition, offset, 50_000)
                        .await;

                    drop(broker_lock);

                    while let Some(val) = consumption.recv().await {
                        for consume_batch in val.batches.iter() {
                            let stream_reader = read_arrow(&consume_batch.data);

                            let batches =
                                stream_reader.filter_map(|val| val.ok()).collect::<Vec<_>>();

                            for record_batch in batches {
                                let broker_lock = left_broker.read().await;

                                let epoch_val = epoch();

                                for index in 0..record_batch.num_rows() {
                                    let partition_val = get_partition_key_from_record_batch(
                                        &record_batch,
                                        index,
                                        String::from_utf8_lossy(left_stream_partition_key.inner())
                                            .to_string()
                                            .as_str(),
                                    );

                                    let right_record = broker_lock
                                        .get_by_timestamp(
                                            &right_stream_name,
                                            partition_val,
                                            epoch_val,
                                        )
                                        .await
                                        .map(|consume| {
                                            consume.batches.first().map(|batch| {
                                                let stream_reader = read_arrow(&batch.data);

                                                let batches = stream_reader
                                                    .filter_map(|val| val.ok())
                                                    .collect::<Vec<_>>();
                                                batches.into_iter().nth(0)
                                            })
                                        })
                                        .flatten()
                                        .flatten();
                                }

                                drop(broker_lock);
                            }
                        }
                    }
                }
            };
        }
    });

    Ok(())
}

/// Function to effectively take the Join, the right and left values
/// and return the resultant record batch that needs to be written to the new
/// stream.
fn values_to_batches(
    join: Join,
    left: Option<RecordBatch>,
    right: Option<RecordBatch>,
) -> Option<RecordBatch> {
    None
}

fn get_partition_key_from_record_batch<'a>(
    batch: &'a RecordBatch,
    index: usize,
    col_name: &str,
) -> &'a [u8] {
    let schema_index = batch
        .schema()
        .index_of(col_name)
        .inspect(|err| {
            tracing::error!(
                "Unexpected error not being able to retrieve partition key by name: {:#?}",
                err
            );
        })
        .unwrap();

    let col = batch.column(schema_index);

    let value = col.as_string::<i64>().value(index); // TODO: What is offset size here?

    value.as_bytes()
}
