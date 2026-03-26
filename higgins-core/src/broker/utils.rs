//! Arbitrary utilities for doing specific tasks given the broker reference.

use crate::broker::Broker;
use arrow::record_batch::RecordBatch;
use higgins_shared::PartitionName;
use higgins_shared::read_arrow;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Retrieve the arrow data at the specified index.
pub async fn get_arrow_data_at(
    stream: &[u8],
    partition: &PartitionName,
    offset: u64,
    broker: Arc<RwLock<Broker>>,
) -> RecordBatch {
    let broker_lock = broker.write().await;
    tracing::trace!("[FOURTH HANDLE] We have successfully locked the broker. ");

    let data = broker_lock
        .get_at(stream, partition, offset)
        .await
        .inspect_err(|err| {
            tracing::error!(
                "Retrieved an error when trying to unwrap this value: {:#?}",
                err
            )
        })
        .unwrap()
        .unwrap();

    tracing::trace!("[FOURTH HANDLE] We are dropping the broker. ");
    drop(broker_lock); // Explicitly drop the lock here.

    tracing::trace!(
        "[JOIN COMPLETION] Retrieved the data at for index {:#?}.",
        offset
    );

    // Retrieve the first record, as there should be only one record.

    read_arrow(&data).next().and_then(|r| r.ok()).unwrap()
}
