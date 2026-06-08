use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::broker::utils::get_arrow_data_at;
use crate::storage::index::joined_index::JoinedIndex;
use crate::{broker::Broker, derive::joining::join::JoinDefinition};
use higgins_shared::{HigginsError, PartitionName};

use crate::subscription::Subscription;

#[allow(unused)]
static N: u64 = 10;

/// Allowing unused as this might be the strategy to redo failed operations within the future.
#[allow(unused)]
pub async fn eager_range_take_or_wait(
    subscription: Arc<RwLock<Subscription>>,
    notify: Arc<tokio::sync::Notify>,
    client_id: u64,
) -> Result<Vec<(PartitionName, std::ops::Range<u64>)>, HigginsError> {
    let mut offsets = {
        tracing::trace!("[EAGER TAKE] Querying this again, taking {N} items.");
        let mut lock = subscription.write().await;
        lock.take_range(N)?
    };

    // If there are no given offsts, await the wakener then.
    match offsets.len() {
        0 => {
            tracing::trace!("[EAGER TAKE] Awaiting to be notified for produce..");
            notify.notified().await;
            tracing::trace!("[EAGER TAKE] We've been notified!");

            offsets = {
                tracing::trace!("[EAGER TAKE] Acquiring the lock.!");
                let mut lock = subscription.write().await;
                tracing::trace!(
                    "[EAGER TAKE] Acquired the lock, attempting to take {N} items from {client_id}!"
                );
                let taken = lock.take_range(N)?;
                tracing::trace!("[EAGER TAKE] Retrieved {:#?}", taken);

                // TODO: this likely should be removed and added once the join stream has been implemented.
                // Because we don't have shadow acknowledgements, we can't really support this right now.
                for (key, range) in taken.iter() {
                    if let Err(err) = lock.acknowledge(
                        key,
                        // The reason for this is that in acknowledgement, 0..0 represents the value 0, so the
                        // range itself is inclusive.
                        &std::ops::Range {
                            start: range.start,
                            end: range.end.saturating_sub(1),
                        },
                    ) {
                        tracing::error!("{:#?} when trying to acknowledge the partition.", err);
                    };
                }

                taken
            };

            Ok(offsets)
        }
        _ => Ok(offsets),
    }
}

pub async fn amalgamate_join(
    index: &[u8],
    definition: JoinDefinition,
    partition: PartitionName,
    broker: Arc<RwLock<Broker>>,
) -> Result<RecordBatch, HigginsError> {
    let index = JoinedIndex::of(index);
    let join_mapping = definition.clone().mapping;

    // Query the other offset data from this index_file.
    let derivative_data = futures::future::join_all((0..index.offset_len()).map(async |i| {
        let offset = index.get_offset(i);

        tracing::trace!(
            "[JOIN COMPLETION] Working on the offset for derivate data: {}",
            i,
        );

        tracing::trace!("[JOIN COMPLETION] Offset data: {:#?}", offset);

        match offset {
            Some(offset) => {
                tracing::trace!("[JOIN COMPLETION] Successfully retrieved the offset.");

                tracing::trace!(
                    "[FOURTH HANDLE] We are attempting to retrieve the lock on the broker. "
                );

                let arrow_data = get_arrow_data_at(
                    &definition.joins.get(i).unwrap().stream.0,
                    &partition,
                    offset,
                    broker.clone(),
                )
                .await;

                Some((i, arrow_data))
            }
            None => {
                tracing::trace!("[JOIN COMPLETION] Couldn't find data for indexed value");

                // This means that a derivative offset in the joined stream doesn't exist yet.
                None
            }
        }
    }))
    .await
    .iter()
    // Retrieve the stream names for the given indexes.
    .map(|data| {
        data.as_ref().map(|(index, data)| {
            let stream = definition.joins.get(*index).unwrap();
            (
                String::from_utf8(stream.stream.0.as_bytes().to_owned()).unwrap(),
                data.clone(),
            )
        })
    })
    .collect::<Vec<_>>();

    tracing::info!("We are amalgamating the derivative data now.");
    tracing::trace!("Derived Data: {:#?}", derivative_data);
    let resultant_record_batch = join_mapping.map_arrow(derivative_data).unwrap();

    Ok(resultant_record_batch)
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn test_eager_range_take_sync() {
        let sub_path = "sub_take_eager_range";
        let notify = Arc::new(tokio::sync::Notify::new());
        let client_id = 1;
        let subscription = Arc::new(RwLock::new(Subscription::new(sub_path)));
        let partition = &PartitionName::try_from("1").unwrap();
        let mut guard = subscription.write().await;

        guard.add_partition(&partition, 0, 0).unwrap();

        drop(guard);

        let values = eager_range_take_or_wait(subscription.clone(), notify.clone(), client_id)
            .await
            .unwrap();

        for value in values {
            for record in value.1.start..=value.1.end {
                let mut guard = subscription.write().await;

                guard.acknowledge(&partition, &(record..record)).unwrap(); // acknowledging the entire range

                dbg!(&guard.partitions);
            }
        }
        let values = tokio::time::timeout(
            Duration::from_millis(100),
            eager_range_take_or_wait(subscription.clone(), notify.clone(), client_id),
        )
        .await;

        assert!(values.is_err()); // Timeout because there is no value.

        std::fs::remove_file(sub_path).unwrap();
    }
}
