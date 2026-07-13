use crate::broker::Broker;
use crate::broker::subscriptions::OffsetPayload;

use higgins_codec::message::Type;
use higgins_codec::{Message, TakeRecordsResponse};
use higgins_shared::{PartitionName, StreamName};

use higgins_shared::HigginsError;
use std::sync::atomic::Ordering;

pub async fn push_subscriptions(
    stream_name: StreamName,
    partition: PartitionName,
    offset: std::ops::Range<u64>,
    broker: &mut Broker,
) -> Result<(), HigginsError> {
    let subscription = broker.get_subscriptions_for_stream(&stream_name);

    // If there are subscriptions, produce to them.
    if let Some(subscriptions) = subscription {
        for (_, subscription) in subscriptions.values() {
            let mut subscription_guard = subscription.write().await;

            tracing::trace!(
                "[PRODUCE] Found a subscription for this produce request: {:#?}",
                subscription_guard
            );

            if subscription_guard
                .partitions
                .iter()
                .find(|sub_key| sub_key.partition_id == partition)
                .is_some()
            {
                subscription_guard.set_end(&partition, offset.end as u64)?;
            } else {
                subscription_guard.add_partition(&partition, 0, offset.end as u64)?;
            };

            tracing::trace!(
                "Set the end of this given subscription: {:#?}",
                subscription_guard
            );

            let client_ids = subscription_guard
                .client_counts
                .iter()
                .map(|(client_id, _)| *client_id)
                .collect::<Vec<_>>();

            tracing::trace!("Clients: {:#?}", client_ids);

            for client_id in client_ids {
                let client_ref = if let Some(r) = broker.get_client_by_id(client_id) {
                    r
                } else {
                    continue;
                };

                tracing::trace!("Retrieved client ref: {:#?}", client_ref);

                let n = match subscription_guard
                    .client_counts
                    .binary_search_by(|(id, _)| client_id.cmp(id))
                    .map(|index| subscription_guard.client_counts.get(index))
                    .ok()
                    .flatten()
                {
                    Some(c) => c.1.load(Ordering::Relaxed),
                    None => continue,
                };

                tracing::trace!("[TAKE] Taking the amount: {n}");

                let offsets = subscription_guard.take(n);

                tracing::trace!("{:#?}", &offsets);

                if let Ok(offsets) = offsets.as_ref() {
                    subscription_guard.remove_client_count(&client_id, offsets.len() as u64);

                    subscription_guard.mark_inflight(client_id, offsets);
                }

                if let Ok(offsets) = offsets {
                    //Get payloads from offsets.
                    for (partition, offset) in offsets {
                        let consumption = {
                            let mut results = vec![];

                            let consumption = broker
                                .consume(&stream_name, &partition, offset, 50_000)
                                .await;

                            if let Ok(consumption) = consumption {
                                for result in consumption.into_iter().flatten() {
                                    results.push(OffsetPayload {
                                        stream: stream_name.clone(),
                                        key: partition.clone(),
                                        offset,
                                        bytes: result, // TODO: wrap this in a conversion function and filter out errors.
                                    });
                                }
                            }

                            results
                        };

                        for val in consumption {
                            let resp = TakeRecordsResponse {
                                records: vec![{ val.into() }],
                            };

                            tracing::trace!("[TAKE] Writing the amount back to client.");

                            client_ref
                                .send(Message {
                                    r#type: Type::Takerecordsresponse as i32,
                                    take_records_response: Some(resp),
                                    ..Default::default()
                                })
                                .await
                                .map_err(|err| {
                                    HigginsError::Arbitrary(format!(
                                        "Failed to write offsets to client: {:#?}",
                                        err
                                    ))
                                })?;
                        }
                    }
                };
            }
        }
    }
    Ok(())
}
