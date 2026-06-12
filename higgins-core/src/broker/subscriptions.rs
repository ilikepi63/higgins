use super::Broker;

use bytes::BytesMut;
use higgins_codec::{Message, Record, TakeRecordsResponse, message::Type};
use higgins_shared::{HigginsError, PartitionName, StreamName, SubscriptionError};
use prost::Message as _;
use std::{
    collections::BTreeMap,
    sync::{Arc, atomic::Ordering},
};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::subscription::{Subscription, SubscriptionId};

type SubscriptionCollection = BTreeMap<Vec<u8>, (Arc<Notify>, Arc<RwLock<Subscription>>)>;

impl Broker {
    /// Retrieves the subscription for this specific key.
    pub fn get_subscription_by_key(
        &self,
        stream: &StreamName,
        subscription_id: &[u8],
    ) -> Option<(Arc<Notify>, Arc<RwLock<Subscription>>)> {
        self.subscriptions
            .get(stream)
            .and_then(|v| v.get(subscription_id))
            .cloned()
    }

    pub fn get_non_reactive_subscription_by_key(
        &self,
        stream: &StreamName,
        subscription_id: &SubscriptionId,
    ) -> Option<Arc<RwLock<Subscription>>> {
        self.non_reactive_subscriptions
            .get(stream)
            .and_then(|v| v.get(subscription_id))
            .cloned()
    }

    /// Retrieves the subscription for this specific key.
    pub fn get_subscriptions_for_stream(
        &self,
        stream: &StreamName,
    ) -> Option<SubscriptionCollection> {
        self.subscriptions.get(stream).cloned()
    }

    /// Acknowledge the given subscription's offsets.
    pub async fn acknowledge(
        &self,
        stream: StreamName,
        subscription_id: Vec<u8>,
        offsets: Vec<(PartitionName, std::ops::Range<u64>)>,
    ) -> Result<(String, Vec<(PartitionName, std::ops::Range<u64>)>), SubscriptionError> {
        tracing::info!("Retrieved acknowledgement, acknowledging..");

        let (_, subscription) = self
            .get_subscription_by_key(&stream, &subscription_id)
            .ok_or(SubscriptionError::SubscriptionNotFound)?;

        let mut subscription = subscription.write().await;

        tracing::info!("Retrieved the subscription: {:#?}", subscription);

        let failed_offsets = offsets.iter().fold(
            (String::new(), vec![]),
            |mut failed_offsets, (key, range)| {
                tracing::info!("Acknowledging key {:#?} with range {:#?}", key, range);

                if let Err(e) = subscription.acknowledge(key, range) {
                    failed_offsets.0 = e.to_string();
                    failed_offsets.1.push((key.to_owned(), range.clone()));
                }

                failed_offsets
            },
        );

        Ok(failed_offsets)
    }

    /// Upserts the given subscription into the underlying stream's subscription
    /// list. If the list of the stream does not yet exist, we create one.
    fn upsert_subscription(
        &mut self,
        stream: &StreamName,
        uuid: &[u8],
        value: (Arc<Notify>, Arc<RwLock<Subscription>>),
    ) -> Result<(), HigginsError> {
        match self.subscriptions.entry(stream.clone()) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let mut map = BTreeMap::new();
                map.insert(uuid.to_vec(), value);
                vacant_entry.insert(map);
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry.get_mut().insert(uuid.to_vec(), value);
            }
        }

        Ok(())
    }

    pub fn create_subscription(&mut self, stream: &StreamName) -> Result<Vec<u8>, HigginsError> {
        let uuid = Uuid::new_v4();

        let mut path = self.dir.clone();
        path.push("subscriptions"); // TODO: move to const.
        path.push(uuid.to_string());

        let subscription = Arc::new(RwLock::new(Subscription::new(&path)?));
        let notify = Arc::new(Notify::new());

        // How do we get the list of partitions for a stream?
        // We need to also be able to update the subscriptions for every stream.

        // TODO: This also needs to be done atomically.
        self.upsert_subscription(stream, uuid.as_bytes(), (notify, subscription))?;

        Ok(uuid.as_bytes().to_vec())
    }

    /// Creates a `non-reactive` subscription.
    ///
    /// Non-reactive subscriptions are subscriptions tha are not notified once they are
    /// produced to, but rather are used to keep track of updates to streams.
    pub fn create_non_reactive_subscription(
        &mut self,
        stream: &StreamName,
    ) -> Result<(SubscriptionId, Arc<RwLock<Subscription>>), HigginsError> {
        let uuid = Uuid::new_v4();

        let mut path = self.dir.clone();
        path.push("subscriptions"); // TODO: move to const.
        path.push(uuid.to_string());

        let subscription = Arc::new(RwLock::new(Subscription::new(&path)?));

        let sub = SubscriptionId::from(uuid.as_bytes().to_vec());

        // TODO: This also needs to be done atomically.
        match self.non_reactive_subscriptions.entry(stream.clone()) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let mut map = BTreeMap::new();
                map.insert(sub.clone(), subscription.clone());
                vacant_entry.insert(map);
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry
                    .get_mut()
                    .insert(sub.clone(), subscription.clone());
            }
        }

        Ok((sub, subscription))
    }

    /// A function to extract the current subscription indexes from the
    /// given subscription.
    pub async fn take_from_subscription(
        &mut self,
        client_id: u64,
        stream: &StreamName,
        subscription: &[u8],
        client_ref: tokio::sync::mpsc::Sender<BytesMut>,
        broker: Arc<RwLock<Broker>>,
        count: u64,
    ) -> Result<(), HigginsError> {
        tracing::trace!("Taking from subscription with count {count}");

        let (notify, subscription) = self
            .subscriptions
            .get_mut(stream)
            .and_then(|v| v.get_mut(subscription))
            .ok_or(HigginsError::SubscriptionForStreamDoesNotExist(
                stream.to_string(),
                subscription
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<String>(),
            ))?;

        tracing::trace!(
            "[TAKE] Managed to find the subscription for subscription ID: {:#?}",
            subscription
        );

        let task_subscription = subscription.clone();
        let task_stream_name = stream.clone();
        let task_notify = notify.clone();

        let mut subscription = subscription.write().await;

        // Client ID does not exist on this subscription, therefore we create it.
        if subscription
            .client_counts
            .binary_search_by(|(id, _)| client_id.cmp(id))
            .is_err()
        {
            tracing::trace!("[TAKE] No client count found for subscription. Creating one.");

            let broker = broker.clone();

            // The runner for this subscription.
            tokio::task::spawn(async move {
                loop {
                    let result: Result<(), HigginsError> = {
                        let mut lock = task_subscription.write().await;

                        let n = match lock
                            .client_counts
                            .binary_search_by(|(id, _)| client_id.cmp(id))
                            .map(|index| lock.client_counts.get(index))
                            .ok()
                            .flatten()
                        {
                            Some(c) => c.1.load(Ordering::Relaxed),
                            None => continue,
                        };

                        tracing::trace!("[TAKE] Taking the amount: {n}");

                        let offsets = lock.take(n);

                        tracing::trace!("{:#?}", &offsets);

                        if let Ok(offsets) = offsets.as_ref() {
                            lock.remove_client_count(&client_id, offsets.len() as u64);
                        }

                        drop(lock);

                        if let Ok(offsets) = offsets {
                            //Get payloads from offsets.
                            for (partition, offset) in offsets {
                                let consumption = {
                                    let mut broker_lock = broker.write().await;

                                    let mut results = vec![];

                                    let consumption = broker_lock
                                        .consume(&task_stream_name, &partition, offset, 50_000)
                                        .await;

                                    if let Ok(consumption) = consumption {
                                        for result in consumption {
                                            tracing::trace!(
                                                "RECEIVED DATA FOR SUBSCRIPTION: {:#?}",
                                                result
                                            );

                                            if let Ok(result) = result {
                                                results.push(OffsetPayload {
                                                    stream: task_stream_name.clone(),
                                                    key: partition.clone(),
                                                    offset,
                                                    bytes: result, // TODO: wrap this in a conversion function and filter out errors.
                                                });
                                            }
                                        }
                                    }

                                    results
                                };

                                if let Err(err) =
                                    write_offsets_to_client(consumption, client_ref.clone()).await
                                {
                                    tracing::error!(
                                        "Error occurred when trying to write offsets to client: {:#?}",
                                        err
                                    );
                                };
                            }
                        };

                        tracing::trace!("[TAKE] Awaiting the condvar.");

                        // await the condvar.
                        task_notify.notified().await;

                        tracing::trace!("[TAKE] Condvar has been notified, retrieving the amount.");

                        Ok(())
                    };

                    if let Err(e) = result {
                        tracing::error!("Error taking: {:#?}", e);
                    }
                }
            });
        }

        subscription.increment_amount_to_take(client_id, count);

        notify.notify_waiters();

        Ok(())
    }
}

/// Intermediary Struct that holds the payload and which stream/key/offset it came from.
pub struct OffsetPayload {
    pub stream: StreamName,
    pub key: PartitionName,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl From<OffsetPayload> for Record {
    fn from(val: OffsetPayload) -> Self {
        Record {
            data: val.bytes,
            stream: val.stream.as_bytes().to_vec(),
            partition: val.key.to_vec(),
            offset: val.offset,
        }
    }
}

pub async fn write_offsets_to_client(
    consumption: Vec<OffsetPayload>,
    client_ref: tokio::sync::mpsc::Sender<BytesMut>,
) -> Result<(), HigginsError> {
    for val in consumption {
        let resp = TakeRecordsResponse {
            records: vec![{ val.into() }],
        };

        let mut result = BytesMut::new();

        Message {
            r#type: Type::Takerecordsresponse as i32,
            take_records_response: Some(resp),
            ..Default::default()
        }
        .encode(&mut result)?;

        tracing::trace!("[TAKE] Writing the amount back to client.");

        client_ref.send(result).await.map_err(|err| {
            HigginsError::Arbitrary(format!("Failed to write offsets to client: {:#?}", err))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    #![allow(clippy::unwrap_used)]
    use super::*;
    use bytes::BytesMut;
    use higgins_shared::write_arrow;
    use prost::Message as ProstMessage;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::Sender;

    #[tokio::test]
    async fn write_offsets_sends_one_message_per_payload() -> Result<(), HigginsError> {
        let test_json = write_arrow(&arrow::array::record_batch!(
            ("a", Int32, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )?)?;

        let (tx, mut rx): (Sender<BytesMut>, _) = mpsc::channel(16);

        let payloads = vec![
            OffsetPayload {
                stream: StreamName::from("stream-a"),
                key: PartitionName::try_from("part-1")?,
                offset: 100,
                bytes: test_json.clone(),
            },
            OffsetPayload {
                stream: StreamName::from("stream-b"),
                key: PartitionName::try_from("part-2")?,
                offset: 200,
                bytes: test_json,
            },
        ];

        // Run the function under test
        write_offsets_to_client(payloads, tx).await.unwrap();

        // Collect sent messages
        let mut sent = vec![];
        while let Some(msg) = rx.recv().await {
            sent.push(msg);
        }

        assert_eq!(sent.len(), 2, "should send one message per payload");

        let msg1 = Message::decode(&*sent[0]).expect("valid protobuf");
        assert_eq!(msg1.r#type, Type::Takerecordsresponse as i32);
        let resp1 = msg1.take_records_response.expect("has response");
        assert_eq!(resp1.records.len(), 1);
        let rec1 = &resp1.records[0];
        assert_eq!(rec1.stream, b"stream-a".to_vec());
        assert_eq!(rec1.partition, PartitionName::try_from("part-1")?.to_vec());
        assert_eq!(rec1.offset, 100);

        // Decode second
        let msg2 = Message::decode(&*sent[1]).expect("valid protobuf");
        let resp2 = msg2.take_records_response.expect("has response");
        let rec2 = &resp2.records[0];
        assert_eq!(rec2.stream, b"stream-b".to_vec());
        assert_eq!(rec2.partition, PartitionName::try_from("part-2")?.to_vec());
        assert_eq!(rec2.offset, 200);

        Ok(())
    }
}
