use super::Broker;

use arrow::{array::RecordBatch, datatypes::Schema};
use bytes::BytesMut;
use higgins_codec::{Message, Record, TakeRecordsResponse, message::Type};
use prost::Message as _;
use riskless::{
    batch_coordinator::FindBatchResponse,
    messages::{
        ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceRequestCollection, ProduceResponse,
    },
    object_store::{self, ObjectStore},
};
use std::{
    collections::BTreeMap,
    fs::create_dir,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::{
    broker::object_store::path::Path,
    derive::{
        joining::create_joined_stream_from_definition, map::create_mapped_stream_from_definition,
        reduce::create_reduced_stream_from_definition,
    },
    functions::collection::FunctionCollection,
    topography::FunctionType,
};
use crate::{
    client::ClientCollection,
    error::HigginsError,
    storage::{
        arrow_ipc::{read_arrow, write_arrow},
        indexes::IndexDirectory,
    },
    subscription::Subscription,
    topography::{Topography, apply_configuration_to_topography, config::from_toml},
    utils::request_response::Request,
};
use riskless::messages::ConsumeBatch;
use std::collections::HashSet;

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, ProduceResponse>>,
    )>,
>;

impl Broker {
    /// Retrieves the subscription for this specific key.
    pub fn get_subscription_by_key(
        &self,
        stream: &[u8],
        subscription_id: &[u8],
    ) -> Option<(Arc<Notify>, Arc<RwLock<Subscription>>)> {
        self.subscriptions
            .get(stream)
            .and_then(|v| v.get(subscription_id))
            .cloned()
    }

    /// Upserts the given subscription into the underlying stream's subscription
    /// list. If the list of the stream does not yet exist, we create one.
    fn upsert_subscription(
        &mut self,
        stream: &[u8],
        uuid: &[u8],
        value: (Arc<Notify>, Arc<RwLock<Subscription>>),
    ) -> Result<(), HigginsError> {
        match self.subscriptions.entry(stream.to_vec()) {
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

    pub fn create_subscription(&mut self, stream: &[u8]) -> Vec<u8> {
        let uuid = Uuid::new_v4();

        let mut path = self.dir.clone();
        path.push("subscriptions"); // TODO: move to const.
        path.push(uuid.to_string());

        let subscription = Arc::new(RwLock::new(Subscription::new(&path)));
        let notify = Arc::new(Notify::new());

        // How do we get the list of partitions for a stream?
        // We need to also be able to update the subscriptions for every stream.

        // TODO: This also needs to be done atomically.
        self.upsert_subscription(stream, uuid.as_bytes(), (notify, subscription))
            .unwrap();

        uuid.as_bytes().to_vec()
    }

    /// A function to extract the current subscription indexes from the
    /// given subscription.
    pub async fn take_from_subscription(
        &mut self,
        client_id: u64,
        stream: &[u8],
        subscription: &[u8],
        client_ref: tokio::sync::mpsc::Sender<BytesMut>,
        broker: Arc<RwLock<Broker>>,
        count: u64,
    ) -> Result<(), HigginsError> {
        let (notify, subscription) = self
            .subscriptions
            .get_mut(stream)
            .and_then(|v| v.get_mut(subscription))
            .ok_or(HigginsError::SubscriptionForStreamDoesNotExist(
                stream.iter().map(|v| v.to_string()).collect::<String>(),
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
        let task_client_id = client_id;
        let task_stream_name = stream.to_vec();
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
                    let mut lock = task_subscription.write().await;
                    let broker_lock = broker.read().await;

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

                    if let Ok(offsets) = lock.take(task_client_id, n) {
                        //Get payloads from offsets.
                        for (partition, offset) in offsets {
                            let mut consumption = broker_lock
                                .consume(&task_stream_name, &partition, offset, 50_000)
                                .await;

                            while let Some(val) = consumption.recv().await {
                                let resp = TakeRecordsResponse {
                                    records: val
                                        .batches
                                        .iter()
                                        .map(|batch| {
                                            let stream_reader = read_arrow(&batch.data);

                                            let batches = stream_reader
                                                .filter_map(|val| val.ok())
                                                .collect::<Vec<_>>();

                                            let batch_refs = batches.iter().collect::<Vec<_>>();

                                            // Infer the batches
                                            let buf = Vec::new();
                                            let mut writer =
                                                arrow_json::LineDelimitedWriter::new(buf);
                                            writer.write_batches(&batch_refs).unwrap();
                                            writer.finish().unwrap();

                                            // Get the underlying buffer back,
                                            let buf = writer.into_inner();

                                            Record {
                                                data: buf,
                                                stream: batch.topic.as_bytes().to_vec(),
                                                offset: batch.offset,
                                                partition: batch.partition.clone(),
                                            }
                                        })
                                        .collect::<Vec<_>>(),
                                };

                                let mut result = BytesMut::new();

                                Message {
                                    r#type: Type::Takerecordsresponse as i32,
                                    take_records_response: Some(resp),
                                    ..Default::default()
                                }
                                .encode(&mut result)
                                .unwrap();

                                tracing::trace!("[TAKE] Writing the amount back to client.");

                                client_ref.send(result).await.unwrap();
                            }
                        }
                    };

                    tracing::trace!("[TAKE] Awaiting the condvar.");

                    // await the condvar.
                    task_notify.notified().await;

                    tracing::trace!("[TAKE] Condvar has been notified, retrieving the amount.");
                }
            });
        }

        subscription.increment_amount_to_take(client_id, count);

        notify.notify_waiters();

        Ok(())
    }
}
