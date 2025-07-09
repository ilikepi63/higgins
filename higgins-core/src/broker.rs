use arrow::{array::RecordBatch, datatypes::Schema};
use bytes::BytesMut;
use higgins_codec::{Message, Record, TakeRecordsResponse, message::Type};
use prost::Message as _;
use riskless::{
    messages::{
        ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceRequestCollection, ProduceResponse,
    },
    object_store::{self, ObjectStore},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::sync::{Notify, RwLock};
use uuid::Uuid;

use crate::{
    client::ClientCollection,
    error::HigginsError,
    storage::{
        arrow_ipc::{read_arrow, write_arrow},
        indexes::IndexDirectory,
    },
    subscription::Subscription,
    topography::{Topography, apply_configuration_to_topography, config::from_yaml},
    utils::request_response::Request,
};

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, ProduceResponse>>,
    )>,
>;

/// This is a pretty naive implementation of what the broker might look like.
#[derive(Debug)]
pub struct Broker {
    streams: BTreeMap<Vec<u8>, (Arc<Schema>, Sender, Receiver)>,
    object_store: Arc<dyn ObjectStore>,
    indexes: Arc<IndexDirectory>,
    pub flush_interval_in_ms: u64,
    pub segment_size_in_bytes: u64,
    collection: MutableCollection,
    flush_tx: tokio::sync::mpsc::Sender<()>,

    // Subscriptions.
    subscriptions: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, (Arc<Notify>, Arc<RwLock<Subscription>>)>>,

    // Clients
    pub clients: ClientCollection,

    // Topography.
    topography: Topography,
}

impl Default for Broker {
    fn default() -> Self {
        Self::new()
    }
}

impl Broker {
    /// Creates a new instance of a Broker.
    pub fn new() -> Self {
        let index_dir = {
            let path = PathBuf::from("index");

            if !path.exists() {
                std::fs::create_dir(&path).unwrap();
                path
            } else {
                path
            }
        };

        let flush_interval_in_ms: u64 = 500;
        let segment_size_in_bytes: u64 = 50_000;

        let object_store =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix("data").unwrap());
        let object_store_ref = object_store.clone();

        let indexes = Arc::new(IndexDirectory::new(index_dir));
        let indexes_ref = indexes.clone();

        let buffer: MutableCollection =
            Arc::new(RwLock::new((ProduceRequestCollection::new(), vec![])));

        let cloned_buffer_ref = buffer.clone();

        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);

        // Flusher task.
        tokio::task::spawn(async move {
            loop {
                let indexes_ref = indexes_ref.clone();

                let timer = tokio::time::sleep(Duration::from_millis(flush_interval_in_ms)); // TODO: retrieve this from the configuration.

                // Await either a flush command or a timer expiry.
                tokio::select! {
                    _timer = timer => {    },
                    _recv = flush_rx.recv() => {}
                };

                let mut buffer_lock = buffer.write().await;

                if buffer_lock.0.size() > 0 {
                    let mut new_ref = ProduceRequestCollection::new();
                    let mut new_collection_vec = vec![];
                    std::mem::swap(&mut buffer_lock.0, &mut new_ref);
                    std::mem::swap(&mut buffer_lock.1, &mut new_collection_vec);

                    drop(buffer_lock); // Explicitly drop the lock.

                    match riskless::flush(new_ref, object_store_ref.clone(), indexes_ref).await {
                        Ok(responses) => {
                            let mut iter = new_collection_vec.into_iter();

                            // We need to fix riskless here.
                            for response in responses {
                                // TODO: O(n^2) here
                                let res = iter
                                    .find(|r| r.inner().request_id == response.request_id)
                                    .unwrap();

                                res.respond(response).unwrap();
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                "Error occurred when trying to flush buffer: {:#?}",
                                err
                            );
                        }
                    }
                }
            }
        });

        Self {
            streams: BTreeMap::new(),
            object_store,
            indexes,
            segment_size_in_bytes,
            flush_interval_in_ms,
            collection: cloned_buffer_ref,
            flush_tx,
            subscriptions: BTreeMap::new(),
            topography: Topography::new(),
            clients: ClientCollection::empty(),
        }
    }

    pub fn get_stream(&self, stream_name: &[u8]) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        self.streams.get(stream_name)
    }

    /// Produce a data set onto the named stream.
    pub async fn produce(
        &mut self,
        stream_name: &[u8],
        partition: &[u8],
        record_batch: RecordBatch,
    ) -> Result<ProduceResponse, HigginsError> {
        let data = write_arrow(&record_batch);

        let request = ProduceRequest {
            request_id: 1,
            topic: String::from_utf8(stream_name.to_vec()).unwrap(),
            partition: partition.to_vec(),
            data,
        };

        let (request, response) = Request::<ProduceRequest, ProduceResponse>::new(request);

        let mut buffer_lock = self.collection.write().await;

        let _ = buffer_lock.0.collect(request.inner().clone());

        buffer_lock.1.push(request);

        // TODO: This is currently hardcoded to 50kb, but we possibly want to make
        if buffer_lock.0.size() > 50_000 {
            let _ = self.flush_tx.send(()).await;
        }

        drop(buffer_lock);

        let response = response.recv().await.unwrap();

        // TODO: fix this to actually return the error?
        if !response.errors.is_empty() {
            tracing::error!(
                "Error when attempting to write out to producer. Request: {}",
                response.request_id
            );
        } else {
            // Watermark the subscription.
            let subscription = self.subscriptions.get(stream_name);

            if let Some(subscriptions) = subscription {

                tracing::trace!("[PRODUCE] Found a subscription for this produce request." );

                for (subscription_id, (notify, subscription)) in subscriptions {
                    let subscription = subscription.write().await;

                tracing::trace!("[PRODUCE] Notifying the subscrition." );


                    // Set the max offset of the subscription.
                    subscription.set_max_offset(partition, response.batch.offset)?;

                    // Notify the tasks awaiting this subscription.
                    notify.notify_waiters();
                                    tracing::trace!("[PRODUCE] Notified the subscrition." );


                    tracing::info!(
                        "Updated Max offset for subscription: {}, watermark: {}",
                        subscription_id
                            .iter()
                            .map(u8::to_string)
                            .collect::<String>(),
                        response.batch.offset
                    );
                }
            }
        }

        Ok(response)
    }

    pub async fn consume(
        &self,
        topic: &[u8],
        _partition: &[u8],
        offset: u64,
        max_partition_fetch_bytes: u32,
    ) -> tokio::sync::mpsc::Receiver<ConsumeResponse> {
        let object_store = self.object_store.clone();
        let indexes = self.indexes.clone();

        riskless::consume(
            ConsumeRequest {
                topic: String::from_utf8_lossy(topic).to_string(),
                partition: vec![],
                offset,
                max_partition_fetch_bytes,
            },
            object_store,
            indexes,
        )
        .await
        .unwrap()
    }

    /// Retrieve the receiver for a named stream.
    pub fn get_receiver(&self, stream_name: &[u8]) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (_, tx, _rx))| tx.subscribe())
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &[u8], schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_owned(), (schema, tx, rx));
    }

    /// Apply a reduction function to the stream.
    pub fn reduce(
        &mut self,
        stream_name: &[u8],
        reduced_stream_name: &[u8],
        reduced_stream_schema: Arc<Schema>,
        func: ReductionFn,
    ) {
        let current_rx = self.get_receiver(stream_name).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(100);

        ReduceFunction::new(func, current_rx, tx.clone());

        self.streams.insert(
            reduced_stream_name.to_owned(),
            (reduced_stream_schema, tx, rx),
        );
    }

    /// Creates a partition from a partition key.
    ///
    /// This is primarily just to notify a subcription for a stream that it has a new
    /// partition key if there doesn't exist one yet.
    ///
    /// TODO: This needs to be fault-tolerant.
    pub async fn create_partition(
        &mut self,
        stream_name: &[u8],
        partition_key: &[u8],
    ) -> Result<(), HigginsError> {
        if let Some(subs) = self.subscriptions.get_mut(stream_name) {
            for (_id, (_, sub)) in subs {
                let sub = sub.write().await;
                sub.add_partition(partition_key, None, None)?;
            }
        }

        Ok(())
    }

    pub fn create_subscription(
        &mut self,
        stream: &[u8],
        //         ConsumerOffsetType offset_type = 2;
        //   optional int64 timestamp = 3;
        //   optional int64 offset = 4;
    ) -> Vec<u8> {
        let uuid = Uuid::new_v4();

        let mut path = PathBuf::new();
        path.push("subscriptions"); // TODO: move to const.
        path.push(uuid.to_string());

        let subscription = Arc::new(RwLock::new(Subscription::new(&path)));
        let notify = Arc::new(Notify::new());

        // How do we get the list of partitions for a stream?
        // We need to also be able to update the subscriptions for every stream.

        // TODO: This also needs to be done atomically.
        match self.subscriptions.entry(stream.to_vec()) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let mut map = BTreeMap::new();
                map.insert(uuid.as_bytes().to_vec(), (notify, subscription));
                vacant_entry.insert(map);
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                occupied_entry
                    .get_mut()
                    .insert(uuid.as_bytes().to_vec(), (notify, subscription));
            }
        }

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
        tracing::trace!("[TAKE] Taking for subscription for client {client_id}, amount: {count}.");

        let (notify, subscription) = self
            .subscriptions
            .get_mut(stream)
            .map(|v| v.get_mut(subscription))
            .flatten()
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
        if let Err(_) = subscription
            .client_counts
            .binary_search_by(|(id, _)| client_id.cmp(id))
        {
            tracing::trace!("[TAKE] No client count found for subscription. Creating one.");

            let broker = broker.clone();

            // The runner for this subscription.
            tokio::task::spawn(async move {
                loop {
                    tracing::trace!("[TAKE] Awaiting the condvar.");

                    // await the condvar.
                    task_notify.notified().await;

                    tracing::trace!("[TAKE] Condvar has been notified, retrieving the amount.");

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

                                client_ref.send(result).await;
                            }
                        }
                    };
                }
            });
        }

        subscription.increment_amount_to_take(client_id, count);

        notify.notify_waiters();

        Ok(())
    }

    // Ideally what should happen here is that configurations get applied to topographies,
    // and then the state of the topography creates resources inside of the broker. However,
    // due to focus on naive implementations, we're going to just apply the configuration directly.
    pub fn apply_configuration(&mut self, config: &[u8]) -> Result<(), HigginsError> {
        let config = from_yaml(config);

        apply_configuration_to_topography(config, &mut self.topography);

        // We need to figure out a nice way to do state updates here.

        let streams_to_create = self
            .topography
            .streams
            .iter()
            .filter_map(|(stream_key, def)| {
                if let None = self.streams.get(stream_key.inner()) {
                    let schema = self.topography.schema.get(&def.schema).unwrap().clone();

                    return Some((stream_key.clone(), schema));
                }

                None
            })
            .collect::<Vec<_>>();

        for (key, schema) in streams_to_create {
            self.create_stream(key.inner(), schema);
        }

        // Right now, we don't actually have engineered ways of doing subscriptions.
        // // Subscription state update -> another nice way to do this.
        // let subscriptions_to_create = self.topography.subscriptions.iter().filter_map(|(key, sub_def)| {

        // });

        Ok(())
    }
}

pub trait ReductionFnTypeSig:
    Fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch + Send + 'static
{
}

type ReductionFn = fn(&Option<RecordBatch>, &RecordBatch) -> RecordBatch;

pub struct ReduceFunction;

impl ReduceFunction {
    /// Create a new instance of a reduction function.
    pub fn new(func: ReductionFn, mut rx: Receiver, tx: Sender) -> Self {
        tokio::spawn(async move {
            let last_value: Option<RecordBatch> = None;

            while let Ok(value) = rx.recv().await {
                let result = func(&last_value, &value);

                if let Err(e) = tx.send(result) {
                    tracing::error!("Error sending result: {:#?}", e);
                };
            }
        });

        Self
    }
}
