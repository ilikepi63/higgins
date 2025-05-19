use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use arrow::{array::RecordBatch, datatypes::Schema};
use riskless::{
    messages::{
        ConsumeRequest, ConsumeResponse, ProduceRequest, ProduceRequestCollection, ProduceResponse,
    },
    object_store::{self, ObjectStore},
};
use tokio::sync::RwLock;

use crate::{
    config::{Configuration, schema_to_arrow_schema},
    storage::{arrow_ipc::write_arrow, indexes::IndexDirectory},
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
    streams: BTreeMap<String, (Arc<Schema>, Sender, Receiver)>,
    object_store: Arc<dyn ObjectStore>,
    indexes: Arc<IndexDirectory>,
    pub flush_interval_in_ms: u64,
    pub segment_size_in_bytes: u64,
    collection: MutableCollection,
    flush_tx: tokio::sync::mpsc::Sender<()>,
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

                                res.respond(ProduceResponse {
                                    request_id: response.request_id,
                                    errors: response.errors,
                                })
                                .unwrap();
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
        }
    }

    /// Create a new instance from a given configuration.
    pub fn from_config(config: Configuration) -> Self {
        let mut broker = Broker::new();

        let schema = config
            .schema
            .iter()
            .map(|(name, schema)| (name.clone(), Arc::new(schema_to_arrow_schema(schema))))
            .collect::<BTreeMap<String, Arc<arrow::datatypes::Schema>>>();

        // Create the non-derived streams first.
        for (stream_name, topic_defintion) in config
            .topics
            .iter()
            .filter(|(_, def)| def.derived.is_none())
        {
            match &topic_defintion.derived {
                Some(_derived_from) => unreachable!(),
                None => {
                    // Create just normal schema.
                    let schema = schema.get(&topic_defintion.schema).unwrap_or_else(|| {
                        panic!("No Schema defined for key {}", topic_defintion.schema)
                    });

                    broker.create_stream(stream_name, schema.clone());
                }
            }
        }

        for (stream_name, topic_defintion) in config
            .topics
            .iter()
            .filter(|(_, def)| def.derived.is_some())
        {
            match &topic_defintion.derived {
                Some(derived_from) => {
                    // Create just normal schema.
                    let schema = schema.get(&topic_defintion.schema).unwrap_or_else(|| {
                        panic!("No Schema defined for key {}", topic_defintion.schema)
                    });

                    let topic_type = topic_defintion.fn_type.as_ref().unwrap();

                    match topic_type.as_ref() {
                        "reduce" => {
                            broker.reduce(
                                derived_from,
                                topic_defintion.schema.as_str(),
                                schema.clone(),
                                |_a, b| RecordBatch::new_empty(b.schema()),
                            );
                        }
                        _ => unimplemented!(),
                    }

                    broker.create_stream(stream_name, schema.clone());
                }
                None => unreachable!(),
            }
        }

        broker
    }

    pub fn get_stream(&self, stream_name: &str) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        self.streams.get(stream_name)
    }

    /// Produce a data set onto the named stream.
    pub async fn produce(&mut self, topic_name: &str, _partition: &str, record_batch: RecordBatch) {
        let data = write_arrow(&record_batch);

        let request = ProduceRequest {
            request_id: 1,
            topic: topic_name.to_string(),
            partition: 1,
            data,
        };

        let (request, response) = Request::<ProduceRequest, ProduceResponse>::new(request);

        // self.produce_request_tx.send(request).await.unwrap();

        let mut buffer_lock = self.collection.write().await;

        let _ = buffer_lock.0.collect(request.inner().clone());

        buffer_lock.1.push(request);

        // TODO: This is currently hardcoded to 50kb, but we possibly want to make
        if buffer_lock.0.size() > 50_000 {
            let _ = self.flush_tx.send(()).await;
        }

        drop(buffer_lock);

        let message = response.recv().await.unwrap();

        // TODO: fix this to actually return the error?
        if !message.errors.is_empty() {
            tracing::error!("Error when attempting to write out to producer.");
            return;
        }

        let (_stream_id, (_, tx, _rx)) = self
            .streams
            .iter()
            .find(|(id, _)| *id == topic_name)
            .unwrap();

        tx.send(record_batch).unwrap(); // This should change to a standard notification.
    }

    pub async fn consume(
        &self,
        topic: &str,
        _partition: &[u8],
        offset: u64,
        max_partition_fetch_bytes: u32,
    ) -> tokio::sync::mpsc::Receiver<ConsumeResponse> {
        let object_store = self.object_store.clone();
        let indexes = self.indexes.clone();

        riskless::consume(
            ConsumeRequest {
                topic: topic.to_string(),
                partition: 1,
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
    pub fn get_receiver(&self, stream_name: &str) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (_, tx, _rx))| tx.subscribe())
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &str, schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_string(), (schema, tx, rx));
    }

    /// Apply a reduction function to the stream.
    pub fn reduce(
        &mut self,
        stream_name: &str,
        reduced_stream_name: &str,
        reduced_stream_schema: Arc<Schema>,
        func: ReductionFn,
    ) {
        let current_rx = self.get_receiver(stream_name).unwrap();

        let (tx, rx) = tokio::sync::broadcast::channel(100);

        ReduceFunction::new(func, current_rx, tx.clone());

        self.streams.insert(
            reduced_stream_name.to_string(),
            (reduced_stream_schema, tx, rx),
        );
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
