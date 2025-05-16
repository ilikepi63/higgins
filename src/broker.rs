use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arrow::{array::RecordBatch, datatypes::Schema};
use bytes::BytesMut;
use riskless::{
    messages::{ProduceRequest, ProduceRequestCollection, ProduceResponse},
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

/// This is a pretty naive implementation of what the broker might look like.
#[derive(Debug)]
pub struct Broker {
    streams: BTreeMap<String, (Arc<Schema>, Sender, Receiver)>,
    object_store: Arc<dyn ObjectStore>,
    indexes: Arc<IndexDirectory>,
    pub flush_interval_in_ms: u64,
    pub segment_size_in_bytes: u64,
    produce_request_tx: tokio::sync::mpsc::Sender<Request<ProduceRequest, ProduceResponse>>,
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

        let object_store = Arc::new(object_store::memory::InMemory::new());
        let object_store_ref = object_store.clone();

        let indexes = Arc::new(IndexDirectory::new(index_dir));
        let indexes_ref = indexes.clone();

        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Request<ProduceRequest, ProduceResponse>>(100);

        let buffer: Arc<RwLock<ProduceRequestCollection>> =
            Arc::new(RwLock::new(ProduceRequestCollection::new()));

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

                if buffer_lock.size() > 0 {
                    let mut new_ref = ProduceRequestCollection::new();

                    std::mem::swap(&mut *buffer_lock, &mut new_ref);

                    drop(buffer_lock); // Explicitly drop the lock.

                    if let Err(err) =
                        riskless::flush(new_ref, object_store_ref.clone(), indexes_ref).await
                    {
                        tracing::error!("Error occurred when trying to flush buffer: {:#?}", err);
                    }
                }
            }
        });

        // Accumulator task.
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                tracing::info!("Received request: {:#?}", req);
                let buffer_lock = cloned_buffer_ref.read().await;

                let _ = buffer_lock.collect(req.inner().clone());

                // TODO: This is currently hardcoded to 50kb, but we possibly want to make
                if buffer_lock.size() > segment_size_in_bytes {
                    let _ = flush_tx.send(()).await;
                }
            }

            // TODO: what here if there is None?
        });

        Self {
            streams: BTreeMap::new(),
            object_store: Arc::new(object_store::memory::InMemory::new()),
            indexes,
            segment_size_in_bytes,
            flush_interval_in_ms,
            produce_request_tx: tx,
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
    pub async fn produce(&self, topic_name: &str, partition: &str, record_batch: RecordBatch) {
        let data = write_arrow(&record_batch);

        let request = ProduceRequest {
            request_id: 1,
            topic: topic_name.to_string(),
            partition: 1,
            data,
        };

        tracing::info!("Producing Request {:#?}.", request);

        let (request, response) = Request::new(request);

        // let (produce_response_tx, produce_response_rx) = tokio::sync::oneshot::channel();

        self.produce_request_tx.send(request).await.unwrap();

        let message = response.recv().await.unwrap();

        // TODO: fix this to actually return the error?
        if message.errors.len() > 0 {
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
    pub fn new(func: ReductionFn, mut rx: Receiver, tx: Sender) {
        tokio::spawn(async move {
            let last_value: Option<RecordBatch> = None;

            while let Ok(value) = rx.recv().await {
                let result = func(&last_value, &value);

                if let Err(e) = tx.send(result) {
                    tracing::error!("Error sending result: {:#?}", e);
                };
            }
        });
    }
}
