//! Currently, this is referred to as the `backing store`, which is a temporary name
//! given to the hard storage that backs up the streams from Higgins.

use crate::storage::shared_log_segment::SharedLogSegment;
use crate::utils::request_response::Response;
use higgins_shared::{HigginsError, PartitionName, StreamName, UniqueCollection};
use std::sync::Mutex;
use std::time::Duration;

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{storage::batch_coordinate::BatchCoordinate, utils::request_response::Request};

use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{DashMap, Entry, iter::IterMut};

/// A collection of ProduceRequests.
///
///  This is primarily used to be converted into a SharedLogSegment.
#[derive(Debug)]
pub struct ProduceRequestCollection {
    /// A concurrent data structure for handling produce requests for each topic/partition combination.
    pub inner: DashMap<(StreamName, PartitionName), Vec<ProduceRequest>>,
    /// The size in bytes for this collection.
    pub size: AtomicU64,
}

impl Default for ProduceRequestCollection {
    fn default() -> Self {
        Self::new()
    }
}

impl ProduceRequestCollection {
    /// Create a new intance of this struct.
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
            size: AtomicU64::new(0),
        }
    }

    /// Clear this struct.
    pub fn clear(&mut self) {
        self.inner.clear();
        self.size = AtomicU64::new(0);
    }

    /// Creates a new collection, swaps it with this instance and returns the given collection.
    pub fn take(&mut self) -> Self {
        let mut other = ProduceRequestCollection::new();

        std::mem::swap(&mut *self, &mut other);

        other
    }

    /// Collect a produce request into this struct.
    pub fn collect(&self, req: ProduceRequest) -> Result<(), HigginsError> {
        let topic_id_partition = (req.stream.clone(), req.partition.clone());

        let entry = self.inner.entry(topic_id_partition);

        match entry {
            Entry::Occupied(mut occupied_entry) => {
                self.size
                    .fetch_add(TryInto::<u64>::try_into(req.data.len())?, Ordering::Relaxed);
                occupied_entry.get_mut().push(req.clone());
            }
            Entry::Vacant(vacant_entry) => {
                self.size
                    .fetch_add(TryInto::<u64>::try_into(req.data.len())?, Ordering::Relaxed);
                vacant_entry.insert(vec![req.clone()]);
            }
        }

        Ok(())
    }

    /// Get the size in bytes for this collection.
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Iterate over the partitions of this structure.
    pub fn iter_partitions(
        &mut self,
    ) -> IterMut<'_, (StreamName, PartitionName), Vec<ProduceRequest>> {
        self.inner.iter_mut()
    }
}

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub request_id: u64,
    pub stream: StreamName,
    pub partition: PartitionName,
    pub data: Vec<u8>,
}

/// Represents the roles starting
pub trait BackingStore: Send + Sync + std::fmt::Debug {
    type Error;

    fn start_task(&mut self) -> Result<(), Self::Error>;
    // Temporary shim to allow consumptions to happen.
    fn get_object_store(&self) -> Arc<dyn ObjectStore>;

    /// Put data into this data store.
    fn put(
        &self,
        stream: StreamName,
        partition: PartitionName,
        data: Vec<u8>,
    ) -> Result<Response<BatchCoordinate>, Self::Error>;
}

pub struct Flusher(pub tokio::sync::mpsc::Sender<()>);

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, BatchCoordinate>>,
    )>,
>;

/// Backing store that replicates the S3 API.
#[derive(Debug)]
pub struct ObjectBackingStore {
    flush_interval_in_ms: u64,
    object_store: Arc<dyn ObjectStore>,
    collection: MutableCollection,
    flush_tx: Option<tokio::sync::mpsc::Sender<()>>,
    request_id_collection: Arc<Mutex<UniqueCollection<()>>>,
}

impl ObjectBackingStore {
    pub fn new(store: Arc<dyn ObjectStore>, flush_interval: u64) -> Self {
        let collection = Arc::new(RwLock::new((ProduceRequestCollection::new(), vec![])));

        Self {
            flush_interval_in_ms: flush_interval,
            object_store: store,
            collection,
            flush_tx: None,
            request_id_collection: Arc::new(Mutex::new(UniqueCollection::empty())),
        }
    }
}

impl BackingStore for ObjectBackingStore {
    type Error = HigginsError;

    fn get_object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    fn put(
        &self,
        stream: StreamName,
        partition: PartitionName,
        data: Vec<u8>,
    ) -> Result<Response<BatchCoordinate>, Self::Error> {
        let request_id = {
            let mut unique_collection = self
                .request_id_collection
                .lock()
                .map_err(|e| HigginsError::Arbitrary(e.to_string()))?;
            unique_collection.insert(()).ok_or(HigginsError::Arbitrary(
                "Failed to retrieve request ID".to_string(),
            ))?
        };

        let request = ProduceRequest {
            request_id,
            stream,
            partition,
            data,
        };

        let (request, response) = Request::<ProduceRequest, BatchCoordinate>::new(request);

        let collection_ref = self.collection.clone();
        let flush_tx = self
            .flush_tx
            .as_ref()
            .ok_or(HigginsError::Arbitrary(
                "No flush task found for this backing store.".to_string(),
            ))?
            .clone();

        tokio::spawn(async move {
            let mut buffer_lock = collection_ref.write().await;

            let _ = buffer_lock.0.collect(request.inner().clone());

            buffer_lock.1.push(request);

            // TODO: This is currently hardcoded to 50kb, but we possibly want to make
            if buffer_lock.0.size() > 50_000 {
                let _ = flush_tx.send(()).await;
            }

            drop(buffer_lock);
        });

        Ok(response)
    }

    fn start_task(&mut self) -> Result<(), Self::Error> {
        let object_store = self.object_store.clone();

        let flush_interval_in_ms = self.flush_interval_in_ms;

        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);
        // self.flush_tx = Some(flush_tx.clone());
        let object_store_ref = object_store.clone();
        let buffer = self.collection.clone();

        let request_id_collection = self.request_id_collection.clone();

        // Flusher task.
        tokio::task::spawn(async move {
            loop {
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

                    match flush(new_ref, object_store_ref.clone()).await {
                        Ok(responses) => {
                            while let Some(r) = new_collection_vec.pop() {
                                if let Some(batch_coord) =
                                    responses.iter().find(|batch_coordinate| {
                                        batch_coordinate.request.request_id == r.inner().request_id
                                    })
                                {
                                    let request_id = r.inner().request_id;

                                    if let Err(err) = r.respond(batch_coord.clone()) {
                                        tracing::error!(
                                            "Failed to respond to storage input: {:#?}",
                                            err
                                        );
                                        continue;
                                    };

                                    if let Ok(mut collection) = request_id_collection.lock() {
                                        collection.remove(request_id);
                                    }
                                } else {
                                    tracing::error!(
                                        "Couldn't find response for request id {}",
                                        r.inner().request_id
                                    );
                                };
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

        self.flush_tx = Some(flush_tx);

        Ok(())
    }
}

// This is a shim for a workaround while we fix flush.
use object_store::ObjectStore;
use object_store::PutPayload;

async fn flush(
    reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    // index_dir: Arc<IndexDirectory>,
    // broker: std::sync::Arc<tokio::sync::RwLock<Broker>>,
) -> Result<Vec<BatchCoordinate>, Box<dyn std::error::Error>> {
    let path = uuid::Uuid::new_v4();

    let reqs: SharedLogSegment = (path.as_bytes().to_owned(), reqs).try_into()?;

    let batch_coords = reqs.get_batch_coords().clone();

    let buf: bytes::Bytes = reqs.into();

    let path_string = object_store::path::Path::from(path.to_string());

    let _put_result = object_storage
        .put(&path_string, PutPayload::from_bytes(buf))
        .await?;

    Ok(batch_coords)
}
