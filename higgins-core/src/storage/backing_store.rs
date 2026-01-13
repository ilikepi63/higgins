//! Currently, this is referred to as the `backing store`, which is a temporary name
//! given to the hard storage that backs up the streams from Higgins.

use crate::error::HigginsError;
use crate::storage::shared_log_segment::SharedLogSegment;
use std::time::Duration;

use riskless::{
    messages::{ProduceRequest, ProduceRequestCollection},
    object_store::{self},
};
use std::{collections::BTreeMap, fs::create_dir, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

use crate::functions::collection::FunctionCollection;
use crate::{
    client::ClientCollection, storage::batch_coordinate::BatchCoordinate,
    storage::index::directory::IndexDirectory, topography::Topography,
    utils::request_response::Request,
};

/// Represents the roles starting
trait BackingStore {
    type Error;

    fn start_task(&self) -> Result<Flusher, Self::Error>;
}

pub struct Flusher(tokio::sync::mpsc::Sender<()>);

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, BatchCoordinate>>,
    )>,
>;

/// Backing store that replicates the S3 API.
struct ObjectBackingStore {
    flush_interval_in_ms: u64,
    object_store: Arc<dyn ObjectStore>,
    collection: MutableCollection,
    flush_tx: Option<tokio::sync::mpsc::Sender<()>>,
}

impl ObjectBackingStore {
    pub fn new(store: Arc<dyn ObjectStore>, flush_interval: u64) -> Self {
        let collection = Arc::new(RwLock::new((ProduceRequestCollection::new(), vec![])));

        Self {
            flush_interval_in_ms: flush_interval,
            object_store: store,
            collection,
            flush_tx: None,
        }
    }
}

impl BackingStore for ObjectBackingStore {
    type Error = HigginsError;

    fn start_task(&self) -> Result<Flusher, Self::Error> {
        let object_store = self.object_store.clone();

        let flush_interval_in_ms = self.flush_interval_in_ms;

        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);
        // self.flush_tx = Some(flush_tx.clone());
        let object_store_ref = object_store.clone();
        let buffer = self.collection.clone();

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
                            let mut iter = new_collection_vec.into_iter();

                            // We need to fix riskless here.
                            for response in responses {
                                // TODO: O(n^2) here
                                let res = iter
                                    .find(|r| r.inner().request_id == response.request.request_id)
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

        Ok(Flusher(flush_tx))
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

    // TODO: assert put_result has the correct response?

    // TODO: The responses here?
    // let put_result = index_dir
    //     .commit_file(
    //         path.into_bytes(),
    //         1,
    //         buf_size.try_into()?,
    //         batch_coords
    //             .iter()
    //             .map(CommitBatchRequest::from)
    //             .collect::<Vec<_>>(),
    //         broker,
    //     )
    //     .await;

    // Ok(put_result
    //     .iter()
    //     .map(ProduceResponse::from)
    //     .collect::<Vec<_>>())

    Ok(batch_coords)
}
