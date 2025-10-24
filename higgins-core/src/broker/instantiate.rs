use super::Broker;
use crate::storage::shared_log_segment::SharedLogSegment;

use riskless::{
    messages::{BatchCoordinate, ProduceRequest, ProduceRequestCollection, ProduceResponse},
    object_store::{self},
};
use std::{collections::BTreeMap, fs::create_dir, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;

use crate::functions::collection::FunctionCollection;
use crate::{
    client::ClientCollection, storage::index::directory::IndexDirectory, topography::Topography,
    utils::request_response::Request,
};

type MutableCollection = Arc<
    RwLock<(
        ProduceRequestCollection,
        Vec<Request<ProduceRequest, BatchCoordinate>>,
    )>,
>;

impl Broker {
    /// Creates a new instance of a Broker.
    pub fn new(dir: PathBuf) -> Self {
        if !dir.exists() {
            std::fs::create_dir(&dir).unwrap();
        }
        let index_dir = {
            let mut path = dir.clone();

            path.push("index");

            if !path.exists() {
                std::fs::create_dir(&path).unwrap();
                path
            } else {
                path
            }
        };

        let flush_interval_in_ms: u64 = 500;
        let segment_size_in_bytes: u64 = 50_000;

        let object_store_ref = {
            let mut dir = dir.clone();

            dir.push("data");

            if !dir.exists() {
                std::fs::create_dir(&dir).unwrap();
            }

            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir).unwrap())
        };

        let object_store = object_store_ref.clone();

        let indexes = Arc::new(IndexDirectory::new(index_dir).unwrap());

        let buffer: MutableCollection =
            Arc::new(RwLock::new((ProduceRequestCollection::new(), vec![])));

        let cloned_buffer_ref = buffer.clone();

        let functions_dir = {
            let mut cwd = dir.clone();
            cwd.push("functions");
            if let Err(e) = create_dir(&cwd) {
                tracing::trace!("Error when creating functions dir: {:#?}", e);
            }
            cwd
        };

        Self {
            streams: BTreeMap::new(),
            object_store,
            indexes,
            segment_size_in_bytes,
            flush_interval_in_ms,
            collection: cloned_buffer_ref,
            flush_tx: None,
            dir,
            subscriptions: BTreeMap::new(),
            topography: Topography::new(),
            clients: ClientCollection::empty(),
            functions: FunctionCollection::new(functions_dir),
            broker_indexes: Vec::new(),
        }
    }

    pub fn start(&mut self, broker: Arc<RwLock<Self>>) {
        let flush_interval_in_ms = self.flush_interval_in_ms;

        let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(1);
        self.flush_tx = Some(flush_tx);
        let object_store_ref = self.object_store.clone();
        let buffer = self.collection.clone();
        let indexes_ref = self.indexes.clone();

        // Flusher task.
        tokio::task::spawn(async move {
            loop {
                let broker = broker.clone();

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
    }
}

// This is a shim for a workaround while we fix flush.
use object_store::ObjectStore;
use object_store::PutPayload;
use riskless::messages::CommitBatchRequest;

async fn flush(
    reqs: ProduceRequestCollection,
    object_storage: Arc<dyn ObjectStore>,
    // index_dir: Arc<IndexDirectory>,
    // broker: std::sync::Arc<tokio::sync::RwLock<Broker>>,
) -> Result<Vec<BatchCoordinate>, Box<dyn std::error::Error>> {
    let reqs: SharedLogSegment = reqs.try_into()?;

    let batch_coords = reqs.get_batch_coords().clone();

    let buf: bytes::Bytes = reqs.into();

    // let buf_size = buf.len();

    let path = uuid::Uuid::new_v4();

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
