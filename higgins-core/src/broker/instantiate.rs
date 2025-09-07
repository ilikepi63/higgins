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


impl Broker{
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
            flush_tx,
            dir,
            subscriptions: BTreeMap::new(),
            topography: Topography::new(),
            clients: ClientCollection::empty(),
            functions: FunctionCollection::new(functions_dir),
        }
    }
}
