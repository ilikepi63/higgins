mod configuration;
mod consume;
mod default;
mod indexes;
mod instantiate;
mod produce;
mod streams;
mod subscriptions;

pub use indexes::BrokerIndexFile;

use arrow::{array::RecordBatch, datatypes::Schema};
use riskless::{
    messages::{ProduceRequest, ProduceRequestCollection, ProduceResponse},
    object_store::{self, ObjectStore},
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
};
use tokio::sync::{Notify, RwLock};

use crate::functions::collection::FunctionCollection;
use crate::{
    client::ClientCollection, error::HigginsError, storage::index::directory::IndexDirectory,
    subscription::Subscription, topography::Topography, utils::request_response::Request,
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
    dir: PathBuf,
    streams: BTreeMap<Vec<u8>, (Arc<Schema>, Sender, Receiver)>,
    object_store: Arc<dyn ObjectStore>,

    // Concurrency control for indexing files.
    indexes: Arc<IndexDirectory>,
    broker_indexes: Vec<(String, Vec<u8>, std::sync::Arc<tokio::sync::Mutex<()>>)>,
    pub flush_interval_in_ms: u64,
    pub segment_size_in_bytes: u64,
    collection: MutableCollection,
    flush_tx: tokio::sync::mpsc::Sender<()>,

    // Subscriptions.
    #[allow(clippy::type_complexity)]
    subscriptions: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, (Arc<Notify>, Arc<RwLock<Subscription>>)>>,

    // Clients
    pub clients: ClientCollection,

    // Topography.
    topography: Topography,

    // Functions
    pub functions: FunctionCollection,
}

impl Broker {
    /// Retrieve the receiver for a named stream.
    pub fn get_receiver(&self, stream_name: &[u8]) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (_, tx, _rx))| tx.subscribe())
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
            for (_, sub) in subs.values_mut() {
                let sub = sub.write().await;

                sub.add_partition(partition_key, None, None)?;
            }
        }

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
