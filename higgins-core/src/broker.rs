mod configuration;
mod consume;
mod default;
mod indexes;
mod instantiate;
mod produce;
mod streams;
mod subscriptions;

use crate::task::TaskHandler;
use arrow::{array::RecordBatch, datatypes::Schema};
use higgins_shared::PartitionName;
pub use indexes::BrokerIndexFile;
use riskless::object_store;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::{Notify, RwLock};

use crate::functions::collection::FunctionCollection;
use crate::{
    client::ClientCollection, error::HigginsError, storage::backing_store::BackingStore,
    storage::index::directory::IndexDirectory, subscription::Subscription, topography::Topography,
};

type Receiver = tokio::sync::broadcast::Receiver<RecordBatch>;
type Sender = tokio::sync::broadcast::Sender<RecordBatch>;

/// This is a pretty naive implementation of what the broker might look like.
#[derive(Debug)]
pub struct Broker {
    dir: PathBuf,
    streams: BTreeMap<Vec<u8>, (Arc<Schema>, Sender, Receiver)>,

    // Concurrency control for indexing files.
    indexes: Arc<IndexDirectory>,
    broker_indexes: Vec<(String, Vec<u8>, std::sync::Arc<tokio::sync::Mutex<()>>)>,
    pub backing_store: Option<Arc<dyn BackingStore<Error = HigginsError>>>,

    // Subscriptions.
    #[allow(clippy::type_complexity)]
    subscriptions: BTreeMap<Vec<u8>, BTreeMap<Vec<u8>, (Arc<Notify>, Arc<RwLock<Subscription>>)>>,

    // Clients
    pub clients: ClientCollection,

    // Topography.
    topography: Topography,

    // Functions
    pub functions: FunctionCollection,

    pub task_handler: TaskHandler,
}

impl Broker {
    /// Retrieve the receiver for a named stream.
    pub fn get_receiver(&self, stream_name: &[u8]) -> Option<Receiver> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_name)
            .map(|(_, (_, tx, _rx))| tx.subscribe())
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
        key: &PartitionName,
    ) -> Result<(), HigginsError> {
        tracing::trace!("[CREATE PARTITION] Creating the partition");
        if let Some(subs) = self.subscriptions.get_mut(stream_name) {
            for (_, sub) in subs.values_mut() {
                tracing::trace!("[CREATE PARTITION] Taking the lock.");

                let mut sub = sub.write().await;

                tracing::trace!("[CREATE PARTITION] Retrieved the lock..");

                tracing::trace!("[CREATE_PARTITION] Creating: {:#?}", sub);

                if sub
                    .partitions
                    .iter()
                    .find(|sub_key| sub_key.partition_id == *key)
                    .is_none()
                {
                    sub.add_partition(key, None, None)?;
                };
            }
        }

        Ok(())
    }

    /// Retrieves a ClientRef given a client id.
    pub fn get_client_by_id(&self, id: u64) -> Option<crate::client::ClientRef> {
        self.clients.get(id).cloned()
    }
}
