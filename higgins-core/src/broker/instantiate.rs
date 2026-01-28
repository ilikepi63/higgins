use super::Broker;

// use riskless::messages::{ProduceRequest, ProduceRequestCollection};
use std::{collections::BTreeMap, fs::create_dir, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

use crate::functions::collection::FunctionCollection;
use crate::task::TaskHandler;
use crate::{
    client::ClientCollection, storage::index::directory::IndexDirectory, topography::Topography,
};

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

        let indexes = Arc::new(IndexDirectory::new(index_dir).unwrap());

        let functions_dir = {
            let mut cwd = dir.clone();
            cwd.push("functions");
            if let Err(e) = create_dir(&cwd) {
                tracing::trace!("Error when creating functions dir: {:#?}", e);
            }
            cwd
        };

        let _subscriptions_dir = {
            let mut cwd = dir.clone();
            cwd.push("subscriptions");
            if let Err(e) = create_dir(&cwd) {
                tracing::trace!("Error when creating functions dir: {:#?}", e);
            }
            cwd
        };

        Self {
            streams: BTreeMap::new(),
            indexes,
            dir,
            backing_store: None,
            subscriptions: BTreeMap::new(),
            topography: Topography::new(),
            clients: ClientCollection::empty(),
            functions: FunctionCollection::new(functions_dir),
            broker_indexes: Vec::new(),
            task_handler: TaskHandler::new(),
        }
    }
}
