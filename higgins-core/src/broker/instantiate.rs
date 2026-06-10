use super::Broker;

use std::{collections::BTreeMap, fs::create_dir, path::PathBuf, sync::Arc};

use crate::functions::collection::FunctionCollection;
use crate::task::TaskHandler;
use crate::{
    client::ClientCollection, storage::index::directory::IndexDirectory, topography::Topography,
};
use higgins_functions::wasmtime::{Config, Engine, OptLevel};
use higgins_shared::HigginsError;

impl Broker {
    /// Creates a new instance of a Broker.
    pub fn new(dir: PathBuf) -> Result<Self, HigginsError> {
        if !dir.exists() {
            std::fs::create_dir(&dir)?;
        }
        let index_dir = {
            let mut path = dir.clone();

            path.push("index");

            if !path.exists() {
                std::fs::create_dir(&path)?;
                path
            } else {
                path
            }
        };

        let indexes = Arc::new(IndexDirectory::new(index_dir)?);

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

        let topography_dir = dir.clone();

        Ok(Self {
            streams: BTreeMap::new(),
            indexes,
            dir,
            backing_store: None,
            subscriptions: BTreeMap::new(),
            topography: Topography::from_file(topography_dir)?,
            clients: ClientCollection::empty(),
            functions: FunctionCollection::new(functions_dir),
            broker_indexes: Vec::new(),
            task_handler: TaskHandler::new(),
            wasm_engine: Engine::new(
                Config::new()
                    .debug_info(true)
                    .coredump_on_trap(true)
                    .cranelift_opt_level(OptLevel::None),
            )
            .map_err(|err| HigginsError::Arbitrary(err.to_string()))?,
            wasm_modules: vec![],
            non_reactive_subscriptions: BTreeMap::new(),
            relations: vec![],
        })
    }
}
