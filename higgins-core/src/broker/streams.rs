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


impl Broker {
    pub fn get_stream(&self, stream_name: &[u8]) -> Option<&(Arc<Schema>, Sender, Receiver)> {
        self.streams.get(stream_name)
    }

    /// Create a Stream.
    pub fn create_stream(&mut self, stream_name: &[u8], schema: Arc<Schema>) {
        let (tx, rx) = tokio::sync::broadcast::channel(100);

        self.streams
            .insert(stream_name.to_owned(), (schema, tx, rx));
    }


}
