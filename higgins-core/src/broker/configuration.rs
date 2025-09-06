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


    // Ideally what should happen here is that configurations get applied to topographies,
    // and then the state of the topography creates resources inside of the broker. However,
    // due to focus on naive implementations, we're going to just apply the configuration directly.
    pub async fn apply_configuration(
        &mut self,
        config: &[u8],
        broker: Arc<RwLock<Self>>,
    ) -> Result<(), HigginsError> {
        // Deserialize configuratio from TOML.
        let config = from_toml(config);

        // Apply the configuration to the topography.
        apply_configuration_to_topography(config, &mut self.topography)?;

        // Generate Stream metadata to create.
        let streams_to_create = self
            .topography
            .streams
            .iter()
            .filter_map(|(stream_key, def)| {
                if !self.streams.contains_key(stream_key.inner()) {
                    let schema = self.topography.schema.get(&def.schema).unwrap().clone();

                    return Some((stream_key.clone(), schema));
                }

                None
            })
            .collect::<Vec<_>>();

        for (key, schema) in streams_to_create {
            self.create_stream(key.inner(), schema);
        }

        // Retrieve derived streams metadata.
        let derived_streams = self
            .topography
            .streams
            .iter()
            .filter_map(|(key, def)| def.base.as_ref().map(|_| (key.to_owned(), def.to_owned())))
            .collect::<Vec<_>>();

        for (derived_stream_key, derived_stream_definition) in derived_streams {
            match derived_stream_definition.stream_type {
                Some(FunctionType::Join) => {
                    let join = derived_stream_definition.join.as_ref().cloned().unwrap();

                    let left = self
                        .topography
                        .streams
                        .iter()
                        .find(|(key, _)| *key == derived_stream_definition.base.as_ref().unwrap())
                        .map(|(key, def)| (key.clone(), def.clone()))
                        .unwrap();

                    let right = self
                        .topography
                        .streams
                        .iter()
                        .find(|(key, _)| {
                            key.inner() == derived_stream_definition.join.as_ref().unwrap().key()
                        })
                        .map(|(key, def)| (key.clone(), def.clone()))
                        .unwrap();

                    create_joined_stream_from_definition(
                        derived_stream_key.clone(),
                        derived_stream_definition.clone(),
                        left.clone(),
                        right.clone(),
                        join.clone(),
                        self,
                        broker.clone(),
                    )
                    .await
                    .unwrap();

                    create_joined_stream_from_definition(
                        derived_stream_key,
                        derived_stream_definition,
                        right,
                        left,
                        join,
                        self,
                        broker.clone(),
                    )
                    .await
                    .unwrap();
                }
                Some(FunctionType::Map) => {
                    tracing::trace!("Creating Mapped stream definition.");

                    let left = self
                        .topography
                        .streams
                        .iter()
                        .find(|(key, _)| *key == derived_stream_definition.base.as_ref().unwrap())
                        .map(|(key, def)| (key.clone(), def.clone()))
                        .unwrap();

                    create_mapped_stream_from_definition(
                        derived_stream_key,
                        derived_stream_definition,
                        left,
                        self,
                        broker.clone(),
                    )
                    .await
                    .unwrap();
                }
                Some(FunctionType::Reduce) => {
                    tracing::trace!("Creating Reduced stream definition.");

                    let left = self
                        .topography
                        .streams
                        .iter()
                        .find(|(key, _)| *key == derived_stream_definition.base.as_ref().unwrap())
                        .map(|(key, def)| (key.clone(), def.clone()))
                        .unwrap();

                    create_reduced_stream_from_definition(
                        derived_stream_key,
                        derived_stream_definition,
                        left,
                        self,
                        broker.clone(),
                    )
                    .await
                    .unwrap();
                }
                Some(_) => todo!(),
                None => {
                    panic!("There should be a type associated with a derived stream.");
                }
            }
        }

        Ok(())
    }
