use super::Broker;
use crate::derive::joining::{create_joined_stream_from_definition, join::JoinDefinition};
use crate::derive::subscription::create_derived_stream_subscription;
use crate::derive::windowed::create_windowed_stream_from_definition;
use crate::derive::windowed::definition::WindowedStreamDefinition;
use crate::storage::backing_store::{BackingStore, ObjectBackingStore};
use crate::topography::config::{Storage, StorageType};
use crate::topography::{Relation, StreamName};
use object_store::aws::AmazonS3Builder;
use riskless::object_store::memory::InMemory;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    derive::{
        map::create_mapped_stream_from_definition, reduce::create_reduced_stream_from_definition,
    },
    topography::FunctionType,
};
use crate::{error::HigginsError, topography::config::from_toml};

impl Broker {
    // Ideally what should happen here is that configurations get applied to topographies,
    // and then the state of the topography creates resources inside of the broker. However,
    // due to focus on naive implementations, we're going to just apply the configuration directly.
    pub async fn apply_configuration(
        &mut self,
        config: &[u8],
        broker: Arc<RwLock<Self>>,
    ) -> Result<(), HigginsError> {
        tracing::trace!("Deserializing the toml.");
        tracing::trace!("{:#?}", String::from_utf8(config.to_vec()));

        // Deserialize configuration from TOML.
        let config = from_toml(config);
        tracing::trace!("Retrieved the config: {:#?}.", config);

        // Apply the configuration to the topography.
        self.topography.apply_configuration_to_topography(&config)?;

        tracing::trace!("Successfully applied the configuration to the topography.");

        // Apply the storages.
        if let Some(storage) = self.topography.get_storage() {
            let backing_store = instantiate_storage_from_configuration(storage);
            self.backing_store = Some(backing_store);
        }

        tracing::trace!("Successfully instantiated the storage.");

        // Generate Stream metadata to create.
        let streams_to_create = self
            .topography
            .get_streams()
            .iter()
            .filter_map(|(stream_key, def)| {
                if !self
                    .streams
                    .contains_key(&Into::<Vec<u8>>::into(stream_key))
                {
                    let schema = self
                        .topography
                        .get_schema_by_key(def.schema.clone().into())?
                        .clone();

                    return Some((stream_key.clone(), schema));
                }

                None
            })
            .collect::<Vec<_>>();

        tracing::trace!("Creating streams...");

        for (key, schema) in streams_to_create {
            self.create_stream(Into::<Vec<u8>>::into(key).as_ref(), schema);
        }

        // Retrieve derived streams metadata.
        let derived_streams = self
            .topography
            .get_streams()
            .iter()
            .filter_map(|(key, def)| def.base.as_ref().map(|_| (key.to_owned(), def.to_owned())))
            .collect::<Vec<_>>();

        for (derived_stream_key, derived_stream_definition) in derived_streams {
            match derived_stream_definition.stream_type {
                Some(FunctionType::Join) => {
                    tracing::trace!("Creating the Joined Stream definition.");

                    let definition = {
                        let b: &Broker = self;

                        JoinDefinition::try_from((
                            derived_stream_key,
                            derived_stream_definition,
                            b,
                        ))?
                    };

                    create_joined_stream_from_definition(definition, self, broker.clone())
                        .await
                        .unwrap();
                }
                Some(FunctionType::Map) => {
                    tracing::trace!("Creating Mapped stream definition.");

                    let stream_name = StreamName::from(derived_stream_key.clone());

                    let (_client_id, subscription) =
                        create_derived_stream_subscription(stream_name.clone(), broker.clone())
                            .await;

                    let relation = Relation {
                        stream_name,
                        definition: derived_stream_definition.clone(),
                        subscription,
                        join_index: None,
                    };

                    let base_key = derived_stream_definition
                        .base
                        .as_ref()
                        .map(|base_key| StreamName::from(base_key.clone()))
                        .ok_or(HigginsError::Unknown)?;

                    self.relations.push((base_key, relation));

                    // let base = self
                    //     .topography
                    //     .get_streams()
                    //     .iter()
                    //     .find(|(key, _)| *key == derived_stream_definition.base.as_ref().unwrap())
                    //     .map(|(key, def)| (key.clone(), def.clone()))
                    //     .unwrap();

                    // create_mapped_stream_from_definition(
                    //     derived_stream_key,
                    //     derived_stream_definition,
                    //     base,
                    //     self,
                    //     broker.clone(),
                    // )
                    // .await
                    // .unwrap();
                }
                Some(FunctionType::Reduce) => {
                    tracing::trace!("Creating Reduced stream definition.");

                    let left = self
                        .topography
                        .get_streams()
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
                Some(FunctionType::Window) => {
                    tracing::trace!("Creating Windowed stream from stream definition.");
                    let b: &Broker = self;

                    create_windowed_stream_from_definition(
                        WindowedStreamDefinition::try_from((
                            derived_stream_key,
                            derived_stream_definition,
                            b,
                        ))
                        .unwrap(),
                        self,
                        broker.clone(),
                    )
                    .await;
                }
                Some(_) => todo!(),
                None => {
                    panic!("There should be a type associated with a derived stream.");
                }
            }
        }

        Ok(())
    }

    pub fn get_topography_as_config_string(&self) -> Result<String, HigginsError> {
        Ok(self.topography.to_config_toml()?)
    }
}

use crate::topography::config::AwsS3Storage;

pub fn instantiate_storage_from_configuration(
    (_storage_config_name, storage_config): &(String, Storage),
) -> Arc<dyn BackingStore<Error = HigginsError>> {
    match storage_config.storage_type {
        StorageType::Memory => {
            let store = InMemory::new();

            let mut backing_store = ObjectBackingStore::new(Arc::new(store), 250);
            backing_store.start_task().unwrap();

            Arc::new(backing_store) // TODO: magic number here.
        }
        StorageType::File => {
            todo!()
        }
        StorageType::S3 => {
            let s3_builder = AmazonS3Builder::new();

            let AwsS3Storage {
                aws_access_key_id,
                aws_allow_http,
                aws_endpoint,
                aws_region,
                aws_secret_access_key,
                aws_token,
                bucket_name,
                ..
            } = storage_config.aws_s3_config.clone();

            let s3_builder = add_to_builder(
                aws_access_key_id,
                |builder, val| builder.with_access_key_id(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                aws_allow_http,
                |builder, val| builder.with_allow_http(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                aws_endpoint,
                |builder, val| builder.with_endpoint(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                aws_region,
                |builder, val| builder.with_region(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                aws_secret_access_key,
                |builder, val| builder.with_secret_access_key(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                aws_token,
                |builder, val| builder.with_token(val),
                s3_builder,
            );

            let s3_builder = add_to_builder(
                bucket_name,
                |builder, val| builder.with_bucket_name(val),
                s3_builder,
            );

            let store = s3_builder.build().unwrap();

            let mut backing_store = ObjectBackingStore::new(Arc::new(store), 250);
            backing_store.start_task().unwrap();

            Arc::new(backing_store) // TODO: magic number here.
        }
    }
}

fn add_to_builder<T, F: Fn(AmazonS3Builder, T) -> AmazonS3Builder>(
    val: Option<T>,
    f: F,
    builder: AmazonS3Builder,
) -> AmazonS3Builder {
    if let Some(val) = val {
        f(builder, val)
    } else {
        builder
    }
}
