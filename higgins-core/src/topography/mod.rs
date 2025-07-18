//! A topography is the definition of the higgins' cluster at any given time.
//!
//! This includes the metadata of which topics exist, what schema they have
//! and how they are partitioned.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::topography::{
    config::{Configuration, ConfigurationStreamDefinition, schema_to_arrow_schema},
    errors::TopographyError,
};

pub mod config;
pub mod errors;

/// Used to index into Topography system.
/// TODO: perhaps make this sized?
#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Key(Vec<u8>);

impl Key {
    pub fn inner(&self) -> &[u8] {
        &self.0
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

/// A topography explains all of the existing streams, schema and the associated keys within them.
#[derive(Debug)]
pub struct Topography {
    pub streams: BTreeMap<Key, StreamDefinition>,
    pub schema: BTreeMap<Key, Arc<Schema>>,
    pub functions: BTreeMap<Key, Vec<u8>>,
    pub configurations: BTreeMap<Key, Configuration>,
    pub subscriptions: BTreeMap<Key, SubscriptionDeclaration>,
}

impl Topography {
    pub fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
            schema: BTreeMap::new(),
            functions: BTreeMap::new(),
            configurations: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
        }
    }

    pub fn add_schema(&mut self, key: Key, schema: Arc<Schema>) -> Result<(), TopographyError> {
        // For the most part, this will just upload the schema as there should not be any dependencies/references inside of it.

        let entry = self.schema.entry(key);

        match entry {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(schema);
                Ok(())
            }
            Entry::Occupied(_) => Err(TopographyError::Occupied(format!(""))), // TODO: add more meat to this error messag .
        }
    }
    pub fn add_stream(
        &mut self,
        key: Key,
        stream: StreamDefinition,
    ) -> Result<(), TopographyError> {
        // Check the schema exists.
        if let None = self.schema.get(&stream.schema) {
            return Err(TopographyError::SchemaNotFound(format!(
                "{:#?}",
                stream.schema
            )));
        }

        // Check if the derivations exist inside of this topography.
        if let Some(key) = stream.base.as_ref() {
            if let None = self.streams.get(key) {
                return Err(TopographyError::DerivativeNotFound(format!("{:#?}", key)));
            }
        }

        // Check if the function exists.
        if let Some(key) = stream.base.as_ref() {
            if let None = self.streams.get(key) {
                return Err(TopographyError::DerivativeNotFound(format!("{:#?}", key)));
            }
        }

        let entry = self.streams.entry(key);

        match entry {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(stream);
                Ok(())
            }
            Entry::Occupied(_) => Err(TopographyError::Occupied(format!(""))), // TODO: add more meat to this error messag .
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StreamDefinition {
    /// From which this topic is derived.
    pub base: Option<Key>,
    /// The Function type for this derived function if it is a derived function.
    #[serde(rename = "type")]
    pub stream_type: Option<FunctionType>,
    /// The partition key for this topic.
    pub partition_key: Key,
    /// The schema for this, references a key in schema.
    pub schema: Key,
}

impl From<&ConfigurationStreamDefinition> for StreamDefinition {
    fn from(value: &ConfigurationStreamDefinition) -> Self {
        StreamDefinition {
            base: value.base.as_ref().map(|s| s.as_str().into()),
            stream_type: value.stream_type.as_ref().map(|s| s.as_str().into()),
            partition_key: Key::from(value.partition_key.as_str()),
            schema: value.schema.as_str().into(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FunctionType {
    Reduce,
    Map,
    Aggregate,
    Join,
}

impl From<&str> for FunctionType {
    fn from(value: &str) -> Self {
        match value {
            "reduce" => FunctionType::Reduce,
            "map" => FunctionType::Map,
            "aggregate" => FunctionType::Aggregate,
            "join" => FunctionType::Join,
            _ => {
                panic!("Unmplemented function type {value}. Options are reduce, map and aggregate.")
            }
        }
    }
}

#[derive(Debug)]
pub struct SubscriptionDeclaration {
    #[allow(unused)]
    topic: Vec<u8>,
}

pub fn apply_configuration_to_topography(
    configuration: Configuration,
    topography: &mut Topography,
) -> Result<Key, TopographyError> {
    tracing::info!(
        "Applying configuration {:#?} to Topography: {:#?}",
        configuration,
        topography
    );

    let _schema = configuration
        .schema
        .iter()
        .map(|(name, schema)| (name.clone(), Arc::new(schema_to_arrow_schema(schema))))
        .for_each(|(key, schema)| {
            let _ = topography.add_schema(Key::from(key.as_str()), schema); // TODO: perhaps this should be a warning?.
        });

    // Create the non-derived streams first.
    for (stream_name, topic_defintion) in configuration
        .streams
        .iter()
        .filter(|(_, def)| def.base.is_none())
    {
        match &topic_defintion.base {
            Some(_derived_from) => unreachable!(),
            None => {
                tracing::trace!("Applying stream {}", stream_name);
                let result =
                    topography.add_stream(Key::from(stream_name.as_str()), topic_defintion.into());

                tracing::trace!("Result from applying stream: {:#?}", result);
            }
        }
    }

    for (stream_name, topic_defintion) in configuration
        .streams
        .iter()
        .filter(|(_, def)| def.base.is_some())
    {
        match &topic_defintion.base {
            Some(_derived_from) => {
                tracing::trace!("Applying a derived stream: {stream_name}..");

                // Create just normal schema.
                let _schema = topography
                    .schema
                    .get(&Key::from(topic_defintion.schema.as_str()))
                    .unwrap_or_else(|| {
                        panic!("No Schema defined for key {}", topic_defintion.schema)
                    });

                let _topic_type = FunctionType::from(
                    topic_defintion
                        .stream_type
                        .as_ref()
                        .expect("Derived stream without a function type.")
                        .as_str(),
                );

                let _ = topography.add_stream(
                    Key::from(stream_name.as_str()),
                    StreamDefinition::from(topic_defintion),
                ); // TODO: This should likely be a warning.
            }
            None => unreachable!(),
        }
    }

    let config_id = uuid::Uuid::new_v4();

    let config_id = Key(config_id.as_bytes().to_vec());

    topography
        .configurations
        .insert(config_id.clone(), configuration);

    tracing::trace!("Typography after application: {:#?}", topography);

    Ok(config_id)
}
