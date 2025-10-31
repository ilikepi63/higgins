//! A topography is the definition of the higgins' cluster at any given time.
//!
//! This includes the metadata of which topics exist, what schema they have
//! and how they are partitioned.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::Debug,
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
#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Key(pub Vec<u8>);

impl Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("inner", &String::from_utf8(self.0.clone()))
            .finish()
    }
}

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

impl Default for Topography {
    fn default() -> Self {
        Self::new()
    }
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
            Entry::Occupied(_) => Err(TopographyError::Occupied(String::new())), // TODO: add more meat to this error messag .
        }
    }
    pub fn add_stream(
        &mut self,
        key: Key,
        stream: StreamDefinition,
    ) -> Result<(), TopographyError> {
        // Check the schema exists.
        if !self.schema.contains_key(&stream.schema) {
            return Err(TopographyError::SchemaNotFound(format!(
                "{:#?}",
                stream.schema
            )));
        }

        // Check if the derivations exist inside of this topography.
        if let Some(key) = stream.base.as_ref() {
            if !self.streams.contains_key(key) {
                return Err(TopographyError::DerivativeNotFound(format!("{key:#?}")));
            }
        }

        // Check if the function exists.
        if let Some(key) = stream.base.as_ref() {
            if !self.streams.contains_key(key) {
                return Err(TopographyError::DerivativeNotFound(format!("{key:#?}")));
            }
        }

        let entry = self.streams.entry(key);

        match entry {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(stream);
                Ok(())
            }
            Entry::Occupied(_) => Err(TopographyError::Occupied(String::new())), // TODO: add more meat to this error messag .
        }
    }

    /// Retrieve the stream definition of the given stream key.
    pub fn get_stream_definition_by_key(&self, stream: String) -> Option<&StreamDefinition> {
        self.streams.get(&Key(stream.as_bytes().to_owned()))
    }
}

#[derive(Serialize, Deserialize, Clone)]
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
    /// The Join for this stream definition.
    pub join: Option<Vec<String>>,
    /// The mapping of values given this is a join operation.
    pub map: Option<BTreeMap<String, String>>, // TODO: This needs to reflect the hierarchical nature of this string implementation.
    /// The name of the function that needs to be applied to this configuration.
    #[serde(rename = "fn")]
    pub function_name: Option<String>,
}

impl Debug for StreamDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDefinition")
            .field(
                "base",
                &self.base.as_ref().map(|v| String::from_utf8(v.0.clone())),
            )
            .field("stream_type", &self.stream_type)
            .field(
                "partition_key",
                &String::from_utf8(self.partition_key.0.clone()),
            )
            .field("schema", &self.schema)
            .field("join", &self.join)
            .field("map", &self.map)
            .field("function_name", &self.function_name)
            .finish()
    }
}

impl From<&ConfigurationStreamDefinition> for StreamDefinition {
    fn from(value: &ConfigurationStreamDefinition) -> Self {
        StreamDefinition {
            base: value.base.as_ref().map(|s| s.as_str().into()),
            stream_type: value.stream_type.as_ref().map(|s| s.as_str().into()),
            partition_key: Key::from(value.partition_key.as_str()),
            schema: value.schema.as_str().into(),
            join: value.join.clone(),
            map: value.map.clone(),
            function_name: value.function_name.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Join {
    Inner(Key),
    LeftOuter(Key),
    RightOuter(Key),
    Full(Key),
}

impl Join {
    pub fn key(&self) -> &[u8] {
        match self {
            Join::Inner(key) => key.inner(),
            Join::LeftOuter(key) => key.inner(),
            Join::RightOuter(key) => key.inner(),
            Join::Full(key) => key.inner(),
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

    configuration
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

    Ok(config_id)
}
