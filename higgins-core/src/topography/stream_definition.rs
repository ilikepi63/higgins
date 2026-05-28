use super::config::WindowDefinition;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug};

use crate::{
    storage::index::{IndexType, index_size_from_index_type_and_definition},
    topography::config::ConfigurationStreamDefinition,
};

use super::{FunctionType, Key};

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
    /// Windowing configuration
    pub window: Option<WindowDefinition>,
}

impl StreamDefinition {
    /// Returns the index size specified from this stream definition.
    ///
    /// The index size should always be able to be calculated from the definition given
    /// the dynamic properties of some of the stream values.
    pub fn index_size(&self) -> usize {
        index_size_from_index_type_and_definition(&self.index_type(), self)
    }

    pub fn index_type(&self) -> IndexType {
        match self.stream_type {
            Some(FunctionType::Join) => IndexType::Join,
            Some(FunctionType::Window) => IndexType::Window,
            Some(FunctionType::Aggregate)
            | Some(FunctionType::Map)
            | Some(FunctionType::Reduce)
            | None => IndexType::Default,
        }
    }
}

impl Debug for StreamDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamDefinition")
            .field("base", &self.base.as_ref())
            .field("stream_type", &self.stream_type)
            .field("partition_key", &self.partition_key.0)
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
            window: value.window.clone(),
        }
    }
}

#[allow(unused)]
#[derive(Serialize, Deserialize, Clone)]
struct JoinStreamDefinition {
    /// The partition key for this topic.
    pub partition_key: Key,
    /// The schema for this, references a key in schema.
    pub schema: Key,
    /// The Join for this stream definition.
    pub join: Vec<String>,
    /// The mapping of values given this is a join operation.
    pub map: BTreeMap<String, String>, // TODO: This needs to reflect the hierarchical nature of this string implementation.
}

#[allow(unused)]
#[derive(Serialize, Deserialize, Clone)]
struct WindowedStreamDefinition {
    /// From which this topic is derived.
    pub base: Key,
    /// The partition key for this topic.
    pub partition_key: Key,
    /// The schema for this, references a key in schema.
    pub schema: Key,
    /// Windowing configuration
    pub window: WindowDefinition,
}

#[allow(unused)]
#[derive(Serialize, Deserialize, Clone)]
struct MapStreamDefinition {
    /// From which this topic is derived.
    pub base: Key,
    /// The partition key for this topic.
    pub partition_key: Key,
    /// The schema for this, references a key in schema.
    pub schema: Key,
    /// The name of the function that needs to be applied to this configuration.
    #[serde(rename = "fn")]
    pub function_name: String,
}

#[allow(unused)]
#[derive(Serialize, Deserialize, Clone)]
struct ReduceStreamDefinition {
    /// From which this topic is derived.
    pub base: Key,
    /// The partition key for this topic.
    pub partition_key: Key,
    /// The schema for this, references a key in schema.
    pub schema: Key,
    /// The name of the function that needs to be applied to this configuration.
    #[serde(rename = "fn")]
    pub function_name: String,
}
