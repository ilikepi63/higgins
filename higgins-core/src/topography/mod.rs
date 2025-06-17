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

use crate::topography::errors::TopographyError;

pub mod errors;
pub mod config;

/// Used to index into Topography system.
/// TODO: perhaps make this sized?
#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Key(Vec<u8>);

/// A topography explains all of the existing streams, schema and the associated keys within them.
pub struct Topography {
    streams: BTreeMap<Key, StreamDefinition>,
    schema: BTreeMap<Key, Arc<Schema>>,
    functions: BTreeMap<Key, Vec<u8>>,
}

impl Topography {
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
        if let Some(key) = stream.derived.as_ref() {
            if let None = self.streams.get(key) {
                return Err(TopographyError::DerivativeNotFound(format!("{:#?}", key)));
            }
        }

        // Check if the function exists.
        if let Some(key) = stream.derived.as_ref() {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct StreamDefinition {
    /// From which this topic is derived.
    pub derived: Option<Key>,
    /// The Function type for this derived function if it is a derived function.
    #[serde(rename = "type")]
    pub fn_type: Option<FunctionType>,
    /// The partition key for this topic.
    pub partition_key: String,
    /// The schema for this, references a key in schema.
    pub schema: Key,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FunctionType {
    Reduce,
    Map,
    Aggregate,
}
