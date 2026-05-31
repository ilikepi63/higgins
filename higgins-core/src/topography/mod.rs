//! A topography is the definition of the higgins' cluster at any given time.
//!
//! This includes the metadata of which topics exist, what schema they have
//! and how they are partitioned.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    fmt::Debug,
    sync::Arc,
};

use crate::topography::{
    config::{
        Configuration, ConfigurationStreamDefinition, Storage, arrow_schema_to_schema,
        schema_to_arrow_schema,
    },
    errors::TopographyError,
};

pub mod config;
mod stream_definition;
pub use stream_definition::*;
mod data_type_parser;
pub mod errors;
mod file;

use file::TopographyFile;

use crate::subscription::SubscriptionId;
pub use data_type_parser::parse_time_unit;

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct StreamName(String);

impl From<&[u8]> for StreamName {
    fn from(value: &[u8]) -> Self {
        Self(String::from_utf8_lossy(value).to_string())
    }
}

/// Used to index into Topography system.
/// TODO: perhaps make this sized?
#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Key(String);

impl Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key").field("inner", &self.0).finish()
    }
}

impl Key {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<&str> for Key {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<&String> for Key {
    fn from(value: &String) -> Self {
        Self(value.to_owned())
    }
}

impl From<Key> for String {
    fn from(val: Key) -> Self {
        val.0
    }
}

impl From<&Key> for String {
    fn from(val: &Key) -> Self {
        val.0.to_owned()
    }
}

impl From<Key> for Vec<u8> {
    fn from(val: Key) -> Self {
        val.0.into_bytes()
    }
}
impl From<&Key> for Vec<u8> {
    fn from(val: &Key) -> Self {
        val.0.clone().into_bytes()
    }
}

impl TryFrom<&[u8]> for Key {
    type Error = std::string::FromUtf8Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Key(String::from_utf8(value.to_owned())?))
    }
}

/// A topography explains all of the existing streams, schema and the associated keys within them.
#[derive(Debug)]
pub struct Topography {
    file: TopographyFile,
    streams: BTreeMap<Key, StreamDefinition>,
    schema: BTreeMap<Key, Arc<Schema>>,
    storage: Option<(String, Storage)>,
    /// The streams that represent the streams that can be produced to.
    ///
    /// This structure is used for quick lookup to a dependency tree for each stream.
    primitive_streams_dependency_graph: Vec<(Key, Vec<(Key, SubscriptionId)>)>,
}

type Described<T> = (String, T);

/// A unit that is atomically added to a typography.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum TopographyUnit {
    Stream(Described<StreamDefinition>),
    Schema(Described<Schema>),
    Storage(Described<Storage>),
}

impl Topography {
    pub fn from_file(file: std::path::PathBuf) -> Result<Self, TopographyError> {
        let file = TopographyFile::new(file);

        let (streams, schema, storage) = match file.read() {
            Ok(operations) => Ok(operations.iter().fold(
                (BTreeMap::new(), BTreeMap::new(), None),
                |mut acc, unit| {
                    match unit {
                        TopographyUnit::Stream((key, stream)) => {
                            acc.0.insert(Key::from(key), stream.clone());
                        }
                        TopographyUnit::Schema((key, schema)) => {
                            acc.1.insert(Key::from(key), Arc::new(schema.clone()));
                        }
                        TopographyUnit::Storage((key, storage)) => {
                            acc.2 = Some((key.to_owned(), storage.clone()))
                        }
                    };

                    acc
                },
            )),
            Err(err) => {
                if let TopographyError::IOError(err) = &err
                    && err.kind() == std::io::ErrorKind::NotFound
                {
                    Ok((BTreeMap::new(), BTreeMap::new(), None))
                } else {
                    Err(err)
                }
            }
        }?;

        Ok(Self {
            file,
            streams,
            schema,
            storage,
            primitive_streams_dependency_graph: vec![],
        })
    }

    /// Converst this to TOML-represented Config.
    pub fn to_config_toml(&self) -> Result<String, TopographyError> {
        Ok(toml::to_string(&self.to_config())?)
    }

    /// Converts this Topography into a configuration.
    pub fn to_config(&self) -> Configuration {
        let streams = if !self.streams.is_empty() {
            Some(
                self.streams
                    .iter()
                    .map(|(key, definition)| {
                        (
                            key.clone().into(),
                            ConfigurationStreamDefinition::from(definition.clone()),
                        )
                    })
                    .collect::<BTreeMap<String, ConfigurationStreamDefinition>>(),
            )
        } else {
            None
        };

        let schema = if !self.schema.is_empty() {
            Some(
                self.schema
                    .iter()
                    .map(|(key, definition)| {
                        (key.clone().into(), arrow_schema_to_schema(definition))
                    })
                    .collect::<BTreeMap<String, config::Schema>>(),
            )
        } else {
            None
        };

        let storage = self
            .storage
            .clone()
            .map(|storage| BTreeMap::from([storage]));

        Configuration {
            streams,
            schema,
            storage,
        }
    }

    pub fn add_schema(&mut self, key: Key, schema: Arc<Schema>) -> Result<(), TopographyError> {
        // For the most part, this will just upload the schema as there should not be any dependencies/references inside of it.

        let entry = self.schema.entry(key.clone());

        match entry {
            Entry::Vacant(vacant_entry) => {
                self.file
                    .add_item(TopographyUnit::Schema((key.into(), (*schema).clone())))?;
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
        if let Some(base_key) = stream.base.as_ref()
            && !self.streams.contains_key(base_key)
        {
            return Err(TopographyError::DerivativeNotFound(format!("{key:#?}")));
        }

        // Check if the function exists.
        if let Some(key) = stream.base.as_ref()
            && !self.streams.contains_key(key)
        {
            return Err(TopographyError::DerivativeNotFound(format!("{key:#?}")));
        }

        let entry = self.streams.entry(key.clone());

        match entry {
            Entry::Vacant(vacant_entry) => {
                self.file
                    .add_item(TopographyUnit::Stream((key.into(), stream.clone())))?;
                vacant_entry.insert(stream);
                Ok(())
            }
            Entry::Occupied(_) => Err(TopographyError::Occupied(String::new())), // TODO: add more meat to this error messag .
        }
    }

    /// Retrieve the stream definition of the given stream key.
    pub fn get_stream_definition_by_key(&self, stream: String) -> Option<&StreamDefinition> {
        self.streams.get(&Key::from(&stream))
    }

    /// Retrieve the stream definition of the given stream key.
    pub fn get_schema_by_key(&self, schema_key: String) -> Option<&Arc<Schema>> {
        self.schema.get(&Key::from(&schema_key))
    }

    /// Gets the storage setup for this topography.
    pub fn get_storage(&self) -> Option<&(String, Storage)> {
        self.storage.as_ref()
    }

    /// Gets the storage setup for this topography.
    pub fn add_storage(&mut self, storage: &(String, Storage)) -> Result<(), TopographyError> {
        self.file.add_item(TopographyUnit::Storage((
            storage.0.clone(),
            storage.1.clone(),
        )))?;
        self.storage = Some(storage.clone());
        Ok(())
    }

    pub fn get_streams(&self) -> &BTreeMap<Key, StreamDefinition> {
        &self.streams
    }

    /// Applies an entire configuration to this topography.
    pub fn apply_configuration_to_topography(
        &mut self,
        configuration: &Configuration,
    ) -> Result<(), TopographyError> {
        tracing::info!(
            "Applying configuration {:#?} to Topography: {:#?}",
            configuration,
            self
        );

        // Apply the Storage configuration..
        if let Some(storage) = configuration.storage.as_ref()
            && let Some((name, storage)) = storage.first_key_value()
        {
            self.add_storage(&(name.clone(), storage.clone()))?;
        }

        if let Some(schema) = configuration.schema.as_ref() {
            schema
                .iter()
                .map(|(name, schema)| (name.clone(), Arc::new(schema_to_arrow_schema(schema))))
                .for_each(|(key, schema)| {
                    let _ = self.add_schema(Key::from(key.as_str()), schema); // TODO: perhaps this should be a warning?.
                });
        }

        // Create the non-derived streams first.
        for (stream_name, topic_defintion) in configuration
            .streams
            .as_ref()
            .unwrap()
            .iter()
            .filter(|(_, def)| def.base.is_none())
        {
            match &topic_defintion.base {
                Some(_derived_from) => {
                    unreachable!()
                }
                None => {
                    tracing::trace!("Applying stream {}", stream_name);
                    let result =
                        self.add_stream(Key::from(stream_name.as_str()), topic_defintion.into());

                    tracing::trace!("Result from applying stream: {:#?}", result);
                }
            }
        }

        for (stream_name, stream_definition) in configuration
            .streams
            .as_ref()
            .unwrap()
            .iter()
            .filter(|(_, def)| def.base.is_some())
        {
            match &stream_definition.base {
                Some(_derived_from) => {
                    tracing::trace!("Applying a derived stream: {stream_name}..");

                    // // Create just normal schema.
                    // let _schema = self
                    //     .schema
                    //     .get(&Key::from(topic_defintion.schema.as_str()))
                    //     .unwrap_or_else(|| {
                    //         panic!("No Schema defined for key {}", topic_defintion.schema)
                    //     });

                    // let _topic_type = FunctionType::from(
                    //     topic_defintion
                    //         .stream_type
                    //         .as_ref()
                    //         .expect("Derived stream without a function type.")
                    //         .as_str(),
                    // );

                    let _ = self.add_stream(
                        Key::from(stream_name.as_str()),
                        StreamDefinition::from(stream_definition),
                    ); // TODO: This should likely be a warning.
                }
                None => {
                    tracing::error!("Unreachable code.");
                    unreachable!()
                }
            }
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FunctionType {
    Reduce,
    Map,
    Aggregate,
    Join,
    Window,
}

impl From<&str> for FunctionType {
    fn from(value: &str) -> Self {
        match value {
            "reduce" => FunctionType::Reduce,
            "map" => FunctionType::Map,
            "aggregate" => FunctionType::Aggregate,
            "join" => FunctionType::Join,
            "window" => FunctionType::Window,
            _ => {
                tracing::error!("Panicked on unimplemented type.");
                panic!("Unmplemented function type {value}. Options are reduce, map and aggregate.")
            }
        }
    }
}

impl From<FunctionType> for String {
    fn from(val: FunctionType) -> Self {
        match val {
            FunctionType::Reduce => "reduce".to_string(),
            FunctionType::Map => "map".to_string(),
            FunctionType::Aggregate => "aggregate".to_string(),
            FunctionType::Join => "join".to_string(),
            FunctionType::Window => "window".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct SubscriptionDeclaration {
    #[allow(unused)]
    topic: Vec<u8>,
}

#[cfg(test)]
pub mod test {

    use super::*;
    use crate::topography::config::from_toml;
    use std::panic::catch_unwind;

    static BASIC_CONFIG: &str = r#"
    [storage.s3]
    type="s3"
    aws_access_key_id = "rustfsadmin"
    aws_secret_access_key = "rustfsadmin"
    aws_endpoint = "http://localhost:9000"
    bucket_name = "bucket"
    aws_allow_http = true

    [schema.update_customer_event]
    id = "string"
    first_name = "string"
    last_name = "string"
    age = "int32"

    [streams.update_customer]
    schema = "update_customer_event"
    partition_key = "id"
    "#;

    #[test]
    pub fn can_convert_topography_to_configuration() {
        let file_name = std::path::PathBuf::from(uuid::Uuid::new_v4().to_string());

        let destroy_file_name = file_name.clone();

        let result = catch_unwind(|| {
            let config = from_toml(BASIC_CONFIG.as_bytes());

            let mut topography = Topography::from_file(file_name).unwrap();

            topography
                .apply_configuration_to_topography(&config)
                .unwrap();

            let config_from_topography = topography.to_config();

            assert_eq!(config, config_from_topography);
        });

        std::fs::remove_dir_all(destroy_file_name).unwrap();

        result.unwrap();
    }
}
