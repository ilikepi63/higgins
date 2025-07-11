use std::{collections::BTreeMap, sync::Arc};

use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

use crate::topography::{Key, StreamDefinition};

/// A Configuration is a serializable value that corresponds to a
/// unit of implementation. These implementations aggregate to become a
/// Topography. A configuration itself is also a Topography once it has been applied.
#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    pub streams: BTreeMap<String, ConfigurationStreamDefinition>,
    pub schema: BTreeMap<String, Schema>,
    // functions: BTreeMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConfigurationStreamDefinition {
    /// From which this topic is derived.
    pub derived: Option<String>,
    /// The Function type for this derived function if it is a derived function.
    #[serde(rename = "type")]
    pub fn_type: Option<String>,
    /// The partition key for this topic.
    pub partition_key: String,
    /// The schema for this, references a key in schema.
    pub schema: String,
}

type NamedSchemaCollection = BTreeMap<String, Schema>;

type Schema = BTreeMap<String, String>;

pub fn schema_to_arrow_schema(schema: &Schema) -> arrow::datatypes::Schema {
    let fields = schema
        .iter()
        .map(|(key, value)| {
            let data_type = match value.as_ref() {
                "string" => DataType::Utf8,
                "uint8" => DataType::UInt8,
                "uint16" => DataType::UInt16,
                "uint32" => DataType::UInt32,
                "uint64" => DataType::UInt64,
                "int8" => DataType::Int8,
                "int16" => DataType::Int16,
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                _ => unimplemented!(),
            };

            Field::new(key, data_type, false) // TODO: how do we handle nullable here? OR how do we actually determine them?
        })
        .collect::<Vec<_>>();

    arrow::datatypes::Schema::new(fields)
}

/// Deserializes the given byte array into a configuration.
pub fn from_yaml(config: &[u8]) -> Configuration {
    let config: Configuration = serde_yaml::from_slice(config).unwrap();

    config
}


#[cfg(test)]
mod test {
    use crate::topography::config::from_yaml;

    #[test]
    fn can_deserialize_basic_yaml() {
        let example_config = std::fs::read_to_string("tests/basic_config.yaml").unwrap();

        let _config = from_yaml(example_config.as_bytes());
    }
}
