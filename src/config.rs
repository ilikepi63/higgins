use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    pub schema: NamedSchemaCollection,
    pub topics: Topics,
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

type Topics = BTreeMap<String, TopicsDefinition>;

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicsDefinition {
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

impl Configuration {
    pub fn from_env() -> Self {
        let config = std::fs::read_to_string("config.yaml").unwrap();

        let config: Configuration = serde_yaml::from_str(&config).unwrap();

        config
    }
}
