use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

/// A Configuration is a serializable value that corresponds to a
/// unit of implementation. These implementations aggregate to become a
/// Topography. A configuration itself is also a Topography once it has been applied.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Configuration {
    pub streams: BTreeMap<String, ConfigurationStreamDefinition>,
    pub schema: BTreeMap<String, Schema>,
    // functions: BTreeMap<String, Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ConfigurationStreamDefinition {
    /// From which this stream is derived.
    pub base: Option<String>,
    /// The Function type for this derived function if it is a derived function.
    #[serde(rename = "type")]
    pub stream_type: Option<String>,
    /// The partition key for this topic.
    pub partition_key: String,
    /// The schema for this, references a key in schema.
    pub schema: String,

    /// Joins for this stream.
    pub join: Option<Vec<String>>,

    /// The mapping of values given this is a join operation.
    pub map: Option<BTreeMap<String, String>>, // TODO: This needs to reflect the hierarchical nature of this string implementation.

    /// The name of the function that needs to be applied to this configuration.
    #[serde(rename = "fn")]
    pub function_name: Option<String>,
}

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
pub fn from_toml(config: &[u8]) -> Configuration {
    let config: Configuration = toml::from_slice(config).unwrap();

    config
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::topography::config::from_toml;

    #[test]
    fn can_deserialize_basic_toml() {
        let example_config = r#"
            [schema.update_customer_event]
            id = "string"
            first_name = "string"
            last_name = "string"
            age = "int32"

            [schema.customer]
            id = "string"
            first_name = "string"
            last_name = "string"
            age = "int32"

            [streams.update_customer]
            schema = "update_customer_event"
            partition_key = "id"

            [streams.customer]
            base = "update_customer"
            type = "reduce"
            partition_key = "id"
            schema = "customer"
            "#;

        let config = from_toml(example_config.as_bytes());

        // Assert entire struct equality with inline expected values
        assert_eq!(
            config,
            Configuration {
                streams: {
                    let mut streams = BTreeMap::new();
                    streams.insert(
                        "customer".to_string(),
                        ConfigurationStreamDefinition {
                            base: Some("update_customer".to_string()),
                            stream_type: Some("reduce".to_string()),
                            partition_key: "id".to_string(),
                            schema: "customer".to_string(),
                            join: None,
                            map: None,
                            function_name: None,
                        },
                    );
                    streams.insert(
                        "update_customer".to_string(),
                        ConfigurationStreamDefinition {
                            base: None,
                            stream_type: None,
                            partition_key: "id".to_string(),
                            schema: "update_customer_event".to_string(),
                            join: None,
                            map: None,
                            function_name: None,
                        },
                    );
                    streams
                },
                schema: {
                    let mut schema = BTreeMap::new();
                    let mut customer_fields = BTreeMap::new();
                    customer_fields.insert("age".to_string(), "int32".to_string());
                    customer_fields.insert("first_name".to_string(), "string".to_string());
                    customer_fields.insert("id".to_string(), "string".to_string());
                    customer_fields.insert("last_name".to_string(), "string".to_string());
                    schema.insert("customer".to_string(), customer_fields);

                    let mut update_customer_fields = BTreeMap::new();
                    update_customer_fields.insert("age".to_string(), "int32".to_string());
                    update_customer_fields.insert("first_name".to_string(), "string".to_string());
                    update_customer_fields.insert("id".to_string(), "string".to_string());
                    update_customer_fields.insert("last_name".to_string(), "string".to_string());
                    schema.insert("update_customer_event".to_string(), update_customer_fields);
                    schema
                },
            },
            "Configuration struct should match expected values"
        );

        // Assert individual fields
        assert_eq!(
            config.streams.len(),
            2,
            "Should have two stream definitions"
        );
        assert_eq!(
            config.streams.get("customer").unwrap().partition_key,
            "id",
            "Customer stream partition key should be id"
        );
        assert_eq!(
            config.streams.get("customer").unwrap().stream_type,
            Some("reduce".to_string()),
            "Customer stream type should be reduce"
        );
        assert_eq!(
            config.streams.get("update_customer").unwrap().base,
            None,
            "Update_customer stream base should be None"
        );
        assert_eq!(config.schema.len(), 2, "Should have two schema definitions");
        assert_eq!(
            config.schema.get("customer").unwrap().get("age").unwrap(),
            "int32",
            "Customer schema age field should be int32"
        );
    }

    #[test]
    fn can_deserialize_join_toml() {
        let example_config = r#"
            [schema.customer]
            id = "string"
            first_name = "string"
            last_name = "string"
            age = "int32"

            [schema.address]
            customer_id = "string"
            address_line_1 = "string"
            address_line_2 = "string"
            city = "string"
            province = "string"

            [schema.customer_address]
            customer_id = "string"
            customer_first_name = "string"
            customer_last_name = "string"
            age = "int32"
            address_line_1 = "string"
            address_line_2 = "string"
            city = "string"
            province = "string"

            [streams.customer]
            schema = "customer"
            partition_key = "id"

            [streams.address]
            schema = "address"
            partition_key = "id"

            [streams.customer_address]
            type = "join"
            schema = "customer_address"
            partition_key = "customer_id"
            base = "customer"
            inner_join = "address"

            [streams.customer_address.map]
            customer_id = "customer.id"
            customer_first_name = "customer.first_name"
            customer_last_name = "customer.last_name"
            age = "customer.age"
            address_line_1 = "address.address_line_1"
            address_line_2 = "address.address_line_2"
            city = "address.city"
            province = "address.province"
            "#;

        let config = from_toml(example_config.as_bytes());

        // Define the expected Configuration struct
        let expected = Configuration {
            schema: BTreeMap::from([
                (
                    "customer".to_string(),
                    BTreeMap::from([
                        ("id".to_string(), "string".to_string()),
                        ("first_name".to_string(), "string".to_string()),
                        ("last_name".to_string(), "string".to_string()),
                        ("age".to_string(), "int32".to_string()),
                    ]),
                ),
                (
                    "address".to_string(),
                    BTreeMap::from([
                        ("customer_id".to_string(), "string".to_string()),
                        ("address_line_1".to_string(), "string".to_string()),
                        ("address_line_2".to_string(), "string".to_string()),
                        ("city".to_string(), "string".to_string()),
                        ("province".to_string(), "string".to_string()),
                    ]),
                ),
                (
                    "customer_address".to_string(),
                    BTreeMap::from([
                        ("customer_id".to_string(), "string".to_string()),
                        ("customer_first_name".to_string(), "string".to_string()),
                        ("customer_last_name".to_string(), "string".to_string()),
                        ("age".to_string(), "int32".to_string()),
                        ("address_line_1".to_string(), "string".to_string()),
                        ("address_line_2".to_string(), "string".to_string()),
                        ("city".to_string(), "string".to_string()),
                        ("province".to_string(), "string".to_string()),
                    ]),
                ),
            ]),
            streams: BTreeMap::from([
                (
                    "customer".to_string(),
                    ConfigurationStreamDefinition {
                        base: None,
                        stream_type: None,
                        partition_key: "id".to_string(),
                        schema: "customer".to_string(),
                        join: None,
                        map: None,
                        function_name: None,
                    },
                ),
                (
                    "address".to_string(),
                    ConfigurationStreamDefinition {
                        base: None,
                        stream_type: None,
                        partition_key: "id".to_string(),
                        schema: "address".to_string(),
                        join: None,
                        map: None,
                        function_name: None,
                    },
                ),
                (
                    "customer_address".to_string(),
                    ConfigurationStreamDefinition {
                        base: Some("customer".to_string()),
                        stream_type: Some("join".to_string()),
                        partition_key: "customer_id".to_string(),
                        schema: "customer_address".to_string(),
                        join: None,
                        map: Some(BTreeMap::from([
                            ("customer_id".to_string(), "customer.id".to_string()),
                            (
                                "customer_first_name".to_string(),
                                "customer.first_name".to_string(),
                            ),
                            (
                                "customer_last_name".to_string(),
                                "customer.last_name".to_string(),
                            ),
                            ("age".to_string(), "customer.age".to_string()),
                            (
                                "address_line_1".to_string(),
                                "address.address_line_1".to_string(),
                            ),
                            (
                                "address_line_2".to_string(),
                                "address.address_line_2".to_string(),
                            ),
                            ("city".to_string(), "address.city".to_string()),
                            ("province".to_string(), "address.province".to_string()),
                        ])),
                        function_name: None,
                    },
                ),
            ]),
        };

        // Assert that the deserialized configuration matches the expected one
        assert_eq!(
            config, expected,
            "Deserialized configuration does not match expected"
        );
    }
}
