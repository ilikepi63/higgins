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
    /// Inner join definition if this is a joined stream.
    pub inner_join: Option<String>,
    /// Left Outer join definition if this is a joined stream.
    pub left_join: Option<String>,
    /// Right Outer join definition if this is a joined stream.
    pub right_join: Option<String>,
    /// Full Outer Join definition if this is a joined stream.
    pub full_join: Option<String>,

    /// The mapping of values given this is a join operation.
    pub map: Option<BTreeMap<String, String>>, // TODO: This needs to reflect the hierarchical nature of this string implementation. 
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
pub fn from_yaml(config: &[u8]) -> Configuration {
    let config: Configuration = serde_yaml::from_slice(config).unwrap();

    config
}

#[cfg(test)]
mod test {
    use core::panic;

    use super::*;
    use crate::topography::config::from_yaml;

    #[test]
    fn can_deserialize_basic_yaml() {
        let example_config = std::fs::read_to_string("tests/configs/basic_config.yaml").unwrap();

        let config = from_yaml(example_config.as_bytes());

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
                            inner_join: None,
                            left_join: None,
                            right_join: None,
                            full_join: None,
                            map: None,
                        },
                    );
                    streams.insert(
                        "update_customer".to_string(),
                        ConfigurationStreamDefinition {
                            base: None,
                            stream_type: None,
                            partition_key: "id".to_string(),
                            schema: "update_customer_event".to_string(),
                            inner_join: None,
                            left_join: None,
                            right_join: None,
                            full_join: None,
                            map: None,
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
    fn can_deserialize_join_yaml() {
        let example_config = std::fs::read_to_string("tests/configs/join_config.yaml").unwrap();

        let config = from_yaml(example_config.as_bytes());

        // Assert entire struct equality with inline expected values
        // assert_eq!(
        //     config,
        //     Configuration {
        //         streams: {
        //             let mut streams = BTreeMap::new();
        //             streams.insert(
        //                 "customer".to_string(),
        //                 ConfigurationStreamDefinition {
        //                     base: None,
        //                     stream_type: None,
        //                     partition_key: "id".to_string(),
        //                     schema: "update_customer_event".to_string(),
        //                     inner_join: None,
        //                     left_join: None,
        //                     right_join: None,
        //                     full_join: None,
        //                     map: None,
        //                 },
        //             );
        //             streams.insert(
        //                 "customer_product".to_string(),
        //                 ConfigurationStreamDefinition {
        //                     base: Some("customer".to_string()),
        //                     stream_type: Some("join".to_string()),
        //                     partition_key: "customer_id".to_string(),
        //                     schema: "customer_address".to_string(),
        //                     inner_join: Some("address".to_string()),
        //                     left_join: None,
        //                     right_join: None,
        //                     full_join: None,
        //                     map: Some({
        //                         let mut map = BTreeMap::new();
        //                         map.insert(
        //                             "address_line_1".to_string(),
        //                             "address.address_line_1".to_string(),
        //                         );
        //                         map.insert(
        //                             "address_line_2".to_string(),
        //                             "address.address_line_2".to_string(),
        //                         );
        //                         map.insert("age".to_string(), "customer.age".to_string());
        //                         map.insert("city".to_string(), "address.city".to_string());
        //                         map.insert(
        //                             "customer_first_name".to_string(),
        //                             "customer.first_name".to_string(),
        //                         );
        //                         map.insert("customer_id".to_string(), "customer.id".to_string());
        //                         map.insert(
        //                             "customer_last_name".to_string(),
        //                             "customer.last_name".to_string(),
        //                         );
        //                         map.insert("province".to_string(), "address.province".to_string());
        //                         map
        //                     }),
        //                 },
        //             );
        //             streams.insert(
        //                 "product".to_string(),
        //                 ConfigurationStreamDefinition {
        //                     base: None,
        //                     stream_type: None,
        //                     partition_key: "id".to_string(),
        //                     schema: "product".to_string(),
        //                     inner_join: None,
        //                     left_join: None,
        //                     right_join: None,
        //                     full_join: None,
        //                     map: None,
        //                 },
        //             );
        //             streams
        //         },
        //         schema: {
        //             let mut schema = BTreeMap::new();
        //             let mut customer_fields = BTreeMap::new();
        //             customer_fields.insert("age".to_string(), "int32".to_string());
        //             customer_fields.insert("first_name".to_string(), "string".to_string());
        //             customer_fields.insert("id".to_string(), "string".to_string());
        //             customer_fields.insert("last_name".to_string(), "string".to_string());
        //             schema.insert("customer".to_string(), customer_fields);

        //             let mut address_fields = BTreeMap::new();
        //             address_fields.insert("address_line_1".to_string(), "string".to_string());
        //             address_fields.insert("address_line_2".to_string(), "string".to_string());
        //             address_fields.insert("city".to_string(), "string".to_string());
        //             address_fields.insert("id".to_string(), "string".to_string());
        //             address_fields.insert("province".to_string(), "string".to_string());
        //             schema.insert("address".to_string(), address_fields);

        //             let mut customer_address_fields = BTreeMap::new();
        //             customer_address_fields
        //                 .insert("address_line_1".to_string(), "string".to_string());
        //             customer_address_fields
        //                 .insert("address_line_2".to_string(), "string".to_string());
        //             customer_address_fields.insert("age".to_string(), "int32".to_string());
        //             customer_address_fields.insert("city".to_string(), "string".to_string());
        //             customer_address_fields
        //                 .insert("customer_first_name".to_string(), "string".to_string());
        //             customer_address_fields.insert("customer_id".to_string(), "string".to_string());
        //             customer_address_fields
        //                 .insert("customer_last_name".to_string(), "string".to_string());
        //             customer_address_fields.insert("province".to_string(), "string".to_string());
        //             schema.insert("customer_address".to_string(), customer_address_fields);
        //             schema
        //         },
        //     },
        //     "Configuration struct should match expected values"
        // );

        // Assert individual fields
        assert_eq!(
            config.streams.len(),
            3,
            "Should have three stream definitions"
        );
        assert_eq!(
            config.streams.get("customer").unwrap().partition_key,
            "id",
            "Customer stream partition key should be id"
        );
        assert_eq!(
            config.streams.get("customer_product").unwrap().stream_type,
            Some("join".to_string()),
            "Customer_product stream type should be join"
        );
        assert_eq!(
            config.streams.get("customer_product").unwrap().inner_join,
            Some("address".to_string()),
            "Customer_product inner join should be address"
        );
        assert_eq!(
            config.streams.get("product").unwrap().schema,
            "product",
            "Product stream schema should be product"
        );
        assert_eq!(
            config.schema.len(),
            3,
            "Should have three schema definitions"
        );
        assert_eq!(
            config
                .schema
                .get("customer_address")
                .unwrap()
                .get("customer_id")
                .unwrap(),
            "string",
            "Customer_address schema customer_id field should be string"
        );
    }
}
